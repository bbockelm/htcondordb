package scheddsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// These tests lock in the tailer's handling of DURABLE (105/106-wrapped) vs NON-DURABLE (bare 103,
// no transaction framing) job_queue.log updates -- the negotiator LastRejMatch* / shadow chirp
// updates a busy schedd writes constantly. The invariant they assert: a job present in the mirror
// must never lack JobStatus (the "partial-ad / orphan" signature). A SetAttribute always follows its
// NewClassAd in a valid log, so a merge onto an existing job must never strip its core attributes.

func durTxn(ops string) string { return "105 \n" + ops + "106 \n" }

func submitJob(c, p int) string {
	return durTxn(fmt.Sprintf(
		"101 %d.%d Job Machine\n103 %d.%d ClusterId %d\n103 %d.%d ProcId %d\n103 %d.%d JobStatus 1\n103 %d.%d Owner \"u\"\n",
		c, p, c, p, c, c, p, p, c, p, c, p))
}

// bareRej is a NON-DURABLE negotiator update: bare 103s, NO 105/106 framing.
func bareRej(c, p, ts int) string {
	return fmt.Sprintf(
		"103 %d.%d LastRejMatchReason \"no match found\"\n103 %d.%d LastRejMatchNegotiator \"cm\"\n103 %d.%d LastRejMatchTime %d\n",
		c, p, c, p, c, p, ts)
}

func jobOrphans(t *testing.T, d *db.DB, submitted map[string]bool) []string {
	t.Helper()
	var bad []string
	for _, k := range d.Keys() {
		if submitted != nil && !submitted[k] {
			bad = append(bad, "unexpected:"+k)
			continue
		}
		if ad, ok := d.LookupClassAd(k); ok {
			if _, has := ad.EvaluateAttrInt("JobStatus"); !has {
				bad = append(bad, k)
			}
		}
	}
	return bad
}

func persistentDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir(), SegmentSize: 1 << 14}) // small -> forces sealing
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestDurableNonDurableSequences replays realistic interleavings of durable transactions and bare
// non-durable updates across many poll boundaries, asserting no job is ever left without JobStatus.
func TestDurableNonDurableSequences(t *testing.T) {
	d := persistentDB(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "")
	s := NewJobSync(d, JobSyncConfig{Filename: logPath, Store: store})
	runtime := func(c, p, ts int) string { return durTxn(fmt.Sprintf("103 %d.%d LastJobLeaseRenewal %d\n", c, p, ts)) }

	chunks := []string{
		"107 1 CreationTimestamp 1000\n" + submitJob(100, 0) + submitJob(100, 1) + submitJob(200, 0),
		bareRej(100, 0, 1001) + bareRej(100, 1, 1001) + runtime(200, 0, 1001),                         // bare then durable
		bareRej(100, 0, 1002) + bareRej(100, 1, 1002) + bareRej(200, 0, 1002) + submitJob(300, 0),     // cycle + submit
		bareRej(100, 0, 1003) + bareRej(300, 0, 1003),                                                 // bare-only chunk
		runtime(100, 0, 1004) + bareRej(100, 1, 1004) + runtime(200, 0, 1004),                         // sandwiched
		bareRej(100, 0, 1005) + bareRej(100, 1, 1005) + bareRej(200, 0, 1005) + bareRej(300, 0, 1005), // repeat
	}
	submitted := map[string]bool{"100.0": true, "100.1": true, "200.0": true, "300.0": true}
	for i, c := range chunks {
		appendFile(t, logPath, c)
		if err := s.Poll(context.Background()); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		if bad := jobOrphans(t, d, submitted); len(bad) > 0 {
			t.Errorf("chunk %d: orphans/unexpected: %v", i, bad)
		}
	}
}

// TestPartialRevealNoOrphans reveals a durable+non-durable log to the tailer a few bytes at a time
// (modeling stdio auto-flush landing mid-line / mid-transaction / mid-non-durable-run while the
// tailer polls), asserting no orphan appears at any partial read.
func TestPartialRevealNoOrphans(t *testing.T) {
	d := persistentDB(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewJobSync(d, JobSyncConfig{Filename: logPath, Store: store})

	var full string
	full = "107 1 CreationTimestamp 1000\n" + submitJob(100, 0) + submitJob(100, 1) + submitJob(200, 0) + submitJob(200, 1)
	for ts := 1001; ts <= 1008; ts++ {
		full += bareRej(100, 0, ts) + bareRej(100, 1, ts) + bareRej(200, 0, ts) + bareRej(200, 1, ts)
		full += durTxn(fmt.Sprintf("103 100.0 LastJobLeaseRenewal %d\n", ts))
	}
	submitted := map[string]bool{"100.0": true, "100.1": true, "200.0": true, "200.1": true}
	const step = 7 // irregular -> lands mid-line often
	for pos := 0; pos < len(full); pos += step {
		end := pos + step
		if end > len(full) {
			end = len(full)
		}
		f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
		_, _ = f.WriteString(full[pos:end])
		_ = f.Close()
		if err := s.Poll(context.Background()); err != nil {
			t.Fatalf("pos %d: %v", pos, err)
		}
		if bad := jobOrphans(t, d, submitted); len(bad) > 0 {
			t.Fatalf("pos %d: orphans: %v", pos, bad)
		}
	}
}

// TestConcurrentTailerReader runs a realistic negotiation-cycle write stream through the tailer while
// a reader concurrently counts JobStatus-undefined; every job always has JobStatus, so the count
// must stay 0. Guards against a read/write race producing transient partial ads.
func TestConcurrentTailerReader(t *testing.T) {
	d := persistentDB(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	const J = 300
	seed := "107 1 CreationTimestamp 1000\n"
	for i := 0; i < J; i++ {
		seed += submitJob(1000+i, 0)
	}
	if err := os.WriteFile(logPath, []byte(seed), 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewJobSync(d, JobSyncConfig{Filename: logPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
		defer f.Close()
		for ts := 2000; !stop.Load(); ts++ {
			var chunk string
			for i := 0; i < J; i++ {
				chunk += bareRej(1000+i, 0, ts)
			}
			chunk += durTxn(fmt.Sprintf("103 %d.0 LastJobLeaseRenewal %d\n", 1000+(ts%J), ts))
			_, _ = f.WriteString(chunk)
			_ = s.Poll(context.Background())
		}
	}()
	deadline := time.Now().Add(1500 * time.Millisecond)
	maxUndef := 0
	for time.Now().Before(deadline) {
		if c, _ := d.CountConstraint("JobStatus is undefined"); c > maxUndef {
			maxUndef = c
		}
	}
	stop.Store(true)
	wg.Wait()
	if maxUndef > 0 {
		t.Errorf("JobStatus-undefined reached %d during concurrent tailer writes (want 0)", maxUndef)
	}
}
