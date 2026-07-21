package kafkasync

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// fakeProducer is an in-memory Producer. It records every acknowledged record and can be
// told to fail its next N Produce calls, to exercise the at-least-once replay path.
type fakeProducer struct {
	mu       sync.Mutex
	records  []Record
	failNext int
}

func (f *fakeProducer) Produce(_ context.Context, recs []Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return errors.New("fake broker unavailable")
	}
	for _, r := range recs {
		f.records = append(f.records, r) // per-record byte slices are unique; safe to retain
	}
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func (f *fakeProducer) snapshot() []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Record(nil), f.records...)
}

// latestByKey returns the last record seen for each key (compaction's effect).
func (f *fakeProducer) latestByKey() map[string]Record {
	out := map[string]Record{}
	for _, r := range f.snapshot() {
		out[string(r.Key)] = r
	}
	return out
}

func recVersion(t *testing.T, r Record) uint64 {
	t.Helper()
	for _, h := range r.Headers {
		if h.Key == HeaderVersion {
			return binary.BigEndian.Uint64(h.Value)
		}
	}
	t.Fatalf("record for key %q has no version header", r.Key)
	return 0
}

func isTombstone(r Record) bool { return r.Value == nil }

// testServer starts a privileged, catalog-backed dbrpc server over an in-process pipe and
// returns a client plus the catalog (for direct table writes) and a cleanup.
func testServer(t *testing.T) (*dbrpc.Client, *db.Catalog, string) {
	t.Helper()
	dir := t.TempDir()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := clientFor(t, cat)
	t.Cleanup(func() { cat.Close() })
	return c, cat, dir
}

// clientFor wires a fresh privileged client to a (possibly reopened) catalog.
func clientFor(t *testing.T, cat *db.Catalog) *dbrpc.Client {
	t.Helper()
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	t.Cleanup(func() { c.Close(); s.Close() })
	return c
}

func putAd(t *testing.T, d *db.DB, key, owner string, mem int) {
	t.Helper()
	ad, err := classad.ParseOld("Owner = \"" + owner + "\"; RequestMemory = " + itoa(mem))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put(key, ad); err != nil {
		t.Fatal(err)
	}
}

func newKafkaExporter(t *testing.T, c *dbrpc.Client, name, table string) Config {
	t.Helper()
	cfg, err := Config{Table: table, Brokers: []string{"unused:9092"}, Topic: "t", BatchSize: 2, FlushInterval: Duration(50 * time.Millisecond)}.Validate()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cfg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateExporter(context.Background(), db.ExporterDef{Name: name, Kind: Kind, Config: raw}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// waitFor polls cond until true or the deadline, failing the test on timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRunnerFullExportThenLive: a fresh exporter replays the table as upserts, then mirrors
// live upserts and deletes, with strictly increasing version headers and checkpointed state.
func TestRunnerFullExportThenLive(t *testing.T) {
	c, cat, _ := testServer(t)
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	putAd(t, jobs, "1.0", "alice", 100)
	putAd(t, jobs, "2.0", "bob", 200)

	cfg := newKafkaExporter(t, c, "jobs-kafka", "jobs")
	fp := &fakeProducer{}
	r := NewRunner("jobs-kafka", cfg, c, fp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Initial replay: both keys present.
	waitFor(t, "initial export", func() bool {
		l := fp.latestByKey()
		return len(l) == 2 && l["1.0"].Value != nil && l["2.0"].Value != nil
	})
	init := fp.latestByKey()
	if !strings.Contains(string(init["1.0"].Value), "alice") {
		t.Fatalf("record 1.0 value = %q, want it to contain the ad", init["1.0"].Value)
	}
	// Version headers are unique and increasing across the two.
	if recVersion(t, init["1.0"]) == recVersion(t, init["2.0"]) {
		t.Fatal("versions should differ per record")
	}
	// State was checkpointed with both keys.
	waitFor(t, "state checkpoint", func() bool {
		blob, ok, _ := c.GetExporterState(ctx, "jobs-kafka")
		if !ok {
			return false
		}
		st, _ := decodeState(blob)
		return len(st.KeyVersions) == 2 && len(st.WireCursor) > 0
	})

	// Live upsert + delete.
	putAd(t, jobs, "3.0", "carol", 300)
	if _, err := jobs.Delete("1.0"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "live upsert+delete", func() bool {
		l := fp.latestByKey()
		return l["3.0"].Value != nil && isTombstone(l["1.0"])
	})

	// Highest version belongs to one of the two most recent changes.
	maxVer := uint64(0)
	for _, r := range fp.snapshot() {
		if v := recVersion(t, r); v > maxVer {
			maxVer = v
		}
	}
	if maxVer < 3 {
		t.Fatalf("expected at least 4 versioned records (0..3), max version = %d", maxVer)
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
}

// TestRunnerAtLeastOnceReplay: if a produce fails, the cursor is not advanced, so the
// records are re-produced on the retry (at-least-once). The duplicate carries the same key,
// so a compacted consumer converges.
func TestRunnerAtLeastOnceReplay(t *testing.T) {
	c, cat, _ := testServer(t)
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	putAd(t, jobs, "1.0", "alice", 100)

	cfg := newKafkaExporter(t, c, "jobs-kafka", "jobs")
	fp := &fakeProducer{failNext: 1} // fail the first produce
	r := NewRunner("jobs-kafka", cfg, c, fp, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Despite the first produce failing, the record is eventually delivered on reconnect.
	waitFor(t, "delivery after retry", func() bool {
		return fp.latestByKey()["1.0"].Value != nil
	})
	cancel()
	<-done
}

// TestRunnerDeleteSweepOnReset is the core correctness case: a key deleted while the
// exporter was down must become a tombstone after the reset-driven resync, even though the
// change stream (no before-image) never reports that delete.
func TestRunnerDeleteSweepOnReset(t *testing.T) {
	dir := t.TempDir()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	putAd(t, jobs, "1.0", "alice", 100)
	putAd(t, jobs, "2.0", "bob", 200)
	putAd(t, jobs, "3.0", "carol", 300)

	c := clientFor(t, cat)
	cfg := newKafkaExporter(t, c, "jobs-kafka", "jobs")

	// First run: full export of the three keys, then stop.
	fp1 := &fakeProducer{}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- NewRunner("jobs-kafka", cfg, c, fp1, nil).Run(ctx1) }()
	waitFor(t, "first export", func() bool { return len(fp1.latestByKey()) == 3 })
	cancel1()
	<-done1
	cat.Close()

	// While the exporter is down, reopen the catalog (new watch epoch, forcing a reset on
	// resume) and delete bob.
	cat2, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cat2.Close()
	jobs2, _ := cat2.Table("jobs")
	if ok, err := jobs2.Delete("2.0"); err != nil || !ok {
		t.Fatalf("delete bob: ok=%v err=%v", ok, err)
	}

	// Second run against the reopened catalog: the old cursor no longer matches, so the
	// server replays a reset+snapshot (alice, carol). The sweep must tombstone bob.
	c2 := clientFor(t, cat2)
	fp2 := &fakeProducer{}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- NewRunner("jobs-kafka", cfg, c2, fp2, nil).Run(ctx2) }()

	waitFor(t, "reset sweep tombstones bob", func() bool {
		l := fp2.latestByKey()
		return l["1.0"].Value != nil && l["3.0"].Value != nil && isTombstone(l["2.0"])
	})
	// State no longer tracks bob.
	waitFor(t, "state drops bob", func() bool {
		blob, ok, _ := c2.GetExporterState(ctx2, "jobs-kafka")
		if !ok {
			return false
		}
		st, _ := decodeState(blob)
		_, hasBob := st.KeyVersions["2.0"]
		return !hasBob && len(st.KeyVersions) == 2
	})
	cancel2()
	<-done2
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
