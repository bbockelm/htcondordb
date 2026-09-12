//go:build unix

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/dbstatus"
)

// TestSyncStatusCommandIntegration is the end-to-end check on DBSyncStatus: a real daemon process
// with a real schedd-sync tailer, dialed over an authenticated CEDAR connection by the client a
// consumer would actually use, and the reply handed to the parser a consumer would actually use.
//
// The point of the command is that a client with no collector can still learn whether the mirror
// is current, so the assertion is not "the daemon answered" but "the answer carries live sync
// health for the log this daemon was actually pointed at". A reply that is well-formed and empty
// would satisfy the weaker check and tell a consumer nothing.
//
// That the health then parses into a routing decision is proved where both sides are already
// dependencies (pelican-ap-manager, via golang-htcondor's dbmirror); asserting it here would mean
// this producer taking a dependency on a consumer-side module for a test.
func TestSyncStatusCommandIntegration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	bin := htcondordbBinary(t)
	dir := t.TempDir()

	scheddDir := filepath.Join(dir, "schedd")
	if err := os.MkdirAll(scheddDir, 0o755); err != nil {
		t.Fatal(err)
	}
	jobLog := filepath.Join(scheddDir, "job_queue.log")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfgPath := writeNodeConfig(t, dir, fsIdentity(t), "HTCONDORDB_SYNC_SCHEDD = true\nJOB_QUEUE_LOG = "+jobLog+"\n")
	addr := startNode(t, bin, dir, cfgPath)
	cfg := clientConfig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The tailer needs a moment to read the log and mark itself caught up, so poll rather than
	// asserting on the first reply.
	var lastErr error
	var lastAd string
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		ad, err := dbstatus.Query(ctx, cfg, addr)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if mt, _ := ad.EvaluateAttrString("MyType"); mt != "HTCondorDB" {
			t.Fatalf("MyType = %q, want HTCondorDB", mt)
		}
		// Identity comes from the generic base ad, not from dbad: a reply missing it means the
		// response was assembled from only half of what an advertisement carries, and a consumer
		// keying on Name would have nothing to key on.
		if name, _ := ad.EvaluateAttrString("Name"); name == "" {
			t.Fatalf("status reply carries no Name; got ad: %s", ad.String())
		}
		if myAddr, _ := ad.EvaluateAttrString("MyAddress"); myAddr == "" {
			t.Fatalf("status reply carries no MyAddress; got ad: %s", ad.String())
		}
		// The source path proves the health describes the log this daemon was pointed at, and
		// the caught-up flag is the fact a consumer gates its reads on.
		if src, _ := ad.EvaluateAttrString("JobQueueSource"); src != jobLog {
			t.Fatalf("JobQueueSource = %q, want %q", src, jobLog)
		}
		caughtUp, present := ad.EvaluateAttrBool("JobQueueCaughtUp")
		if !present {
			t.Fatalf("status reply carries no JobQueueCaughtUp; got ad: %s", ad.String())
		}
		if lastSync, ok := ad.EvaluateAttrInt("JobQueueLastSyncTime"); caughtUp && ok && lastSync > 0 {
			return // live sync health, reached over the command port with no collector involved
		}
		lastErr = nil
		lastAd = ad.String()
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("status query never succeeded: %v", lastErr)
	}
	t.Fatalf("mirror never reported caught up for the job queue; last reply: %s", lastAd)
}
