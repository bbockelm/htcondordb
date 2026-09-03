package scheddsync

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// decodedUndef counts JobStatus-undefined rows via the decoded full scan (KeysWhere), which is
// accurate regardless of whether the columnar count fast path is eligible on a small fresh table.
func decodedUndef(t *testing.T, d *db.DB) int {
	t.Helper()
	seq, err := d.KeysWhere("JobStatus is undefined")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

// nonContiguousLog builds a job_queue.log where every job's JobStatus is set in a transaction
// SEPARATE from its NewClassAd (all submissions first, then all JobStatus updates) -- mirroring the
// real schedd log, where a job's JobStatus is typically logged in a run non-contiguous with its
// NewClassAd. n jobs, spanning more than one reconcile batch.
func nonContiguousLog(n, jobStatus int) string {
	var b strings.Builder
	b.WriteString("107 1 CreationTimestamp 1000\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "105 \n101 %d.0 Job Machine\n103 %d.0 ClusterId %d\n103 %d.0 ProcId 0\n106 \n", i, i, i, i)
	}
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "105 \n103 %d.0 JobStatus %d\n106 \n", i, jobStatus)
	}
	return b.String()
}

// TestReconcileNoTransientUndefinedNonContiguousJobStatus is the regression for the reconcile
// partial-visibility bug. The schedd logs a job's JobStatus in a run separate from its NewClassAd,
// so a reconcile that blind-replaced each key with only its first contiguous run committed the job
// WITHOUT JobStatus until its later run was reached -- a large transient "JobStatus is undefined"
// spike on every reconcile (which fires on each ~hourly log compaction). The fix merges each run
// onto the existing row, so a key never loses JobStatus mid-reconcile.
//
// It syncs a non-contiguous log (JobStatus=1), points the log at JobStatus=2, forces a reconcile,
// and samples the committed state at every batch boundary via reconcileBatchCommittedHook. No batch
// may ever expose a JobStatus-undefined job. The value change (1->2) ensures the reconcile actually
// writes (so the hook fires and the assertion is not vacuous).
func TestReconcileNoTransientUndefinedNonContiguousJobStatus(t *testing.T) {
	d := persistentDB(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	const n = 5000 // > reconcileBatch (4096), so a batch commits between submissions and JobStatus runs
	ctx := context.Background()

	writeFile(t, logPath, nonContiguousLog(n, 1))
	s := NewJobSync(d, JobSyncConfig{Filename: logPath})
	for i := 0; i < 100000; i++ {
		if err := s.Poll(ctx); err != nil {
			t.Fatal(err)
		}
		if s.Status().CaughtUp {
			break
		}
	}
	if got := decodedUndef(t, d); got != 0 {
		t.Fatalf("after sync: %d jobs undefined, want 0", got)
	}

	// Point the log at the new state and force a full reconcile of it.
	writeFile(t, logPath, nonContiguousLog(n, 2))

	worst := 0
	reconcileBatchCommittedHook = func() {
		if got := decodedUndef(t, d); got > worst {
			worst = got
		}
	}
	defer func() { reconcileBatchCommittedHook = nil }()

	s.Resync()
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if worst != 0 {
		t.Errorf("reconcile transiently committed %d JobStatus-undefined jobs at a batch boundary (want 0)", worst)
	}
	if got := decodedUndef(t, d); got != 0 {
		t.Errorf("after reconcile: %d jobs undefined, want 0", got)
	}
}
