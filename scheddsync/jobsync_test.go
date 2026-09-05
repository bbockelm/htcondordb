package scheddsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

// TestJobSync covers replay of committed transactions, incremental tailing (an update +
// a destroy), and a log rotation that fully re-syncs.
func TestJobSync(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, `105
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 Owner "alice"
103 1.0 JobStatus 1
106
105
101 2.0 Job Machine
103 2.0 ProcId 0
103 2.0 Owner "bob"
103 2.0 JobStatus 1
106
`)

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	ctx := context.Background()

	if err := s.Poll(ctx); err != nil {
		t.Fatalf("initial poll: %v", err)
	}
	if target.Len() != 2 {
		t.Fatalf("after initial sync Len = %d, want 2", target.Len())
	}
	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("job 1.0 missing")
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Fatalf("1.0 Owner = %q, want alice", v)
	}
	if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 1 {
		t.Fatalf("1.0 JobStatus = %d, want 1", v)
	}

	// Incremental: 1.0 goes to status 4 (completed), 2.0 is destroyed.
	appendFile(t, logPath, `105
103 1.0 JobStatus 4
102 2.0
106
`)
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("incremental poll: %v", err)
	}
	if target.Len() != 1 {
		t.Fatalf("after incremental Len = %d, want 1", target.Len())
	}
	ad, _ = target.LookupClassAd("1.0")
	if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 4 {
		t.Fatalf("1.0 JobStatus = %d, want 4", v)
	}
	if _, ok := target.LookupClassAd("2.0"); ok {
		t.Error("destroyed job 2.0 still present")
	}

	// Rotation: the schedd rewrites job_queue.log with only a fresh job 3.0.
	writeFile(t, logPath, `105
101 3.0 Job Machine
103 3.0 ProcId 0
103 3.0 Owner "carol"
106
`)
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("post-rotation poll: %v", err)
	}
	if target.Len() != 1 {
		t.Fatalf("after rotation Len = %d, want 1 (only the rewritten job)", target.Len())
	}
	if _, ok := target.LookupClassAd("1.0"); ok {
		t.Error("pre-rotation job 1.0 survived the re-sync")
	}
	ad, ok = target.LookupClassAd("3.0")
	if !ok {
		t.Fatal("post-rotation job 3.0 missing")
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "carol" {
		t.Fatalf("3.0 Owner = %q, want carol", v)
	}
}

// TestJobSyncCommitConflictRecovers proves a commit conflict on the incremental path is not
// silently dropped: the pass is rewound to the durable resume offset and re-applied on a fresh
// snapshot, so no data is lost. It uses an explicit transaction that spans a poll boundary (the
// case where a pass-start rewind would be wrong -- it sits after the BeginTransaction), so it also
// covers the rewind-to-durable-offset correctness.
func TestJobSyncCommitConflictRecovers(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	// A transaction that spans the poll boundary: BeginTransaction + ops, but no EndTransaction yet.
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n103 1.0 JobStatus 1\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	ctx := context.Background()

	// Poll A: open the explicit transaction and apply its ops, but do not commit (no 106 yet).
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("poll A: %v", err)
	}
	if target.Len() != 0 {
		t.Fatalf("uncommitted transaction should not be visible; Len = %d", target.Len())
	}

	// A second writer supersedes key 1.0 AFTER the tailer's transaction took its snapshot, so the
	// tailer's commit will lose the optimistic race (the double-launched-tailer scenario).
	interloper := classad.New()
	interloper.InsertAttrString(KeyAttr, "1.0")
	interloper.InsertAttrString("Owner", "interloper")
	itx := target.Begin()
	itx.NewClassAd("1.0", interloper)
	if err := itx.Commit(); err != nil {
		t.Fatalf("interloper commit: %v", err)
	}

	// Poll B: the EndTransaction arrives; committing the tailer's transaction now conflicts. The
	// conflict must be handled internally (rewound), not returned as a poll error.
	appendFile(t, logPath, "106\n")
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("poll B should handle the conflict, not return it: %v", err)
	}
	if s.mConflicts.Load() == 0 {
		t.Fatal("expected a commit conflict to be recorded")
	}

	// Poll C: the rewind re-reads the whole (now-complete) transaction on a fresh snapshot and
	// commits it -- so the tailer's values win over the interloper and nothing was dropped.
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("poll C (re-apply): %v", err)
	}
	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("job 1.0 missing after conflict recovery")
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Fatalf("1.0 Owner = %q, want alice (tailer re-applied over the interloper)", v)
	}
	if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 1 {
		t.Fatalf("1.0 JobStatus = %d, want 1", v)
	}
}

// TestJobSyncCommitConflictEscalatesToReconcile verifies the retry is bounded: after
// maxConflictRetries consecutive conflicts rewinding to the same offset (a persistent second
// writer), it escalates to a full reconcileReload rather than spinning.
func TestJobSyncCommitConflictEscalatesToReconcile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "105\n101 7.0 Job Machine\n103 7.0 Owner \"grace\"\n103 7.0 ProcId 0\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	ctx := context.Background()
	// Prime the parser so reconcileReload has a real file to replay.
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if s.mReconciles.Load() != 0 {
		t.Fatalf("priming poll should not reconcile, got mReconciles=%d", s.mReconciles.Load())
	}

	conflict := &db.ConflictError{Keys: []string{"7.0"}}
	// The first maxConflictRetries-1 conflicts just rewind and retry, no reconcile.
	for i := 1; i < maxConflictRetries; i++ {
		if err := s.handleCommitConflict(ctx, conflict); err != nil {
			t.Fatalf("conflict %d returned error: %v", i, err)
		}
		if got := s.mReconciles.Load(); got != 0 {
			t.Fatalf("reconcile fired early after %d conflicts (mReconciles=%d)", i, got)
		}
	}
	// The maxConflictRetries-th consecutive conflict escalates to a full reload.
	if err := s.handleCommitConflict(ctx, conflict); err != nil {
		t.Fatalf("escalating conflict returned error: %v", err)
	}
	if got := s.mReconciles.Load(); got != 1 {
		t.Fatalf("expected exactly one reconcileReload on escalation, got mReconciles=%d", got)
	}
	if s.conflictRuns != 0 {
		t.Fatalf("conflict streak should reset after escalation, got %d", s.conflictRuns)
	}
	if got := s.mConflicts.Load(); got != int64(maxConflictRetries) {
		t.Fatalf("mConflicts = %d, want %d", got, maxConflictRetries)
	}
}

// TestJobSyncNonTransactional verifies ops written outside an explicit transaction (an
// implicit batch) are still applied.
func TestJobSyncNonTransactional(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "101 5.0 Job Machine\n103 5.0 Owner \"dave\"\n103 5.0 ProcId 0\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target.Len() != 1 {
		t.Fatalf("Len = %d, want 1", target.Len())
	}
	ad, _ := target.LookupClassAd("5.0")
	if v, _ := ad.EvaluateAttrString("Owner"); v != "dave" {
		t.Fatalf("5.0 Owner = %q, want dave", v)
	}
}
