package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestJobSyncResyncHeals: Resync() makes the next Poll rebuild the jobs mirror from the current
// log, healing a row an older sync had corrupted -- without truncating.
func TestJobSyncResyncHeals(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, `105
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 Owner "alice"
103 1.0 JobStatus 2
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

	// Corrupt the mirror: overwrite 1.0 with a stripped ad missing JobStatus (as the old
	// non-contiguity bug would have left a running job).
	tx := target.Begin()
	stripped, _ := classad.ParseOld(`MyType = "Job"; Key = "1.0"; Owner = "alice"`)
	tx.NewClassAd("1.0", stripped)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if ad, _ := target.LookupClassAd("1.0"); ad != nil {
		if _, ok := ad.EvaluateAttrInt("JobStatus"); ok {
			t.Fatal("setup: JobStatus should be absent after corruption")
		}
	}

	// Resync heals it from the current log.
	s.Resync()
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("resync poll: %v", err)
	}
	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("1.0 missing after resync")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 2 {
		t.Errorf("after resync 1.0 JobStatus = %d (ok=%v), want 2 (healed from log)", v, ok)
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Errorf("after resync 1.0 Owner = %q, want alice", v)
	}
}
