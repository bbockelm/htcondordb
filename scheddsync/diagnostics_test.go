package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestSetAttrAbsentKeyCounter checks the observe-only orphan diagnostic on the steady-state tail
// (applyEntry) path: a SetAttribute on a key with no prior NewClassAd fabricates an identity-less
// orphan AND increments the counter, while an update to a present job does not. Behavior is
// unchanged (the counter measures, it does not skip). No Store is used, so Poll replays the tail
// through applyEntry rather than the reconciler (which restore() takes on a fresh position store).
func TestSetAttrAbsentKeyCounter(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// A bare SetAttribute for 5.0 with NO preceding NewClassAd: an absent key.
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n103 5.0 LastRejMatchReason \"no match found\"\n")
	s := NewJobSync(d, JobSyncConfig{Filename: logPath})
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Status().SetAttrAbsentKey; got != 1 {
		t.Fatalf("SetAttrAbsentKey after absent-key update = %d, want 1", got)
	}
	if got := s.Status().Reconciles; got != 0 {
		t.Errorf("Reconciles on the steady-state tail path = %d, want 0", got)
	}
	// The orphan is still created (observe-only did not change behavior).
	if _, ok := d.LookupClassAd("5.0"); !ok {
		t.Error("observe-only counter must not change behavior: 5.0 should still be (orphan-)created")
	}

	// A normal submit + update to a PRESENT job must NOT increment the counter.
	appendFile(t, logPath, "105 \n101 6.0 Job Machine\n103 6.0 JobStatus 1\n106 \n103 6.0 LastRejMatchTime 42\n")
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Status().SetAttrAbsentKey; got != 1 {
		t.Errorf("SetAttrAbsentKey after present-key update = %d, want still 1", got)
	}
}
