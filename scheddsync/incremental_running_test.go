package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestIncrementalRunningTransition exercises the incremental (applyEntry) tail path for a job that
// starts running: initial submit is synced, then the schedd APPENDS the running transition
// (JobStatus 1->2 + RemoteHost + MachineAttr...). The proc row must gain the running attributes
// while keeping its submission + chained cluster attributes.
func TestIncrementalRunningTransition(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, `103 0.0 NextClusterNum 2
101 01.-1 Job Machine
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 JobStatus 1
103 01.-1 ClusterId 1
103 01.-1 Owner "alice"
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
	if ad, ok := target.LookupClassAd("1.0"); !ok {
		t.Fatal("proc 1.0 missing after initial poll")
	} else if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 1 {
		t.Fatalf("after submit, 1.0 JobStatus = %d, want 1", v)
	}

	// The schedd appends the running transition.
	appendFile(t, logPath, `105
103 1.0 JobStatus 2
103 1.0 RemoteHost "slot1@node.example.com"
103 1.0 MachineAttrGLIDEIN_Site0 "SiteA"
106
`)
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("incremental poll: %v", err)
	}

	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("proc 1.0 missing after running transition")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 2 {
		t.Errorf("1.0 JobStatus = %d (ok=%v), want 2", v, ok)
	}
	if v, _ := ad.EvaluateAttrString("RemoteHost"); v != "slot1@node.example.com" {
		t.Errorf("1.0 RemoteHost = %q, want slot1@node.example.com", v)
	}
	if v, _ := ad.EvaluateAttrString("MachineAttrGLIDEIN_Site0"); v != "SiteA" {
		t.Errorf("1.0 MachineAttrGLIDEIN_Site0 = %q, want SiteA", v)
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Errorf("1.0 Owner = %q, want alice (chained, preserved across the update)", v)
	}
}
