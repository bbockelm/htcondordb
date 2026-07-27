package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestReconcileRunningJobNonContiguous models the real production case: a running job under the
// cluster-owned-attributes HTCondor behavior. The cluster ad "01.-1" holds Owner/ClusterId, the
// proc "1.0" is submitted (JobStatus=1), and LATER -- after another job's ops, so non-contiguous --
// the schedd appends the running transition (JobStatus 1->2 + RemoteHost + MachineAttr...). The
// reconciled proc row must end up with the proc-native running attributes AND the chained Owner.
func TestReconcileRunningJobNonContiguous(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, `103 0.0 NextClusterNum 3
101 01.-1 Job Machine
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 JobStatus 1
103 01.-1 ClusterId 1
103 01.-1 Owner "alice"
101 02.-1 Job Machine
101 2.0 Job Machine
103 2.0 ProcId 0
103 2.0 JobStatus 1
103 02.-1 ClusterId 2
103 02.-1 Owner "bob"
105
103 1.0 JobStatus 2
103 1.0 RemoteHost "slot1@node.example.com"
103 1.0 EnteredCurrentStatus 1700000500
103 1.0 MachineAttrGLIDEIN_Site0 "SiteA"
106
`)

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	if err := s.reconcileReload(context.Background()); err != nil {
		t.Fatalf("reconcileReload: %v", err)
	}

	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("proc ad 1.0 missing")
	}
	// Proc-native running attributes (the ones the operator reported missing).
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 2 {
		t.Errorf("1.0 JobStatus = %d (ok=%v), want 2", v, ok)
	}
	if v, _ := ad.EvaluateAttrString("RemoteHost"); v != "slot1@node.example.com" {
		t.Errorf("1.0 RemoteHost = %q, want slot1@node.example.com", v)
	}
	if v, _ := ad.EvaluateAttrString("MachineAttrGLIDEIN_Site0"); v != "SiteA" {
		t.Errorf("1.0 MachineAttrGLIDEIN_Site0 = %q, want SiteA", v)
	}
	if v, ok := ad.EvaluateAttrInt("EnteredCurrentStatus"); !ok || v != 1700000500 {
		t.Errorf("1.0 EnteredCurrentStatus = %d (ok=%v), want 1700000500", v, ok)
	}
	// Submission + cluster-chained attributes must still be present.
	if v, ok := ad.EvaluateAttrInt("ProcId"); !ok || v != 0 {
		t.Errorf("1.0 ProcId = %d (ok=%v), want 0", v, ok)
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Errorf("1.0 Owner = %q, want alice (chained from cluster)", v)
	}
	if v, ok := ad.EvaluateAttrInt("ClusterId"); !ok || v != 1 {
		t.Errorf("1.0 ClusterId = %d (ok=%v), want 1 (chained from cluster)", v, ok)
	}
}
