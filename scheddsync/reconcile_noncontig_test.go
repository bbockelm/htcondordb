package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestReconcileNonContiguousOps reproduces the running-attributes-vanish bug: in a live (appended-
// to) job_queue.log, a job's attributes are NOT contiguous -- its submission block is early and its
// runtime updates (JobStatus 1->2, RemoteHost, ...) are appended later, far from the submit block.
// reconcileReload must reconstruct the FULL ad by merging every op for a key across the whole log,
// not just the key's last contiguous run.
func TestReconcileNonContiguousOps(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	// 1.0 is submitted (Owner/Cmd/JobStatus=1), then 2.0 is submitted, then 1.0 starts running --
	// its JobStatus=2 + RemoteHost are appended in a SEPARATE, non-contiguous transaction.
	writeFile(t, logPath, `105
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 Owner "alice"
103 1.0 Cmd "/bin/sleep"
103 1.0 JobStatus 1
106
105
101 2.0 Job Machine
103 2.0 ProcId 0
103 2.0 Owner "bob"
103 2.0 JobStatus 1
106
105
103 1.0 JobStatus 2
103 1.0 RemoteHost "slot1@node"
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
		t.Fatal("job 1.0 missing after reconcile")
	}
	// The running update must have landed...
	if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 2 {
		t.Errorf("1.0 JobStatus = %d, want 2 (running update)", v)
	}
	if v, _ := ad.EvaluateAttrString("RemoteHost"); v != "slot1@node" {
		t.Errorf("1.0 RemoteHost = %q, want slot1@node", v)
	}
	// ...WITHOUT wiping the submission attributes (the bug: reconstructed ad = last contiguous run only).
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Errorf("1.0 Owner = %q, want alice -- submission attributes were wiped by the non-contiguous runtime update", v)
	}
	if v, _ := ad.EvaluateAttrString("Cmd"); v != "/bin/sleep" {
		t.Errorf("1.0 Cmd = %q, want /bin/sleep -- submission attributes were wiped", v)
	}
	if v, ok := ad.EvaluateAttrInt("ProcId"); !ok || v != 0 {
		t.Errorf("1.0 ProcId = %d (ok=%v), want 0 -- submission attributes were wiped", v, ok)
	}
}
