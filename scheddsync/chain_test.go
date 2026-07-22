package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// realSubmitLog is a verbatim job_queue.log fragment from a real HTCondor submit (one vanilla
// job) where the proc ad "1.0" carries its own attributes (this HTCondor writes ClusterId
// directly on the proc). A mirror must preserve the proc's own attributes.
const realSubmitLog = `107 1 CreationTimestamp 1784733861
101 0.0 Job Machine
105
103 0.0 UidDomain "localhost"
106
103 0.0 NextClusterNum 2
101 01.-1 Job Machine
101 1.-2 ClusterPvt (empty)
101 1.0 Job Machine
103 1.0 GlobalJobId "test_schedd@localhost#1.0#1784733866"
103 1.0 ClusterId 1
103 1.0 ProcId 0
103 1.0 JobStatus 1
103 1.0 Cmd "/bin/sleep"
103 01.-1 TotalSubmitProcs 1
103 01.-1 Owner "bbockelm"
`

// clusterOwnedLog models the other HTCondor behavior (seen with the packaged 25.x collector in
// CI): cluster-wide attributes -- ClusterId, Owner -- live ONLY on the cluster ad "01.-1", set
// AFTER the proc ad "1.0" is created, and the proc chains to them. A mirror must materialize
// them onto the proc row regardless of order.
const clusterOwnedLog = `103 0.0 NextClusterNum 2
101 01.-1 Job Machine
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 JobStatus 1
103 01.-1 ClusterId 1
103 01.-1 Owner "alice"
`

func TestJobSyncRealSubmitProcAttrs(t *testing.T) {
	assertVia(t, realSubmitLog, false, procExpect{cluster: 1, status: 1, proc: 0, cmd: "/bin/sleep"})
}

func TestReconcileRealSubmitProcAttrs(t *testing.T) {
	assertVia(t, realSubmitLog, true, procExpect{cluster: 1, status: 1, proc: 0, cmd: "/bin/sleep"})
}

func TestJobSyncClusterOwnedChains(t *testing.T) {
	assertVia(t, clusterOwnedLog, false, procExpect{cluster: 1, status: 1, proc: 0, owner: "alice"})
}

func TestReconcileClusterOwnedChains(t *testing.T) {
	assertVia(t, clusterOwnedLog, true, procExpect{cluster: 1, status: 1, proc: 0, owner: "alice"})
}

type procExpect struct {
	cluster, status, proc int64
	cmd, owner            string
}

// assertVia replays log through either the incremental (applyEntry) path via Poll or the
// reconcile-reload path, then checks the materialized proc ad "1.0".
func assertVia(t *testing.T, log string, reconcile bool, want procExpect) {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, log)
	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	if reconcile {
		err = s.reconcileReload(context.Background())
	} else {
		err = s.Poll(context.Background())
	}
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("proc ad 1.0 missing")
	}
	if v, ok := ad.EvaluateAttrInt("ClusterId"); !ok || v != want.cluster {
		t.Errorf("1.0 ClusterId = %d (ok=%v), want %d", v, ok, want.cluster)
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != want.status {
		t.Errorf("1.0 JobStatus = %d (ok=%v), want %d", v, ok, want.status)
	}
	if v, ok := ad.EvaluateAttrInt("ProcId"); !ok || v != want.proc {
		t.Errorf("1.0 ProcId = %d (ok=%v), want %d", v, ok, want.proc)
	}
	if want.cmd != "" {
		if v, _ := ad.EvaluateAttrString("Cmd"); v != want.cmd {
			t.Errorf("1.0 Cmd = %q, want %q", v, want.cmd)
		}
	}
	if want.owner != "" {
		if v, _ := ad.EvaluateAttrString("Owner"); v != want.owner {
			t.Errorf("1.0 Owner = %q, want %q (chained from cluster ad)", v, want.owner)
		}
	}
}
