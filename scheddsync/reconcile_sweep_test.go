package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestReconcileSweepsMisroutedRows is the regression for the undeletable "Owner" rows: a global
// per-reconcile "seen" set let a key legitimately routed to one table protect a stale, misrouted
// row under the SAME key in another table. Here an owner key ("0.1") sits stale in the jobs table
// (as a pre-routing sync would have left it) while the log routes that owner to the users table.
// The jobs-table sweep must drop it (the log routed nothing under "0.1" to jobs), not keep it
// because "0.1" is live in users.
func TestReconcileSweepsMisroutedRows(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, `105
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 Owner "alice"
103 1.0 JobStatus 2
106
105
101 0.1 Owner Machine
103 0.1 Name "alice"
106
`)

	jobs, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer jobs.Close()
	users, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer users.Close()

	// Pre-populate the jobs table with a stale, misrouted owner row under the owner key -- the same
	// key the log now routes to the users table.
	stale, err := classad.ParseOld(`MyType = "Owner"; Name = "alice"`)
	if err != nil {
		t.Fatal(err)
	}
	tx := jobs.Begin()
	tx.NewClassAd("0.1", stale)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	s := NewJobSync(jobs, JobSyncConfig{Filename: logPath, Users: users})
	if err := s.reconcileReload(context.Background()); err != nil {
		t.Fatalf("reconcileReload: %v", err)
	}

	if _, ok := jobs.LookupClassAd("0.1"); ok {
		t.Error("stale misrouted owner row 0.1 survived in the jobs table (global-seen bug)")
	}
	if _, ok := jobs.LookupClassAd("1.0"); !ok {
		t.Error("real job 1.0 missing from the jobs table")
	}
	if _, ok := users.LookupClassAd("0.1"); !ok {
		t.Error("owner 0.1 missing from the users table")
	}
}
