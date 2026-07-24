package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestJobsTableMigrationSweepsStaleKeys models upgrading a jobs table that predates
// record-type routing: it holds cluster/user/jobset/header ads alongside real jobs. On the
// next resume the migration must sweep the non-job rows while keeping the proc ads.
func TestJobsTableMigrationSweepsStaleKeys(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	// First run syncs the proc ad 1.0 and checkpoints the resume position.
	if err := restartJobSync(target, logPath, store).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Simulate the pre-routing pollution: stale non-job rows written directly into jobs.
	stale := []string{"01.-1", "0.1", "1.-100", "0.0"}
	tx := target.Begin()
	for _, k := range stale {
		ad := classad.New()
		_ = ad.Set("Owner", "stale")
		tx.NewClassAd(k, ad)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := len(target.Keys()); got != 1+len(stale) {
		t.Fatalf("pre-migration key count = %d, want %d", got, 1+len(stale))
	}

	// Restart: the resume path runs the migration, sweeping the stale rows.
	if err := restartJobSync(target, logPath, store).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	keys := target.Keys()
	if len(keys) != 1 || keys[0] != "1.0" {
		t.Fatalf("after migration jobs keys = %v, want [1.0]", keys)
	}
	// The surviving proc still carries its attributes.
	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("proc 1.0 missing after migration")
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Errorf("1.0 Owner = %q, want alice", v)
	}
}
