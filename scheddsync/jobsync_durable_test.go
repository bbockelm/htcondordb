package scheddsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// renameOver replaces path with new content under a fresh inode (a temp file + rename),
// simulating the schedd compacting job_queue.log (writes job_queue.log.tmp, renames over).
func renameOver(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".new"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// restartJobSync makes a fresh JobSync over the same target + position store, modeling a
// process restart (in-memory prober/offset are gone; only the durable store survives).
func restartJobSync(target *db.DB, logPath string, store PositionStore) *JobSync {
	return NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store})
}

// TestJobSyncDurableResume: after a restart, the syncer resumes from the persisted offset and
// applies the changes that happened while it was down (here a destroy + a new job), rather
// than starting cold.
func TestJobSyncDurableResume(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	// First run: sync job 1.0 and checkpoint.
	if err := restartJobSync(target, logPath, store).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target.Len() != 1 {
		t.Fatalf("after first sync Len = %d, want 1", target.Len())
	}
	// A durable position was written past the start.
	blob, ok, _ := store.Load()
	if !ok {
		t.Fatal("no position checkpointed")
	}
	pos, _ := decodeJobPosition(blob)
	if pos.Offset <= 0 {
		t.Fatalf("checkpointed offset = %d, want > 0", pos.Offset)
	}

	// While "down": 1.0 completes and is destroyed, 2.0 is added.
	appendFile(t, logPath, "105\n102 1.0\n101 2.0 Job Machine\n103 2.0 Owner \"bob\"\n106\n")

	// Restart: a fresh syncer resumes and applies exactly those changes.
	if err := restartJobSync(target, logPath, store).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, present := target.LookupClassAd("1.0"); present {
		t.Error("1.0 should have been destroyed on resume")
	}
	ad, present := target.LookupClassAd("2.0")
	if !present {
		t.Fatal("2.0 (added while down) missing after resume")
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "bob" {
		t.Fatalf("2.0 Owner = %q, want bob", v)
	}
}

// TestJobSyncRebuildAfterCompaction: if job_queue.log was compacted while the syncer was down
// (rewritten smaller, dropping the destroy records for jobs that ended), a restart must
// detect the rotation and rebuild -- not leave the completed jobs as ghosts in the table.
func TestJobSyncRebuildAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n"+
		"105\n101 2.0 Job Machine\n103 2.0 Owner \"bob\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := restartJobSync(target, logPath, store).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target.Len() != 2 {
		t.Fatalf("after first sync Len = %d, want 2", target.Len())
	}

	// While down, the schedd compacts: 2.0 completed and is gone; the rewritten log (fresh
	// inode, smaller) carries only the still-live 1.0 -- with no destroy record for 2.0.
	renameOver(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n")

	// Restart: the fresh syncer must notice the compaction and rebuild, dropping the 2.0 ghost.
	if err := restartJobSync(target, logPath, store).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ghost := target.LookupClassAd("2.0"); ghost {
		t.Error("2.0 is a ghost: compacted away while down but still in the table")
	}
	if target.Len() != 1 {
		t.Fatalf("after rebuild Len = %d, want 1 (only the live 1.0)", target.Len())
	}
	if _, ok := target.LookupClassAd("1.0"); !ok {
		t.Error("1.0 should survive the rebuild")
	}
}
