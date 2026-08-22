package scheddsync

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// countingStore wraps a FileStore and counts Save calls, to observe checkpoint throttling.
type countingStore struct {
	inner FileStore
	saves int
}

func (c *countingStore) Save(blob []byte) error      { c.saves++; return c.inner.Save(blob) }
func (c *countingStore) Load() ([]byte, bool, error) { return c.inner.Load() }

// TestJobSyncCheckpointThrottle verifies that with SaveInterval set, the steady append path
// saves the position at most once per interval (the first save always goes through), while a
// rotation/compaction reload still forces a save regardless of the throttle.
func TestJobSyncCheckpointThrottle(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &countingStore{inner: FileStore{Path: filepath.Join(dir, "jobs.pos")}}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	// A huge interval means every save after the first is throttled away.
	s := NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store, SaveInterval: time.Hour})

	// First append+poll: first save always goes through.
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.saves != 1 {
		t.Fatalf("after first poll saves=%d, want 1", store.saves)
	}

	// Two more appends+polls within the interval: applied, but the position is not re-saved.
	appendFile(t, logPath, "105\n101 2.0 Job Machine\n103 2.0 Owner \"bob\"\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	appendFile(t, logPath, "105\n101 3.0 Job Machine\n103 3.0 Owner \"carol\"\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.saves != 1 {
		t.Fatalf("throttled polls saves=%d, want 1 (position write suppressed within interval)", store.saves)
	}
	// The data itself is applied regardless of the position-save throttle.
	if target.Len() != 3 {
		t.Fatalf("target Len=%d, want 3 (all jobs applied)", target.Len())
	}

	// A compaction/rotation reload must force a checkpoint even within the interval.
	renameOver(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n"+
		"105\n101 3.0 Job Machine\n103 3.0 Owner \"carol\"\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.saves < 2 {
		t.Fatalf("after rotation saves=%d, want >=2 (reload must force a checkpoint)", store.saves)
	}
}

// TestJobSyncCheckpointEveryBatchByDefault confirms the default (SaveInterval 0) still saves
// after every batch -- the pre-throttle behavior.
func TestJobSyncCheckpointEveryBatchByDefault(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &countingStore{inner: FileStore{Path: filepath.Join(dir, "jobs.pos")}}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	s := NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store}) // SaveInterval 0
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	appendFile(t, logPath, "105\n101 2.0 Job Machine\n103 2.0 Owner \"bob\"\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.saves != 2 {
		t.Fatalf("default saves=%d, want 2 (one per batch)", store.saves)
	}
}
