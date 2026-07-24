package scheddsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestJobSyncStatus: after a poll drains to EOF the status reports progress and caught-up, and
// Status() is safe to call concurrently with a running Poll (run under -race).
func TestJobSyncStatus(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"a\"\n106\n")
	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := s.Status()
	if st.Kind != "job_queue.log" {
		t.Errorf("Kind = %q", st.Kind)
	}
	if !st.CaughtUp {
		t.Error("should be caught up after draining to EOF")
	}
	if st.Offset <= 0 {
		t.Errorf("Offset = %d, want > 0", st.Offset)
	}
	if st.LastSync.IsZero() {
		t.Error("LastSync should be set after a progressing poll")
	}

	// Concurrent reader while polling: the race detector validates the atomic snapshot.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = s.Status()
			}
		}
	}()
	for i := 0; i < 200; i++ {
		if err := s.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	<-done
}

// TestHistorySyncStatusResync: a retention-loss recovery bumps the resync counters exposed in
// the status.
func TestHistorySyncStatusResync(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	store := &FileStore{Path: filepath.Join(dir, "state", "history.pos")}

	writeFile(t, histPath, histRecord(1, 0, 4))
	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Saved file rotates out of retention (fresh inode, older records gone).
	renameOver(t, histPath, histRecord(3, 0, 4))

	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := s.Status()
	if st.Kind != "history" {
		t.Errorf("Kind = %q", st.Kind)
	}
	if st.Resyncs != 1 {
		t.Errorf("Resyncs = %d, want 1", st.Resyncs)
	}
	if st.LastResync.IsZero() {
		t.Error("LastResync should be set after a gap")
	}
}
