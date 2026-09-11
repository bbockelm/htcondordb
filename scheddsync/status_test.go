package scheddsync

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// TestJobSyncTrackBehind drives the behind-episode state machine: a small lag is not an episode; a
// large lag starts one but does not log until behindLogThreshold; it tracks the peak; and recovery
// clears it.
func TestJobSyncTrackBehind(t *testing.T) {
	s := &JobSync{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	base := time.Unix(1_000_000, 0)

	s.trackBehind(base, 1024) // below behindLagThreshold -> not behind
	if !s.behindSince.IsZero() {
		t.Fatal("a small (churn) lag should not start an episode")
	}

	s.trackBehind(base.Add(time.Second), 5<<20) // large lag -> episode starts
	if s.behindSince.IsZero() {
		t.Fatal("a large lag should start an episode")
	}
	if s.behindLogged {
		t.Fatal("must not log before behindLogThreshold elapses")
	}

	s.trackBehind(base.Add(2*time.Second), 9<<20) // peak grows
	if s.behindPeak != 9<<20 {
		t.Errorf("peak = %d, want %d", s.behindPeak, 9<<20)
	}

	// behindLogThreshold after the episode START (base+1s): fires the one-shot WARN.
	s.trackBehind(base.Add(time.Second+behindLogThreshold), 6<<20)
	if !s.behindLogged {
		t.Fatal("should log once the source has been behind for behindLogThreshold")
	}

	s.trackBehind(base.Add(time.Minute), 0) // recovered
	if !s.behindSince.IsZero() || s.behindLogged || s.behindPeak != 0 {
		t.Fatal("recovery should clear the episode")
	}
}

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
