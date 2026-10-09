package scheddsync

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestHistorySyncResyncOnTruncate verifies the self-heal path: when the history archive is
// emptied out from under a running syncer (an admin `.truncate history` / from-scratch
// re-sync), the next Poll detects the emptied archive and re-reads the history file from its
// head, rebuilding every record -- even though no new bytes were appended to the file. It
// also confirms the syncer does not spuriously re-read once the archive is non-empty again.
func TestHistorySyncResyncOnTruncate(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	store := &FileStore{Path: filepath.Join(dir, "history.pos")}

	// Seed three completed jobs and sync them.
	writeFile(t, histPath, histRecord(1, 0, 4)+histRecord(2, 0, 4)+histRecord(3, 0, 4))
	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 3 {
		t.Fatalf("after initial sync Count = %d, want 3", arch.Count())
	}

	// Empty the archive out from under the running syncer -- exactly what an admin
	// `.truncate history` does at runtime.
	arch.Truncate()
	if arch.Count() != 0 {
		t.Fatalf("Truncate left %d records", arch.Count())
	}

	// The next poll must rebuild the archive from the file head despite no new file bytes.
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 3 {
		t.Fatalf("after truncate-resync Count = %d, want 3 (history not re-read from head)", arch.Count())
	}

	// A subsequent poll with a non-empty archive and no new file bytes must NOT re-read or
	// duplicate -- the self-heal fires only on the non-empty -> empty transition.
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 3 {
		t.Fatalf("steady-state poll changed Count to %d, want 3 (spurious re-read)", arch.Count())
	}
}

// TestHistorySyncTruncateOnRunGoroutine covers the operator truncate routed through the owner:
// Truncate wipes the archive and rewinds the running syncer in one step on its own goroutine, so
// the file is re-read (a record the file does not hold is gone), and it fails cleanly -- having
// truncated nothing -- once the syncer has stopped.
func TestHistorySyncTruncateOnRunGoroutine(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	writeFile(t, histPath, histRecord(1, 0, 4)+histRecord(2, 0, 4)+histRecord(3, 0, 4))
	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: &FileStore{Path: filepath.Join(dir, "history.pos")},
		PollInterval: 10 * time.Millisecond, IdleMaxInterval: time.Hour, DisableNotify: true})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = s.Run(ctx) }()
	waitArchiveCount(t, arch, 3)

	stray := classad.New()
	stray.InsertAttr("ClusterId", 99)
	stray.InsertAttr("ProcId", 0)
	if err := arch.Append(stray); err != nil {
		t.Fatal(err)
	}
	// Let the idle backoff grow so the truncate, not a timer, wakes the syncer.
	time.Sleep(200 * time.Millisecond)

	tctx, tcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tcancel()
	if err := s.Truncate(tctx); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	waitArchiveCount(t, arch, 3)
	seq, err := arch.Query("ClusterId == 99")
	if err != nil {
		t.Fatal(err)
	}
	for range seq {
		t.Fatal("record not in the history file survived the truncate")
	}

	cancel()
	<-runDone
	if err := s.Truncate(context.Background()); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Errorf("Truncate after Run exited: err = %v, want 'not running'", err)
	}
	if arch.Count() != 3 {
		t.Errorf("Truncate on a stopped syncer changed the archive: Count = %d", arch.Count())
	}
}

func waitArchiveCount(t *testing.T, a *db.ArchiveTable, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for a.Count() != want {
		if time.Now().After(deadline) {
			t.Fatalf("archive Count = %d, want %d", a.Count(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
