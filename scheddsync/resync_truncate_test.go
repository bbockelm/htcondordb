package scheddsync

import (
	"context"
	"path/filepath"
	"testing"
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

	// Simulate the external truncate. The pinned classad has no in-place Archive.Truncate
	// yet, so we swap in a fresh, empty archive: the syncer's detection keys only off
	// Count()==0, which is exactly what a truncate produces. (Once classad ships
	// Archive.Truncate, this can call arch.Truncate() on the same object.)
	empty, cleanup2 := newArchive(t)
	defer cleanup2()
	s.archive = empty

	// The next poll must rebuild the archive from the file head despite no new file bytes.
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if empty.Count() != 3 {
		t.Fatalf("after truncate-resync Count = %d, want 3 (history not re-read from head)", empty.Count())
	}

	// A subsequent poll with a non-empty archive and no new file bytes must NOT re-read or
	// duplicate -- the self-heal fires only on the non-empty -> empty transition.
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if empty.Count() != 3 {
		t.Fatalf("steady-state poll changed Count to %d, want 3 (spurious re-read)", empty.Count())
	}
}
