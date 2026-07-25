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
