package scheddsync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// archiveClusterCount returns how many archived records match ClusterId == cluster.
func archiveClusterCount(t *testing.T, s *HistorySync, cluster int) int {
	t.Helper()
	seq, err := s.archive.Query(fmt.Sprintf("ClusterId == %d", cluster))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

// TestHistorySyncDedupOnReplay: a restart that re-reads already-appended records (its
// checkpoint lagged the archive at crash time) must NOT duplicate them -- the archive is the
// dedup oracle.
func TestHistorySyncDedupOnReplay(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	store := &FileStore{Path: filepath.Join(dir, "history.pos")}
	writeFile(t, histPath, histRecord(1, 0, 4))

	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 1 {
		t.Fatalf("after first sync count = %d, want 1", arch.Count())
	}

	// Simulate a crash where the checkpoint lagged: rewind the saved offset to 0 so the
	// restart re-reads job 1.0.
	id, _ := statIdentity(histPath)
	blob, _ := json.Marshal(historyPos{File: id, Offset: 0})
	if err := store.Save(blob); err != nil {
		t.Fatal(err)
	}

	// Restart + a newly completed job 2.0.
	appendFile(t, histPath, histRecord(2, 0, 4))
	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 2 {
		t.Fatalf("after restart count = %d, want 2 (1.0 deduped, 2.0 appended)", arch.Count())
	}
}

// TestHistorySyncRotationRecovery: if the history file rotated while we were down, the tail
// of the rotated-away file (jobs that completed but weren't yet synced) must be recovered
// from the rotated file, and the fresh file appended -- nothing missed, nothing duplicated.
func TestHistorySyncRotationRecovery(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	store := &FileStore{Path: filepath.Join(dir, "history.pos")}

	// First run syncs job 1.0 only.
	writeFile(t, histPath, histRecord(1, 0, 4))
	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 1 {
		t.Fatalf("after first sync count = %d, want 1", arch.Count())
	}

	// While down: job 2.0 completes (appended to the same file), THEN the schedd rotates --
	// history -> history.1 -- and starts a fresh history with job 3.0.
	appendFile(t, histPath, histRecord(2, 0, 4))
	if err := os.Rename(histPath, histPath+".1"); err != nil {
		t.Fatal(err)
	}
	rotFI, err := os.Stat(histPath + ".1")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, histPath, histRecord(3, 0, 4))
	// Ensure the fresh file sorts as strictly newer than the rotated one.
	_ = os.Chtimes(histPath, rotFI.ModTime().Add(time.Second), rotFI.ModTime().Add(time.Second))

	// Restart: recover 2.0 from history.1, then 3.0 from the fresh history.
	s2 := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store})
	if err := s2.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 3 {
		t.Fatalf("after rotation recovery count = %d, want 3 (1.0, 2.0 from history.1, 3.0)", arch.Count())
	}
	for _, c := range []int{1, 2, 3} {
		if n := archiveClusterCount(t, s2, c); n != 1 {
			t.Errorf("cluster %d appears %d times, want exactly 1", c, n)
		}
	}
}
