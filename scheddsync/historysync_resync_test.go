package scheddsync

import (
	"context"
	"path/filepath"
	"testing"
)

// TestHistorySyncResyncOnRetentionLoss: if the history file we last synced has rotated out of
// retention entirely (its inode is no longer anywhere in the chain), recovery must still
// proceed against what remains AND fire a structured resync event quantifying the gap by the
// oldest CompletionDate still on disk -- so an operator learns some completed jobs were lost.
func TestHistorySyncResyncOnRetentionLoss(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	store := &FileStore{Path: filepath.Join(dir, "history.pos")}

	// First run syncs job 1.0 and records the position (its inode + offset).
	writeFile(t, histPath, histRecord(1, 0, 4))
	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 1 {
		t.Fatalf("after first sync count = %d, want 1", arch.Count())
	}

	// While down: the file we synced is replaced by a brand-new inode (rotated away AND its
	// rotated copy already pruned by retention -- nothing named history.* survives), carrying
	// only a later job 3.0. Job 2.0 completed in the lost window and is gone forever.
	renameOver(t, histPath, histRecord(3, 0, 4))

	var events []ResyncEvent
	s := NewHistorySync(arch, HistorySyncConfig{
		Filename: histPath,
		Store:    store,
		OnResync: func(ev ResyncEvent) { events = append(events, ev) },
	})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 3.0 is recovered from what remains on disk.
	if archiveClusterCount(t, s, 3) != 1 {
		t.Errorf("job 3.0 not recovered from surviving file")
	}
	// Exactly one resync event, quantified by 3.0's CompletionDate (1700000000+cluster).
	if len(events) != 1 {
		t.Fatalf("resync events = %d, want 1: %+v", len(events), events)
	}
	if events[0].Reason != "history-file-rotated-out-of-retention" {
		t.Errorf("resync reason = %q", events[0].Reason)
	}
	if want := int64(1700000003); events[0].OldestAvailableCompletion != want {
		t.Errorf("OldestAvailableCompletion = %d, want %d", events[0].OldestAvailableCompletion, want)
	}
}

// TestHistorySyncNoResyncOnCleanResume: the resync event must NOT fire on a normal resume
// where the saved file is still present -- that is not a data-loss gap.
func TestHistorySyncNoResyncOnCleanResume(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	store := &FileStore{Path: filepath.Join(dir, "history.pos")}

	writeFile(t, histPath, histRecord(1, 0, 4))
	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Same file grows with a new completed job (no rotation).
	appendFile(t, histPath, histRecord(2, 0, 4))

	fired := 0
	s := NewHistorySync(arch, HistorySyncConfig{
		Filename: histPath,
		Store:    store,
		OnResync: func(ResyncEvent) { fired++ },
	})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fired != 0 {
		t.Errorf("resync fired %d times on a clean resume; want 0", fired)
	}
	if arch.Count() != 2 {
		t.Fatalf("count = %d, want 2", arch.Count())
	}
}
