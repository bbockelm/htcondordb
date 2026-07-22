package scheddsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestLastRecordAd: lastRecordAd returns the final record of a multi-record file.
func TestLastRecordAd(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "history")
	writeFile(t, p, histRecord(1, 0, 4)+histRecord(2, 0, 4)+histRecord(3, 0, 4))
	ad, ok := lastRecordAd(p)
	if !ok {
		t.Fatal("lastRecordAd returned ok=false")
	}
	if cid, _ := ad.EvaluateAttrInt("ClusterId"); cid != 3 {
		t.Errorf("last record ClusterId = %d, want 3", cid)
	}
}

// TestFileFullyArchived: a rotated file is "fully archived" iff its last record is present.
func TestFileFullyArchived(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")

	// Archive jobs 1.0 and 2.0.
	writeFile(t, histPath, histRecord(1, 0, 4)+histRecord(2, 0, 4))
	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath})

	// A rotated file whose records are all archived (last record 2.0 present).
	synced := filepath.Join(dir, "history.synced")
	writeFile(t, synced, histRecord(1, 0, 4)+histRecord(2, 0, 4))
	if !s.fileFullyArchived(synced) {
		t.Error("file with only archived records should be fully archived")
	}
	// A rotated file whose last record (3.0) is NOT archived.
	partial := filepath.Join(dir, "history.partial")
	writeFile(t, partial, histRecord(1, 0, 4)+histRecord(3, 0, 4))
	if s.fileFullyArchived(partial) {
		t.Error("file whose last record is unarchived must not be fully archived")
	}
}

// TestHistorySyncRecoversFromArchiveWithoutPosition: if the position store is lost but the
// archive persists, recovery must re-derive the frontier from the archive across the rotation
// chain -- back-filling a rotated file's unsynced tail (which the old head-only behavior
// missed) and skipping a fully-archived older file -- rather than only tailing the current
// file. The position store is thus a fast-path hint, not a hard dependency.
func TestHistorySyncRecoversFromArchiveWithoutPosition(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	// Position store in a separate directory (as in production: HTCONDORDB_DIR vs the spool).
	store := &FileStore{Path: filepath.Join(dir, "state", "history.pos")}

	// First run syncs job 1.0 and checkpoints.
	writeFile(t, histPath, histRecord(1, 0, 4))
	if err := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store}).Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A completed job 2.0 is appended but NOT yet synced, then the schedd rotates: the current
	// file (with 1.0 + the unsynced 2.0) becomes history.1, and a fresh history holds 3.0. An
	// older, fully-synced rotated file history.0 (just 1.0) also sits in the chain.
	writeFile(t, filepath.Join(dir, "history.0"), histRecord(1, 0, 4))
	appendFile(t, histPath, histRecord(2, 0, 4))
	if err := os.Rename(histPath, filepath.Join(dir, "history.1")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, histPath, histRecord(3, 0, 4))

	// The position store is lost (state dir cleared), but the archive still holds job 1.0.
	if err := os.RemoveAll(filepath.Dir(store.Path)); err != nil {
		t.Fatal(err)
	}

	// Restart with the same archive: recovery re-derives from the archive, back-filling 2.0
	// from history.1 (skipping the fully-archived history.0) and tailing 3.0 from the fresh
	// file.
	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 3 {
		t.Fatalf("after archive-driven recovery count = %d, want 3 (1.0, 2.0 back-filled, 3.0)", arch.Count())
	}
	for _, c := range []int{1, 2, 3} {
		if n := archiveClusterCount(t, s, c); n != 1 {
			t.Errorf("cluster %d appears %d times, want exactly 1", c, n)
		}
	}
}
