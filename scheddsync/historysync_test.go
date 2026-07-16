package scheddsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// histRecord renders one completed-job record terminated by a banner line.
func histRecord(cluster, proc, status int) string {
	return fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nProcId = %d\nJobStatus = %d\nCompletionDate = %d\n*** Offset = 0 ClusterId = %d ProcId = %d\n",
		cluster, cluster, proc, status, 1700000000+cluster, cluster, proc)
}

func newArchive(t *testing.T) (*db.ArchiveTable, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	return a, func() { cat.Close() }
}

// TestHistorySync covers ingest of complete records, a partial (incomplete) trailing
// record that completes on a later poll, and rotation that drains the old file (including
// records written just before the rename) before switching.
func TestHistorySync(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	writeFile(t, histPath, histRecord(1, 0, 4)+histRecord(2, 0, 4))

	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath})
	ctx := context.Background()
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 2 {
		t.Fatalf("after initial poll Count = %d, want 2", arch.Count())
	}

	// A partial trailing record (no banner yet) is NOT appended until it completes.
	appendFile(t, histPath, "Owner = \"user3\"\nClusterId = 3\nProcId = 0\n")
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 2 {
		t.Fatalf("partial record was appended prematurely: Count = %d", arch.Count())
	}
	appendFile(t, histPath, "JobStatus = 4\n*** Offset = 0 ClusterId = 3 ProcId = 0\n")
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 3 {
		t.Fatalf("completed record not appended: Count = %d, want 3", arch.Count())
	}

	// Rotation: a record is written to the current file, THEN it is rotated aside and a
	// fresh history starts. The pre-rotation record must not be lost.
	appendFile(t, histPath, histRecord(4, 0, 4))
	if err := os.Rename(histPath, filepath.Join(dir, "history.old")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, histPath, histRecord(5, 0, 4))
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 5 {
		t.Fatalf("after rotation Count = %d, want 5 (record 4 drained from old + record 5 from new)", arch.Count())
	}

	// All five clusters are queryable.
	for _, cl := range []int{1, 2, 3, 4, 5} {
		seq, err := arch.Query(fmt.Sprintf("ClusterId == %d", cl))
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for range seq {
			n++
		}
		if n != 1 {
			t.Errorf("cluster %d: found %d records, want 1", cl, n)
		}
	}
}
