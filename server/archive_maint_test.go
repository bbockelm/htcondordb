package server

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestMaintainArchivesOnce verifies the periodic archive maintenance pass rotates and
// reindexes every archive, leaving the data intact and queryable. Reindex correctness
// (building sidecars for sealed segments) is covered upstream; here we assert the pass
// runs over a multi-segment history without disturbing its contents.
func TestMaintainArchivesOnce(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		SegmentSize: 1 << 12, // small -> several sealed segments to reindex
		ValueAttrs:  []string{"ClusterId"},
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 400
	for i := 0; i < n; i++ {
		ad, _ := classad.Parse(fmt.Sprintf(`[ ClusterId = %d; Owner = "u%d"; JobStatus = 4 ]`, i, i%10))
		if err := arch.Append(ad); err != nil {
			t.Fatal(err)
		}
	}

	s := &Service{cat: cat, log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	// Two passes: idempotent, no error, data preserved.
	before := arch.Count()
	s.maintainArchivesOnce(0)
	s.maintainArchivesOnce(0)
	if got := arch.Count(); got != before {
		t.Errorf("Count changed across maintenance: before %d, after %d", before, got)
	}

	// A query on the reindexed archive still returns the right rows.
	q, err := arch.Query("ClusterId >= 200")
	if err != nil {
		t.Fatal(err)
	}
	cnt := 0
	for range q {
		cnt++
	}
	if cnt != 200 {
		t.Errorf("ClusterId>=200 after maintenance = %d, want 200", cnt)
	}
}
