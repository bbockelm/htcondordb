package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

const syncOn = "HTCONDORDB_SYNC_SCHEDD = true\nSPOOL = /var/spool/condor\nHTCONDORDB_HISTORY = /var/spool/condor/history\n"

// TestArchiveConfigDefaults pins the defaults the history archive is created with. Segment
// size is deliberately left to the library (small segments keep the unindexed tail short,
// and segment count is a merge problem, not a seal-size one), while the categorical index
// is not: an archive with no categorical index cannot answer a GROUP BY from its indexes at
// any segment size.
func TestArchiveConfigDefaults(t *testing.T) {
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	if s.archiveSegSize != 0 {
		t.Errorf("archiveSegSize = %d, want 0 (library default)", s.archiveSegSize)
	}
	if s.archiveCatAttrs != "Owner" {
		t.Errorf("archiveCatAttrs = %q, want %q", s.archiveCatAttrs, "Owner")
	}
	if s.archiveValAttrs != "ClusterId" {
		t.Errorf("archiveValAttrs = %q, want %q", s.archiveValAttrs, "ClusterId")
	}
}

// TestArchiveConfigOverrides checks each knob is actually read.
func TestArchiveConfigOverrides(t *testing.T) {
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+
		"HTCONDORDB_ARCHIVE_SEGMENT_SIZE = 268435456\n"+
		"HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS = Owner, AccountingGroup\n"+
		"HTCONDORDB_ARCHIVE_VALUE_ATTRS = ClusterId RequestMemory\n"))
	if s.archiveSegSize != 268435456 {
		t.Errorf("archiveSegSize = %d, want 268435456", s.archiveSegSize)
	}
	if got := splitAttrList(s.archiveCatAttrs); !slices.Equal(got, []string{"Owner", "AccountingGroup"}) {
		t.Errorf("categorical = %v, want [Owner AccountingGroup]", got)
	}
	if got := splitAttrList(s.archiveValAttrs); !slices.Equal(got, []string{"ClusterId", "RequestMemory"}) {
		t.Errorf("value = %v, want [ClusterId RequestMemory]", got)
	}
}

// TestArchiveAttrListCanonical is why the lists are canonicalized rather than stored raw:
// scheddSyncSettings is compared by equality to decide whether a reconfig must restart the
// tailers, so two spellings of the same attribute list must not look like a change.
func TestArchiveAttrListCanonical(t *testing.T) {
	spellings := []string{
		"HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS = Owner,AccountingGroup\n",
		"HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS = Owner, AccountingGroup\n",
		"HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS = Owner   AccountingGroup\n",
	}
	var first scheddSyncSettings
	for i, body := range spellings {
		s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+body))
		if i == 0 {
			first = s
			continue
		}
		if s != first {
			t.Errorf("spelling %d resolved differently (%q vs %q); a reformat would restart the tailers",
				i, s.archiveCatAttrs, first.archiveCatAttrs)
		}
	}
}

// TestReconcileArchiveIndexesAdds covers the reason this reconciliation exists at all:
// archiveconfig.json is authoritative on reopen, so an archive created before an attribute
// was configured keeps its old index set forever unless something adds it explicitly.
func TestReconcileArchiveIndexesAdds(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	// An archive as it exists in a deployment today: no categorical index.
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := hist.AppendOld("ClusterId = 1\nOwner = \"alice\""); err != nil {
			t.Fatal(err)
		}
	}

	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	// The backfill runs in its own goroutine (so it cannot stall daemon startup) but is
	// registered on wg, which the manager joins on stop; the test joins it the same way.
	var wg sync.WaitGroup
	m.reconcileArchiveIndexes(context.Background(), hist, s, &wg)
	wg.Wait()

	if c, _ := hist.IndexedAttrs(); !slices.Contains(c, "Owner") {
		t.Fatalf("Owner was not backfilled; categorical = %v", c)
	}
}

// TestReconcileArchiveIndexesNeverDrops checks the reconciliation is add-only: an index
// created by hand (via the repl) must survive a daemon restart whose configured attribute
// list does not mention it.
func TestReconcileArchiveIndexesNeverDrops(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		CategoricalAttrs: []string{"Owner", "AccountingGroup"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := hist.AppendOld("Owner = \"alice\"\nAccountingGroup = \"grp\""); err != nil {
		t.Fatal(err)
	}

	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// Config names only Owner; AccountingGroup was added out of band.
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	var wg sync.WaitGroup
	m.reconcileArchiveIndexes(context.Background(), hist, s, &wg)
	wg.Wait() // join the backfill (adding the ClusterId value index) before asserting

	c, _ := hist.IndexedAttrs()
	if !slices.Contains(c, "AccountingGroup") {
		t.Errorf("categorical = %v, want AccountingGroup retained (reconciliation must not drop)", c)
	}
	if !slices.Contains(c, "Owner") {
		t.Errorf("categorical = %v, want Owner retained", c)
	}
}

// TestReconcileArchiveIndexesSurvivesRestart is the property the reconciliation depends on
// and could not have before classad v0.23.1: AddIndex now folds its result into
// archiveconfig.json, which is authoritative on reopen. Without that the daemon would find
// the attribute missing again on every start and re-run the full backfill -- minutes on a
// young archive, hours on a mature one.
func TestReconcileArchiveIndexesSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := hist.AppendOld(fmt.Sprintf("ClusterId = %d\nOwner = \"alice\"", i)); err != nil {
			t.Fatal(err)
		}
	}
	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	// Join the backfill via wg (as the manager does on stop): wg.Wait returns only after
	// AddIndex has BOTH reindexed and persisted archiveconfig.json, so the reopen below sees the
	// persisted index. Polling IndexedAttrs instead raced the persist step (IndexedAttrs flips
	// after the reindex, before saveIndexConfig) and flaked.
	var wg sync.WaitGroup
	m.reconcileArchiveIndexes(context.Background(), hist, s, &wg)
	wg.Wait()
	if c, _ := hist.IndexedAttrs(); !slices.Contains(c, "Owner") {
		t.Fatalf("Owner was not backfilled; categorical = %v", c)
	}
	cat.Close()

	// Restart: the index must still be there, and reconciliation must find nothing to do.
	cat2, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cat2.Close()
	h2, ok := cat2.ArchiveTable("history")
	if !ok {
		t.Fatal("archive not recovered")
	}
	if c, _ := h2.IndexedAttrs(); !slices.Contains(c, "Owner") {
		t.Fatalf("after restart categorical = %v, want Owner retained", c)
	}
	if h2.AddIndex([]string{"Owner"}, nil) {
		t.Error("re-adding the persisted index reported a change; the daemon would re-backfill every restart")
	}
}

// TestArchiveRowGroupBytesRead checks the knob is read, and that leaving it out reports 0 rather
// than a number -- 0 is what tells applyArchiveRowGroupBytes to leave a tuned archive alone.
func TestArchiveRowGroupBytesRead(t *testing.T) {
	if s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn)); s.archiveRowGroupBytes != 0 {
		t.Errorf("unset archiveRowGroupBytes = %d, want 0", s.archiveRowGroupBytes)
	}
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_ARCHIVE_ROW_GROUP_BYTES = 131072\n"))
	if s.archiveRowGroupBytes != 131072 {
		t.Errorf("archiveRowGroupBytes = %d, want 131072", s.archiveRowGroupBytes)
	}
}

// TestApplyArchiveRowGroupBytes covers the half of this knob that config alone cannot do.
//
// An ArchiveConfig is only honoured when the archive is CREATED -- archiveconfig.json is
// authoritative on reopen -- so a budget that travelled only that way would apply to a brand new
// archive and never to the one an operator wants to tune. This applies it to an archive that already
// exists, and requires the records written before the change to still read back: a new budget governs
// segments sealed from now on, and blocks already written keep the layout they recorded.
func TestApplyArchiveRowGroupBytes(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	// An archive as it exists in a deployment today: created without the knob.
	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := hist.AppendOld(fmt.Sprintf("ClusterId = %d\nOwner = \"alice\"", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := hist.RowGroupBytes(); got != 0 {
		t.Fatalf("archive starts at RowGroupBytes=%d, want 0", got)
	}

	m := &scheddSyncManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s := resolveScheddSyncSettings(mkSyncCfg(t, syncOn+"HTCONDORDB_ARCHIVE_ROW_GROUP_BYTES = 131072\n"))
	m.applyArchiveRowGroupBytes(hist, "history", s)
	if got := hist.RowGroupBytes(); got != 131072 {
		t.Errorf("after apply RowGroupBytes = %d, want 131072", got)
	}
	if n := hist.Count(); n != 50 {
		t.Errorf("%d records after the change, want 50", n)
	}

	// Unset must not reset a deliberately tuned archive.
	m.applyArchiveRowGroupBytes(hist, "history", resolveScheddSyncSettings(mkSyncCfg(t, syncOn)))
	if got := hist.RowGroupBytes(); got != 131072 {
		t.Errorf("removing the setting reset the archive to %d; unset means leave it alone", got)
	}
}
