package main

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

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
	m.reconcileArchiveIndexes(context.Background(), hist, s)

	// The backfill is asynchronous so it cannot stall daemon startup.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if c, _ := hist.IndexedAttrs(); slices.Contains(c, "Owner") {
			break
		}
		if time.Now().After(deadline) {
			c, _ := hist.IndexedAttrs()
			t.Fatalf("Owner was not backfilled; categorical = %v", c)
		}
		time.Sleep(10 * time.Millisecond)
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
	m.reconcileArchiveIndexes(context.Background(), hist, s)

	// Nothing to add, so this is synchronous -- but give any stray goroutine a chance.
	time.Sleep(100 * time.Millisecond)
	c, _ := hist.IndexedAttrs()
	if !slices.Contains(c, "AccountingGroup") {
		t.Errorf("categorical = %v, want AccountingGroup retained (reconciliation must not drop)", c)
	}
	if !slices.Contains(c, "Owner") {
		t.Errorf("categorical = %v, want Owner retained", c)
	}
}
