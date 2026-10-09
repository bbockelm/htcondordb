package federate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// TestScheddNameCaseInsensitive: HTCondor daemon names are case-insensitive, so two spellings of one
// schedd name are one source -- one federation_sources row, one cursor directory -- and retiring
// either spelling retires both spellings' rows.
func TestScheddNameCaseInsensitive(t *testing.T) {
	disc := &fakeDiscovery{}
	disc.set(members("ap1", "AP1"))
	dir := t.TempDir()
	cat := openCatalog(t, dir+"/db")
	t.Cleanup(func() { _ = cat.Close() })
	seedHubRows(t, cat, "ap1", 2)
	seedHubRows(t, cat, "AP1", 3)
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable, CursorDir: dir + "/cursors"})
	defer hub.stop(t)
	waitFor(t, "known", func() bool { return sourceState(cat, "ap1") != "" })
	time.Sleep(50 * time.Millisecond)
	src, _ := cat.Table(TableSources)
	if n := countWhere(t, src, "true"); n != 1 {
		t.Fatalf("federation_sources rows = %d, want 1 for two spellings of one schedd", n)
	}
	if hub.hub.cursorDir("AP1") != hub.hub.cursorDir("ap1") {
		t.Error("two spellings get two cursor directories")
	}
	if err := hub.hub.Retire(context.Background(), "Ap1"); err != nil {
		t.Fatal(err)
	}
	j, _ := cat.Table(TableJobs)
	if n := countWhere(t, j, "true"); n != 0 {
		t.Errorf("jobs rows after retiring Ap1 = %d, want 0 (both spellings)", n)
	}
	if n := countWhere(t, src, "true"); n != 0 {
		t.Errorf("federation_sources rows after retire = %d", n)
	}
	if _, err := os.Stat(hub.hub.cursorDir("ap1")); !os.IsNotExist(err) {
		t.Errorf("cursor dir after retire: %v", err)
	}
}

// TestCaseVariantRowsMergedOnRestore: a hub from before case folding persisted one
// federation_sources row per spelling (keyed by spelling); a restart merges them into one source
// and one row under the folded key, and drops cursors kept under a capitalised spelling.
func TestCaseVariantRowsMergedOnRestore(t *testing.T) {
	dir := t.TempDir()
	cat := openCatalog(t, dir+"/db")
	t.Cleanup(func() { _ = cat.Close() })
	src := mustTable(t, cat, TableSources)
	now := time.Now().Unix()
	for key, text := range map[string]string{
		"ap1": `ScheddName = "ap1"; LastSeen = ` + itoa(int(now-100)),
		"AP1": `ScheddName = "AP1"; LastSeen = ` + itoa(int(now)),
	} {
		tx := src.Begin()
		tx.NewClassAd(key, parseAd(t, text))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	h := &Hub{cfg: Config{CursorDir: dir + "/cursors"}}
	legacy := h.legacyCursorDir("AP1")
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatal(err)
	}
	disc := &fakeDiscovery{}
	disc.set(members("AP1"))
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable, CursorDir: dir + "/cursors"})
	defer hub.stop(t)
	waitFor(t, "one row", func() bool { return countWhere(t, src, "true") == 1 })
	row, ok := src.LookupClassAd("ap1")
	if !ok {
		t.Fatal("no row under the folded key")
	}
	if v, _ := row.EvaluateAttrInt("LastSeen"); v < now {
		t.Errorf("merged LastSeen = %d, want the later (%d)", v, now)
	}
	waitFor(t, "legacy cursor dir dropped", func() bool { _, err := os.Stat(legacy); return os.IsNotExist(err) })
}

// TestDiscoveryPairsCaseInsensitively: a spoke's MirroredScheddName pairs with the schedd whatever
// its capitalisation.
func TestDiscoveryPairsCaseInsensitively(t *testing.T) {
	fc := &fakeCollector{
		schedds: []*classad.ClassAd{scheddAd(t, "ap1.example.org", "<10.0.0.1:9618>")},
		dbs:     []*classad.ClassAd{spokeAd(t, "db1", "<10.0.0.1:9620>", "AP1.Example.org", true, true)},
	}
	snap, err := (&Discovery{Collector: fc, ScheddConstraint: `true`,
		Resolve: resolveMap(map[string][]string{"ap1.example.org": {"10.0.0.1"}})}).Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Spokes["ap1.example.org"].SpokeName != "db1" {
		t.Errorf("spokes = %+v, rejected = %+v", snap.Spokes, snap.Rejected)
	}
}
