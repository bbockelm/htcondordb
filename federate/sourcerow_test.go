package federate

import (
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// TestSourceRowVolatileFieldsCoarse: federation_sources is rewritten when a source's state changes,
// not on every state pass. LastContact, LastSeen and StalenessSeconds move every pass for a
// connected source, so writing on any change rewrote every row every few seconds and sent every
// watcher an event per source per pass. They are refreshed at most once per sourceRowRefresh; the
// summary and metrics carry the live values.
func TestSourceRowVolatileFieldsCoarse(t *testing.T) {
	a := newTestSpoke(t)
	a.putJobs(1)
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: func(string) cedarsync.Dial { return a.dial },
		Now: clock.now, FlushInterval: 20 * time.Millisecond, StateInterval: 5 * time.Millisecond})
	defer hub.stop(t)
	waitFor(t, "connected", func() bool { v, _ := sourceRowAttrBool(cat, "ap1", "JobsConnected"); return v })
	hub.statePasses(t, 2)

	wc := countWrites(t, mustTable(t, cat, TableSources))
	for i := 0; i < 24; i++ { // two minutes of 5 s state passes
		clock.advance(5 * time.Second)
		hub.statePasses(t, 2)
	}
	upserts, _ := wc.settle(t)
	if upserts > 3 {
		t.Errorf("federation_sources rewritten %d times in two minutes of an unchanged source, want <= 3", upserts)
	}
	if upserts == 0 {
		t.Error("volatile fields never refreshed")
	}
	row, _ := sourceRow(cat, "ap1")
	if v, _ := row.EvaluateAttrInt("LastContact"); clock.now().Unix()-v > int64(sourceRowRefresh/time.Second) {
		t.Errorf("LastContact %d older than the refresh interval at %d", v, clock.now().Unix())
	}
}
