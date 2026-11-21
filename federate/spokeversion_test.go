package federate

import (
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/cedarsync"
	"github.com/bbockelm/htcondordb/syncstatus"
)

// TestSpokeBuildReachesTheSourceRow: the build a spoke reports on its heartbeat is published on
// its federation_sources row, so "which spokes are running the build with the fix in them?" is a
// query rather than a shell session on the hub's pod reading cursor files.
//
// It matters because several replication fixes live in the SPOKE -- a catch-up cursor is minted by
// whichever daemon serves the watch, so upgrading the hub alone changes nothing -- and at the hub
// an un-upgraded spoke is indistinguishable from a slow one: connected, applying records, never
// reaching Synced.
func TestSpokeBuildReachesTheSourceRow(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	ss := mustTable(t, cat, TableSyncStatus)
	spoke := newTestSpoke(t)
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: func(string) cedarsync.Dial { return spoke.dial },
		Now: clock.now, FreshThreshold: 60 * time.Second})
	defer hub.stop(t)

	waitFor(t, "ap1's syncstatus stream caught up", func() bool {
		s, _ := sourceRowAttrBool(cat, "ap1", "SyncStatusSynced")
		return s
	})

	// Before any heartbeat the attributes must be absent, not empty: absent is what an
	// un-upgraded spoke looks like, and "" would be a second spelling of it.
	if row, ok := sourceRow(cat, "ap1"); ok && row != nil {
		for _, attr := range []string{syncstatus.AttrSpokeVersion, syncstatus.AttrSpokeClassAdVersion} {
			if _, ok := row.EvaluateAttrString(attr); ok {
				t.Errorf("%s published before any heartbeat", attr)
			}
		}
	}

	sink, err := newTableSink(ss, TableSyncStatus, "ap1", &replicate.MemCursorStore{}, NewMetrics(), clock.now, nil)
	if err != nil {
		t.Fatal(err)
	}
	apply(t, sink, reset(), synced("c"))
	apply(t, sink, upsert("status", parseAd(t,
		`HeartbeatSeq = 1; HeartbeatTime = 1; SpokeLagSeconds = 2; HeartbeatIntervalSeconds = 5;`+
			` SpokeVersion = "v0.28.0"; SpokeClassAdVersion = "v0.43.0"`)))
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
	clock.advance(sourceRowRefresh)

	waitFor(t, "the spoke's build reaches the source row", func() bool {
		row, ok := sourceRow(cat, "ap1")
		if !ok || row == nil {
			return false
		}
		v, ok1 := row.EvaluateAttrString(syncstatus.AttrSpokeVersion)
		c, ok2 := row.EvaluateAttrString(syncstatus.AttrSpokeClassAdVersion)
		return ok1 && ok2 && v == "v0.28.0" && c == "v0.43.0"
	})
}
