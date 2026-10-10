package federate

import (
	"fmt"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
)

// histAt is a history record of cluster on schedd that entered history at t.
func histAt(t *testing.T, schedd string, cluster int, at int64) *classad.ClassAd {
	t.Helper()
	return parseAd(t, fmt.Sprintf(`GlobalJobId = "%s#%d.0#1700000000"; ClusterId = %d; ProcId = 0; Owner = "alice"; EnteredHistoryTime = %d`,
		schedd, cluster, cluster, at))
}

func cappedHistory(t *testing.T, cat *db.Catalog, maxBytes int64) *db.ArchiveTable {
	t.Helper()
	var wg sync.WaitGroup
	ht, err := ensureTables(cat, []string{TableHistory}, ArchiveOptions{SegmentSize: 4096, MaxBytes: map[string]int64{TableHistory: maxBytes}}, discard, &wg)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	return ht.archives[TableHistory]
}

// TestReplaySkipsWhatTheCapDropped: a hub whose history is at its size cap has dropped its oldest
// records. A spoke restart replays the spoke's whole retained history; re-appending the records
// the cap already dropped would make them the newest data in the archive and push out other APs'
// recent history at the next rotation -- every spoke restart again. Records older than anything
// the hub still holds for that AP are skipped; newer missing ones are appended.
func TestReplaySkipsWhatTheCapDropped(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := cappedHistory(t, cat, 4*4096)
	m := NewMetrics()
	s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, m)
	const n = 300
	replay := func(count int, cur string) {
		s.BeginSession()
		changes := []replicate.Change{reset()}
		for i := 1; i <= count; i++ {
			changes = append(changes, upsert("r", histAt(t, "ap1", i, 1_700_000_000+int64(i))))
		}
		apply(t, s, append(changes, synced(cur))...)
		s.EndSession()
	}
	replay(n, "c1")
	if _, err := hist.Rotate(2_000_000_000); err != nil {
		t.Fatal(err)
	}
	held := countArchive(t, hist, `true`)
	if held >= n {
		t.Fatalf("precondition: the cap dropped nothing (%d of %d held)", held, n)
	}
	other := newHistSink(t, hist, "ap2", &replicate.MemCursorStore{}, m)
	apply(t, other, reset(), upsert("o", histAt(t, "ap2", 1, 1_600_000_000)), synced("o1"))

	replay(n+1, "c2") // the spoke restarts with one new completion
	if got := countArchive(t, hist, `ScheddName == "ap1"`); got != held+1 {
		t.Errorf("ap1 records after the replay = %d, want %d (what the cap kept, plus the new one)", got, held+1)
	}
	if got := countArchive(t, hist, fmt.Sprintf(`ClusterId == %d`, n+1)); got != 1 {
		t.Errorf("the new completion appended %d times, want 1", got)
	}
	if v := val(m.BelowRetention.WithLabelValues(TableHistory)); v != float64(n-held) {
		t.Errorf("below_retention_total = %v, want %d", v, n-held)
	}
}

// TestReplayBelowFloorKeptWithoutCap: with no cap, or a cap not yet reached, nothing was dropped,
// so a replayed record older than anything the hub holds for the AP (a spoke that imported older
// history) is appended.
func TestReplayBelowFloorKeptWithoutCap(t *testing.T) {
	for _, capped := range []bool{false, true} {
		t.Run(fmt.Sprintf("capped=%v", capped), func(t *testing.T) {
			cat := openCatalog(t, t.TempDir())
			t.Cleanup(func() { _ = cat.Close() })
			hist := hubArchive(t, cat, TableHistory)
			if capped {
				hist = cappedHistory(t, cat, 1<<30)
			}
			s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, NewMetrics())
			s.BeginSession()
			apply(t, s, reset(), upsert("r", histAt(t, "ap1", 10, 1_700_000_010)), synced("c1"))
			s.BeginSession()
			apply(t, s, reset(), upsert("r", histAt(t, "ap1", 1, 1_700_000_001)), upsert("r", histAt(t, "ap1", 10, 1_700_000_010)), synced("c2"))
			if got := countArchive(t, hist, `true`); got != 2 {
				t.Errorf("hub history = %d, want 2", got)
			}
		})
	}
}
