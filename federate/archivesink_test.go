package federate

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
)

func hubArchive(t *testing.T, cat *db.Catalog, table string) *db.ArchiveTable {
	t.Helper()
	var wg sync.WaitGroup
	ht, err := ensureTables(cat, []string{table}, ArchiveOptions{}, discard, &wg)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	return ht.archives[table]
}

func histRecord(t *testing.T, schedd string, cluster int) *classad.ClassAd {
	t.Helper()
	return parseAd(t, fmt.Sprintf(`GlobalJobId = "%s#%d.0#1700000000"; ClusterId = %d; ProcId = 0; Owner = "alice"; CompletionDate = %d`,
		schedd, cluster, cluster, 1_700_000_000+cluster))
}

func newHistSink(t *testing.T, a *db.ArchiveTable, schedd string, store replicate.CursorStore, m *Metrics) *archiveSink {
	t.Helper()
	s, err := newArchiveSink(a, TableHistory, schedd, store, m, discard, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestArchiveReplayNoDuplicates: every spoke restart replays its whole retained history (a Reset).
// The stock archive sink re-appended all of it each time; the hub must append only what is new.
func TestArchiveReplayNoDuplicates(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	m := NewMetrics()
	s := newHistSink(t, hist, "ap1.example.org", &replicate.MemCursorStore{}, m)

	session := func(n int, cur string) {
		s.BeginSession()
		changes := []replicate.Change{reset()}
		for i := 1; i <= n; i++ {
			changes = append(changes, upsert(fmt.Sprintf("r%d", i), histRecord(t, "ap1.example.org", i)))
		}
		apply(t, s, append(changes, synced(cur))...)
		s.EndSession()
	}
	session(5, "c1")
	session(5, "c2") // spoke restart, nothing new
	session(6, "c3") // spoke restart, one new completion

	if n := countArchive(t, hist, `true`); n != 6 {
		t.Fatalf("hub history holds %d records after three replays of 5, 5, 6, want 6", n)
	}
	for i := 1; i <= 6; i++ {
		if n := countArchive(t, hist, fmt.Sprintf(`ClusterId == %d`, i)); n != 1 {
			t.Errorf("job %d.0 appears %d times, want 1", i, n)
		}
	}
	if v := val(m.DedupHits.WithLabelValues(TableHistory)); v != 10 {
		t.Errorf("dedup_hits = %v, want 10", v)
	}
	if n := countArchive(t, hist, `ScheddName == "ap1.example.org"`); n != 6 {
		t.Errorf("records stamped with the source = %d, want 6", n)
	}
}

// TestArchiveRedeliveryAfterCrash: the hub crashes after appending live records but before their
// cursor is committed. On restart the spoke re-delivers them from the committed cursor during
// catch-up; none may be appended twice.
func TestArchiveRedeliveryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	cursorPath := filepath.Join(t.TempDir(), "history.cursor")
	m := NewMetrics()

	cat := openCatalog(t, dir)
	hist := hubArchive(t, cat, TableHistory)
	s := newHistSink(t, hist, "ap1.example.org", fileCursorStore{path: cursorPath}, m)
	s.BeginSession()
	apply(t, s, reset(), upsert("r1", histRecord(t, "ap1.example.org", 1)), synced("after-1"))
	for i := 2; i <= 4; i++ {
		live := upsert(fmt.Sprintf("r%d", i), histRecord(t, "ap1.example.org", i))
		live.Cursor = []byte(fmt.Sprintf("after-%d", i))
		apply(t, s, live)
	}
	// Crash: no Flush, no EndSession. The appends are in the archive; the cursor is not.
	_ = cat.Close()

	cat = openCatalog(t, dir)
	t.Cleanup(func() { _ = cat.Close() })
	hist = hubArchive(t, cat, TableHistory)
	if n := countArchive(t, hist, `true`); n != 4 {
		t.Fatalf("precondition: %d records survived the crash, want 4", n)
	}
	s = newHistSink(t, hist, "ap1.example.org", fileCursorStore{path: cursorPath}, m)
	if got := string(s.Cursor()); got != "after-1" {
		t.Fatalf("resume cursor = %q, want after-1 (the last committed)", got)
	}
	// The spoke resumes after-1: catch-up re-delivers 2..4 (no cursors), then Synced, then live 5.
	s.BeginSession()
	for i := 2; i <= 4; i++ {
		apply(t, s, upsert(fmt.Sprintf("r%d", i), histRecord(t, "ap1.example.org", i)))
	}
	apply(t, s, synced("after-4"))
	live := upsert("r5", histRecord(t, "ap1.example.org", 5))
	live.Cursor = []byte("after-5")
	apply(t, s, live)
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	if n := countArchive(t, hist, `true`); n != 5 {
		t.Fatalf("hub history = %d records after redelivery, want 5", n)
	}
	for i := 1; i <= 5; i++ {
		if n := countArchive(t, hist, fmt.Sprintf(`ClusterId == %d`, i)); n != 1 {
			t.Errorf("job %d.0 appears %d times, want 1", i, n)
		}
	}
	if v := val(m.DedupHits.WithLabelValues(TableHistory)); v != 3 {
		t.Errorf("dedup_hits = %v, want 3", v)
	}
}

// TestArchiveLiveTailAppends: after Synced, records are new by construction and are appended
// without a probe -- so the live tail costs no index lookup per record.
func TestArchiveLiveTailAppends(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	m := NewMetrics()
	s := newHistSink(t, hist, "ap1.example.org", &replicate.MemCursorStore{}, m)
	apply(t, s, reset(), synced("c"))
	for i := 1; i <= 3; i++ {
		apply(t, s, upsert(fmt.Sprintf("r%d", i), histRecord(t, "ap1.example.org", i)))
	}
	// A record whose identity is already present still appends: the live tail does not probe.
	apply(t, s, upsert("r1-again", histRecord(t, "ap1.example.org", 1)))
	if n := countArchive(t, hist, `true`); n != 4 {
		t.Fatalf("live tail appended %d, want 4", n)
	}
	if v := val(m.DedupHits.WithLabelValues(TableHistory)); v != 0 {
		t.Errorf("live tail probed: dedup_hits = %v", v)
	}
}

// TestArchiveIdentity covers the identity edge cases: a record without GlobalJobId appends and is
// counted; epochs dedup on the (GlobalJobId, RunInstanceID, EpochAdType) triple; and dedup is
// scoped to the source, so one AP's record cannot suppress another's.
func TestArchiveIdentity(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	m := NewMetrics()

	hist := hubArchive(t, cat, TableHistory)
	s := newHistSink(t, hist, "ap1.example.org", &replicate.MemCursorStore{}, m)
	noID := parseAd(t, `ClusterId = 99; ProcId = 0`)
	apply(t, s, reset(), upsert("x", noID), upsert("x", parseAd(t, `ClusterId = 99; ProcId = 0`)), synced("c"))
	if n := countArchive(t, hist, `ClusterId == 99`); n != 2 {
		t.Errorf("identity-less records appended = %d, want 2", n)
	}
	if v := val(m.MissingIdentity.WithLabelValues(TableHistory)); v != 2 {
		t.Errorf("missing_identity = %v, want 2", v)
	}

	// ap2 replays a record carrying ap1's GlobalJobId: it is ap2's row, not a duplicate of ap1's.
	b := newHistSink(t, hist, "ap2.example.org", &replicate.MemCursorStore{}, m)
	apply(t, s, reset(), upsert("r1", histRecord(t, "ap1.example.org", 1)), synced("c2"))
	apply(t, b, reset(), upsert("r1", histRecord(t, "ap1.example.org", 1)), synced("d"))
	if n := countArchive(t, hist, `ClusterId == 1`); n != 2 {
		t.Errorf("cross-AP identical GlobalJobId rows = %d, want 2 (one per AP)", n)
	}

	ep := hubArchive(t, cat, TableEpochHistory)
	es, err := newArchiveSink(ep, TableEpochHistory, "ap1.example.org", &replicate.MemCursorStore{}, m, discard, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec := func(run int, typ string) *classad.ClassAd {
		return parseAd(t, fmt.Sprintf(`GlobalJobId = "ap1#5.0#1"; ClusterId = 5; ProcId = 0; RunInstanceID = %d; EpochAdType = "%s"`, run, typ))
	}
	batch := []replicate.Change{reset(), upsert("e", rec(0, "SPAWN")), upsert("e", rec(0, "EPOCH")), upsert("e", rec(1, "EPOCH")), synced("e1")}
	apply(t, es, batch...)
	es.BeginSession()
	batch = []replicate.Change{reset(), upsert("e", rec(0, "SPAWN")), upsert("e", rec(0, "EPOCH")), upsert("e", rec(1, "EPOCH")), synced("e2")}
	apply(t, es, batch...)
	if n := countArchive(t, ep, `true`); n != 3 {
		t.Errorf("epoch records = %d, want 3 (three distinct run/type pairs, replayed once)", n)
	}
}

// TestArchiveLargeReplayUsesIdentitySet: a replay longer than the probe budget switches to the
// in-memory identity set; it must deduplicate exactly as the per-record probe does, including
// records appended earlier in the same replay and epochs with no EpochAdType.
func TestArchiveLargeReplayUsesIdentitySet(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	m := NewMetrics()
	ep := hubArchive(t, cat, TableEpochHistory)
	s, err := newArchiveSink(ep, TableEpochHistory, "ap1.example.org", &replicate.MemCursorStore{}, m, discard, nil)
	if err != nil {
		t.Fatal(err)
	}
	n := probeBudget + 200
	rec := func(i int) *classad.ClassAd {
		typ := `EpochAdType = "EPOCH"; `
		if i%3 == 0 {
			typ = "" // no EpochAdType: identity reads it as ""
		}
		return parseAd(t, fmt.Sprintf(`%sGlobalJobId = "ap1#%d.0#1"; ClusterId = %d; ProcId = 0; RunInstanceID = 0`, typ, i, i))
	}
	replay := func(count int, cur string) {
		s.BeginSession()
		changes := []replicate.Change{reset()}
		for i := 1; i <= count; i++ {
			changes = append(changes, upsert("e", rec(i)))
		}
		apply(t, s, append(changes, synced(cur))...)
		s.EndSession()
	}
	replay(n, "c1")
	if got := countArchive(t, ep, `true`); got != n {
		t.Fatalf("first replay appended %d, want %d", got, n)
	}
	replay(n+5, "c2")
	if got := countArchive(t, ep, `true`); got != n+5 {
		t.Fatalf("after second replay: %d records, want %d", got, n+5)
	}
	if v := val(m.DedupHits.WithLabelValues(TableEpochHistory)); v != float64(n) {
		t.Errorf("dedup_hits = %v, want %d", v, n)
	}
}
