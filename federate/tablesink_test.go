package federate

import (
	"errors"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
)

func newJobsSink(t *testing.T, tbl *db.DB, schedd string, m *Metrics) *tableSink {
	t.Helper()
	s, err := newTableSink(tbl, TableJobs, schedd, &replicate.MemCursorStore{}, m, time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestTwoSourcesSameKey: two APs both have job 123.0. The hub must hold two rows, each naming its
// own AP, and a delete from one AP must leave the other's row alone. With raw source keys (the
// stock replicate sink) the second upsert overwrote the first and the delete destroyed both APs'
// job.
func TestTwoSourcesSameKey(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	m := NewMetrics()
	a := newJobsSink(t, jobs, "ap1.example.org", m)
	b := newJobsSink(t, jobs, "ap2.example.org", m)

	apply(t, a, reset(), upsert("123.0", jobAd(t, 123, 0, `Owner = "alice"`)), synced("a1"))
	apply(t, b, reset(), upsert("123.0", jobAd(t, 123, 0, `Owner = "bob"`)), synced("b1"))

	if n := jobs.Len(); n != 2 {
		t.Fatalf("hub holds %d rows for two APs' 123.0, want 2", n)
	}
	if n := countWhere(t, jobs, `ScheddName == "ap1.example.org" && ClusterId == 123 && ProcId == 0 && Owner == "alice"`); n != 1 {
		t.Errorf("ap1's 123.0 rows = %d, want 1", n)
	}
	if n := countWhere(t, jobs, `ScheddName == "ap2.example.org" && ClusterId == 123 && ProcId == 0 && Owner == "bob"`); n != 1 {
		t.Errorf("ap2's 123.0 rows = %d, want 1", n)
	}

	apply(t, a, del("123.0"))
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := countWhere(t, jobs, `ScheddName == "ap1.example.org"`); n != 0 {
		t.Errorf("ap1 rows after its delete = %d, want 0", n)
	}
	if n := countWhere(t, jobs, `ScheddName == "ap2.example.org" && ClusterId == 123 && ProcId == 0`); n != 1 {
		t.Errorf("ap2's 123.0 after ap1 deleted its own = %d rows, want 1", n)
	}
	if n := jobs.Len(); n != 1 {
		t.Errorf("hub rows = %d, want 1", n)
	}
}

// TestForgedScheddNameOverwritten: a row arriving with a ScheddName naming another AP is stored
// under the source's identity. A row must never be able to claim another AP.
func TestForgedScheddNameOverwritten(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	a := newJobsSink(t, jobs, "ap1.example.org", NewMetrics())
	apply(t, a, reset(), upsert("7.0", jobAd(t, 7, 0, `ScheddName = "ap2.example.org"`)), synced("c"))

	if n := countWhere(t, jobs, `ScheddName == "ap2.example.org"`); n != 0 {
		t.Fatalf("forged row kept its claim: %d rows name ap2", n)
	}
	if n := countWhere(t, jobs, `ScheddName == "ap1.example.org" && ClusterId == 7`); n != 1 {
		t.Fatalf("row not stamped with its source: %d rows", n)
	}
}

// TestResetReconcileNoPhantoms: rows the spoke deleted while the hub was away must not survive the
// Reset replay that follows, rows that changed must be updated, new rows added -- and nothing of
// another AP's touched. Writes are counted: only the real deltas are written.
func TestResetReconcileNoPhantoms(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	m := NewMetrics()
	a := newJobsSink(t, jobs, "ap1.example.org", m)
	b := newJobsSink(t, jobs, "ap2.example.org", m)

	apply(t, a, reset(),
		upsert("1.0", jobAd(t, 1, 0, "")), upsert("2.0", jobAd(t, 2, 0, "")), upsert("3.0", jobAd(t, 3, 0, "")),
		synced("a1"))
	apply(t, b, reset(), upsert("2.0", jobAd(t, 2, 0, "")), synced("b1"))
	a.EndSession()

	// The spoke restarted (new watch epoch => Reset). Meanwhile 2.0 left its queue, 3.0 changed
	// and 4.0 was submitted.
	wc := countWrites(t, jobs)
	a.BeginSession()
	apply(t, a, reset(),
		upsert("1.0", jobAd(t, 1, 0, "")),
		upsert("3.0", jobAd(t, 3, 0, "JobStatus = 2")),
		upsert("4.0", jobAd(t, 4, 0, "")),
		synced("a2"))

	got := map[string]bool{}
	seq, _ := jobs.Query(`ScheddName == "ap1.example.org"`)
	for ad := range seq {
		k, _ := ad.EvaluateAttrString("Key")
		got[k] = true
	}
	if len(got) != 3 || !got["1.0"] || !got["3.0"] || !got["4.0"] {
		t.Fatalf("ap1 rows after reconcile = %v, want exactly 1.0 3.0 4.0", got)
	}
	if n := countWhere(t, jobs, `ScheddName == "ap1.example.org" && ClusterId == 3 && JobStatus == 2`); n != 1 {
		t.Errorf("changed row not updated")
	}
	if n := countWhere(t, jobs, `ScheddName == "ap2.example.org" && ClusterId == 2`); n != 1 {
		t.Errorf("ap2's 2.0 was swept by ap1's reconcile: %d rows", n)
	}
	ups, dels := wc.settle(t)
	if ups != 2 || dels != 1 {
		t.Errorf("reconcile wrote %d upserts and %d deletes, want 2 (3.0 changed, 4.0 new) and 1 (2.0)", ups, dels)
	}
	if v := val(m.PhantomDeletes.WithLabelValues(TableJobs)); v != 1 {
		t.Errorf("phantom_deletes = %v, want 1", v)
	}
	if v := val(m.IdenticalSkips.WithLabelValues(TableJobs)); v != 1 {
		t.Errorf("identical_skips = %v, want 1 (1.0)", v)
	}
}

// TestResetUnchangedSourceWritesNothing: a spoke restart against an unchanged queue replays every
// row; the hub must write none of them (no store churn, no watch events downstream, no false
// "job changed" signals).
func TestResetUnchangedSourceWritesNothing(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	m := NewMetrics()
	a := newJobsSink(t, jobs, "ap1.example.org", m)
	replay := func(cur string) {
		changes := []replicate.Change{reset()}
		for i := 1; i <= 50; i++ {
			changes = append(changes, upsert(jobKey(i), jobAd(t, i, 0, "")))
		}
		apply(t, a, append(changes, synced(cur))...)
	}
	replay("c1")
	a.EndSession()
	appliedBefore := val(m.EventsApplied.WithLabelValues(TableJobs, "upsert"))

	wc := countWrites(t, jobs)
	a.BeginSession()
	replay("c2")
	ups, dels := wc.settle(t)
	if ups != 0 || dels != 0 {
		t.Fatalf("replay of an unchanged source wrote %d upserts and %d deletes, want 0 and 0", ups, dels)
	}
	if after := val(m.EventsApplied.WithLabelValues(TableJobs, "upsert")); after != appliedBefore {
		t.Errorf("events_applied grew from %v to %v on an unchanged replay", appliedBefore, after)
	}
	if v := val(m.IdenticalSkips.WithLabelValues(TableJobs)); v != 50 {
		t.Errorf("identical_skips = %v, want 50", v)
	}
	if n := countWhere(t, jobs, `ScheddName == "ap1.example.org"`); n != 50 {
		t.Errorf("rows = %d, want 50", n)
	}
}

func jobKey(i int) string { return itoa(i) + ".0" }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

// TestIncompleteResetDoesNotSweep: a replay cut off before Synced did not deliver the whole source,
// so its untouched rows are not "gone at the source" and must not be deleted.
func TestIncompleteResetDoesNotSweep(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	a := newJobsSink(t, jobs, "ap1.example.org", NewMetrics())
	apply(t, a, reset(), upsert("1.0", jobAd(t, 1, 0, "")), upsert("2.0", jobAd(t, 2, 0, "")), synced("c1"))
	a.EndSession()

	a.BeginSession()
	apply(t, a, reset(), upsert("1.0", jobAd(t, 1, 0, "")))
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	a.EndSession() // the stream dropped mid-replay

	if n := countWhere(t, jobs, `ScheddName == "ap1.example.org"`); n != 2 {
		t.Fatalf("rows after an interrupted replay = %d, want 2 (nothing swept)", n)
	}
	// The Reset cleared the cursor, so the next session is a full replay again and that one sweeps.
	if got := a.Cursor(); len(got) != 0 {
		t.Errorf("cursor after interrupted replay = %q, want none", got)
	}
}

// TestCursorCommittedOnFlush: live events' cursors are committed by the periodic flush, after the
// batch they cover -- not only at Synced (which made a hub restart replay everything since its
// last connect).
func TestCursorCommittedOnFlush(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	store := &replicate.MemCursorStore{}
	a, err := newTableSink(jobs, TableJobs, "ap1.example.org", store, NewMetrics(), time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	apply(t, a, reset(), synced("s"))
	live := upsert("9.0", jobAd(t, 9, 0, ""))
	live.Cursor = []byte("live-9")
	apply(t, a, live)
	if cur, _ := store.Load(); string(cur) != "s" {
		t.Fatalf("cursor %q committed before its batch", cur)
	}
	if n := countWhere(t, jobs, `ClusterId == 9`); n != 0 {
		t.Fatalf("batch visible before flush (%d rows); expected buffered", n)
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if cur, _ := store.Load(); string(cur) != "live-9" {
		t.Fatalf("cursor after flush = %q, want live-9", cur)
	}
	if n := countWhere(t, jobs, `ClusterId == 9`); n != 1 {
		t.Fatalf("row not committed by flush")
	}
}

// TestHeartbeatReceiptTime: a syncstatus row is stamped with the hub's clock on receipt, and a
// redelivery of the same heartbeat keeps its original receipt time -- otherwise a replay would
// make an old heartbeat look new.
func TestHeartbeatReceiptTime(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	ss := mustTable(t, cat, TableSyncStatus)
	clock := time.Unix(1_700_000_000, 0)
	s, err := newTableSink(ss, TableSyncStatus, "ap1.example.org", &replicate.MemCursorStore{}, NewMetrics(),
		func() time.Time { return clock }, nil)
	if err != nil {
		t.Fatal(err)
	}
	hb := func(seq int) replicate.Change {
		return upsert("status", parseAd(t, `HeartbeatSeq = `+itoa(seq)+`; HeartbeatTime = `+itoa(1_600_000_000+seq)+`; SpokeLagSeconds = 1; HeartbeatIntervalSeconds = 5`))
	}
	recv := func() int64 {
		row, ok := ss.LookupClassAd(HubKey("ap1.example.org", "status"))
		if !ok {
			t.Fatal("no hub syncstatus row")
		}
		v, _ := row.EvaluateAttrInt(HubReceivedTimeAttr)
		return v
	}
	apply(t, s, reset(), hb(1), synced("c"))
	if got := recv(); got != clock.Unix() {
		t.Fatalf("HubReceivedTime = %d, want %d", got, clock.Unix())
	}
	clock = clock.Add(time.Minute)
	s.BeginSession()
	apply(t, s, reset(), hb(1), synced("c2")) // replay of the same heartbeat
	if got := recv(); got != clock.Add(-time.Minute).Unix() {
		t.Errorf("redelivered heartbeat restamped: HubReceivedTime = %d", got)
	}
	apply(t, s, hb(2))
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := recv(); got != clock.Unix() {
		t.Errorf("new heartbeat HubReceivedTime = %d, want %d", got, clock.Unix())
	}
}

// TestScheddIdentityIgnoresCase: schedd names are case-insensitive (as in HTCondor, and as ClassAd
// == compares ScheddName), so rows written under two spellings of one schedd are one AP's rows --
// the same keys, no duplicates, and a Reset under either spelling reconciles all of them. Another
// schedd's rows are never touched.
func TestScheddIdentityIgnoresCase(t *testing.T) {
	if HubKey("AP1.example.org", "1.0") != HubKey("ap1.example.org", "1.0") {
		t.Fatal("HubKey depends on the case of the schedd name")
	}
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	lower := newJobsSink(t, jobs, "ap1.example.org", NewMetrics())
	upper := newJobsSink(t, jobs, "AP1.example.org", NewMetrics())
	other := newJobsSink(t, jobs, "ap10.example.org", NewMetrics())
	apply(t, other, reset(), upsert("1.0", jobAd(t, 1, 0, "")), upsert("2.0", jobAd(t, 2, 0, "")), synced("o1"))
	apply(t, lower, reset(), upsert("1.0", jobAd(t, 1, 0, "")), upsert("2.0", jobAd(t, 2, 0, "")), synced("l1"))
	// The same AP, now spelled in capitals, replays: 2.0 is gone at the source, 7.0 is new.
	apply(t, upper, reset(), upsert("1.0", jobAd(t, 1, 0, "")), upsert("7.0", jobAd(t, 7, 0, "")), synced("u1"))

	if got := jobKeysOf(t, cat, "ap1.example.org"); !sameKeys(got, 1, 7) {
		t.Errorf("ap1 rows = %v, want 1.0 and 7.0", got)
	}
	if got := jobKeysOf(t, cat, "ap10.example.org"); !sameKeys(got, 1, 2) {
		t.Errorf("ap10 rows = %v, want 1.0 and 2.0 (another schedd's rows untouched)", got)
	}
	if n := jobs.Len(); n != 4 {
		t.Errorf("hub rows = %d, want 4 (no duplicate rows for one AP under two spellings)", n)
	}
	row, ok := jobs.LookupClassAd(HubKey("ap1.example.org", "1.0"))
	if !ok {
		t.Fatal("ap1's 1.0 missing")
	}
	if name, _ := row.EvaluateAttrString(ScheddNameAttr); name != "AP1.example.org" {
		t.Errorf("ScheddName = %q, want the spelling the source last used", name)
	}
	// A delete under one spelling removes the row written under the other.
	apply(t, lower, del("7.0"))
	if err := lower.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := jobKeysOf(t, cat, "ap1.example.org"); !sameKeys(got, 1) {
		t.Errorf("ap1 rows after a delete under the other spelling = %v, want 1.0", got)
	}
}

// undecodable is the change the Runner hands an UndecodableSink for an upsert whose ad could not
// be decoded.
func undecodable(key string) replicate.Change {
	return replicate.Change{Kind: replicate.KindUpsert, Key: key}
}

// TestUndecodableReplayRowKept: a Reset replay that delivers a key whose ad could not be decoded
// has still delivered the key -- the source has the row. The hub keeps the row it holds rather than
// sweeping it as gone, and counts the event. A key the replay does not deliver at all is still
// swept.
func TestUndecodableReplayRowKept(t *testing.T) {
	cat := openCatalog(t, "")
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	m := NewMetrics()
	a := newJobsSink(t, jobs, "ap1.example.org", m)
	apply(t, a, reset(), upsert("1.0", jobAd(t, 1, 0, "")), upsert("2.0", jobAd(t, 2, 0, "")), upsert("3.0", jobAd(t, 3, 0, "")), synced("c1"))
	a.EndSession()

	a.BeginSession()
	apply(t, a, reset(), upsert("1.0", jobAd(t, 1, 0, "")))
	if err := a.ApplyUndecodable(undecodable("2.0"), errors.New("bad wire ad")); err != nil {
		t.Fatal(err)
	}
	apply(t, a, synced("c2"))
	if got := jobKeysOf(t, cat, "ap1.example.org"); !sameKeys(got, 1, 2) {
		t.Errorf("rows after a replay with an undecodable 2.0 and no 3.0 = %v, want 1.0 and 2.0", got)
	}
	if v := val(m.Undecodable.WithLabelValues(TableJobs)); v != 1 {
		t.Errorf("undecodable_total = %v, want 1", v)
	}

	// Live: the row is left as it was and the cursor moves past the event.
	live := undecodable("1.0")
	live.Cursor = []byte("c3")
	if err := a.ApplyUndecodable(live, errors.New("bad wire ad")); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := string(a.Cursor()); got != "c3" {
		t.Errorf("cursor = %q, want c3", got)
	}
	if got := jobKeysOf(t, cat, "ap1.example.org"); !sameKeys(got, 1, 2) {
		t.Errorf("rows after a live undecodable upsert = %v, want 1.0 and 2.0", got)
	}
}
