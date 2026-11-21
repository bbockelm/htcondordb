package federate

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db/replicate"
)

// histRec is a history record as a spoke writes one: with EnteredHistoryTime, which scheddsync
// stamps on every record it archives and which narrows the batch-tail probe.
func histRec(t *testing.T, cluster int) *classad.ClassAd {
	t.Helper()
	return histAt(t, "ap1", cluster, 1_700_000_000+int64(cluster))
}

// batchOf sizes a sink's skip-ahead buffer to hold about n records like ad.
func batchOf(s *archiveSink, ad *classad.ClassAd, n int) {
	s.batchLimit = n * (changeOverheadBytes + 8 + approxAdBytes(ad))
}

// TestResumeSkipsHeldPrefixInBatches: a resume re-delivers everything since the committed cursor,
// and what the hub already holds is a prefix of it. The sink must find the boundary with a probe
// per BATCH, not a probe per record -- an AP whose cursor has not advanced in a day re-delivers
// six figures of records, and a query each is minutes of work the hub repeats on every restart.
func TestResumeSkipsHeldPrefixInBatches(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	m := NewMetrics()
	s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, m)
	batchOf(s, histRec(t, 1), 10)

	// The hub holds 60 records, appended live (the live tail does not dedup).
	apply(t, s, reset(), synced("c0"))
	for i := 1; i <= 60; i++ {
		apply(t, s, liveChange(upsert("r", histRec(t, i)), "l"+itoa(i)))
	}
	s.EndSession()

	// The resume re-delivers all 60 and then 5 new ones.
	s.BeginSession()
	for i := 1; i <= 65; i++ {
		apply(t, s, upsert("r", histRec(t, i)))
	}
	apply(t, s, synced("c1"))
	s.EndSession()

	if n := countArchive(t, hist, `true`); n != 65 {
		t.Fatalf("hub holds %d records after a resume re-delivered 60 held and 5 new, want 65", n)
	}
	for i := 1; i <= 65; i++ {
		if n := countArchive(t, hist, fmt.Sprintf(`ClusterId == %d`, i)); n != 1 {
			t.Errorf("job %d.0 appears %d times, want 1", i, n)
		}
	}
	if v := tableTotal(m.DedupHits, TableHistory); v != 60 {
		t.Errorf("dedup_hits = %v, want 60", v)
	}
	// Six full batches of held records are dropped on one probe each; only the batch holding the
	// boundary is probed record by record, and only up to its first miss.
	if s.probes > 10 {
		t.Errorf("%d per-record probes for a 65-record resume, want at most one batch's worth", s.probes)
	}
	if s.batchProbes < 6 || s.batchProbes > 8 {
		t.Errorf("%d batch probes, want about one per 10-record batch", s.batchProbes)
	}
}

// TestResumeBoundaryBatchIsCheckedRecordByRecord: the batch holding the boundary is part held,
// part new. Skipping it whole would lose the new records; appending it whole would duplicate the
// held ones.
func TestResumeBoundaryBatchIsCheckedRecordByRecord(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	m := NewMetrics()
	s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, m)
	batchOf(s, histRec(t, 1), 10)

	apply(t, s, reset(), synced("c0"))
	for i := 1; i <= 55; i++ { // the boundary falls halfway through the sixth batch
		apply(t, s, liveChange(upsert("r", histRec(t, i)), "l"+itoa(i)))
	}
	s.EndSession()

	s.BeginSession()
	for i := 1; i <= 60; i++ {
		apply(t, s, upsert("r", histRec(t, i)))
	}
	apply(t, s, synced("c1"))
	s.EndSession()

	if n := countArchive(t, hist, `true`); n != 60 {
		t.Fatalf("hub holds %d records, want 60 (55 held, 5 new)", n)
	}
	for i := 1; i <= 60; i++ {
		if n := countArchive(t, hist, fmt.Sprintf(`ClusterId == %d`, i)); n != 1 {
			t.Errorf("job %d.0 appears %d times, want 1", i, n)
		}
	}
	if v := tableTotal(m.DedupHits, TableHistory); v != 55 {
		t.Errorf("dedup_hits = %v, want 55", v)
	}
}

// TestResumeAppendsEverythingAfterTheBoundary: past the boundary nothing is probed, and that is
// not only a saving. A skipped batch's records are never counted against the hub's copies of their
// identity, so a probe past the boundary would match a later record against copies the skipped
// prefix already accounts for and drop a record the hub does not hold. Here a record repeats an
// identity the hub holds from before the boundary: it must be appended.
func TestResumeAppendsEverythingAfterTheBoundary(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	m := NewMetrics()
	s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, m)
	batchOf(s, histRec(t, 1), 5)

	apply(t, s, reset(), synced("c0"))
	for i := 1; i <= 5; i++ {
		apply(t, s, liveChange(upsert("r", histRec(t, i)), "l"+itoa(i)))
	}
	s.EndSession()

	s.BeginSession()
	for i := 1; i <= 5; i++ { // the held prefix: one full batch, skipped on one probe
		apply(t, s, upsert("r", histRec(t, i)))
	}
	apply(t, s,
		upsert("r", histRec(t, 6)), // the boundary
		upsert("r", histRec(t, 3)), // past it, repeating an identity the hub holds
		upsert("r", histRec(t, 7)),
		synced("c1"))
	s.EndSession()

	if n := countArchive(t, hist, `ClusterId == 3`); n != 2 {
		t.Errorf("job 3.0 appears %d times, want 2: a record past the boundary was matched against a held copy of its identity and dropped", n)
	}
	if n := countArchive(t, hist, `true`); n != 8 {
		t.Errorf("hub holds %d records, want 8", n)
	}
	if v := tableTotal(m.DedupHits, TableHistory); v != 5 {
		t.Errorf("dedup_hits = %v, want 5", v)
	}
}

// TestBoundaryBatchStillCountsRepeatedIdentities: an identity is not unique -- a run instance
// writes a CHECKPOINT epoch record per checkpoint -- so the boundary batch keeps the exact
// per-record path, which drops the first k of an identity when the hub holds k and appends the
// rest. Batch skipping must not turn that into "the hub holds one, drop them all".
func TestBoundaryBatchStillCountsRepeatedIdentities(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	ep := hubArchive(t, cat, TableEpochHistory)
	m := NewMetrics()
	s, err := newArchiveSink(ep, TableEpochHistory, "ap1", &replicate.MemCursorStore{}, m, discard, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The hub holds the first two CHECKPOINTs of one run instance.
	apply(t, s, reset(), synced("c0"))
	apply(t, s, liveChange(checkpointRec(t, 1), "l1"), liveChange(checkpointRec(t, 2), "l2"))
	s.EndSession()

	s.BeginSession()
	apply(t, s, checkpointRec(t, 1), checkpointRec(t, 2), checkpointRec(t, 3), synced("c1"))
	s.EndSession()

	if n := countArchive(t, ep, `EpochAdType == "CHECKPOINT"`); n != 3 {
		t.Fatalf("hub holds %d CHECKPOINT records of one run instance after a resume re-delivered 3 into a hub holding 2, want 3", n)
	}
	if v := tableTotal(m.DedupHits, TableEpochHistory); v != 2 {
		t.Errorf("dedup_hits = %v, want 2", v)
	}
}

// TestFlushDrainsBeforeCommittingTheCursor: the flush interval fires in the middle of a long
// overlap, and a buffered record is a record that is not in the archive yet. The cursor it would
// be covered by must not be committed until it is -- otherwise a crash right after the flush loses
// it, which is exactly what the sink's cursor discipline exists to prevent.
func TestFlushDrainsBeforeCommittingTheCursor(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, NewMetrics())

	s.BeginSession()
	for i := 1; i <= 3; i++ { // new records, buffered: the batch is nowhere near full
		apply(t, s, liveChange(upsert("r", histRec(t, i)), "c"+itoa(i)))
	}
	if len(s.batch) != 3 {
		t.Fatalf("precondition: %d records buffered, want 3", len(s.batch))
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := countArchive(t, hist, `true`); n != 3 {
		t.Errorf("hub holds %d records after the flush, want 3: the cursor moved over records still in the buffer", n)
	}
	if got := string(s.Cursor()); got != "c3" {
		t.Errorf("cursor = %q, want c3", got)
	}
}

// TestFederateMetricsAreLabelledBySchedd: the per-table counters carry the source AP, so one AP's
// replay storm or dedup rate can be told from another's on a hub with dozens of them.
func TestFederateMetricsAreLabelledBySchedd(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	m := NewMetrics()
	for _, ap := range []string{"ap1", "ap2"} {
		s := newHistSink(t, hist, ap, &replicate.MemCursorStore{}, m)
		s.BeginSession()
		apply(t, s, reset(), upsert("r", histRec(t, 1)), synced("c1"))
		s.EndSession()
		s.BeginSession() // a spoke restart replays it; the hub holds it already
		apply(t, s, reset(), upsert("r", histRec(t, 1)), synced("c2"))
		s.EndSession()
	}
	for _, ap := range []string{"ap1", "ap2"} {
		if v := val(m.DedupHits.WithLabelValues(TableHistory, ap)); v != 1 {
			t.Errorf("dedup_hits{%s} = %v, want 1", ap, v)
		}
		if v := val(m.EventsApplied.WithLabelValues(TableHistory, ap, "upsert")); v != 1 {
			t.Errorf("events_applied{%s,upsert} = %v, want 1", ap, v)
		}
		if v := val(m.Resets.WithLabelValues(TableHistory, ap)); v != 2 {
			t.Errorf("resets{%s} = %v, want 2", ap, v)
		}
	}
	if v := tableTotal(m.DedupHits, TableHistory); v != 2 {
		t.Errorf("dedup_hits summed over both APs = %v, want 2", v)
	}
	if v := tableTotal(m.EventsApplied, TableHistory, "upsert"); v != 2 {
		t.Errorf("events_applied summed over both APs = %v, want 2", v)
	}
}

// TestCatchupBatchIsBoundedByBytes: the buffer is bounded by memory, not by a record count. The
// same budget holds far fewer wide records than narrow ones, and never more than one record past
// the budget.
func TestCatchupBatchIsBoundedByBytes(t *testing.T) {
	wide := strings.Repeat("x", 8000)
	run := func(t *testing.T, pad string) (maxLen, maxBytes, perRecord int) {
		t.Helper()
		cat := openCatalog(t, t.TempDir())
		t.Cleanup(func() { _ = cat.Close() })
		hist := hubArchive(t, cat, TableHistory)
		s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, NewMetrics())
		s.batchLimit = 256 << 10
		rec := func(i int) *classad.ClassAd {
			ad := histRec(t, i)
			if pad != "" {
				ad.InsertAttrString("Args", pad)
			}
			return ad
		}
		perRecord = changeOverheadBytes + approxAdBytes(rec(1))
		s.BeginSession()
		for i := 1; i <= 400; i++ {
			apply(t, s, upsert("r", rec(i)))
			// The buffer drains the moment it reaches the budget, so between records it is always
			// under it; its peak is the budget plus the one record that tipped it over.
			if s.batchBytes >= s.batchLimit {
				t.Fatalf("buffer holds %d bytes with a %d-byte budget", s.batchBytes, s.batchLimit)
			}
			maxLen, maxBytes = max(maxLen, len(s.batch)), max(maxBytes, s.batchBytes)
		}
		return maxLen, maxBytes, perRecord
	}
	narrowLen, narrowBytes, narrowPer := run(t, "")
	wideLen, wideBytes, widePer := run(t, wide)

	t.Logf("narrow: %d records/batch at ~%d B each (peak %d B); wide: %d records/batch at ~%d B each (peak %d B)",
		narrowLen, narrowPer, narrowBytes, wideLen, widePer, wideBytes)
	if wideLen >= narrowLen/4 {
		t.Errorf("wide records buffer %d per batch and narrow ones %d: the bound is behaving like a record count, not a memory budget", wideLen, narrowLen)
	}
	for _, c := range []struct {
		name  string
		bytes int
	}{{"narrow", narrowBytes}, {"wide", wideBytes}} {
		if c.bytes >= 256<<10 {
			t.Errorf("%s peak buffer %d bytes exceeds the budget", c.name, c.bytes)
		}
	}
}

// TestIdentitySetLoadAbandonedWhenTooBig: a full replay loads the AP's identity counts to avoid a
// query per record, but a hub holding tens of millions of rows cannot materialise every AP's share
// of them at once. Past the hub's pooled budget the load is abandoned and the replay goes back to
// probing -- slow, where running out of memory would be fatal -- and it still deduplicates
// exactly.
func TestIdentitySetLoadAbandonedWhenTooBig(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, NewMetrics())
	// A budget this sink can fill: ten identities, taken one at a time.
	s.budget, s.identityChunk = newIdentityBudget(10), 1
	n := probeBudget + 20

	replay := func(cur string) {
		s.BeginSession()
		changes := []replicate.Change{reset()}
		for i := 1; i <= n; i++ {
			changes = append(changes, upsert("r", histRec(t, i)))
		}
		apply(t, s, append(changes, synced(cur))...)
		s.EndSession()
	}
	replay("c1")
	replay("c2") // a spoke restart replays the lot again; the hub must not append a second copy

	if got := countArchive(t, hist, `true`); got != n {
		t.Fatalf("hub holds %d records after two replays of %d, want %d", got, n, n)
	}
	if s.setLoads != 0 {
		t.Errorf("identity set loaded %d times although it could not fit", s.setLoads)
	}
}

// BenchmarkResumeCatchupMemory reports what a resume's catch-up holds down while it skips a long
// held prefix, with the real catchupBatchBytes budget: the heap it adds, the records a batch comes
// to, and how many queries the whole overlap cost. What it has to show is figures that do not move
// with the length of the overlap -- the buffer is dropped at every batch, so a 20k-record overlap
// and a 200k-record one hold the same memory and differ only in the number of batch probes.
//
// HTCONDORDB_BENCH_OVERLAP sets the overlap (default 20000).
//
//	go test -run '^$' -bench ResumeCatchupMemory -benchtime 1x ./federate/
func BenchmarkResumeCatchupMemory(b *testing.B) {
	n := 20000
	if v, err := strconv.Atoi(os.Getenv("HTCONDORDB_BENCH_OVERLAP")); err == nil && v > 0 {
		n = v
	}
	t := &testing.T{}
	cat := openCatalog(t, b.TempDir())
	defer func() { _ = cat.Close() }()
	hist := hubArchive(t, cat, TableHistory)
	s := newHistSink(t, hist, "ap1", &replicate.MemCursorStore{}, NewMetrics())
	rec := func(i int) *classad.ClassAd {
		ad := histRec(t, i)
		ad.InsertAttrString("Args", strings.Repeat("x", 1500)) // a history ad is a couple of KiB
		return ad
	}
	for i := 1; i <= n; i++ {
		ad := rec(i)
		ad.InsertAttrString(ScheddNameAttr, "ap1") // as the sink stamps them
		if err := hist.Append(ad); err != nil {
			b.Fatal(err)
		}
	}
	if err := hist.Flush(); err != nil {
		b.Fatal(err)
	}
	// The measured pass is not the first: the archive fills its own read caches on the way through,
	// and those are not what this is measuring.
	catchup := func() (peakHeap uint64, peakBatch, batchLen int) {
		s.BeginSession()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		base := ms.HeapAlloc
		for i := 1; i <= n; i++ {
			if err := s.Apply(upsert("r", rec(i))); err != nil {
				b.Fatal(err)
			}
			peakBatch, batchLen = max(peakBatch, s.batchBytes), max(batchLen, len(s.batch))
			if i%2000 == 0 {
				runtime.GC()
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > base {
					peakHeap = max(peakHeap, ms.HeapAlloc-base)
				}
			}
		}
		if err := s.Apply(synced("c")); err != nil {
			b.Fatal(err)
		}
		s.EndSession()
		return peakHeap, peakBatch, batchLen
	}
	catchup()
	b.ResetTimer()
	for range b.N {
		peakHeap, peakBatch, batchLen := catchup()
		b.ReportMetric(float64(peakHeap)/(1<<20), "peak-heap-MiB")
		b.ReportMetric(float64(peakBatch)/(1<<20), "peak-batch-MiB")
		b.ReportMetric(float64(batchLen), "records/batch")
		b.ReportMetric(float64(s.batchProbes), "batch-probes")
		b.ReportMetric(float64(s.probes), "record-probes")
	}
}
