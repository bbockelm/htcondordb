package federate

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/scheddsync"
)

// sample is a job_metrics record as a spoke's sampler writes it: job cluster.0, run 0, taken at
// SampleTime at (an ingest-clock time, so samples read in one pass share it), from the commit
// ending at offset in log generation 3.
func sample(t *testing.T, schedd string, cluster int, at, offset int64) *classad.ClassAd {
	t.Helper()
	return parseAd(t, fmt.Sprintf(`Key = "%d.0"; ClusterId = %d; ProcId = 0; GlobalJobId = "%s#%d.0#1700000000"; `+
		`Owner = "alice"; User = "alice@example.org"; RunInstanceID = 0; SampleTime = %d; SampleTrigger = "update"; `+
		`SampleTimeFromIngest = true; LogSeq = 3; LogOffset = %d; MemoryUsage = %d`,
		cluster, cluster, schedd, cluster, at, offset, offset))
}

func sampleChange(t *testing.T, schedd string, cluster int, at, offset int64) replicate.Change {
	return upsert("", sample(t, schedd, cluster, at, offset))
}

func metricsArchive(t *testing.T, cat *db.Catalog) *db.ArchiveTable {
	t.Helper()
	return hubArchive(t, cat, TableJobMetrics)
}

func newMetricsSink(t *testing.T, a *db.ArchiveTable, schedd string, store replicate.CursorStore, m *Metrics) *archiveSink {
	t.Helper()
	s, err := newArchiveSink(a, TableJobMetrics, schedd, store, m, discard, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestJobMetricsIdentity: a sample's identity is its job, run, SampleTime, trigger and log
// position. Two samples equal in all but LogOffset -- ingest-clock samples of one job read in one
// pass -- are different samples; a record from a spoke too old to write LogOffset still has an
// identity; one without Key, RunInstanceID or SampleTime has none. The projected form (the
// identity set a long replay loads) agrees with the record form.
func TestJobMetricsIdentity(t *testing.T) {
	a := sample(t, "ap1", 1, 1_700_000_100, 400)
	b := sample(t, "ap1", 1, 1_700_000_100, 520)
	ia, ok1 := archiveIdentity(TableJobMetrics, a)
	ib, ok2 := archiveIdentity(TableJobMetrics, b)
	if !ok1 || !ok2 {
		t.Fatal("a full sample has no identity")
	}
	if ia.digest() == ib.digest() {
		t.Error("samples at different log offsets share an identity")
	}
	if again, _ := archiveIdentity(TableJobMetrics, sample(t, "ap1", 1, 1_700_000_100, 400)); again.digest() != ia.digest() {
		t.Error("the same sample has two identities")
	}
	old := parseAd(t, `Key = "1.0"; RunInstanceID = 0; SampleTime = 5; SampleTrigger = "update"; LogSeq = 3`)
	if _, ok := archiveIdentity(TableJobMetrics, old); !ok {
		t.Error("a sample without LogOffset (an older spoke) has no identity")
	}
	for _, drop := range []string{"Key", "RunInstanceID", "SampleTime"} {
		ad := sample(t, "ap1", 1, 5, 1)
		ad.Delete(drop)
		if _, ok := archiveIdentity(TableJobMetrics, ad); ok {
			t.Errorf("a sample without %s has an identity", drop)
		}
	}

	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	for _, ad := range []*classad.ClassAd{a, old} {
		ad.InsertAttrString(ScheddNameAttr, "ap1")
		if err := jm.Append(ad); err != nil {
			t.Fatal(err)
		}
	}
	seq, err := jm.QueryProject(scheddConstraint("ap1"), identityAttrs(TableJobMetrics))
	if err != nil {
		t.Fatal(err)
	}
	got := map[[16]byte]bool{}
	for vals := range seq {
		id, ok := identityFromValues(TableJobMetrics, vals)
		if !ok {
			t.Fatalf("projected identity missing: %v", vals)
		}
		got[id.digest()] = true
	}
	iold, _ := archiveIdentity(TableJobMetrics, old)
	if !got[ia.digest()] || !got[iold.digest()] || len(got) != 2 {
		t.Errorf("projected identities do not match the records' (%d distinct)", len(got))
	}
	// The exact probe finds each, and only it.
	for _, id := range []recordIdentity{ia, ib, iold} {
		want := 1
		if id.digest() == ib.digest() {
			want = 0
		}
		if n := countArchive(t, jm, id.constraint("ap1")); n != want {
			t.Errorf("probe %s matched %d, want %d", id.constraint("ap1"), n, want)
		}
	}
}

// TestJobMetricsReplayNoDuplicates: every spoke Reset replays its whole retained job_metrics. The
// hub appends only what is new -- including samples that share job, run, SampleTime and trigger
// and differ only in log position -- through both the per-record probe and the identity set a
// replay past the probe budget loads.
func TestJobMetricsReplayNoDuplicates(t *testing.T) {
	for _, n := range []int{20, probeBudget + 40} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			cat := openCatalog(t, t.TempDir())
			t.Cleanup(func() { _ = cat.Close() })
			jm := metricsArchive(t, cat)
			m := NewMetrics()
			s := newMetricsSink(t, jm, "ap1", &replicate.MemCursorStore{}, m)
			replay := func(count int, cur string) {
				s.BeginSession()
				changes := []replicate.Change{reset()}
				for i := 1; i <= count; i++ {
					// Five jobs, every sample of a pass at one ingest second.
					changes = append(changes, sampleChange(t, "ap1", 1+i%5, 1_700_000_000+int64(i/50), int64(100*i)))
				}
				apply(t, s, append(changes, synced(cur))...)
				s.EndSession()
			}
			replay(n, "c1")
			replay(n, "c2")   // spoke restart, nothing new
			replay(n+3, "c3") // spoke restart, three new samples
			if got := countArchive(t, jm, `true`); got != n+3 {
				t.Fatalf("hub job_metrics holds %d samples after replays of %d, %d, %d; want %d", got, n, n, n+3, n+3)
			}
			for i := 1; i <= n+3; i++ {
				if c := countArchive(t, jm, fmt.Sprintf(`LogOffset == %d`, 100*i)); c != 1 {
					t.Fatalf("sample at offset %d appears %d times, want 1", 100*i, c)
				}
			}
			if v := val(m.DedupHits.WithLabelValues(TableJobMetrics)); v != float64(2*n) {
				t.Errorf("dedup_hits = %v, want %d", v, 2*n)
			}
		})
	}
}

// TestJobMetricsOverlapKeepsNewSample: a resume's catch-up re-delivers from the committed cursor,
// so it carries samples the hub never received, and each is checked against what the hub held. One
// that shares job, run, SampleTime and trigger with a held sample from before the cursor -- the next
// ingest-clock sample of the same job, taken while the stream was down -- is new: the log position
// says so. Matching on the rest alone would count the held sample as this one and drop it for good.
func TestJobMetricsOverlapKeepsNewSample(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	m := NewMetrics()
	store := &replicate.MemCursorStore{}
	s := newMetricsSink(t, jm, "ap1", store, m)
	s.BeginSession()
	apply(t, s, reset(), synced("s0"), liveChange(sampleChange(t, "ap1", 1, 1_700_000_100, 400), "after-400"))
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	s.EndSession() // the stream drops; the cursor is after-400

	s.BeginSession() // resume after-400: the catch-up starts with the sample taken meanwhile
	apply(t, s, sampleChange(t, "ap1", 1, 1_700_000_100, 520), synced("after-520"))
	s.EndSession()
	if got := countArchive(t, jm, `true`); got != 2 {
		t.Fatalf("hub holds %d samples, want 2 (the held one and the one taken while the stream was down)", got)
	}
	if got := countArchive(t, jm, `LogOffset == 520`); got != 1 {
		t.Errorf("the new sample in the catch-up appended %d times, want 1", got)
	}
	if v := val(m.DedupHits.WithLabelValues(TableJobMetrics)); v != 0 {
		t.Errorf("dedup_hits = %v, want 0", v)
	}
}

// TestJobMetricsRedeliveryAfterHubRestart: the hub crashes after appending live samples but before
// their cursor is committed. The restarted hub resumes from the committed cursor and the spoke
// re-delivers them; none is appended twice, and the new one after them is appended.
func TestJobMetricsRedeliveryAfterHubRestart(t *testing.T) {
	dir := t.TempDir()
	cursorPath := filepath.Join(t.TempDir(), "job_metrics.cursor")
	m := NewMetrics()
	cat := openCatalog(t, dir)
	jm := metricsArchive(t, cat)
	s := newMetricsSink(t, jm, "ap1", fileCursorStore{path: cursorPath}, m)
	s.BeginSession()
	apply(t, s, reset(), sampleChange(t, "ap1", 1, 1_700_000_100, 100), synced("after-1"))
	for i := 2; i <= 4; i++ {
		apply(t, s, liveChange(sampleChange(t, "ap1", 1, 1_700_000_100, int64(100*i)), "after-"+itoa(i)))
	}
	_ = cat.Close() // crash: no Flush

	cat = openCatalog(t, dir)
	t.Cleanup(func() { _ = cat.Close() })
	jm = metricsArchive(t, cat)
	s = newMetricsSink(t, jm, "ap1", fileCursorStore{path: cursorPath}, m)
	if got := string(s.Cursor()); got != "after-1" {
		t.Fatalf("resume cursor = %q, want after-1", got)
	}
	s.BeginSession()
	for i := 2; i <= 5; i++ {
		apply(t, s, sampleChange(t, "ap1", 1, 1_700_000_100, int64(100*i)))
	}
	apply(t, s, synced("after-5"))
	for i := 1; i <= 5; i++ {
		if c := countArchive(t, jm, fmt.Sprintf(`LogOffset == %d`, 100*i)); c != 1 {
			t.Errorf("sample %d appears %d times after the hub restart, want 1", i, c)
		}
	}
	if v := val(m.DedupHits.WithLabelValues(TableJobMetrics)); v != 3 {
		t.Errorf("dedup_hits = %v, want 3", v)
	}
}

// TestJobMetricsTwoAPsSameJob: two APs' job 1.0 sample at the same instant from the same log
// position -- identical records but for the AP. They are two APs' samples, each stored under its
// own ScheddName, and neither AP's replay suppresses the other's, by probe or by identity set.
func TestJobMetricsTwoAPsSameJob(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	n := probeBudget + 20
	replay := func(schedd, cur string) {
		s := newMetricsSink(t, jm, schedd, &replicate.MemCursorStore{}, NewMetrics())
		s.BeginSession()
		changes := []replicate.Change{reset()}
		for i := 1; i <= n; i++ {
			changes = append(changes, sampleChange(t, "ap1", 1, 1_700_000_000, int64(100*i))) // same records from both
		}
		apply(t, s, append(changes, synced(cur))...)
		s.EndSession()
	}
	replay("ap1.example.org", "a1")
	replay("ap2.example.org", "b1")
	replay("ap1.example.org", "a2") // a restart of ap1 must not count ap2's samples as its own
	for _, schedd := range []string{"ap1.example.org", "ap2.example.org"} {
		if got := countArchive(t, jm, fmt.Sprintf(`ScheddName == %q && ClusterId == 1 && ProcId == 0`, schedd)); got != n {
			t.Errorf("%s job 1.0: %d samples, want %d", schedd, got, n)
		}
	}
	if got := countArchive(t, jm, `true`); got != 2*n {
		t.Errorf("hub holds %d samples, want %d", got, 2*n)
	}
}

// TestJobMetricsScheddNameOverwritten: a sample claiming another AP is stored under its source's
// identity, in catch-up and live alike; the owner-scoping attributes ride through untouched.
func TestJobMetricsScheddNameOverwritten(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	s := newMetricsSink(t, jm, "ap1.example.org", &replicate.MemCursorStore{}, NewMetrics())
	forged := func(offset int64) replicate.Change {
		ad := sample(t, "ap2.example.org", 1, 1_700_000_000, offset)
		ad.InsertAttrString(ScheddNameAttr, "ap2.example.org")
		return upsert("", ad)
	}
	s.BeginSession()
	apply(t, s, reset(), forged(100), synced("c"), liveChange(forged(200), "l"))
	if n := countArchive(t, jm, `ScheddName == "ap2.example.org"`); n != 0 {
		t.Errorf("%d samples kept their claim to ap2", n)
	}
	if n := countArchive(t, jm, `ScheddName == "ap1.example.org" && User == "alice@example.org" && ClusterId == 1 && ProcId == 0 && RunInstanceID == 0`); n != 2 {
		t.Errorf("%d samples stamped with their source and carrying User and the job id, want 2", n)
	}
}

// TestJobMetricsFailedSyncKeepsCursor: after a failed flush no cursor is committed over the
// samples it covered, for the rest of the session; the next session re-delivers them, deduped.
func TestJobMetricsFailedSyncKeepsCursor(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	store := &replicate.MemCursorStore{}
	s := newMetricsSink(t, jm, "ap1", store, NewMetrics())
	fail := true
	s.syncData = func() error {
		if fail {
			return errors.New("EIO")
		}
		return nil
	}
	s.BeginSession()
	apply(t, s, reset(), synced("s0"))
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	apply(t, s, liveChange(sampleChange(t, "ap1", 1, 1_700_000_000, 100), "c1"))
	if err := s.Flush(); err == nil {
		t.Fatal("flush succeeded with a failed sync")
	}
	fail = false
	if err := s.Apply(liveChange(sampleChange(t, "ap1", 1, 1_700_000_000, 200), "c2")); err == nil {
		t.Error("a sink whose flush failed accepted another sample in the same session")
	}
	_ = s.Flush()
	s.EndSession()
	if cur, _ := store.Load(); string(cur) != "s0" {
		t.Fatalf("cursor after a failed sync = %q, want s0", cur)
	}
	s.BeginSession()
	apply(t, s, sampleChange(t, "ap1", 1, 1_700_000_000, 100), sampleChange(t, "ap1", 1, 1_700_000_000, 200), synced("s1"))
	if cur, _ := store.Load(); string(cur) != "s1" {
		t.Fatalf("cursor after the next session = %q, want s1", cur)
	}
	if n := countArchive(t, jm, `true`); n != 2 {
		t.Fatalf("hub job_metrics = %d samples, want 2", n)
	}
}

// TestJobMetricsMissingIdentity: a sample without its identity (no Key) is appended, unchecked,
// and counted.
func TestJobMetricsMissingIdentity(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	m := NewMetrics()
	s := newMetricsSink(t, jm, "ap1", &replicate.MemCursorStore{}, m)
	noKey := func() replicate.Change {
		ad := sample(t, "ap1", 9, 1_700_000_000, 100)
		ad.Delete("Key")
		return upsert("", ad)
	}
	for i := 0; i < 2; i++ {
		s.BeginSession()
		apply(t, s, reset(), noKey(), synced("c"+itoa(i)))
		s.EndSession()
	}
	if n := countArchive(t, jm, `ClusterId == 9`); n != 2 {
		t.Errorf("identity-less samples = %d, want 2 (appended unchecked)", n)
	}
	if v := val(m.MissingIdentity.WithLabelValues(TableJobMetrics)); v != 2 {
		t.Errorf("missing_identity = %v, want 2", v)
	}
}

// TestJobMetricsReplaySkipsWhatTheCapDropped: in an age-capped job_metrics archive, a Reset replay
// skips samples older (by SampleTime) than the oldest the hub still holds for the AP.
func TestJobMetricsReplaySkipsWhatTheCapDropped(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	var wg sync.WaitGroup
	ht, err := ensureTables(cat, []string{TableJobMetrics}, ArchiveOptions{MaxAge: map[string]float64{TableJobMetrics: 86400}}, discard, &wg)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	jm := ht.archives[TableJobMetrics]
	m := NewMetrics()
	s := newMetricsSink(t, jm, "ap1", &replicate.MemCursorStore{}, m)
	s.BeginSession()
	apply(t, s, reset(), sampleChange(t, "ap1", 1, 1_700_000_500, 500), synced("c1"))
	s.EndSession()
	s.BeginSession()
	apply(t, s, reset(), sampleChange(t, "ap1", 1, 1_700_000_100, 100), sampleChange(t, "ap1", 1, 1_700_000_500, 500),
		sampleChange(t, "ap1", 1, 1_700_000_600, 600), synced("c2"))
	s.EndSession()
	if n := countArchive(t, jm, `true`); n != 2 {
		t.Errorf("hub holds %d samples, want 2 (the held one and the newer one)", n)
	}
	if v := val(m.BelowRetention.WithLabelValues(TableJobMetrics)); v != 1 {
		t.Errorf("below_retention_total = %v, want 1", v)
	}
}

// TestHubJobMetricsSchema pins what the API reads job_metrics by: categorical ScheddName, Owner,
// User and GlobalJobId; value ClusterId; a SampleTime zone map; and the configured size and age
// caps (the age cap against SampleTime).
func TestHubJobMetricsSchema(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	defer func() { _ = cat.Close() }()
	var wg sync.WaitGroup
	ht, err := ensureTables(cat, []string{TableJobMetrics}, ArchiveOptions{
		MaxBytes: map[string]int64{TableJobMetrics: 1 << 30},
		MaxAge:   map[string]float64{TableJobMetrics: 30 * 86400},
	}, discard, &wg)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	jm := ht.archives[TableJobMetrics]
	cats, vals := jm.IndexedAttrs()
	for _, want := range []string{ScheddNameAttr, "Owner", "User", "GlobalJobId"} {
		if !hasFold(cats, want) {
			t.Errorf("categorical indexes %v lack %s", cats, want)
		}
	}
	if !hasFold(vals, "ClusterId") {
		t.Errorf("value indexes %v lack ClusterId", vals)
	}
	if z := jm.ZoneAttrs(); !hasFold(z, "SampleTime") {
		t.Errorf("zone maps %v lack SampleTime", z)
	}
	r := jm.Retention()
	if r.MaxBytes != 1<<30 || r.MaxAge != 30*86400 || r.MaxAgeAttr != "SampleTime" {
		t.Errorf("retention = %+v, want MaxBytes 1GiB, MaxAge 30d on SampleTime", r)
	}
}

// TestJobMetricsNamesMatchSpoke: the hub names the spoke's table and the sample attributes its
// identity is built from by literal; they must be the spoke's.
func TestJobMetricsNamesMatchSpoke(t *testing.T) {
	if TableJobMetrics != scheddsync.DefaultJobMetricsTable {
		t.Errorf("TableJobMetrics = %q, spoke writes %q", TableJobMetrics, scheddsync.DefaultJobMetricsTable)
	}
	want := []string{scheddsync.KeyAttr, scheddsync.RunInstanceAttr, scheddsync.SampleTimeAttr,
		scheddsync.SampleTriggerAttr, scheddsync.SampleLogSeqAttr, scheddsync.SampleLogOffsetAttr}
	if got := identityAttrs(TableJobMetrics); !slices.Equal(got, want) {
		t.Errorf("job_metrics identity attributes = %v, spoke writes %v", got, want)
	}
	if floorAttr(TableJobMetrics) != scheddsync.SampleTimeAttr {
		t.Errorf("job_metrics floor attribute = %q", floorAttr(TableJobMetrics))
	}
}
