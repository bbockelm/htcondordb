package federate

import (
	"fmt"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db/replicate"
)

// oldSample is a job_metrics record from a spoke that predates LogOffset.
func oldSample(t *testing.T, cluster int, at int64) *classad.ClassAd {
	t.Helper()
	ad := sample(t, "ap1", cluster, at, 1)
	ad.Delete("LogOffset")
	return ad
}

// TestJobMetricsProbeAbsentOffsetIsExact: the probe for a record without LogOffset must not match a
// held record that has one and agrees on everything else, nor the reverse.
func TestJobMetricsProbeAbsentOffsetIsExact(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	withOff := sample(t, "ap1", 1, 1_700_000_100, 400)
	noOff := sample(t, "ap1", 1, 1_700_000_100, 400)
	noOff.Delete("LogOffset")
	for _, ad := range []*classad.ClassAd{withOff, noOff} {
		ad.InsertAttrString(ScheddNameAttr, "ap1")
		if err := jm.Append(ad); err != nil {
			t.Fatal(err)
		}
	}
	for _, ad := range []*classad.ClassAd{withOff, noOff} {
		id, ok := archiveIdentity(TableJobMetrics, ad)
		if !ok {
			t.Fatal("no identity")
		}
		if n := countArchive(t, jm, id.constraint("ap1")); n != 1 {
			t.Errorf("probe %s matched %d, want 1", id.constraint("ap1"), n)
		}
	}
}

// TestJobMetricsOldSpokeReplayCounts: a spoke that predates LogOffset replays records whose identity is
// (Key, run, SampleTime, trigger, LogSeq) alone, and two genuinely distinct samples can share it.
// A full replay must keep exactly the count the spoke holds -- through the probe and through the
// identity set -- and append the one new record that shares an identity with held ones.
func TestJobMetricsOldSpokeReplayCounts(t *testing.T) {
	for _, n := range []int{10, probeBudget + 30} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			cat := openCatalog(t, t.TempDir())
			t.Cleanup(func() { _ = cat.Close() })
			jm := metricsArchive(t, cat)
			s := newMetricsSink(t, jm, "ap1", &replicate.MemCursorStore{}, NewMetrics())
			records := func(dups int) []replicate.Change {
				var out []replicate.Change
				for i := 0; i < n; i++ {
					out = append(out, upsert("", oldSample(t, 1+i%7, 1_700_000_000+int64(i))))
				}
				// Same identity as the last regular record: past the probe budget in the long
				// replay, so its held count comes from the identity set.
				last := n - 1
				for i := 0; i < dups; i++ {
					out = append(out, upsert("", oldSample(t, 1+last%7, 1_700_000_000+int64(last))))
				}
				return out
			}
			replay := func(dups int, cur string) {
				s.BeginSession()
				apply(t, s, append(append([]replicate.Change{reset()}, records(dups)...), synced(cur))...)
				s.EndSession()
			}
			replay(1, "c1") // the spoke holds two samples sharing the first identity
			replay(1, "c2")
			replay(2, "c3") // a third one arrives
			if got := countArchive(t, jm, `true`); got != n+2 {
				t.Fatalf("hub holds %d, want %d", got, n+2)
			}
			if got := countArchive(t, jm, fmt.Sprintf(`ClusterId == %d && SampleTime == %d`, 1+(n-1)%7, 1_700_000_000+n-1)); got != 3 {
				t.Errorf("shared-identity samples = %d, want 3", got)
			}
		})
	}
}

// TestJobMetricsMaxAgeClears: the hub's job_metrics age cap follows configuration both ways -- an
// unset HTCONDORDB_JOB_METRICS_MAX_AGE (MaxAge 0) removes a cap a previous configuration set.
func TestJobMetricsMaxAgeClears(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	open := func(age float64) {
		var wg sync.WaitGroup
		ht, err := ensureTables(cat, []string{TableJobMetrics}, ArchiveOptions{MaxAge: map[string]float64{TableJobMetrics: age}}, discard, &wg)
		if err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if r := ht.archives[TableJobMetrics].Retention(); r.MaxAge != age {
			t.Fatalf("after MaxAge %v: retention MaxAge = %v", age, r.MaxAge)
		}
	}
	open(86400)
	open(0)
}

// TestJobMetricsOverlapNewGenerationSample: a compaction restarts job_queue.log offsets under a new
// LogSeq. A sample from the new generation that agrees with a held one on job, run, SampleTime,
// trigger and LogOffset -- the same ingest second, the same byte offset in the new file -- is a new
// sample, and a resume's overlap must append it. Only LogSeq tells them apart.
func TestJobMetricsOverlapNewGenerationSample(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jm := metricsArchive(t, cat)
	s := newMetricsSink(t, jm, "ap1", &replicate.MemCursorStore{}, NewMetrics())
	s.BeginSession()
	apply(t, s, reset(), synced("s0"), liveChange(sampleChange(t, "ap1", 1, 1_700_000_100, 400), "after-400"))
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	s.EndSession()

	next := sample(t, "ap1", 1, 1_700_000_100, 400)
	next.InsertAttr("LogSeq", 4)
	s.BeginSession()
	apply(t, s, upsert("", next), synced("after-gen4"))
	s.EndSession()
	if got := countArchive(t, jm, `true`); got != 2 {
		t.Fatalf("hub holds %d samples, want 2 (one per log generation)", got)
	}
}
