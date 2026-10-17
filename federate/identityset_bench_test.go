package federate

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db/replicate"
)

// BenchmarkJobMetricsIdentitySet measures what a Reset replay of job_metrics past the probe budget
// costs the hub: loadIdentities' scan of the AP's held samples and the memory of the identity set
// it builds, per held sample. The set is O(held samples): a Reset replay is checked against all of
// them. HTCONDORDB_BENCH_IDENTITIES sets how many samples the hub holds (default 20000).
//
//	HTCONDORDB_BENCH_IDENTITIES=1000000 go test -run '^$' -bench JobMetricsIdentitySet -benchtime 1x ./federate/
func BenchmarkJobMetricsIdentitySet(b *testing.B) {
	n := 20000
	if v, err := strconv.Atoi(os.Getenv("HTCONDORDB_BENCH_IDENTITIES")); err == nil && v > 0 {
		n = v
	}
	cat := openCatalog(&testing.T{}, b.TempDir())
	defer func() { _ = cat.Close() }()
	jm := hubArchive(&testing.T{}, cat, TableJobMetrics)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		ad := sample(&testing.T{}, "ap1", 1+i%10000, 1_700_000_000+int64(i/10000)*900, int64(100*i))
		ad.InsertAttrString(ScheddNameAttr, "ap1")
		if err := jm.Append(ad); err != nil {
			b.Fatal(err)
		}
	}
	b.Logf("appended %d samples in %s", n, time.Since(t0).Round(time.Millisecond))
	b.ResetTimer()
	for range b.N {
		s, err := newArchiveSink(jm, TableJobMetrics, "ap1", &replicate.MemCursorStore{}, NewMetrics(), discard, nil)
		if err != nil {
			b.Fatal(err)
		}
		s.full = true
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		if err := s.loadIdentities(); err != nil {
			b.Fatal(err)
		}
		took := time.Since(start)
		runtime.GC()
		runtime.ReadMemStats(&after)
		if len(s.remaining) != n {
			b.Fatalf("identity set holds %d, want %d", len(s.remaining), n)
		}
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(n), "heapB/identity")
		b.ReportMetric(float64(took.Nanoseconds())/float64(n), "scan-ns/identity")
		runtime.KeepAlive(s)
	}
}
