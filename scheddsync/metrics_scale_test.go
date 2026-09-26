package scheddsync

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
)

// Phase 0 of the job_metrics design: measure what a sample actually costs before any retention
// or segment-size default is trusted. The numbers the design sketch carried were arithmetic on a
// guess, and this repo has retracted a scaling claim made from a benchmark that was too small and
// too warm to separate its terms -- so the measurement is a test, it builds its records through
// the REAL sampler rather than hand-writing them, and it reports rather than asserts wherever the
// honest answer is "it depends on the workload".
//
// These three are SCALE-GATED (HTCONDORDB_SCALE=1) and do not run in CI. Not because they are
// slow -- though under -race they are, 13x -- but because every number they produce depends on
// having production's shape: enough records to fill many segments, and enough concurrently
// running jobs that a segment holds one sample each from thousands of DIFFERENT ones. Shrink
// either and the measurement stops describing any deployment, which is worse than not running
// it. The structural guard that CAN run cheaply -- is the record still columnar, is anything
// escaping to row form, did the rate columns make the schema -- lives in
// metrics_composition_test.go and runs on every CI build.

// scaleSize is the population. The JOB COUNT is the same at both sizes and the sample count is
// what shrinks, which is deliberate: on a real AP every running job is sampled in the same round,
// so a segment holds one sample each from thousands of DIFFERENT jobs. Shrinking the job count
// instead would put a job's consecutive samples in the same segment, where the strings dedupe and
// the counters delta-compress -- measuring a locality production never has, and calibrating the
// regression guard below against a number no deployment will see.
func scaleSize(t *testing.T) (jobs, perJob int) {
	t.Helper()
	if os.Getenv("HTCONDORDB_SCALE") == "" {
		t.Skip("set HTCONDORDB_SCALE=1: this measurement is only meaningful at production size " +
			"(see metrics_composition_test.go for the guard that runs in CI)")
	}
	return 20000, 24
}

// genJobAd builds a job ad with the shape and value distributions a real running job has, since
// compression is entirely a function of those. Owners and hosts repeat (they are the grouping
// dimensions); the counters climb monotonically per job; memory ratchets.
type jobGen struct {
	rnd      *rand.Rand
	owners   []string
	hosts    []string
	projects []string
}

func newJobGen(seed int64) *jobGen {
	g := &jobGen{rnd: rand.New(rand.NewSource(seed))}
	for i := 0; i < 50; i++ {
		g.owners = append(g.owners, fmt.Sprintf("user%03d", i))
	}
	for i := 0; i < 2000; i++ {
		g.hosts = append(g.hosts, fmt.Sprintf("slot1_%d@e%04d.chtc.wisc.edu", 1+i%8, i))
	}
	g.projects = []string{"CMS", "ATLAS", "IceCube", "LIGO", "Chemistry", "Ecology"}
	return g
}

// jobState is one job's evolving counters across its run.
type jobState struct {
	cluster, proc int64
	owner, host   string
	project       string
	reqMem        int64
	reqCPU        int64
	cores         float64
	startedAt     int64

	// container and gpu mark the job shapes that carry attributes MOST jobs do not: NetworkIn/Out
	// are only populated for container universes, and the GPU metrics only where the pool runs a
	// GPU monitor. They are what makes a real pool's population heterogeneous, and the reason a
	// single flat schema is the wrong model for it.
	container bool
	gpu       bool

	netIn, netOut float64
	gpuAvg        float64

	userCPU, sysCPU float64
	memMB           int64
	rssKB           int64
	blockRead       int64
	blockWrite      int64
	blockReads      int64
	instructions    int64
	bytesSent       float64
	bytesRecvd      float64
}

func (g *jobGen) newJob(i int) *jobState {
	reqCPU := int64(1 + g.rnd.Intn(8))
	return &jobState{
		// A fifth of the pool in containers, a seventh on GPUs -- both plausible, and both well
		// under the 90% presence a field needs to enter the BASE schema, so these attributes
		// cannot be carried there however common they feel.
		container: i%5 == 0,
		gpu:       i%7 == 0,
		cluster:   int64(100000 + i/10),
		proc:      int64(i % 10),
		owner:     g.owners[g.rnd.Intn(len(g.owners))],
		host:      g.hosts[g.rnd.Intn(len(g.hosts))],
		project:   g.projects[g.rnd.Intn(len(g.projects))],
		reqMem:    []int64{2048, 4096, 8192, 16384, 32768}[g.rnd.Intn(5)],
		reqCPU:    reqCPU,
		cores:     float64(reqCPU) * (0.3 + g.rnd.Float64()*0.7),
		startedAt: base + int64(g.rnd.Intn(3600)),
		bytesSent: float64(g.rnd.Intn(1 << 20)),
	}
}

// advance moves one job's counters forward by dt seconds and renders the job ad the sampler would
// read out of the mirror at that moment.
func (g *jobGen) advance(j *jobState, at int64, dt float64) *classad.ClassAd {
	j.userCPU += j.cores * dt * 0.9
	j.sysCPU += j.cores * dt * 0.1
	// Memory ratchets: it climbs for a while and then holds, which is what a high-water mark does.
	if grown := int64(float64(j.reqMem) * (0.2 + g.rnd.Float64()*0.6)); grown > j.memMB {
		j.memMB = grown
	}
	j.rssKB = j.memMB * 1024
	j.blockRead += int64(g.rnd.Intn(64 << 20))
	j.blockWrite += int64(g.rnd.Intn(16 << 20))
	j.blockReads += int64(g.rnd.Intn(5000))
	j.instructions += int64(g.rnd.Intn(1 << 30))
	j.bytesRecvd += float64(g.rnd.Intn(1 << 16))

	ad := classad.New()
	set := func(n string, v any) { _ = ad.Set(n, v) }
	set("ClusterId", j.cluster)
	set("ProcId", j.proc)
	set("GlobalJobId", "ap2001.chtc.wisc.edu#"+strconv.FormatInt(j.cluster, 10)+"."+
		strconv.FormatInt(j.proc, 10)+"#"+strconv.FormatInt(j.startedAt, 10))
	set("Owner", j.owner)
	set("User", j.owner+"@chtc.wisc.edu")
	set("RemoteHost", j.host)
	set("ProjectName", j.project)
	set("JobStatus", int64(2))
	set("JobUniverse", int64(5))
	set("NumShadowStarts", int64(1))
	set("NumJobStarts", int64(1))
	set("JobCurrentStartExecutingDate", j.startedAt)
	set("RequestCpus", j.reqCPU)
	set("RequestMemory", j.reqMem)
	set("RequestDisk", int64(10<<20))
	set("StatsLastUpdateTimeStarter", at)
	set("StatsLifetimeStarter", at-j.startedAt)
	set("RemoteUserCpu", j.userCPU)
	set("RemoteSysCpu", j.sysCPU)
	set("CumulativeRemoteUserCpu", j.userCPU)
	set("CumulativeRemoteSysCpu", j.sysCPU)
	set("MemoryUsage", j.memMB)
	set("ResidentSetSize", j.rssKB)
	set("ImageSize", j.rssKB+4096)
	set("DiskUsage", int64(1<<20))
	set("BlockReadBytes", j.blockRead)
	set("BlockWriteBytes", j.blockWrite)
	set("BlockReads", j.blockReads)
	set("BlockWrites", j.blockReads/3)
	set("JobCpuInstructions", j.instructions)
	set("BytesSent", j.bytesSent)
	set("BytesRecvd", j.bytesRecvd)
	set("IOWait", g.rnd.Float64()*0.2)
	set("CumulativeSuspensionTime", int64(0))
	set("TotalSuspensions", int64(0))
	set("CommittedTime", at-j.startedAt)

	// The heterogeneous tail. Each shape's attributes always appear TOGETHER and only on its own
	// jobs, which is exactly the co-occurrence a secondary (group) schema exists to capture.
	if j.container {
		j.netIn += g.rnd.Float64() * 8
		j.netOut += g.rnd.Float64() * 2
		set("NetworkIn", j.netIn)
		set("NetworkOut", j.netOut)
	}
	if j.gpu {
		j.gpuAvg = 0.4 + g.rnd.Float64()*0.6
		set("GPUsAverageUsage", j.gpuAvg)
		set("GPUsMemoryUsage", int64(4096+g.rnd.Intn(8192)))
	}
	return ad
}

// buildPopulation appends jobs*perJob samples through the real sampler and returns how long the
// ingest took. interval is the spacing between a job's samples.
func buildPopulation(t *testing.T, arch *db.ArchiveTable, jobs, perJob int, interval int64) time.Duration {
	t.Helper()
	took, _ := buildPopulationStats(t, arch, jobs, perJob, interval)
	return took
}

// buildPopulationBaselines is buildPopulation, returning how many of the appended samples carried
// no derived rates -- the quantity that decides whether the rate columns enter the schema at all.
func buildPopulationBaselines(t *testing.T, arch *db.ArchiveTable, jobs, perJob int, interval int64) int {
	t.Helper()
	_, baselines := buildPopulationStats(t, arch, jobs, perJob, interval)
	return baselines
}

func buildPopulationStats(t *testing.T, arch *db.ArchiveTable, jobs, perJob int, interval int64) (time.Duration, int) {
	t.Helper()
	g := newJobGen(1)
	states := make([]*jobState, jobs)
	for i := range states {
		states[i] = g.newJob(i)
	}
	m := newJobMetrics(JobMetricsConfig{Archive: arch, Attrs: []string{"ProjectName"}})
	// The sampler's recovery dedup would issue one archive query per record here. It exists for
	// the log-replay path, not for a fresh archive; turn it off so the measurement times ingest.
	m.dedup = false

	start := time.Now()
	baselines := 0
	for s := 0; s < perJob; s++ {
		at := base + int64(s+1)*interval
		for _, j := range states {
			ad := g.advance(j, at, float64(interval))
			key := strconv.FormatInt(j.cluster, 10) + "." + strconv.FormatInt(j.proc, 10)
			var upd []prevUpdate
			rec := m.build(key, triggerPeriodic, ad, 1, &upd)
			m.commitPending(upd)
			if rec != nil {
				if v, _ := rec.EvaluateAttrBool(SampleBaselineAttr); v {
					baselines++
				}
				if err := arch.Append(rec); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
		}
	}
	return time.Since(start), baselines
}

// TestJobMetricsRecordSize is the Phase 0 number every retention default depends on: how many
// bytes one sample actually costs on disk. UsedBytes, not file size -- a segment is preallocated
// to its configured size, so comparing files would flatter the result.
func TestJobMetricsRecordSize(t *testing.T) {
	jobs, perJob := scaleSize(t)
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable("job_metrics", db.ArchiveConfig{
		// The shipped configuration: no segment-size override. See TestJobMetricsSegmentSizeAB
		// for why -- the library default measured best, and a smaller segment measured 1.5x worse.
		CategoricalAttrs: JobMetricsCategoricalAttrs,
		ValueAttrs:       JobMetricsValueAttrs,
		ZoneAttrs:        JobMetricsZoneAttrs,
	})
	if err != nil {
		t.Fatal(err)
	}

	took := buildPopulation(t, arch, jobs, perJob, 900)
	n := arch.Count()
	if n != jobs*perJob {
		t.Fatalf("archive holds %d records, want %d", n, jobs*perJob)
	}
	raw := arch.Stats()
	rawPerRec := float64(raw.UsedBytes) / float64(n)
	t.Logf("BEFORE maintenance: records=%d segments=%d used=%s per_record=%.1fB ingest=%s (%.0f rec/s)",
		n, raw.Segments, humanBytes(raw.UsedBytes), rawPerRec, took.Round(time.Millisecond),
		float64(n)/took.Seconds())

	// Freshly appended records are stored in ROW form. The per-segment columnar accelerator --
	// which for a columnar-native build is also the storage, not a second copy -- is built by the
	// maintenance pass, which htcondordb runs with ArchiveSchemaScanHotTopN set by default
	// (server.defaultMaintainOptions). Measuring only the pre-maintenance number would report a
	// size no steady-state deployment ever has.
	arch.Reindex()
	if !arch.BuildAndEnableSchemaScan(2000, 32) {
		t.Fatal("the columnar accelerator did not build -- the steady-state size below would be wrong")
	}
	info := arch.SchemaScanInfo()
	st := arch.Stats()
	perRec := float64(st.UsedBytes) / float64(n)
	sc := arch.SidecarSizes()
	t.Logf("AFTER maintenance:  segments=%d used=%s per_record=%.1fB (%.1fx smaller) "+
		"schema_fields=%d coverage=%d/%d sidecar=%+v",
		st.Segments, humanBytes(st.UsedBytes), perRec, rawPerRec/perRec,
		info.SchemaFields, info.CoveredSegments, info.SealedSegments, sc)

	// What this costs a real AP, spelled out so the number is actionable rather than trivia.
	// Event-driven sampling adds to the 900s floor in proportion to how eventful the workload is,
	// so treat this as a lower bound on a busy pool.
	const running = 20000
	perDay := float64(running) * (86400.0 / 900.0) * perRec
	t.Logf("projection: %d running jobs at the 900s floor = %s/day, %s for 30 days",
		running, humanBytes(int64(perDay)), humanBytes(int64(perDay*30)))

	// Regression guard, not a target: comfortably above the measured steady-state value, so a
	// change that makes a sample much more expensive (an un-columnarized attribute, a nested ad
	// slipping into the projection, the accelerator silently declining to build, a segment-size
	// default that starves compression) fails rather than quietly multiplying the retention bill.
	const ceiling = 450
	if perRec > ceiling {
		t.Errorf("%.1f bytes/record exceeds the %dB ceiling -- a sample got much more expensive",
			perRec, ceiling)
	}
}

// TestJobMetricsDashboardQuery proves the read pattern the table was indexed for: a panel asks
// for a recent window, and the zone map on SampleTime has to drop the segments outside it. If it
// does not, every refresh scans the whole table and the retention story is the only thing keeping
// the dashboard usable.
func TestJobMetricsDashboardQuery(t *testing.T) {
	jobs, perJob := scaleSize(t)
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable("job_metrics", db.ArchiveConfig{
		// The shipped configuration: no segment-size override. See TestJobMetricsSegmentSizeAB
		// for why -- the library default measured best, and a smaller segment measured 1.5x worse.
		CategoricalAttrs: JobMetricsCategoricalAttrs,
		ValueAttrs:       JobMetricsValueAttrs,
		ZoneAttrs:        JobMetricsZoneAttrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	buildPopulation(t, arch, jobs, perJob, 900)

	// The default panel range: the newest quarter of the series. Expressed as a fraction so the
	// zone map stays load-bearing at either test size -- a window covering everything would prune
	// nothing however well it worked.
	since := recentWindow(perJob)
	var stats collections.ScanStats
	start := time.Now()
	rows, err := arch.AggregateColsStats(
		fmt.Sprintf("SampleTime >= %d", since),
		[]db.GroupCol{{Attr: "SampleTime", BucketWidth: 900}, {Attr: "Owner"}},
		[]db.AggSpec{{Func: db.AggAvg, Arg: "CpuUtil"}, {Func: db.AggCount, Arg: "*"}},
		&stats)
	took := time.Since(start)
	if err != nil {
		t.Fatalf("dashboard aggregate: %v", err)
	}
	t.Logf("last-hour panel: %d groups in %s; segments total=%d pruned=%d scanned=%d; records visited=%d matched=%d",
		len(rows), took.Round(time.Microsecond), stats.SegmentsTotal, stats.SegmentsPruned,
		stats.SegmentsScanned, stats.RecordsVisited, stats.RowsMatched)

	if len(rows) == 0 {
		t.Fatal("the panel query returned nothing -- the measurement below would be vacuous")
	}
	// The point of zone-mapping SampleTime. With several segments and a window covering a small
	// fraction of the table, most segments must be dropped without reading a record.
	if stats.SegmentsTotal > 2 && stats.SegmentsPruned == 0 {
		t.Errorf("no segments pruned out of %d: a time-range panel is scanning the whole table, "+
			"so the zone map on %s is not doing its job", stats.SegmentsTotal, SampleTimeAttr)
	}
}

// TestJobMetricsSegmentSizeAB is the measurement that overturned a shipped default. job_metrics
// originally sealed at 2 MiB instead of the archive's 8 MiB, on the argument that the ACTIVE
// segment carries no sidecar and is rescanned in full, so a dashboard reading the newest data
// wants that window short. Both halves of that argument turned out to be wrong:
//
//   - The read cost is a wash. At 480k records the last-hour panel took 686ms at 2 MiB and 718ms
//     at 8 MiB -- a 4% difference, in favour of the smaller segment, against a 1.5x storage
//     penalty. Not a trade worth making.
//   - Storage is NOT monotone in segment size, and 2 MiB is on the wrong side of the curve.
//     Measured at 20k jobs (240k records), bytes per record after the columnar build:
//     2 MiB -> 425, 8 MiB -> 281, 32 MiB -> 328, 64 MiB -> 424.
//
// So the library default is the right one and job_metrics no longer overrides it. The U-shape is
// not explained here -- it is not the locality story it first looked like, since the 64 MiB arm
// spans four sampling rounds and is as bad as the 2 MiB arm that spans an eighth of one. Recorded
// as a measurement, not a theory.
//
// Skipped unless HTCONDORDB_SCALE is set: it builds the population four times.
func TestJobMetricsSegmentSizeAB(t *testing.T) {
	jobs, perJob := scaleSize(t)
	sizes := []int{2 << 20, 8 << 20, 32 << 20, 64 << 20}
	best, bestSize := 0.0, 0
	for _, seg := range sizes {
		cat, err := db.OpenCatalog(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		arch, err := cat.CreateArchiveTable("job_metrics", db.ArchiveConfig{
			SegmentSize:      seg,
			CategoricalAttrs: JobMetricsCategoricalAttrs,
			ValueAttrs:       JobMetricsValueAttrs,
			ZoneAttrs:        JobMetricsZoneAttrs,
		})
		if err != nil {
			t.Fatal(err)
		}
		buildPopulation(t, arch, jobs, perJob, 900)
		arch.Reindex()
		arch.BuildAndEnableSchemaScan(2000, 32)

		since := recentWindow(perJob)
		var stats collections.ScanStats
		start := time.Now()
		_, err = arch.AggregateColsStats(fmt.Sprintf("SampleTime >= %d", since),
			[]db.GroupCol{{Attr: "Owner"}},
			[]db.AggSpec{{Func: db.AggAvg, Arg: "CpuUtil"}}, &stats)
		took := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		st := arch.Stats()
		perRec := float64(st.UsedBytes) / float64(arch.Count())
		t.Logf("segment=%2dMiB  %.0fB/rec  last-hour panel %s  segments=%d pruned=%d scanned=%d",
			seg>>20, perRec, took.Round(time.Millisecond), stats.SegmentsTotal,
			stats.SegmentsPruned, stats.SegmentsScanned)
		if best == 0 || perRec < best {
			best, bestSize = perRec, seg
		}
		cat.Close()
	}
	// The shipped configuration leaves the segment size alone, so the library default had better
	// still be the best of the arms. If some other size wins on a future classad, this fails and
	// the default gets revisited on evidence rather than drifting.
	if bestSize != 8<<20 {
		t.Errorf("%dMiB segments measured best (%.0fB/rec), but job_metrics ships the library "+
			"default of 8MiB -- revisit HTCONDORDB_JOB_METRICS_SEGMENT_SIZE", bestSize>>20, best)
	}
}

// recentWindow is the start of the newest quarter of a perJob-round series -- the "recent range"
// a dashboard panel asks for, expressed so it stays a fraction of the data at either test size.
func recentWindow(perJob int) int64 {
	rounds := perJob / 4
	if rounds < 1 {
		rounds = 1
	}
	return base + int64(perJob-rounds)*900
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGT"[exp])
}
