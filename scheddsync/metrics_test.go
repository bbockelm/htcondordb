package scheddsync

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// base is a fixed epoch the sample timeline is built on, so every expected rate is exact
// arithmetic rather than something derived from the wall clock.
const base int64 = 1700000000

// newMetricsArchive opens a job_metrics archive with the same index set the daemon creates.
func newMetricsArchive(t *testing.T) (*db.ArchiveTable, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, err := cat.CreateArchiveTable(DefaultJobMetricsTable, db.ArchiveConfig{
		ValueAttrs: JobMetricsValueAttrs,
		ZoneAttrs:  JobMetricsZoneAttrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	return a, func() { cat.Close() }
}

// samples reads every record out of the archive, oldest-first (the archive reads newest-first, so
// the slice is reversed) -- which is the order a time series is reasoned about.
func samples(t *testing.T, a *db.ArchiveTable) []*classad.ClassAd {
	t.Helper()
	seq, err := a.Query("true")
	if err != nil {
		t.Fatalf("query samples: %v", err)
	}
	var out []*classad.ClassAd
	for ad := range seq {
		out = append(out, ad)
	}
	slices.Reverse(out)
	return out
}

func trig(t *testing.T, ad *classad.ClassAd) string {
	t.Helper()
	v, _ := ad.EvaluateAttrString(SampleTriggerAttr)
	return v
}

// num fetches a numeric attribute, failing the test when it is absent -- so a missing derived
// column is a loud failure rather than a silent zero.
func num(t *testing.T, ad *classad.ClassAd, attr string) float64 {
	t.Helper()
	v, ok := ad.EvaluateAttrNumber(attr)
	if !ok {
		t.Fatalf("sample is missing %s: %s", attr, ad.String())
	}
	return v
}

func absent(t *testing.T, ad *classad.ClassAd, attr string) {
	t.Helper()
	if _, ok := ad.EvaluateAttrNumber(attr); ok {
		t.Fatalf("expected %s to be undefined, got %s", attr, ad.String())
	}
}

func closeTo(t *testing.T, got, want float64, what string) {
	t.Helper()
	if diff := got - want; diff > 1e-6 || diff < -1e-6 {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// pinClock freezes nowFn, so the ingest-clock fallback is deterministic.
func pinClock(t *testing.T, at int64) {
	t.Helper()
	real := nowFn
	t.Cleanup(func() { nowFn = real })
	nowFn = func() time.Time { return time.Unix(at, 0) }
}

// newSampledSync wires a JobSync with sampling on, over a fresh log.
func newSampledSync(t *testing.T, logPath string, cfg JobMetricsConfig) (*JobSync, *db.ArchiveTable) {
	t.Helper()
	arch, cleanup := newMetricsArchive(t)
	t.Cleanup(cleanup)
	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { target.Close() })
	cfg.Archive = arch
	return NewJobSync(target, JobSyncConfig{Filename: logPath, Metrics: cfg}), arch
}

// submitted is the log a job's submission writes: no NumShadowStarts, so the job has never run.
const submitted = `105
101 1.0 Job Machine
103 1.0 ClusterId 1
103 1.0 ProcId 0
103 1.0 Owner "alice"
103 1.0 JobStatus 1
103 1.0 RequestMemory 2048
103 1.0 RequestCpus 2
103 1.0 RequestDisk 1048576
106
`

// spawned is the schedd's own commit when it starts a shadow for the job.
func spawned(run int) string {
	return fmt.Sprintf(`105
103 1.0 JobStatus 2
103 1.0 NumShadowStarts %d
103 1.0 JobCurrentStartExecutingDate %d
106
`, run, base)
}

// starterUpdate is one periodic shadow flush: the starter's stats clock plus the counters it
// gathered, in the shape RemoteResource::updateFromStarter writes them.
func starterUpdate(at int64, userCPU, sysCPU float64, memMB, blockRead int64) string {
	return fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu %.1f
103 1.0 RemoteSysCpu %.1f
103 1.0 MemoryUsage %d
103 1.0 ResidentSetSize %d
103 1.0 BlockReadBytes %d
106
`, at, userCPU, sysCPU, memMB, memMB*1024, blockRead)
}

// TestJobMetricsSeries is the end-to-end proof: a job's life through the real tailer produces a
// sample per shadow commit, with the rates derived from consecutive observations, and the run's
// LAST sample is its endpoint (which is what lets a resource plot avoid epoch_history entirely).
func TestJobMetricsSeries(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	ctx := context.Background()

	if err := s.Poll(ctx); err != nil {
		t.Fatalf("poll after submit: %v", err)
	}
	// A submitted job has never run, so it has no resource usage to record. Without this gate a
	// large submit would write one useless record per job.
	if got := samples(t, arch); len(got) != 0 {
		t.Fatalf("submit produced %d samples, want 0: %s", len(got), got[0].String())
	}

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 570, 30, 1024, 1048576))
	appendFile(t, logPath, starterUpdate(base+600, 1140, 60, 1536, 3145728))
	// Terminal: the shadow's last update carries the final counters in the same commit as the
	// exit status, because HTCondor's queue-update whitelist is shared by every update type.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 1235.0
103 1.0 RemoteSysCpu 65.0
103 1.0 JobStatus 4
103 1.0 ExitCode 0
106
`, base+650))
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("poll after run: %v", err)
	}

	got := samples(t, arch)
	if len(got) != 4 {
		var kinds []string
		for _, ad := range got {
			kinds = append(kinds, trig(t, ad))
		}
		t.Fatalf("got %d samples %v, want 4 (spawn, two periodic, terminal)", len(got), kinds)
	}

	// 1. The spawn commit: the run's first observation. No predecessor, so no rates.
	if k := trig(t, got[0]); k != "status" {
		t.Fatalf("sample 0 trigger = %q, want status", k)
	}
	if v, _ := got[0].EvaluateAttrBool(SampleBaselineAttr); !v {
		t.Fatalf("sample 0 should be a baseline: %s", got[0].String())
	}
	absent(t, got[0], "CpuUtil")
	if v, _ := got[0].EvaluateAttrInt(RunInstanceAttr); v != 0 {
		t.Fatalf("sample 0 RunInstanceID = %d, want 0 (NumShadowStarts-1)", v)
	}
	if v, _ := got[0].EvaluateAttrString("Owner"); v != "alice" {
		t.Fatalf("sample 0 Owner = %q, want alice -- context must be read back from the row, "+
			"not from the transaction that triggered the sample", v)
	}

	// 2. First periodic. Its predecessor is the spawn commit, which carried no counters at all
	// (the starter had not reported yet), so there is nothing to differentiate FROM: this is
	// still a baseline. That is the deliberate choice in sumDelta -- reading "absent" as zero
	// would measure the first rate across an interval that includes input file transfer, when
	// the job was not running. The interval and the ratios, which need no predecessor, are
	// present either way.
	if k := trig(t, got[1]); k != "periodic" {
		t.Fatalf("sample 1 trigger = %q, want periodic", k)
	}
	closeTo(t, num(t, got[1], SampleTimeAttr), float64(base+300), "sample 1 SampleTime")
	closeTo(t, num(t, got[1], "MemUtil"), 1024.0/2048.0, "sample 1 MemUtil")
	absent(t, got[1], "CpuUtil")
	// No SampleInterval either, and for a second reason worth keeping separate from the missing
	// counters: its predecessor is the spawn sample, whose SampleTime came from the INGEST clock
	// (the starter had not reported yet) while this one comes from the starter's. The difference
	// between those two is AP/EP skew, not elapsed time, so there is no interval to report.
	absent(t, got[1], SampleIntervalAttr)
	if n := s.metrics.status().ClockMix; n != 1 {
		t.Errorf("ClockMix = %d, want 1 (the spawn sample's clock differs from the starter's)", n)
	}
	if v, _ := got[1].EvaluateAttrBool(SampleBaselineAttr); !v {
		t.Fatalf("sample 1 should be a baseline (no counters in its predecessor): %s", got[1].String())
	}

	// 3. Second periodic: the first real interval, between two starter reports. 600 CPU-seconds
	// (1140+60 less 570+30) over 300s = 2 cores.
	closeTo(t, num(t, got[2], SampleIntervalAttr), 300, "sample 2 SampleInterval")
	closeTo(t, num(t, got[2], "CpuUtil"), 2.0, "sample 2 CpuUtil")
	closeTo(t, num(t, got[2], "BlockReadRate"), 2097152.0/300.0, "sample 2 BlockReadRate")
	if v, _ := got[2].EvaluateAttrBool(SampleBaselineAttr); v {
		t.Fatal("sample 2 derived rates, so it is not a baseline")
	}

	// 4. The endpoint. This is the §4.3 guarantee: the run's final numbers are in job_metrics,
	// so a resource plot needs no epoch_history lookup to find where the run ended.
	last := got[3]
	if k := trig(t, last); k != "terminal" {
		t.Fatalf("last sample trigger = %q, want terminal", k)
	}
	closeTo(t, num(t, last, "RemoteUserCpu"), 1235, "terminal RemoteUserCpu")
	closeTo(t, num(t, last, "CpuUtil"), 2.0, "terminal CpuUtil")
	if v, _ := last.EvaluateAttrInt("JobStatus"); v != 4 {
		t.Fatalf("terminal JobStatus = %d, want 4", v)
	}
	// The predecessor cache is dropped at the endpoint, so a re-run starts a fresh series.
	if _, ok := s.metrics.prev["1.0"]; ok {
		t.Fatal("terminal sample should drop the job's predecessor entry")
	}
}

// TestJobMetricsRunBoundary is the counter-reset rule: HTCondor zeroes RemoteUserCpu at run start
// but carries BytesSent across runs, so a delta spanning a run boundary is meaningless for the
// first and correct for the second. Getting this wrong produces a huge negative or a huge positive
// spike at every rerun.
func TestJobMetricsRunBoundary(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	ctx := context.Background()

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 600.0
103 1.0 RemoteSysCpu 0.0
103 1.0 BytesSent 1000.0
106
`, base+300))
	// Evicted, then restarted: NumShadowStarts goes to 2 (RunInstanceID 1) and the shadow zeroed
	// RemoteUserCpu for the new run -- but by the time the new run is first OBSERVED it has
	// already burned 900 CPU-seconds, MORE than run 0's 600.
	//
	// That is the shape that makes the run-boundary rule load-bearing. When a new run is first
	// seen with a SMALLER counter the delta is negative and the backwards-counter guard catches
	// it anyway; here the delta is +300 and looks like a perfectly ordinary 1-core interval. Only
	// knowing that the counter restarted rejects it.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 JobStatus 2
103 1.0 NumShadowStarts 2
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 900.0
103 1.0 RemoteSysCpu 0.0
103 1.0 BytesSent 1300.0
106
`, base+600))
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("poll: %v", err)
	}

	got := samples(t, arch)
	if len(got) != 3 {
		t.Fatalf("got %d samples, want 3", len(got))
	}
	seam := got[2]
	if v, _ := seam.EvaluateAttrInt(RunInstanceAttr); v != 1 {
		t.Fatalf("seam sample RunInstanceID = %d, want 1", v)
	}
	// Differentiating across the seam would report a plausible 1.0 cores ((900-600)/300) for an
	// interval that spans two different runs' counters. Refusing to is the whole point, and no
	// other guard would catch it -- the delta is positive.
	absent(t, seam, "CpuUtil")
	if n := s.metrics.status().Resets; n != 0 {
		t.Fatalf("Resets = %d, want 0: the delta here is POSITIVE, so if the run-boundary rule "+
			"were removed the backwards-counter guard would not catch it", n)
	}
	// And NO rate crosses the seam, not even for the counters that accumulate across runs.
	// BytesSent does not restart, so its delta is sound -- but the DENOMINATOR is not: the
	// interval from a run's last sample to the next run's first spans however long the job sat
	// idle in between. Deriving one produced a plausible, unmarked, wrong number.
	absent(t, seam, "BytesSentRate")
	absent(t, seam, SampleIntervalAttr)
	if v, _ := seam.EvaluateAttrBool(SampleBaselineAttr); !v {
		t.Error("a seam sample derives nothing, so it is a baseline")
	}
}

// TestJobMetricsContextOnlyCommit: a commit that moves only context attributes -- the schedd
// rewriting bookkeeping on a running job -- is not an observation and must produce no sample.
// Without this rule the submit of a large cluster would write one record per job (each blocked
// only by the executing gate), and every schedd touch of a running job would add a duplicate
// point to its series.
func TestJobMetricsContextOnlyCommit(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 600, 0, 1024, 4096))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	before := len(samples(t, arch))

	// The job is RUNNING (so the executing gate lets it through) and these are all attributes the
	// sample carries -- but none of them is an observation of resource usage.
	appendFile(t, logPath, `105
103 1.0 RequestMemory 4096
103 1.0 Owner "alice"
103 1.0 GlobalJobId "ap.example.edu#1.0#1700000000"
106
`)
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll after context-only commit: %v", err)
	}
	if got := len(samples(t, arch)); got != before {
		t.Fatalf("a context-only commit added %d samples, want 0", got-before)
	}
}

// TestJobMetricsAbortDropsPending: a read pass that fails after noting some jobs must not carry
// those notes into the next pass, where they would sample a row that the re-read did not touch.
func TestJobMetricsAbortDropsPending(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted+spawned(1))
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	before := len(samples(t, arch))

	// Simulate a pass that noted a job and then failed before committing.
	s.metrics.note("1.0", "MemoryUsage")
	if len(s.metrics.pending) != 1 {
		t.Fatal("note should have recorded the job")
	}
	s.abort()
	if len(s.metrics.pending) != 0 {
		t.Fatal("abort must drop the noted set")
	}
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll after abort: %v", err)
	}
	if got := len(samples(t, arch)); got != before {
		t.Fatalf("an aborted pass produced %d samples, want 0", got-before)
	}
}

// TestJobMetricsCounterReset covers the guard for a reset the sampler does not model: within one
// run, a counter that goes backwards must produce no rate (not a negative one) and must be
// counted, because a climbing count is how we learn the reset table in metrics.go is incomplete.
func TestJobMetricsCounterReset(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 600, 0, 1024, 4096))
	appendFile(t, logPath, starterUpdate(base+600, 10, 0, 1024, 8192))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	got := samples(t, arch)
	after := got[len(got)-1]
	absent(t, after, "CpuUtil")
	// The block counter kept climbing, so its rate is unaffected -- the guard is per rate, not
	// per sample.
	closeTo(t, num(t, after, "BlockReadRate"), 4096.0/300.0, "BlockReadRate across a CPU reset")
	if n := s.metrics.status().Resets; n != 1 {
		t.Fatalf("Resets = %d, want 1", n)
	}
}

// TestJobMetricsSuspensionNetsOut: a suspended job's clock advances while its counters stand
// still. Dividing by raw elapsed time would report a spuriously low rate instead of the real one
// over the time the job was actually running.
func TestJobMetricsSuspensionNetsOut(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 0.0
103 1.0 RemoteSysCpu 0.0
103 1.0 CumulativeSuspensionTime 0
106
`, base))
	// 300s of wall clock, 200s of it suspended, 100 CPU-seconds burned: 1 core over the 100s the
	// job was actually running, not 1/3 of a core over the whole interval.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 100.0
103 1.0 RemoteSysCpu 0.0
103 1.0 CumulativeSuspensionTime 200
106
`, base+300))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	got := samples(t, arch)
	last := got[len(got)-1]
	closeTo(t, num(t, last, SampleIntervalAttr), 100, "SampleInterval net of suspension")
	closeTo(t, num(t, last, "CpuUtil"), 1.0, "CpuUtil net of suspension")
}

// TestJobMetricsThrottle: MinInterval drops redundant periodic samples but must never drop a
// state change or a run endpoint, because the endpoint guarantee and a plot's transition
// annotations depend on those.
func TestJobMetricsThrottle(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{MinInterval: 600 * time.Second})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+60, 60, 0, 1024, 4096))   // 61s after spawn: dropped
	appendFile(t, logPath, starterUpdate(base+120, 120, 0, 1024, 8192)) // 121s after spawn: dropped
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 180.0
103 1.0 JobStatus 7
106
`, base+180)) // suspended: a state change, so it survives the throttle
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 200.0
103 1.0 JobStatus 4
103 1.0 ExitCode 0
106
`, base+200)) // terminal: always survives
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	var kinds []string
	for _, ad := range samples(t, arch) {
		kinds = append(kinds, trig(t, ad))
	}
	want := []string{"status", "status", "terminal"}
	if !slices.Equal(kinds, want) {
		t.Fatalf("triggers = %v, want %v (the two periodic samples throttled away, the "+
			"state change and the endpoint kept)", kinds, want)
	}
	if n := s.metrics.status().Throttled; n != 2 {
		t.Fatalf("Throttled = %d, want 2", n)
	}
}

// TestJobMetricsExtraAttrs: an admin-named attribute is copied onto every sample, and moving it
// on its own is enough to produce one -- which is what turns a chirp-published job metric into a
// plottable series.
func TestJobMetricsExtraAttrs(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{Attrs: []string{"ProjectName", "TrainingLoss"}})

	appendFile(t, logPath, `105
103 1.0 ProjectName "cms"
106
`)
	appendFile(t, logPath, spawned(1))
	// A lone chirp write, in its own transaction -- which is how condor_chirp reaches the queue.
	appendFile(t, logPath, `105
103 1.0 TrainingLoss 0.42
106
`)
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	got := samples(t, arch)
	if len(got) != 2 {
		t.Fatalf("got %d samples, want 2 (spawn, chirp)", len(got))
	}
	if k := trig(t, got[1]); k != "chirp" {
		t.Fatalf("second sample trigger = %q, want chirp", k)
	}
	closeTo(t, num(t, got[1], "TrainingLoss"), 0.42, "TrainingLoss")
	// Context set before the run still reaches the sample: it is read back off the row, not
	// taken from the triggering transaction.
	for _, ad := range got {
		if v, _ := ad.EvaluateAttrString("ProjectName"); v != "cms" {
			t.Fatalf("ProjectName = %q, want cms on every sample", v)
		}
	}
}

// TestJobMetricsCustomResourceUsage: GPU and other custom machine-resource metrics are discovered
// by the same *Usage / *AverageUsage suffix rule HTCondor's own shadow uses, because which of
// them exist depends on how the pool configured its startd cron.
func TestJobMetricsCustomResourceUsage(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 GPUsAverageUsage 0.5
103 1.0 GPUsMemoryUsage 8192
106
`, base+300))
	// GPUsAverageUsage is a LIFETIME average, so the interval value has to be un-averaged:
	// 0.5 over 300s then 0.75 over 600s is 0.75*600 - 0.5*300 = 300 GPU-seconds in 300s = 1.0.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 GPUsAverageUsage 0.75
103 1.0 GPUsMemoryUsage 9216
106
`, base+600))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	got := samples(t, arch)
	last := got[len(got)-1]
	// Never named in sampleAttrs -- picked up purely by the suffix rule.
	closeTo(t, num(t, last, "GPUsMemoryUsage"), 9216, "GPUsMemoryUsage")
	closeTo(t, num(t, last, "GpuUtil"), 1.0, "GpuUtil de-averaged")
}

// TestJobMetricsRestartDedup: a restart re-applies the log from the last durable position, which
// re-produces samples that were already appended. Appends are not idempotent, so the replayed
// prefix must be skipped -- and the check must turn itself off once past it, or every later
// sample would pay for a query.
func TestJobMetricsRestartDedup(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	arch, cleanup := newMetricsArchive(t)
	defer cleanup()

	run := func() *JobSync {
		target, err := db.Open("")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { target.Close() })
		return NewJobSync(target, JobSyncConfig{
			Filename: logPath,
			Metrics:  JobMetricsConfig{Archive: arch},
		})
	}

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 600, 0, 1024, 4096))
	first := run()
	if err := first.Poll(context.Background()); err != nil {
		t.Fatalf("first poll: %v", err)
	}
	before := len(samples(t, arch))
	if before != 2 {
		t.Fatalf("first pass wrote %d samples, want 2", before)
	}

	// A second syncer with no persisted position replays the whole log from the start -- the
	// worst case of what a crash-and-restart re-reads.
	second := run()
	appendFile(t, logPath, starterUpdate(base+600, 1200, 0, 1536, 8192))
	if err := second.Poll(context.Background()); err != nil {
		t.Fatalf("second poll: %v", err)
	}

	got := samples(t, arch)
	if len(got) != 3 {
		var kinds []string
		for _, ad := range got {
			ts, _ := ad.EvaluateAttrInt(SampleTimeAttr)
			kinds = append(kinds, fmt.Sprintf("%s@%d", trig(t, ad), ts-base))
		}
		t.Fatalf("after replay got %d samples %v, want 3 (the two replayed ones deduped)",
			len(got), kinds)
	}
	if n := second.metrics.status().Deduped; n != 2 {
		t.Fatalf("Deduped = %d, want 2", n)
	}
	// Once a sample is new, every later one is too, so the per-sample query stops.
	if second.metrics.dedup {
		t.Fatal("dedup should be off after the first new sample")
	}
}

// TestJobMetricsReconcileSuppressed: a compaction makes the tailer replay the whole log through
// reconcileReload. That is not new information about any running job, so it must not emit a
// sample per job -- which on a busy AP would be tens of thousands of duplicate records per
// compaction.
func TestJobMetricsReconcileSuppressed(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 600, 0, 1024, 4096))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	before := len(samples(t, arch))

	// Compaction: the schedd rewrites the log with the same jobs in their current state.
	writeFile(t, logPath, submitted+spawned(1)+starterUpdate(base+300, 600, 0, 1024, 4096))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll after compaction: %v", err)
	}
	if n := s.metrics.status().Appended; int(n) != before {
		t.Fatalf("a compaction added %d samples, want 0 -- a reconcile reload is a replay, "+
			"not an observation", int(n)-before)
	}
	if got := len(samples(t, arch)); got != before {
		t.Fatalf("archive holds %d samples after compaction, want %d", got, before)
	}
}

// TestTriggerPrecedence pins the classification: when one transaction carries several kinds of
// change, the sample is named by the most significant one, because that is what a reader filters
// on. Terminal outranks everything, and a context attribute triggers nothing at all.
func TestTriggerPrecedence(t *testing.T) {
	for _, tc := range []struct {
		attr string
		want sampleTrigger
	}{
		{"ExitCode", triggerTerminal},
		{"HoldReasonCode", triggerTerminal},
		{"LastVacateTime", triggerTerminal},
		{"JobCheckpointNumber", triggerCheckpoint},
		{"JobStatus", triggerStatus},
		{"NumShadowStarts", triggerStatus},
		{"StatsLastUpdateTimeStarter", triggerPeriodic},
		{"MemoryUsage", triggerUpdate},
		{"memoryusage", triggerUpdate}, // ClassAd names are case-insensitive
		{"RemoteUserCpu", triggerUpdate},
		{"Owner", triggerNone},         // context: carried, never a trigger
		{"RequestMemory", triggerNone}, // ditto -- otherwise every submit samples
		{"Arguments", triggerNone},
	} {
		if got := triggerFor(tc.attr); got != tc.want {
			t.Errorf("triggerFor(%q) = %v, want %v", tc.attr, got, tc.want)
		}
	}
	// Precedence is the ordering of the constants, which is what note() relies on when one
	// transaction carries several kinds of change.
	ordered := []sampleTrigger{
		triggerChirp, triggerUpdate, triggerPeriodic, triggerStatus, triggerCheckpoint, triggerTerminal,
	}
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1] >= ordered[i] {
			t.Fatalf("sampleTrigger constants are not in precedence order: %v >= %v",
				ordered[i-1], ordered[i])
		}
	}
}

// TestJobMetricsDisabled: with no archive configured the sampler is nil and every call site
// tolerates it, so the feature costs nothing when off.
func TestJobMetricsDisabled(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted+spawned(1)+starterUpdate(base+300, 600, 0, 1024, 4096))
	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	if s.metrics != nil {
		t.Fatal("sampler should be nil when no archive is configured")
	}
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if (s.Status().Metrics != MetricsStatus{}) {
		t.Fatal("disabled sampler should report zero counters")
	}
}

// TestJobMetricsTerminalRequiresTerminalStatus: an attribute name is a hint about a commit, not a
// verdict on the job. A commit that touches a terminal-looking attribute while the job is still
// running must not be labelled the run's endpoint -- a consumer filtering to terminal samples
// would find one mid-run, and the guarantee that a run's LAST sample is its end would be false.
//
// Found by running against a real schedd, which wrote exactly this shape.
func TestJobMetricsTerminalRequiresTerminalStatus(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	// A terminal-looking attribute on a job that is still Running.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 60.0
103 1.0 LastVacateTime %d
106
`, base+60, base+60))
	// And the real ending.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 RemoteUserCpu 120.0
103 1.0 JobStatus 4
103 1.0 ExitCode 0
106
`, base+120))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	got := samples(t, arch)
	var kinds []string
	for _, ad := range got {
		kinds = append(kinds, trig(t, ad))
	}
	if n := len(got); n < 2 || kinds[n-1] != "terminal" {
		t.Fatalf("triggers = %v; the LAST sample must be the terminal one", kinds)
	}
	for i, k := range kinds[:len(kinds)-1] {
		if k == "terminal" {
			t.Errorf("sample %d of %d is labelled terminal while the job was still running "+
				"(all triggers: %v)", i, len(kinds), kinds)
		}
	}
}

// TestCompletionDateIsNotATerminalTrigger: the schedd writes CompletionDate = 0 at SUBMIT, so
// treating the attribute's presence as a run ending misclassifies every submitted job.
func TestCompletionDateIsNotATerminalTrigger(t *testing.T) {
	if got := triggerFor("CompletionDate"); got == triggerTerminal {
		t.Error("CompletionDate classified as terminal; it is written as 0 at submit, so only " +
			"its VALUE is a signal and JobStatus/ExitCode carry the real transition")
	}
}

// TestJobMetricsCurrentRSS covers the one true memory gauge in the table: HTCondor's
// ResidentSetSize is a high-water mark, so a working-set curve needs the un-maxed value. The
// attribute does not exist in HTCondor yet, which is precisely why this matters -- a pool that
// gains it must start recording without any change here, and the KiB-over-MiB ratio must not be
// wrong by a factor of 1024.
func TestJobMetricsCurrentRSS(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted) // RequestMemory = 2048 MiB
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	// 1 GiB resident (KiB), and a high-water mark that is already higher -- the shape a job has
	// after it has freed memory, and the whole reason the gauge is worth recording.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 CurrentResidentSetSize 1048576
103 1.0 ResidentSetSize 2097152
103 1.0 MemoryUsage 2048
106
`, base+300))
	// Now it drops. ResidentSetSize cannot follow it down; the gauge must.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 CurrentResidentSetSize 524288
103 1.0 ResidentSetSize 2097152
103 1.0 MemoryUsage 2048
106
`, base+600))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	got := samples(t, arch)
	last := got[len(got)-1]
	closeTo(t, num(t, last, "CurrentResidentSetSize"), 524288, "CurrentResidentSetSize")
	// 524288 KiB = 512 MiB against a 2048 MiB request = 0.25. Getting the scale wrong gives 256.
	closeTo(t, num(t, last, "CurrentMemUtil"), 0.25, "CurrentMemUtil (KiB numerator, MiB request)")
	// The high-water ratio stays pinned at 1.0, which is exactly the difference being recorded.
	closeTo(t, num(t, last, "MemUtil"), 1.0, "MemUtil")
	if prev := num(t, got[len(got)-2], "CurrentMemUtil"); prev <= num(t, last, "CurrentMemUtil") {
		t.Errorf("CurrentMemUtil did not decrease (%v then %v); a gauge that cannot go down is "+
			"just the high-water mark again", prev, num(t, last, "CurrentMemUtil"))
	}
}

// TestJobMetricsRunStartOrdering: HTCondor starts a run in TWO transactions and bumps
// NumShadowStarts in the SECOND one. Scheduler::start_std calls mark_serial_job_running() --
// its own transaction, which sets JobStatus=2 and does not touch NumShadowStarts -- before
// add_shadow_rec(), which is where the counter moves. So on the JobStatus=2 commit the row still
// reports the id of the run that just ENDED.
//
// Without a record of which runs have already ended, that commit produces an extra sample for the
// finished run, AFTER its own endpoint, carrying the previous run's counters and clock. It also
// resurrects the predecessor across the boundary, which is what let a later rate span the idle gap.
func TestJobMetricsRunStartOrdering(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 300, 0, 1024, 4096))
	// Evicted back to idle: the run's endpoint.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 JobStatus 1
103 1.0 LastVacateTime %d
106
`, base+400))
	// Re-run, in HTCondor's real order: JobStatus=2 FIRST, NumShadowStarts a transaction later.
	appendFile(t, logPath, `105
103 1.0 JobStatus 2
106
`)
	appendFile(t, logPath, `105
103 1.0 NumShadowStarts 2
106
`)
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	got := samples(t, arch)
	var runs []int64
	var kinds []string
	for _, ad := range got {
		r, _ := ad.EvaluateAttrInt(RunInstanceAttr)
		runs = append(runs, r)
		kinds = append(kinds, trig(t, ad))
	}
	// Exactly one endpoint for run 0, and nothing for run 0 after it.
	lastRun0 := -1
	for i, r := range runs {
		if r == 0 {
			lastRun0 = i
		}
	}
	if lastRun0 < 0 || kinds[lastRun0] != "terminal" {
		t.Fatalf("run 0's last sample is %v (all runs=%v kinds=%v), want its terminal one",
			kinds[lastRun0], runs, kinds)
	}
	terminals := 0
	for i, k := range kinds {
		if k == "terminal" && runs[i] == 0 {
			terminals++
		}
	}
	if terminals != 1 {
		t.Errorf("%d terminal samples for run 0, want exactly 1 (runs=%v kinds=%v)",
			terminals, runs, kinds)
	}
	if n := s.metrics.status().AfterEnd; n == 0 {
		t.Error("AfterEnd = 0: the pre-bump window should have been observed and suppressed")
	}
}

// TestJobMetricsHeldWhileIdle: condor_hold and condor_rm set HoldReason/RemoveReason together
// with JobStatus in ONE transaction, and they do it for idle jobs too. A job that ran earlier
// still carries NumShadowStarts, so a terminal-looking attribute alone used to produce a SECOND
// endpoint for a run that had already ended -- same SampleTime, same run id, old counters.
func TestJobMetricsHeldWhileIdle(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 300, 0, 1024, 4096))
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 JobStatus 1
103 1.0 LastVacateTime %d
106
`, base+400))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	before := len(samples(t, arch))

	// Now held while idle, long after the run ended.
	appendFile(t, logPath, `105
103 1.0 JobStatus 5
103 1.0 HoldReasonCode 3
103 1.0 HoldReason "periodic hold"
106
`)
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll after hold: %v", err)
	}
	if got := len(samples(t, arch)); got != before {
		t.Errorf("holding an idle job added %d samples, want 0: its run already had an endpoint",
			got-before)
	}
}

// TestJobMetricsCacheDeferredToCommit: the predecessor cache must not move for a transaction that
// did not commit. The terminal case is the damaging one -- the update DELETES the predecessor, and
// a re-applied pass then finds none, does not recognise the endpoint, and emits nothing for it.
func TestJobMetricsCacheDeferredToCommit(t *testing.T) {
	pinClock(t, base-1)
	arch, cleanup := newMetricsArchive(t)
	defer cleanup()
	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	m := newJobMetrics(JobMetricsConfig{Archive: arch})

	job := classad.New()
	for k, v := range map[string]any{
		"ClusterId": int64(1), "ProcId": int64(0), "Owner": "alice", "JobStatus": int64(2),
		"NumShadowStarts": int64(1), "RequestMemory": int64(2048),
		"RemoteUserCpu": 300.0, statsClockAttr: base + 300,
	} {
		_ = job.Set(k, v)
	}
	if err := target.Put("1.0", job); err != nil {
		t.Fatal(err)
	}

	// A running sample, collected but NOT committed.
	m.note("1.0", "RemoteUserCpu")
	s1, upd1 := m.collect(target, 1)
	if len(s1) != 1 || len(upd1) != 1 {
		t.Fatalf("collect = %d samples %d updates, want 1/1", len(s1), len(upd1))
	}
	if _, ok := m.prev["1.0"]; ok {
		t.Fatal("collect advanced the predecessor cache before the commit landed")
	}
	m.commitPending(upd1)
	if _, ok := m.prev["1.0"]; !ok {
		t.Fatal("commitPending did not advance the cache")
	}

	// Now the run ends. Collect the endpoint, then DISCARD it as a failed commit would.
	_ = job.Set("JobStatus", int64(1))
	_ = job.Set("LastVacateTime", base+400)
	if err := target.Put("1.0", job); err != nil {
		t.Fatal(err)
	}
	m.note("1.0", "LastVacateTime")
	sTerm, _ := m.collect(target, 1)
	if len(sTerm) != 1 || trig(t, sTerm[0]) != "terminal" {
		t.Fatalf("expected one terminal sample, got %d", len(sTerm))
	}
	// Commit failed: apply nothing.
	if _, ok := m.prev["1.0"]; !ok {
		t.Fatal("the discarded terminal collect deleted the predecessor anyway -- a re-applied " +
			"pass would find none and the run would lose its endpoint")
	}
	// Re-apply the identical entry, as the rewind does. The endpoint must come back.
	m.note("1.0", "LastVacateTime")
	sAgain, updAgain := m.collect(target, 1)
	if len(sAgain) != 1 || trig(t, sAgain[0]) != "terminal" {
		t.Fatalf("re-applying after a failed commit produced %d samples, want the endpoint back",
			len(sAgain))
	}
	m.commitPending(updAgain)
	if _, ok := m.prev["1.0"]; ok {
		t.Error("the committed terminal update should have dropped the predecessor")
	}
}

// TestJobMetricsPerInputResetGuard: guarding only the SUM of a multi-input rate lets one input's
// reset hide inside another's progress. CpuUtil sums user+sys, so 100->160 with 50->0 sums to a
// plausible +10 and was written as 0.1 cores against a true value of at least 0.6 -- neither
// suppressed nor counted, which is exactly what the reset counter exists to surface.
func TestJobMetricsPerInputResetGuard(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+100, 100, 50, 1024, 4096))
	appendFile(t, logPath, starterUpdate(base+200, 160, 0, 1024, 8192))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	last := samples(t, arch)[len(samples(t, arch))-1]
	absent(t, last, "CpuUtil")
	if n := s.metrics.status().Resets; n != 1 {
		t.Errorf("Resets = %d, want 1: one input went backwards and must be counted", n)
	}
	// The unaffected rate still derives -- the guard is per rate, not per sample.
	closeTo(t, num(t, last, "BlockReadRate"), 4096.0/100.0, "BlockReadRate")
}

// TestJobMetricsSampleCarriesKey: ClusterId reaches a proc row only through cluster-ad chaining,
// so an unchained row yields a sample with no ClusterId and no GlobalJobId. In an append-only
// table that record is unattributable and unrepairable. The storage key is in hand when the
// sample is built, so it goes on every one.
func TestJobMetricsSampleCarriesKey(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 300, 0, 1024, 4096))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	for i, ad := range samples(t, arch) {
		k, ok := ad.EvaluateAttrString(KeyAttr)
		if !ok || k != "1.0" {
			t.Errorf("sample %d has %s=%q (present=%v), want \"1.0\"", i, KeyAttr, k, ok)
		}
	}
}

// TestJobMetricsUnappliedErrorStillSamples: db.UnappliedError means the transaction COMMITTED and
// a few keys could not be composed -- Poll treats it as progress and checkpoints past it. Dropping
// the batch's samples on that error lost them permanently, for writes that had landed.
func TestJobMetricsUnappliedErrorStillSamples(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, starterUpdate(base+300, 300, 0, 1024, 4096))
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	before := len(samples(t, arch))
	if before == 0 {
		t.Fatal("no samples before the injection; the rest would be vacuous")
	}

	// Inject on the NEXT commit only. Injecting on every one aborts the pass at the submit
	// transaction, before the job is even running.
	appendFile(t, logPath, starterUpdate(base+600, 600, 0, 1024, 8192))
	real := commitErrorHook
	t.Cleanup(func() { commitErrorHook = real })
	// Only on a commit that actually carries a sample: injecting on every one aborts the pass at
	// an earlier, sample-less transaction and the interesting commit is never reached.
	commitErrorHook = func(err error, samples int) error {
		if samples == 0 {
			return err
		}
		return &db.UnappliedError{Keys: []string{"99.0"}}
	}
	_ = s.Poll(context.Background()) // Poll surfaces the error; the commit still landed

	if got := len(samples(t, arch)); got == before {
		t.Error("an UnappliedError discarded the sample from a batch that committed; Poll " +
			"treats that error as progress and checkpoints past it, so it is lost for good")
	}
}

// TestJobMetricsTerminalNeedsPredecessorAfterRestart isolates the rule that a terminal-LOOKING
// attribute is not by itself evidence a run ended here.
//
// Within one process the `ended` map also suppresses this, so the two gates cover for each other
// and neither is tested alone. After a restart both the predecessor cache and `ended` are empty --
// they are in-memory caches, rebuilt from the stream -- and only this rule is left. A job that ran
// earlier still carries NumShadowStarts, and condor_hold sets HoldReason and JobStatus in one
// transaction, so without it a fresh daemon writes a bogus endpoint for a run it never observed.
func TestJobMetricsTerminalNeedsPredecessorAfterRestart(t *testing.T) {
	pinClock(t, base-1)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	// The log as a restarted daemon reads it: a job that has already run, now idle, then held.
	writeFile(t, logPath, submitted+`105
103 1.0 NumShadowStarts 1
103 1.0 JobStatus 1
103 1.0 RemoteUserCpu 300.0
106
105
103 1.0 JobStatus 5
103 1.0 HoldReasonCode 3
103 1.0 HoldReason "periodic hold"
106
`)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if got := samples(t, arch); len(got) != 0 {
		t.Errorf("a fresh sampler wrote %d samples for a run it never observed (first: %s)",
			len(got), got[0].String())
	}
}
