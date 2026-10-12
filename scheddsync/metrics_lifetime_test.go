package scheddsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// lifetimeUpdate is a shadow flush from a default starter: the counters and the starter's uptime
// (StatsLifetimeStarter), but no StatsLastUpdateTimeStarter, which a starter publishes only at
// the verbose statistics level.
func lifetimeUpdate(lifetime int64, userCPU, sysCPU float64) string {
	return fmt.Sprintf(`105
103 1.0 StatsLifetimeStarter %d
103 1.0 RecentStatsLifetimeStarter 1200
103 1.0 RemoteUserCpu %.1f
103 1.0 RemoteSysCpu %.1f
106
`, lifetime, userCPU, sysCPU)
}

// setIngest moves the sampler's ingest clock: when the tailer reads the next commits.
func setIngest(s *JobSync, at int64) { s.metrics.now = func() time.Time { return time.Unix(at, 0) } }

func clockOf(ad *classad.ClassAd) string {
	v, _ := ad.EvaluateAttrString(SampleIntervalClockAttr)
	return v
}

// The 276-core point from ap2001 (job 12112856.0, run 7, a 1-core self-checkpointing job): two
// starter updates the tailer read one second apart. On the ingest clock 276 CPU-seconds over
// "1 s" was 276 cores; the starter's uptime says the updates were 280 s apart, which is 0.99.
func TestJobMetricsLifetimeClock(t *testing.T) {
	pinClock(t, base)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	ctx := context.Background()

	appendFile(t, logPath, spawned(8))
	appendFile(t, logPath, lifetimeUpdate(3315, 1049, 2221))
	setIngest(s, base+537)
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	appendFile(t, logPath, lifetimeUpdate(3595, 1133, 2413))
	setIngest(s, base+538)
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}

	got := samples(t, arch)
	if len(got) != 3 {
		t.Fatalf("got %d samples, want 3", len(got))
	}
	// The spawn commit carries no starter stats (ingest clock); the first update has the uptime
	// counter. Two clocks: no interval.
	absent(t, got[1], SampleIntervalAttr)
	if n := s.metrics.status().ClockMix; n != 1 {
		t.Errorf("ClockMix = %d, want 1", n)
	}
	last := got[2]
	closeTo(t, num(t, last, SampleIntervalAttr), 280, "SampleInterval")
	closeTo(t, num(t, last, "CpuUtil"), 276.0/280.0, "CpuUtil")
	if c := clockOf(last); c != "lifetime" {
		t.Errorf("SampleIntervalClock = %q, want lifetime", c)
	}
	// The time axis stays on the ingest clock: the uptime counter has no exact origin in the
	// job ad to anchor it on.
	closeTo(t, num(t, last, SampleTimeAttr), float64(base+538), "SampleTime")
	if v, _ := last.EvaluateAttrBool("SampleTimeFromIngest"); !v {
		t.Error("SampleTimeFromIngest should be set")
	}
}

// A self-checkpointing job commits the same starter update several times -- the checkpoint and
// its follow-ups -- and the tailer reads them all in one go. The uptime does not move between
// them, so they derive nothing; the next real update is measured from the last of them.
func TestJobMetricsLifetimeCheckpointCommits(t *testing.T) {
	pinClock(t, base)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	ctx := context.Background()

	appendFile(t, logPath, spawned(8))
	appendFile(t, logPath, lifetimeUpdate(3315, 1049, 2221))
	appendFile(t, logPath, lifetimeUpdate(3595, 1133, 2413))
	// The checkpoint, then a follow-up, both under the update's uptime.
	appendFile(t, logPath, `105
103 1.0 JobCheckpointNumber 3
103 1.0 CommittedTime 3595
106
`)
	appendFile(t, logPath, `105
103 1.0 LastCheckpointTime 1700003595
103 1.0 CommittedSlotTime 3595
106
`)
	appendFile(t, logPath, lifetimeUpdate(3895, 1233, 2611))
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}

	got := samples(t, arch)
	if len(got) != 6 {
		t.Fatalf("got %d samples, want 6", len(got))
	}
	closeTo(t, num(t, got[2], "CpuUtil"), 276.0/280.0, "update CpuUtil")
	for _, i := range []int{3, 4} {
		if k := trig(t, got[i]); k != "checkpoint" && k != "update" {
			t.Errorf("sample %d trigger = %q", i, k)
		}
		absent(t, got[i], SampleIntervalAttr)
		absent(t, got[i], "CpuUtil")
	}
	if n := s.metrics.status().Stalled; n != 2 {
		t.Errorf("Stalled = %d, want 2 (the two re-commits of one starter update)", n)
	}
	// 298 CPU-seconds over the 300 s since the update the checkpoint commits repeated.
	closeTo(t, num(t, got[5], SampleIntervalAttr), 300, "next SampleInterval")
	closeTo(t, num(t, got[5], "CpuUtil"), 298.0/300.0, "next CpuUtil")
}

// A new run's first observation still carries the previous starter's uptime. Measured from it,
// the first interval of the run spans two starters: here the previous run lasted 100 s and the
// new starter first reports at 300 s, which would read as 200 s. No rate is derived until the
// run has an uptime of its own.
func TestJobMetricsLifetimeRunBoundary(t *testing.T) {
	pinClock(t, base)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	ctx := context.Background()

	appendFile(t, logPath, spawned(1))
	appendFile(t, logPath, lifetimeUpdate(40, 30, 0))
	appendFile(t, logPath, lifetimeUpdate(100, 90, 0))
	// Evicted; the next run starts. The shadow zeroes the run's counters, but the job ad still
	// holds the old starter's uptime.
	appendFile(t, logPath, `105
103 1.0 JobStatus 1
106
`)
	appendFile(t, logPath, `105
103 1.0 JobStatus 2
103 1.0 NumShadowStarts 2
103 1.0 RemoteUserCpu 0.0
103 1.0 RemoteSysCpu 0.0
106
`)
	// The new run begins executing: still the old starter's uptime, carried into another sample.
	appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 JobCurrentStartExecutingDate %d
106
`, base+700))
	appendFile(t, logPath, lifetimeUpdate(300, 290, 0))
	appendFile(t, logPath, lifetimeUpdate(600, 580, 0))
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}

	got := samples(t, arch)
	if len(got) != 8 {
		t.Fatalf("got %d samples, want 8", len(got))
	}
	closeTo(t, num(t, got[2], "CpuUtil"), 1.0, "run 0 CpuUtil")
	seam, first, second := got[4], got[6], got[7]
	absent(t, got[5], "CpuUtil")
	if v, _ := seam.EvaluateAttrInt(RunInstanceAttr); v != 1 {
		t.Fatalf("seam RunInstanceID = %d", v)
	}
	absent(t, seam, "CpuUtil")
	// 290 CPU-seconds over a "200 s" spanning two starters would be 1.45 cores.
	absent(t, first, "CpuUtil")
	absent(t, first, SampleIntervalAttr)
	closeTo(t, num(t, second, SampleIntervalAttr), 300, "second SampleInterval")
	closeTo(t, num(t, second, "CpuUtil"), 290.0/300.0, "second CpuUtil")
}

// A run whose updates switch clocks mid-run -- the starter's timestamp, then only its uptime --
// derives nothing across the switch, and each interval says which clock it was measured on.
func TestJobMetricsLifetimeClockMix(t *testing.T) {
	pinClock(t, base)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, submitted)
	s, arch := newSampledSync(t, logPath, JobMetricsConfig{})
	ctx := context.Background()

	appendFile(t, logPath, spawned(1))
	for _, at := range []int64{300, 600} {
		appendFile(t, logPath, fmt.Sprintf(`105
103 1.0 StatsLastUpdateTimeStarter %d
103 1.0 StatsLifetimeStarter %d
103 1.0 RemoteUserCpu %d.0
103 1.0 RemoteSysCpu 0.0
106
`, base+at, at, at))
	}
	// The verbose statistics go away (a reconfigured starter). The shadow deletes an attribute
	// an update no longer carries, so the timestamp leaves the job ad with this update.
	appendFile(t, logPath, `105
104 1.0 StatsLastUpdateTimeStarter
103 1.0 StatsLifetimeStarter 900
103 1.0 RemoteUserCpu 900.0
103 1.0 RemoteSysCpu 0.0
106
`)
	appendFile(t, logPath, lifetimeUpdate(1200, 1200, 0))
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	got := samples(t, arch)
	if len(got) != 5 {
		t.Fatalf("got %d samples, want 5", len(got))
	}
	// spawn (ingest) -> 300 (starter): mixed. 300 -> 600: the starter's clock, though the uptime
	// is there too -- the timestamp is the first choice.
	absent(t, got[1], "CpuUtil")
	closeTo(t, num(t, got[2], "CpuUtil"), 1.0, "starter-clock CpuUtil")
	if c := clockOf(got[2]); c != "starter" {
		t.Errorf("sample 2 SampleIntervalClock = %q, want starter", c)
	}
	// 600 (starter) -> 900 (uptime only): mixed, so nothing -- not 300 CPU-seconds over the
	// difference of a timestamp and an uptime.
	absent(t, got[3], "CpuUtil")
	absent(t, got[3], SampleIntervalAttr)
	if n := s.metrics.status().ClockMix; n != 2 {
		t.Errorf("ClockMix = %d, want 2 (the spawn's and the switch's)", n)
	}
	// 900 -> 1200, both on the uptime.
	closeTo(t, num(t, got[4], "CpuUtil"), 1.0, "lifetime CpuUtil")
	if c := clockOf(got[4]); c != "lifetime" {
		t.Errorf("sample 4 SampleIntervalClock = %q, want lifetime", c)
	}
}

// suspectIngestIntervals is the WHERE clause docs/schedd-sync.md gives for finding the rate rows
// already stored whose interval came from the ingest clock and was short: the ones the 276-core
// point is one of.
const suspectIngestIntervals = `SampleInterval < 60 && (SampleIntervalClock =?= "ingest" || ` +
	`(SampleIntervalClock =?= undefined && SampleTimeFromIngest =?= true))`

// The documented query selects old and new ingest-clock rows with a short interval, and nothing
// measured on the starter's clocks.
func TestSuspectIngestIntervalsQuery(t *testing.T) {
	doc, err := os.ReadFile("../docs/schedd-sync.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), suspectIngestIntervals) {
		t.Fatal("docs/schedd-sync.md no longer carries the query this test checks")
	}
	expr, err := classad.ParseExpr(suspectIngestIntervals)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		row  string
		want bool
	}{
		{"SampleInterval = 1; SampleTimeFromIngest = true; CpuUtil = 276", true}, // before this change
		{"SampleInterval = 899; SampleTimeFromIngest = true", false},             // long enough to be sound
		{"SampleInterval = 1; CpuUtil = 2", false},                               // starter clock, before
		{`SampleInterval = 30; SampleIntervalClock = "ingest"`, true},            // the fallback, now
		{`SampleInterval = 30; SampleIntervalClock = "lifetime"; SampleTimeFromIngest = true`, false},
		{`SampleInterval = 30; SampleIntervalClock = "starter"`, false},
		{"SampleTimeFromIngest = true; SampleBaseline = true", false}, // no interval, no rate
	} {
		ad, err := classad.Parse("[" + c.row + "]")
		if err != nil {
			t.Fatal(err)
		}
		got, _ := expr.Eval(ad).BoolValue()
		if got != c.want {
			t.Errorf("%s: %v, want %v", c.row, got, c.want)
		}
	}
}
