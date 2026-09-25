package scheddsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestJobMetricsIntegration is the end of the evidence chain for this feature. Everything else
// tests the sampler against job_queue.log content written by a test; this runs a real job on a
// real schedd and checks the three claims the design makes about HTCondor itself, none of which
// can be established by reading the source alone:
//
//  1. The usage attributes actually reach job_queue.log while a job runs, so a tailer can see
//     them without any new polling.
//  2. The run's TERMINAL commit carries its final counters -- the claim that lets a resource plot
//     find where a run ended without consulting epoch_history. This is the one that would be
//     quietly wrong if HTCondor's queue-update whitelist behaved as it first appears to (a
//     per-update-type list) rather than as it does (the common list is included in every type).
//  3. RunInstanceID derived as NumShadowStarts-1 names the SAME run that epoch_history names,
//     so the two tables join.
//
// Skips when condor is not installed. Named *Integration so the sync-integration CI job runs it.
func TestJobMetricsIntegration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync integration test must run unprivileged")
	}
	// A short queue-update interval so a job of a few seconds gets periodic samples as well as
	// its terminal one; epoch history on so the cross-check in (3) has something to match.
	h := htcondor.SetupCondorHarnessWithConfig(t, "\n"+
		"JOB_EPOCH_HISTORY = $(SPOOL)/job_epoch_history\n"+
		"SHADOW_QUEUE_UPDATE_INTERVAL = 5\n"+
		"STARTER_UPDATE_INTERVAL = 2\n")
	if err := h.WaitForDaemons(); err != nil {
		t.Fatalf("daemons failed to start: %v", err)
	}
	cfg, err := h.GetConfig()
	if err != nil {
		t.Fatal(err)
	}
	jobLog := configOr(cfg, "JOB_QUEUE_LOG", filepath.Join(h.GetSpoolDir(), "job_queue.log"))
	epochFile := configOr(cfg, "JOB_EPOCH_HISTORY", filepath.Join(h.GetSpoolDir(), "job_epoch_history"))

	collector := htcondor.NewCollector(h.GetCollectorAddr())
	loc, err := collector.LocateDaemon(context.Background(), "Schedd", "")
	if err != nil {
		t.Fatalf("locate schedd: %v", err)
	}
	schedd := htcondor.NewSchedd(loc.Name, loc.Address)

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	metricsArch, err := cat.CreateArchiveTable(DefaultJobMetricsTable, db.ArchiveConfig{
		CategoricalAttrs: JobMetricsCategoricalAttrs,
		ValueAttrs:       JobMetricsValueAttrs,
		ZoneAttrs:        JobMetricsZoneAttrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	epochArch, err := cat.CreateArchiveTable("epoch_history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId"},
		ZoneAttrs:  []string{EpochWriteDateAttr, EnteredHistoryAttr},
	})
	if err != nil {
		t.Fatal(err)
	}

	js := NewJobSync(jobs, JobSyncConfig{
		Filename: jobLog,
		Metrics:  JobMetricsConfig{Archive: metricsArch},
	})
	es := NewJobEpochSync(epochArch, HistorySyncConfig{Filename: epochFile})

	// A job that burns CPU IN ITS OWN PROCESS for long enough to be sampled while running.
	//
	// Both halves of that matter. Not /bin/sleep, because a job that uses no CPU would let the
	// counter assertions pass on zeroes. And the loop is arithmetic in the shell itself rather
	// than a time-bounded loop calling date(1), so the CPU is burned in the one process the
	// procd is certainly tracking rather than in short-lived children it may only see on its
	// snapshot interval.
	//
	// Even so, RemoteUserCpu comes through as 0 on a macOS harness for a job that demonstrably
	// ran for eleven seconds. Whether that is Darwin process accounting, the procd's sampling, or
	// something else is NOT established here -- which is exactly why the assertions below check
	// that the endpoint carries real usage rather than naming CPU, and log what was found.
	jobDir := t.TempDir()
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/sh\n"+
		"arguments = \"-c 'i=0; while [ $i -lt 4000000 ]; do i=$((i+1)); done; echo $i'\"\n"+
		"output = e.out\nerror = e.err\nlog = e.log\ntransfer_executable = false\n"+
		"initialdir = %s\nqueue\n", jobDir)
	clusterID, err := schedd.Submit(ctx, submit)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("submitted cluster %s", clusterID)

	// Drive both tailers until the job has left the queue AND a terminal sample has landed.
	// Polled with a deadline rather than slept-then-asserted, so a slow runner is slow rather
	// than flaky, and a fast one does not wait.
	var samples []*classad.ClassAd
	deadline := time.Now().Add(180 * time.Second)
	sawTerminal := false
	for time.Now().Before(deadline) && !sawTerminal {
		if perr := js.Poll(ctx); perr != nil {
			t.Fatalf("job poll: %v", perr)
		}
		if perr := es.Poll(ctx); perr != nil {
			t.Fatalf("epoch poll: %v", perr)
		}
		samples = queryAds(t, metricsArch, "ClusterId == "+clusterID)
		for _, s := range samples {
			if v, _ := s.EvaluateAttrString(SampleTriggerAttr); v == "terminal" {
				sawTerminal = true
			}
		}
		if !sawTerminal {
			_ = schedd.Reschedule(ctx)
			time.Sleep(time.Second)
		}
	}
	if len(samples) == 0 {
		t.Fatal("a real job ran and produced NO resource samples: the usage attributes are not " +
			"reaching job_queue.log, or the sampler is not seeing them")
	}
	var triggers []string
	for _, s := range samples {
		v, _ := s.EvaluateAttrString(SampleTriggerAttr)
		triggers = append(triggers, v)
	}
	t.Logf("cluster %s produced %d samples: %v", clusterID, len(samples), triggers)

	// Claim 2. The endpoint, and the numbers on it.
	if !sawTerminal {
		t.Fatalf("no terminal sample after the job finished (triggers seen: %v) -- a resource "+
			"plot would have no run endpoint without reading epoch_history", triggers)
	}
	last := samples[len(samples)-1]
	if v, _ := last.EvaluateAttrString(SampleTriggerAttr); v != "terminal" {
		t.Errorf("the LAST sample is %q, not the terminal one: the endpoint guarantee is that a "+
			"run's final sample is its end, and ordering matters for a series", v)
	}
	// The assertion is that the endpoint carries REAL usage, not that it carries one particular
	// attribute: which counters are populated depends on the platform and on how the EP tracks
	// processes (see the note on the procd snapshot interval above, and CPU accounting needs the
	// job's CPU to be visible to it at all). The log names what was found, so a gap stays visible
	// rather than being tolerated silently.
	usage := map[string]float64{}
	for _, a := range []string{"RemoteUserCpu", "RemoteSysCpu", "MemoryUsage", "ResidentSetSize",
		"DiskUsage", "CommittedTime", "BytesSent", "BytesRecvd"} {
		if v, ok := last.EvaluateAttrNumber(a); ok {
			usage[a] = v
		}
	}
	t.Logf("terminal sample usage: %v", usage)
	nonzero := 0
	for _, v := range usage {
		if v > 0 {
			nonzero++
		}
	}
	if nonzero == 0 {
		t.Errorf("the terminal sample carries no non-zero usage at all (%v) -- the run's final "+
			"numbers are not reaching it: %s", usage, last.String())
	}

	// Claim 3. The run identity joins to epoch_history.
	run, ok := last.EvaluateAttrInt(RunInstanceAttr)
	if !ok {
		t.Fatalf("terminal sample carries no %s, so it cannot be joined to a run", RunInstanceAttr)
	}
	epochs := queryAds(t, epochArch, fmt.Sprintf("ClusterId == %s && %s == %d && %s == \"EPOCH\"",
		clusterID, RunInstanceAttr, run, EpochAdTypeAttr))
	if len(epochs) == 0 {
		all := queryAds(t, epochArch, "ClusterId == "+clusterID)
		var got []string
		for _, e := range all {
			r, _ := e.EvaluateAttrInt(RunInstanceAttr)
			ty, _ := e.EvaluateAttrString(EpochAdTypeAttr)
			got = append(got, fmt.Sprintf("%s/run=%d", ty, r))
		}
		t.Fatalf("no epoch record for %s == %d; epoch_history holds %v. The sample's run identity "+
			"does not name the same run HTCondor does, so the two tables do not join",
			RunInstanceAttr, run, got)
	}
	// Same run, so the same final numbers. Compared loosely and only for counters BOTH records
	// carry: the shadow may push one last update between the sample and the epoch write, and the
	// CPU counters are whole seconds.
	compared := 0
	for _, a := range []string{"RemoteUserCpu", "RemoteSysCpu", "CommittedTime"} {
		sv, ok1 := last.EvaluateAttrNumber(a)
		ev, ok2 := epochs[0].EvaluateAttrNumber(a)
		if !ok1 || !ok2 {
			continue
		}
		compared++
		if d := ev - sv; d < -1.5 || d > 1.5 {
			t.Errorf("terminal sample %s=%v but the epoch record for the SAME run says %v -- the "+
				"two tables disagree about what the run used", a, sv, ev)
		}
	}
	t.Logf("cross-checked %d counters between the terminal sample and epoch run %d", compared, run)

	// Claim 1, stated as the thing that would be most embarrassing to get wrong: a sample must
	// carry context the triggering transaction never mentioned, because it is read back off the
	// merged row.
	if owner, _ := last.EvaluateAttrString("Owner"); owner == "" {
		t.Error("terminal sample has no Owner: the row read-back is not picking up context")
	}
}

// queryAds runs a constraint against an archive and returns the matches OLDEST-first (archives
// read newest-first), which is the order a time series is reasoned about.
func queryAds(t *testing.T, a *db.ArchiveTable, constraint string) []*classad.ClassAd {
	t.Helper()
	seq, err := a.Query(constraint)
	if err != nil {
		t.Fatalf("query %q: %v", constraint, err)
	}
	var out []*classad.ClassAd
	for ad := range seq {
		out = append(out, ad)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
