package scheddsync

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestQueueLogRestartRestore is the Level-2 round-trip proof: a REAL schedd must accept a
// job_queue.log reconstructed from the htcondordb mirror. It submits held jobs to a live
// schedd, removes one to open a cluster-id gap (so the header's NextClusterNum sits ahead of
// the highest surviving job), stops the schedd, mirrors its job_queue.log into the tables,
// reconstructs a fresh job_queue.log, overwrites the schedd's spool copy, and restarts the
// schedd. It then asserts condor_q reports the same surviving jobs -- and that a newly
// submitted job continues the cluster counter past the gap rather than reusing an id, which
// is only possible because the header ad was preserved and restored.
func TestQueueLogRestartRestoreIntegration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("must run unprivileged (the schedd's spool must not be read/written as root)")
	}
	// condor_off/condor_on are required to cycle the schedd under the master.
	for _, tool := range []string{"condor_off", "condor_on"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH", tool)
		}
	}

	h := htcondor.SetupCondorHarness(t) // skips if condor_master et al. are absent
	if err := h.WaitForDaemons(); err != nil {
		t.Fatalf("daemons failed to start: %v", err)
	}
	cfg, err := h.GetConfig()
	if err != nil {
		t.Fatalf("harness config: %v", err)
	}
	jobLog := configOr(cfg, "JOB_QUEUE_LOG", filepath.Join(h.GetSpoolDir(), "job_queue.log"))
	cfgFile := h.GetConfigFile()

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	collector := htcondor.NewCollector(h.GetCollectorAddr())
	schedd := locateSchedd(t, ctx, collector)

	// Submit three held jobs so they persist in the queue (status 5, never run/leave).
	const held = "universe = vanilla\nexecutable = /bin/sleep\narguments = 300\nhold = true\nqueue\n"
	var clusterIDs []int
	for i := 0; i < 3; i++ {
		id, err := schedd.Submit(ctx, held)
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		c, _ := strconv.Atoi(id)
		clusterIDs = append(clusterIDs, c)
	}
	sort.Ints(clusterIDs)
	maxCluster := clusterIDs[len(clusterIDs)-1]

	// Remove the highest cluster to open a gap: its id is now retired, but the schedd's
	// NextClusterNum counter (in the header ad) stays past it. A backup that loses the header
	// would recompute the counter from the surviving jobs and reuse the retired id.
	if _, err := schedd.RemoveJobsByID(ctx, []string{fmt.Sprintf("%d.0", maxCluster)}, "restore test gap"); err != nil {
		t.Fatalf("remove cluster %d: %v", maxCluster, err)
	}
	survivors := clusterIDs[:len(clusterIDs)-1]
	waitForJobKeys(t, ctx, schedd, jobKeys(survivors))

	// Snapshot the surviving queue for post-restore comparison.
	before := queryJobMap(t, ctx, schedd)

	// Stop the schedd; the master keeps it down until condor_on.
	runCondorTool(t, cfgFile, "condor_off", "-schedd")
	waitScheddDown(t, ctx, schedd)

	// Mirror the now-static job_queue.log into the five namespace tables.
	n := tables(t)
	js := NewJobSync(n.jobs, JobSyncConfig{
		Filename: jobLog, Users: n.users, Jobsets: n.jobsets, Clusters: n.clusters, Header: n.header,
		ClusterPrivate: n.clusterprivate, LogMeta: n.logmeta,
	})
	if err := js.Poll(ctx); err != nil {
		t.Fatalf("mirror job_queue.log: %v", err)
	}
	if n.jobs.Len() != len(survivors) {
		t.Fatalf("mirror holds %d jobs, want %d", n.jobs.Len(), len(survivors))
	}
	if _, ok := n.header.LookupClassAd("0.0"); !ok {
		t.Fatal("header ad 0.0 was not mirrored; the counter cannot be restored")
	}
	// The schedd creates a cluster-private ad ("C.-2") alongside every cluster, and every
	// job_queue.log starts with a 107 sequence header -- both must be captured for a faithful backup.
	if n.clusterprivate.Len() == 0 {
		t.Fatal("no cluster-private ads (C.-2) mirrored; the schedd creates one per cluster")
	}
	if _, ok := n.logmeta.LookupClassAd(LogMetaKey); !ok {
		t.Fatal("log sequence header (107) was not captured into logmeta")
	}

	// Reconstruct the log and atomically overwrite the schedd's spool copy.
	restore := jobLog + ".restore"
	if err := n.writer().WriteFile(restore); err != nil {
		t.Fatalf("reconstruct log: %v", err)
	}
	if err := os.Rename(restore, jobLog); err != nil {
		t.Fatalf("overwrite job_queue.log: %v", err)
	}

	// Restart the schedd; it must load the reconstructed log.
	runCondorTool(t, cfgFile, "condor_on", "-schedd")
	schedd = waitScheddUp(t, ctx, collector, h)

	// The surviving jobs must reappear with identical key attributes.
	after := queryJobMap(t, ctx, schedd)
	assertJobMapsEqual(t, before, after)

	// Header fidelity: a new submission must continue the counter past the retired id, proving
	// NextClusterNum survived the round-trip (no id reuse).
	newIDStr, err := schedd.Submit(ctx, held)
	if err != nil {
		t.Fatalf("post-restore submit: %v", err)
	}
	newID, _ := strconv.Atoi(newIDStr)
	if newID <= maxCluster {
		t.Fatalf("post-restore submit reused cluster id %d (<= retired %d); header counter was not restored", newID, maxCluster)
	}
	t.Logf("restored %d jobs; new submission got cluster %d (> retired %d) -- header counter preserved",
		len(survivors), newID, maxCluster)
}

// jobKeys returns the "C.0" key for each cluster id.
func jobKeys(clusters []int) []string {
	keys := make([]string, len(clusters))
	for i, c := range clusters {
		keys[i] = fmt.Sprintf("%d.0", c)
	}
	return keys
}

// locateSchedd finds the schedd via the collector, failing the test if it cannot.
func locateSchedd(t *testing.T, ctx context.Context, c *htcondor.Collector) *htcondor.Schedd {
	t.Helper()
	loc, err := c.LocateDaemon(ctx, "Schedd", "")
	if err != nil {
		t.Fatalf("locate schedd: %v", err)
	}
	return htcondor.NewSchedd(loc.Name, loc.Address)
}

// runCondorTool runs a condor admin tool against the harness config, failing on error.
func runCondorTool(t *testing.T, cfgFile, tool string, args ...string) {
	t.Helper()
	path, err := exec.LookPath(tool)
	if err != nil {
		t.Fatalf("%s not found: %v", tool, err)
	}
	cmd := exec.CommandContext(context.Background(), path, args...)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfgFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", tool, args, err, out)
	}
}

// waitScheddDown polls until the schedd stops answering pings (its command port is closed),
// so the job_queue.log is stable before it is read and overwritten.
func waitScheddDown(t *testing.T, ctx context.Context, schedd *htcondor.Schedd) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := schedd.Ping(pctx)
		cancel()
		if err != nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("schedd did not go down within 60s of condor_off")
}

// waitScheddUp polls the collector for a schedd that answers pings (its address changes across
// a restart), returning a client bound to the fresh address.
func waitScheddUp(t *testing.T, ctx context.Context, c *htcondor.Collector, h *htcondor.CondorTestHarness) *htcondor.Schedd {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if loc, err := c.LocateDaemon(ctx, "Schedd", ""); err == nil {
			s := htcondor.NewSchedd(loc.Name, loc.Address)
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			_, perr := s.Ping(pctx)
			cancel()
			if perr == nil {
				return s
			}
		}
		time.Sleep(1 * time.Second)
	}
	h.PrintScheddLog()
	t.Fatal("schedd did not come back within 90s of condor_on (it may have rejected the reconstructed log)")
	return nil
}

// waitForJobKeys polls condor_q until exactly the given job keys are present.
func waitForJobKeys(t *testing.T, ctx context.Context, schedd *htcondor.Schedd, want []string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		got := make([]string, 0)
		for k := range queryJobMap(t, ctx, schedd) {
			got = append(got, k)
		}
		sort.Strings(got)
		if equalStrings(got, want) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("queue did not settle to keys %v within 30s", want)
}

// queryJobMap returns the live jobs keyed "C.P" with the identity attributes we compare.
func queryJobMap(t *testing.T, ctx context.Context, schedd *htcondor.Schedd) map[string]*classad.ClassAd {
	t.Helper()
	ads, err := schedd.Query(ctx, "true", []string{"ClusterId", "ProcId", "Owner", "JobStatus", "Cmd"})
	if err != nil {
		t.Fatalf("condor_q: %v", err)
	}
	out := map[string]*classad.ClassAd{}
	for _, ad := range ads {
		c, _ := ad.EvaluateAttrInt("ClusterId")
		p, _ := ad.EvaluateAttrInt("ProcId")
		out[fmt.Sprintf("%d.%d", c, p)] = ad
	}
	return out
}

// assertJobMapsEqual checks two queue snapshots hold the same job keys with matching identity
// attributes (ClusterId, ProcId, Owner, JobStatus, Cmd).
func assertJobMapsEqual(t *testing.T, want, got map[string]*classad.ClassAd) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("job count %d -> %d after restore (keys before=%v after=%v)",
			len(want), len(got), mapKeys(want), mapKeys(got))
	}
	for k, a := range want {
		b, ok := got[k]
		if !ok {
			t.Fatalf("job %s missing after restore", k)
		}
		for _, attr := range []string{"ClusterId", "ProcId", "Owner", "JobStatus", "Cmd"} {
			av, _ := a.EvaluateAttrString(attr)
			bv, _ := b.EvaluateAttrString(attr)
			if av != bv {
				t.Errorf("job %s attr %s = %q after restore, want %q", k, attr, bv, av)
			}
		}
	}
}

func mapKeys(m map[string]*classad.ClassAd) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
