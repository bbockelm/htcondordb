package scheddsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
)

// resolveSchedd starts a mini HTCondor via the golang-htcondor harness and returns a
// Schedd client plus the on-disk job_queue.log and history paths. It skips the test if
// HTCondor is not installed (the harness does this) or if running as root -- the sync must
// never read schedd files privileged.
func resolveSchedd(t *testing.T) (*htcondor.CondorTestHarness, *htcondor.Schedd, string, string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync integration test must run unprivileged (schedd files must not be read as root)")
	}
	h := htcondor.SetupCondorHarness(t) // skips if condor_master et al. are not in PATH
	if err := h.WaitForDaemons(); err != nil {
		t.Fatalf("daemons failed to start: %v", err)
	}
	cfg, err := h.GetConfig()
	if err != nil {
		t.Fatalf("harness config: %v", err)
	}
	jobLog := configOr(cfg, "JOB_QUEUE_LOG", filepath.Join(h.GetSpoolDir(), "job_queue.log"))
	histFile := configOr(cfg, "HISTORY", filepath.Join(h.GetSpoolDir(), "history"))

	collector := htcondor.NewCollector(h.GetCollectorAddr())
	loc, err := collector.LocateDaemon(context.Background(), "Schedd", "")
	if err != nil {
		t.Fatalf("locate schedd: %v", err)
	}
	return h, htcondor.NewSchedd(loc.Name, loc.Address), jobLog, histFile
}

func configOr(cfg *config.Config, key, fallback string) string {
	if v, ok := cfg.Get(key); ok && v != "" {
		return v
	}
	return fallback
}

// TestJobSyncIntegration submits a real job to a real schedd and verifies JobSync mirrors
// it (and its cluster ad) from the actual job_queue.log into a DB table.
func TestJobSyncIntegration(t *testing.T) {
	_, schedd, jobLog, _ := resolveSchedd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	clusterID, err := schedd.Submit(ctx, "universe = vanilla\nexecutable = /bin/sleep\narguments = 30\nqueue\n")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("submitted cluster %s", clusterID)

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	js := NewJobSync(target, JobSyncConfig{Filename: jobLog})

	key := clusterID + ".0"
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err := js.Poll(ctx); err != nil {
			t.Fatalf("poll: %v", err)
		}
		if _, ok := target.LookupClassAd(key); ok {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	ad, ok := target.LookupClassAd(key)
	if !ok {
		t.Fatalf("submitted job %s was not mirrored into the DB", key)
	}
	wantCluster, _ := strconv.Atoi(clusterID)
	if v, _ := ad.EvaluateAttrInt("ClusterId"); int(v) != wantCluster {
		t.Errorf("mirrored ClusterId = %d, want %d", v, wantCluster)
	}
	if v, _ := ad.EvaluateAttrInt("ProcId"); v != 0 {
		t.Errorf("mirrored ProcId = %d, want 0", v)
	}
	// The cluster ad is mirrored too. HTCondor writes its key with a namespace-sorting
	// leading zero (e.g. "01.-1" for cluster 1), so match any cluster ad (key ".-1"),
	// proving the syncer faithfully mirrors every key the schedd writes, not only procs.
	hasCluster := false
	for _, k := range target.Keys() {
		if strings.HasSuffix(k, ".-1") {
			hasCluster = true
			break
		}
	}
	if !hasCluster {
		t.Errorf("no cluster ad (key ending .-1) mirrored; keys: %v", target.Keys())
	}
}

// TestHistorySyncIntegration submits a job, runs it to completion on a real schedd, and
// verifies HistorySync archives it from the real history file.
func TestHistorySyncIntegration(t *testing.T) {
	_, schedd, _, histFile := resolveSchedd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// transfer_executable=false (the executable is a system binary already present on the
	// execute node) + output/log files + an initialdir: without these the job holds or never
	// runs on a personal condor. This mirrors golang-htcondor's own history integration test.
	jobDir := t.TempDir()
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/echo\narguments = hi\n"+
		"output = h.out\nerror = h.err\nlog = h.log\ntransfer_executable = false\ninitialdir = %s\nqueue\n", jobDir)
	clusterID, err := schedd.Submit(ctx, submit)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("submitted cluster %s", clusterID)
	_ = schedd.Reschedule(ctx) // nudge the negotiator so the fresh job matches promptly

	// Wait for the job to reach a terminal state (Completed=4 / Removed=3) or leave the queue
	// -- at which point it has been written to the history file.
	done := false
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ads, qerr := schedd.Query(ctx, "ClusterId == "+clusterID, []string{"JobStatus"})
		if qerr == nil {
			if len(ads) == 0 {
				done = true
				break
			}
			if s, ok := ads[0].EvaluateAttrInt("JobStatus"); ok && (s == 3 || s == 4) {
				done = true
				break
			}
		}
		_ = schedd.Reschedule(ctx)
		time.Sleep(1 * time.Second)
	}
	if !done {
		t.Fatal("job did not reach a terminal state in time")
	}

	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	hs := NewHistorySync(arch, HistorySyncConfig{Filename: histFile})

	found := false
	for i := 0; i < 20 && !found; i++ {
		if err := hs.Poll(ctx); err != nil {
			t.Fatalf("history poll: %v", err)
		}
		seq, err := arch.Query("ClusterId == " + clusterID)
		if err != nil {
			t.Fatal(err)
		}
		for range seq {
			found = true
		}
		if !found {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !found {
		t.Errorf("completed job %s not found in the archived history", clusterID)
	}
}
