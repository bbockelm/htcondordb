package historyimport

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// epochConfig enables per-run-instance (epoch) history recording on the harness
// schedd, into a file the schedd serves via condor_history -epochs.
const epochConfig = `
JOB_EPOCH_HISTORY = $(SPOOL)/job_epoch_history
`

// TestEpochImportIntegration is the end-to-end proof for epoch import: a real
// schedd with epoch recording on, a job run to completion (one run instance ->
// one epoch record), then the real importer pulls it via condor_history -epochs
// into an archive -- stamped with ScheddName, carrying RunInstanceID -- and a
// second run is a no-op (the EpochWriteDate cursor). Skips without condor.
func TestEpochImportIntegration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("history-import integration test must run unprivileged")
	}
	h := htcondor.SetupCondorHarnessWithConfig(t, epochConfig) // skips if condor is not in PATH
	t.Setenv("CONDOR_CONFIG", h.GetConfigFile())

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pool := h.GetCollectorAddr()
	schedds, err := (CollectorDiscovery{}).Schedds(ctx, pool, "")
	if err != nil || len(schedds) == 0 {
		t.Fatalf("discover schedds: %v (n=%d)", err, len(schedds))
	}
	sd := schedds[0]

	// Run a job to completion so the schedd writes one epoch record.
	runJobToCompletion(t, ctx, sd)

	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	im := &Importer{
		Disc: CollectorDiscovery{},
		Src:  ScheddHistorySource{HistorySource: htcondor.HistorySourceJobEpoch},
		W:    &ArchiveWriter{Cat: cat},
		Cur:  NewMapCursors(),
	}
	job := Job{Name: "ep", Pool: pool, Table: "epoch_history", Source: SourceEpoch}

	// The epoch record may lag the job's queue departure slightly; retry the cycle.
	var st Stats
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		st, err = im.RunJob(ctx, job)
		if err != nil {
			t.Fatalf("RunJob: %v", err)
		}
		if st.Imported > 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if st.Imported == 0 {
		t.Fatal("no epoch records imported")
	}

	// The imported records carry ScheddName and a RunInstanceID (proving it is the
	// epoch stream, not the completed-job history).
	arch, _ := cat.ArchiveTable("epoch_history")
	seq, err := arch.Query("true")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ad := range seq {
		n++
		if sn, ok := ad.EvaluateAttrString(ScheddNameAttr); !ok || sn != sd.Name {
			t.Errorf("epoch record ScheddName = %q, want %q", sn, sd.Name)
		}
		if _, ok := ad.EvaluateAttrInt(runInstanceAttr); !ok {
			t.Errorf("epoch record missing %s", runInstanceAttr)
		}
	}
	if n == 0 {
		t.Fatal("epoch table empty after a positive import count")
	}

	// A second cycle imports nothing new (the EpochWriteDate cursor holds).
	st2, err := im.RunJob(ctx, job)
	if err != nil {
		t.Fatalf("second RunJob: %v", err)
	}
	if st2.Imported != 0 {
		t.Errorf("second run imported %d epoch records, want 0", st2.Imported)
	}
}

// runJobToCompletion submits a trivial job to sd and waits for it to leave the
// queue (run to completion), so the schedd records a run instance.
func runJobToCompletion(t *testing.T, ctx context.Context, sd ScheddRef) {
	t.Helper()
	sc := htcondor.NewSchedd(sd.Name, sd.Address)
	jobDir := t.TempDir()
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/echo\narguments = hi\n"+
		"output = e.out\nerror = e.err\nlog = e.log\ntransfer_executable = false\ninitialdir = %s\nqueue\n", jobDir)
	clusterID, err := sc.Submit(ctx, submit)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		ads, qerr := sc.Query(ctx, "ClusterId == "+clusterID, []string{"JobStatus"})
		if qerr == nil {
			if len(ads) == 0 {
				return
			}
			if s, ok := ads[0].EvaluateAttrInt("JobStatus"); ok && (s == 3 || s == 4) {
				return
			}
		}
		_ = sc.Reschedule(ctx)
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("job %s.0 did not run to completion in time", clusterID)
}
