package scheddsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestJobEpochSyncIntegration runs a job to completion on a real schedd with epoch
// recording enabled, then verifies JobEpochSync tails the local job_epoch_history
// file into an archive -- with RunInstanceID present (proving it is the epoch
// stream) and EnteredHistoryTime stamped. Skips when condor is not installed.
func TestJobEpochSyncIntegration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync integration test must run unprivileged")
	}
	h := htcondor.SetupCondorHarnessWithConfig(t, "\nJOB_EPOCH_HISTORY = $(SPOOL)/job_epoch_history\n")
	if err := h.WaitForDaemons(); err != nil {
		t.Fatalf("daemons failed to start: %v", err)
	}
	cfg, err := h.GetConfig()
	if err != nil {
		t.Fatal(err)
	}
	epochFile := configOr(cfg, "JOB_EPOCH_HISTORY", filepath.Join(h.GetSpoolDir(), "job_epoch_history"))

	collector := htcondor.NewCollector(h.GetCollectorAddr())
	loc, err := collector.LocateDaemon(context.Background(), "Schedd", "")
	if err != nil {
		t.Fatalf("locate schedd: %v", err)
	}
	schedd := htcondor.NewSchedd(loc.Name, loc.Address)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	// Run a job to completion so the schedd writes an epoch record.
	jobDir := t.TempDir()
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/echo\narguments = hi\n"+
		"output = e.out\nerror = e.err\nlog = e.log\ntransfer_executable = false\ninitialdir = %s\nqueue\n", jobDir)
	clusterID, err := schedd.Submit(ctx, submit)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		ads, qerr := schedd.Query(ctx, "ClusterId == "+clusterID, []string{"JobStatus"})
		if qerr == nil {
			if len(ads) == 0 {
				break
			}
			if s, ok := ads[0].EvaluateAttrInt("JobStatus"); ok && (s == 3 || s == 4) {
				break
			}
		}
		_ = schedd.Reschedule(ctx)
		time.Sleep(1 * time.Second)
	}

	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable("epoch_history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId"},
		ZoneAttrs:  []string{EpochWriteDateAttr, EnteredHistoryAttr},
	})
	if err != nil {
		t.Fatal(err)
	}
	es := NewJobEpochSync(arch, HistorySyncConfig{Filename: epochFile})

	found := false
	for i := 0; i < 30 && !found; i++ {
		if err := es.Poll(ctx); err != nil {
			t.Fatalf("epoch poll: %v", err)
		}
		seq, err := arch.Query("ClusterId == " + clusterID)
		if err != nil {
			t.Fatal(err)
		}
		for ad := range seq {
			if _, ok := ad.EvaluateAttrInt(RunInstanceAttr); !ok {
				t.Errorf("epoch record missing %s", RunInstanceAttr)
			}
			if _, ok := ad.EvaluateAttrInt(EnteredHistoryAttr); !ok {
				t.Errorf("epoch record missing %s (event-time stamp)", EnteredHistoryAttr)
			}
			found = true
		}
		if !found {
			time.Sleep(1 * time.Second)
		}
	}
	if !found {
		t.Fatalf("job %s.0 produced no epoch record in the archive", clusterID)
	}

	// A second poll of the same file appends nothing new (offset resume).
	before := arch.Count()
	if err := es.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != before {
		t.Errorf("second poll appended %d new records, want 0", arch.Count()-before)
	}
}
