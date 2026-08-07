package historyimport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// secondScheddConfig adds a second schedd instance to the personal-condor harness
// via the -local-name mechanism, so one collector advertises two schedds and the
// importer has a real multi-schedd pool to aggregate. The base harness config
// already sets DAEMON_LIST, SCHEDD_INTERVAL, and permissive security; this only
// adds the extra daemon and its per-instance (SCHEDD2.*) knobs.
const secondScheddConfig = `
SCHEDD2 = $(SCHEDD)
SCHEDD2_ARGS = -local-name schedd2
DAEMON_LIST = $(DAEMON_LIST) SCHEDD2
SCHEDD2.SCHEDD_NAME = schedd2
SCHEDD2.SCHEDD_LOG = $(LOG)/ScheddLog2
SCHEDD2.SPOOL = $(SPOOL)/schedd2
SCHEDD2.SCHEDD_ADDRESS_FILE = $(LOG)/.schedd2_address
SCHEDD2.SCHEDD_DAEMON_AD_FILE = $(LOG)/.schedd2_classad
SCHEDD2.HISTORY = $(SPOOL)/history
`

// TestTwoScheddImportIntegration is the end-to-end proof: two real schedds in one
// pool, each with a completed job in its history, are discovered and aggregated
// into a single archive table -- with ScheddName stamped and per-schedd cursors
// making a second run a no-op. Skips when condor_master is not installed.
func TestTwoScheddImportIntegration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("history-import integration test must run unprivileged")
	}
	h := htcondor.SetupCondorHarnessWithConfig(t, secondScheddConfig) // skips if condor is not in PATH
	// The importer's CEDAR client calls authenticate from CONDOR_CONFIG, like every
	// pool client; point it at the harness so discovery and history queries work.
	t.Setenv("CONDOR_CONFIG", h.GetConfigFile())

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pool := h.GetCollectorAddr()

	// Wait until both schedds advertise, then keep the discovered refs.
	var schedds []ScheddRef
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		refs, err := (CollectorDiscovery{}).Schedds(ctx, pool, "")
		if err == nil && len(refs) >= 2 {
			schedds = refs
			break
		}
		time.Sleep(2 * time.Second)
	}
	if len(schedds) < 2 {
		t.Fatalf("expected 2 schedds to advertise, saw %d", len(schedds))
	}

	// Put one job into each schedd's history.
	for _, sd := range schedds {
		submitAndRetire(t, ctx, sd, h)
	}

	// Import the pool into a fresh in-process archive.
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	im := &Importer{
		Disc: CollectorDiscovery{},
		Src:  ScheddHistorySource{},
		W:    &ArchiveWriter{Cat: cat},
		Cur:  NewMapCursors(),
	}
	job := Job{Name: "it", Pool: pool, Table: "history"}

	st, err := im.RunJob(ctx, job)
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if st.Schedds != 2 {
		t.Fatalf("imported from %d schedds, want 2 (failures=%d)", st.Schedds, st.Failures)
	}
	if st.Imported < 2 {
		t.Fatalf("imported %d records, want >= 2 (one per schedd)", st.Imported)
	}

	// Every schedd is represented, and every record carries its ScheddName.
	seen := map[string]int{}
	arch, _ := cat.ArchiveTable("history")
	seq, err := arch.Query("true")
	if err != nil {
		t.Fatal(err)
	}
	for ad := range seq {
		sn, ok := ad.EvaluateAttrString(ScheddNameAttr)
		if !ok || sn == "" {
			t.Errorf("record missing ScheddName: %v", ad)
			continue
		}
		seen[sn]++
	}
	for _, sd := range schedds {
		if seen[sd.Name] == 0 {
			t.Errorf("no records imported for schedd %q (seen: %v)", sd.Name, seen)
		}
	}

	// A second run is incremental: the cursors are current, so nothing new imports.
	st2, err := im.RunJob(ctx, job)
	if err != nil {
		t.Fatalf("second RunJob: %v", err)
	}
	if st2.Imported != 0 {
		t.Errorf("second run imported %d records, want 0 (cursor should make it a no-op)", st2.Imported)
	}
}

// submitAndRetire submits a trivial job to sd and removes it: a job written to
// history the moment it leaves the queue, whether or not it ran, so the test does
// not depend on the shared startd matching it. It then confirms the record via a
// remote history query. On failure it dumps diagnostics (the last history error,
// the schedd log, the on-disk history file) so a broken second-schedd setup is
// visible.
func submitAndRetire(t *testing.T, ctx context.Context, sd ScheddRef, h *htcondor.CondorTestHarness) {
	t.Helper()
	sc := htcondor.NewSchedd(sd.Name, sd.Address)

	jobDir := t.TempDir()
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/echo\narguments = hi\n"+
		"output = h.out\nerror = h.err\nlog = h.log\ntransfer_executable = false\ninitialdir = %s\nqueue\n", jobDir)
	clusterID, err := sc.Submit(ctx, submit)
	if err != nil {
		t.Fatalf("submit to %s: %v", sd.Name, err)
	}
	if _, err := sc.RemoveJobs(ctx, "ClusterId == "+clusterID, "history-import integration test"); err != nil {
		t.Fatalf("remove on %s: %v", sd.Name, err)
	}
	t.Logf("submitted+removed %s.0 on %s", clusterID, sd.Name)

	// Wait for the job to leave the queue (into history).
	deadline := time.Now().Add(60 * time.Second)
	left := false
	for time.Now().Before(deadline) {
		ads, qerr := sc.Query(ctx, "ClusterId == "+clusterID, []string{"JobStatus"})
		if qerr == nil && len(ads) == 0 {
			left = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !left {
		t.Fatalf("job %s.0 on %s did not leave the queue in time", clusterID, sd.Name)
	}

	var lastErr error
	for time.Now().Before(deadline) {
		ads, herr := sc.QueryHistory(ctx, "ClusterId == "+clusterID, []string{"ClusterId", "GlobalJobId"})
		lastErr = herr
		if herr == nil && len(ads) > 0 {
			return
		}
		time.Sleep(1 * time.Second)
	}
	dumpScheddDiag(t, h, sd)
	t.Fatalf("job %s.0 on %s never reached history (last QueryHistory err: %v)", clusterID, sd.Name, lastErr)
}

// dumpScheddDiag prints the schedd's log tail and its history file state to help
// diagnose why a completed job never reached history.
func dumpScheddDiag(t *testing.T, h *htcondor.CondorTestHarness, sd ScheddRef) {
	t.Helper()
	logDir := h.GetLogDir()
	logName := "ScheddLog"
	histPath := filepath.Join(h.GetSpoolDir(), "history")
	if sd.Name == "schedd2" || strings.HasPrefix(sd.Name, "schedd2@") {
		logName = "ScheddLog2"
		histPath = filepath.Join(h.GetSpoolDir(), "schedd2", "history")
	}
	if b, err := os.ReadFile(filepath.Join(logDir, logName)); err == nil {
		t.Logf("--- %s (tail) ---\n%s", logName, tailStr(string(b), 4000))
	} else {
		t.Logf("could not read %s: %v", logName, err)
	}
	if fi, err := os.Stat(histPath); err == nil {
		t.Logf("history file %s: %d bytes", histPath, fi.Size())
	} else {
		t.Logf("history file %s: %v", histPath, err)
	}
}

func tailStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}
