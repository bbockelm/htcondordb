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

// recordsForLimitTest is comfortably above the history client's interactive
// default of 50 records per query, which is the cap this test exists to catch.
const recordsForLimitTest = 60

// TestImportIsNotCappedIntegration is the end-to-end guard for a silent cap. The
// history client reads a zero Limit as "unset" and substitutes 50; the importer
// means "no cap" by it. That mattered more than a slow import: the scan is
// newest-first and the cursor advances to the newest record seen, so a cycle
// truncated at 50 leaves the records below the cut UNIMPORTED and then skips past
// them for good.
//
// So: put more than 50 records in a real schedd's history and require one cycle to
// take all of them. The jobs are removed rather than run -- a removed job lands in
// history the same way a completed one does, without waiting on a match.
func TestImportIsNotCappedIntegration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("history-import integration test must run unprivileged")
	}
	h := htcondor.SetupCondorHarness(t) // skips if condor is not in PATH
	t.Setenv("CONDOR_CONFIG", h.GetConfigFile())

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pool := h.GetCollectorAddr()
	schedds, err := (CollectorDiscovery{}).Schedds(ctx, pool, "")
	if err != nil || len(schedds) == 0 {
		t.Fatalf("discover schedds: %v (n=%d)", err, len(schedds))
	}
	sd := schedds[0]
	sc := htcondor.NewSchedd(sd.Name, sd.Address)

	// Requirements = false keeps them idle (no match, no execution); removing them
	// is what writes the history records.
	submit := fmt.Sprintf("universe = vanilla\nexecutable = /bin/true\n"+
		"requirements = false\ntransfer_executable = false\ninitialdir = %s\nqueue %d\n",
		t.TempDir(), recordsForLimitTest)
	clusterID, err := sc.Submit(ctx, submit)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := sc.RemoveJobs(ctx, "ClusterId == "+clusterID, "history-import limit test"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	// Wait for the schedd to drain the cluster into history.
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ads, qerr := sc.Query(ctx, "ClusterId == "+clusterID, []string{"ClusterId"})
		if qerr == nil && len(ads) == 0 {
			break
		}
		time.Sleep(time.Second)
	}

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
	job := Job{Name: "cap", Pool: pool, Table: "history"}

	// One cycle, with no MAX_RECORDS_PER_CYCLE set: it must take every record.
	var st Stats
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		st, err = im.RunJob(ctx, job)
		if err != nil {
			t.Fatalf("RunJob: %v", err)
		}
		if st.Imported >= recordsForLimitTest {
			break
		}
		if st.Imported > 0 {
			break // a partial first cycle is the failure this test is about
		}
		time.Sleep(2 * time.Second)
	}
	if st.Imported < recordsForLimitTest {
		t.Fatalf("one cycle imported %d of %d records; the query is capped (50 is the client's default)",
			st.Imported, recordsForLimitTest)
	}
}
