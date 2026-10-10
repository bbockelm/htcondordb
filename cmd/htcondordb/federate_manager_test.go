package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/federate"
)

func TestResolveFederationSettings(t *testing.T) {
	s, bad, err := resolveFederationSettings(mkSyncCfg(t, ""))
	if err != nil || s.enabled || len(bad) != 0 {
		t.Fatalf("unset: %+v %v %v", s, bad, err)
	}

	s, bad, err = resolveFederationSettings(mkSyncCfg(t, `
HTCONDORDB_DIR = /var/lib/hub
HTCONDORDB_FEDERATE_SPOKES = ap1, ap2
HTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <10.0.0.1:9620>
HTCONDORDB_FEDERATE_SPOKE_AP1_SCHEDD = ap1.example.org
HTCONDORDB_FEDERATE_SPOKE_AP2_ADDRESS = <10.0.0.2:9620>
HTCONDORDB_FEDERATE_TABLES = jobs history epoch_history syncstatus
HTCONDORDB_FEDERATE_RETIRE_AFTER = 2d
HTCONDORDB_FEDERATE_FRESH_SECONDS = 30
HTCONDORDB_FEDERATE_STATE_INTERVAL = bogus
HTCONDORDB_HISTORY_MAX_BYTES = 10 GB
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.enabled || len(s.static) != 2 || s.static[0].Schedd != "ap1.example.org" || s.static[1].Schedd != "ap2" ||
		s.static[1].Address != "<10.0.0.2:9620>" || !s.static[0].Static {
		t.Errorf("static spokes = %+v", s.static)
	}
	if strings.Join(s.tables, " ") != "jobs history epoch_history syncstatus" {
		t.Errorf("tables = %v", s.tables)
	}
	if s.retireAfter != 48*time.Hour || s.fresh != 30*time.Second || s.stateInterval != federate.DefaultStateInterval {
		t.Errorf("durations: retire=%v fresh=%v state=%v", s.retireAfter, s.fresh, s.stateInterval)
	}
	if len(bad) != 1 || !strings.HasPrefix(bad[0], "HTCONDORDB_FEDERATE_STATE_INTERVAL") {
		t.Errorf("bad = %v, want the unparseable state interval named", bad)
	}
	if s.archive.MaxBytes[federate.TableHistory] != 10_000_000_000 {
		t.Errorf("history max bytes = %d", s.archive.MaxBytes[federate.TableHistory])
	}

	if _, _, err := resolveFederationSettings(mkSyncCfg(t, "HTCONDORDB_FEDERATE_SPOKES = ap1\n")); err == nil {
		t.Error("a static spoke with no address was accepted")
	}
	if _, _, err := resolveFederationSettings(mkSyncCfg(t, "HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT = true\n")); err == nil {
		t.Error("a constraint with no COLLECTOR_HOST was accepted")
	}
}

// TestFederationRefusesScheddSync: a hub and schedd sync in one daemon would write the same tables
// under different keys; both managers refuse the combination, and a reconfigure into it leaves
// whichever was running running, and the other stopped.
func TestFederationRefusesScheddSync(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	dir := t.TempDir()
	jobLog := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	spokeCfg := "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = " + dir + "/db\nJOB_EPOCH_HISTORY =\nHISTORY =\nJOB_QUEUE_LOG = " + jobLog + "\n"
	hubCfg := "HTCONDORDB_DIR = " + dir + "/db\nHTCONDORDB_FEDERATE_SPOKES = ap1\nHTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <127.0.0.1:1>\n"
	both := mkSyncCfg(t, spokeCfg+"HTCONDORDB_FEDERATE_SPOKES = ap1\nHTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <127.0.0.1:1>\n")

	for _, running := range []string{"hub", "schedd sync"} {
		t.Run(running+" running", func(t *testing.T) {
			svc := newOwnerTestService(t)
			fm := newFederationManager(t.Context(), svc.Catalog(), svc.Owners(), discardLogger())
			defer fm.Stop()
			sm := &scheddSyncManager{parent: t.Context(), svc: svc, logger: discardLogger()}
			defer sm.Stop()
			hubRunning := func() bool { fm.mu.Lock(); defer fm.mu.Unlock(); return fm.hub != nil }
			syncRunning := func() bool { sm.mu.Lock(); defer sm.mu.Unlock(); return sm.cancel != nil }
			if running == "hub" {
				if err := fm.apply(mkSyncCfg(t, hubCfg)); err != nil {
					t.Fatal(err)
				}
			} else if err := sm.apply(mkSyncCfg(t, spokeCfg)); err != nil {
				t.Fatal(err)
			}

			// The reconfigure into both, in main.go's order.
			serr := sm.apply(both)
			ferr := fm.apply(both)
			if ferr == nil || !strings.Contains(ferr.Error(), "HTCONDORDB_SYNC_SCHEDD") {
				t.Errorf("hub apply with schedd sync on: err = %v", ferr)
			}
			if running == "hub" && (serr == nil || !strings.Contains(serr.Error(), "federation hub")) {
				t.Errorf("schedd-sync apply with a hub configured: err = %v", serr)
			}
			if hubRunning() != (running == "hub") || syncRunning() != (running == "schedd sync") {
				t.Errorf("after the refused reconfigure: hub running=%v, schedd sync running=%v; want only the %s",
					hubRunning(), syncRunning(), running)
			}
		})
	}
}

func TestSyncControlRetireNotHub(t *testing.T) {
	sc := &syncController{fed: newFederationManager(t.Context(), nil, nil, discardLogger())}
	req := classad.New()
	req.InsertAttrString("Action", "retire")
	req.InsertAttrString("Target", "ap1")
	resp := sc.handle(t.Context(), req)
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
		t.Fatal("retire succeeded on a daemon that is not a hub")
	}
	if msg, _ := resp.EvaluateAttrString("Error"); !strings.Contains(msg, "not a federation hub") {
		t.Errorf("error = %q", msg)
	}
}
