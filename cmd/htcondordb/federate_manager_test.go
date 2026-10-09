package main

import (
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
// under different keys; both managers refuse the combination.
func TestFederationRefusesScheddSync(t *testing.T) {
	cfg := mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nJOB_QUEUE_LOG = /nonexistent\n"+
		"HTCONDORDB_FEDERATE_SPOKES = ap1\nHTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <127.0.0.1:1>\n")
	m := newFederationManager(t.Context(), nil, nil, discardLogger())
	err := m.apply(cfg)
	if err == nil || !strings.Contains(err.Error(), "HTCONDORDB_SYNC_SCHEDD") {
		t.Fatalf("hub apply with schedd sync on: err = %v", err)
	}
	sm := &scheddSyncManager{parent: t.Context(), logger: discardLogger()}
	if err := sm.apply(cfg); err == nil || !strings.Contains(err.Error(), "federation hub") {
		t.Fatalf("schedd-sync apply with a hub configured: err = %v", err)
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
