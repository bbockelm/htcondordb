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
HTCONDORDB_FEDERATE_TABLES = jobs history epoch_history job_metrics syncstatus
HTCONDORDB_FEDERATE_RETIRE_AFTER = 2d
HTCONDORDB_FEDERATE_FRESH_SECONDS = 30
HTCONDORDB_FEDERATE_STATE_INTERVAL = bogus
HTCONDORDB_HISTORY_MAX_BYTES = 10 GB
HTCONDORDB_ARCHIVE_MAX_BYTES = 2 GB
HTCONDORDB_JOB_METRICS_MAX_AGE = 30d
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.enabled || len(s.static) != 2 || s.static[0].Schedd != "ap1.example.org" || s.static[1].Schedd != "ap2" ||
		s.static[1].Address != "<10.0.0.2:9620>" || !s.static[0].Static {
		t.Errorf("static spokes = %+v", s.static)
	}
	if strings.Join(s.tables, " ") != "jobs history epoch_history job_metrics syncstatus" {
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
	// job_metrics is sized as on a spoke: its own knobs, the shared archive cap by default.
	if s.archive.MaxBytes[federate.TableJobMetrics] != 2_000_000_000 {
		t.Errorf("job_metrics max bytes = %d, want HTCONDORDB_ARCHIVE_MAX_BYTES", s.archive.MaxBytes[federate.TableJobMetrics])
	}
	if s.archive.MaxAge[federate.TableJobMetrics] != 30*86400 {
		t.Errorf("job_metrics max age = %v, want 30d", s.archive.MaxAge[federate.TableJobMetrics])
	}
	s, bad, _ = resolveFederationSettings(mkSyncCfg(t, "HTCONDORDB_FEDERATE_SPOKES = ap1\nHTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <10.0.0.1:9620>\n"+
		"HTCONDORDB_JOB_METRICS_MAX_AGE = 30 days\nHTCONDORDB_JOB_METRICS_MAX_BYTES = 5 GB\n"))
	if len(bad) != 1 || !strings.HasPrefix(bad[0], "HTCONDORDB_JOB_METRICS_MAX_AGE") || s.archive.MaxAge[federate.TableJobMetrics] != 0 {
		t.Errorf("unparseable job_metrics max age: bad = %v, age = %v; want it named and no cap", bad, s.archive.MaxAge[federate.TableJobMetrics])
	}
	if s.archive.MaxBytes[federate.TableJobMetrics] != 5_000_000_000 {
		t.Errorf("job_metrics max bytes = %d, want 5 GB", s.archive.MaxBytes[federate.TableJobMetrics])
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

// TestFederationNegativeJobMetricsMaxAge: a negative HTCONDORDB_JOB_METRICS_MAX_AGE is named as bad and
// becomes no cap, never a cap that is passed through to the archive.
func TestFederationNegativeJobMetricsMaxAge(t *testing.T) {
	for _, v := range []string{"-1", "-2d"} {
		s, bad, err := resolveFederationSettings(mkSyncCfg(t, "HTCONDORDB_FEDERATE_SPOKES = ap1\nHTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <10.0.0.1:9620>\n"+
			"HTCONDORDB_JOB_METRICS_MAX_AGE = "+v+"\n"))
		if err != nil {
			t.Fatal(err)
		}
		if len(bad) != 1 || !strings.HasPrefix(bad[0], "HTCONDORDB_JOB_METRICS_MAX_AGE") || s.archive.MaxAge[federate.TableJobMetrics] != 0 {
			t.Errorf("%s: bad = %v, age = %v; want it named and no cap", v, bad, s.archive.MaxAge[federate.TableJobMetrics])
		}
	}
}

// TestFederationJobMetricsStorage: the hub stores job_metrics with the spoke's
// HTCONDORDB_JOB_METRICS_* storage knobs and their defaults -- not HTCONDORDB_ARCHIVE_SEGMENT_SIZE
// or the history index extras.
func TestFederationJobMetricsStorage(t *testing.T) {
	const spokes = "HTCONDORDB_FEDERATE_SPOKES = ap1\nHTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <10.0.0.1:9620>\n"
	s, bad, err := resolveFederationSettings(mkSyncCfg(t, spokes+
		"HTCONDORDB_ARCHIVE_SEGMENT_SIZE = 67108864\nHTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS = AcctGroup\n"))
	if err != nil || len(bad) != 0 {
		t.Fatalf("defaults: %v %v", bad, err)
	}
	if st := s.archive.Storage[federate.TableJobMetrics]; st.SegmentSize != 0 || st.GroupSchemaCount != 0 ||
		strings.Join(st.ExtraCategorical, ",") != "Owner" {
		t.Errorf("default job_metrics storage = %+v, want the library segment size, group schemas on, Owner", st)
	}
	s, bad, err = resolveFederationSettings(mkSyncCfg(t, spokes+
		"HTCONDORDB_JOB_METRICS_SEGMENT_SIZE = 16 MiB\nHTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS = Owner, ProjectName\n"+
		"HTCONDORDB_JOB_METRICS_GROUP_SCHEMAS = false\n"))
	if err != nil || len(bad) != 0 {
		t.Fatalf("set: %v %v", bad, err)
	}
	if st := s.archive.Storage[federate.TableJobMetrics]; st.SegmentSize != 16<<20 || st.GroupSchemaCount != -1 ||
		strings.Join(st.ExtraCategorical, ",") != "Owner,ProjectName" {
		t.Errorf("job_metrics storage = %+v", st)
	}
	_, bad, _ = resolveFederationSettings(mkSyncCfg(t, spokes+"HTCONDORDB_JOB_METRICS_SEGMENT_SIZE = 1 KiB\n"))
	if len(bad) != 1 || !strings.HasPrefix(bad[0], "HTCONDORDB_JOB_METRICS_SEGMENT_SIZE") {
		t.Errorf("unusable segment size: bad = %v, want it named", bad)
	}
}
