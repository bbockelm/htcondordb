package federate

import (
	"fmt"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/cedarsync"
	"github.com/bbockelm/htcondordb/syncstatus"
)

// enableJobMetrics gives the spoke a job_metrics archive, as HTCONDORDB_JOB_METRICS does.
func (s *testSpoke) enableJobMetrics() {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.cat.CreateArchiveTable(TableJobMetrics, db.ArchiveConfig{ZoneAttrs: []string{"SampleTime"}}); err != nil {
		s.t.Fatal(err)
	}
}

func (s *testSpoke) appendSamples(schedd string, cluster int, offsets ...int64) {
	s.t.Helper()
	s.mu.Lock()
	a, _ := s.cat.ArchiveTable(TableJobMetrics)
	s.mu.Unlock()
	for _, off := range offsets {
		if err := a.Append(sample(s.t, schedd, cluster, 1_700_000_000+off, off)); err != nil {
			s.t.Fatal(err)
		}
	}
}

// heartbeat writes a live heartbeat row into the spoke's syncstatus table, caught up (lag 0).
func (s *testSpoke) heartbeat(seq int) {
	s.t.Helper()
	tx := s.table(TableSyncStatus).Begin()
	tx.NewClassAd(syncstatus.Key, parseAd(s.t, fmt.Sprintf(
		`HeartbeatSeq = %d; HeartbeatTime = %d; SpokeLagSeconds = 0; HeartbeatIntervalSeconds = 1`, seq, time.Now().Unix())))
	if err := tx.Commit(); err != nil {
		s.t.Fatal(err)
	}
}

// TestSpokeWithoutJobMetrics: a hub federating job_metrics from a spoke that does not sample (no
// job_metrics table) reports that table as missing -- JobMetricsMissing, JobMetricsConnected and
// JobMetricsSynced false, a LastError saying why -- while the AP's other tables stream and the
// source is fresh. Another AP's job_metrics stream is unaffected. When the spoke turns sampling on,
// the stream picks the table up without a hub restart.
func TestSpokeWithoutJobMetrics(t *testing.T) {
	a, b := newTestSpoke(t), newTestSpoke(t)
	a.enableJobMetrics()
	a.putJobs(1)
	b.putJobs(1)
	a.appendHistory("ap1", 1)
	b.appendHistory("ap2", 1)
	a.appendSamples("ap1", 1, 100, 200)

	disc := &Discovery{Static: []Spoke{{Schedd: "ap1", Address: "A"}, {Schedd: "ap2", Address: "B"}}}
	dial := func(addr string) cedarsync.Dial {
		if addr == "A" {
			return a.dial
		}
		return b.dial
	}
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: dial, CursorDir: t.TempDir(),
		Tables:        []string{TableJobs, TableHistory, TableSyncStatus, TableJobMetrics},
		FlushInterval: 20 * time.Millisecond, FreshThreshold: 60 * time.Second, MissingTableRetry: 300 * time.Millisecond})
	defer hub.stop(t)

	for _, sp := range []struct {
		s      *testSpoke
		schedd string
	}{{a, "ap1"}, {b, "ap2"}} {
		waitFor(t, sp.schedd+" syncstatus caught up", func() bool {
			v, _ := sourceRowAttrBool(cat, sp.schedd, "SyncStatusSynced")
			return v
		})
		sp.s.heartbeat(1) // live, so the hub stamps its receipt
	}
	waitFor(t, "both APs fresh", func() bool { return sourceState(cat, "ap1") == StateFresh && sourceState(cat, "ap2") == StateFresh })
	waitFor(t, "ap2's job_metrics reported missing", func() bool {
		v, _ := sourceRowAttrBool(cat, "ap2", "JobMetricsMissing")
		return v
	})
	waitFor(t, "ap1's job_metrics synced", func() bool {
		v, _ := sourceRowAttrBool(cat, "ap1", "JobMetricsSynced")
		return v
	})

	row, _ := sourceRow(cat, "ap2")
	if st, _ := row.EvaluateAttrString("State"); st != StateFresh {
		t.Errorf("ap2 State = %q, want fresh: a spoke without job_metrics must not hold its AP back", st)
	}
	for _, attr := range []string{"JobsSynced", "SyncStatusSynced", "HistorySynced", "JobsConnected", "HistoryConnected"} {
		if v, ok := row.EvaluateAttrBool(attr); !ok || !v {
			t.Errorf("ap2 %s = %v (present %v), want true", attr, v, ok)
		}
	}
	for _, attr := range []string{"JobMetricsConnected", "JobMetricsSynced"} {
		if v, ok := row.EvaluateAttrBool(attr); !ok || v {
			t.Errorf("ap2 %s = %v (present %v), want false", attr, v, ok)
		}
	}
	if e, _ := row.EvaluateAttrString("JobMetricsLastError"); e != missingTableError(TableJobMetrics) {
		t.Errorf("ap2 JobMetricsLastError = %q, want %q", e, missingTableError(TableJobMetrics))
	}
	if _, ok := row.Lookup("JobMetricsSyncedAt"); ok {
		t.Error("ap2 has a JobMetricsSyncedAt for a table it never had")
	}
	for _, attr := range []string{"JobsLastError", "HistoryLastError", "SyncStatusLastError"} {
		if e, ok := row.EvaluateAttrString(attr); ok {
			t.Errorf("ap2 %s = %q: the missing table leaked into another table's status", attr, e)
		}
	}

	row1, _ := sourceRow(cat, "ap1")
	if _, ok := row1.Lookup("JobMetricsMissing"); ok {
		t.Error("ap1, which has job_metrics, is reported missing it")
	}
	jm, _ := cat.ArchiveTable(TableJobMetrics)
	if n := countArchive(t, jm, `ScheddName == "ap1" && User == "alice@example.org"`); n != 2 {
		t.Errorf("ap1 samples on the hub = %d, want 2", n)
	}

	// ap2 turns sampling on: its stream finds the table at the next retry.
	b.enableJobMetrics()
	b.appendSamples("ap1", 1, 100) // a record naming ap1, as a misconfigured spoke might
	waitFor(t, "ap2's job_metrics synced once the spoke has it", func() bool {
		v, _ := sourceRowAttrBool(cat, "ap2", "JobMetricsSynced")
		return v
	})
	waitFor(t, "ap2's sample on the hub", func() bool { return countArchive(t, jm, `ScheddName == "ap2"`) == 1 })
	row, _ = sourceRow(cat, "ap2")
	if _, ok := row.Lookup("JobMetricsMissing"); ok {
		t.Error("ap2 still reported missing job_metrics after it streamed")
	}
	if n := countArchive(t, jm, `ScheddName == "ap1"`); n != 2 {
		t.Errorf("ap1 samples = %d after ap2's arrived, want 2", n)
	}
}
