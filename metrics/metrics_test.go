package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/scheddsync"
)

// TestHandlerExposesStorageAndOpMetrics: the handler emits per-table storage gauges
// and the operational timing counter families (one series per op), plus a 200.
func TestHandlerExposesStorageAndOpMetrics(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	if _, err := cat.CreateTable("Machine"); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	Handler(cat, nil, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	want := []string{
		`htcondordb_ads{table="Machine"}`,
		`htcondordb_dead_bytes{table="Machine"}`,
		`htcondordb_segments{table="Machine"}`,
		`htcondordb_op_ops_total{op="shard_write_hold",table="Machine"}`,
		`htcondordb_op_seconds_total{op="sync",table="Machine"}`,
		`htcondordb_op_ops_total{op="snapshot_lock",table="Machine"}`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("metrics output missing %q", w)
		}
	}
}

type fakeSource struct{ st scheddsync.SyncStatus }

func (f fakeSource) Status() scheddsync.SyncStatus { return f.st }

// TestHandlerExposesSyncAndExporterMetrics: the handler emits schedd-sync tailer health and
// per-exporter health, the "is anything falling behind" signals an operator alerts on.
func TestHandlerExposesSyncAndExporterMetrics(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	sources := func() []dbad.StatusSource {
		return []dbad.StatusSource{
			// No Source path -> live-lag recompute leaves the reported values as-is.
			fakeSource{st: scheddsync.SyncStatus{Kind: "history", Offset: 500, FileSize: 700, LagBytes: 200, CaughtUp: false, LastSync: time.Unix(1_700_000_400, 0), Resyncs: 2}},
		}
	}
	exporters := func() []dbad.ExporterStatus {
		return []dbad.ExporterStatus{
			{Name: "jobs-os", Kind: "opensearch", Running: true, Restarts: 1, DocsIndexed: 4200, DocsSkipped: 3, InFlight: 7, LastBeat: time.Unix(1_700_000_450, 0)},
			{Name: "jobs-kafka", Kind: "kafka", Running: false, Restarts: 4},
		}
	}

	rec := httptest.NewRecorder()
	Handler(cat, sources, exporters).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	want := []string{
		`htcondordb_sync_lag_bytes{kind="history",source=""} 200`,
		`htcondordb_sync_caught_up{kind="history",source=""} 0`,
		`htcondordb_sync_resyncs_total{kind="history",source=""} 2`,
		`htcondordb_exporter_up{exporter="jobs-os",kind="opensearch"} 1`,
		`htcondordb_exporter_restarts_total{exporter="jobs-os",kind="opensearch"} 1`,
		`htcondordb_exporter_docs_indexed_total{exporter="jobs-os",kind="opensearch"} 4200`,
		`htcondordb_exporter_in_flight{exporter="jobs-os",kind="opensearch"} 7`,
		`htcondordb_exporter_up{exporter="jobs-kafka",kind="kafka"} 0`,
		`htcondordb_exporter_restarts_total{exporter="jobs-kafka",kind="kafka"} 4`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("metrics output missing %q", w)
		}
	}
	// A running exporter with no beat yet still emits up/restarts but no last_beat sample.
	if strings.Contains(body, `htcondordb_exporter_last_beat_timestamp_seconds{exporter="jobs-kafka"`) {
		t.Error("exporter with zero LastBeat should emit no last_beat sample")
	}
}
