package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
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
	Handler(cat).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
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
