package metrics

import (
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestHandlerExposesViewMetrics: a materialized view's label_*/metric_* columns become
// Prometheus gauges named <view>_<suffix> with the labels stripped of their label_ prefix.
func TestHandlerExposesViewMetrics(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	base, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range []struct {
		key, owner string
		mem        int
	}{{"1", "alice", 100}, {"2", "alice", 200}, {"3", "bob", 400}} {
		ad, _ := classad.ParseOld("Owner = \"" + j.owner + "\"; RequestMemory = " + strconv.Itoa(j.mem))
		if err := base.Put(j.key, ad); err != nil {
			t.Fatal(err)
		}
	}
	spec := db.ViewSpec{
		BaseTable:   "jobs",
		Groups:      []db.ViewGroupCol{{Attr: "Owner", Alias: "label_owner"}},
		Metrics:     []db.ViewMetric{{Func: db.ViewCount, Arg: "*", Alias: "metric_jobs"}, {Func: db.ViewSum, Arg: "RequestMemory", Alias: "metric_memory"}},
		Cardinality: 100,
	}
	if err := cat.CreateView("cluster_usage", spec); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	rec := httptest.NewRecorder()
	Handler(cat).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	want := []string{
		"# TYPE cluster_usage_jobs gauge",
		"# TYPE cluster_usage_memory gauge",
		`cluster_usage_jobs{owner="alice"} 2`,
		`cluster_usage_memory{owner="alice"} 300`,
		`cluster_usage_jobs{owner="bob"} 1`,
		`cluster_usage_memory{owner="bob"} 400`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("metrics output missing %q; got:\n%s", w, body)
		}
	}
}
