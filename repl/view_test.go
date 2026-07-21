package repl

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

func TestParseCreateMaterializedView(t *testing.T) {
	st, err := Parse(`CREATE MATERIALIZED VIEW cluster_usage MAXSERIES 500 AS
		SELECT Owner AS label_owner, JobStatus AS label_status,
		       COUNT(*) AS metric_jobs, SUM(RequestMemory) AS metric_memory
		FROM jobs GROUP BY Owner, JobStatus`)
	if err != nil {
		t.Fatal(err)
	}
	if st.Kind != StmtCreateView || st.ViewName != "cluster_usage" || st.ViewMaxSeries != 500 {
		t.Fatalf("parsed = %+v, want CreateView cluster_usage MAXSERIES 500", st)
	}
	if st.ViewSelect == nil || st.ViewSelect.Table != "jobs" {
		t.Fatalf("ViewSelect = %+v, want SELECT ... FROM jobs", st.ViewSelect)
	}

	spec, err := viewSpecFromSelect(st)
	if err != nil {
		t.Fatalf("viewSpecFromSelect: %v", err)
	}
	if spec.BaseTable != "jobs" || spec.Cardinality != 500 {
		t.Fatalf("spec = %+v, want BaseTable jobs Cardinality 500", spec)
	}
	if len(spec.Groups) != 2 || spec.Groups[0].Attr != "Owner" || spec.Groups[0].Alias != "label_owner" {
		t.Fatalf("groups = %+v", spec.Groups)
	}
	if len(spec.Metrics) != 2 {
		t.Fatalf("metrics = %+v, want 2", spec.Metrics)
	}
	if spec.Metrics[0].Func != db.ViewCount || spec.Metrics[1].Func != db.ViewSum {
		t.Fatalf("metric funcs = %v, %v; want count, sum", spec.Metrics[0].Func, spec.Metrics[1].Func)
	}

	// Omitting MAXSERIES falls back to the default cardinality.
	def, err := Parse(`CREATE MATERIALIZED VIEW v AS SELECT Owner AS label_owner, COUNT(*) AS metric_n FROM jobs GROUP BY Owner`)
	if err != nil {
		t.Fatal(err)
	}
	dspec, err := viewSpecFromSelect(def)
	if err != nil {
		t.Fatal(err)
	}
	if dspec.Cardinality != defaultViewCardinality {
		t.Fatalf("default cardinality = %d, want %d", dspec.Cardinality, defaultViewCardinality)
	}
}

func TestViewRejectsMinMax(t *testing.T) {
	for _, agg := range []string{"MIN", "MAX"} {
		st, err := Parse(`CREATE MATERIALIZED VIEW v AS SELECT Owner AS label_owner, ` +
			agg + `(RequestMemory) AS metric_m FROM jobs GROUP BY Owner`)
		if err != nil {
			t.Fatalf("%s parse: %v", agg, err)
		}
		if _, err := viewSpecFromSelect(st); err == nil {
			t.Fatalf("%s should be rejected in a materialized view", agg)
		}
	}
}

func TestViewRequiresGroupByAndAggregate(t *testing.T) {
	// A projected non-aggregate column not in GROUP BY must be rejected -- at parse (the
	// SELECT grammar enforces GROUP BY membership) or, failing that, when building the spec.
	rejected := func(q string) bool {
		st, err := Parse(q)
		if err != nil {
			return true
		}
		_, err = viewSpecFromSelect(st)
		return err != nil
	}
	if !rejected(`CREATE MATERIALIZED VIEW v AS SELECT Owner AS label_owner, JobStatus AS label_status, COUNT(*) AS metric_n FROM jobs GROUP BY Owner`) {
		t.Fatal("projected column not in GROUP BY should be rejected")
	}
	// No aggregate at all is an error.
	if !rejected(`CREATE MATERIALIZED VIEW v AS SELECT Owner AS label_owner FROM jobs GROUP BY Owner`) {
		t.Fatal("a view without an aggregate should be rejected")
	}
}

// TestExecMaterializedViewEndToEnd creates a view over a seeded base table via the CLI exec
// path, queries it like a table, renders its Prometheus export, and drops it.
func TestExecMaterializedViewEndToEnd(t *testing.T) {
	e, cat, cleanup := newPrivCatalogExec(t)
	defer cleanup()

	if _, err := e.ExecString("CREATE TABLE jobs"); err != nil {
		t.Fatal(err)
	}
	seed := []struct {
		key, owner    string
		status, memMB int
	}{
		{"1.0", "alice", 1, 100},
		{"2.0", "alice", 1, 200},
		{"3.0", "alice", 2, 50},
		{"4.0", "bob", 1, 400},
	}
	for _, s := range seed {
		q := "INSERT INTO jobs (Key, Owner, JobStatus, RequestMemory) VALUES ('" +
			s.key + "', '" + s.owner + "', " + strconv.Itoa(s.status) + ", " + strconv.Itoa(s.memMB) + ")"
		if _, err := e.ExecString(q); err != nil {
			t.Fatalf("insert %s: %v", s.key, err)
		}
	}

	if _, err := e.ExecString(`CREATE MATERIALIZED VIEW cluster_usage AS
		SELECT Owner AS label_owner, JobStatus AS label_status,
		       COUNT(*) AS metric_jobs, SUM(RequestMemory) AS metric_memory
		FROM jobs GROUP BY Owner, JobStatus`); err != nil {
		t.Fatalf("CREATE MATERIALIZED VIEW: %v", err)
	}

	// The view is registered in the catalog (not as a table).
	if views := cat.Views(); len(views) != 1 || views[0] != "cluster_usage" {
		t.Fatalf("catalog views = %v, want [cluster_usage]", views)
	}

	// Queried like a table: three groups (alice/1, alice/2, bob/1).
	r, err := e.ExecString("SELECT * FROM cluster_usage")
	if err != nil {
		t.Fatalf("SELECT * FROM view: %v", err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("view has %d rows, want 3", len(r.Rows))
	}

	// Prometheus export: two metric families, samples labeled owner/status.
	var buf bytes.Buffer
	(&session{exec: e}).exportViews(&buf, "cluster_usage")
	out := buf.String()
	for _, want := range []string{
		"# TYPE cluster_usage_jobs gauge",
		"# TYPE cluster_usage_memory gauge",
		`cluster_usage_jobs{owner="bob",status="1"} 1`,
		`cluster_usage_memory{owner="bob",status="1"} 400`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("export missing %q; got:\n%s", want, out)
		}
	}
	// A non-prefixed column would be ignored; assert nothing leaked a raw attr name.
	if strings.Contains(out, "label_owner") || strings.Contains(out, "metric_jobs{") {
		t.Fatalf("export leaked raw alias names:\n%s", out)
	}

	// Drop it.
	if _, err := e.ExecString("DROP MATERIALIZED VIEW cluster_usage"); err != nil {
		t.Fatalf("DROP MATERIALIZED VIEW: %v", err)
	}
	if views := cat.Views(); len(views) != 0 {
		t.Fatalf("views after drop = %v, want none", views)
	}
}

// TestViewSpecTimeBucket: a time_bucket in the view SELECT/GROUP BY produces a
// bucketed ViewGroupCol (a continuous aggregate).
func TestViewSpecTimeBucket(t *testing.T) {
	st, err := Parse(`CREATE MATERIALIZED VIEW jobs_ts AS
		SELECT time_bucket(QDate, '1h') AS time, Owner AS label_owner, COUNT(*) AS metric_jobs
		FROM jobs GROUP BY time_bucket(QDate, '1h'), Owner`)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := viewSpecFromSelect(st)
	if err != nil {
		t.Fatalf("viewSpecFromSelect: %v", err)
	}
	if len(spec.Groups) != 2 {
		t.Fatalf("groups = %+v, want 2", spec.Groups)
	}
	if spec.Groups[0].Attr != "QDate" || spec.Groups[0].Alias != "time" || spec.Groups[0].BucketWidth != 3600 {
		t.Fatalf("bucket group = %+v, want {QDate time 3600}", spec.Groups[0])
	}
	if spec.Groups[1].BucketWidth != 0 || spec.Groups[1].Attr != "Owner" {
		t.Fatalf("label group = %+v, want plain Owner", spec.Groups[1])
	}
}

// TestExecContinuousAggregate creates a time-bucketed view via SQL and reads the series.
func TestExecContinuousAggregate(t *testing.T) {
	e, _, cleanup := newPrivCatalogExec(t)
	defer cleanup()
	if _, err := e.ExecString("CREATE TABLE jobs"); err != nil {
		t.Fatal(err)
	}
	// 1h buckets: (3600,alice)=2, (3600,bob)=1, (7200,alice)=1.
	seed := []struct {
		key, owner string
		qdate      int
	}{
		{"1.0", "alice", 3600}, {"2.0", "alice", 3700},
		{"3.0", "bob", 3800}, {"4.0", "alice", 7200},
	}
	for _, s := range seed {
		q := "INSERT INTO jobs (Key, Owner, QDate) VALUES ('" + s.key + "', '" + s.owner + "', " + strconv.Itoa(s.qdate) + ")"
		if _, err := e.ExecString(q); err != nil {
			t.Fatalf("insert %s: %v", s.key, err)
		}
	}
	if _, err := e.ExecString(`CREATE MATERIALIZED VIEW jobs_ts AS
		SELECT time_bucket(QDate, '1h') AS time, Owner AS label_owner, COUNT(*) AS metric_jobs
		FROM jobs GROUP BY time_bucket(QDate, '1h'), Owner`); err != nil {
		t.Fatalf("CREATE continuous aggregate: %v", err)
	}
	r, err := e.ExecString("SELECT time, label_owner, metric_jobs FROM jobs_ts ORDER BY time, label_owner")
	if err != nil {
		t.Fatalf("read view: %v", err)
	}
	want := [][]string{
		{"3600", "alice", "2"},
		{"3600", "bob", "1"},
		{"7200", "alice", "1"},
	}
	if len(r.Rows) != len(want) {
		t.Fatalf("rows = %v, want %v", r.Rows, want)
	}
	for i, w := range want {
		if r.Rows[i][0] != w[0] || r.Rows[i][1] != w[1] || r.Rows[i][2] != w[2] {
			t.Errorf("row %d = %v, want %v", i, r.Rows[i], w)
		}
	}
}
