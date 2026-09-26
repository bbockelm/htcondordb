package plugin

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/repl"
)

// The plugin ships dashboards as JSON (src/dashboards, bundled via plugin.json includes). Their
// panel targets are the same queryModel the editor produces, so they can be compiled here and
// handed to the very parser the server runs -- which turns "a shipped dashboard is broken" from
// something a human notices in a browser into a unit-test failure.
//
// This catches the mistakes JSON authored by hand actually makes: an attribute that does not
// exist in the query model, a filter whose value needs quoting, an aggregate shape the SQL
// surface does not support (an expression inside AVG(), say), a macro that never expands.

type shippedDashboard struct {
	Title  string `json:"title"`
	UID    string `json:"uid"`
	Panels []struct {
		Title   string            `json:"title"`
		Type    string            `json:"type"`
		Targets []json.RawMessage `json:"targets"`
	} `json:"panels"`
	Templating struct {
		List []struct {
			Name  string `json:"name"`
			Type  string `json:"type"`
			Query string `json:"query"`
		} `json:"list"`
	} `json:"templating"`
}

func loadShippedDashboards(t *testing.T) []shippedDashboard {
	t.Helper()
	dir := filepath.Join("..", "..", "src", "dashboards")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	var out []shippedDashboard
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			t.Fatalf("reading %s: %v", e.Name(), rerr)
		}
		var d shippedDashboard
		if uerr := json.Unmarshal(b, &d); uerr != nil {
			t.Fatalf("%s is not valid dashboard JSON: %v", e.Name(), uerr)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		t.Fatal("no shipped dashboards found -- this test would pass vacuously")
	}
	return out
}

// TestShippedDashboardsCompile turns every shipped panel into SQL and parses it with the server's
// own parser.
func TestShippedDashboardsCompile(t *testing.T) {
	tr := timeRange{fromUnix: 1700000000, toUnix: 1700003600}
	panels := 0
	for _, d := range loadShippedDashboards(t) {
		if d.UID == "" || d.Title == "" {
			t.Errorf("dashboard %q has no uid/title", d.Title)
		}
		for _, p := range d.Panels {
			for i, raw := range p.Targets {
				var q queryModel
				if err := json.Unmarshal(raw, &q); err != nil {
					t.Errorf("%s / %q target %d: not a query model: %v", d.UID, p.Title, i, err)
					continue
				}
				sql, err := q.toSQL(tr, time.Minute)
				if err != nil {
					t.Errorf("%s / %q target %d: toSQL: %v", d.UID, p.Title, i, err)
					continue
				}
				// A dashboard variable is substituted by Grafana before the query reaches the
				// backend; the parser would choke on the raw ${...}, so stand one in.
				probe := strings.NewReplacer("$owner", "alice", "${owner}", "alice").Replace(sql)
				if _, perr := repl.Parse(probe); perr != nil {
					t.Errorf("%s / %q target %d does not parse:\n  sql: %s\n  err: %v",
						d.UID, p.Title, i, probe, perr)
				}
				panels++
			}
		}
	}
	t.Logf("compiled and parsed %d panel targets", panels)
}

// TestShippedDashboardVariablesCompile: a query variable's SQL runs through the same parser when
// the datasource populates a dropdown, so a broken one leaves the dashboard unusable in a way no
// panel test would show.
func TestShippedDashboardVariablesCompile(t *testing.T) {
	n := 0
	for _, d := range loadShippedDashboards(t) {
		for _, v := range d.Templating.List {
			if v.Type != "query" || strings.TrimSpace(v.Query) == "" {
				continue
			}
			if _, err := repl.Parse(v.Query); err != nil {
				t.Errorf("%s / variable %q does not parse: %q: %v", d.UID, v.Name, v.Query, err)
			}
			n++
		}
	}
	if n == 0 {
		t.Fatal("no query variables found -- this test would pass vacuously")
	}
}

// TestJobMetricsDashboardIsShipped pins the two things about the job-metrics dashboard that a
// reader has to be able to trust: it is registered for bundling, and the panels whose numbers are
// only meaningful for a job's FIRST run actually say so in the query. The memory high-water mark
// carries across runs (the shadow seeds it from the previous one), so a panel that aggregates it
// without restricting to RunInstanceID == 0 is reporting a whole-job maximum as if it were this
// run's.
func TestJobMetricsDashboardIsShipped(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "src", "plugin.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pj struct {
		Includes []struct{ Type, Name, Path string } `json:"includes"`
	}
	if err := json.Unmarshal(b, &pj); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, inc := range pj.Includes {
		if strings.HasSuffix(inc.Path, "job-metrics.json") {
			found = true
		}
	}
	if !found {
		t.Fatal("job-metrics.json is not in plugin.json includes, so it is never bundled")
	}

	var dash shippedDashboard
	for _, d := range loadShippedDashboards(t) {
		if d.UID == "htcondordb-job-metrics" {
			dash = d
		}
	}
	if dash.UID == "" {
		t.Fatal("the job-metrics dashboard is missing")
	}
	tr := timeRange{fromUnix: 1700000000, toUnix: 1700003600}
	checked := 0
	for _, p := range dash.Panels {
		for _, raw := range p.Targets {
			var q queryModel
			if err := json.Unmarshal(raw, &q); err != nil {
				continue
			}
			sql, err := q.toSQL(tr, time.Minute)
			if err != nil {
				continue
			}
			if !strings.Contains(sql, "MemoryUsage") && !strings.Contains(sql, "MemUtil") {
				continue
			}
			checked++
			if !strings.Contains(sql, "RunInstanceID == 0") {
				t.Errorf("panel %q aggregates a memory high-water mark without restricting to "+
					"RunInstanceID == 0, so a rerun's value is the maximum over ALL of the job's "+
					"runs:\n  %s", p.Title, sql)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no memory panel found to check -- this assertion would pass vacuously")
	}
}
