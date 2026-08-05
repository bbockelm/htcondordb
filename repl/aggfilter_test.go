package repl

import (
	"fmt"
	"strings"
	"testing"
)

// filterExec seeds a status/owner mix -- the shape a per-status dashboard pivots over.
//
//	alice: 2 completed (4), 1 running (2), 1 held (5)   Cpus 1,2,4,8
//	bob:   1 completed,     2 running                   Cpus 1,2,16
func filterExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	rows := []struct {
		key, owner string
		status     int
		cpus       int
	}{
		{"1", "alice", 4, 1}, {"2", "alice", 4, 2}, {"3", "alice", 2, 4}, {"4", "alice", 5, 8},
		{"5", "bob", 4, 1}, {"6", "bob", 2, 2}, {"7", "bob", 2, 16},
	}
	for _, r := range rows {
		mustExec(t, e, fmt.Sprintf(
			"INSERT INTO jobs (Key, Owner, JobStatus, Cpus) VALUES ('%s', '%s', %d, %d)",
			r.key, r.owner, r.status, r.cpus))
	}
	return e, cleanup
}

// TestAggregateFilterPivot is the query the feature exists for: totals and per-status counts
// side by side, from one pass instead of one query per status.
func TestAggregateFilterPivot(t *testing.T) {
	e, cleanup := filterExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT Owner, COUNT(*) AS total, `+
		`COUNT(*) FILTER (WHERE JobStatus = 2) AS running, `+
		`COUNT(*) FILTER (WHERE JobStatus = 4) AS done `+
		`FROM jobs GROUP BY Owner`)
	if !eqStrs(r.Columns, []string{"Owner", "total", "running", "done"}) {
		t.Fatalf("columns = %v", r.Columns)
	}
	got := map[string][]string{}
	for _, row := range r.Rows {
		got[row[0]] = row[1:]
	}
	want := map[string][]string{"alice": {"4", "1", "2"}, "bob": {"3", "2", "1"}}
	for owner, w := range want {
		if !eqStrs(got[owner], w) {
			t.Errorf("%s = %v, want %v", owner, got[owner], w)
		}
	}
}

// TestAggregateFilterForms covers the filter on each aggregate function and with no grouping.
func TestAggregateFilterForms(t *testing.T) {
	e, cleanup := filterExec(t)
	defer cleanup()

	for _, tc := range []struct{ sql, want string }{
		{`SELECT COUNT(*) FILTER (WHERE JobStatus == 2) FROM jobs`, "3"},
		{`SELECT SUM(Cpus) FILTER (WHERE JobStatus = 2) FROM jobs`, "22"},
		{`SELECT MAX(Cpus) FILTER (WHERE JobStatus = 2) FROM jobs`, "16"},
		{`SELECT MIN(Cpus) FILTER (WHERE JobStatus = 4) FROM jobs`, "1"},
		// The filter is a full expression, with the SQL spellings translated as elsewhere.
		{`SELECT COUNT(*) FILTER (WHERE JobStatus = 2 OR JobStatus = 5) FROM jobs`, "4"},
		{`SELECT COUNT(*) FILTER (WHERE Owner = 'bob' AND Cpus > 1) FROM jobs`, "2"},
		{`SELECT COUNT(*) FILTER (WHERE Missing IS NULL) FROM jobs`, "7"},
		// A filter matching nothing counts zero rather than dropping the row.
		{`SELECT COUNT(*) FILTER (WHERE JobStatus = 99) FROM jobs`, "0"},
	} {
		r := mustExec(t, e, tc.sql)
		if len(r.Rows) != 1 || r.Rows[0][0] != tc.want {
			t.Errorf("%s = %v, want %s", tc.sql, r.Rows, tc.want)
		}
	}
}

// TestAggregateFilterComposes checks that a filtered aggregate is just an aggregate: it can
// sit inside an expression column and inside a HAVING.
func TestAggregateFilterComposes(t *testing.T) {
	e, cleanup := filterExec(t)
	defer cleanup()

	// The running-percentage-per-owner query, which needs both features at once.
	r := mustExec(t, e, `SELECT Owner, 100 * COUNT(*) FILTER (WHERE JobStatus = 2) / COUNT(*) AS pct `+
		`FROM jobs GROUP BY Owner`)
	got := rowsByKey(r, 0)
	if got["alice"] != "25" || got["bob"] != "66" { // 1/4 and 2/3
		t.Errorf("percentages = %v, want alice 25 and bob 66", got)
	}

	// HAVING over a filtered aggregate.
	r = mustExec(t, e, `SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner `+
		`HAVING COUNT(*) FILTER (WHERE JobStatus = 2) > 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "bob" {
		t.Errorf("HAVING over a filtered aggregate = %v, want just bob", r.Rows)
	}
}

// TestConditionalAggregateLowering covers the portable spelling: the CASE forms that are
// provably the same question lower onto a filtered aggregate, and the ones that are not are
// refused rather than guessed at.
func TestConditionalAggregateLowering(t *testing.T) {
	e, cleanup := filterExec(t)
	defer cleanup()

	for _, tc := range []struct{ sql, want string }{
		// THEN 1 / ELSE 0 counts the matching rows.
		{`SELECT SUM(CASE WHEN JobStatus = 2 THEN 1 ELSE 0 END) FROM jobs`, "3"},
		// THEN <attr> / ELSE 0 sums that attribute over the matching rows.
		{`SELECT SUM(CASE WHEN JobStatus = 2 THEN Cpus ELSE 0 END) FROM jobs`, "22"},
		// No ELSE: SUM skips undefined, so it is the same question.
		{`SELECT SUM(CASE WHEN JobStatus = 2 THEN Cpus END) FROM jobs`, "22"},
		{`SELECT COUNT(CASE WHEN JobStatus = 2 THEN Cpus END) FROM jobs`, "3"},
	} {
		r := mustExec(t, e, tc.sql)
		if len(r.Rows) != 1 || r.Rows[0][0] != tc.want {
			t.Errorf("%s = %v, want %s", tc.sql, r.Rows, tc.want)
		}
	}

	// The lowering must produce the same answer as the FILTER spelling it stands for.
	a := mustExec(t, e, `SELECT SUM(CASE WHEN JobStatus = 2 THEN Cpus ELSE 0 END) FROM jobs`)
	b := mustExec(t, e, `SELECT SUM(Cpus) FILTER (WHERE JobStatus = 2) FROM jobs`)
	if a.Rows[0][0] != b.Rows[0][0] {
		t.Errorf("lowered %s != FILTER %s", a.Rows[0][0], b.Rows[0][0])
	}

	for _, tc := range []struct{ sql, want string }{
		// A non-zero ELSE is a different question.
		{`SELECT SUM(CASE WHEN JobStatus = 2 THEN 1 ELSE 9 END) FROM jobs`, "ELSE 0"},
		// An arithmetic THEN cannot be pushed to the store.
		{`SELECT SUM(CASE WHEN JobStatus = 2 THEN Cpus + 1 ELSE 0 END) FROM jobs`, "FILTER"},
		// A second arm is not a single condition.
		{`SELECT SUM(CASE WHEN JobStatus = 2 THEN 1 WHEN JobStatus = 4 THEN 2 ELSE 0 END) FROM jobs`, "ELSE 0"},
		// An aggregate inside an aggregate.
		{`SELECT SUM(CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END) FROM jobs`, "cannot appear inside another"},
	} {
		_, err := e.ExecString(tc.sql)
		if err == nil {
			t.Errorf("%s: expected an error", tc.sql)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q, want it to mention %q", tc.sql, err, tc.want)
		}
	}
}

// TestAggregateFilterParse pins the grammar and the lowering at the statement level, so the
// translation is reviewable without running a query.
func TestAggregateFilterParse(t *testing.T) {
	st, err := Parse(`SELECT COUNT(*) FILTER (WHERE JobStatus = 2) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	it := st.Items[0]
	if !it.IsAggregate() || it.Agg != "COUNT" || it.Col != "*" {
		t.Fatalf("item = %+v, want a bare COUNT(*)", it)
	}
	if it.AggFilter != "JobStatus == 2" {
		t.Errorf("filter = %q, want the SQL `=` translated", it.AggFilter)
	}
	if h := it.header(); !strings.Contains(h, "FILTER (WHERE") {
		t.Errorf("header = %q, want the filter shown", h)
	}

	// The CASE spelling lowers to exactly the filtered call it stands for.
	st, err = Parse(`SELECT SUM(CASE WHEN JobStatus = 2 THEN Cpus ELSE 0 END) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	it = st.Items[0]
	if it.Agg != "SUM" || it.Col != "Cpus" || it.AggFilter != "JobStatus == 2" {
		t.Errorf("lowered item = %+v, want SUM(Cpus) FILTER (WHERE JobStatus == 2)", it)
	}
	// THEN 1 counts rows, so it lowers to COUNT(*) regardless of the outer function.
	st, err = Parse(`SELECT SUM(CASE WHEN JobStatus = 2 THEN 1 ELSE 0 END) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	if it := st.Items[0]; it.Agg != "COUNT" || it.Col != "*" || it.AggFilter != "JobStatus == 2" {
		t.Errorf("lowered item = %+v, want COUNT(*) FILTER (WHERE JobStatus == 2)", it)
	}

	// An unfiltered aggregate is untouched.
	st, err = Parse(`SELECT COUNT(*) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	if it := st.Items[0]; it.AggFilter != "" {
		t.Errorf("unfiltered item carries a filter: %+v", it)
	}
}

// TestAggregateFilterMalformed checks the syntax errors are about the syntax.
func TestAggregateFilterMalformed(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT COUNT(*) FILTER JobStatus = 2 FROM jobs`, "expected (WHERE"},
		{`SELECT COUNT(*) FILTER (JobStatus = 2) FROM jobs`, "expected WHERE"},
		{`SELECT COUNT(*) FILTER (WHERE) FROM jobs`, "FILTER"},
	} {
		_, err := Parse(tc.sql)
		if err == nil {
			t.Errorf("%s: expected an error", tc.sql)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q, want it to mention %q", tc.sql, err, tc.want)
		}
	}
}
