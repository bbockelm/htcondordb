package repl

import (
	"fmt"
	"strings"
	"testing"
)

// aggExprExec seeds a small jobs table: two owners, four rows, with numeric attributes to
// aggregate over.
func aggExprExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	for _, v := range []string{
		"('1.0', 'alice', 4, 100)",
		"('1.1', 'alice', 2, 200)",
		"('2.0', 'bob', 8, 300)",
		"('2.1', 'bob', 2, 400)",
	} {
		mustExec(t, e, "INSERT INTO jobs (Key, Owner, Cpus, Mem) VALUES "+v)
	}
	return e, cleanup
}

// TestSelectAggregateExpressions covers the point of the feature: an aggregate may appear
// anywhere inside a SELECT expression, and the surrounding arithmetic is evaluated once per
// group. The residual is a real ClassAd expression, so the ClassAd language comes with it.
func TestSelectAggregateExpressions(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	for _, tc := range []struct {
		sql  string
		cols []string
		rows [][]string
	}{
		// The reported shape: a constant times an aggregate.
		{`SELECT 2 * COUNT(*) FROM jobs`, []string{"2 * COUNT(*)"}, [][]string{{"8"}}},
		// An aggregate leading the expression -- the parser must not stop at the call.
		{`SELECT SUM(Cpus) / COUNT(*) AS avg_cpus FROM jobs`, []string{"avg_cpus"}, [][]string{{"4"}}},
		{`SELECT MAX(Mem) - MIN(Mem) AS spread FROM jobs`, []string{"spread"}, [][]string{{"300"}}},
		// Several aggregates, mixed with constants, in one expression.
		{`SELECT (SUM(Cpus) + COUNT(*)) * 10 AS n FROM jobs`, []string{"n"}, [][]string{{"200"}}},
		// ClassAd conditional over an aggregate.
		{`SELECT COUNT(*) > 3 ? "busy" : "quiet" AS state FROM jobs`, []string{"state"}, [][]string{{"busy"}}},
		// A bare aggregate still takes the unchanged path.
		{`SELECT COUNT(*) FROM jobs`, []string{"COUNT(*)"}, [][]string{{"4"}}},
	} {
		r := mustExec(t, e, tc.sql)
		if !eqStrs(r.Columns, tc.cols) {
			t.Errorf("%s: columns = %v, want %v", tc.sql, r.Columns, tc.cols)
		}
		if len(r.Rows) != len(tc.rows) {
			t.Errorf("%s: %d rows, want %d (%v)", tc.sql, len(r.Rows), len(tc.rows), r.Rows)
			continue
		}
		for i := range tc.rows {
			if !eqStrs(r.Rows[i], tc.rows[i]) {
				t.Errorf("%s: row %d = %v, want %v", tc.sql, i, r.Rows[i], tc.rows[i])
			}
		}
	}
}

// TestSelectAggregateExpressionsGrouped checks that the expression is evaluated per GROUP,
// and that a group column can appear alongside -- and inside -- the expression.
func TestSelectAggregateExpressionsGrouped(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT Owner, SUM(Cpus) / COUNT(*) AS avg FROM jobs GROUP BY Owner`)
	got := rowsByKey(r, 0)
	if got["alice"] != "3" || got["bob"] != "5" {
		t.Errorf("per-group average = %v, want alice 3 and bob 5", got)
	}

	// The group column is in scope for the expression itself.
	r = mustExec(t, e, `SELECT Owner, strcat(Owner, ":", string(COUNT(*))) AS tag FROM jobs GROUP BY Owner`)
	got = rowsByKey(r, 0)
	if got["alice"] != "alice:2" || got["bob"] != "bob:2" {
		t.Errorf("group column in expression = %v, want alice:2 / bob:2", got)
	}

	// ORDER BY over a plain aggregate must keep working next to an expression column.
	r = mustExec(t, e, `SELECT Owner, 2 * SUM(Cpus) AS d FROM jobs GROUP BY Owner ORDER BY Owner DESC`)
	if len(r.Rows) != 2 || r.Rows[0][0] != "bob" || r.Rows[0][1] != "20" {
		t.Errorf("ordered rows = %v, want bob first with 20", r.Rows)
	}
}

// TestSelectAggregateExpressionErrors pins the shapes that must still be refused, so the
// looser SELECT grammar does not quietly accept nonsense.
func TestSelectAggregateExpressionErrors(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	for _, tc := range []struct{ sql, want string }{
		// An expression over aggregates is group-level, so it cannot mix with a plain
		// column unless that column is grouped.
		{`SELECT Owner, 2 * COUNT(*) FROM jobs`, "without GROUP BY"},
		{`SELECT Owner, Cpus, 2 * COUNT(*) FROM jobs GROUP BY Owner`, "must appear in GROUP BY"},
		// SUM(*) is not meaningful.
		{`SELECT 2 * SUM(*) FROM jobs`, "needs an attribute"},
		// A view stores raw metrics; there is nowhere to keep the arithmetic.
		{`CREATE MATERIALIZED VIEW v AS SELECT Owner, 2 * COUNT(*) FROM jobs GROUP BY Owner`,
			"expressions over aggregates"},
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

// TestAggregateExprParse checks the lifting itself: the expression keeps the aggregate's
// position, the calls come out in source order, and the column header is the source text.
func TestAggregateExprParse(t *testing.T) {
	st, err := Parse(`SELECT 2 * COUNT(*) + SUM(Cpus) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(st.Items))
	}
	it := st.Items[0]
	if it.Expr != "2 * __agg_0 + __agg_1" {
		t.Errorf("expression = %q, want the aggregates replaced by placeholders in order", it.Expr)
	}
	if len(it.Aggs) != 2 || it.Aggs[0].Func != "COUNT" || it.Aggs[0].Arg != "*" ||
		it.Aggs[1].Func != "SUM" || it.Aggs[1].Arg != "Cpus" {
		t.Errorf("lifted aggregates = %+v, want COUNT(*) then SUM(Cpus)", it.Aggs)
	}
	if it.header() != "2 * COUNT(*) + SUM(Cpus)" {
		t.Errorf("header = %q, want the source text", it.header())
	}
	if !it.GroupLevel() || it.IsAggregate() {
		t.Errorf("an expression over aggregates is group-level but not a bare aggregate")
	}

	// A bare aggregate is unchanged: no lifting, no expression.
	st, err = Parse(`SELECT COUNT(*) AS n FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	if it := st.Items[0]; !it.IsAggregate() || it.Expr != "" || len(it.Aggs) != 0 {
		t.Errorf("bare aggregate = %+v, want Agg set with no lifting", it)
	}

	// An expression with no aggregates stays a per-row expression.
	st, err = Parse(`SELECT Cpus * 2 FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	if it := st.Items[0]; it.GroupLevel() || it.Expr != "Cpus * 2" {
		t.Errorf("per-row expression = %+v, want no lifting and not group-level", it)
	}
}

func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// rowsByKey indexes a result's rows by the value in column k, mapping it to the next column.
func rowsByKey(r *Result, k int) map[string]string {
	out := map[string]string{}
	for _, row := range r.Rows {
		if len(row) > k+1 {
			out[row[k]] = row[k+1]
		}
	}
	return out
}

// TestSelectAggregateExpressionsBucketed covers the time-series wiring, where the group tuple
// is addressed by POSITION rather than by name -- a separate projector setup from the plain
// GROUP BY path, and the one the Grafana/dashboard queries use.
func TestSelectAggregateExpressionsBucketed(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE jobs")
	for i, q := range []string{"1700000000", "1700000100", "1700003700", "1700003800"} {
		mustExec(t, e, fmt.Sprintf(
			"INSERT INTO jobs (Key, QDate, Cpus) VALUES ('k%d', %s, 4)", i, q))
	}
	const bucket = `time_bucket(QDate, '1h')`

	r := mustExec(t, e, `SELECT `+bucket+` AS t, 2 * COUNT(*) AS n FROM jobs GROUP BY `+bucket)
	if !eqStrs(r.Columns, []string{"t", "n"}) {
		t.Fatalf("columns = %v, want [t n]", r.Columns)
	}
	got := rowsByKey(r, 0)
	if len(got) != 2 {
		t.Fatalf("buckets = %v, want two", got)
	}
	for b, n := range got {
		if n != "4" { // two rows per bucket, doubled
			t.Errorf("bucket %s = %s, want 4", b, n)
		}
	}

	// A ratio over two aggregates, per bucket.
	r = mustExec(t, e, `SELECT `+bucket+` AS t, SUM(Cpus) / COUNT(*) AS avg FROM jobs GROUP BY `+bucket)
	for b, avg := range rowsByKey(r, 0) {
		if avg != "4" {
			t.Errorf("bucket %s average = %s, want 4", b, avg)
		}
	}
}
