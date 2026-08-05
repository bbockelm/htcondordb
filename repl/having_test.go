package repl

import (
	"strings"
	"testing"
)

// TestHavingFiltersGroups is the core behaviour: HAVING drops whole groups after aggregation,
// where WHERE would have dropped rows before it.
func TestHavingFiltersGroups(t *testing.T) {
	e, cleanup := aggExprExec(t) // alice: Cpus 4,2 (sum 6); bob: Cpus 8,2 (sum 10)
	defer cleanup()

	for _, tc := range []struct {
		sql  string
		want map[string]string // Owner -> second column, or nil for no rows
	}{
		{`SELECT Owner, SUM(Cpus) AS c FROM jobs GROUP BY Owner HAVING SUM(Cpus) > 8`,
			map[string]string{"bob": "10"}},
		{`SELECT Owner, SUM(Cpus) AS c FROM jobs GROUP BY Owner HAVING SUM(Cpus) > 5`,
			map[string]string{"alice": "6", "bob": "10"}},
		{`SELECT Owner, SUM(Cpus) AS c FROM jobs GROUP BY Owner HAVING SUM(Cpus) > 100`,
			map[string]string{}},
		// An expression over several aggregates.
		{`SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner HAVING SUM(Cpus) / COUNT(*) > 4`,
			map[string]string{"bob": "2"}},
		// A group column is in scope.
		{`SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner HAVING Owner == "bob"`,
			map[string]string{"bob": "2"}},
		// CASE composes into the filter.
		{`SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner ` +
			`HAVING CASE WHEN SUM(Cpus) > 8 THEN 1 ELSE 0 END == 1`,
			map[string]string{"bob": "2"}},
		// An aggregate that appears ONLY in the HAVING, never projected.
		{`SELECT Owner FROM jobs GROUP BY Owner HAVING SUM(Cpus) > 8`,
			map[string]string{"bob": ""}},
	} {
		r := mustExec(t, e, tc.sql)
		got := map[string]string{}
		for _, row := range r.Rows {
			v := ""
			if len(row) > 1 {
				v = row[1]
			}
			got[row[0]] = v
		}
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.sql, got, tc.want)
			continue
		}
		for k, v := range tc.want {
			if got[k] != v {
				t.Errorf("%s: got %v, want %v", tc.sql, got, tc.want)
				break
			}
		}
	}
}

// TestHavingWithoutGroupBy checks the implicit single group: HAVING with no GROUP BY either
// keeps the one aggregate row or removes it entirely.
func TestHavingWithoutGroupBy(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT COUNT(*) FROM jobs HAVING COUNT(*) > 3`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "4" {
		t.Errorf("passing HAVING = %v, want one row of 4", r.Rows)
	}
	r = mustExec(t, e, `SELECT COUNT(*) FROM jobs HAVING COUNT(*) > 100`)
	if len(r.Rows) != 0 {
		t.Errorf("failing HAVING = %v, want no rows", r.Rows)
	}
}

// TestHavingIsNotWhere pins the distinction that motivates the feature: WHERE filters rows
// before grouping and changes the aggregate; HAVING filters groups after and does not.
func TestHavingIsNotWhere(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	// WHERE drops alice's 2-Cpu row, so her SUM changes and her group survives.
	where := mustExec(t, e, `SELECT Owner, SUM(Cpus) AS c FROM jobs WHERE Cpus > 2 GROUP BY Owner`)
	got := rowsByKey(where, 0)
	if got["alice"] != "4" || got["bob"] != "8" {
		t.Errorf("WHERE = %v, want alice 4 and bob 8 (rows filtered before the sum)", got)
	}
	// HAVING leaves the sums alone and removes the group that fails the test.
	having := mustExec(t, e, `SELECT Owner, SUM(Cpus) AS c FROM jobs GROUP BY Owner HAVING SUM(Cpus) > 8`)
	got = rowsByKey(having, 0)
	if len(got) != 1 || got["bob"] != "10" {
		t.Errorf("HAVING = %v, want only bob with the unfiltered sum 10", got)
	}
}

// TestHavingOrderAndLimit checks that HAVING applies before ORDER BY and LIMIT, so a limit
// counts surviving groups rather than groups the filter was about to drop.
func TestHavingOrderAndLimit(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT Owner, SUM(Cpus) AS c FROM jobs GROUP BY Owner `+
		`HAVING SUM(Cpus) > 5 ORDER BY c DESC LIMIT 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "bob" {
		t.Errorf("ordered+limited = %v, want just bob", r.Rows)
	}
	// If the filter ran after the limit, this would return nothing (alice sorts first).
	r = mustExec(t, e, `SELECT Owner, SUM(Cpus) AS c FROM jobs GROUP BY Owner `+
		`HAVING SUM(Cpus) > 8 ORDER BY Owner LIMIT 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "bob" {
		t.Errorf("filter-before-limit = %v, want bob", r.Rows)
	}
}

// TestHavingBucketed covers the time-series path, whose group tuple is positional.
func TestHavingBucketed(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE jobs")
	// Three rows in the first hour bucket, one in the second.
	for i, q := range []string{"1700000000", "1700000100", "1700000200", "1700003700"} {
		mustExec(t, e, "INSERT INTO jobs (Key, QDate, Cpus) VALUES ('k"+string(rune('0'+i))+"', "+q+", 4)")
	}
	const bucket = `time_bucket(QDate, '1h')`

	r := mustExec(t, e, `SELECT `+bucket+` AS t, COUNT(*) AS n FROM jobs GROUP BY `+bucket)
	if len(r.Rows) != 2 {
		t.Fatalf("unfiltered buckets = %v, want two", r.Rows)
	}
	r = mustExec(t, e, `SELECT `+bucket+` AS t, COUNT(*) AS n FROM jobs GROUP BY `+bucket+` HAVING COUNT(*) > 1`)
	if len(r.Rows) != 1 || r.Rows[0][1] != "3" {
		t.Errorf("filtered buckets = %v, want the single 3-row bucket", r.Rows)
	}
}

// TestHavingErrors pins the refusals. The first is the important one: without it the
// reference evaluates to undefined and every group is silently dropped.
func TestHavingErrors(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	for _, tc := range []struct{ sql, want string }{
		// A column that is neither grouped nor aggregated.
		{`SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner HAVING Cpus > 2`,
			"neither a GROUP BY column nor inside an aggregate"},
		// The same, with the hint pointing at WHERE.
		{`SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner HAVING Mem > 100`, "did you mean WHERE"},
		// A plain column with no GROUP BY: the implicit single group has no value for it.
		{`SELECT Owner FROM jobs HAVING COUNT(*) > 1`, "without GROUP BY"},
		// A view is refreshed into stored metrics; a post-aggregation filter has no place.
		{`CREATE MATERIALIZED VIEW v AS SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner HAVING COUNT(*) > 1`,
			"does not support HAVING"},
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

// TestHavingParse pins the grammar: HAVING sits between GROUP BY and ORDER BY, its aggregates
// are lifted like a SELECT item's, and it does not get swallowed into the WHERE clause.
func TestHavingParse(t *testing.T) {
	st, err := Parse(`SELECT Owner, COUNT(*) FROM jobs WHERE Cpus > 2 GROUP BY Owner ` +
		`HAVING SUM(Cpus) > 8 ORDER BY Owner LIMIT 5`)
	if err != nil {
		t.Fatal(err)
	}
	if st.Where != "Cpus > 2" {
		t.Errorf("WHERE = %q, want it to stop at HAVING", st.Where)
	}
	if st.Having != "__agg_0 > 8" {
		t.Errorf("HAVING = %q, want the aggregate lifted", st.Having)
	}
	if len(st.HavingAggs) != 1 || st.HavingAggs[0].Func != "SUM" || st.HavingAggs[0].Arg != "Cpus" {
		t.Errorf("HAVING aggregates = %+v, want SUM(Cpus)", st.HavingAggs)
	}
	if len(st.OrderBy) != 1 || st.Limit != 5 {
		t.Errorf("ORDER BY / LIMIT lost after HAVING: %+v limit=%d", st.OrderBy, st.Limit)
	}

	// The HAVING's aggregates are numbered after the projection's, so an item's own results
	// keep their positions.
	st, err = Parse(`SELECT 2 * COUNT(*) FROM jobs HAVING SUM(Cpus) > 1`)
	if err != nil {
		t.Fatal(err)
	}
	if st.Items[0].Expr != "2 * __agg_0" || st.Having != "__agg_0 > 1" {
		t.Errorf("placeholders = item %q / having %q; each numbering is local to its expression",
			st.Items[0].Expr, st.Having)
	}
	calls := aggCallOrder(st)
	if len(calls) != 2 || calls[0].Func != "COUNT" || calls[1].Func != "SUM" {
		t.Errorf("aggregate order = %+v, want the projection's first then the HAVING's", calls)
	}
}
