package repl

import (
	"fmt"
	"strings"
	"testing"
)

// sizeCase is the bucketing expression these tests group by.
const sizeCase = `CASE WHEN Memory > 4096 THEN 'big' ELSE 'small' END`

// groupExprExec seeds five jobs across two owners with a spread of memory sizes:
//
//	u0: 1024, 8192, 512   u1: 2048, 16384      -> big {8192,16384}, small {1024,512,2048}
func groupExprExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	for i, m := range []int{1024, 2048, 8192, 16384, 512} {
		mustExec(t, e, fmt.Sprintf(
			"INSERT INTO jobs (Key, Owner, Memory) VALUES ('k%d', 'u%d', %d)", i, i%2, m))
	}
	return e, cleanup
}

// TestGroupByExpression covers grouping on a computed key, named either by its alias or by
// repeating the expression -- the two spellings must mean the same thing.
func TestGroupByExpression(t *testing.T) {
	e, cleanup := groupExprExec(t)
	defer cleanup()

	want := map[string]string{"big": "2", "small": "3"}
	for _, sql := range []string{
		`SELECT ` + sizeCase + ` AS sz, COUNT(*) AS n FROM jobs GROUP BY sz`,
		`SELECT ` + sizeCase + ` AS sz, COUNT(*) AS n FROM jobs GROUP BY ` + sizeCase,
		`SELECT ` + sizeCase + `, COUNT(*) FROM jobs GROUP BY ` + sizeCase, // no alias at all
	} {
		r := mustExec(t, e, sql)
		got := rowsByKey(r, 0)
		if len(got) != 2 || got["big"] != want["big"] || got["small"] != want["small"] {
			t.Errorf("%s: got %v, want %v", sql, got, want)
		}
	}

	// Arithmetic, not just CASE.
	r := mustExec(t, e, `SELECT Memory / 1024 AS gb, COUNT(*) AS n FROM jobs GROUP BY gb ORDER BY gb`)
	if len(r.Rows) != 5 { // 0, 1, 2, 8, 16
		t.Errorf("Memory/1024 groups = %v, want five distinct sizes", r.Rows)
	}
}

// TestGroupByAliasOfPlainColumn is the regression for the bug this feature nearly shipped:
// an alias for a PLAIN column passed validation and was then sent to the server as an
// attribute name, which no ad has -- one group holding every row, with an empty key.
func TestGroupByAliasOfPlainColumn(t *testing.T) {
	e, cleanup := groupExprExec(t)
	defer cleanup()

	aliased := mustExec(t, e, `SELECT Owner AS o, COUNT(*) AS n FROM jobs GROUP BY o`)
	plain := mustExec(t, e, `SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner`)
	if len(aliased.Rows) != 2 {
		t.Fatalf("GROUP BY <alias> produced %v, want the same two groups as GROUP BY Owner", aliased.Rows)
	}
	if a, p := rowsByKey(aliased, 0), rowsByKey(plain, 0); a["u0"] != p["u0"] || a["u1"] != p["u1"] {
		t.Errorf("alias grouping %v != attribute grouping %v", a, p)
	}
	// The alias is resolved to the attribute, so the plan carries a groupable name.
	st, err := Parse(`SELECT Owner AS o, COUNT(*) FROM jobs GROUP BY o`)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.GroupBy) != 1 || !strings.EqualFold(st.GroupBy[0], "Owner") {
		t.Errorf("GroupBy = %v, want it resolved to Owner", st.GroupBy)
	}
}

// TestGroupByExpressionComposes checks that a computed group key works alongside a plain one,
// and with HAVING and ORDER BY over the result.
func TestGroupByExpressionComposes(t *testing.T) {
	e, cleanup := groupExprExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT Owner, `+sizeCase+` AS sz, COUNT(*) AS n FROM jobs GROUP BY Owner, sz`)
	if len(r.Rows) != 4 { // u0 big/small, u1 big/small
		t.Errorf("two-key grouping = %v, want four groups", r.Rows)
	}

	r = mustExec(t, e, `SELECT `+sizeCase+` AS sz, COUNT(*) AS n FROM jobs GROUP BY sz HAVING COUNT(*) > 2`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "small" {
		t.Errorf("HAVING over a computed group = %v, want just small", r.Rows)
	}

	r = mustExec(t, e, `SELECT `+sizeCase+` AS sz, COUNT(*) AS n FROM jobs GROUP BY sz ORDER BY n DESC LIMIT 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "small" {
		t.Errorf("ordered/limited = %v, want the larger group", r.Rows)
	}
}

// TestGroupByExpressionErrors keeps the existing rules in force: a projected column still has
// to be grouped, and a computed key has to be projected (the grouping is client-side over the
// projected items, so a term that is not selected has nothing to group by).
func TestGroupByExpressionErrors(t *testing.T) {
	e, cleanup := groupExprExec(t)
	defer cleanup()

	for _, tc := range []struct{ sql, want string }{
		{`SELECT Memory / 1024 AS gb, COUNT(*) FROM jobs GROUP BY Owner`, "must appear in GROUP BY"},
		{`SELECT ` + sizeCase + ` AS sz, COUNT(*) FROM jobs GROUP BY sz, Owner`, "must also be selected"},
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

// TestGroupByExpressionParse pins that a repeated expression and its alias resolve to the
// same group term, so the two spellings cannot drift apart.
func TestGroupByExpressionParse(t *testing.T) {
	byAlias, err := Parse(`SELECT ` + sizeCase + ` AS sz, COUNT(*) FROM jobs GROUP BY sz`)
	if err != nil {
		t.Fatal(err)
	}
	byExpr, err := Parse(`SELECT ` + sizeCase + ` AS sz, COUNT(*) FROM jobs GROUP BY ` + sizeCase)
	if err != nil {
		t.Fatal(err)
	}
	if len(byAlias.GroupBy) != 1 || len(byExpr.GroupBy) != 1 {
		t.Fatalf("group terms: %v vs %v", byAlias.GroupBy, byExpr.GroupBy)
	}
	if byAlias.GroupBy[0] != byExpr.GroupBy[0] {
		t.Errorf("alias resolved to %q but the expression captured %q; the two must agree",
			byAlias.GroupBy[0], byExpr.GroupBy[0])
	}
	if byAlias.GroupBy[0] != byAlias.Items[0].Expr {
		t.Errorf("group term %q does not match the item's expression %q",
			byAlias.GroupBy[0], byAlias.Items[0].Expr)
	}
}

// TestGroupByExpressionOnArchive covers the combination the archive bucketed pushdown made
// possible: an archive now groups time buckets server-side, but a COMPUTED key still cannot
// be pushed down, so it must fall to the client-side path on a history table too.
func TestGroupByExpressionOnArchive(t *testing.T) {
	e, cleanup := newArchiveExec(t) // "history": ClusterId 1,2,3; Owner alice,bob,alice
	defer cleanup()

	r := mustExec(t, e, `SELECT CASE WHEN ClusterId > 1 THEN 'high' ELSE 'low' END AS c, `+
		`COUNT(*) AS n FROM history GROUP BY c`)
	got := rowsByKey(r, 0)
	if got["high"] != "2" || got["low"] != "1" {
		t.Errorf("archive computed grouping = %v, want high 2 and low 1", got)
	}

	// An alias for a plain column resolves to the attribute, so it keeps the server path.
	r = mustExec(t, e, `SELECT Owner AS o, COUNT(*) AS n FROM history GROUP BY o`)
	got = rowsByKey(r, 0)
	if got["alice"] != "2" || got["bob"] != "1" {
		t.Errorf("archive alias grouping = %v, want alice 2 and bob 1", got)
	}
}
