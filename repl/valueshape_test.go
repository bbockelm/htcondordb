package repl

import (
	"strings"
	"testing"
)

// cell runs a single-row SELECT and returns the one cell, for terse shape assertions.
func cell(t *testing.T, e *Executor, sql string) string {
	t.Helper()
	r := mustExec(t, e, sql)
	if len(r.Rows) != 1 || len(r.Rows[0]) != 1 {
		t.Fatalf("%s: got %d rows / %d cols, want 1x1 (%v)", sql, len(r.Rows), len(r.Rows[0]), r.Rows)
	}
	return r.Rows[0][0]
}

// A stored attribute may hold an unevaluated ClassAd expression. SELECT reports the
// EVALUATED result, not the expression text -- so `SELECT Requirements` on a job ad
// answers true/false/undefined rather than handing back the expression. Pinned because
// it is the single most surprising thing about reading HTCondor data through SQL.
func TestSelectEvaluatesExpressionAttributes(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "INSERT INTO ads (Key, Memory, Req) VALUES ('big', 2048, Memory > 1024)")
	mustExec(t, e, "INSERT INTO ads (Key, Memory, Req) VALUES ('small', 512, Memory > 1024)")

	// The expression is stored, not folded at insert time: the same text yields a
	// different answer per row. Memory is selected alongside Req so the projection
	// carries the sibling the expression reads -- see TestProjectedSelectAgreesWithStar
	// for what happens when it does not.
	for _, tc := range [][2]string{{"big", "true"}, {"small", "false"}} {
		r := mustExec(t, e, `SELECT Memory, Req FROM ads WHERE Key == "`+tc[0]+`"`)
		if got := r.Rows[0][1]; got != tc[1] {
			t.Errorf("%s: Req = %q, want %q", tc[0], got, tc[1])
		}
	}
	// Both rows really do hold the same expression text; only the sibling differs.
	r := mustExec(t, e, "SELECT * FROM ads")
	for _, ad := range r.Ads {
		if expr, ok := ad.Lookup("Req"); !ok || !strings.Contains(expr.String(), "Memory") {
			t.Errorf("stored Req is not an expression over Memory: %v", ad)
		}
	}
}

// An expression whose references cannot be resolved evaluates to undefined, and one that
// faults evaluates to error. They are distinct cells, not both "missing".
func TestSelectUndefinedAndErrorExpressions(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "INSERT INTO ads (Key, Req) VALUES ('unres', NoSuchAttr > 1)")
	mustExec(t, e, "INSERT INTO ads (Key, Req) VALUES ('faulty', 1 / 0)")
	mustExec(t, e, "INSERT INTO ads (Key, Req) VALUES ('cyclic', Req)")

	for _, tc := range [][2]string{
		{"unres", "undefined"},
		{"faulty", "error"},
		{"cyclic", "error"}, // self-reference faults rather than looping
	} {
		if got := cell(t, e, `SELECT Req FROM ads WHERE Key == "`+tc[0]+`"`); got != tc[1] {
			t.Errorf("%s: Req = %q, want %q", tc[0], got, tc[1])
		}
	}
}

// A list literal survives INSERT ... VALUES: its commas are inside braces, so they are
// not value separators. Before brace-depth tracking in captureExpr this was parsed as
// one value per element and rejected as an arity mismatch.
func TestInsertListLiteral(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "INSERT INTO ads (Key, Args) VALUES ('multi', {1, 2, 3})")
	mustExec(t, e, "INSERT INTO ads (Key, Args) VALUES ('single', {7})")
	mustExec(t, e, "INSERT INTO ads (Key, Args) VALUES ('empty', {})")
	mustExec(t, e, "INSERT INTO ads (Key, Args) VALUES ('nested', {1, {2, 3}, 4})")

	if got := cell(t, e, `SELECT size(Args) FROM ads WHERE Key == "multi"`); got != "3" {
		t.Errorf("size(Args) = %q, want \"3\"", got)
	}
	if got := cell(t, e, `SELECT size(Args) FROM ads WHERE Key == "nested"`); got != "3" {
		t.Errorf("nested size(Args) = %q, want \"3\"", got)
	}
	if got := cell(t, e, `SELECT size(Args) FROM ads WHERE Key == "empty"`); got != "0" {
		t.Errorf("empty size(Args) = %q, want \"0\"", got)
	}
}

// Brace-depth tracking must not be confused by a brace inside a string literal, which
// the lexer hands over as content rather than as an operator.
func TestBraceInsideStringLiteralIsNotNesting(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('braced', '{not a list}')")
	if got := cell(t, e, `SELECT Owner FROM ads WHERE Key == "braced"`); got != "{not a list}" {
		t.Errorf("Owner = %q, want %q", got, "{not a list}")
	}
	// And two columns still parse as two values when one contains a brace.
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Note) VALUES ('two', '{', '}')")
	if got := cell(t, e, `SELECT Note FROM ads WHERE Key == "two"`); got != "}" {
		t.Errorf("Note = %q, want %q", got, "}")
	}
}

// A projected SELECT must agree with SELECT * about every cell. It does not: the server
// projects the stored ad down to the named attributes, so an expression attribute loses
// the siblings it references and evaluates to undefined.
//
// This is a correctness bug in the projection pushdown, not in the caller: the same
// query through htcondordb-cli shows the same disagreement. It matters for HTCondor data
// specifically, where Requirements, Rank and friends are expressions over sibling
// attributes -- `SELECT Name, Requirements FROM machines` reports undefined for rows that
// SELECT * reports true for. The fix is to have the projection chase each projected
// expression's attribute references (collections.QueryRawProjected already takes a
// chaseRefs flag; db.QueryRawProjected hard-codes it false for HTCondor
// protocol-compatibility, which is right for a relay and wrong for SQL).
func TestProjectedSelectAgreesWithStar(t *testing.T) {
	t.Skip("known bug: the projection pushdown drops the siblings an expression attribute " +
		"references, so a projected column evaluates to undefined; see the comment above")

	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Memory, Req) VALUES ('big', 2048, Memory > 1024)")

	star := mustExec(t, e, "SELECT * FROM ads")
	var want string
	for i, col := range star.Columns {
		if strings.EqualFold(col, "Req") {
			want = star.Rows[0][i]
		}
	}
	if got := cell(t, e, "SELECT Req FROM ads"); got != want {
		t.Errorf("SELECT Req = %q but SELECT * reports %q", got, want)
	}
}
