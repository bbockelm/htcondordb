package repl

import (
	"strings"
	"testing"
)

// TestCaseExpression covers SQL CASE in a projected column: both forms, several arms, a
// missing ELSE, nesting, and composition inside a larger expression.
func TestCaseExpression(t *testing.T) {
	e, cleanup := aggExprExec(t) // alice: Cpus 4/2 Mem 100/200; bob: Cpus 8/2 Mem 300/400
	defer cleanup()

	for _, tc := range []struct {
		sql  string
		want []string // one value per row, in row order
	}{
		// Searched form.
		{`SELECT CASE WHEN Cpus > 2 THEN 'big' ELSE 'small' END AS sz FROM jobs ORDER BY Mem`,
			[]string{"big", "small", "big", "small"}},
		// Several arms, first match wins.
		{`SELECT CASE WHEN Cpus > 4 THEN 'huge' WHEN Cpus > 2 THEN 'big' ELSE 'small' END AS sz FROM jobs ORDER BY Mem`,
			[]string{"big", "small", "huge", "small"}},
		// Simple form: the operand is compared against each WHEN value.
		{`SELECT CASE Owner WHEN 'alice' THEN 1 WHEN 'bob' THEN 2 ELSE 0 END AS n FROM jobs ORDER BY Mem`,
			[]string{"1", "1", "2", "2"}},
		// No ELSE: SQL's NULL, which is ClassAd's undefined.
		{`SELECT CASE WHEN Cpus > 99 THEN 'x' END AS none FROM jobs ORDER BY Mem`,
			[]string{"undefined", "undefined", "undefined", "undefined"}},
		// Nested CASE in a THEN arm: the inner END must not close the outer.
		{`SELECT CASE WHEN Cpus > 2 THEN CASE WHEN Mem > 250 THEN 'a' ELSE 'b' END ELSE 'c' END AS n FROM jobs ORDER BY Mem`,
			[]string{"b", "c", "a", "c"}},
		// Composes inside arithmetic, so the translation must parenthesize itself.
		{`SELECT 10 * CASE WHEN Cpus > 2 THEN 2 ELSE 1 END AS scaled FROM jobs ORDER BY Mem`,
			[]string{"20", "10", "20", "10"}},
	} {
		r := mustExec(t, e, tc.sql)
		var got []string
		for _, row := range r.Rows {
			got = append(got, row[0])
		}
		if !eqStrs(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.sql, got, tc.want)
		}
	}
}

// TestCaseOverAggregates checks that CASE composes with the aggregate lifting: an aggregate
// inside a CASE arm is hoisted like any other, and the conditional is evaluated per group.
func TestCaseOverAggregates(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT CASE WHEN COUNT(*) > 3 THEN 'busy' ELSE 'quiet' END AS state FROM jobs`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "busy" {
		t.Errorf("aggregate in CASE = %v, want one row 'busy'", r.Rows)
	}

	// Per group, with the aggregate inside the condition.
	r = mustExec(t, e, `SELECT Owner, CASE WHEN SUM(Cpus) > 8 THEN 'heavy' ELSE 'light' END AS w `+
		`FROM jobs GROUP BY Owner`)
	got := rowsByKey(r, 0)
	if got["alice"] != "light" || got["bob"] != "heavy" { // alice 6, bob 10
		t.Errorf("per-group CASE = %v, want alice light and bob heavy", got)
	}

	// SUM(CASE WHEN c THEN 1 ELSE 0 END) is the portable conditional-aggregate spelling; it
	// lowers onto a filtered aggregate (see TestConditionalAggregateLowering). A shape that
	// cannot lower is still refused with a message pointing at FILTER.
	r = mustExec(t, e, `SELECT SUM(CASE WHEN Cpus > 2 THEN 1 ELSE 0 END) AS n FROM jobs`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "2" { // alice 4 and bob 8 exceed 2
		t.Errorf("SUM(CASE ...) = %v, want one row of 2", r.Rows)
	}
	_, err := e.ExecString(`SELECT SUM(CASE WHEN Cpus > 2 THEN Cpus + 1 ELSE 0 END) FROM jobs`)
	if err == nil || !strings.Contains(err.Error(), "FILTER") {
		t.Errorf("un-lowerable SUM(CASE ...) error = %v, want it to point at FILTER", err)
	}
}

// TestSQLSpellingsInExpressions covers the SQL forms an administrator types inside an
// expression. Every one of these was a parse error before -- except a single-quoted string,
// which ClassAd read as a quoted attribute name and silently evaluated to undefined.
func TestSQLSpellingsInExpressions(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	for _, tc := range []struct {
		sql  string
		want string // the single count value
	}{
		{`SELECT COUNT(*) FROM jobs WHERE Owner = 'alice'`, "2"},
		{`SELECT COUNT(*) FROM jobs WHERE Owner <> 'alice'`, "2"},
		{`SELECT COUNT(*) FROM jobs WHERE Cpus > 2 AND Owner = 'bob'`, "1"},
		{`SELECT COUNT(*) FROM jobs WHERE Cpus > 4 OR Owner = 'alice'`, "3"},
		{`SELECT COUNT(*) FROM jobs WHERE NOT (Cpus > 2)`, "2"},
		{`SELECT COUNT(*) FROM jobs WHERE Missing IS NULL`, "4"},
		{`SELECT COUNT(*) FROM jobs WHERE Owner IS NOT NULL`, "4"},
		// CASE in a WHERE clause, and the ClassAd spellings still work unchanged.
		{`SELECT COUNT(*) FROM jobs WHERE CASE WHEN Cpus > 2 THEN 1 ELSE 0 END == 1`, "2"},
		{`SELECT COUNT(*) FROM jobs WHERE Owner == "alice" && Cpus >= 4`, "1"},
	} {
		r := mustExec(t, e, tc.sql)
		if len(r.Rows) != 1 || r.Rows[0][0] != tc.want {
			t.Errorf("%s: got %v, want %s", tc.sql, r.Rows, tc.want)
		}
	}

	// A single-quoted string in a projected expression is a string, not an attribute name.
	r := mustExec(t, e, `SELECT 'literal' AS s FROM jobs LIMIT 1`)
	if r.Rows[0][0] != "literal" {
		t.Errorf("single-quoted literal = %q, want the string", r.Rows[0][0])
	}
}

// TestSQLSpellingTranslation pins the translation itself, so the generated ClassAd is
// reviewable without running a query.
func TestSQLSpellingTranslation(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT 1 FROM t WHERE a = 'x'`, `a == "x"`},
		{`SELECT 1 FROM t WHERE a <> 1 AND b > 2 OR NOT c`, `a != 1 && b > 2 || ! c`},
		{`SELECT 1 FROM t WHERE a IS NULL`, `a is undefined`},
		{`SELECT 1 FROM t WHERE a IS NOT NULL`, `a isnt undefined`},
		// Already-ClassAd text passes through byte for byte.
		{`SELECT 1 FROM t WHERE a =?= "x" && member(b, c)`, `a =?= "x" && member(b, c)`},
		// A quote inside a SQL string ('' escape) round-trips into a ClassAd escape.
		{`SELECT 1 FROM t WHERE a = 'it''s'`, `a == "it's"`},
	} {
		st, err := Parse(tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		if st.Where != tc.want {
			t.Errorf("%s:\n  got  %s\n  want %s", tc.sql, st.Where, tc.want)
		}
	}

	// CASE translates to a right-nested ClassAd conditional, parenthesized so it composes.
	st, err := Parse(`SELECT CASE WHEN a > 1 THEN 'x' WHEN a > 0 THEN 'y' ELSE 'z' END FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	const want = `((a > 1) ? ("x") : ((a > 0) ? ("y") : ("z")))`
	if st.Items[0].Expr != want {
		t.Errorf("CASE translation:\n  got  %s\n  want %s", st.Items[0].Expr, want)
	}
	// The header keeps the SQL the user typed, not the translation.
	if h := st.Items[0].Header(); !strings.HasPrefix(h, "CASE WHEN") {
		t.Errorf("header = %q, want the source CASE text", h)
	}

	// The simple form compares the operand against each WHEN value.
	st, err = Parse(`SELECT CASE s WHEN 'a' THEN 1 END FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Items[0].Expr; got != `(((s) == ("a")) ? (1) : (undefined))` {
		t.Errorf("simple CASE translation = %s", got)
	}
}

// TestCaseErrors checks that a malformed CASE is reported as such.
func TestCaseErrors(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT CASE WHEN a > 1 'x' END FROM t`, "expected THEN"},
		{`SELECT CASE WHEN a > 1 THEN 'x' FROM t`, "expected END"},
		{`SELECT CASE ELSE 'x' END FROM t`, "expected at least one WHEN"},
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

// IN now lowers to ClassAd's member() rather than being refused, but only where the left
// operand is still in hand: as a whole conjunct of a WHERE clause. Everywhere else the old
// refusal stands, and it still has to name the spelling that works.
//
// NOT IN is the case that mattered most: NOT used to translate to `!` on its own, so
// `Owner NOT IN ('alice')` became `Owner ! IN (...)` and surfaced as a ClassAd syntax error
// pointing at the `!`.
func TestInLowersToMemberInAConjunct(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`SELECT Owner FROM ads WHERE Owner IN ('alice', 'bob')`,
			`member(Owner, {"alice", "bob"})`},
		{`SELECT Owner FROM ads WHERE ClusterId IN (1, 2)`, `member(ClusterId, {1, 2})`},
		{`SELECT Owner FROM ads WHERE Owner NOT IN ('alice')`, `!member(Owner, {"alice"})`},
		// The left operand may be any expression, not just an attribute.
		{`SELECT Owner FROM ads WHERE strcat(a, b) IN ('ab')`,
			`member(strcat(a, b), {"ab"})`},
		// It is one conjunct among others, in its own position.
		{`SELECT Owner FROM ads WHERE JobStatus == 5 AND Owner IN ('a') AND Cpus > 1`,
			`JobStatus == 5 && member(Owner, {"a"}) && Cpus > 1`},
	} {
		st, err := Parse(tc.query)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		if st.Where != tc.want {
			t.Errorf("%s:\n  Where = %q\n  want    %q", tc.query, st.Where, tc.want)
		}
	}
	// The member() spelling the refusal used to point at still works unchanged.
	if _, err := Parse(`SELECT Owner FROM ads WHERE member(Owner, {"alice", "bob"})`); err != nil {
		t.Errorf("member() spelling: %v", err)
	}
}

// Outside a WHERE conjunct the left operand has already been written through to the output
// verbatim and cannot be moved inside a call, so IN is still refused -- with the spelling
// that works, as before.
func TestInIsStillRefusedWhereTheOperandIsGone(t *testing.T) {
	for _, q := range []string{
		`SELECT Owner IN ('a') FROM ads`,
		`SELECT Owner FROM ads WHERE (Owner IN ('a'))`,
	} {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("accepted %s", q)
			continue
		}
		if !strings.Contains(err.Error(), "member(") {
			t.Errorf("%s:\n  error %q\n  does not name the member() spelling", q, err)
		}
	}
}

// IN binds tighter than every comparison and than OR, but the capture that produces its left
// operand runs to the end of a top-level conjunct. An operand that reached too far left used
// to be built into the call -- `member(a == 1 || Owner, {"a"})` -- answering a different
// question with no symptom. BETWEEN is read the same way and had the same hole.
func TestPostfixKeywordRefusesAnOperandThatReachesTooFar(t *testing.T) {
	for _, tc := range []struct{ query, kw string }{
		{`SELECT Owner FROM ads WHERE a == 1 OR Owner IN ('a')`, "IN"},
		{`SELECT Owner FROM ads WHERE a > 1 AND b == 2 OR Owner NOT IN ('a')`, "IN"},
		{`SELECT * FROM a JOIN b ON a.k == b.k AND a.x == 1 OR b.t BETWEEN 1 AND 2`, "BETWEEN"},
	} {
		_, err := Parse(tc.query)
		if err == nil {
			t.Errorf("accepted %s", tc.query)
			continue
		}
		if !strings.Contains(err.Error(), tc.kw+" binds tighter than") {
			t.Errorf("%s:\n  error %q\n  does not explain the precedence", tc.query, err)
		}
	}
	// The operand a parenthesis really does delimit is still accepted.
	for _, q := range []string{
		`SELECT Owner FROM ads WHERE (a == 1) AND Owner IN ('a')`,
		`SELECT * FROM a JOIN b ON a.k == b.k AND b.t BETWEEN 1 AND 2`,
	} {
		if _, err := Parse(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}
