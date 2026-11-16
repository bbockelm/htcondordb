package repl

import (
	"sort"
	"strings"
	"testing"
)

// A subquery predicate is lifted out of the WHERE clause into Subqueries, and what is left
// of the clause is the predicate the scan runs.
func TestSubqueryPredicatesAreLiftedOutOfWhere(t *testing.T) {
	for _, tc := range []struct {
		query      string
		where      string
		kind       SubqueryKind
		expr       string
		innerFrom  string
		innerWhere string
	}{
		{
			query:      `SELECT Owner FROM jobs WHERE EXISTS (SELECT 1 FROM epoch_history e WHERE e.GlobalJobId == jobs.GlobalJobId)`,
			where:      "",
			kind:       SubqueryExists,
			innerFrom:  "epoch_history",
			innerWhere: "e.GlobalJobId == jobs.GlobalJobId",
		},
		{
			query:      `SELECT ClusterId FROM jobs WHERE JobStatus == 5 AND NOT EXISTS (SELECT 1 FROM e WHERE e.g == jobs.g)`,
			where:      "JobStatus == 5",
			kind:       SubqueryNotExists,
			innerFrom:  "e",
			innerWhere: "e.g == jobs.g",
		},
		{
			query:      `SELECT * FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Priority > 100)`,
			where:      "",
			kind:       SubqueryIn,
			expr:       "Owner",
			innerFrom:  "users",
			innerWhere: "Priority > 100",
		},
		{
			query:      `SELECT * FROM jobs WHERE Owner NOT IN (SELECT Owner FROM users) AND Cpus > 1`,
			where:      "Cpus > 1",
			kind:       SubqueryNotIn,
			expr:       "Owner",
			innerFrom:  "users",
			innerWhere: "",
		},
	} {
		st, err := Parse(tc.query)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		if st.Where != tc.where {
			t.Errorf("%s:\n  residual Where = %q, want %q", tc.query, st.Where, tc.where)
		}
		if len(st.Subqueries) != 1 {
			t.Errorf("%s: lifted %d subqueries, want 1", tc.query, len(st.Subqueries))
			continue
		}
		sub := st.Subqueries[0]
		if sub.Kind != tc.kind {
			t.Errorf("%s: kind = %v, want %v", tc.query, sub.Kind, tc.kind)
		}
		if sub.Expr != tc.expr {
			t.Errorf("%s: left operand = %q, want %q", tc.query, sub.Expr, tc.expr)
		}
		if sub.Inner.Table != tc.innerFrom {
			t.Errorf("%s: inner FROM = %q, want %q", tc.query, sub.Inner.Table, tc.innerFrom)
		}
		if sub.Inner.Where != tc.innerWhere {
			t.Errorf("%s: inner WHERE = %q, want %q", tc.query, sub.Inner.Where, tc.innerWhere)
		}
		if sub.Raw == "" {
			t.Errorf("%s: the predicate's raw source was not kept", tc.query)
		}
	}
}

// A statement may carry more than one, and they keep their source order.
func TestSeveralSubqueryPredicatesKeepSourceOrder(t *testing.T) {
	st, err := Parse(`SELECT * FROM jobs WHERE EXISTS (SELECT 1 FROM a WHERE a.k == jobs.k) ` +
		`AND Cpus > 1 AND NOT EXISTS (SELECT 1 FROM b WHERE b.k == jobs.k)`)
	if err != nil {
		t.Fatal(err)
	}
	if st.Where != "Cpus > 1" {
		t.Errorf("residual Where = %q, want %q", st.Where, "Cpus > 1")
	}
	if len(st.Subqueries) != 2 {
		t.Fatalf("lifted %d subqueries, want 2", len(st.Subqueries))
	}
	if st.Subqueries[0].Kind != SubqueryExists || st.Subqueries[0].Inner.Table != "a" {
		t.Errorf("first = %v over %s", st.Subqueries[0].Kind, st.Subqueries[0].Inner.Table)
	}
	if st.Subqueries[1].Kind != SubqueryNotExists || st.Subqueries[1].Inner.Table != "b" {
		t.Errorf("second = %v over %s", st.Subqueries[1].Kind, st.Subqueries[1].Inner.Table)
	}
}

// Lifting a conjunct out of a clause that is not a pure AND-spine would answer a different
// query: with `a OR EXISTS (...)` and `c`, the clause means `a || (EXISTS && c)`, and the
// lifted form would answer `(a || c) && EXISTS`.
func TestSubqueryInsideAnOrIsRefused(t *testing.T) {
	for _, q := range []string{
		`SELECT * FROM jobs WHERE a == 1 OR EXISTS (SELECT 1 FROM o)`,
		`SELECT * FROM jobs WHERE EXISTS (SELECT 1 FROM o) OR a == 1`,
		`SELECT * FROM jobs WHERE a == 1 OR Owner IN (SELECT Owner FROM o)`,
	} {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("accepted %s", q)
			continue
		}
		if !strings.Contains(err.Error(), "conjunct") {
			t.Errorf("%s:\n  error %q\n  does not say the predicate must be a conjunct", q, err)
		}
	}
}

func TestSubqueryParseRefusals(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`DELETE FROM jobs WHERE EXISTS (SELECT 1 FROM o)`, "only supported in a SELECT"},
		{`UPDATE jobs SET a = 1 WHERE Owner IN (SELECT Owner FROM o)`, "only supported in a SELECT"},
		{`SELECT * FROM jobs WHERE EXISTS (Owner == 1)`, "does not open one"},
		{`SELECT * FROM jobs WHERE EXISTS (SELECT 1 FROM o`, "closing parenthesis"},
		{`SELECT * FROM jobs WHERE EXISTS`, "parenthesized subquery"},
		{`SELECT * FROM jobs WHERE Owner IN ()`, "at least one value"},
	} {
		_, err := Parse(tc.query)
		if err == nil {
			t.Errorf("accepted %s", tc.query)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s:\n  error %q\n  want it to mention %q", tc.query, err, tc.want)
		}
	}
}

// An inner SELECT ends at its closing parenthesis, not at the end of the input, so the
// clauses after it belong to the OUTER statement.
func TestSubqueryEndsAtItsParenthesis(t *testing.T) {
	st, err := Parse(`SELECT Owner FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Prio > 1) ` +
		`GROUP BY Owner ORDER BY Owner LIMIT 7`)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.GroupBy) != 1 || st.GroupBy[0] != "Owner" {
		t.Errorf("outer GROUP BY = %v", st.GroupBy)
	}
	if len(st.OrderBy) != 1 {
		t.Errorf("outer ORDER BY = %v", st.OrderBy)
	}
	if st.Limit != 7 {
		t.Errorf("outer LIMIT = %d, want 7", st.Limit)
	}
	if len(st.Subqueries) != 1 || st.Subqueries[0].Inner.Limit != 0 {
		t.Errorf("the inner statement took the outer clauses: %+v", st.Subqueries)
	}
}

// A predicate that is parsed and then not applied would run the statement with a filter
// missing and answer with MORE rows than it asked for, which reads as success. That is why
// the resolution walks the whole statement tree rather than reading the outermost statement:
// a subquery in a UNION branch or under an EXPLAIN is as much a subquery as one in a bare
// SELECT, and each of these queries returns strictly more rows if its predicate is dropped.
func TestSubqueriesNestedInOtherStatementsAreResolved(t *testing.T) {
	e, cleanup := subqueryFixture(t)
	defer cleanup()

	// Branch two is empty, so every row of the answer comes through the predicate. Drop
	// the predicate and bob and carol appear.
	const inAlice = `SELECT Owner FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Priority > 100)`
	for _, tc := range []struct{ name, query string }{
		{"subquery in the first branch",
			inAlice + ` UNION SELECT Owner FROM users WHERE Priority < 0`},
		{"subquery in the second branch",
			`SELECT Owner FROM users WHERE Priority < 0 UNION ` + inAlice},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := e.ExecString(tc.query)
			if err != nil {
				t.Fatalf("%s: %v", tc.query, err)
			}
			var got []string
			for _, row := range r.Rows {
				got = append(got, row[0])
			}
			sort.Strings(got)
			if strings.Join(got, ",") != "alice" {
				t.Errorf("%s:\n  got %v, want [alice]", tc.query, got)
			}
		})
	}

	// EXPLAIN reports a plan for the constraint the statement will actually run, which
	// means resolving the predicate first.
	if _, err := e.ExecString(`EXPLAIN ` + inAlice); err != nil {
		t.Errorf("EXPLAIN over a subquery: %v", err)
	}
}
