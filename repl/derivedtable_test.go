package repl

import (
	"strings"
	"testing"
)

// A common table expression is refused by name, rather than as an unknown statement keyword.
func TestACommonTableExpressionIsRefusedByName(t *testing.T) {
	_, err := Parse(`WITH t AS (SELECT Owner FROM jobs) SELECT Owner FROM t`)
	if err == nil {
		t.Fatal("accepted a CTE")
	}
	for _, want := range []string{"common table expression", "materialized view"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// The CTE refusal must match the keyword WITH, not every statement starting with those
// letters -- otherwise a typo is reported as an unsupported construct the reader never used.
func TestAWordBeginningWithWITHIsStillAnUnknownStatement(t *testing.T) {
	_, err := Parse(`WITHOUT`)
	if err == nil {
		t.Fatal("accepted WITHOUT")
	}
	if strings.Contains(err.Error(), "common table expression") {
		t.Errorf("a typo was reported as a CTE: %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported statement") {
		t.Errorf("expected the generic unknown-statement error, got: %v", err)
	}
}

// The language already had a WITH: the trailing option list on a materialized view. Refusing a
// LEADING one must not touch it. The two never occupy the same position, and this is what holds
// that apart -- a refusal matched too early would make every continuous aggregate unparseable.
func TestAViewsTrailingWITHStillParses(t *testing.T) {
	st, err := Parse(`CREATE MATERIALIZED VIEW jobs_ts AS
		SELECT time_bucket(QDate,'5m') AS time, COUNT(*) AS metric_jobs
		FROM jobs GROUP BY time_bucket(QDate,'5m') WITH (grace = '10m', retention = '30d')`)
	if err != nil {
		t.Fatal(err)
	}
	if st.ViewGrace != 600 || st.ViewRetention != 2592000 {
		t.Fatalf("grace=%d retention=%d, want 600, 2592000", st.ViewGrace, st.ViewRetention)
	}
}

// A derived table that flattens must answer exactly what the merged query answers written by
// hand. That is the whole claim of flattening: the two are the same statement.
func TestADerivedTableAnswersAsTheFlatQueryDoes(t *testing.T) {
	store, _, _, cleanup := newSubqueryPair(t)
	defer cleanup()

	for _, c := range []struct{ name, derived, flat string }{
		{"projection only",
			`SELECT Owner FROM (SELECT Owner FROM jobs) t ORDER BY Owner`,
			`SELECT Owner FROM jobs ORDER BY Owner`},
		{"inner filter",
			`SELECT Owner FROM (SELECT Owner FROM jobs WHERE JobStatus == 5) t ORDER BY Owner`,
			`SELECT Owner FROM jobs WHERE JobStatus == 5 ORDER BY Owner`},
		{"both filters",
			`SELECT Owner FROM (SELECT Owner, JobStatus FROM jobs WHERE JobStatus == 5) t
				WHERE Owner == "alice" ORDER BY Owner`,
			`SELECT Owner FROM jobs WHERE JobStatus == 5 AND Owner == "alice" ORDER BY Owner`},
		{"renamed column",
			`SELECT o FROM (SELECT Owner AS o FROM jobs) t WHERE o == "alice" ORDER BY o`,
			`SELECT Owner FROM jobs WHERE Owner == "alice" ORDER BY Owner`},
		{"renamed column, qualified",
			`SELECT t.o FROM (SELECT Owner AS o FROM jobs) t WHERE t.o == "alice" ORDER BY t.o`,
			`SELECT Owner FROM jobs WHERE Owner == "alice" ORDER BY Owner`},
		{"select star",
			`SELECT Owner FROM (SELECT * FROM jobs WHERE JobStatus == 5) t ORDER BY Owner`,
			`SELECT Owner FROM jobs WHERE JobStatus == 5 ORDER BY Owner`},
		{"outer aggregate over it",
			`SELECT COUNT(*) FROM (SELECT Owner FROM jobs WHERE JobStatus == 5) t`,
			`SELECT COUNT(*) FROM jobs WHERE JobStatus == 5`},
		{"outer group by over it",
			`SELECT Owner, COUNT(*) FROM (SELECT Owner FROM jobs) t GROUP BY Owner ORDER BY Owner`,
			`SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner ORDER BY Owner`},
		{"as a join side",
			`SELECT j.Owner FROM jobs j
				JOIN (SELECT ClusterId FROM epochs WHERE Restarts > 0) d ON j.ClusterId == d.ClusterId
				ORDER BY j.Owner`,
			`SELECT j.Owner FROM jobs j JOIN epochs d ON j.ClusterId == d.ClusterId
				WHERE d.Restarts > 0 ORDER BY j.Owner`},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := setOpRows(t, store, c.derived)
			want := setOpRows(t, store, c.flat)
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Errorf("the derived form answers differently\n  derived %v\n  flat    %v", got, want)
			}
		})
	}
}

// A derived table on the nullable side of a LEFT JOIN keeps the outer rows that match nothing.
//
// Its filter applies to that side BEFORE the pairing, so it belongs in the ON clause. Hoisting
// it to the outer WHERE would reject the unmatched rows and turn the LEFT JOIN into an inner
// one -- a wrong answer that reads like a plan choice, which is why both halves are asserted:
// it must equal the ON spelling AND differ from the WHERE spelling.
func TestADerivedTableOnTheNullableSideKeepsUnmatchedRows(t *testing.T) {
	store, _, _, cleanup := newSubqueryPair(t)
	defer cleanup()

	derived := setOpRows(t, store, `SELECT j.Owner FROM jobs j
		LEFT JOIN (SELECT ClusterId FROM epochs WHERE Restarts > 0) d ON j.ClusterId == d.ClusterId
		ORDER BY j.Owner`)
	onForm := setOpRows(t, store, `SELECT j.Owner FROM jobs j
		LEFT JOIN epochs d ON j.ClusterId == d.ClusterId AND d.Restarts > 0
		ORDER BY j.Owner`)
	whereForm := setOpRows(t, store, `SELECT j.Owner FROM jobs j
		LEFT JOIN epochs d ON j.ClusterId == d.ClusterId
		WHERE d.Restarts > 0 ORDER BY j.Owner`)

	if strings.Join(derived, "|") != strings.Join(onForm, "|") {
		t.Errorf("the derived form is not the ON spelling\n  derived %v\n  ON      %v", derived, onForm)
	}
	if strings.Join(derived, "|") == strings.Join(whereForm, "|") {
		t.Fatalf("the two spellings answer alike here, so this proves nothing: %v", derived)
	}
}

// Each shape that cannot flatten is refused by the clause that stops it, because the remedy
// differs: a grouped derived table becomes a view, while a LIMIT is usually a habit to drop.
func TestADerivedTableThatCannotFlattenIsRefusedByClause(t *testing.T) {
	for _, c := range []struct{ clause, q string }{
		{"aggregates", `SELECT n FROM (SELECT COUNT(*) AS n FROM jobs) t`},
		{"has a GROUP BY", `SELECT Owner FROM (SELECT Owner FROM jobs GROUP BY Owner) t`},
		{"is DISTINCT", `SELECT Owner FROM (SELECT DISTINCT Owner FROM jobs) t`},
		{"has a LIMIT", `SELECT Owner FROM (SELECT Owner FROM jobs LIMIT 10) t`},
		{"is itself a join", `SELECT Owner FROM (SELECT j.Owner FROM jobs j
			JOIN epochs e ON j.ClusterId == e.ClusterId) t`},
		{"carries a subquery predicate", `SELECT Owner FROM (SELECT Owner FROM jobs
			WHERE ClusterId IN (SELECT ClusterId FROM epochs)) t`},
	} {
		t.Run(c.clause, func(t *testing.T) {
			_, err := Parse(c.q)
			if err == nil {
				t.Fatal("accepted a derived table that cannot flatten")
			}
			if !strings.Contains(err.Error(), c.clause) {
				t.Errorf("the refusal does not name %q: %v", c.clause, err)
			}
		})
	}
}

// A bare ORDER BY inside a derived table is DROPPED, not refused: a derived table is a
// relation, so the outer statement's own ordering is what survives.
func TestADerivedTablesOwnOrderingIsDropped(t *testing.T) {
	store, _, _, cleanup := newSubqueryPair(t)
	defer cleanup()
	got := setOpRows(t, store, `SELECT Owner FROM (SELECT Owner FROM jobs ORDER BY JobStatus) t
		ORDER BY Owner`)
	want := setOpRows(t, store, `SELECT Owner FROM jobs ORDER BY Owner`)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("dropping the inner ordering changed the answer\n  got  %v\n  want %v", got, want)
	}
}

// A column the derived table does not expose is an error where it is written, not an
// undefined value at evaluation time.
func TestAColumnTheDerivedTableDoesNotExposeIsRefused(t *testing.T) {
	for _, q := range []string{
		`SELECT JobStatus FROM (SELECT Owner FROM jobs) t`,
		`SELECT Owner FROM (SELECT Owner FROM jobs) t WHERE JobStatus == 1`,
		`SELECT t.JobStatus FROM (SELECT Owner FROM jobs) t`,
	} {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("accepted a column the derived table does not expose: %s", q)
			continue
		}
		if !strings.Contains(err.Error(), "not a column of the derived table") {
			t.Errorf("unexpected error for %s: %v", q, err)
		}
	}
}

// A derived table has no name of its own, so one must be given.
func TestADerivedTableNeedsAName(t *testing.T) {
	_, err := Parse(`SELECT Owner FROM (SELECT Owner FROM jobs)`)
	if err == nil {
		t.Fatal("accepted an unnamed derived table")
	}
	if !strings.Contains(err.Error(), "needs a name") {
		t.Errorf("the refusal does not ask for a name: %v", err)
	}
}
