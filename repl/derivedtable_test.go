package repl

import (
	"strings"
	"testing"
)

// A derived table is refused by name, wherever it appears in the FROM clause.
//
// The generic parser error this replaces -- `expected an identifier, got "("` -- describes the
// parser's state rather than the reader's query, and leaves them guessing whether they typed
// something wrong or asked for something absent.
func TestADerivedTableIsRefusedByName(t *testing.T) {
	for _, c := range []struct{ name, q string }{
		{"in FROM", `SELECT Owner FROM (SELECT Owner FROM jobs) t`},
		{"in FROM, unaliased", `SELECT Owner FROM (SELECT Owner FROM jobs)`},
		{"as a join side", `SELECT j.Owner FROM jobs j
			JOIN (SELECT ClusterId FROM epochs) t ON j.ClusterId == t.ClusterId`},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse(c.q)
			if err == nil {
				t.Fatal("accepted a derived table")
			}
			for _, want := range []string{"derived table", "materialized view"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not mention %q: %v", want, err)
				}
			}
		})
	}
}

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
