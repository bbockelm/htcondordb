package repl

import (
	"strings"
	"testing"
)

// The shape dashboards actually ask for: group the joined rows and count them.
func TestJoinAggregateGroupBy(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, COUNT(*) FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner ORDER BY j.Owner`)
	wantRows(t, r, "alice|1", "bob|1")
	if r.Columns[1] != "COUNT(*)" {
		t.Errorf("second column = %q, want COUNT(*)", r.Columns[1])
	}
}

// An aggregate over an attribute of the JOINED side -- the thing the old refusal was about,
// since the argument has to say which table it comes from.
func TestJoinAggregateOverRightSideAttribute(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, SUM(h.ExitCode), MAX(h.ExitCode), COUNT(h.ExitCode)
		FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
		GROUP BY j.Owner ORDER BY j.Owner`)
	// alice joins hist h1 (ExitCode 0); bob joins h2 (ExitCode 1).
	wantRows(t, r, "alice|0|0|1", "bob|1|1|1")
}

func TestJoinAggregateNoGroupBy(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`)
	wantRows(t, r, "2")
}

// A one-to-many join: the aggregate must count run attempts, not jobs.
func TestJoinAggregateOneToMany(t *testing.T) {
	e, cleanup := newEpochExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT h.Owner, COUNT(*) AS attempts
		FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
		GROUP BY h.Owner ORDER BY h.Owner`)
	// alice: cluster 1 ran once, cluster 2 ran twice = 3. bob: cluster 3 once.
	wantRows(t, r, "alice|3", "bob|1")
}

func TestJoinAggregateHaving(t *testing.T) {
	e, cleanup := newEpochExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT h.Owner, COUNT(*) AS attempts
		FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
		GROUP BY h.Owner HAVING COUNT(*) > 1`)
	wantRows(t, r, "alice|3")
}

// HAVING on a grouped column of a join, which reads as a qualified path rather than a bare
// attribute name -- the scope check has to recognise it as grouped.
func TestJoinAggregateHavingOnGroupColumn(t *testing.T) {
	e, cleanup := newEpochExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT h.Owner, COUNT(*) FROM history h
		JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
		GROUP BY h.Owner HAVING h.Owner == "bob"`)
	wantRows(t, r, "bob|1")
}

// An aggregate expression over a join, with the aggregates lifted out of it.
func TestJoinAggregateInsideExpression(t *testing.T) {
	e, cleanup := newEpochExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT h.Owner, COUNT(*) * 10 AS scaled FROM history h
		JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
		GROUP BY h.Owner ORDER BY h.Owner`)
	wantRows(t, r, "alice|30", "bob|10")
}

// A conditional aggregate: one pass answering two differently-filtered questions over a join.
func TestJoinAggregateFilter(t *testing.T) {
	e, cleanup := newEpochExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT h.Owner, COUNT(*) AS total,
		COUNT(*) FILTER (WHERE e.ExitCode == 0) AS ok
		FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
		GROUP BY h.Owner ORDER BY h.Owner`)
	// alice has 3 attempts, 2 with ExitCode 0; bob has 1, exit 0.
	wantRows(t, r, "alice|3|2", "bob|1|1")
}

// Over a single table an aggregate still takes a bare attribute name, because the store
// aggregates over an attribute and has no dotted path to resolve.
func TestQualifiedAggregateArgRefusedWithoutAJoin(t *testing.T) {
	_, err := Parse(`SELECT SUM(a.b) FROM jobs`)
	if err == nil {
		t.Fatal("accepted a qualified aggregate argument over one table")
	}
	if !strings.Contains(err.Error(), "only meaningful in a join") {
		t.Fatalf("error %q does not explain why", err)
	}
}

// The grouped join is reduced here, not on the server, and EXPLAIN has to say so rather
// than implying a pushdown that does not exist.
func TestJoinAggregateExplainSaysClientSide(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `EXPLAIN SELECT j.Owner, COUNT(*) FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`)
	var text strings.Builder
	for _, row := range r.Rows {
		text.WriteString(strings.Join(row, " "))
		text.WriteByte('\n')
	}
	if !strings.Contains(text.String(), "client-side") {
		t.Errorf("EXPLAIN does not say where the grouping runs:\n%s", text.String())
	}
}
