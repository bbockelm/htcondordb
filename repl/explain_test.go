package repl

import (
	"fmt"
	"strings"
	"testing"
)

func explainText(t *testing.T, e *Executor, sql string) string {
	t.Helper()
	r := mustExec(t, e, sql)
	if len(r.Columns) != 1 || r.Columns[0] != "QUERY PLAN" {
		t.Fatalf("EXPLAIN columns = %v, want [QUERY PLAN]", r.Columns)
	}
	var b strings.Builder
	for _, row := range r.Rows {
		b.WriteString(row[0])
		b.WriteByte('\n')
	}
	return b.String()
}

func mustContain(t *testing.T, hay, needle string) {
	t.Helper()
	if !strings.Contains(hay, needle) {
		t.Errorf("plan missing %q in:\n%s", needle, hay)
	}
}

func TestExplainSelect(t *testing.T) {
	e, cleanup := filterExec(t) // table "jobs" with Owner/JobStatus/Cpus
	defer cleanup()

	// Projected row scan with ORDER BY + LIMIT -- the user's shape: limit must NOT push down.
	plan := explainText(t, e, "EXPLAIN SELECT Owner, JobStatus FROM jobs WHERE JobStatus == 2 ORDER BY Cpus DESC LIMIT 1")
	mustContain(t, plan, `SELECT on table "jobs"`)
	mustContain(t, plan, "filter: JobStatus == 2")
	mustContain(t, plan, "projected row scan")
	mustContain(t, plan, "limit: 1 applied client-side (after ORDER BY")
	mustContain(t, plan, "order: client-side sort")

	// Plain projected scan with a bare LIMIT -- limit pushes down.
	plan = explainText(t, e, "EXPLAIN SELECT Owner FROM jobs WHERE Owner == \"alice\" LIMIT 3")
	mustContain(t, plan, "limit: 3 pushed to the server")

	// Aggregate path.
	plan = explainText(t, e, "EXPLAIN SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner")
	mustContain(t, plan, "aggregate")
}

func TestExplainAnalyze(t *testing.T) {
	e, cleanup := filterExec(t)
	defer cleanup()

	plan := explainText(t, e, "EXPLAIN ANALYZE SELECT Owner, JobStatus FROM jobs WHERE JobStatus == 2 ORDER BY Cpus DESC LIMIT 1")
	mustContain(t, plan, "projected row scan")
	mustContain(t, plan, "time: total=")
	mustContain(t, plan, "fetch=")
	mustContain(t, plan, "rows: fetched=")
	// JobStatus==2 matches 2 of the 7 rows; ORDER BY+LIMIT means all matches fetched, 1 returned.
	mustContain(t, plan, "returned=1")
}

func TestExplainRejectsNonSelect(t *testing.T) {
	e, cleanup := filterExec(t)
	defer cleanup()
	if _, err := e.ExecString("EXPLAIN INSERT INTO jobs (Key, Owner) VALUES ('9', 'z')"); err == nil {
		t.Fatal("EXPLAIN of INSERT should error")
	}
	if _, err := e.ExecString("EXPLAIN EXPLAIN SELECT Owner FROM jobs"); err == nil {
		t.Fatal("nested EXPLAIN should error")
	}
	_ = fmt.Sprint
}
