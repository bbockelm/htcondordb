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

// TestExplainAnalyzeBucketedTier pins the property the bucketed EXPLAIN ANALYZE was missing: a
// bucketed aggregate the archive answers out of its per-segment indexes and one that falls to a
// per-record scan must RENDER DIFFERENTLY. The two differ by orders of magnitude in wall time on a
// real history table while printing the same plan line (both say "columnar aggregate on the
// archive"), and before the bucketed path asked for the stats trailer the slow one printed strictly
// less than the fast one -- no numbers at all, which reads as "nothing to see here".
//
// The pair is the same query with and without a WHERE: the index path can attribute whole segments
// to a (categorical, bucketed-numeric) group tuple only for an unconstrained COUNT(*), since a
// constraint cannot be proven to hold for an entire segment, so adding a filter forces the scan.
func TestExplainAnalyzeBucketedTier(t *testing.T) {
	const days, perDay = 4, 60
	e, cleanup, _ := archiveExecBucketed(t, days, perDay)
	defer cleanup()

	const sel = `SELECT Owner, time_bucket(CompletionDate, '1d') AS day, COUNT(*) FROM history `
	const grp = `GROUP BY Owner, time_bucket(CompletionDate, '1d')`
	fast := explainText(t, e, "EXPLAIN ANALYZE "+sel+grp)
	slow := explainText(t, e, "EXPLAIN ANALYZE "+sel+`WHERE JobStatus == 4 `+grp)

	// The regression this guards against: the bucketed path calling the stats-free opcode, so the
	// report falls back to the apology line and carries no breakdown at all.
	for name, out := range map[string]string{"unconstrained": fast, "constrained": slow} {
		if strings.Contains(out, "no per-segment scan breakdown") {
			t.Errorf("%s bucketed aggregate reported no scan breakdown:\n%s", name, out)
		}
		if !strings.Contains(out, "scan: segments scanned=") {
			t.Errorf("%s bucketed aggregate is missing the scan line:\n%s", name, out)
		}
	}
	if !strings.Contains(fast, "answered by: index / columnar accelerator") {
		t.Errorf("the unconstrained bucketed aggregate should be answered without reading records:\n%s", fast)
	}
	if !strings.Contains(fast, "records visited=0") {
		t.Errorf("the unconstrained bucketed aggregate read records:\n%s", fast)
	}
	if !strings.Contains(slow, "answered by: record scan") {
		t.Errorf("the constrained bucketed aggregate should report the record scan that answered it:\n%s", slow)
	}
	// Not just "nonzero": the scan reads every retained record, which is the cost the report exists
	// to make visible.
	if want := fmt.Sprintf("records visited=%d", days*perDay); !strings.Contains(slow, want) {
		t.Errorf("the constrained bucketed aggregate should report %q:\n%s", want, slow)
	}
	if fast == slow {
		t.Errorf("the fast and slow bucketed aggregates rendered identically:\n%s", fast)
	}
}
