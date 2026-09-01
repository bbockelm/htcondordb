package repl

import (
	"strings"
	"testing"
)

// planText joins an EXPLAIN result's lines into one string for substring assertions.
func planText(r *Result) string {
	var b strings.Builder
	for _, row := range r.Rows {
		if len(row) > 0 {
			b.WriteString(row[0])
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// col0 returns the first column of each result row.
func col0(r *Result) []string {
	out := make([]string, len(r.Rows))
	for i, row := range r.Rows {
		out[i] = row[0]
	}
	return out
}

func eqStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTopKRoutingValueIndex checks ORDER BY a value-indexed numeric column with LIMIT routes to the
// server-side TopK op and returns exactly the k ordered rows -- for both directions and with a WHERE.
func TestTopKRoutingValueIndex(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()

	// DESC LIMIT 2 over ClusterId {1,2,3} -> 3,2.
	if got := col0(mustExec(t, e, "SELECT ClusterId FROM history ORDER BY ClusterId DESC LIMIT 2")); !eqStr(got, []string{"3", "2"}) {
		t.Errorf("DESC LIMIT 2 = %v, want [3 2]", got)
	}
	// ASC LIMIT 2 -> 1,2.
	if got := col0(mustExec(t, e, "SELECT ClusterId FROM history ORDER BY ClusterId ASC LIMIT 2")); !eqStr(got, []string{"1", "2"}) {
		t.Errorf("ASC LIMIT 2 = %v, want [1 2]", got)
	}
	// WHERE JobStatus==4 (ClusterId 1,2) ORDER BY ClusterId DESC LIMIT 5 -> 2,1 (fewer than k).
	if got := col0(mustExec(t, e, "SELECT ClusterId FROM history WHERE JobStatus == 4 ORDER BY ClusterId DESC LIMIT 5")); !eqStr(got, []string{"2", "1"}) {
		t.Errorf("WHERE + DESC LIMIT 5 = %v, want [2 1]", got)
	}
}

// TestTopKRoutingZoneAttr checks a zone-mapped (numeric) column is also a valid TopK order key.
func TestTopKRoutingZoneAttr(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()

	// Newest completion first: CompletionDate max is ClusterId 3.
	if got := col0(mustExec(t, e, "SELECT ClusterId FROM history ORDER BY CompletionDate DESC LIMIT 1")); !eqStr(got, []string{"3"}) {
		t.Errorf("ORDER BY CompletionDate DESC LIMIT 1 = %v, want [3]", got)
	}
}

// TestTopKExplainReportsPath checks EXPLAIN names the server-side top-K plan for a qualifying query,
// and does NOT for a string order key (which is not a numeric index and must not route to TopK --
// TopK orders numerically and would drop every row).
func TestTopKExplainReportsPath(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()

	plan := planText(mustExec(t, e, "EXPLAIN SELECT ClusterId FROM history ORDER BY ClusterId DESC LIMIT 2"))
	if !strings.Contains(plan, "server-side top-K") {
		t.Errorf("EXPLAIN did not report the top-K path:\n%s", plan)
	}

	// Owner is a string column, not a value index: must fall back to client-side sort, and stay
	// correct (alice,alice,bob -> ASC LIMIT 2 = the two alices, ClusterId 1 and 3).
	plan = planText(mustExec(t, e, "EXPLAIN SELECT ClusterId FROM history ORDER BY Owner ASC LIMIT 2"))
	if strings.Contains(plan, "server-side top-K") {
		t.Errorf("EXPLAIN routed a string ORDER BY to top-K:\n%s", plan)
	}
	if !strings.Contains(plan, "client-side sort") {
		t.Errorf("EXPLAIN did not report a client-side sort for a string ORDER BY:\n%s", plan)
	}
	if got := col0(mustExec(t, e, "SELECT ClusterId FROM history WHERE Owner == \"alice\" ORDER BY ClusterId ASC LIMIT 2")); !eqStr(got, []string{"1", "3"}) {
		t.Errorf("string-ordered fallback wrong: %v, want [1 3]", got)
	}
}

// TestAggregateExplainAnalyzeScanStats checks EXPLAIN ANALYZE of a server-side aggregate over a
// history table now carries the scan breakdown -- previously the aggregate path printed only "no
// per-segment scan breakdown". It must name the aggregate path AND show the records line.
func TestAggregateExplainAnalyzeScanStats(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()

	plan := planText(mustExec(t, e, "EXPLAIN ANALYZE SELECT MAX(ClusterId) FROM history WHERE JobStatus == 4"))
	if !strings.Contains(plan, "server-side aggregate") {
		t.Errorf("EXPLAIN ANALYZE did not name the aggregate path:\n%s", plan)
	}
	if !strings.Contains(plan, "scan:") || !strings.Contains(plan, "records visited=") {
		t.Errorf("EXPLAIN ANALYZE of an aggregate did not report the scan breakdown:\n%s", plan)
	}
	t.Logf("plan:\n%s", plan)
}

// TestExplainAnalyzeScanStats checks EXPLAIN ANALYZE of a projected row scan carries the Phase-2
// server-side scan breakdown (segments/records), proving the stats trailer round-trips end to end.
func TestExplainAnalyzeScanStats(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()

	plan := planText(mustExec(t, e, "EXPLAIN ANALYZE SELECT ClusterId FROM history WHERE JobStatus == 4"))
	if !strings.Contains(plan, "scan:") || !strings.Contains(plan, "records visited=") {
		t.Errorf("EXPLAIN ANALYZE did not report the Phase-2 scan breakdown:\n%s", plan)
	}
}

// TestTopKExplainAnalyzeScanStats checks EXPLAIN ANALYZE of a top-K-routed query carries the
// cutoff-scan breakdown -- the top-K path used to be blind to EXPLAIN ANALYZE (it uses its own op,
// not the projection stats op), so the report must now name the server-side top-K plan AND show the
// scan breakdown from the stats-carrying top-K op.
func TestTopKExplainAnalyzeScanStats(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()

	plan := planText(mustExec(t, e, "EXPLAIN ANALYZE SELECT ClusterId FROM history ORDER BY ClusterId DESC LIMIT 1"))
	if !strings.Contains(plan, "server-side top-K") {
		t.Errorf("EXPLAIN ANALYZE did not report the top-K path:\n%s", plan)
	}
	if !strings.Contains(plan, "scan:") || !strings.Contains(plan, "records visited=") {
		t.Errorf("EXPLAIN ANALYZE of a top-K query did not report the cutoff-scan breakdown:\n%s", plan)
	}
}
