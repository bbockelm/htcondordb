package repl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/collections"
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
	if !strings.Contains(fast, "answered by: index") {
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

// TestAggregateTierLineNamesTheTier pins what the line is for: the server REPORTS which tier
// answered, and each one has to read differently. Inferring it from the scan counts could not: only
// the fall-through scan fills the trailer, so index and columnar both left it zero and printed one
// undifferentiated "index / columnar accelerator" -- which reads as good news and hid a constrained
// COUNT(*) answered from per-segment metadata at ten times the cost of the columnar count beside it.
func TestAggregateTierLineNamesTheTier(t *testing.T) {
	// Counts that would, under the old inference, have read as "no segment opened, no record read"
	// for every one of these tiers.
	zero := collections.ScanStats{}

	lines := map[collections.AggTier]string{}
	for _, tier := range []collections.AggTier{
		collections.TierMatchNone,
		collections.TierIndex,
		collections.TierColumnar,
		collections.TierScan,
	} {
		s := zero
		s.Tier = tier
		lines[tier] = aggregateTierLine(s)
	}

	for tier, line := range lines {
		for other, otherLine := range lines {
			if tier != other && line == otherLine {
				t.Errorf("tiers %v and %v render identically: %q", tier, other, line)
			}
		}
	}

	// The index line has to say the thing that makes it actionable. A reader who sees "index" and
	// assumes it is the fast answer draws exactly the wrong conclusion for a selective predicate:
	// its cost follows the segment count, so it loses to a scan that visits only the matches.
	if !strings.Contains(lines[collections.TierIndex], "SEGMENTS") {
		t.Errorf("the index line does not say what drives its cost: %q", lines[collections.TierIndex])
	}

	// A server too old to report a tier must not be made to look like one that did.
	old := aggregateTierLine(zero)
	if !strings.Contains(old, "unreported") {
		t.Errorf("an unreported tier should say so, got %q", old)
	}
	for tier, line := range lines {
		if old == line {
			t.Errorf("an unreported tier renders as %v: %q", tier, old)
		}
	}
}
