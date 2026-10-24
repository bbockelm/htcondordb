package repl

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// newSlowExec serves a seeded "jobs" table with query aggregation on, at the given
// authorization level, and returns a REPL executor against it.
func newSlowExec(t *testing.T, opts dbrpc.ServeOptions) (*Executor, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	tx := jobs.Begin()
	for i := 0; i < 20; i++ {
		ad, perr := classad.Parse(fmt.Sprintf(
			`[ ClusterId = %d; Owner = "alice"; JobStatus = %d; CompletionDate = %d ]`,
			i, i%5, 1700000000+i))
		if perr != nil {
			t.Fatal(perr)
		}
		tx.NewClassAd(fmt.Sprintf("j%d", i), ad)
	}
	if cerr := tx.Commit(); cerr != nil {
		t.Fatal(cerr)
	}

	s := dbrpc.NewServerCatalog(cat)
	s.EnableQueryStats(dbrpc.QueryStatsOptions{})
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), opts) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	return e, func() { c.Close(); s.Close(); cat.Close() }
}

// runSlow runs `.slow <arg>` and returns what it printed, waiting for the daemon-side
// recording (which happens after the query's last frame) to catch up.
func runSlow(t *testing.T, s *session, arg string, wantShapes int) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var buf bytes.Buffer
		if !s.runDiagMeta(&buf, ".slow", arg) {
			t.Fatal(".slow was not handled by runDiagMeta")
		}
		out := buf.String()
		if strings.Count(out, "\n#") >= wantShapes {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf(".slow never reported %d shape(s) within 3s; last output:\n%s", wantShapes, out)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSlowReportsShapesAndExemplars is the end-to-end REPL view: queries that differ only
// in a time literal collapse into one line with a count, and the DAEMON-level view names
// the actual constraints that produced it.
func TestSlowReportsShapesAndExemplars(t *testing.T) {
	e, cleanup := newSlowExec(t, dbrpc.ServeOptions{Privileged: true})
	defer cleanup()
	sess := &session{exec: e, table: "jobs"}

	for _, cutoff := range []int{1700000002, 1700000008, 1700000015} {
		if _, err := e.ExecString(fmt.Sprintf("SELECT Owner FROM jobs WHERE CompletionDate >= %d", cutoff)); err != nil {
			t.Fatal(err)
		}
	}
	out := runSlow(t, sess, "", 1)

	if !strings.Contains(out, "count 3") {
		t.Errorf("the three time-literal variants did not aggregate into one shape:\n%s", out)
	}
	if !strings.Contains(out, "completiondate") {
		t.Errorf("shape line missing the attribute:\n%s", out)
	}
	if strings.Contains(out, "1700000002") && !strings.Contains(out, "slowest:") {
		t.Errorf("a literal leaked into the shape line:\n%s", out)
	}
	if !strings.Contains(out, "slowest:") {
		t.Errorf("no exemplar shown to a DAEMON peer:\n%s", out)
	}
	if !strings.Contains(out, "CompletionDate >= 170000") {
		t.Errorf("the exemplar did not quote a real constraint:\n%s", out)
	}
	// The two fields that make the view diagnostic rather than descriptive.
	if !strings.Contains(out, "projection:") {
		t.Errorf("no projection accounting:\n%s", out)
	}
	if !strings.Contains(out, "segments:") {
		t.Errorf("no scan accounting:\n%s", out)
	}
	if !strings.Contains(out, "TOTAL time") {
		t.Errorf("the ordering is not explained:\n%s", out)
	}
}

// TestSlowSeparatesShapes confirms the REPL view does not merge genuinely different
// queries into the line above.
func TestSlowSeparatesShapes(t *testing.T) {
	e, cleanup := newSlowExec(t, dbrpc.ServeOptions{Privileged: true})
	defer cleanup()
	sess := &session{exec: e, table: "jobs"}

	if _, err := e.ExecString("SELECT Owner FROM jobs WHERE CompletionDate >= 1700000002"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ExecString(`SELECT Owner FROM jobs WHERE Owner == "alice"`); err != nil {
		t.Fatal(err)
	}
	out := runSlow(t, sess, "", 2)
	if !strings.Contains(out, "completiondate") || !strings.Contains(out, "owner") {
		t.Errorf("expected both shapes listed separately:\n%s", out)
	}
	if strings.Contains(out, "count 2") {
		t.Errorf("two different predicates were merged into one shape:\n%s", out)
	}
}

// TestSlowHidesExemplarsFromReader covers the privacy split at the REPL: a READ-level
// session still sees which shapes cost, but not the constraints or the identities, and
// the output says so rather than looking like nothing was recorded.
func TestSlowHidesExemplarsFromReader(t *testing.T) {
	e, cleanup := newSlowExec(t, dbrpc.ServeOptions{ReadOnly: true})
	defer cleanup()
	sess := &session{exec: e, table: "jobs"}

	if _, err := e.ExecString(`SELECT Owner FROM jobs WHERE Owner == "alice"`); err != nil {
		t.Fatal(err)
	}
	out := runSlow(t, sess, "", 1)
	if strings.Contains(out, "slowest:") || strings.Contains(out, `"alice"`) {
		t.Errorf("a READ session was shown exemplars:\n%s", out)
	}
	if !strings.Contains(out, "need DAEMON authorization") {
		t.Errorf("the missing exemplars are unexplained:\n%s", out)
	}
	if !strings.Contains(out, "(owner == ?)") {
		t.Errorf("a READ session should still see the shape:\n%s", out)
	}
}

// TestSlowTableFilter narrows the report to one table.
func TestSlowTableFilter(t *testing.T) {
	e, cleanup := newSlowExec(t, dbrpc.ServeOptions{Privileged: true})
	defer cleanup()
	sess := &session{exec: e, table: "jobs"}
	if _, err := e.ExecString(`SELECT Owner FROM jobs WHERE Owner == "alice"`); err != nil {
		t.Fatal(err)
	}
	out := runSlow(t, sess, "jobs", 1)
	if !strings.Contains(out, "query shapes (jobs)") {
		t.Errorf("the table filter is not reflected in the header:\n%s", out)
	}
}

// TestSlowDisabled reports the off state rather than an empty success.
func TestSlowDisabled(t *testing.T) {
	var buf bytes.Buffer
	showQueryStats(&buf, &dbrpc.QueryStatsReport{}, "", slowDefaultTop)
	if !strings.Contains(buf.String(), "aggregation is off") {
		t.Errorf("a disabled daemon is not reported as such: %q", buf.String())
	}
}

// TestSlowRendersUnprunedScan covers the two accounting lines an operator reads next to
// the duration: how much of the scan was skipped, and whether the client pulled whole ads.
func TestSlowRendersUnprunedScan(t *testing.T) {
	var buf bytes.Buffer
	showQueryStats(&buf, &dbrpc.QueryStatsReport{
		Enabled: true,
		Since:   time.Now().Add(-time.Minute),
		Shapes: []dbrpc.QueryShapeStat{{
			Op: "QueryRawProjRefs", Table: "history", Shape: "(owner == ?)",
			Count: 4, Total: time.Second, Max: 400 * time.Millisecond,
			SegmentsScanned: 400, SegmentsPruned: 0, ScanSamples: 4,
			Unprojected: 4,
		}},
	}, "", slowDefaultTop)
	out := buf.String()
	if !strings.Contains(out, "400 scanned, 0 pruned (0% skipped)") {
		t.Errorf("the scan ratio is not reported:\n%s", out)
	}
	if !strings.Contains(out, "every attribute of every matched ad") {
		t.Errorf("an unprojected read is not called out:\n%s", out)
	}
}
