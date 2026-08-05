package repl

import (
	"fmt"
	"strings"
	"testing"
)

// windowExec seeds two owners with descending timestamps, plus a tie so RANK and DENSE_RANK
// differ from each other and from ROW_NUMBER.
//
//	alice: QDate 30, 20, 10   (ClusterId 1, 2, 3)
//	bob:   QDate 25, 15, 15   (ClusterId 4, 5, 6)  <- the tie
func windowExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	rows := []struct {
		owner   string
		qdate   int
		cluster int
	}{
		{"alice", 30, 1}, {"alice", 20, 2}, {"alice", 10, 3},
		{"bob", 25, 4}, {"bob", 15, 5}, {"bob", 15, 6},
	}
	for i, r := range rows {
		mustExec(t, e, fmt.Sprintf(
			"INSERT INTO jobs (Key, Owner, QDate, ClusterId) VALUES ('k%d', '%s', %d, %d)",
			i, r.owner, r.qdate, r.cluster))
	}
	return e, cleanup
}

// TestRowNumberPerPartition covers the numbering itself: restarting at 1 per partition, in the
// window's own order rather than the scan's.
func TestRowNumberPerPartition(t *testing.T) {
	e, cleanup := windowExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT Owner, ClusterId, `+
		`ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate DESC) AS n `+
		`FROM jobs ORDER BY Owner, n`)
	want := [][]string{
		{"alice", "1", "1"}, {"alice", "2", "2"}, {"alice", "3", "3"},
		{"bob", "4", "1"}, {"bob", "5", "2"}, {"bob", "6", "3"},
	}
	if len(r.Rows) != len(want) {
		t.Fatalf("rows = %v, want %v", r.Rows, want)
	}
	for i := range want {
		if !eqStrs(r.Rows[i], want[i]) {
			t.Errorf("row %d = %v, want %v", i, r.Rows[i], want[i])
		}
	}

	// With no PARTITION BY the whole result is one partition.
	r = mustExec(t, e, `SELECT ClusterId, ROW_NUMBER() OVER (ORDER BY QDate DESC) AS n `+
		`FROM jobs ORDER BY n`)
	if len(r.Rows) != 6 || r.Rows[0][0] != "1" || r.Rows[5][1] != "6" {
		t.Errorf("unpartitioned numbering = %v, want 1..6 newest first", r.Rows)
	}
}

// TestRankVsDenseRankTies is why there are three functions: they differ only on ties.
func TestRankVsDenseRankTies(t *testing.T) {
	e, cleanup := windowExec(t)
	defer cleanup()

	// bob's two 15s tie. RANK gives 1,1,3 (the tie consumes rank 2); DENSE_RANK gives 1,1,2.
	for _, tc := range []struct {
		fn   string
		want []string
	}{
		{"RANK", []string{"1", "1", "3"}},
		{"DENSE_RANK", []string{"1", "1", "2"}},
		{"ROW_NUMBER", []string{"1", "2", "3"}},
	} {
		r := mustExec(t, e, `SELECT QDate, `+tc.fn+`() OVER (PARTITION BY Owner ORDER BY QDate) AS r `+
			`FROM jobs WHERE Owner = 'bob' ORDER BY r, QDate`)
		var got []string
		for _, row := range r.Rows {
			got = append(got, row[1])
		}
		if !eqStrs(got, tc.want) {
			t.Errorf("%s over a tie = %v, want %v", tc.fn, got, tc.want)
		}
	}
}

// TestQualifyTopNPerGroup is the query the feature exists for.
func TestQualifyTopNPerGroup(t *testing.T) {
	e, cleanup := windowExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT Owner, ClusterId, `+
		`ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate DESC) AS n `+
		`FROM jobs QUALIFY n <= 2 ORDER BY Owner, n`)
	want := [][]string{{"alice", "1", "1"}, {"alice", "2", "2"}, {"bob", "4", "1"}, {"bob", "5", "2"}}
	if len(r.Rows) != len(want) {
		t.Fatalf("top-2-per-owner = %v, want %v", r.Rows, want)
	}
	for i := range want {
		if !eqStrs(r.Rows[i], want[i]) {
			t.Errorf("row %d = %v, want %v", i, r.Rows[i], want[i])
		}
	}
}

// TestWindowLimitIsNotPushed is the correctness guard on the fetch. A LIMIT pushed to the
// server would cap the rows BEFORE they were ranked, so the ranking would be of an arbitrary
// prefix -- the numbering would be right about the wrong rows.
func TestWindowLimitIsNotPushed(t *testing.T) {
	e, cleanup := windowExec(t)
	defer cleanup()

	// The single newest row overall is alice's QDate 30. A pushed LIMIT 1 would rank whichever
	// row the scan happened to reach first and call it number 1.
	r := mustExec(t, e, `SELECT Owner, QDate, ROW_NUMBER() OVER (ORDER BY QDate DESC) AS n `+
		`FROM jobs ORDER BY n LIMIT 1`)
	if len(r.Rows) != 1 {
		t.Fatalf("rows = %v, want one", r.Rows)
	}
	if !eqStrs(r.Rows[0], []string{"alice", "30", "1"}) {
		t.Errorf("row = %v, want alice/30 ranked 1 -- the ranking must see every row", r.Rows[0])
	}

	// Same with QUALIFY: the filter applies to the full ranking, not to a truncated prefix.
	r = mustExec(t, e, `SELECT QDate, ROW_NUMBER() OVER (ORDER BY QDate) AS n `+
		`FROM jobs QUALIFY n = 6`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "30" {
		t.Errorf("last-ranked row = %v, want the newest (30)", r.Rows)
	}
}

// TestWindowOrderByWindowColumn checks that the statement's own ORDER BY can sort by a window
// column, and that Result.Ads stays in step with Result.Rows (it backs the JSON output).
func TestWindowOrderByWindowColumn(t *testing.T) {
	e, cleanup := windowExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT Owner, ClusterId, `+
		`ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate DESC) AS n `+
		`FROM jobs ORDER BY n DESC, Owner LIMIT 2`)
	if len(r.Rows) != 2 || r.Rows[0][2] != "3" || r.Rows[1][2] != "3" {
		t.Errorf("ORDER BY n DESC = %v, want the two rank-3 rows first", r.Rows)
	}
	if len(r.Ads) != len(r.Rows) {
		t.Fatalf("Ads (%d) and Rows (%d) must stay aligned", len(r.Ads), len(r.Rows))
	}
	for i, ad := range r.Ads {
		if got := valueDisplay(ad.EvaluateAttr("ClusterId")); got != r.Rows[i][1] {
			t.Errorf("row %d: Ads says ClusterId %s but Rows says %s", i, got, r.Rows[i][1])
		}
	}
}

// TestWindowWhereRejected is the foot-gun this must not have. `WHERE n <= 5` is what everyone
// types for "top five per group"; the constraint goes to the store, no attribute is named n,
// and the query would return NOTHING rather than complaining.
func TestWindowWhereRejected(t *testing.T) {
	_, err := Parse(`SELECT Owner, ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate) AS n ` +
		`FROM jobs WHERE n <= 2`)
	if err == nil {
		t.Fatal("WHERE over a window column must be refused, not silently return no rows")
	}
	if !strings.Contains(err.Error(), "QUALIFY") {
		t.Errorf("error %q should point at QUALIFY", err)
	}

	// A WHERE on the rows themselves is fine, and composes with QUALIFY.
	if _, err := Parse(`SELECT Owner, ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate) AS n ` +
		`FROM jobs WHERE Owner = 'alice' QUALIFY n <= 2`); err != nil {
		t.Errorf("WHERE on a row attribute alongside QUALIFY: %v", err)
	}
}

// TestWindowErrors pins the remaining refusals.
func TestWindowErrors(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		// A window is per row; an aggregate collapses rows.
		{`SELECT Owner, COUNT(*), ROW_NUMBER() OVER (ORDER BY Owner) FROM jobs GROUP BY Owner`,
			"cannot be combined with aggregates"},
		// Without ORDER BY the numbering would be arbitrary.
		{`SELECT ROW_NUMBER() OVER (PARTITION BY Owner) FROM jobs`, "requires ORDER BY"},
		// QUALIFY with no window at all.
		{`SELECT Owner FROM jobs QUALIFY 1 = 1`, "has none"},
		// QUALIFY over a row attribute belongs in WHERE.
		{`SELECT Owner, ROW_NUMBER() OVER (ORDER BY QDate) AS n FROM jobs QUALIFY Owner = 'x'`,
			"belongs in WHERE"},
		// No arguments, and OVER is mandatory.
		{`SELECT ROW_NUMBER(QDate) OVER (ORDER BY QDate) FROM jobs`, "takes no arguments"},
		{`SELECT ROW_NUMBER() FROM jobs`, "must be followed by OVER"},
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

// TestWindowParse pins the parsed shape and the default header.
func TestWindowParse(t *testing.T) {
	st, err := Parse(`SELECT ROW_NUMBER() OVER (PARTITION BY Owner, JobStatus ORDER BY QDate DESC) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	it := st.Items[0]
	if it.Window != "ROW_NUMBER" {
		t.Errorf("Window = %q", it.Window)
	}
	if !eqStrs(it.WinPartition, []string{"Owner", "JobStatus"}) {
		t.Errorf("WinPartition = %v", it.WinPartition)
	}
	if len(it.WinOrder) != 1 || it.WinOrder[0].Item.Col != "QDate" || !it.WinOrder[0].Desc {
		t.Errorf("WinOrder = %+v, want QDate DESC", it.WinOrder)
	}
	const wantHeader = "ROW_NUMBER() OVER (PARTITION BY Owner, JobStatus ORDER BY QDate DESC)"
	if it.header() != wantHeader {
		t.Errorf("header = %q, want %q", it.header(), wantHeader)
	}
	// A statement with no window carries no QUALIFY and takes the ordinary path.
	st, err = Parse(`SELECT Owner FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	if hasWindow(st) || st.Qualify != "" {
		t.Errorf("plain SELECT looks windowed: %+v", st.Items)
	}
}
