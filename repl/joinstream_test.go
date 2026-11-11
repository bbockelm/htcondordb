package repl

import "testing"

// A join must deliver JOINED rows through StreamSelect, not the FROM table's rows.
//
// The streaming path reads one table by name (QueryTableStream over st.Table) with the
// statement's raw WHERE. For a join that table is only the first side, so streaming a join
// as an ordinary SELECT hands back the left table alone and calls it the join -- the same
// trap a set operation has, where the statement carries the first branch and streaming it
// would return that branch alone. It is a WRONG ANSWER rather than an error, and the C API
// (and through it the Python DB-API driver) reads results this way.
func TestStreamSelectJoinYieldsJoinedRows(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	// Precondition: the left table has a row with no match, so the left table's row count
	// and the inner join's row count genuinely differ. Without this the assertion below
	// could pass while streaming the wrong table.
	jobs := mustExec(t, e, "SELECT Key FROM jobs")
	if len(jobs.Rows) != 3 {
		t.Fatalf("fixture: jobs has %d rows, want 3", len(jobs.Rows))
	}
	joined := mustExec(t, e,
		"SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId")
	if len(joined.Rows) != 2 {
		t.Fatalf("fixture: inner join has %d rows, want 2", len(joined.Rows))
	}
	if len(joined.Rows) == len(jobs.Rows) {
		t.Fatal("fixture is vacuous: the join and the left table have the same row count, " +
			"so streaming the wrong one would not be visible")
	}

	ads := collectStream(t, e,
		"SELECT j.Owner, h.ExitCode FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId")
	if len(ads) != len(joined.Rows) {
		t.Fatalf("streamed %d rows, want %d (the join's own count); streaming returned the "+
			"FROM table rather than the join", len(ads), len(joined.Rows))
	}
	// Every streamed row must carry BOTH sides. A row streamed from the jobs table alone
	// has no member for h at all, so h reads as undefined.
	for i, ad := range ads {
		if got := valueDisplay(ad.EvaluateAttr("h")); got == "undefined" {
			t.Errorf("streamed row %d has no member for the right side (h = %s): it is the "+
				"FROM table's row, not a joined one", i, got)
		}
		if got := valueDisplay(ad.EvaluateAttr("j")); got == "undefined" {
			t.Errorf("streamed row %d has no member for the left side (j = %s)", i, got)
		}
	}
}

// A LEFT join streams its null-extended rows too: the count must match the left table, not
// the inner join.
func TestStreamSelectLeftJoinStreamsNullExtendedRows(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	left := mustExec(t, e,
		"SELECT j.Owner, h.ExitCode FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId")
	if len(left.Rows) != 3 {
		t.Fatalf("fixture: LEFT join has %d rows, want 3", len(left.Rows))
	}
	ads := collectStream(t, e,
		"SELECT j.Owner, h.ExitCode FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId")
	if len(ads) != len(left.Rows) {
		t.Fatalf("streamed %d rows, want %d", len(ads), len(left.Rows))
	}
	// The count alone is vacuous here: this fixture's LEFT join has exactly as many rows as
	// the jobs table, so streaming the wrong table would match it. What distinguishes them
	// is the right side -- two rows matched and carry h.ExitCode, one did not and does not.
	matched, nullExtended := 0, 0
	for _, ad := range ads {
		if valueDisplay(ad.EvaluateAttr("h")) == "undefined" {
			nullExtended++
		} else {
			matched++
		}
	}
	if matched != 2 || nullExtended != 1 {
		t.Fatalf("streamed %d matched and %d null-extended rows, want 2 and 1; a row with no "+
			"right side at all means the jobs table was streamed instead of the join",
			matched, nullExtended)
	}
}
