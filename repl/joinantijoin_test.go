package repl

import "testing"

// The anti-join idiom: a LEFT JOIN whose WHERE keeps only the rows that did NOT match.
//
// It is the one shape where separating "did this pair match the ON clause" from "did the
// row survive WHERE" is load-bearing. A left row that matches must not be null-extended,
// even when its matched row is then dropped by WHERE -- otherwise the null-extended row
// appears in its place, and `WHERE h.<key> =?= undefined` reports every left row instead of
// the unmatched ones.
func TestLeftJoinAntiJoinKeepsOnlyUnmatchedRows(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId
		WHERE h.ClusterId =?= undefined`)
	// Only job 3 has no history row. Jobs 1 and 2 matched, so they are gone -- not
	// null-extended and kept.
	wantRows(t, r, "alice")
}

// The same thing read the other way: a WHERE conjunct that the matched row fails must not
// resurrect the left row as a null-extended one.
func TestLeftJoinWhereDoesNotResurrectAMatchedRow(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	// Job 1 matches history 0 and job 2 matches history 1. Neither exit code is 7, so both
	// matched rows fail WHERE. Job 3 matched nothing, and undefined == 7 is undefined, so
	// it fails too. Nothing survives.
	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId
		WHERE h.ExitCode == 7`)
	wantRows(t, r)
}
