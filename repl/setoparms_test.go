package repl

import (
	"sort"
	"strings"
	"testing"
)

// setOpArmQueries are UNION shapes whose branches carry subquery predicates. Each must answer
// the same whichever arm its branches take.
//
// The predicate sits in the first branch, the second, and both, because the first branch IS
// the statement the operation hangs off -- it is reached by a different route from the rest,
// and a change that fixes one position can leave the other behind.
var setOpArmQueries = []string{
	`SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)
		UNION ALL SELECT Owner FROM jobs WHERE JobStatus == 1`,
	`SELECT Owner FROM jobs WHERE JobStatus == 1
		UNION ALL SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)`,
	`SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)
		UNION ALL SELECT Owner FROM jobs WHERE ClusterId NOT IN (SELECT ClusterId FROM epochs)`,
	// Bare UNION: the de-duplication runs over the combined rows, after both arms.
	`SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)
		UNION SELECT Owner FROM jobs WHERE JobStatus == 5`,
	// EXISTS rather than IN, and a correlated one.
	`SELECT Owner FROM jobs j WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == j.ClusterId)
		UNION ALL SELECT Owner FROM jobs WHERE JobStatus == 1`,
	// Three branches, so the "rest" is more than one statement.
	`SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)
		UNION ALL SELECT Owner FROM jobs WHERE JobStatus == 1
		UNION ALL SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)`,
	// The operation's ORDER BY and LIMIT bind to the combined rows, not to a branch, so a
	// branch answered by a different arm must not change which rows survive them.
	`SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)
		UNION ALL SELECT Owner FROM jobs WHERE JobStatus == 1 ORDER BY Owner LIMIT 3`,
}

func setOpRows(t *testing.T, e *Executor, q string) []string {
	t.Helper()
	r, err := e.ExecString(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	out := make([]string, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, strings.Join(row, "\x1f"))
	}
	return out
}

// A set operation's branches may each take either arm, and the operation must answer the same
// either way.
//
// This test does NOT establish that the two arms ran: both executors reducing everything the
// same way would satisfy it, which is exactly what happened before branches could reach the
// store. TestASetOpBranchTakesTheStoreArm is what holds that end; this one covers the shapes.
func TestSetOpBranchArmsAgree(t *testing.T) {
	store, member, _, cleanup := newSubqueryPair(t)
	defer cleanup()

	for _, q := range setOpArmQueries {
		t.Run(shortName(q), func(t *testing.T) {
			got, want := setOpRows(t, store, q), setOpRows(t, member, q)
			if !strings.Contains(strings.ToUpper(q), "ORDER BY") {
				sort.Strings(got)
				sort.Strings(want)
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Errorf("the arms disagree\n  store      %v\n  membership %v", got, want)
			}
		})
	}
}

// And the store arm must be the thing running inside the branch, or the agreement above
// compares the membership arm with itself.
//
// Both positions are checked: before this, every branch was resolved by one walk over the
// statement tree that reached the membership arm only, so a test of either position alone
// would have reported the same answer for the wrong reason.
func TestASetOpBranchTakesTheStoreArm(t *testing.T) {
	for _, c := range []struct{ name, q string }{
		{"first branch", `SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)
			UNION ALL SELECT Owner FROM jobs WHERE JobStatus == 1`},
		{"second branch", `SELECT Owner FROM jobs WHERE JobStatus == 1
			UNION ALL SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)`},
	} {
		t.Run(c.name, func(t *testing.T) {
			store, _, storeOps, cleanup := newSubqueryPair(t)
			defer cleanup()
			storeOps.reset()
			if _, err := store.ExecString(c.q); err != nil {
				t.Fatal(err)
			}
			// Two ops: the branch that pairs in the store, and the branch that scans.
			ops := storeOps.waitOps(t, 2)
			if !storeOps.saw("JoinWire") {
				t.Errorf("the branch did not pair in the store: saw %v", ops)
			}
		})
	}
}

// The case the membership arm cannot answer at all.
//
// A key set past the inlining cap is the one failure the store arm exists to catch. Inside a
// UNION it used to be reported as an error telling the reader to point the session at a store
// that serves wire-form joined rows -- which this session already was.
func TestASetOpBranchPastTheCapIsStillAnswered(t *testing.T) {
	store, member, _, cleanup := newSubqueryPair(t)
	defer cleanup()

	const q = `SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users)
		UNION ALL SELECT ClusterId FROM jobs WHERE JobStatus == 1`
	want := setOpRows(t, member, q) // under the default cap, the membership arm answers it
	store.maxInList = 1             // now force every key set past the cap
	got := setOpRows(t, store, q)

	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("the capped branch answered differently\n  capped %v\n  want   %v", got, want)
	}
	if len(got) == 0 {
		t.Fatal("answered nothing, so this proves nothing")
	}
}

// EXPLAIN must describe every branch, not only the one the operation hangs off.
func TestExplainDescribesEverySetOpBranch(t *testing.T) {
	store, _, _, cleanup := newSubqueryPair(t)
	defer cleanup()

	const q = `EXPLAIN SELECT Owner FROM jobs WHERE JobStatus == 1
		UNION ALL SELECT Owner FROM jobs WHERE ClusterId IN (SELECT ClusterId FROM epochs)`
	out := render(mustExec(t, store, q))
	for _, want := range []string{
		"UNION ALL of 2 branches",
		"branch 1:",
		"branch 2:",
		"store-side semi join", // branch 2's arm, which describing branch 1 alone would hide
	} {
		if !strings.Contains(out, want) {
			t.Errorf("EXPLAIN does not mention %q:\n%s", want, out)
		}
	}
}
