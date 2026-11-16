package repl

import (
	"sort"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// newSubqueryPair builds one catalog and two executors over it: one given the server's
// handshake, so its subquery predicates go to the store as a semi or anti join, and one given
// none, so they are answered by the membership constraint instead.
//
// Comparing the two is the point. Two implementations of one question that merely look right
// is the failure this guards against, and an oracle that is the other implementation cannot
// drift from the thing under test by accident.
func newSubqueryPair(t *testing.T) (store, member *Executor, storeOps *opRecorder, cleanup func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mk := func(name string) *db.DB {
		d, cerr := cat.CreateTable(name)
		if cerr != nil {
			t.Fatal(cerr)
		}
		return d
	}
	jobs, epochs, users := mk("jobs"), mk("epochs"), mk("users")
	put := func(d *db.DB, key, text string) {
		ad, perr := classad.Parse(text)
		if perr != nil {
			t.Fatal(perr)
		}
		if werr := d.Put(key, ad); werr != nil {
			t.Fatal(werr)
		}
	}
	// Cluster 1 ran once, cluster 2 ran twice (so a semi join must still keep it once),
	// cluster 3 ran once, cluster 4 never ran. Job 5 has NO ClusterId at all and job 6 no
	// Owner: an undefined join key on the OUTER side is the case the two arms are most
	// likely to disagree about. Epoch "z" has no ClusterId, which is the same question on
	// the INNER side.
	put(jobs, "1", `[ ClusterId = 1; Owner = "alice"; JobStatus = 4 ]`)
	put(jobs, "2", `[ ClusterId = 2; Owner = "bob";   JobStatus = 2 ]`)
	put(jobs, "3", `[ ClusterId = 3; Owner = "alice"; JobStatus = 5 ]`)
	put(jobs, "4", `[ ClusterId = 4; Owner = "carol"; JobStatus = 5 ]`)
	put(jobs, "5", `[ Owner = "alice"; JobStatus = 5 ]`)
	put(jobs, "6", `[ ClusterId = 6; JobStatus = 1 ]`)
	put(epochs, "e1", `[ ClusterId = 1; Restarts = 0 ]`)
	put(epochs, "e2", `[ ClusterId = 2; Restarts = 3 ]`)
	put(epochs, "e3", `[ ClusterId = 2; Restarts = 1 ]`)
	put(epochs, "e4", `[ ClusterId = 3; Restarts = 0 ]`)
	put(epochs, "z", `[ Restarts = 9 ]`)
	put(users, "u1", `[ Owner = "alice"; Priority = 500 ]`)
	put(users, "u2", `[ Owner = "bob";   Priority = 10 ]`)
	put(users, "u3", `[ Priority = 900 ]`)

	s := dbrpc.NewServerCatalog(cat)
	storeOps = &opRecorder{}
	store = joinExecutor(t, s, s.SessionAttrs(), true, storeOps)
	member = joinExecutor(t, s, nil, true, &opRecorder{})
	return store, member, storeOps, func() { s.Close(); cat.Close() }
}

// subqueryArmQueries are the shapes both arms must answer identically. Every one is
// correlated, because an uncorrelated predicate is a constant that never reaches the store.
var subqueryArmQueries = []string{
	`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
	`SELECT ClusterId FROM jobs WHERE NOT EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
	`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId AND e.Restarts > 0)`,
	`SELECT ClusterId FROM jobs WHERE NOT EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId AND e.Restarts > 0)`,
	`SELECT ClusterId FROM jobs WHERE JobStatus == 5 AND NOT EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
	`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Priority > 100)`,
	`SELECT ClusterId FROM jobs WHERE Owner NOT IN (SELECT Owner FROM users WHERE Priority > 100)`,
	`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users)`,
	`SELECT ClusterId FROM jobs WHERE Owner NOT IN (SELECT Owner FROM users)`,
	// An empty inner side, both ways round.
	`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Priority > 9000)`,
	`SELECT ClusterId FROM jobs WHERE Owner NOT IN (SELECT Owner FROM users WHERE Priority > 9000)`,
	// A conjunct of the subquery's WHERE that reads only the outer row.
	`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId AND jobs.Owner == 'alice')`,
	// Composition with the rest of the statement.
	`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId) ORDER BY ClusterId DESC`,
	`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId) ORDER BY ClusterId LIMIT 2`,
	`SELECT ClusterId, Owner FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
	`SELECT * FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
}

// subqueryRowsOf runs a query and returns every cell, row by row, so a comparison sees the
// whole answer rather than one column of it.
func subqueryRowsOf(t *testing.T, e *Executor, query string) []string {
	t.Helper()
	rows, _ := subqueryRowsAndArm(t, e, query)
	return rows
}

// subqueryRowsAndArm also reports which arm ANSWERED the query.
//
// Which arm ran is read from the execution trace rather than from the ops the server logged,
// because a join the server REFUSED is logged exactly like one it served -- and a refused
// join falls back to the membership arm, which is how the first version of
// TestSubqueryArmsAgree came to pass while comparing that arm with itself.
func subqueryRowsAndArm(t *testing.T, e *Executor, query string) ([]string, string) {
	t.Helper()
	st, err := Parse(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	tr := &explainTrace{}
	e.explain = tr
	defer func() { e.explain = nil }()
	r, err := e.Exec(st)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	out := make([]string, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, strings.Join(row, "\x1f"))
	}
	return out, tr.subqueryArm
}

func TestSubqueryArmsAgree(t *testing.T) {
	store, member, storeOps, cleanup := newSubqueryPair(t)
	defer cleanup()

	for _, q := range subqueryArmQueries {
		t.Run(q, func(t *testing.T) {
			storeOps.reset()
			got, storeArm := subqueryRowsAndArm(t, store, q)
			want, memberArm := subqueryRowsAndArm(t, member, q)
			// The agreement is worth nothing if both runs took the same arm.
			if !strings.HasPrefix(storeArm, "store-side") {
				t.Fatalf("the store arm was not taken: %q (ops %v)", storeArm, storeOps.all())
			}
			if !strings.HasPrefix(memberArm, "membership") {
				t.Fatalf("the membership arm was not taken: %q", memberArm)
			}
			// ORDER BY fixes the order; without one neither arm promises any.
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

// The shapes the store arm declines are declined for a stated reason, and the membership arm
// still answers them.
func TestSubqueryStoreArmDeclinesWithAReason(t *testing.T) {
	store, _, _, cleanup := newSubqueryPair(t)
	defer cleanup()

	for _, tc := range []struct{ query, why string }{
		{`SELECT COUNT(*) FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
			"aggregates"},
		{`SELECT Owner, COUNT(*) FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId) GROUP BY Owner`,
			"aggregates"},
		{`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.Restarts > 0)`,
			"does not depend on the outer row"},
		{`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId) AND Owner IN (SELECT Owner FROM users)`,
			"carries two tables"},
	} {
		st, err := Parse(tc.query)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		plans, err := planSubqueries(st)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		if why := store.unsendableSubqueryWhy(st, plans); !strings.Contains(why, tc.why) {
			t.Errorf("%s:\n  declined because %q\n  want it to mention %q", tc.query, why, tc.why)
		}
		// Declined is not unanswered.
		if _, err := store.ExecString(tc.query); err != nil {
			t.Errorf("%s: the membership arm did not answer it: %v", tc.query, err)
		}
	}
}

// EXISTS AND NOT EXISTS PARTITION THE TABLE, including the rows whose own correlation key is
// undefined, and that is worth pinning because getting it wrong is invisible.
//
// A row with no key matches nothing, so NOT EXISTS keeps it: "nothing matched" is true of it.
// The membership arm reaches that by testing the membership against true rather than negating
// it -- `!member(undefined, {...})` is undefined, and an undefined constraint keeps no row, so
// the obvious spelling would silently drop exactly the rows a NOT EXISTS is usually asked
// about. The store's anti join keeps them. This test is what holds the two together; its
// first version asserted the opposite, because at the time the store arm was never taken and
// both runs were the arm with the bug.
func TestExistsAndNotExistsPartitionTheTable(t *testing.T) {
	store, member, _, cleanup := newSubqueryPair(t)
	defer cleanup()

	// Job 5 has no ClusterId (shown as -1); jobs 1-3 have epochs, 4 and 6 do not.
	const col = `SELECT ifThenElse(ClusterId is undefined, -1, ClusterId) FROM jobs `
	for _, e := range []*Executor{store, member} {
		yes := subqueryRowsOf(t, e, col+`WHERE EXISTS (SELECT 1 FROM epochs x WHERE x.ClusterId == jobs.ClusterId)`)
		no := subqueryRowsOf(t, e, col+`WHERE NOT EXISTS (SELECT 1 FROM epochs x WHERE x.ClusterId == jobs.ClusterId)`)
		sort.Strings(yes)
		sort.Strings(no)
		if strings.Join(yes, ",") != "1,2,3" {
			t.Errorf("EXISTS = %v, want [1 2 3]", yes)
		}
		if strings.Join(no, ",") != "-1,4,6" {
			t.Errorf("NOT EXISTS = %v, want [-1 4 6] -- the keyless job matched nothing, "+
				"so nothing is what it matched", no)
		}
		if len(yes)+len(no) != 6 {
			t.Errorf("the halves cover %d of 6 rows; they must partition the table",
				len(yes)+len(no))
		}
	}

	// On the INNER side an undefined contributes nothing to the set, so `NOT IN` keeps the
	// rows that are not in what remains -- where SQL's three-valued logic would return no
	// rows at all, because the set contains a NULL. Job 6 has no Owner and is kept for the
	// outer-side reason above; job 4 is carol, who is not a user.
	for _, e := range []*Executor{store, member} {
		got := subqueryRowsOf(t, e, col+`WHERE Owner NOT IN (SELECT Owner FROM users)`)
		sort.Strings(got)
		if strings.Join(got, ",") != "4,6" {
			t.Errorf("NOT IN over a set containing an undefined = %v, want [4 6]", got)
		}
	}
}
