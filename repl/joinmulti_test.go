package repl

import (
	"strings"
	"testing"
)

// threeTableFixture adds a third table to joinFixture's two, keyed by owner rather than by
// cluster, so a three-table join has to carry TWO different keys and cannot be mistaken for
// one join against a wider table.
//
//	jobs  1/alice  2/bob  3/alice        (3 pairs with no hist row)
//	hist  1/exit 0  2/exit 1  9/exit 7   (9 pairs with no job)
//	users alice/physics  carol/chem      (bob has no users row)
func threeTableFixture(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := joinFixture(t)
	mustExec(t, e, "CREATE TABLE users")
	mustExec(t, e, `INSERT INTO users (Key, Owner, Dept) VALUES ('u1', 'alice', 'physics')`)
	mustExec(t, e, `INSERT INTO users (Key, Owner, Dept) VALUES ('u2', 'carol', 'chem')`)
	return e, cleanup
}

func TestThreeTableInnerJoin(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.ClusterId, h.ExitCode, u.Dept FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner`)
	// Job 1 (alice) pairs with hist 1 and users alice. Job 2 (bob) pairs with hist 2 but
	// bob has no users row, so the second join drops it. Job 3 never reached the second
	// join at all.
	wantRows(t, r, "1|0|physics")
}

// The chain's selectivity has to compose: each step narrows what the next one sees. This
// asserts the contents rather than the mechanism, but the row that is MISSING is the point
// -- bob survived step one and was dropped by step two.
func TestThreeTableLeftJoinNullExtendsTheLastTable(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.ClusterId, u.Dept FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		LEFT JOIN users u ON u.Owner == j.Owner`)
	// bob keeps his row with an undefined Dept; alice gets physics.
	wantRows(t, r, "1|physics", "2|undefined")
}

// A LEFT join in the MIDDLE of the chain: the third table is joined to a row that may have
// no second member at all, so its ON clause reads an undefined value. That must drop the
// pair rather than error or match.
func TestThreeTableLeftJoinInTheMiddle(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.ClusterId, h.ExitCode, u.Dept FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner`)
	// Jobs 1 and 3 are alice, so both reach users; job 3 has no hist row and keeps an
	// undefined ExitCode. bob is dropped by the inner join against users.
	wantRows(t, r, "1|0|physics", "3|undefined|physics")
}

// A WHERE conjunct naming the third table must reach that table's scan when nothing
// null-extends it -- the pushdown rule has to follow the chain, not stop at two tables.
func TestThreeTableWherePushesToTheThirdTable(t *testing.T) {
	_, cleanup := threeTableFixture(t)
	defer cleanup()

	st, err := Parse(`SELECT j.ClusterId FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner
		WHERE u.Dept == "physics"`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.sides) != 3 {
		t.Fatalf("planned %d sides, want 3", len(p.sides))
	}
	if !strings.Contains(p.sides[2].where, "Dept") {
		t.Errorf("users constraint = %q, want the Dept conjunct pushed into it", p.sides[2].where)
	}
	if strings.Contains(p.sides[2].where, "u.") {
		t.Errorf("users constraint %q still carries the alias", p.sides[2].where)
	}
}

// ...and must NOT reach it when that table is the inner side of a LEFT join, for the same
// reason it must not in the two-table case: the conjunct belongs after null-extension.
func TestThreeTableWhereIsNotPushedIntoAnOuterJoinedTable(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	st, err := Parse(`SELECT j.ClusterId FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		LEFT JOIN users u ON u.Owner == j.Owner
		WHERE u.Dept == "physics"`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if p.sides[2].where != "" {
		t.Errorf("LEFT JOIN pushed a WHERE conjunct into the users scan: %q", p.sides[2].where)
	}
	if len(p.post) == 0 {
		t.Error("the conjunct went nowhere; it should be a post-filter")
	}
	// And the answer is still right: only alice's row passes the filter.
	r := mustExec(t, e, `SELECT j.ClusterId FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		LEFT JOIN users u ON u.Owner == j.Owner
		WHERE u.Dept == "physics"`)
	wantRows(t, r, "1")
}

// An ON clause may only name the tables above it. Naming one joined later is refused
// rather than quietly deferred to where it would happen to work.
func TestJoinOnCannotReadATableJoinedLater(t *testing.T) {
	st, err := Parse(`SELECT j.ClusterId FROM jobs j
		JOIN hist h ON h.ClusterId == u.Cluster
		JOIN users u ON u.Owner == j.Owner`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = planJoin(st)
	if err == nil {
		t.Fatal("accepted an ON clause reading a table joined later")
	}
	if !strings.Contains(err.Error(), "joined later") {
		t.Errorf("error %q does not say what is wrong", err)
	}
}

// SELECT * over three tables shows every side, in FROM-then-JOIN order.
func TestThreeTableStarShowsEverySide(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT * FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner`)
	cols := strings.Join(r.Columns, " ")
	for _, want := range []string{"j.Owner", "h.ExitCode", "u.Dept"} {
		if !strings.Contains(cols, want) {
			t.Errorf("columns %q do not include %s", cols, want)
		}
	}
	// The sides appear in order, not interleaved.
	ji, hi, ui := strings.Index(cols, "j."), strings.Index(cols, "h."), strings.Index(cols, "u.")
	if !(ji < hi && hi < ui) {
		t.Errorf("columns %q are not in FROM-then-JOIN order", cols)
	}
}

// Aggregates and GROUP BY work over a three-table row for the same reason they work over a
// two-table one: the row is one ClassAd either way.
func TestThreeTableGroupBy(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT u.Dept, COUNT(*) FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner
		GROUP BY u.Dept`)
	// Both of alice's jobs reach users; physics therefore has two rows.
	wantRows(t, r, "physics|2")
}

// Four tables, to make sure nothing in the pipeline is quietly indexed for three.
func TestFourTableJoin(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE depts")
	mustExec(t, e, `INSERT INTO depts (Key, Dept, Building) VALUES ('d1', 'physics', 'Chamberlin')`)

	r := mustExec(t, e, `SELECT j.ClusterId, d.Building FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner
		JOIN depts d ON d.Dept == u.Dept`)
	wantRows(t, r, "1|Chamberlin")
}

// EXPLAIN describes every step, not only the first.
func TestExplainDescribesEveryJoinStep(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `EXPLAIN SELECT j.ClusterId FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner`)
	text := strings.Join(rowSet(r), "\n")
	for _, want := range []string{"3 tables", "j -> h -> u", "hist h", "users u"} {
		if !strings.Contains(text, want) {
			t.Errorf("EXPLAIN output does not mention %q:\n%s", want, text)
		}
	}
}

// The chain is paired here, whatever the server can do: the opcode carries two tables.
func TestThreeTableJoinIsNotSentToTheStore(t *testing.T) {
	e, cleanup := threeTableFixture(t)
	defer cleanup()

	r := mustExec(t, e, `EXPLAIN SELECT j.ClusterId FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId
		JOIN users u ON u.Owner == j.Owner`)
	text := strings.Join(rowSet(r), "\n")
	if !strings.Contains(text, "client-side") {
		t.Errorf("EXPLAIN does not say the pairing happens here:\n%s", text)
	}
	if !strings.Contains(text, "two tables") {
		t.Errorf("EXPLAIN does not say why:\n%s", text)
	}
}
