package repl

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// joinFixture builds two small tables with a deliberate shape: one job has no history row
// (so a LEFT JOIN has something to null-extend), and one history row belongs to no job (so
// an inner join has something to drop from the right side).
func joinFixture(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "CREATE TABLE hist")
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner, Cpus) VALUES ('1', 1, 'alice', 2)`)
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner, Cpus) VALUES ('2', 2, 'bob', 4)`)
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner, Cpus) VALUES ('3', 3, 'alice', 8)`)
	mustExec(t, e, `INSERT INTO hist (Key, ClusterId, ExitCode) VALUES ('h1', 1, 0)`)
	mustExec(t, e, `INSERT INTO hist (Key, ClusterId, ExitCode) VALUES ('h2', 2, 1)`)
	mustExec(t, e, `INSERT INTO hist (Key, ClusterId, ExitCode) VALUES ('h9', 9, 7)`)
	return e, cleanup
}

// rowSet renders a result as a sorted set of joined cells, so a test asserts on content
// rather than on the order the join happened to produce.
func rowSet(r *Result) []string {
	out := make([]string, 0, len(r.Rows))
	for _, row := range r.Rows {
		out = append(out, strings.Join(row, "|"))
	}
	sort.Strings(out)
	return out
}

func wantRows(t *testing.T, r *Result, want ...string) {
	t.Helper()
	got := rowSet(r)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("got %d rows %v, want %d rows %v", len(got), got, len(want), want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("row %d = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
}

func TestInnerJoinEquality(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId`)
	// Job 3 has no history row and history 9 has no job: an inner join drops both.
	wantRows(t, r, "alice|0", "bob|1")
}

func TestLeftJoinNullExtends(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId`)
	// Job 3 survives with an undefined right side -- ClassAd's spelling of SQL NULL.
	wantRows(t, r, "alice|0", "alice|undefined", "bob|1")
}

// A right-side WHERE conjunct must NOT be pushed into the right scan of a LEFT JOIN: doing
// so would silently turn it into an inner join. Applied after null-extension it correctly
// drops the unmatched row, which is what SQL says and what this asserts.
func TestLeftJoinRightSideWhereIsNotPushed(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	st, err := Parse(`SELECT j.Owner FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId
		WHERE h.ExitCode == 0`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if p.right.where != "" {
		t.Fatalf("LEFT JOIN pushed a WHERE conjunct into the right scan: %q", p.right.where)
	}
	if len(p.post) != 1 {
		t.Fatalf("expected the right-side WHERE to be a post-filter, got %d terms", len(p.post))
	}
	r := mustExec(t, e, `SELECT j.Owner FROM jobs j LEFT JOIN hist h ON j.ClusterId == h.ClusterId
		WHERE h.ExitCode == 0`)
	wantRows(t, r, "alice")
}

// The mirror rule: a LEFT JOIN's ON conjunct that reads only the preserved side must NOT
// filter the left scan, because a left row failing the condition still appears.
func TestLeftJoinLeftSideOnIsNotPushed(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	st, err := Parse(`SELECT j.Owner, h.ExitCode FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId && j.Owner == "alice"`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if p.left.where != "" {
		t.Fatalf("LEFT JOIN pushed an ON conjunct into the left scan: %q", p.left.where)
	}
	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		LEFT JOIN hist h ON j.ClusterId == h.ClusterId && j.Owner == "alice"`)
	// bob's row fails the condition but is still emitted, null-extended.
	wantRows(t, r, "alice|0", "alice|undefined", "bob|undefined")
}

// For an INNER join both directions push, which is what makes the join cheap.
func TestInnerJoinPushesBothSides(t *testing.T) {
	st, err := Parse(`SELECT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
		WHERE j.Owner == "alice" && h.ExitCode == 0`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.left.where, "Owner") {
		t.Errorf("left constraint = %q, want the Owner conjunct pushed", p.left.where)
	}
	if strings.Contains(p.left.where, "j.") {
		t.Errorf("left constraint %q still carries the alias", p.left.where)
	}
	if !strings.Contains(p.right.where, "ExitCode") {
		t.Errorf("right constraint = %q, want the ExitCode conjunct pushed", p.right.where)
	}
}

func TestJoinWhereFilters(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Owner == "bob"`)
	wantRows(t, r, "bob|1")
}

// A non-equality condition has nothing to hash on and falls to the nested loop. The point
// of the ON clause being a ClassAd expression is that this works at all.
func TestJoinNonEquiCondition(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON h.ClusterId > j.ClusterId && h.ExitCode > 0`)
	// hist rows with ExitCode > 0 are h2 (cluster 2) and h9 (cluster 9).
	wantRows(t, r,
		"alice|1", // job 1 < hist 2
		"alice|7", // job 1 < hist 9
		"bob|7",   // job 2 < hist 9
		"alice|7", // job 3 < hist 9
	)
}

func TestCrossJoin(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.ClusterId, h.ClusterId FROM jobs j CROSS JOIN hist h`)
	if len(r.Rows) != 9 {
		t.Fatalf("CROSS JOIN produced %d rows, want 9", len(r.Rows))
	}
}

func TestJoinSelfJoinNeedsDistinctAliases(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT a.Owner, b.Owner FROM jobs a
		JOIN jobs b ON a.Cpus < b.Cpus`)
	wantRows(t, r, "alice|bob", "alice|alice", "bob|alice")

	if _, err := Parse(`SELECT j.Owner FROM jobs j JOIN jobs j ON j.Cpus == j.Cpus`); err == nil {
		t.Fatal("expected duplicate alias to be refused")
	}
}

func TestJoinOrderByAndLimit(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId ORDER BY h.ExitCode DESC LIMIT 1`)
	if len(r.Rows) != 1 || r.Rows[0][1] != "1" {
		t.Fatalf("got %v, want the single highest ExitCode row", r.Rows)
	}
}

func TestJoinStarExpansion(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT * FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`)
	for _, col := range r.Columns {
		if !strings.HasPrefix(col, "j.") && !strings.HasPrefix(col, "h.") {
			t.Fatalf("SELECT * column %q is not alias-qualified", col)
		}
	}
	// Both sides carry ClusterId; qualification is what keeps them distinct.
	var jc, hc bool
	for _, col := range r.Columns {
		jc = jc || col == "j.ClusterId"
		hc = hc || col == "h.ClusterId"
	}
	if !jc || !hc {
		t.Fatalf("columns %v lost one side's ClusterId", r.Columns)
	}

	r = mustExec(t, e, `SELECT h.* FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`)
	for _, col := range r.Columns {
		if !strings.HasPrefix(col, "h.") {
			t.Fatalf("SELECT h.* produced column %q", col)
		}
	}
}

func TestJoinExpressionColumn(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT j.Owner, j.Cpus * 2 AS twice FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId ORDER BY twice`)
	wantRows(t, r, "alice|4", "bob|8")
}

// Every attribute in a join must say which table it comes from. The alternative -- resolving
// an unqualified name to the driving side -- turns `WHERE ExitCode == 0` meaning the right
// side into a silently empty result.
func TestJoinRefusesUnqualifiedReference(t *testing.T) {
	cases := []string{
		`SELECT Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`,
		`SELECT j.Owner FROM jobs j JOIN hist h ON ClusterId == h.ClusterId`,
		`SELECT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId WHERE ExitCode == 0`,
		`SELECT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId ORDER BY Owner`,
	}
	for _, q := range cases {
		st, err := Parse(q)
		if err != nil {
			continue // refused at parse time is also fine
		}
		if _, err := planJoin(st); err == nil {
			t.Errorf("accepted an unqualified reference: %s", q)
		} else if !strings.Contains(err.Error(), "not qualified") {
			t.Errorf("%s: error %q does not explain the qualification rule", q, err)
		}
	}
}

func TestJoinUnknownAliasIsRefused(t *testing.T) {
	st, err := Parse(`SELECT x.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`)
	if err != nil {
		t.Skip("refused at parse time")
	}
	if _, err := planJoin(st); err == nil {
		t.Fatal("accepted a reference to an unknown alias")
	}
}

func TestJoinRefusesUnsupportedForms(t *testing.T) {
	cases := map[string]string{
		`SELECT j.Owner FROM jobs j RIGHT JOIN hist h ON j.ClusterId == h.ClusterId`:                  "LEFT JOIN",
		`SELECT j.Owner FROM jobs j FULL JOIN hist h ON j.ClusterId == h.ClusterId`:                   "LEFT JOIN",
		`SELECT j.Owner FROM jobs j NATURAL JOIN hist h`:                                              "ON",
		`SELECT j.Owner FROM jobs j JOIN hist h USING (ClusterId)`:                                    "ON",
		`SELECT j.Owner FROM jobs j, hist h`:                                                          "JOIN",
		`SELECT j.Owner FROM jobs j CROSS JOIN hist h ON j.ClusterId == h.ClusterId`:                  "no ON",
		`SELECT j.Owner FROM jobs j JOIN hist h`:                                                      "needs an ON",
		`SELECT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId JOIN x y ON y.a == j.a`: "at most two",
	}
	for q, want := range cases {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("accepted %s", q)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s:\n  error %q\n  does not mention %q", q, err, want)
		}
	}
}

func TestJoinRefusesAggregates(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	if _, err := e.ExecString(`SELECT COUNT(*) FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId`); err == nil {
		t.Fatal("accepted an aggregate over a join")
	} else if !strings.Contains(err.Error(), "not supported yet") {
		t.Fatalf("error %q does not say the feature is missing", err)
	}
}

// Key batching narrows the right scan to the left side's keys. It is an optimization, so
// what the test pins is that it produces the same rows and names the right attribute.
func TestJoinKeyBatching(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	st, err := Parse(`SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Owner == "alice"`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if p.batchAttr != "ClusterId" {
		t.Fatalf("batchAttr = %q, want ClusterId", p.batchAttr)
	}
	left, err := e.fetchJoinSide(st, p.left, 0)
	if err != nil {
		t.Fatal(err)
	}
	batch := p.keyBatchConstraint(left)
	if !strings.Contains(batch, "ClusterId == 1") || !strings.Contains(batch, "ClusterId == 3") {
		t.Fatalf("key batch %q does not name alice's clusters", batch)
	}
	if strings.Contains(batch, "ClusterId == 2") {
		t.Fatalf("key batch %q carries a cluster the left side filtered out", batch)
	}

	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Owner == "alice"`)
	wantRows(t, r, "alice|0")
}

// The hash is a candidate filter, not a decision: ClassAd's == coerces int to real and
// folds string case, so values that compare equal must land in the same bucket or a real
// match would never be considered.
func TestJoinHashAgreesWithClassAdEquality(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE l")
	mustExec(t, e, "CREATE TABLE r")
	mustExec(t, e, `INSERT INTO l (Key, N, S) VALUES ('a', 1, 'Alice')`)
	mustExec(t, e, `INSERT INTO r (Key, N, S) VALUES ('b', 1.0, 'alice')`)

	if got := mustExec(t, e, `SELECT l.Key, r.Key FROM l l JOIN r r ON l.N == r.N`); len(got.Rows) != 1 {
		t.Fatalf("int 1 did not join real 1.0: %v", got.Rows)
	}
	if got := mustExec(t, e, `SELECT l.Key, r.Key FROM l l JOIN r r ON l.S == r.S`); len(got.Rows) != 1 {
		t.Fatalf("ClassAd string equality is case-insensitive; the join missed it: %v", got.Rows)
	}
}

// An undefined key equals nothing -- ClassAd's == propagates undefined, and undefined is not
// true -- so rows missing the key must not join each other.
func TestJoinUndefinedKeyMatchesNothing(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE l")
	mustExec(t, e, "CREATE TABLE r")
	mustExec(t, e, `INSERT INTO l (Key, Other) VALUES ('a', 1)`)
	mustExec(t, e, `INSERT INTO r (Key, Other) VALUES ('b', 2)`)

	r := mustExec(t, e, `SELECT l.Key, r.Key FROM l l JOIN r r ON l.Missing == r.Missing`)
	if len(r.Rows) != 0 {
		t.Fatalf("rows with an undefined key joined: %v", r.Rows)
	}
	// =?= is the operator that DOES equate two undefineds, and must still work.
	r = mustExec(t, e, `SELECT l.Key, r.Key FROM l l JOIN r r ON l.Missing =?= r.Missing`)
	if len(r.Rows) != 1 {
		t.Fatalf("=?= did not equate two undefined keys: %v", r.Rows)
	}
}

// The hash path and the nested-loop path must agree. Forcing the equality conjunct into the
// non-hashable form is the cheapest way to run the same question down both.
func TestJoinHashAndNestedLoopAgree(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	hashed := rowSet(mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId`))
	// `>=` && `<=` is the same predicate with nothing for the hash to key on.
	looped := rowSet(mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId >= h.ClusterId && j.ClusterId <= h.ClusterId`))
	if strings.Join(hashed, ";") != strings.Join(looped, ";") {
		t.Fatalf("hash join %v != nested loop %v", hashed, looped)
	}
}

func TestJoinExplainNamesThePlan(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, `EXPLAIN SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Owner == "alice"`)
	var text strings.Builder
	for _, row := range r.Rows {
		text.WriteString(strings.Join(row, " "))
		text.WriteByte('\n')
	}
	for _, want := range []string{"hash join", "key batching", "client-side"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("EXPLAIN output does not mention %q:\n%s", want, text.String())
		}
	}
}

// Backward compatibility is structural: execJoin fetches only through queryAdsProjected and
// queryAdsAsOf, which long predate joins, and adds no opcode of its own. What this test
// pins is the end of that claim it can actually observe -- a join running to completion
// through the ordinary client against an ordinary server, over a projected read, a
// constraint read and a key-batched read at once.
func TestJoinRunsOverOrdinaryQueryOps(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	// A projected read, a constraint read and a key-batched read in one statement.
	r := mustExec(t, e, `SELECT j.Owner, h.ExitCode FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Cpus >= 2 ORDER BY j.Owner`)
	if len(r.Rows) != 2 {
		t.Fatalf("got %v, want 2 rows", r.Rows)
	}
}

func TestSingleTableAliasStillWorks(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	// An alias on a single-table SELECT parses, and unqualified references keep working --
	// the qualification rule applies to joins only.
	r := mustExec(t, e, `SELECT Owner FROM jobs j WHERE Owner == "bob"`)
	wantRows(t, r, "bob")
}

func TestAsOfStillParsesAfterAliasSupport(t *testing.T) {
	// `AS OF` and `AS <alias>` both start with AS; the lookahead must keep them apart.
	st, err := Parse(`SELECT * FROM jobs AS OF '-1h'`)
	if err != nil {
		t.Fatal(err)
	}
	if st.AsOf != "-1h" {
		t.Fatalf("AsOf = %q, want -1h", st.AsOf)
	}
	if st.From[0].Alias != "jobs" {
		t.Fatalf("alias = %q, want the table's own name", st.From[0].Alias)
	}
	st, err = Parse(`SELECT * FROM jobs AS j`)
	if err != nil {
		t.Fatal(err)
	}
	if st.From[0].Alias != "j" || st.AsOf != "" {
		t.Fatalf("alias = %q, AsOf = %q", st.From[0].Alias, st.AsOf)
	}
}

// --- the guard rails ---

// The per-side row cap does not bound the pair count, and hashing does not either: a join
// key with one distinct value puts every row in one bucket, and that bucket is the cross
// product. Both shapes must be refused rather than run.
func TestJoinRefusesUnboundedPairCounts(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE l")
	mustExec(t, e, "CREATE TABLE r")
	for i := 0; i < 40; i++ {
		mustExec(t, e, fmt.Sprintf(`INSERT INTO l (Key, N, K) VALUES ('l%d', %d, 1)`, i, i))
		mustExec(t, e, fmt.Sprintf(`INSERT INTO r (Key, N, K) VALUES ('r%d', %d, 1)`, i, i))
	}

	// Small enough to run under the real budget, so the budget itself is exercised by
	// lowering it rather than by building a huge fixture.
	restore := maxJoinPairsForTest(100)
	defer restore()

	if _, err := e.ExecString(`SELECT l.Key, r.Key FROM l l JOIN r r ON l.N < r.N`); err == nil {
		t.Error("a join with no equality term ran unbounded")
	} else if !strings.Contains(err.Error(), "no equality") {
		t.Errorf("error %q does not say the ON clause gave it nothing to key on", err)
	}

	if _, err := e.ExecString(`SELECT l.Key, r.Key FROM l l JOIN r r ON l.K == r.K`); err == nil {
		t.Error("a degenerate hash (one distinct key) ran unbounded")
	} else if !strings.Contains(err.Error(), "distinct values") {
		t.Errorf("error %q does not say the key failed to separate the rows", err)
	}

	// The same shapes are fine once the key actually separates rows.
	if r := mustExec(t, e, `SELECT l.Key, r.Key FROM l l JOIN r r ON l.N == r.N`); len(r.Rows) != 40 {
		t.Errorf("a selective key gave %d rows, want 40", len(r.Rows))
	}
}

// --- the shapes people actually ask for ---

// "One user's live jobs plus their completed ones" is a SET UNION, not a join: a job is in
// the queue or in history, never both, so pairing the two tables finds nothing to pair.
// This pins the trap, because the join runs happily and returns an empty right side --
// which reads like a broken feature rather than the wrong question.
func TestLiveAndHistoryIsNotAJoin(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	// Live jobs carry cluster ids that history does not: the sets are disjoint.
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner) VALUES ('7.0', 7, 'alice')`)
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner) VALUES ('8.0', 8, 'alice')`)

	r := mustExec(t, e, `SELECT j.ClusterId, h.CompletionDate FROM jobs j
		LEFT JOIN history h ON j.ClusterId == h.ClusterId WHERE j.Owner == "alice"`)
	for _, row := range r.Rows {
		if row[1] != "undefined" {
			t.Fatalf("expected every right side undefined for disjoint sets, got %v", r.Rows)
		}
	}
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want alice's 2 live jobs null-extended", len(r.Rows))
	}

	// The set operation that DOES answer it stacks the two tables instead of pairing them.
	u := mustExec(t, e, `SELECT ClusterId FROM jobs WHERE Owner == "alice"
		UNION ALL SELECT ClusterId FROM history WHERE Owner == "alice"`)
	if len(u.Rows) != 4 {
		t.Fatalf("got %v, want alice's 2 live clusters and her 2 completed ones", u.Rows)
	}
}

// "The epochs of each completed job from the last day" IS a join: two archives, a composite
// key, and a time filter that pushes into the history scan. One history row joins several
// epoch rows, so the result is one row per run attempt.
func TestHistoryJoinEpochsLastDay(t *testing.T) {
	e, cleanup := newEpochExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT h.ClusterId, e.RunInstanceId, e.ExitCode
		FROM history h
		JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
		WHERE h.CompletionDate > 1700000050
		ORDER BY h.ClusterId, e.RunInstanceId`)
	// Cluster 1 completed too early to pass the filter; cluster 2 ran twice, cluster 3 once.
	wantRows(t, r, "2|0|1", "2|1|0", "3|0|0")

	// Both equality terms are used, and the time filter is pushed into the history scan.
	st, err := Parse(`SELECT h.ClusterId FROM history h
		JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
		WHERE h.CompletionDate > 1700000050`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.equi) != 2 {
		t.Errorf("got %d equality terms, want both ClusterId and ProcId hashed", len(p.equi))
	}
	if !strings.Contains(p.left.where, "CompletionDate") {
		t.Errorf("left constraint %q did not take the time filter", p.left.where)
	}
}

// newEpochExec builds the two archives the schedd mirror actually keeps: completed jobs in
// "history" and one record per run attempt in "epoch". A job appears once in history and
// once per run instance in epoch, so joining them is one-to-many.
func newEpochExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	add := func(name string, cfg db.ArchiveConfig, ads ...string) {
		arch, aerr := cat.CreateArchiveTable(name, cfg)
		if aerr != nil {
			t.Fatal(aerr)
		}
		for _, text := range ads {
			ad, perr := classad.Parse(text)
			if perr != nil {
				t.Fatal(perr)
			}
			if werr := arch.Append(ad); werr != nil {
				t.Fatal(werr)
			}
		}
	}
	add("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}, ZoneAttrs: []string{"CompletionDate"}},
		`[ ClusterId = 1; ProcId = 0; Owner = "alice"; CompletionDate = 1700000000 ]`,
		`[ ClusterId = 2; ProcId = 0; Owner = "alice"; CompletionDate = 1700000100 ]`,
		`[ ClusterId = 3; ProcId = 0; Owner = "bob";   CompletionDate = 1700000200 ]`)
	add("epoch", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}},
		`[ ClusterId = 1; ProcId = 0; RunInstanceId = 0; ExitCode = 0 ]`,
		`[ ClusterId = 2; ProcId = 0; RunInstanceId = 0; ExitCode = 1 ]`,
		`[ ClusterId = 2; ProcId = 0; RunInstanceId = 1; ExitCode = 0 ]`,
		`[ ClusterId = 3; ProcId = 0; RunInstanceId = 0; ExitCode = 0 ]`)

	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return NewExecutor(c, ExecConfig{}), func() { c.Close(); s.Close(); cat.Close() }
}
