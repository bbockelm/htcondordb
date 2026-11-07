package repl

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// WHICH ARM RAN, not which rows came back.
//
// Both arms return the same rows -- that is the point of the server-side join, and the
// differential tests next door pin it -- so a placement test that asserted on the answer
// would pass whichever arm ran. What these assert on is the ops the server was actually
// asked to run, which is the only direct evidence of where the pairing happened.

// placementSQL joins the two fixture tables on their only shared attribute.
const placementSQL = `SELECT l.ClusterId, l.Owner, r.ExitCode ` +
	`FROM lefttab l JOIN righttab r ON l.ClusterId == r.ClusterId`

// newPlacementFixture builds a store with leftRows rows in lefttab and rightRows in
// righttab, and returns an executor per ExecConfig given, all on advertising connections to
// the same store.
//
// Every right row's key is drawn from the left side's keys in turn, so the expansion factor
// the decision computes from the two whole-table counts -- rightRows/leftRows -- is also the
// expansion the join actually produces. When there are fewer right rows than left, the left
// rows past the end match nothing, which is what the LEFT JOIN agreement case needs.
func newPlacementFixture(t testing.TB, leftRows, rightRows int, cfgs ...ExecConfig) ([]*Executor, *opRecorder, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	left, err := cat.CreateTable("lefttab")
	if err != nil {
		t.Fatal(err)
	}
	right, err := cat.CreateTable("righttab")
	if err != nil {
		t.Fatal(err)
	}
	put := func(d *db.DB, key, text string) {
		ad, perr := classad.Parse(text)
		if perr != nil {
			t.Fatal(perr)
		}
		if werr := d.Put(key, ad); werr != nil {
			t.Fatal(werr)
		}
	}
	owners := []string{"alice", "bob", "carol"}
	for i := 1; i <= leftRows; i++ {
		put(left, fmt.Sprint(i), fmt.Sprintf(`[ ClusterId = %d; Owner = %q; Cpus = %d ]`,
			i, owners[i%len(owners)], 1+i%8))
	}
	for j := 0; j < rightRows; j++ {
		put(right, fmt.Sprint(j), fmt.Sprintf(`[ ClusterId = %d; ExitCode = %d ]`,
			1+j%max(leftRows, 1), j%3))
	}

	s := dbrpc.NewServerCatalog(cat)
	ops := &opRecorder{}
	var execs []*Executor
	var conns []*dbrpc.Client
	for _, cfg := range cfgs {
		cp, sp := net.Pipe()
		go func() {
			_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{
				Privileged: true,
				QueryLog:   func(q dbrpc.QueryLog) { ops.add(q.Op) },
			})
		}()
		c := dbrpc.NewClient(dbrpc.NewStreamConn(cp), dbrpc.WithPeerSessionAttrs(s.SessionAttrs()))
		conns = append(conns, c)
		execs = append(execs, NewExecutor(c, cfg))
	}
	return execs, ops, func() {
		for _, c := range conns {
			c.Close()
		}
		s.Close()
		cat.Close()
	}
}

// ranInStore runs q and reports whether the store was asked to pair it.
//
// It waits for the server's log rather than reading it straight after the call: a server logs
// an op as its handler returns, which is after the reply is written, so the client's call can
// return first. What it waits FOR is each arm's own finishing op -- the join for the store
// arm, the second side read for the client arm -- because the two arms issue different
// numbers of ops and a fixed count would either race one or stall the other. The placement
// decision's row counts come first on both, and are not what either arm is identified by.
func ranInStore(t *testing.T, ex *Executor, ops *opRecorder, q string) bool {
	t.Helper()
	ops.reset()
	if _, err := ex.ExecString(q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		seen := ops.all()
		reads := 0
		for _, op := range seen {
			if strings.HasPrefix(op, "Join") {
				return true
			}
			if strings.Contains(op, "Query") {
				reads++ // a side read, including the transaction's own TxnQuery
			}
		}
		if reads >= 2 {
			return false // both sides read here: the pairing happened here too
		}
		if time.Now().After(deadline) {
			t.Fatalf("neither arm finished within 5s: saw %v", seen)
		}
		time.Sleep(time.Millisecond)
	}
}

// A join that does not expand ships no more from the store than from here and spends less
// CPU, so there is no link on which pairing here is right -- and no configuration needed to
// get it.
func TestJoinPlacementTakesTheStoreWhenTheJoinDoesNotExpand(t *testing.T) {
	for _, c := range []struct {
		name                string
		leftRows, rightRows int
	}{
		{"k=0.25", 64, 16},
		{"k=1", 64, 64},
	} {
		t.Run(c.name, func(t *testing.T) {
			execs, ops, cleanup := newPlacementFixture(t, c.leftRows, c.rightRows, ExecConfig{})
			defer cleanup()
			if !ranInStore(t, execs[0], ops, placementSQL) {
				t.Errorf("a join that does not expand was paired here: %v", ops.all())
			}
			// And for the reason that holds at every capacity, not for a comparison
			// against this session's link: a non-expanding join ships no more from the
			// store, so there is no link on which the answer is different.
			res, err := execs[0].ExecString("EXPLAIN " + placementSQL)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(render(res), "the join does not expand") {
				t.Errorf("EXPLAIN gives the wrong reason for taking the store:\n%s", render(res))
			}
		})
	}
}

// Past the link's crossover the store's extra bytes cost more than its CPU advantage saves,
// and the default link capacity is an assumption about the deployment rather than a measured
// property of this one -- so the arm that never ships more is the one that runs.
func TestJoinPlacementStaysHereWhenTheJoinExpandsPastTheLink(t *testing.T) {
	execs, ops, cleanup := newPlacementFixture(t, 64, 512, ExecConfig{})
	defer cleanup()
	if ranInStore(t, execs[0], ops, placementSQL) {
		t.Errorf("a join expanding 8x went to the store on an unstated link: %v", ops.all())
	}
}

// Just inside the crossover: at k=2 the store ships one extra copy of the left side, which
// needs 91 Mbps to be worth its CPU advantage, and the assumed link carries 100.
func TestJoinPlacementTakesTheStoreJustInsideTheCrossover(t *testing.T) {
	execs, ops, cleanup := newPlacementFixture(t, 64, 128, ExecConfig{})
	defer cleanup()
	if !ranInStore(t, execs[0], ops, placementSQL) {
		t.Errorf("a join expanding 2x was paired here on the assumed link: %v", ops.all())
	}
}

// A site that states a faster link gets the store arm at an expansion the assumed link
// declines, which is the whole reason the knob describes the link rather than the join.
func TestJoinPlacementTakesTheStoreWhenTheLinkIsStatedFast(t *testing.T) {
	execs, ops, cleanup := newPlacementFixture(t, 64, 512, ExecConfig{LinkCapacityMbps: 10_000})
	defer cleanup()
	if !ranInStore(t, execs[0], ops, placementSQL) {
		t.Errorf("a stated 10 Gbps link did not reach the store at k=8: %v", ops.all())
	}
}

// And a site that states a slower one loses it at an expansion the assumed link accepts.
func TestJoinPlacementStaysHereWhenTheLinkIsStatedSlow(t *testing.T) {
	execs, ops, cleanup := newPlacementFixture(t, 64, 128, ExecConfig{LinkCapacityMbps: 10})
	defer cleanup()
	if ranInStore(t, execs[0], ops, placementSQL) {
		t.Errorf("a stated 10 Mbps link went to the store at k=2: %v", ops.all())
	}
}

// Both overrides force their arm whatever the expansion factor says, because an operator who
// has measured their own deployment is holding a better number than this decision computes.
func TestJoinPlacementOverridesForceEitherArm(t *testing.T) {
	// k=8, which the assumed link declines, forced on by each spelling.
	t.Run("forced to the store", func(t *testing.T) {
		execs, ops, cleanup := newPlacementFixture(t, 64, 512,
			ExecConfig{ServerSideJoin: true}, ExecConfig{JoinPlacement: JoinPlacementStore})
		defer cleanup()
		for i, name := range []string{"ServerSideJoin", "JoinPlacementStore"} {
			if !ranInStore(t, execs[i], ops, placementSQL) {
				t.Errorf("%s did not force the pairing into the store: %v", name, ops.all())
			}
		}
	})
	// k=0.25, which every link accepts, forced off.
	t.Run("forced to the client", func(t *testing.T) {
		execs, ops, cleanup := newPlacementFixture(t, 64, 16,
			ExecConfig{JoinPlacement: JoinPlacementClient})
		defer cleanup()
		if ranInStore(t, execs[0], ops, placementSQL) {
			t.Errorf("JoinPlacementClient did not keep the pairing here: %v", ops.all())
		}
	})
}

// The arms must agree on every shape, including the one where they can differ silently: a
// LEFT JOIN whose unmatched left rows have to be null-extended rather than dropped.
func TestJoinArmsAgreeAcrossJoinShapes(t *testing.T) {
	// Fewer right rows than left, so the left rows past the end of the key range match
	// nothing and the LEFT JOIN cases carry null-extended rows.
	execs, _, cleanup := newPlacementFixture(t, 32, 12,
		ExecConfig{JoinPlacement: JoinPlacementStore}, ExecConfig{JoinPlacement: JoinPlacementClient})
	defer cleanup()
	store, client := execs[0], execs[1]

	for _, q := range []string{
		placementSQL,
		`SELECT l.ClusterId, r.ExitCode FROM lefttab l LEFT JOIN righttab r ON l.ClusterId == r.ClusterId`,
		`SELECT l.ClusterId, l.Owner, r.ExitCode FROM lefttab l LEFT JOIN righttab r ` +
			`ON l.ClusterId == r.ClusterId WHERE l.Owner == "alice"`,
		`SELECT l.ClusterId, r.ExitCode FROM lefttab l JOIN righttab r ` +
			`ON l.ClusterId == r.ClusterId AND r.ExitCode == 0`,
		`SELECT l.Owner, r.ExitCode FROM lefttab l JOIN righttab r ON l.ClusterId == r.ClusterId ` +
			`WHERE l.Cpus > 2`,
		`SELECT l.ClusterId, r.ExitCode FROM lefttab l LEFT JOIN righttab r ON l.ClusterId == r.ClusterId ` +
			`ORDER BY l.ClusterId LIMIT 10`,
		`SELECT l.Owner, COUNT(*) FROM lefttab l LEFT JOIN righttab r ON l.ClusterId == r.ClusterId ` +
			`GROUP BY l.Owner`,
	} {
		sres, err := store.ExecString(q)
		if err != nil {
			t.Fatalf("in the store: %s: %v", q, err)
		}
		cres, err := client.ExecString(q)
		if err != nil {
			t.Fatalf("client-side: %s: %v", q, err)
		}
		sortRowsForCompare(sres)
		sortRowsForCompare(cres)
		if render(sres) != render(cres) {
			t.Errorf("the arms disagree on %s:\nin the store:\n%s\nclient-side:\n%s",
				q, render(sres), render(cres))
		}
	}
}

// THE MODEL IS THE THING UNDER TEST.
//
// The decision is computed from a crossover rather than looked up in the measured table, so
// that it extends past the largest expansion anyone has measured instead of falling off the
// end of a row. What makes that defensible is that the rule reproduces the measurement: for
// every cell of the capacity-against-expansion sweep, the arm the rule picks is the arm that
// sweep found faster. All sixteen.
func TestJoinPlacementModelAgreesWithTheMeasuredSweep(t *testing.T) {
	// want is which arm the sweep measured as faster, so a disagreement here is the rule
	// contradicting a measurement rather than a style preference.
	for _, c := range []struct {
		linkMbps int
		k        float64
		store    bool
		measured string
	}{
		{10_000, 0.25, true, "1.32x faster in the store"},
		{10_000, 1, true, "1.13x faster in the store"},
		{10_000, 2, true, "1.29x faster in the store"},
		{10_000, 8, true, "1.13x faster in the store"},
		{1_000, 0.25, true, "1.32x faster in the store"},
		{1_000, 1, true, "1.19x faster in the store"},
		{1_000, 2, true, "1.24x faster in the store"},
		{1_000, 8, true, "1.23x faster in the store"},
		{100, 0.25, true, "2.24x faster in the store"},
		{100, 1, true, "1.35x faster in the store"},
		{100, 2, true, "1.04x faster in the store"},
		{100, 8, false, "1.41x slower in the store"},
		{10, 0.25, true, "2.21x faster in the store"},
		{10, 1, true, "1.09x faster in the store"},
		{10, 2, false, "1.16x slower in the store"},
		{10, 8, false, "1.40x slower in the store"},
	} {
		need := joinLinkCrossoverMbps * (c.k - 1)
		got := need <= float64(c.linkMbps)
		if got != c.store {
			t.Errorf("at %d Mbps and k=%g the rule picks %s, but the sweep measured %s "+
				"(the rule asks for %.0f Mbps)", c.linkMbps, c.k, armName(got), c.measured, need)
		}
	}
}

// The assumed link is an assumption, and the shape of the default is what it buys: the store
// arm for every join up to roughly k=2, which is most of them.
func TestDefaultLinkCapacityAcceptsExpansionUpToTwo(t *testing.T) {
	accepts := func(k float64) bool {
		return joinLinkCrossoverMbps*(k-1) <= float64(defaultLinkCapacityMbps)
	}
	if !accepts(2) {
		t.Errorf("the assumed %d Mbps link declines k=2, which it was measured to win at",
			defaultLinkCapacityMbps)
	}
	if accepts(3) {
		t.Errorf("the assumed %d Mbps link accepts k=3, which is past where the bytes it "+
			"ships were measured to be worth the CPU it saves", defaultLinkCapacityMbps)
	}
}

// An empty left side joins to nothing, and the store ships that nothing. Pairing it here
// would read the whole right table to match it against no rows at all.
func TestJoinPlacementTakesTheStoreWhenTheLeftSideIsEmpty(t *testing.T) {
	if k := joinExpansion(0, 4096); k != 0 {
		t.Errorf("an empty left side gives k=%v, not 0", k)
	}
	execs, ops, cleanup := newPlacementFixture(t, 0, 64, ExecConfig{})
	defer cleanup()
	if !ranInStore(t, execs[0], ops, placementSQL) {
		t.Errorf("a join with no left rows was paired here: %v", ops.all())
	}
}

// Without both counts there is no expansion factor, and a decision cannot be taken on a
// number nobody has. EXPLAIN is what asks here, because it takes the decision without running
// the statement -- which, against a table the store does not hold, is the only way to see the
// decision at all.
func TestJoinPlacementPairsHereWhenTheCountsAreUnavailable(t *testing.T) {
	execs, _, cleanup := newPlacementFixture(t, 64, 16, ExecConfig{})
	defer cleanup()
	res, err := execs[0].ExecString(`EXPLAIN SELECT l.ClusterId, r.ExitCode ` +
		`FROM nosuchtable l JOIN righttab r ON l.ClusterId == r.ClusterId`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(render(res), "could not be had cheaply") {
		t.Errorf("EXPLAIN does not say the counts were missing:\n%s", render(res))
	}
	if !strings.Contains(render(res), "executed client-side") {
		t.Errorf("a join whose expansion is unknown did not stay here:\n%s", render(res))
	}
}

// EXPLAIN has to show the arithmetic, not the verdict. "Executed client-side" on its own
// reads the same whether the decision was taken on real counts or never taken at all.
func TestExplainShowsTheJoinPlacementArithmetic(t *testing.T) {
	execs, _, cleanup := newPlacementFixture(t, 64, 512, ExecConfig{})
	defer cleanup()
	res, err := execs[0].ExecString("EXPLAIN " + placementSQL)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"executed client-side", "k=8.00", "512 row(s) in righttab",
		"64 in lefttab", "637 Mbps", "the link carries 100"} {
		if !strings.Contains(render(res), want) {
			t.Errorf("EXPLAIN does not report %q:\n%s", want, render(res))
		}
	}
}

func armName(store bool) string {
	if store {
		return "the store"
	}
	return "the client"
}

// THE SHAPES THE OPCODE CANNOT CARRY are refused before any placement question is asked,
// because they are not performance judgements: forcing the store cannot conjure an opcode
// that reads two sides at one instant, or one that sees a transaction's own writes.

// A point-in-time join reads both sides at one instant, which is what makes its result a
// consistent picture rather than two unrelated ones. The join opcode has no way to ask for
// that instant, so the pairing happens here even on a link where the store would win.
func TestAsOfJoinIsPairedHere(t *testing.T) {
	execs, _, cleanup := newPlacementFixture(t, 64, 16, ExecConfig{LinkCapacityMbps: 10_000})
	defer cleanup()
	// The temporal clause belongs to the statement rather than to a table, and is written
	// after the FROM table it follows. EXPLAIN is what asks: the placement is decided before
	// either side is read, so the question it answers does not need the fixture's tables to
	// have time travel enabled, and a link stated at 10 Gbps is one the store would win on
	// if the shape allowed it at all.
	res, err := execs[0].ExecString(`EXPLAIN SELECT l.ClusterId, r.ExitCode ` +
		`FROM lefttab l AS OF '-1h' JOIN righttab r ON l.ClusterId == r.ClusterId`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(render(res), "executed client-side") {
		t.Errorf("a point-in-time join was sent to the store:\n%s", render(res))
	}
	if !strings.Contains(render(res), "AS OF") {
		t.Errorf("EXPLAIN does not say why the pairing happens here:\n%s", render(res))
	}
}

// A read inside a transaction must see that transaction's own uncommitted writes. The store
// joins committed state, so a join over a table the transaction has written is paired here --
// and the row it wrote has to come back.
func TestJoinInsideATransactionIsPairedHere(t *testing.T) {
	execs, ops, cleanup := newPlacementFixture(t, 64, 16, ExecConfig{LinkCapacityMbps: 10_000})
	defer cleanup()
	ex := execs[0]
	for _, q := range []string{
		"BEGIN",
		`INSERT INTO righttab (ClusterId, ExitCode) VALUES (1, 99)`,
	} {
		if _, err := ex.ExecString(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer func() {
		if _, err := ex.ExecString("ROLLBACK"); err != nil {
			t.Fatal(err)
		}
	}()
	const q = `SELECT l.ClusterId, r.ExitCode FROM lefttab l JOIN righttab r ` +
		`ON l.ClusterId == r.ClusterId WHERE r.ExitCode == 99`
	if ranInStore(t, ex, ops, q) {
		t.Errorf("a join over a table the transaction has written was sent to the store: %v",
			ops.all())
	}
	res, err := ex.ExecString(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Errorf("the transaction's own write is not in the join: %d row(s)", len(res.Rows))
	}
}

// The placement decision and the key-batch decision want the same number -- how many rows the
// right side holds -- and a statement asks the store for it once.
//
// The arm this is visible on is the one that pairs here, because it is the arm that goes on to
// take the batching decision at all. Two counts, not three: one per side for the placement,
// and the right side's reused.
func TestJoinPlacementAsksForEachCountOnce(t *testing.T) {
	execs, ops, cleanup := newPlacementFixture(t, 64, 512, ExecConfig{})
	defer cleanup()
	if ranInStore(t, execs[0], ops, placementSQL) {
		t.Fatalf("this measures the arm that pairs here, and the store took it: %v", ops.all())
	}
	counts := 0
	for _, op := range ops.all() {
		if op == "Aggregate" {
			counts++
		}
	}
	if counts != 2 {
		t.Errorf("one statement asked for %d row counts, expected 2 (one per side): %v",
			counts, ops.all())
	}
}
