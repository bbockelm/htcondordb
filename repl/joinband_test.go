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

// A JOIN THAT WINDOWS A RANGE, WRITTEN IN SQL.
//
// `ON e.Ts BETWEEN r.Start AND r.End` declares a window rather than describing two
// comparisons, and the whole of what these tests are for is that the declaration means ONE
// thing: the same rows whichever arm pairs them, and the same rows whatever a bound happens
// to evaluate to. See BandClause for why a window is not the comparisons it reads like.

const bandSQL = `SELECT r.ClusterId, e.Ts, e.Kind FROM runs r JOIN events e ` +
	`ON e.Ts BETWEEN r.Start AND r.End`

// newBandFixture builds runs(Start, End) and events(Ts) reachable through dbrpc, with one
// executor per config.
//
// The events carry values a window has to DECLINE as well as ones it admits -- a string Ts,
// a missing Ts -- because that is where a window and the comparisons it reads like part
// company, and an agreement test over numbers alone would not see it.
// bandFixture is what the fixture's store and its connections are, as against what each
// executor is configured to do with them.
type bandFixture struct {
	// fillers are events outside every window: rows that change nothing about the answer
	// and everything about the two table sizes the placement arithmetic reads k off.
	fillers int
	// without is a feature stripped from what the peer advertises, so a client can be
	// pointed at a store that does not offer one. Empty advertises everything the server
	// really has.
	without dbrpc.Feature
}

func newBandFixture(t testing.TB, fx bandFixture, cfgs ...ExecConfig) ([]*Executor, *opRecorder, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runs, err := cat.CreateTable("runs")
	if err != nil {
		t.Fatal(err)
	}
	events, err := cat.CreateTable("events")
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
	// Four runs, two of them overlapping, and one whose End is missing entirely.
	for _, r := range []string{
		`[ ClusterId = 1; Start = 100; End = 200 ]`,
		`[ ClusterId = 2; Start = 150; End = 250 ]`,
		`[ ClusterId = 3; Start = 400; End = 500 ]`,
		`[ ClusterId = 4; Start = 600 ]`,
		`[ ClusterId = 5; Start = 0; End = 1 ]`,
	} {
		ad, perr := classad.Parse(r)
		if perr != nil {
			t.Fatal(perr)
		}
		v, _ := ad.EvaluateAttrInt("ClusterId")
		put(runs, fmt.Sprint(v), r)
	}
	for i, e := range []string{
		`[ Ts = 90;    Kind = "before" ]`,
		`[ Ts = 100;   Kind = "lo-edge" ]`,
		`[ Ts = 175;   Kind = "both" ]`,
		`[ Ts = 200;   Kind = "hi-edge" ]`,
		`[ Ts = 250;   Kind = "second" ]`,
		`[ Ts = 450.5; Kind = "real" ]`,
		`[ Ts = "175"; Kind = "string" ]`,
		`[ Kind = "absent" ]`,
		`[ Ts = 700;   Kind = "past-every-end" ]`,
		// A boolean is numeric to a window, false below true, because it is numeric to
		// the store's. Run 5 is the only window it falls inside.
		`[ Ts = true;  Kind = "bool" ]`,
	} {
		put(events, fmt.Sprint(i), e)
	}
	// Rows outside every window, which change nothing about the answer and everything
	// about the two table sizes the placement arithmetic reads k off.
	for n := 0; n < fx.fillers; n++ {
		put(events, fmt.Sprintf("fill-%d", n),
			fmt.Sprintf(`[ Ts = %d; Kind = "filler" ]`, 10000+n))
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
				QueryLog:   func(q dbrpc.QueryLog) { ops.addLog(q) },
			})
		}()
		c := dbrpc.NewClient(dbrpc.NewStreamConn(cp),
			dbrpc.WithPeerSessionAttrs(peerAttrs(s.SessionAttrs(), fx.without)))
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

// renderRows renders a result as sorted lines, so two arms are compared on their rows and
// not on the order the arms happened to produce them in.
func renderRows(res *Result) []string {
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		cells := make([]string, len(row))
		copy(cells, row)
		out = append(out, strings.Join(cells, "|"))
	}
	sort.Strings(out)
	return out
}

// THE AGREEMENT THAT THE WHOLE DESIGN EXISTS FOR.
//
// The window is narrower than the comparisons it reads like, so the two arms would answer
// differently if either of them answered the comparisons instead. They are run over the same
// rows, including the ones a window declines and ClassAd's own `>=` would order.
func TestABandSelectsTheSameRowsInBothArms(t *testing.T) {
	execs, _, cleanup := newBandFixture(t, bandFixture{},
		ExecConfig{JoinPlacement: JoinPlacementStore},
		ExecConfig{JoinPlacement: JoinPlacementClient})
	defer cleanup()
	store, client := execs[0], execs[1]

	for _, q := range []string{
		bandSQL,
		// A window and an equality on the same step: the store hashes and checks the
		// window per candidate rather than searching, which is a different code path
		// inside it and must still answer the same question.
		`SELECT r.ClusterId, e.Ts FROM runs r JOIN events e ` +
			`ON e.Ts BETWEEN r.Start AND r.End AND r.ClusterId == e.Ts`,
		// A window beside an ordinary conjunct.
		`SELECT r.ClusterId, e.Ts FROM runs r JOIN events e ` +
			`ON e.Ts BETWEEN r.Start AND r.End AND e.Kind != "both"`,
		// A LEFT join, whose unmatched rows are null-extended rather than dropped.
		`SELECT r.ClusterId, e.Ts FROM runs r LEFT JOIN events e ` +
			`ON e.Ts BETWEEN r.Start AND r.End`,
		// Constant ends, which name no table at all.
		`SELECT r.ClusterId, e.Ts FROM runs r JOIN events e ON e.Ts BETWEEN 100 AND 200`,
		// An end that is an expression over the side already in hand.
		`SELECT r.ClusterId, e.Ts FROM runs r JOIN events e ` +
			`ON e.Ts BETWEEN r.Start AND r.Start + 50`,
		// WHERE, ORDER BY and LIMIT on top of a window.
		`SELECT r.ClusterId, e.Ts FROM runs r JOIN events e ` +
			`ON e.Ts BETWEEN r.Start AND r.End WHERE r.ClusterId <= 2 ORDER BY e.Ts LIMIT 3`,
		// An aggregate over windowed rows.
		`SELECT r.ClusterId, COUNT(*) FROM runs r JOIN events e ` +
			`ON e.Ts BETWEEN r.Start AND r.End GROUP BY r.ClusterId`,
	} {
		sres, err := store.ExecString(q)
		if err != nil {
			t.Fatalf("in the store: %s: %v", q, err)
		}
		cres, err := client.ExecString(q)
		if err != nil {
			t.Fatalf("here: %s: %v", q, err)
		}
		got, want := renderRows(sres), renderRows(cres)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("the arms disagree on %s\n  store:  %v\n  here:   %v", q, got, want)
		}
	}
}

// What the window admits, stated as rows rather than as agreement -- two arms that agreed on
// the wrong answer would pass the test above.
//
// The oracle is written out by hand from the fixture: Ts must be a number inside [Start,
// End] inclusive. 90 is below every start; "175" is a string, which a window declines where
// ClassAd's `>=` would order it against nothing numeric; the event with no Ts is undefined;
// 700 is past every end, and run 4 has no End at all, so its window has no upper bound to
// be inside. A boolean IS a number to a window, false below true, which is why run 5's
// [0, 1] admits one.
func TestTheWindowAdmitsExactlyTheRowsInside(t *testing.T) {
	execs, _, cleanup := newBandFixture(t, bandFixture{}, ExecConfig{JoinPlacement: JoinPlacementStore})
	defer cleanup()
	res, err := execs[0].ExecString(bandSQL)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"1|100|lo-edge",
		"1|175|both",
		"1|200|hi-edge",
		"2|175|both",
		"2|200|hi-edge",
		"2|250|second",
		"3|450.5|real",
		// true is 1, and run 5's window is [0, 1].
		"5|true|bool",
	}
	got := renderRows(res)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("the window admitted\n  %v\nwant\n  %v", got, want)
	}
}

// A window goes to the store whatever the two tables measure, because it is the one
// predicate that changes which algorithm runs rather than how much either one ships.
func TestABandIsPairedInTheStore(t *testing.T) {
	execs, ops, cleanup := newBandFixture(t, bandFixture{fillers: 64}, ExecConfig{})
	defer cleanup()
	if !ranInStore(t, execs[0], ops, bandSQL) {
		t.Errorf("a windowed join was not sent to the store: %v", ops.all())
	}
}

// And the rows are right when it is paired here instead, which is what a peer with no window
// leaves as the only arm.
func TestABandIsPairedHereWhenItMust(t *testing.T) {
	execs, ops, cleanup := newBandFixture(t, bandFixture{}, ExecConfig{JoinPlacement: JoinPlacementClient})
	defer cleanup()
	if ranInStore(t, execs[0], ops, bandSQL) {
		t.Errorf("the pairing was not kept here: %v", ops.all())
	}
}

// THE WINDOW REACHES THE STORE AS A WINDOW.
//
// A band that arrived as two ordinary conjuncts would answer the same rows on this fixture
// and be invisible here, so what is checked is the plan the store is handed: the attribute
// windowed, and the two ends, in the store's own spelling.
func TestTheWindowIsSentAsAWindow(t *testing.T) {
	st, err := Parse(bandSQL)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planJoin(st)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.steps[0].bandSrc) != 1 {
		t.Fatalf("the plan carries %d windows, want 1", len(p.steps[0].bandSrc))
	}
	got := p.steps[0].bandSrc[0]
	want := db.JoinBand{Attr: "Ts", Lo: "r.Start", Hi: "r.End"}
	if got != want {
		t.Errorf("the window was sent as %+v, want %+v", got, want)
	}
	// And nothing repeats it as a comparison: a window that was ALSO a pair of conjuncts
	// would be answered by whichever was narrower, which is not what BETWEEN declared.
	for _, c := range p.steps[0].condSrc {
		if strings.Contains(c, "Ts") {
			t.Errorf("the window is repeated in the condition as %q", c)
		}
	}
}

// The ON text a window was taken out of is the text that remains, so a condition carrying
// both a window and an ordinary conjunct still plans the conjunct.
func TestTheRemainingConjunctsSurviveAWindow(t *testing.T) {
	st, err := Parse(`SELECT r.ClusterId FROM runs r JOIN events e ` +
		`ON e.Ts BETWEEN r.Start AND r.End AND e.Kind == "both" AND r.ClusterId > 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Joins[0].Bands) != 1 {
		t.Fatalf("read %d windows, want 1", len(st.Joins[0].Bands))
	}
	if got, want := st.Joins[0].On, `e.Kind == "both" && r.ClusterId > 1`; got != want {
		t.Errorf("the remaining condition is %q, want %q", got, want)
	}
}

// Shapes a window cannot express are refused, each naming what to write instead. A statement
// that quietly fell back to the comparisons would mean something different from the one
// beside it that did not.
func TestAWindowThatIsNotOneIsRefused(t *testing.T) {
	for _, c := range []struct{ name, sql, want string }{
		{
			name: "NOT BETWEEN",
			sql:  `SELECT r.ClusterId FROM runs r JOIN events e ON e.Ts NOT BETWEEN r.Start AND r.End`,
			want: "complement of a range is not a range",
		},
		{
			name: "no upper bound",
			sql:  `SELECT r.ClusterId FROM runs r JOIN events e ON e.Ts BETWEEN r.Start`,
			want: "has no AND and no upper bound",
		},
		{
			name: "windows a computed value",
			sql:  `SELECT r.ClusterId FROM runs r JOIN events e ON e.Ts + 1 BETWEEN r.Start AND r.End`,
			want: "BETWEEN windows one attribute of e",
		},
		{
			name: "windows the wrong table",
			sql:  `SELECT r.ClusterId FROM runs r JOIN events e ON r.Start BETWEEN e.Ts AND r.End`,
			want: "BETWEEN windows one attribute of e",
		},
		{
			name: "a bound reads the folded side",
			sql:  `SELECT r.ClusterId FROM runs r JOIN events e ON e.Ts BETWEEN r.Start AND e.Kind`,
			want: "a window's ends are fixed before e is reached",
		},
		{
			name: "in a WHERE clause",
			sql:  `SELECT ClusterId FROM runs WHERE Start BETWEEN 1 AND 2`,
			want: "written as a whole conjunct of ON and nowhere else",
		},
		{
			name: "nested inside a larger ON expression",
			sql: `SELECT r.ClusterId FROM runs r JOIN events e ` +
				`ON (e.Ts BETWEEN r.Start AND r.End) OR e.Kind == "both"`,
			want: "written as a whole conjunct of ON and nowhere else",
		},
		{
			name: "an unqualified attribute in a bound",
			sql:  `SELECT r.ClusterId FROM runs r JOIN events e ON e.Ts BETWEEN Start AND r.End`,
			want: "Start",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			st, err := Parse(c.sql)
			if err == nil {
				_, err = planJoin(st)
			}
			if err == nil {
				t.Fatalf("%s was accepted", c.sql)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("the refusal does not say %q: %v", c.want, err)
			}
		})
	}
}

// EXPLAIN names the window, which is the only place a reader can see that a BETWEEN became
// one. Two statements that return the same rows -- one searched, one scanned -- are
// otherwise indistinguishable from the outside.
func TestExplainNamesTheWindow(t *testing.T) {
	execs, _, cleanup := newBandFixture(t, bandFixture{}, ExecConfig{})
	defer cleanup()
	plan := planText(mustExec(t, execs[0], "EXPLAIN "+bandSQL))
	for _, want := range []string{
		"windowed on e, searched rather than scanned",
		"window: e.Ts BETWEEN r.Start AND r.End",
		"over numbers",
		"the join windows a range",
	} {
		if !strings.Contains(plan, want) {
			t.Errorf("EXPLAIN does not say %q:\n%s", want, plan)
		}
	}
	// A window beside an equality is answered by the hash, and the plan says so rather
	// than claiming a search that will not happen.
	plan = planText(mustExec(t, execs[0], `EXPLAIN SELECT r.ClusterId FROM runs r JOIN events e `+
		`ON e.Ts BETWEEN r.Start AND r.End AND r.ClusterId == e.Ts`))
	if !strings.Contains(plan, "hash join on 1 equality term(s)") {
		t.Errorf("EXPLAIN did not name the hash for a step with both:\n%s", plan)
	}
	if strings.Contains(plan, "searched rather than scanned") {
		t.Errorf("EXPLAIN claimed a search on a step that hashes:\n%s", plan)
	}
}

// A STORE WITHOUT THE WINDOW STILL ANSWERS THE STATEMENT.
//
// The window is the one thing here that an older store cannot be sent -- it would answer a
// query with the window removed, which is more rows and reads like a correct answer to a
// laxer question. So the client does not send it, and pairs the rows itself, with the window
// it declared. What must not differ is the answer.
func TestAStoreWithoutTheWindowIsNotSentOne(t *testing.T) {
	execs, ops, cleanup := newBandFixture(t,
		bandFixture{fillers: 64, without: dbrpc.FeatJoinBand}, ExecConfig{})
	defer cleanup()
	if ranInStore(t, execs[0], ops, bandSQL) {
		t.Errorf("a window was sent to a store that does not advertise one: %v", ops.all())
	}

	full, _, cleanupFull := newBandFixture(t, bandFixture{fillers: 64},
		ExecConfig{JoinPlacement: JoinPlacementStore})
	defer cleanupFull()
	want, err := full[0].ExecString(bandSQL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := execs[0].ExecString(bandSQL)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(renderRows(got), "\n") != strings.Join(renderRows(want), "\n") {
		t.Errorf("without the window the answer changed\n  got:  %v\n  want: %v",
			renderRows(got), renderRows(want))
	}

	// And EXPLAIN says which it was, because an arm not taken for a reason reads exactly
	// like an arm that was never there.
	plan := planText(mustExec(t, execs[0], "EXPLAIN "+bandSQL))
	if !strings.Contains(plan, "does not advertise the window") {
		t.Errorf("EXPLAIN does not say the store has no window:\n%s", plan)
	}

	// Forcing the store skips that check, which is the point of forcing it: it is how an
	// operator reaches a store that serves more than it advertises. When the store really
	// does not, the refusal has to come back as the client pairing the rows rather than as
	// a failed statement -- the one path on which the request is actually sent.
	forced, fops, cleanupForced := newBandFixture(t,
		bandFixture{fillers: 64, without: dbrpc.FeatJoinBand},
		ExecConfig{JoinPlacement: JoinPlacementStore})
	defer cleanupForced()
	if ranInStore(t, forced[0], fops, bandSQL) {
		t.Errorf("a forced send to a store with no window was not refused: %v", fops.all())
	}
	forcedRes, err := forced[0].ExecString(bandSQL)
	if err != nil {
		t.Fatalf("forcing the store at a peer without the window failed the statement: %v", err)
	}
	if strings.Join(renderRows(forcedRes), "\n") != strings.Join(renderRows(want), "\n") {
		t.Errorf("the forced fallback changed the answer\n  got:  %v\n  want: %v",
			renderRows(forcedRes), renderRows(want))
	}
}

// peerAttrs is a server's advertisement with one feature struck out, or exactly as stated
// when no feature is named.
func peerAttrs(attrs map[string]string, f dbrpc.Feature) map[string]string {
	if f == "" {
		return attrs
	}
	return withoutFeature(attrs, f)
}
