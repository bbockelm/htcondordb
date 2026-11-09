package repl

import (
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

const joinStatsSQL = "EXPLAIN ANALYZE SELECT j.Owner, h.ExitCode FROM jobs j " +
	"JOIN hist h ON j.ClusterId == h.ClusterId"

// TestExplainAnalyzeJoinReportsTheStoreAccessPath is the end-to-end case: a join the store runs,
// against a store that reports what it did. The report has to name the step, say whether that
// step's scan was narrowed, and -- since this fixture has no index on the join key -- carry the
// store's own reason for not narrowing it. The reason is the half a reader acts on.
func TestExplainAnalyzeJoinReportsTheStoreAccessPath(t *testing.T) {
	srv, cli, ops, _, cleanup := newJoinPair(t)
	defer cleanup()

	plan := explainText(t, srv, joinStatsSQL)
	if _, ok := ops.logFor("JoinWire"); !ok {
		if _, ok := ops.logFor("Join"); !ok {
			t.Fatalf("the join did not run in the store, so there is nothing to report:\n%s", plan)
		}
	}
	mustContain(t, plan, "store join statistics:")
	mustContain(t, plan, "work: hashed=")
	mustContain(t, plan, "access path, one step per side folded in:")
	mustContain(t, plan, "step 1 (h):")
	// The fixture indexes nothing, so the step cannot narrow -- and the store says why. A
	// report that prints the outcome without the reason is the thing this feature exists to
	// stop, so the reason's presence is the assertion, not the outcome's.
	if !strings.Contains(plan, "scan NOT narrowed -- ") {
		t.Errorf("the unnarrowed step gives no reason:\n%s", plan)
	}
	if strings.Contains(plan, "gave no reason") {
		t.Errorf("this store does give a reason, so the report should not say otherwise:\n%s", plan)
	}
	// The two kinds of nothing must not be printed for a report that arrived whole.
	for _, wrong := range []string{"does not report them", "cut short", "PARTIAL:"} {
		if strings.Contains(plan, wrong) {
			t.Errorf("a complete report rendered %q:\n%s", wrong, plan)
		}
	}

	// The same statement against a peer that cannot join at all pairs the rows here, and a
	// block of store statistics over it would credit the store with work this process did.
	if plan := explainText(t, cli, joinStatsSQL); strings.Contains(plan, "store join statistics") {
		t.Errorf("a join executed client-side reported store statistics:\n%s", plan)
	}
}

// TestExplainAnalyzeJoinSaysTheStoreDoesNotReport pins the answer that is a property of the PEER
// rather than of the query. The store still runs the join; it just does not advertise the
// statistics, so none are asked for and none arrive -- and the report must say that, rather than
// render the zero counts it was left with as a join that did no work and narrowed on nothing.
func TestExplainAnalyzeJoinSaysTheStoreDoesNotReport(t *testing.T) {
	s, closeCat := newJoinCatalog(t)
	defer closeCat()
	srv := joinExecutor(t, s, attrsWithout(s.SessionAttrs(), dbrpc.FeatJoinStats), true, &opRecorder{})

	plan := explainText(t, srv, joinStatsSQL)
	mustContain(t, plan, "executed in the store")
	mustContain(t, plan, "this store does not report them")
	// Nothing that would let a reader draw a conclusion about how the join ran.
	for _, wrong := range []string{"work: hashed=", "access path", "scan NOT narrowed", "build side:"} {
		if strings.Contains(plan, wrong) {
			t.Errorf("a store that reported nothing rendered %q:\n%s", wrong, plan)
		}
	}
}

// attrsWithout copies a server's advertised session attributes with one feature removed, which is
// how a test gets a peer that can run the join but cannot report on it.
func attrsWithout(attrs map[string]string, drop dbrpc.Feature) map[string]string {
	out := make(map[string]string, len(attrs))
	for k, v := range attrs {
		out[k] = v
	}
	var kept []string
	for _, f := range strings.Split(out[dbrpc.SessionAttrFeatures], ",") {
		if f != string(drop) {
			kept = append(kept, f)
		}
	}
	out[dbrpc.SessionAttrFeatures] = strings.Join(kept, ",")
	return out
}

// TestExplainAnalyzeJoinSaysNothingWhenTheStoreRefusesTheOpcode pins the guard that decides
// whether there is anything to report at all. Forcing the pairing into the store skips the
// handshake, so a peer that advertises nothing is still SENT the join -- it refuses, and the rows
// are paired here instead. The attempt leaves a report behind saying the peer cannot report, and
// printing it would say "this store does not report join statistics" about a join the store never
// ran.
func TestExplainAnalyzeJoinSaysNothingWhenTheStoreRefusesTheOpcode(t *testing.T) {
	s, closeCat := newJoinCatalog(t)
	defer closeCat()
	cp, sp := net.Pipe()
	go func() {
		_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true})
	}()
	// No peer attributes, so every join opcode is refused before it is sent; ServerSideJoin
	// routes the statement at the store anyway, which is what gets joinServerSide run.
	forced := NewExecutor(dbrpc.NewClient(dbrpc.NewStreamConn(cp)), ExecConfig{ServerSideJoin: true})

	plan := explainText(t, forced, joinStatsSQL)
	mustContain(t, plan, "executed in the store")
	if strings.Contains(plan, "store join statistics") {
		t.Errorf("a join the store refused reported store statistics:\n%s", plan)
	}
}

// TestJoinStatsLinesDistinguishTheFourAnswers is the property the whole type exists for: "the
// peer does not report", "they were asked for and did not arrive", "they arrived but the join
// stopped part way" and "all of it" are four different facts, and an operator acts differently on
// each. Flattening any two of them into one sentence loses the distinction db.JoinStats.ProbeWhy
// already fought for inside the store.
//
// Each answer is built from the SAME statistics, so nothing but the three booleans can be making
// the renderings differ.
func TestJoinStatsLinesDistinguishTheFourAnswers(t *testing.T) {
	stats := db.JoinStats{
		HashRows: 10, ScanRows: 40, Pairs: 12, Rows: 7,
		ProbeSteps: []db.JoinProbeStat{{Alias: "h", Attr: "ClusterId", Keys: 3, EstRecords: 900}},
	}
	answers := map[string]*explainTrace{
		"unsupported": {joinStatsAsked: true, joinStats: dbrpc.JoinStatsReport{Stats: stats, Unsupported: true}},
		"not reported": {joinStatsAsked: true, joinStats: dbrpc.JoinStatsReport{
			// A report that never arrived carries a zero db.JoinStats, whatever is put
			// here: the point of the case is that the renderer must not read it.
			Stats: stats}},
		"partial":  {joinStatsAsked: true, joinStats: dbrpc.JoinStatsReport{Stats: stats, Reported: true}},
		"complete": {joinStatsAsked: true, joinStats: dbrpc.JoinStatsReport{Stats: stats, Reported: true, Complete: true}},
	}

	rendered := map[string]string{}
	for name, tr := range answers {
		rendered[name] = strings.Join(joinStatsLines(tr), "\n")
		if rendered[name] == "" {
			t.Fatalf("%s rendered nothing", name)
		}
	}
	for a, ta := range rendered {
		for b, tb := range rendered {
			if a < b && ta == tb {
				t.Errorf("%s and %s render identically:\n%s", a, b, ta)
			}
		}
	}

	// A peer that cannot report says so as a fact about itself, and says nothing that could be
	// read as a fact about the join.
	if !strings.Contains(rendered["unsupported"], "this store does not report them") {
		t.Errorf("the unsupported answer does not name the peer as the cause:\n%s", rendered["unsupported"])
	}
	// The counts in a report that never arrived were never written. Printing them would say
	// the join narrowed on nothing and did no work, which is the exact confusion the booleans
	// exist to prevent -- so neither the counts nor the access path may appear.
	for _, leaked := range []string{"hashed=", "ClusterId", "access path"} {
		for _, name := range []string{"unsupported", "not reported"} {
			if strings.Contains(rendered[name], leaked) {
				t.Errorf("%s leaked %q from statistics that never crossed:\n%s", name, leaked, rendered[name])
			}
		}
	}
	if !strings.Contains(rendered["not reported"], "cut short") {
		t.Errorf("the missing-trailer answer does not say the join was cut short:\n%s", rendered["not reported"])
	}
	// A partial report keeps its access path -- that is why it is worth printing at all -- but
	// must not let its counts pass as a finished join's.
	if !strings.Contains(rendered["partial"], "PARTIAL:") || !strings.Contains(rendered["partial"], "ClusterId") {
		t.Errorf("the partial answer should mark itself and still name its access path:\n%s", rendered["partial"])
	}
	if strings.Contains(rendered["complete"], "PARTIAL:") {
		t.Errorf("a complete report marked itself partial:\n%s", rendered["complete"])
	}
}

// TestJoinStatsLinesAreSilentWithoutAStoreJoin pins the silence: a statement this process
// joined itself leaves the report untouched, and rendering it would attribute the pipeline's work
// to the store.
func TestJoinStatsLinesAreSilentWithoutAStoreJoin(t *testing.T) {
	if lines := joinStatsLines(&explainTrace{}); lines != nil {
		t.Errorf("a join that did not run in the store rendered %q", lines)
	}
	// Not even when a report is sitting there from an attempt that fell back.
	tr := &explainTrace{joinStats: dbrpc.JoinStatsReport{Reported: true, Complete: true}}
	if lines := joinStatsLines(tr); lines != nil {
		t.Errorf("an abandoned store join rendered %q", lines)
	}
}

// TestJoinProbeLineSeparatesNoIndexFromSilence is the per-step half of the same rule. A step that
// narrowed, a step that did not and said why, and a step that did not and said nothing have to
// read differently: the second sends a reader to the decision, the third to the store that owes
// an answer, and a line carrying only the outcome sends both to the wrong place.
func TestJoinProbeLineSeparatesNoIndexFromSilence(t *testing.T) {
	narrowed := joinProbeLine(0, db.JoinProbeStat{Alias: "h", Attr: "ClusterId", Keys: 4, EstRecords: 900})
	withWhy := joinProbeLine(0, db.JoinProbeStat{Alias: "h", Why: "no index covers h.ClusterId", EstRecords: 900})
	silent := joinProbeLine(0, db.JoinProbeStat{Alias: "h", EstRecords: 900})

	if !strings.Contains(narrowed, "narrowed on ClusterId to 4 key(s)") {
		t.Errorf("the narrowed step does not say what it narrowed on: %q", narrowed)
	}
	if !strings.Contains(withWhy, "no index covers h.ClusterId") {
		t.Errorf("the declined step drops the store's reason: %q", withWhy)
	}
	if !strings.Contains(silent, "gave no reason") {
		t.Errorf("a step with no reason should say the store gave none: %q", silent)
	}
	if withWhy == silent {
		t.Errorf("a step with a reason and one without render identically: %q", silent)
	}
	// The size the decision was priced against: without it, "narrowing did not pay" and
	// "narrowing was priced against the wrong table" read the same, and only one is a bug.
	for _, line := range []string{narrowed, withWhy, silent} {
		if !strings.Contains(line, "priced this side's scan at 900 record(s)") {
			t.Errorf("the line does not say what the decision was priced against: %q", line)
		}
	}
	// A step the gates declined before a size was needed reports none, and a printed zero
	// would read as a side with no rows in it.
	if strings.Contains(joinProbeLine(0, db.JoinProbeStat{Alias: "h", Why: "x"}), "priced") {
		t.Error("an unpriced step printed a price")
	}
}

// TestJoinProbeTupleLineKeepsTheTupleDecisionSeparate pins the second reason a step can carry.
// Declining the tuple form on cost while still narrowing per attribute leaves the step's own Why
// empty, so without this line that step is indistinguishable from one that was never a candidate
// -- and the narrowing it did get admits the cross product of the values rather than the tuples
// the rows hold, which the outcome line alone would overstate.
func TestJoinProbeTupleLineKeepsTheTupleDecisionSeparate(t *testing.T) {
	composite := joinProbeTupleLine(db.JoinProbeStat{Tuple: "ClusterId+ProcId", TupleComposite: true})
	perComponent := joinProbeTupleLine(db.JoinProbeStat{Tuple: "ClusterId+ProcId"})
	declined := joinProbeTupleLine(db.JoinProbeStat{TupleWhy: "the composite lookup cost more than the components"})

	if !strings.Contains(composite, "one lookup") || composite == perComponent {
		t.Errorf("a composite index and a per-component intersection render the same: %q", composite)
	}
	if !strings.Contains(perComponent, "intersected") {
		t.Errorf("the per-component form does not say what it costs: %q", perComponent)
	}
	if !strings.Contains(declined, "the composite lookup cost more than the components") ||
		!strings.Contains(declined, "cross product") {
		t.Errorf("the declined tuple form loses its reason or what it admits instead: %q", declined)
	}
	// A step that narrows on one attribute has no tuple decision to report.
	if line := joinProbeTupleLine(db.JoinProbeStat{Attr: "ClusterId", Keys: 3}); line != "" {
		t.Errorf("a single-attribute step reported a tuple decision: %q", line)
	}
}

// TestJoinOrientationLinesReportEitherAnswer pins that a plan left as written is distinguishable
// from one nothing looked at, for both decisions that change which side is read first -- and so
// change the order the rows arrive in, which is the thing a reader notices and then has to be
// able to explain.
func TestJoinOrientationLinesReportEitherAnswer(t *testing.T) {
	kept := strings.Join(joinOrientationLines(db.JoinStats{
		BuildSideWhy: "both sides are small", BuildSideCost: 100, BuildSideSwapCost: 140}), "\n")
	swapped := strings.Join(joinOrientationLines(db.JoinStats{
		BuildSideSwapped: true, BuildSideWhy: "the right side is smaller",
		BuildSideCost: 40, BuildSideSwapCost: 900}), "\n")
	chain := strings.Join(joinOrientationLines(db.JoinStats{
		ChainFromAlias: "h", ChainOrderWhy: "h carries the selective constraint",
		ChainOrderCost: 50, ChainOrderAsWrittenCost: 5000}), "\n")

	if !strings.Contains(kept, "left the sides as written") || !strings.Contains(kept, "both sides are small") {
		t.Errorf("an unswapped build side does not report itself: %q", kept)
	}
	if !strings.Contains(swapped, "written SECOND") {
		t.Errorf("a swap does not say which side the rows now follow: %q", swapped)
	}
	for _, line := range []string{kept, swapped} {
		if !strings.Contains(line, "priced at") {
			t.Errorf("the build-side decision hides its arithmetic: %q", line)
		}
	}
	if !strings.Contains(chain, "read FROM h") || !strings.Contains(chain, "h carries the selective constraint") {
		t.Errorf("the chain order decision is not reported: %q", chain)
	}
	// Nothing said, nothing printed: a pair of zeros would read as two free plans.
	if lines := joinOrientationLines(db.JoinStats{}); lines != nil {
		t.Errorf("a store that reported no orientation decision rendered %q", lines)
	}
}
