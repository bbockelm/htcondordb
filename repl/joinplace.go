package repl

// WHERE A JOIN'S ROWS GET PAIRED: in the store, or here.
//
// Both arms return the same rows, so the choice is entirely a cost one, and the cost has two
// halves that point in opposite directions.
//
// CPU points at the store, everywhere. The store reads only the attributes the join uses,
// carries its rows as the bytes storage produced rather than as decoded ads, and answers with
// wire-form rows batched many per frame; the client arm instead rebuilds every row of both
// sides into a *classad.ClassAd in order to pair them, and on a selective join rebuilds most
// of them only to discard them. Over an in-memory pipe, where bytes cost nothing, the store
// arm is faster at every expansion factor measured -- 1.0x to 1.3x when both arms read wire
// rows, and 1.9x to 5.1x over archives, which have no wire row stream for the client arm's
// side reads to use.
//
// BYTES point at the client, but only when the join EXPANDS. A joined row carries both sides,
// so with k right rows per left row and per-side projected widths l and r:
//
//	store ships   |L| * k * (l + r)      the left ad repeated once per output row
//	client ships  |L| * (l + k*r)        each side exactly once
//
// The ratio is below 1 exactly when k < 1, whatever the widths, and no encoding moves that:
// an encoding scales both arms alike. So below k=1 the store ships fewer bytes AND spends
// less CPU, and there is no link on which pairing here is the right answer. Above k=1 the
// store ships the difference, which is the left side repeated k-1 extra times, and whether
// that is worth its CPU advantage depends on how fast the link is.
//
// THE CROSSOVER, AND WHY IT IS A MODEL RATHER THAN A TABLE. The store wins while its CPU
// advantage covers the time to ship its extra bytes, so the crossover capacity is
// `extra bytes / CPU advantage`. Both quantities are per left row -- the extra bytes are the
// left side repeated (k-1) times, and the advantage is work done per left row -- so |L|
// cancels and the crossover is a function of k alone. Measured over real TCP charged against
// a fixed capacity, at left=2048, store against client:
//
//	link        k=0.25        k=1           k=2           k=8
//	10 Gbps     1.32x faster  1.13x faster  1.29x faster  1.13x faster
//	1 Gbps      1.32x faster  1.19x faster  1.24x faster  1.23x faster
//	100 Mbps    2.24x faster  1.35x faster  1.04x faster  1.41x SLOWER
//	10 Mbps     2.21x faster  1.09x faster  1.16x SLOWER  1.40x SLOWER
//
// At k=2 the store's extra 51,121 bytes against its 4.49 ms advantage put the crossover at
// 91 Mbps, which is why joinLinkCrossoverMbps is 91 and why the rule is linear in (k-1): one
// extra copy of the left side per unit of k. The linear rule picks the faster arm in all
// SIXTEEN cells above, which is the strongest check available on it and is what
// TestJoinPlacementModelAgreesWithTheMeasuredSweep pins.
//
// The rule is deliberately the CONSERVATIVE reading of that measurement. The store's CPU
// advantage grows with k (4.49 ms at k=2, 9.12 ms at k=8), and holding it fixed at its k=2
// value makes the rule demand more capacity at high k than the measurement does -- 637 Mbps
// at k=8 where the measured crossover is about 390. Demanding too much errs towards pairing
// here, which is what the client already does today and so cannot regress anyone. It also
// means the rule EXTENDS past the measured range by construction: a k of 32 or 100 asks for
// proportionally more link rather than falling off the end of a table.
//
// WHAT IS NOT CHARGED. The capacity measurement charges bytes against a capacity and does not
// charge propagation delay, because charging an RTT per write would penalize a pipelined
// stream for writes a real link overlaps. Leaving latency out biases every crossover above
// towards the client arm, which issues two whole side reads where the store arm issues one
// join, so each one is a conservative bound and a link with real RTT moves it towards the
// store.

import (
	"fmt"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// JoinPlacement overrides where a join's rows are paired. See ExecConfig.JoinPlacement.
type JoinPlacement int

const (
	// JoinPlacementAuto decides per statement, from the expansion factor and the link.
	JoinPlacementAuto JoinPlacement = iota
	// JoinPlacementStore pairs in the store whenever the statement's shape can be sent.
	JoinPlacementStore
	// JoinPlacementClient pairs here, whatever the expansion factor and the link.
	JoinPlacementClient
)

// Placement tuning. Vars rather than consts so a test can move them, and so a site whose
// deployment disagrees with the shapes these were measured on has somewhere to change them.
var (
	// joinLinkCrossoverMbps is the link capacity one unit of expansion above k=1 needs
	// before the store's extra bytes are worth its CPU advantage. Measured at k=2: 51,121
	// extra bytes against a 4.49 ms advantage is 91 Mbps.
	joinLinkCrossoverMbps = 91.0

	// defaultLinkCapacityMbps is what an unstated link is assumed to carry.
	//
	// It is an assumption about the deployment, and it is stated as one: a connection to a
	// store is taken to carry 500 Mbps. At that capacity the store arm is chosen up to about
	// k=6.5, which covers every join that does not expand by a large multiple, and the
	// conditional exists to catch the tail beyond it. A site on a gigabit link raises
	// ExecConfig.LinkCapacityMbps and gets the store arm at every k the sweep measured; a
	// site that knows its link is worse lowers it, and at 10 Mbps the store arm is chosen
	// only for joins that do not expand.
	//
	// The sweep has no reading at this capacity -- it brackets 100 Mbps and 1 Gbps -- so what
	// stands behind the default is the model, which was checked against all sixteen of the
	// sweep's cells. The model is the conservative side of that check: it holds the CPU
	// advantage at its k=2 value although the advantage grows with k, so it declines joins
	// nearer the crossover than the measurements require, and errs towards the client.
	defaultLinkCapacityMbps = 500
)

// joinPlacement is where one statement's rows will be paired, and why.
//
// why is phrased for a reader of EXPLAIN, in the same voice the store's own probe
// diagnostics use: an arm that was not taken for a reason reads exactly like an arm that was
// never available, unless the reason is printed.
type joinPlacement struct {
	store bool
	why   string

	// The decision's inputs, kept so EXPLAIN can show the arithmetic rather than the verdict.
	// known is false when the row counts could not be had cheaply, and k is then meaningless.
	known     bool
	k         float64
	leftRows  int
	rightRows int
}

// linkCapacityMbps is the capacity this session prices the store's extra bytes against.
func (e *Executor) linkCapacityMbps() int {
	if e.linkMbps > 0 {
		return e.linkMbps
	}
	return defaultLinkCapacityMbps
}

// placeJoin decides where this statement's rows are paired.
//
// The order of the questions is the order of their cost. The shape gates come first because
// they read nothing at all; the overrides next, because an operator who has forced an arm
// should not be charged for a decision that cannot change the answer; and only then the two
// row counts, which are round trips.
func (e *Executor) placeJoin(st *Statement, p *joinPlan) joinPlacement {
	if why := unsendableJoinWhy(e, st, p); why != "" {
		return joinPlacement{why: why}
	}
	// Forcing the store skips the handshake check as well as the counts: it is how an
	// operator reaches a store that serves the opcode without advertising it, and a store
	// that refuses the opcode falls back to pairing here anyway.
	if e.placement == JoinPlacementStore || e.serverJoin {
		return joinPlacement{store: true, why: "the session forces the pairing into the store"}
	}
	if e.placement == JoinPlacementClient {
		return joinPlacement{why: "the session forces the pairing to happen here"}
	}
	if !e.c.SupportsNewer(dbrpc.FeatJoin) {
		return joinPlacement{why: "this server does not advertise a server-side join, so no " +
			"join opcode is sent"}
	}

	// k is right rows per left row, which is what decides both halves of the trade: how many
	// times the store repeats the left ad, and how much of the right side the client arm
	// rebuilds only to discard. Both counts are the cheap kind -- an O(1) length for a
	// mutable table, a zone-pruned count for an archive -- and neither falls back to counting
	// rows here, because a decision that scans a table to decide whether to scan a table is
	// worse than not deciding.
	left, lok := e.cheapRowCount(p.sides[0].ref.Table)
	right, rok := e.cheapRowCount(p.sides[1].ref.Table)
	if !lok || !rok {
		return joinPlacement{why: "the sides' row counts could not be had cheaply, so how " +
			"much the join expands is unknown, and an unknown expansion takes the arm that " +
			"never ships more"}
	}
	d := joinPlacement{known: true, leftRows: left, rightRows: right, k: joinExpansion(left, right)}
	need := joinLinkCrossoverMbps * (d.k - 1)
	link := e.linkCapacityMbps()
	switch {
	case need <= 0:
		d.store = true
		d.why = fmt.Sprintf("the join does not expand (k=%.2f: %d row(s) in %s against %d in %s), "+
			"so the store ships no more than this would at any link capacity",
			d.k, right, p.sides[1].ref.Table, left, p.sides[0].ref.Table)
	default:
		// One sentence either way, because the reader needs the same arithmetic whichever
		// arm it picked: what the expansion is, what it costs in extra copies of the left
		// side, what capacity that asks for, and what this session says it has.
		d.store = need <= float64(link)
		ships := "would ship"
		if d.store {
			ships = "ships"
		}
		d.why = fmt.Sprintf("the join expands (k=%.2f: %d row(s) in %s against %d in %s), so the "+
			"store %s the left side %.2f extra time(s), which needs %.0f Mbps to be worth its "+
			"CPU advantage and the link carries %d",
			d.k, right, p.sides[1].ref.Table, left, p.sides[0].ref.Table, ships, d.k-1, need, link)
	}
	return d
}

// joinExpansion is right rows per left row, the k the bytes model is written in.
//
// An EMPTY left side is the store's case rather than a division by zero: the join produces
// nothing, the store ships nothing, and the client arm would read the whole right table to
// pair it against no rows at all.
//
// Taking k from the two whole-table counts assumes the per-left-row expansion is a property
// of the key relationship rather than of either side's filter, which is the usual uniformity
// assumption and is the sense in which it is an ESTIMATE. It errs in one known direction: a
// constraint on the right side can only reduce how many rows actually match, so a right side
// that filters itself makes k an over-estimate and moves the decision towards pairing here.
// What it does not capture is a left side whose join key has few distinct values, where one
// left row can match far more right rows than the ratio of the table sizes suggests -- the
// NDV the planner still does not have.
func joinExpansion(leftRows, rightRows int) float64 {
	if leftRows <= 0 {
		return 0
	}
	return float64(rightRows) / float64(leftRows)
}

// unsendableJoinWhy says why this statement's join cannot be sent to the store at all,
// and "" when it can.
//
// Each of these is a shape the opcode does not carry, not a performance judgement, which is
// why they are answered before any override: forcing the store cannot conjure an opcode that
// carries three tables.
func unsendableJoinWhy(e *Executor, st *Statement, p *joinPlan) string {
	switch {
	case st.AsOf != "":
		return "AS OF reads both sides at one instant, which the join opcode cannot ask for"
	case !p.twoTable():
		return fmt.Sprintf("the join opcode carries two tables and this statement joins %d",
			len(p.sides))
	case p.readsInTx(e):
		return "a read inside a transaction must see that transaction's own writes, and the " +
			"store joins committed state"
	case !joinPlanSendable(p):
		return "the plan could not be expressed for the store"
	}
	return ""
}

// joinPlanSendable reports whether every predicate of the plan survived being rendered to the
// text the opcode carries. A plan that lost one would be sent as a join with fewer conditions
// than the statement asked for, which is a different query.
func joinPlanSendable(p *joinPlan) bool {
	step := &p.steps[0]
	if len(step.cond) != len(step.condSrc) || len(p.post) != len(p.postSrc) ||
		len(step.equi) != len(step.equiSrc) {
		return false
	}
	for _, eq := range step.equiSrc {
		if eq.accStripped == "" {
			return false
		}
	}
	return true
}
