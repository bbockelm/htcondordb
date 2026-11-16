package repl

// REDUCING A GROUPED JOIN IN THE STORE, so that one row per GROUP crosses the wire instead of
// one row per pair.
//
// Until this existed a grouped join was always reduced here: joinplace.go chose an arm for the
// PAIRING, and whichever arm it chose, every joined row came back and aggregateJoinRows folded
// them. For the shape that asked for this -- history joined to epoch_history, one row per job
// out of one row per run attempt -- that is the reply being the wrong shape rather than
// anything being slow. dbrpc measured it at 5.25 s / 69.3 MiB for the join against
// 1.52-1.61 s / 47.3 MiB for two plain reads paired at the far end: the join LOST on every
// shape, because it shipped one row per attempt where the caller wanted one row per job.
//
// WHY THERE IS NO PLACEMENT ARITHMETIC HERE, although the pairing has one.
//
// joinplace.go's whole model turns on one quantity: the store arm repeats the left ad once per
// output row, so it ships (k-1) extra copies of the left side, and the crossover is the link
// capacity at which those bytes cost more than the store's CPU advantage. A REDUCTION ships no
// left ad at all. Its reply is one row per group, of the group tuple's cells and the
// aggregates' -- so the quantity the model prices is not merely small here, it is absent.
//
// What is left is a comparison the reduction wins by construction nearly everywhere: G groups
// of a few typed cells against the client arm's two whole sides, |L| * (l + k*r). It loses
// only when the GROUP BY barely groups -- G approaching the pair count, each group's cells
// wider than a left row -- and that is the case the client arm has to build every pair of
// here anyway, under maxJoinPairs, and that db.JoinAggSpec.MaxGroups refuses outright at a
// hundred thousand groups. So a grouped join goes to the store whenever the statement's shape
// can be sent, which is the same conclusion subqueryjoin.go reaches for a semi join by a
// different route.
//
// THE GROUPS ARE THE SELECT'S NON-AGGREGATE ITEMS, IN ORDER, and that is not a simplification
// of GROUP BY -- it is the definition the client arm already uses. aggregateJoinRows builds
// its key from exactly those items and positionalGroupProjector reads the tuple back by the
// same positions, so sending those items as the group columns is what makes the two arms
// answer alike rather than merely answer. A statement whose GROUP BY names something the
// SELECT does not project groups by what it projects, on both arms.
//
// ONE DIFFERENCE THAT IS NOT NEW. The store groups by GroupValueText, which deliberately puts
// 1 and 1.0 in one group; the client arm groups by valueDisplay, which need not. Any column
// whose rows hold both an int and a real can therefore come back as one group here and two
// there. That is the same pair of answers the single-table paths have always given -- the
// server aggregate against execAggregateAsOf's client-side reduction -- and it is a property
// of grouping typed values at all, not of this opcode.

import (
	"errors"
	"fmt"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// joinAggregateServerSide asks the store to run the join AND reduce its rows, returning the
// formatted result.
//
// ok is false with a nil error when the store declined before producing anything -- an older
// server, or a catalog with no reduce engine wired to its join. The caller then takes the
// unreduced path exactly as before, which is the answer this replaces rather than a lesser
// one.
func (e *Executor) joinAggregateServerSide(st *Statement, spec db.JoinSpec) (*Result, bool, error) {
	cols, aggs := joinAggShape(st)
	return e.reduceInStore(st, spec, cols, aggs)
}

// reduceInStore sends one join and its reduction, and formats the groups that come back.
//
// It is shared by the two statements that have a join to reduce -- an ordinary JOIN and a
// subquery predicate's semi join -- which differ only in how their group columns and
// aggregate arguments are written against the composite row.
func (e *Executor) reduceInStore(st *Statement, spec db.JoinSpec, cols []db.GroupCol, aggs []db.AggSpec) (*Result, bool, error) {
	rows, err := e.c.JoinAggregateStats(e.opCtx(), dbrpc.JoinAggRequest{
		Spec: db.JoinAggSpec{Join: spec, GroupCols: cols, Aggs: aggs},
	}, e.joinStatsReport())
	if errors.Is(err, dbrpc.ErrJoinAggUnsupported) {
		return nil, false, nil
	}
	if err != nil {
		// Including the group cap, which is REFUSED rather than truncated and so arrives as
		// an error with no groups. Falling back here would re-run the whole join in this
		// process to produce an answer of a hundred thousand rows; the store's refusal says
		// what the cap is and is the more useful end.
		return nil, false, err
	}
	if e.explain != nil {
		e.explain.joinStatsAsked = true
		e.explain.rowsFetched = len(rows)
	}
	proj, err := positionalGroupProjector(st)
	if err != nil {
		return nil, false, err
	}
	res := &Result{IsSelect: true, Columns: proj.columns()}
	for _, gr := range rows {
		// HAVING is still applied here. It filters GROUPS, and every aggregate it reads was
		// sent with the others (aggCallOrder puts the items' and HAVING's in one order), so
		// the group arrives carrying what the filter needs.
		if !proj.keep(gr.Group, gr.Values) {
			continue
		}
		addAggRow(res, proj, gr.Group, gr.GroupValues, gr.Values)
	}
	out, ferr := finishAggRows(res, st)
	if ferr != nil {
		return nil, false, ferr
	}
	if e.explain != nil {
		e.explain.rowsReturned = len(out.Rows)
	}
	return out, true, nil
}

// statementReduces reports whether the statement folds its rows into groups.
//
// Deliberately NOT effectiveGroupBy's test: a DISTINCT answered as a grouping is decided
// separately by each caller, because what it groups by is not what an aggregate groups by.
func statementReduces(st *Statement) bool {
	return hasAggregate(st) || len(st.GroupBy) > 0
}

// joinAggShape renders the statement's grouping as the reduction the store will run: the group
// columns in the SELECT's own order, and the aggregates in aggCallOrder's.
//
// Both orders are the ones the reply is read back in -- positionalGroupProjector for the group
// tuple, aggCallOrder for the values -- so this is one place where the order IS the contract
// and not a presentation choice.
func joinAggShape(st *Statement) ([]db.GroupCol, []db.AggSpec) {
	var cols []db.GroupCol
	for _, it := range st.Items {
		if it.GroupLevel() {
			continue
		}
		cols = append(cols, db.GroupCol{Attr: groupSourceOf(it)})
	}
	return cols, aggSpecsFor(st)
}

// groupSourceOf is the expression an item groups by: its captured expression text, or the bare
// attribute when it is a plain column.
func groupSourceOf(it SelectItem) string {
	if it.Expr != "" {
		return it.Expr
	}
	return it.Col
}

// placeJoinAgg decides whether a grouped join's reduction goes to the store.
//
// It answers in joinPlacement's vocabulary so that EXPLAIN prints one kind of sentence about
// where work happens, but it computes no arithmetic: see the file comment for why a reduction
// has no crossover to find. known stays false, and k with it, because neither is consulted.
func (e *Executor) placeJoinAgg(st *Statement, p *joinPlan) joinPlacement {
	if why := unsendableJoinWhy(e, st, p); why != "" {
		return joinPlacement{why: why}
	}
	if e.placement == JoinPlacementClient {
		return joinPlacement{why: "the session forces the pairing to happen here"}
	}
	if why := unsendableJoinAggWhy(e, st); why != "" {
		return joinPlacement{why: why}
	}
	return joinPlacement{store: true, why: "the reduction's reply is one row per group rather " +
		"than one per pair, so the extra copies of the left side that place an unreduced join " +
		"do not exist"}
}

// unsendableJoinAggWhy says why this statement's grouping cannot be sent, and "" when it can.
// Every case is a shape, answered before any round trip.
func unsendableJoinAggWhy(e *Executor, st *Statement) string {
	// Forcing the store does not reach past this one. placeJoin's override exists so an
	// operator can use a store that serves the opcode without advertising it; here the client
	// refuses before sending anything (dbrpc.Client.JoinAggregate checks the handshake
	// itself), so forcing would buy a refusal rather than an attempt.
	if !e.c.SupportsNewer(dbrpc.FeatJoinAgg) {
		return "this server does not advertise a server-side join aggregate"
	}
	for _, it := range st.Items {
		switch {
		case it.Star:
			return "`*` beside an aggregate names no group column, so the group tuple and " +
				"the projection would not line up"
		case it.Bucket:
			return "time_bucket floors a time axis into groups, which this reduction does " +
				"not carry yet"
		case !it.GroupLevel() && groupSourceOf(it) == "":
			return "a selected column has no expression to group by"
		}
	}
	if len(aggCallOrder(st)) == 0 {
		// Grouping with nothing to reduce is a DISTINCT written as a GROUP BY. It has no
		// aggregate to make the reply smaller than the rows, and the unreduced path already
		// dedupes it; sending it would buy nothing and would have to agree about a shape
		// neither arm exercises.
		return "the statement groups but aggregates nothing, which is a DISTINCT over the " +
			"joined rows"
	}
	return ""
}

// joinAggPlanLines is EXPLAIN's account of a grouped join: where the reduction happens and on
// what shape.
//
// reduced says the store answers the whole statement, and is what tells describeJoinPlan to
// print nothing further about where the rows are PAIRED. The two questions are one question
// for a reduction: no joined row crosses the wire, so a placement sentence underneath it
// would describe a reply that is not sent.
func (e *Executor) joinAggPlanLines(st *Statement, p *joinPlan) (lines []string, reduced bool) {
	cols, aggs := joinAggShape(st)
	d := e.placeJoinAgg(st, p)
	if !d.store {
		return []string{fmt.Sprintf("  grouping: here, over the joined rows (%d group "+
			"column(s), %d aggregate(s)) -- %s", len(cols), len(aggs), d.why)}, false
	}
	return []string{
		fmt.Sprintf("  grouping: in the store, one row per group (%d group column(s), "+
			"%d aggregate(s)) -- %s", len(cols), len(aggs), d.why),
		"  executed in the store: the join and its reduction are one request; no joined " +
			"row crosses the wire, only the groups",
	}, true
}
