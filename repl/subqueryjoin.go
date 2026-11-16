package repl

// THE STORE ARM OF A SUBQUERY PREDICATE: a semi or anti join, paired in the store.
//
// The membership arm in subqueryexec.go answers the same question by shipping the subquery's
// key values here and giving the outer scan a `member(k, {...})` constraint. This arm sends
// the predicate itself -- `db.JoinSpec{Kind: JoinSemi}` -- and the store pairs the two sides
// without either one crossing the wire.
//
// WHY THERE IS NO PLACEMENT DECISION HERE, although an ordinary JOIN has one.
//
// joinplace.go prices the store arm's extra bytes -- the left row repeated once per output
// row -- against its CPU advantage, and finds the crossover is a function of the expansion
// factor k alone: `need = joinLinkCrossoverMbps * (k - 1)` Mbps of link. A SEMI or ANTI join
// emits each outer row AT MOST ONCE and carries nothing from the other side, so k <= 1 by
// construction, need <= 0, and the store arm ships fewer bytes AND spends less CPU at every
// link capacity there is. placeJoin says as much for that case in its own words: the store
// "ships no more than this would at any link capacity".
//
// So this arm is preferred whenever the store can take the statement at all, and the
// membership arm is what runs when it cannot -- an older server, a read inside a transaction,
// an AS OF instant, or a shape below.
//
// WHAT COMES BACK, AND WHY IT IS UNWRAPPED. A joined row is a composite whose members are the
// sides under their aliases; a semi or anti join contributes no member for the side it tests,
// so the row carries exactly one. Unwrapping it back to that side's own ad is what keeps the
// predicate a FILTER: the projection, ORDER BY, DISTINCT and every display path downstream
// see the rows the same scan would have produced, and `SELECT * FROM jobs WHERE EXISTS (...)`
// returns job rows rather than `[ jobs = [ ... ] ]`.

import (
	"errors"
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/collections/wire"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// trySubqueryStoreArm runs a SELECT whose WHERE carries subquery predicates by sending them
// to the store as a semi or anti join.
//
// ok is false when this arm does not apply, which is not a failure: the caller then answers
// the predicate the other way. Every such case is a shape the opcode does not carry or a
// decision the statement has already made, never a performance judgement -- see the file
// comment for why there is no judgement to make.
// allowText says whether the store arm may fall back to the RENDERED reply when the store
// cannot serve wire rows. It is normally false, because the measurement says that reply is
// the worst of the three paths -- see BenchmarkSubqueryArms -- so the membership arm is
// preferred to it. It is set only on the retry below, where the membership arm has already
// refused the key set as too large and a slow answer beats no answer.
func (e *Executor) trySubqueryStoreArm(st *Statement, allowText bool) (*Result, bool, error) {
	// A scalar subquery is folded into the predicate first, whichever arm runs: it is a
	// value in the constraint, not a side of the join.
	if err := e.foldScalarSubqueries(st); err != nil {
		return nil, false, err
	}
	plans, err := planSubqueries(st)
	if err != nil {
		return nil, false, err
	}
	if why := e.unsendableSubqueryWhy(st, plans); why != "" {
		if e.explain != nil {
			e.explain.subqueryArm = "membership (" + why + ")"
		}
		return nil, false, nil
	}
	req := e.subqueryJoinRequest(st, plans)
	rows, ok, err := e.semiJoinWireRows(req)
	if !ok && err == nil && allowText {
		rows, ok, err = e.semiJoinTextRows(req)
	}
	if err != nil {
		return nil, false, err
	}
	if !ok {
		if e.explain != nil {
			e.explain.subqueryArm = "membership (the store did not serve wire-form " +
				"joined rows, and the rendered reply costs more than inlining the keys)"
		}
		return nil, false, nil
	}
	if e.explain != nil {
		e.explain.subqueryArm = "store-side " + chainName(plans)
		if allowText {
			e.explain.subqueryArm += " (rendered reply: the key set is past the inlining cap)"
		}
		e.explain.rowsFetched = len(rows)
	}
	// The predicates are answered; nothing downstream should try to answer them again.
	st.Subqueries = nil

	if st.Distinct {
		rows = dedupeAds(rows)
	}
	if len(st.OrderBy) > 0 {
		if err := sortAds(rows, st.OrderBy); err != nil {
			return nil, false, err
		}
	}
	limited := applyLimit(rows, st.Limit)
	if e.explain != nil {
		e.explain.rowsReturned = len(limited)
	}
	res, err := e.renderSelectRows(st, limited)
	return res, err == nil, err
}

// joinKindName names the join a predicate becomes, for EXPLAIN.
func joinKindName(sp *subPlan) string {
	if sp.sub.Kind.negated() {
		return "anti join"
	}
	return "semi join"
}

// chainName names the whole chain for EXPLAIN: one predicate reads as itself, several read as
// the sequence they fold in.
func chainName(plans []*subPlan) string {
	parts := make([]string, len(plans))
	for i, sp := range plans {
		parts[i] = fmt.Sprintf("%s on %q", joinKindName(sp), sp.innerTable)
	}
	return strings.Join(parts, " then ")
}

// allCorrelated reports whether every predicate depends on the outer row. One that does not
// is a constant, which the membership arm settles with a single row rather than a join.
func allCorrelated(plans []*subPlan) bool {
	for _, sp := range plans {
		if !sp.correlated() {
			return false
		}
	}
	return true
}

// chainKeysQualifiable reports whether every predicate past the first correlates on something
// that can be written against the accumulated row.
func chainKeysQualifiable(plans []*subPlan) bool {
	for _, sp := range plans[1:] {
		if !isBareAttr(sp.outerKey) {
			return false
		}
	}
	return true
}

// isBareAttr reports whether src is a single unqualified attribute reference, which is the
// one shape an alias can be prefixed onto without rewriting the expression.
func isBareAttr(src string) bool {
	q, err := vm.Parse(src)
	if err != nil {
		return false
	}
	ref, ok := unwrapParens(q.Expr()).(*ast.AttributeReference)
	return ok && ref.Scope == ast.NoScope
}

// unsendableSubqueryWhy says why these predicates cannot go to the store, and "" when they
// can. Each is a shape the opcode does not carry or a decision already taken, so each is
// answered before any round trip.
func (e *Executor) unsendableSubqueryWhy(st *Statement, plans []*subPlan) string {
	switch {
	case e.placement == JoinPlacementClient:
		return "the session forces the pairing to happen here"
	case len(plans) == 0:
		return "the statement has no subquery predicate"
	case !allCorrelated(plans):
		return "a predicate does not depend on the outer row, so one row settles it"
	case len(plans) > 1 && !e.c.SupportsNewer(dbrpc.FeatJoinChain) && !e.serverJoin:
		return "this server does not advertise a join over more than two tables"
	case len(plans) > 1 && !chainKeysQualifiable(plans):
		// Past the first step the equality's outer half reads the ACCUMULATED row rather
		// than the left table, so it has to name the member it reads -- see db.JoinEqui.
		// Prefixing an alias is only sound for a bare attribute name; an expression would
		// need every reference in it rewritten, which is work the membership arm does not
		// need at all.
		return "a predicate past the first correlates on an expression rather than an " +
			"attribute, which cannot be qualified against the joined row"
	case st.AsOf != "":
		return "AS OF reads both sides at one instant, which the join opcode cannot ask for"
	case len(effectiveGroupBy(st)) > 0 || hasAggregate(st):
		return "the statement aggregates, which the single-table aggregate answers server-side"
	case hasWindow(st):
		return "the statement ranks rows with a window function"
	case st.IsJoin():
		return "the statement already joins tables"
	case e.txReads(st.Table) || e.txReads(plans[0].innerTable):
		return "a read inside a transaction must see that transaction's own writes, and the " +
			"store joins committed state"
	case !e.c.SupportsNewer(dbrpc.FeatJoin) && !e.serverJoin:
		// Forcing the store skips the handshake check, as placeJoin's does: it is how an
		// operator reaches a store that serves the opcode without advertising it, and one
		// that refuses it falls back to the membership arm anyway.
		return "this server does not advertise a server-side join"
	}
	return ""
}

// subqueryJoinRequest renders the predicates as the join the store will run.
//
// One predicate is the two-table spelling, which every server that joins at all can take.
// Several is the chain spelling -- one step each -- which dbrpc routes to its own opcode when
// Spec.Steps is set. Both are the same plan; db compiles the first into the second.
func (e *Executor) subqueryJoinRequest(st *Statement, plans []*subPlan) dbrpc.JoinRequest {
	outerAlias := st.Table
	if len(st.From) > 0 {
		outerAlias = st.From[0].Alias
	}
	outerWhere := st.Where
	for _, sp := range plans {
		outerWhere = andText(outerWhere, sp.outerWhere)
	}
	req := dbrpc.JoinRequest{Spec: db.JoinSpec{
		Left: db.JoinSide{
			Table:      st.Table,
			Alias:      outerAlias,
			Constraint: outerWhere,
			Project:    e.projectionAttrs(st),
		},
	}}
	// The side a semi or anti join tests contributes no member to the row, so it has no
	// output projection to name: db refuses a spec that gives one.
	side := func(sp *subPlan) db.JoinSide {
		return db.JoinSide{Table: sp.innerTable, Alias: sp.innerAlias, Constraint: sp.innerWhere}
	}
	if len(plans) == 1 {
		sp := plans[0]
		req.Spec.Kind = serverSubqueryKind(sp)
		req.Spec.Right = side(sp)
		// Left is written bare because this is the two-table spelling, where the row holds
		// one side and an unqualified path is unambiguous; Right reads the folded table and
		// is bare for the same reason it always is. See db.JoinEqui.
		req.Spec.Equi = []db.JoinEqui{{Left: sp.outerKey, Right: sp.innerKey}}
	} else {
		for i, sp := range plans {
			left := sp.outerKey
			if i > 0 {
				// Past the first step the outer half reads the ACCUMULATED row. Every
				// step here is a semi or anti join, which folds in no member, so that
				// row still holds exactly the left side -- and naming it is enough.
				left = outerAlias + "." + left
			}
			req.Spec.Steps = append(req.Spec.Steps, db.JoinStep{
				Kind: serverSubqueryKind(sp),
				Side: side(sp),
				Equi: []db.JoinEqui{{Left: left, Right: sp.innerKey}},
			})
		}
	}
	// The LIMIT only pushes down when nothing here reorders or removes rows afterwards.
	if st.Limit > 0 && len(st.OrderBy) == 0 && !st.Distinct {
		req.Spec.Limit = st.Limit
	}
	return req
}

// serverSubqueryKind is the join one predicate becomes.
func serverSubqueryKind(sp *subPlan) db.JoinKind {
	if sp.sub.Kind.negated() {
		return db.JoinAnti
	}
	return db.JoinSemi
}

// semiJoinWireRows runs the join and unwraps each row to the side it kept.
//
// ok is false with a nil error when the store declined the request before sending anything,
// which is the signal to answer the predicate the other way.
func (e *Executor) semiJoinWireRows(req dbrpc.JoinRequest) ([]*classad.ClassAd, bool, error) {
	alias := req.Spec.Left.Alias
	var rows []*classad.ClassAd
	var rowErr error
	// Not redacted by request: an unprivileged connection is redacted by the server whatever
	// it asks for, and a privileged one is entitled to what it would otherwise be given.
	err := e.c.JoinWireStats(e.opCtx(), req, false, func(row []byte) bool {
		// row ALIASES the frame buffer and dies when this returns. DecodeInline builds an
		// AST that owns its strings, so the member kept here outlives the frame.
		a, derr := wire.DecodeInline(row)
		if derr != nil {
			rowErr = fmt.Errorf("joined row did not decode: %w", derr)
			return false
		}
		side, serr := unwrapSemiRow(a, alias)
		if serr != nil {
			rowErr = serr
			return false
		}
		rows = append(rows, classad.FromAST(side))
		return true
	}, e.joinStatsReport())
	switch {
	case errors.Is(err, dbrpc.ErrJoinWireUnsupported), errors.Is(err, dbrpc.ErrJoinUnsupported):
		return nil, false, nil
	case rowErr != nil:
		return nil, false, rowErr
	case err != nil:
		// Including a limit the store met part way through, which arrives AFTER the rows: a
		// partial answer must not be rendered as a complete one.
		return nil, false, err
	}
	return rows, true, nil
}

// semiJoinTextRows is the reply form for a store that cannot produce wire rows for this
// request: one rendered old-ClassAd row per frame, parsed here.
//
// IT IS NOT A RARE PATH, and that is worth saying where someone will read it. db.JoinWire
// refuses a REDACTED wire-form join unless every side carries a projection, and a semi or
// anti join's tested side must carry none -- db refuses a spec that projects from a side
// contributing no member. A connection is redacted unless it is privileged, which for the
// daemon means DAEMON level, so every ordinary user's semi join lands here. The two
// requirements are each sound on their own and contradict only together; removing the
// contradiction is a change to db.JoinWire, which has nothing to withhold from a side that
// contributes no member.
func (e *Executor) semiJoinTextRows(req dbrpc.JoinRequest) ([]*classad.ClassAd, bool, error) {
	alias := req.Spec.Left.Alias
	var rows []*classad.ClassAd
	var rowErr error
	err := e.c.JoinStats(e.opCtx(), req, func(row string) bool {
		ad, perr := classad.Parse(row)
		if perr != nil {
			rowErr = fmt.Errorf("joined row did not parse: %w", perr)
			return false
		}
		side, serr := unwrapSemiAd(ad, alias)
		if serr != nil {
			rowErr = serr
			return false
		}
		rows = append(rows, side)
		return true
	}, e.joinStatsReport())
	switch {
	case errors.Is(err, dbrpc.ErrJoinUnsupported):
		return nil, false, nil
	case rowErr != nil:
		return nil, false, rowErr
	case err != nil:
		return nil, false, err
	}
	return rows, true, nil
}

// unwrapSemiAd is unwrapSemiRow for a row that arrived as a parsed ad rather than as wire
// bytes. Same rule: the composite carries exactly one member, and that member is the row.
func unwrapSemiAd(row *classad.ClassAd, alias string) (*classad.ClassAd, error) {
	if inner, verr := row.EvaluateAttr(alias).ClassAdValue(); verr == nil {
		return inner, nil
	}
	return nil, fmt.Errorf("the joined row carries no %q member; a semi join's row should "+
		"carry exactly the side it kept", alias)
}

// unwrapSemiRow takes the one side a semi or anti join's row carries out of the composite.
//
// The row is `[ <alias> = [ ... ] ]`: the tested side contributes no member, so there is
// exactly one. Reaching into it costs a lookup, not a copy -- the nested ad is the AST the
// decode already built.
func unwrapSemiRow(row *ast.ClassAd, alias string) (*ast.ClassAd, error) {
	for _, attr := range row.Attributes {
		if !strings.EqualFold(attr.Name, alias) {
			continue
		}
		switch v := attr.Value.(type) {
		case *ast.RecordLiteral:
			return v.ClassAd, nil
		case *ast.ClassAd:
			return v, nil
		}
		return nil, fmt.Errorf("the joined row's %q member is not an ad", alias)
	}
	return nil, fmt.Errorf("the joined row carries no %q member; a semi join's row should "+
		"carry exactly the side it kept", alias)
}

// subqueryPlanLines reports, for EXPLAIN, what each subquery predicate will do and which arm
// will do it.
//
// It is reported because the two arms have wildly different costs and the reader's only other
// signal is the clock: the store arm sends the predicate and neither side's rows cross the
// wire, while the membership arm reads the subquery's key column here and inlines it. An arm
// that was not taken for a reason also reads exactly like an arm that was never available,
// unless the reason is printed.
//
// It plans without running, so it says which arm WILL run. EXPLAIN ANALYZE additionally
// reports the arm that did, from the trace.
// whole is true when the lines describe the WHOLE read -- the store arm replaces the
// single-table scan rather than adding to it, so the caller must not go on to describe a scan
// that will not happen.
func (e *Executor) subqueryPlanLines(st *Statement) (lines []string, whole bool) {
	if len(st.Subqueries) == 0 {
		return nil, false
	}
	plans, err := planSubqueries(st)
	if err != nil {
		return []string{"  subquery: " + err.Error()}, false
	}
	why := e.unsendableSubqueryWhy(st, plans)
	out := make([]string, 0, len(plans)+1)
	for _, sp := range plans {
		switch {
		case !sp.correlated():
			out = append(out, fmt.Sprintf("  subquery: %s over %q is uncorrelated; one row "+
				"of it settles the predicate for every row", sp.sub.Kind, sp.innerTable))
		default:
			out = append(out, fmt.Sprintf("  subquery: %s over %q on %s == %s",
				sp.sub.Kind, sp.innerTable, sp.outerKey, sp.innerKey))
		}
		if sp.innerWhere != "" {
			out = append(out, "    subquery filter: "+sp.innerWhere)
		}
	}
	if why == "" {
		out = append(out, fmt.Sprintf("  path: store-side %s (neither side's rows cross the "+
			"wire; a semi join never expands, so no link capacity makes pairing here "+
			"cheaper)", chainName(plans)))
		if proj := e.projectionAttrs(st); proj != nil {
			out = append(out, "  attrs: ["+strings.Join(proj, ", ")+"]")
		}
		return out, true
	}
	out = append(out, fmt.Sprintf("    arm: membership constraint, because %s", why),
		fmt.Sprintf("    (the subquery's key column is read here and inlined, up to %d "+
			"distinct values)", e.maxInListValues()))
	return out, false
}
