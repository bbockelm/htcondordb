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
func (e *Executor) trySubqueryStoreArm(st *Statement) (*Result, bool, error) {
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
	req := e.subqueryJoinRequest(st, plans[0])
	rows, ok, err := e.semiJoinWireRows(req, st)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		if e.explain != nil {
			e.explain.subqueryArm = "membership (the store did not serve the join)"
		}
		return nil, false, nil
	}
	if e.explain != nil {
		e.explain.subqueryArm = fmt.Sprintf("store-side %s on %q",
			joinKindName(plans[0]), plans[0].innerTable)
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

// unsendableSubqueryWhy says why these predicates cannot go to the store, and "" when they
// can. Each is a shape the opcode does not carry or a decision already taken, so each is
// answered before any round trip.
func (e *Executor) unsendableSubqueryWhy(st *Statement, plans []*subPlan) string {
	switch {
	case e.placement == JoinPlacementClient:
		return "the session forces the pairing to happen here"
	case len(plans) != 1:
		return fmt.Sprintf("the join opcode carries two tables and this statement has %d "+
			"subquery predicates", len(plans))
	case !plans[0].correlated():
		return "the predicate does not depend on the outer row, so one row settles it"
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

// subqueryJoinRequest renders one predicate as the join the store will run.
func (e *Executor) subqueryJoinRequest(st *Statement, sp *subPlan) dbrpc.JoinRequest {
	kind := db.JoinSemi
	if sp.sub.Kind.negated() {
		kind = db.JoinAnti
	}
	outerAlias := st.Table
	if len(st.From) > 0 {
		outerAlias = st.From[0].Alias
	}
	req := dbrpc.JoinRequest{Spec: db.JoinSpec{
		Kind: kind,
		Left: db.JoinSide{
			Table:      st.Table,
			Alias:      outerAlias,
			Constraint: andText(st.Where, sp.outerWhere),
			Project:    e.projectionAttrs(st),
		},
		// The side a semi or anti join tests contributes no member to the row, so it has
		// no output projection to name: db refuses a spec that gives one.
		Right: db.JoinSide{
			Table:      sp.innerTable,
			Alias:      sp.innerAlias,
			Constraint: sp.innerWhere,
		},
		// Left is written bare because this is the two-table spelling, where the row holds
		// one side and an unqualified path is unambiguous; Right reads the folded table and
		// is bare for the same reason it always is. See db.JoinEqui.
		Equi: []db.JoinEqui{{Left: sp.outerKey, Right: sp.innerKey}},
	}}
	// The LIMIT only pushes down when nothing here reorders or removes rows afterwards.
	if st.Limit > 0 && len(st.OrderBy) == 0 && !st.Distinct {
		req.Spec.Limit = st.Limit
	}
	return req
}

// semiJoinWireRows runs the join and unwraps each row to the side it kept.
//
// ok is false with a nil error when the store declined the request before sending anything,
// which is the signal to answer the predicate the other way.
func (e *Executor) semiJoinWireRows(req dbrpc.JoinRequest, st *Statement) ([]*classad.ClassAd, bool, error) {
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
