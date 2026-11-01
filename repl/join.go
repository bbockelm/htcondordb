package repl

// Executing a JOIN.
//
// A joined row is a ClassAd whose members are the two sides under their aliases, so every
// clause after FROM -- the projection, WHERE, ORDER BY -- is an ordinary ClassAd expression
// evaluated against that one composite ad. `j.Owner` is the ClassAd select operator over a
// nested ad; nothing here teaches the engine a new rule. An unmatched LEFT JOIN row simply
// has no member for the right side, so every `h.X` reads as undefined, which is ClassAd's
// own spelling of SQL's NULL.
//
// This is the CLIENT-side join: it issues only query ops that have existed since before
// joins did, so it works unchanged against any server the rest of this package talks to.
// Nothing in this file needs a new opcode, and nothing in it degrades if the server is
// older than the feature.
//
// What makes it affordable is pushing work into each side's own scan rather than fetching
// two tables whole:
//
//   - CONJUNCT SPLITTING. The && spine of ON and WHERE is split into conjuncts that mention
//     only one side, which become server-side constraints on that side's scan, and the rest,
//     which are evaluated here per candidate pair.
//   - PROJECTION. Each side fetches only the attributes the statement actually reads from it.
//   - KEY BATCHING. When the join has an equality on a bare attribute of the right side, the
//     left side's key values are folded into one membership constraint on the right scan, so
//     a selective left side reads a slice of the right table instead of all of it.
//
// Splitting the spine is sound even though ClassAd's && is left-biased and NOT commutative
// (error && false is error, while false && error is false). It is sound because a row is
// kept only when the constraint evaluates to a TRUE boolean, and the only way `A && B`
// yields true is for both A and B to be true -- which does not depend on their order. So
// the match set survives splitting and reordering, even though the VALUE does not. That is
// why this splitting is applied only to predicates (ON, WHERE) and never to a projected
// expression, where the difference between error, undefined and false is visible.
//
// The same reasoning does NOT extend to ||, which is left-biased in a way that changes the
// match set: `error || true` is error while `true || error` is true. So conjunctsOf splits
// the && spine only and treats a || node as one opaque conjunct, never looking inside it.

import (
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Join guard rails. A join can generate quadratic work from a short query, so the limits
// are explicit and refusing rather than implicit and slow.
const (
	// maxJoinSideRows caps the rows fetched from one side. Beyond it the statement is
	// refused with a pointer at WHERE, rather than silently reading a whole history table
	// into this process.
	maxJoinSideRows = 500000
	// maxJoinKeyBatch caps how many left-side key values are folded into the right side's
	// membership constraint. Past it the constraint would be longer than the scan it saves.
	maxJoinKeyBatch = 512
	// maxJoinOutputRows caps the rows a join may produce. The result is materialized here
	// to be sorted and rendered, so an unbounded one is an out-of-memory, not a slow query.
	// It bounds every INTERMEDIATE result too, not only the last one: a chain of joins can
	// blow up in the middle and come back down, and the row that exhausts memory does not
	// care which step built it.
	maxJoinOutputRows = 1_000_000
	// maxJoinTables caps how many tables one statement may join. The join order is the
	// order they were written -- there is no cost-based planner to choose another -- so
	// past a handful of tables the difference between a fast plan and a cross product is
	// the author's word order, and refusing says that better than a slow query does.
	maxJoinTables = 8
)

// maxJoinPairs caps how many candidate PAIRS the join may examine. The per-side cap does
// not bound this and must not be mistaken for it: two sides at the row cap are 2.5e11
// pairs. Nor does hashing bound it -- a join key with one distinct value puts every row in
// one bucket, and that bucket is the cross product. So the budget is spent where pairs are
// actually examined, which is the only place that sees both sides.
//
// A var rather than a const only so a test can lower it; nothing changes it at run time.
var maxJoinPairs = 20_000_000

// joinSide is one side of a join: which table, and what was pushed into its scan.
type joinSide struct {
	ref     TableRef
	where   string   // constraint pushed into this side's scan ("" = match all)
	project []string // attributes to fetch, or nil for whole ads
}

// joinPlan is a planned join over two or more tables.
//
// The shape is LEFT-DEEP: sides[0] is the FROM table, and each step folds one more table
// into the row built so far. `a JOIN b ON .. JOIN c ON ..` is planned as (a x b) x c, which
// is the only shape SQL's syntax can express without parentheses, and it is what makes the
// composite-ad model carry n tables for free -- a row simply gains a member per step, and
// every predicate stays an ordinary ClassAd expression over one ad.
type joinPlan struct {
	// sides are the tables, in FROM-then-JOIN order. sides[0] is read first; every other
	// side is brought in by the step of the same index.
	sides []joinSide

	// steps are the JOIN clauses, in source order; steps[i] brings in sides[i+1]. A
	// two-table join has exactly one.
	steps []joinStep

	// post are the WHERE conjuncts that could not be pushed down, compiled over the
	// composite ad and applied after the last step (and so after any null-extension).
	post    []*classad.Expr
	postSrc []string
}

// joinStep is one JOIN clause, planned: the table it folds in, and the predicates that
// decide the pairing at that point in the pipeline.
type joinStep struct {
	kind JoinKind
	side int // index into sides of the table this step folds in

	// cond are this step's ON conjuncts that could not be pushed to one side -- the join
	// condition proper -- compiled over the composite ad as it stands at this step. A pair
	// matches when every one is true.
	cond []*classad.Expr

	// equi are equality conjuncts of cond with the accumulated row on one side and this
	// step's table on the other. They drive the hash build/probe, and they NARROW
	// candidates only: every surviving pair is still checked against cond in full, so the
	// hash never has to reproduce ClassAd's equality semantics exactly.
	equi []equiPair

	// batchAttr is this side's bare attribute in a single equi pair, if there is one; it
	// is what key batching builds a membership constraint on.
	batchAttr string

	// The same predicates as text, for a server-side join: the wire carries ClassAd
	// expressions, not compiled ones. They are the text the compiled forms were BUILT from
	// (mustCompile parses e.String()), so they cannot disagree with them -- sending them is
	// exactly as safe as the local evaluation already is.
	condSrc []string
	equiSrc []equiText
}

// equiText is one equality pair as text, in the forms the wire needs.
type equiText struct {
	// acc reads the composite row with its aliases intact, exactly as the compiled acc
	// half does.
	acc string
	// side is this step's half, stripped of its alias so it reads against its own ad.
	side string
	// accStripped is acc with its alias stripped. That is only possible, and only
	// meaningful, when the acc half reads the FIRST table alone: the server-side join
	// evaluates its left half against that table's ad directly, so it needs this form and
	// not acc. It is "" otherwise, which is also how serverJoinEligible learns the pair
	// cannot be sent.
	accStripped string
}

// equiPair is one `<accumulated row> == <this step's table>` equality.
//
// The two halves evaluate in DIFFERENT SCOPES, which is the one asymmetry a reader has to
// hold on to. acc is evaluated against the accumulated composite row, so it is NOT
// alias-stripped: `a.ClusterId` has to read the composite's `a` member, and at the third
// step it might read `a` or `b` or both. side is evaluated against the new table's own ad,
// so it is stripped down to a bare attribute path.
type equiPair struct {
	acc, side *classad.Expr
}

// twoTable reports whether this is the two-table shape every path handled before n-way
// joins existed.
func (p *joinPlan) twoTable() bool { return len(p.sides) == 2 }

// --- planning ---

// planJoin resolves the statement's aliases, splits its predicates, and compiles what is
// left into a plan.
func planJoin(st *Statement) (*joinPlan, error) {
	if len(st.Joins) == 0 {
		return nil, fmt.Errorf("a join reads at least two tables")
	}
	refs := st.tableRefs()
	if len(refs) > maxJoinTables {
		return nil, fmt.Errorf("joining %d tables is not supported; the limit is %d",
			len(refs), maxJoinTables)
	}
	aliases := make(map[string]int, len(refs))
	for i, r := range refs {
		aliases[strings.ToLower(r.Alias)] = i
	}

	// ORDER BY may name a projected alias rather than an attribute -- `SELECT j.Cpus * 2 AS
	// twice ... ORDER BY twice`. Resolve those to the expression they stand for before
	// validation, which would otherwise read the alias as an unqualified attribute.
	resolveJoinOrderAliases(st)

	if err := validateJoinClauses(st, aliases, refs); err != nil {
		return nil, err
	}

	p := &joinPlan{sides: make([]joinSide, len(refs))}
	for i, r := range refs {
		p.sides[i] = joinSide{ref: r}
	}
	for i, jc := range st.Joins {
		p.steps = append(p.steps, joinStep{kind: jc.Kind, side: i + 1})
	}

	for i := range p.steps {
		if err := p.planStep(i, st.Joins[i].On, aliases); err != nil {
			return nil, err
		}
	}
	if err := p.planWhere(st.Where, aliases); err != nil {
		return nil, err
	}
	p.planProjection(st, aliases)
	return p, nil
}

// planStep splits one step's ON clause: what can filter a single table's scan, and what has
// to decide the pairing.
//
// The pushdown rule from the two-table case generalizes along the chain. A conjunct reading
// only the table this step folds in is a filter on it, for an INNER and for a LEFT join
// alike -- both push to the INNER side. A conjunct reading only a table already in the row
// is a filter on the PRESERVED side, which a LEFT join must not apply: a row failing this
// step's condition still appears, null-extended, so filtering that table's scan would drop
// rows the join must emit. Anything reading more than one table decides the pairing here.
//
// A forward reference -- this step's ON naming a table a later step folds in -- is refused
// rather than quietly deferred. SQL does not allow it, and a reader who writes it means
// something this would not do.
func (p *joinPlan) planStep(i int, on string, aliases map[string]int) error {
	st := &p.steps[i]
	parts, err := conjunctsOf(on)
	if err != nil {
		return fmt.Errorf("%s %s ON: %w", st.kind, p.sides[st.side].ref.Alias, err)
	}
	for _, c := range parts {
		mask := sidesOf(c, aliases)
		if ahead := mask &^ visibleAt(st.side); ahead != 0 {
			return fmt.Errorf("%s %s ON reads %s, which is joined later; an ON clause may "+
				"only refer to the tables above it", st.kind, p.sides[st.side].ref.Alias,
				p.aliasesIn(ahead))
		}
		switch one, single := soleSide(mask); {
		case !single:
			st.cond = append(st.cond, mustCompile(c))
			st.condSrc = append(st.condSrc, c.String())
		case one == st.side:
			p.pushTo(one, c, aliases)
		case st.kind == JoinLeft:
			// Preserved side under an outer join: must wait for the pairing.
			st.cond = append(st.cond, mustCompile(c))
			st.condSrc = append(st.condSrc, c.String())
		default:
			p.pushTo(one, c, aliases)
		}
	}
	st.extractEqui(p, on, aliases)
	return nil
}

// planWhere splits the WHERE clause. It is the mirror image of planStep: a conjunct on a
// table that some step null-extends must wait until after that step, where it correctly
// turns the outer join into an inner one only if that is what was written. Every other
// single-table conjunct filters that table's scan.
//
// A table is null-extendable exactly when its own step is a LEFT join -- in a left-deep
// chain every earlier table is on the preserved side of every later step, so being
// preserved once is being preserved throughout.
func (p *joinPlan) planWhere(where string, aliases map[string]int) error {
	parts, err := conjunctsOf(where)
	if err != nil {
		return fmt.Errorf("WHERE: %w", err)
	}
	for _, c := range parts {
		one, single := soleSide(sidesOf(c, aliases))
		if single && !p.nullExtendable(one) {
			p.pushTo(one, c, aliases)
			continue
		}
		p.post = append(p.post, mustCompile(c))
		p.postSrc = append(p.postSrc, c.String())
	}
	return nil
}

// pushTo folds a single-table conjunct into that table's scan constraint. A conjunct that
// does not survive being stripped of its alias is left for the pairing instead -- declining
// costs a pushdown, never an answer.
func (p *joinPlan) pushTo(side int, c ast.Expr, aliases map[string]int) {
	src := stripSide(c, aliases, side)
	if src == "" {
		p.post = append(p.post, mustCompile(c))
		p.postSrc = append(p.postSrc, c.String())
		return
	}
	p.sides[side].where = andText(p.sides[side].where, src)
}

// nullExtendable reports whether a row can reach the output with no member for this side,
// which is true exactly when the step that folds it in is a LEFT join. sides[0] is never
// null-extendable: it is the preserved side of every step.
func (p *joinPlan) nullExtendable(side int) bool {
	for i := range p.steps {
		if p.steps[i].side == side {
			return p.steps[i].kind == JoinLeft
		}
	}
	return false
}

// aliasesIn names the tables a side mask covers, for an error message.
func (p *joinPlan) aliasesIn(mask int) string {
	var names []string
	for i := range p.sides {
		if mask&sideBit(i) != 0 {
			names = append(names, p.sides[i].ref.Alias)
		}
	}
	return strings.Join(names, ", ")
}

// extractEqui finds this step's ON conjuncts of the form
// `<already-joined> == <this step's table>` and compiles each half in the scope it will be
// evaluated in. Used to hash rather than to decide: a pair the hash brings together is
// still checked against the whole condition.
func (st *joinStep) extractEqui(p *joinPlan, on string, aliases map[string]int) {
	parts, err := conjunctsOf(on)
	if err != nil {
		return
	}
	mine := sideBit(st.side)
	for _, c := range parts {
		bo, ok := unwrapParens(c).(*ast.BinaryOp)
		if !ok || bo.Op != "==" {
			continue
		}
		l, r := sidesOf(bo.Left, aliases), sidesOf(bo.Right, aliases)
		var accHalf, sideHalf ast.Expr
		// One half must read this step's table and nothing else; the other must read only
		// tables already in the row, and at least one of them -- a half reading no table is
		// a constant, which is a filter and not a join key.
		switch {
		case r == mine && l != 0 && l&mine == 0:
			accHalf, sideHalf = bo.Left, bo.Right
		case l == mine && r != 0 && r&mine == 0:
			accHalf, sideHalf = bo.Right, bo.Left
		default:
			continue
		}
		// The accumulated half keeps its aliases: it reads the composite row, where
		// `a.ClusterId` is a select over the `a` member. Only this side's half is stripped,
		// because it reads its own table's ad directly.
		accSrc := accHalf.String()
		sideSrc := stripSide(sideHalf, aliases, st.side)
		if sideSrc == "" {
			continue
		}
		ae, aerr := classad.ParseExpr(accSrc)
		se, serr := classad.ParseExpr(sideSrc)
		if aerr != nil || serr != nil {
			continue
		}
		// The server-side join needs the acc half against the first table's own ad, which
		// only exists as text when that is the only table it reads.
		accStripped := ""
		if sidesOf(accHalf, aliases) == sideBit(0) {
			accStripped = stripSide(accHalf, aliases, 0)
		}
		st.equi = append(st.equi, equiPair{acc: ae, side: se})
		st.equiSrc = append(st.equiSrc, equiText{acc: accSrc, side: sideSrc, accStripped: accStripped})
		// Key batching needs a bare attribute on this side so the membership constraint
		// can name it. Anything computed is still hashable, just not batchable.
		if st.batchAttr == "" {
			if ar, ok := unwrapParens(sideHalf).(*ast.SelectExpr); ok {
				if base, attr, ok := qualifiedRef(ar, aliases); ok && base == st.side {
					st.batchAttr = attr
				}
			}
		}
	}
}

// planProjection records, for each side, the attributes the statement actually reads from
// it -- or nil for that side when the statement reads attributes this cannot enumerate (a
// `*`, or an expression that does not parse) and the whole ad is needed.
func (p *joinPlan) planProjection(st *Statement, aliases map[string]int) {
	n := len(p.sides)
	seen := make([]map[string]bool, n)
	out := make([][]string, n)
	whole := make([]bool, n)
	for i := range seen {
		seen[i] = map[string]bool{}
	}
	allWhole := func() {
		for i := range whole {
			whole[i] = true
		}
	}
	add := func(side int, attr string) {
		k := strings.ToLower(attr)
		if attr == "" || seen[side][k] {
			return
		}
		seen[side][k] = true
		out[side] = append(out[side], attr)
	}
	scan := func(src string) {
		if strings.TrimSpace(src) == "" {
			return
		}
		q, err := vm.Parse(src)
		if err != nil {
			allWhole()
			return
		}
		walkAST(q.Expr(), func(n ast.Expr) bool {
			if se, ok := n.(*ast.SelectExpr); ok {
				if side, attr, ok := qualifiedRef(se, aliases); ok {
					add(side, attr)
					return false // the alias itself is not an attribute to fetch
				}
			}
			return true
		})
	}
	for _, it := range st.Items {
		switch {
		case it.Star && it.Col == "":
			allWhole()
		case it.Star:
			if side, ok := aliases[strings.ToLower(it.Col)]; ok {
				whole[side] = true
			}
		case it.Expr != "":
			scan(it.Expr)
		default:
			// A bare column in a join is refused during validation, so reaching here
			// means an attribute set this cannot reason about; fetch everything.
			allWhole()
		}
	}
	scan(st.Where)
	for _, j := range st.Joins {
		scan(j.On)
	}
	for _, t := range st.OrderBy {
		if t.Item.Expr != "" {
			scan(t.Item.Expr)
		} else {
			allWhole()
		}
	}
	for i := range p.sides {
		if whole[i] {
			out[i] = nil
		}
		p.sides[i].project = out[i]
	}
}

// --- reference resolution ---

// sideNone is the mask of an expression that reads no table at all -- a constant, or
// CurrentTime. It is not a side a conjunct can be pushed to, so it decides the pairing
// along with the genuinely cross-table conjuncts.
const sideNone = 0

// sideBit is side i's bit in a side mask. A mask is an int, so maxJoinTables sides fit with
// room to spare.
func sideBit(i int) int { return 1 << i }

// visibleAt is the mask of the sides present in the row at the step that folds in side --
// that side and every one before it. A reference outside it is a forward reference.
func visibleAt(side int) int { return 1<<(side+1) - 1 }

// soleSide reports the one side a mask covers, and whether it covers exactly one. A mask of
// no sides is not a single side: a constant conjunct cannot be pushed into a scan, since
// there is no scan it belongs to more than any other.
func soleSide(mask int) (int, bool) {
	if mask == sideNone || mask&(mask-1) != 0 {
		return 0, false
	}
	return bits.TrailingZeros(uint(mask)), true
}

// magicUnscoped are the unscoped names a join expression may still use: attributes the
// engine itself supplies rather than reading from an ad, so there is no table to qualify
// them with.
var magicUnscoped = map[string]bool{"currenttime": true}

// qualifiedRef reports the side and attribute of an `alias.Attr` select, when its record is
// a bare reference to a known alias. `a.b.c` resolves `a.b` here and leaves `.c` to the
// evaluator as an ordinary nested select.
func qualifiedRef(se *ast.SelectExpr, aliases map[string]int) (side int, attr string, ok bool) {
	ar, isRef := unwrapParens(se.Record).(*ast.AttributeReference)
	if !isRef || ar.Scope != ast.NoScope {
		return 0, "", false
	}
	idx, known := aliases[strings.ToLower(ar.Name)]
	if !known {
		return 0, "", false
	}
	return idx, se.Attr, true
}

// sidesOf reports which tables an expression reads, as a bitmask of sideLeft/sideRight.
func sidesOf(e ast.Expr, aliases map[string]int) int {
	mask := sideNone
	walkAST(e, func(n ast.Expr) bool {
		switch v := n.(type) {
		case *ast.SelectExpr:
			if side, _, ok := qualifiedRef(v, aliases); ok {
				mask |= 1 << side
				return false
			}
		case *ast.AttributeReference:
			// MY and TARGET are the two-table spelling, and mean the first and second
			// tables. With more than two they are ambiguous, which validateJoinExpr
			// refuses outright rather than resolving by position.
			switch v.Scope {
			case ast.MyScope:
				mask |= sideBit(0)
			case ast.TargetScope:
				mask |= sideBit(1)
			}
		}
		return true
	})
	return mask
}

// stripSide rewrites an expression that reads only one side into one that reads that side's
// ad directly: `j.Owner` becomes `Owner`, `MY.Owner` becomes `Owner`. The result is the text
// of a constraint the server can evaluate against that table.
//
// A node kind this does not know how to rewrite makes it give up and return "", and the
// caller then leaves the conjunct to be evaluated over the composite ad. Declining is always
// safe: it costs a pushdown, never an answer.
func stripSide(e ast.Expr, aliases map[string]int, side int) string {
	out := stripExpr(e, aliases, side)
	if out == nil {
		return ""
	}
	return out.String()
}

func stripExpr(e ast.Expr, aliases map[string]int, side int) ast.Expr {
	switch n := e.(type) {
	case nil:
		return nil
	case *ast.SelectExpr:
		if s, attr, ok := qualifiedRef(n, aliases); ok {
			if s != side {
				return nil
			}
			return &ast.AttributeReference{Name: attr, Scope: ast.NoScope}
		}
		rec := stripExpr(n.Record, aliases, side)
		if rec == nil {
			return nil
		}
		return &ast.SelectExpr{Record: rec, Attr: n.Attr}
	case *ast.AttributeReference:
		switch n.Scope {
		case ast.MyScope:
			if side != 0 {
				return nil
			}
			return &ast.AttributeReference{Name: n.Name, Scope: ast.NoScope}
		case ast.TargetScope:
			if side != 1 {
				return nil
			}
			return &ast.AttributeReference{Name: n.Name, Scope: ast.NoScope}
		case ast.NoScope:
			return n // a magic name like CurrentTime; validation let it through
		}
		return nil
	case *ast.BinaryOp:
		l, r := stripExpr(n.Left, aliases, side), stripExpr(n.Right, aliases, side)
		if l == nil || r == nil {
			return nil
		}
		return &ast.BinaryOp{Op: n.Op, Left: l, Right: r}
	case *ast.UnaryOp:
		x := stripExpr(n.Expr, aliases, side)
		if x == nil {
			return nil
		}
		return &ast.UnaryOp{Op: n.Op, Expr: x}
	case *ast.ParenExpr:
		x := stripExpr(n.Inner, aliases, side)
		if x == nil {
			return nil
		}
		return &ast.ParenExpr{Inner: x}
	case *ast.ConditionalExpr:
		c, t, f := stripExpr(n.Condition, aliases, side), stripExpr(n.TrueExpr, aliases, side), stripExpr(n.FalseExpr, aliases, side)
		if c == nil || t == nil || f == nil {
			return nil
		}
		return &ast.ConditionalExpr{Condition: c, TrueExpr: t, FalseExpr: f}
	case *ast.ElvisExpr:
		l, r := stripExpr(n.Left, aliases, side), stripExpr(n.Right, aliases, side)
		if l == nil || r == nil {
			return nil
		}
		return &ast.ElvisExpr{Left: l, Right: r}
	case *ast.FunctionCall:
		args := make([]ast.Expr, len(n.Args))
		for i, a := range n.Args {
			if args[i] = stripExpr(a, aliases, side); args[i] == nil {
				return nil
			}
		}
		return &ast.FunctionCall{Name: n.Name, Args: args}
	case *ast.ListLiteral:
		els := make([]ast.Expr, len(n.Elements))
		for i, el := range n.Elements {
			if els[i] = stripExpr(el, aliases, side); els[i] == nil {
				return nil
			}
		}
		return &ast.ListLiteral{Elements: els}
	case *ast.SubscriptExpr:
		c, i := stripExpr(n.Container, aliases, side), stripExpr(n.Index, aliases, side)
		if c == nil || i == nil {
			return nil
		}
		return &ast.SubscriptExpr{Container: c, Index: i}
	case *ast.IntegerLiteral, *ast.RealLiteral, *ast.StringLiteral, *ast.BooleanLiteral,
		*ast.UndefinedLiteral, *ast.ErrorLiteral:
		return e
	}
	return nil // an unknown node kind: decline the pushdown rather than guess
}

// conjunctsOf parses an expression and returns its top-level && operands. An empty
// expression has none.
func conjunctsOf(src string) ([]ast.Expr, error) {
	if strings.TrimSpace(src) == "" {
		return nil, nil
	}
	q, err := vm.Parse(src)
	if err != nil {
		return nil, err
	}
	var out []ast.Expr
	var walk func(ast.Expr)
	walk = func(e ast.Expr) {
		switch n := e.(type) {
		case *ast.ParenExpr:
			walk(n.Inner)
		case *ast.BinaryOp:
			if n.Op == "&&" {
				walk(n.Left)
				walk(n.Right)
				return
			}
			out = append(out, e)
		default:
			out = append(out, e)
		}
	}
	walk(q.Expr())
	return out, nil
}

func unwrapParens(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.Inner
	}
}

func mustCompile(e ast.Expr) *classad.Expr {
	// The text came from an expression this package just parsed, so re-parsing it cannot
	// fail; a nil result would be caught by the caller's own evaluation anyway.
	ex, err := classad.ParseExpr(e.String())
	if err != nil {
		return nil
	}
	return ex
}

func andText(a, b string) string {
	switch {
	case b == "":
		return a
	case a == "":
		return b
	default:
		return "(" + a + ") && (" + b + ")"
	}
}

// walkAST visits e and every expression reachable from it. visit returns false to stop
// descending into that node.
//
// It walks by REFLECTION rather than a type switch per node kind, for the same reason the
// authorization walk does: a type switch that misses a node kind here would let an
// unqualified reference through validation unnoticed, and the AST has many node types with
// more possible later. This runs once per statement, never per row.
func walkAST(e ast.Expr, visit func(ast.Expr) bool) {
	if e == nil || reflect.ValueOf(e).IsZero() {
		return
	}
	if !visit(e) {
		return
	}
	walkValue(reflect.ValueOf(e), visit, 0)
}

const maxASTDepth = 256

var exprType = reflect.TypeOf((*ast.Expr)(nil)).Elem()

func walkValue(v reflect.Value, visit func(ast.Expr) bool, depth int) {
	if depth > maxASTDepth || !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return
		}
		if v.Type().Implements(exprType) || (v.Kind() == reflect.Interface && v.Elem().Type().Implements(exprType)) {
			if child, ok := v.Interface().(ast.Expr); ok && depth > 0 {
				walkAST(child, visit)
				return
			}
		}
		walkValue(v.Elem(), visit, depth+1)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				walkValue(v.Field(i), visit, depth+1)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			walkValue(v.Index(i), visit, depth+1)
		}
	}
}

// --- validation ---

// validateJoinClauses refuses a join query whose expressions this cannot resolve
// unambiguously, with a message that says which alias to write.
func validateJoinClauses(st *Statement, aliases map[string]int, refs []TableRef) error {
	check := func(what, src string) error {
		return validateJoinExpr(what, src, aliases, refs)
	}
	// checkArg validates one aggregate's argument. COUNT(*) has no attribute to qualify;
	// anything else must name its table, which over a composite row means a dotted path.
	checkArg := func(what string, a AggCall) error {
		if a.Arg != "" && a.Arg != "*" {
			if !strings.Contains(a.Arg, ".") {
				return unqualifiedErr(what+" "+a.Func+"()", a.Arg, refs)
			}
			if err := check(what, a.Arg); err != nil {
				return err
			}
		}
		return check(what+" FILTER", a.Filter)
	}
	for _, j := range st.Joins {
		if err := check("JOIN ON", j.On); err != nil {
			return err
		}
	}
	if err := check("WHERE", st.Where); err != nil {
		return err
	}
	for _, g := range st.GroupBy {
		if err := check("GROUP BY", g); err != nil {
			return err
		}
	}
	for _, it := range append(st.Items, orderItems(st)...) {
		what := "SELECT"
		if it.fromOrderBy {
			what = "ORDER BY"
		}
		switch {
		case it.Star:
			// `*` and `alias.*` name no single attribute; the expansion is checked when
			// the column set is built.
		case it.IsAggregate():
			if err := checkArg(what, AggCall{Func: it.Agg, Arg: it.Col, Filter: it.AggFilter}); err != nil {
				return err
			}
		case len(it.Aggs) > 0:
			// An expression with aggregates lifted out of it: the surrounding expression
			// refers to each as __agg_N, which validateJoinExpr allows unqualified.
			for _, a := range it.Aggs {
				if err := checkArg(what, a); err != nil {
					return err
				}
			}
			if err := check(what, it.Expr); err != nil {
				return err
			}
		case it.Expr == "":
			return unqualifiedErr(what, it.Col, refs)
		default:
			if err := check(what, it.Expr); err != nil {
				return err
			}
		}
	}
	for _, a := range st.HavingAggs {
		if err := checkArg("HAVING", a); err != nil {
			return err
		}
	}
	return check("HAVING", st.Having)
}

// orderItems returns the ORDER BY terms as select items tagged with their origin, so one
// loop validates both lists and the error names the clause the reader wrote.
func orderItems(st *Statement) []SelectItem {
	out := make([]SelectItem, 0, len(st.OrderBy))
	for _, t := range st.OrderBy {
		it := t.Item
		it.fromOrderBy = true
		out = append(out, it)
	}
	return out
}

func validateJoinExpr(what, src string, aliases map[string]int, refs []TableRef) error {
	if strings.TrimSpace(src) == "" {
		return nil
	}
	q, err := vm.Parse(src)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	var bad error
	walkAST(q.Expr(), func(n ast.Expr) bool {
		if bad != nil {
			return false
		}
		switch v := n.(type) {
		case *ast.SelectExpr:
			if _, _, ok := qualifiedRef(v, aliases); ok {
				return false // resolved: the alias is not itself an attribute
			}
			// `x.Attr` where x is not an alias is a nested-ad select, which is fine --
			// but x must itself resolve, so keep descending.
		case *ast.AttributeReference:
			if v.Scope == ast.ParentScope {
				bad = fmt.Errorf("%s: PARENT.%s has no meaning in a join", what, v.Name)
				return false
			}
			// MY and TARGET name the first and second tables. Over three or more that is
			// a guess about which pair was meant, so it is refused and the writer names
			// the table -- the aliases are right there in the FROM clause.
			if (v.Scope == ast.MyScope || v.Scope == ast.TargetScope) && len(refs) > 2 {
				bad = fmt.Errorf("%s: %s.%s is ambiguous across %d tables; qualify it with "+
					"a table alias instead", what, scopeName(v.Scope), v.Name, len(refs))
				return false
			}
			if v.Scope == ast.NoScope && !magicUnscoped[strings.ToLower(v.Name)] &&
				!strings.HasPrefix(strings.ToLower(v.Name), strings.ToLower(aggPlaceholderPrefix)) {
				bad = unqualifiedErr(what, v.Name, refs)
				return false
			}
		case *ast.RecordLiteral:
			bad = fmt.Errorf("%s: a ClassAd literal is not supported in a join condition", what)
			return false
		case *ast.FunctionCall:
			if strings.EqualFold(v.Name, "eval") {
				bad = fmt.Errorf("%s: eval() is not supported in a join -- it builds an "+
					"attribute name at run time, which cannot be resolved to a table", what)
				return false
			}
		}
		return true
	})
	return bad
}

func unqualifiedErr(what, name string, refs []TableRef) error {
	forms := make([]string, len(refs))
	for i, r := range refs {
		forms[i] = r.Alias + "." + name
	}
	return fmt.Errorf("%s: %q is not qualified -- in a join every attribute must say which "+
		"table it comes from, as %s", what, name, strings.Join(forms, " or "))
}

// scopeName spells a scope prefix the way it was written, for an error that quotes it back.
func scopeName(sc ast.AttributeScope) string {
	if sc == ast.TargetScope {
		return "TARGET"
	}
	return "MY"
}

// --- execution ---

// execJoin runs the join entirely in this process, over query ops that predate the feature.
// See the file comment for the strategy.
func (e *Executor) execJoin(st *Statement) (*Result, error) {
	if err := refuseUnsupportedJoin(st); err != nil {
		return nil, err
	}
	p, err := planJoin(st)
	if err != nil {
		return nil, err
	}

	// Ask the store to join, when it can. Only the joined rows then cross the wire, where
	// joining here ships both sides whole first -- a flat ~2.2x on a narrow projection and
	// ~4.2x unprojected, before any network. The server also holds one side where this
	// holds both. Everything after the pairing is unchanged: the rows that come back are
	// the same composite ads the pipeline would have built.
	if rows, ok, serr := e.joinServerSide(st, p); serr != nil {
		return nil, serr
	} else if ok {
		return e.finishJoin(st, p, rows)
	}

	rows, err := e.runJoinPipeline(st, p)
	if err != nil {
		return nil, err
	}
	return e.finishJoin(st, p, rows)
}

// runJoinPipeline folds one table into the row at a time, left to right.
//
// Every step has the same shape of input -- a composite row and one table's ads -- which is
// what the first table is wrapped for. The wrap costs one ad per row of the first table and
// buys a pipeline with no special first iteration, so a three-table join is the two-table
// one run twice rather than a second algorithm.
func (e *Executor) runJoinPipeline(st *Statement, p *joinPlan) ([]*classad.ClassAd, error) {
	first, err := e.fetchJoinSide(st, p.sides[0], 0)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p.sides[0].ref.Table, err)
	}
	rows := p.wrapFirst(first)

	// One pair budget for the whole statement, not one per step. A chain of joins that each
	// stay just inside a per-step budget would otherwise cost the sum of them, and the
	// budget exists to bound the total work.
	budget := joinBudget{pairs: maxJoinPairs}
	for i := range p.steps {
		step := &p.steps[i]
		side := p.sides[step.side]
		// Key batching: with the row so far in hand and an equality on a bare attribute of
		// the table about to be read, that scan can be narrowed to the values the rows
		// actually hold, instead of reading the whole table and discarding most of it here.
		// Whether that is cheaper than reading the whole table depends on how big the whole
		// table is, which is what decideKeyBatch weighs. It applies at every step, because
		// a step's input is a composite row either way.
		if d := e.decideKeyBatch(step, side, rows); d.constraint != "" {
			side.where = andText(side.where, d.constraint)
		}
		ads, ferr := e.fetchJoinSide(st, side, step.side)
		if ferr != nil {
			return nil, fmt.Errorf("%s: %w", side.ref.Table, ferr)
		}
		rows, err = p.runStep(step, rows, ads, &budget)
		if err != nil {
			return nil, err
		}
	}

	return joinRowAds(rows), nil
}

// wrapFirst turns the first table's ads into one-member composite rows -- the input shape
// every step takes.
//
// The one-element tuples are carved out of a single backing array rather than allocated
// one at a time. At the sizes a join reaches, one allocation per row of the first table is
// worth not making.
func (p *joinPlan) wrapFirst(ads []*classad.ClassAd) []joinTuple {
	rows := make([]joinTuple, len(ads))
	backing := make([]*classad.ClassAd, len(ads))
	for i, ad := range ads {
		row := classad.New()
		row.InsertAttrClassAd(p.sides[0].ref.Alias, ad)
		backing[i] = ad
		rows[i] = joinTuple{ads: backing[i : i+1 : i+1], ad: row}
	}
	return rows
}

// joinRowAds drops the per-side tuples, which only the pipeline needs, and keeps the
// composite rows, which are the join's result.
func joinRowAds(rows []joinTuple) []*classad.ClassAd {
	out := make([]*classad.ClassAd, len(rows))
	for i := range rows {
		out[i] = rows[i].ad
	}
	return out
}

// joinTuple is one row in flight: the per-side ads it was built from, and the composite ad
// every predicate is evaluated against.
//
// Carrying both means a step never has to take a composite apart to build the next one. It
// matters because the composite is rebuilt once per CANDIDATE pair, not once per output row
// -- the condition has to be evaluated against a row that may not survive -- so the cost of
// reading the previous composite back would be paid on rejects too.
type joinTuple struct {
	ads []*classad.ClassAd // one per side folded in so far; nil = null-extended
	ad  *classad.ClassAd   // the composite, members under the side aliases
}

// joinBudget is the pair allowance shared by every step of one statement.
type joinBudget struct {
	pairs int
}

// runStep folds one table into the rows built so far and returns the rows that result.
func (p *joinPlan) runStep(step *joinStep, rows []joinTuple, ads []*classad.ClassAd, budget *joinBudget) ([]joinTuple, error) {
	var out []joinTuple
	last := step.side == len(p.sides)-1

	// Hash the new table on the equality keys when there are any; otherwise every row so
	// far is paired against every row of it and decided by the condition alone.
	var buckets map[string][]*classad.ClassAd
	if len(step.equi) > 0 {
		buckets = make(map[string][]*classad.ClassAd, len(ads))
		for _, ad := range ads {
			k, ok := step.sideKey(ad)
			if !ok {
				continue // an undefined key equals nothing: ClassAd == propagates undefined
			}
			buckets[k] = append(buckets[k], ad)
		}
	}

	for _, t := range rows {
		matched := false
		candidates := ads
		if buckets != nil {
			k, ok := step.accKey(t.ad)
			if !ok {
				candidates = nil // an undefined key on the accumulated row matches nothing
			} else {
				candidates = buckets[k]
			}
		}
		for _, r := range candidates {
			budget.pairs--
			if budget.pairs < 0 {
				return nil, p.tooManyPairs(step, len(rows), len(ads))
			}
			// The hash only narrowed the candidates; the condition decides, in full, with
			// ClassAd's own semantics.
			row := p.compose(t, step.side, r)
			if !step.condHolds(row) {
				continue
			}
			matched = true
			out = append(out, p.keep(t, step.side, r, row, last))
			if len(out) > maxJoinOutputRows {
				return nil, fmt.Errorf("the join produced more than %d rows; "+
					"narrow it with WHERE (LIMIT does not help -- the rows are built "+
					"before they are limited)", maxJoinOutputRows)
			}
		}
		if !matched && step.kind == JoinLeft {
			// Null-extended: every reference to this step's table reads as undefined. The
			// row keeps the composite it already had, which is exactly that.
			out = append(out, p.keep(t, step.side, nil, t.ad, last))
		}
	}

	// The WHERE residual is applied once, after the last step, so that a conjunct spanning
	// tables sees every one of them and a LEFT JOIN's null-extension has already happened.
	if last && len(p.post) > 0 {
		kept := out[:0]
		for _, t := range out {
			if allTrue(p.post, t.ad) {
				kept = append(kept, t)
			}
		}
		out = kept
	}
	return out, nil
}

// compose builds the composite row for a candidate pair, without committing to it. r may be
// nil, which leaves that side with no member -- the model's spelling of NULL.
func (p *joinPlan) compose(t joinTuple, side int, r *classad.ClassAd) *classad.ClassAd {
	row := classad.New()
	for i, ad := range t.ads {
		if ad != nil {
			row.InsertAttrClassAd(p.sides[i].ref.Alias, ad)
		}
	}
	if r != nil {
		row.InsertAttrClassAd(p.sides[side].ref.Alias, r)
	}
	return row
}

// keep turns a surviving candidate into a row the next step can extend, reusing the
// composite that was already built to decide it.
//
// On the LAST step there is no next step, so the per-side tuple is dead weight -- and it
// would be one allocation per output row, which is the whole difference between this
// pipeline and the two-table loop it replaced.
func (p *joinPlan) keep(t joinTuple, side int, r, row *classad.ClassAd, last bool) joinTuple {
	if last {
		return joinTuple{ad: row}
	}
	ads := make([]*classad.ClassAd, side+1)
	copy(ads, t.ads)
	ads[side] = r
	return joinTuple{ads: ads, ad: row}
}

// condHolds evaluates this step's join condition over a candidate row.
func (step *joinStep) condHolds(row *classad.ClassAd) bool {
	return allTrue(step.cond, row)
}

// allTrue reports whether every expression evaluates to a true boolean against the row. A
// nil expression is not true: a predicate that failed to compile must not be read as one
// that passed.
func allTrue(exprs []*classad.Expr, row *classad.ClassAd) bool {
	for _, ex := range exprs {
		if ex == nil || !isTrue(ex.Eval(row)) {
			return false
		}
	}
	return true
}

// finishJoin is everything after the pairing: grouping, DISTINCT, ordering, the limit and
// rendering. It is shared by both ways of producing the rows, because the rows are the same
// composite ads either way -- which is what makes the server-side join a change of where the
// pairing happens and not a second implementation of the statement.
func (e *Executor) finishJoin(st *Statement, p *joinPlan, rows []*classad.ClassAd) (*Result, error) {
	// Aggregation happens over the joined rows, which only exist here -- the server's
	// aggregate is single-table and has no notion of a composite row. So a grouped join is
	// reduced client-side, and EXPLAIN says so.
	if hasAggregate(st) || len(st.GroupBy) > 0 {
		return e.aggregateJoinRows(st, rows)
	}
	if st.Distinct {
		rows = dedupeAds(rows)
	}
	if len(st.OrderBy) > 0 {
		if err := sortJoinAds(rows, st.OrderBy); err != nil {
			return nil, err
		}
	}
	rows = applyLimit(rows, st.Limit)
	return e.renderJoinRows(st, p, rows)
}

// refuseUnsupportedJoin rejects the clauses a join does not carry yet, each with the reason
// rather than a bare "unsupported".
func refuseUnsupportedJoin(st *Statement) error {
	if hasWindow(st) {
		return fmt.Errorf("window functions over a join are not supported yet")
	}
	if st.Qualify != "" {
		return fmt.Errorf("QUALIFY filters a window column, which a join does not support yet")
	}
	return nil
}

// fetchJoinSide reads one side, pushing its constraint and projection into the scan.
func (e *Executor) fetchJoinSide(st *Statement, s joinSide, side int) ([]*classad.ClassAd, error) {
	var ads []*classad.ClassAd
	var err error
	// A point-in-time read has no projected variant, so an AS OF join fetches whole ads.
	// Both sides read at the same instant, which is what makes the result a consistent
	// picture rather than two unrelated ones.
	if s.project != nil && st.AsOf == "" && !e.txReads(s.ref.Table) {
		ads, err = e.queryAdsProjected(s.ref.Table, s.where, s.project, 0)
	} else {
		ads, err = e.queryAdsAsOf(s.ref.Table, s.where, 0, st.AsOf)
	}
	if err != nil {
		return nil, err
	}
	if len(ads) > maxJoinSideRows {
		return nil, fmt.Errorf("the join would read %d rows from %s (the cap is %d); "+
			"narrow it with WHERE %s.<attr> = ...",
			len(ads), s.ref.Table, maxJoinSideRows, s.ref.Alias)
	}
	return ads, nil
}

// keyBatchConstraint folds the accumulated rows' key values into one membership constraint
// on this step's table. It returns "" when there is no single bare-attribute equality, when
// a row's key is not a usable literal, or when there are more distinct values than the
// constraint should carry -- in each case the table is simply scanned as planned.
func (step *joinStep) keyBatchConstraint(rows []joinTuple) string {
	return step.batchConstraintFor(step.keyLiterals(rows))
}

// keyLiterals collects the accumulated rows' distinct key values as ClassAd literals,
// sorted so the same query explains the same way. It returns nil when there is no single
// bare-attribute equality to batch on, when no row has a usable key value, or when there
// are more distinct values than one constraint should carry.
func (step *joinStep) keyLiterals(rows []joinTuple) []string {
	if step.batchAttr == "" || len(step.equi) != 1 || len(rows) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var lits []string
	for _, t := range rows {
		lit, ok := literalOf(step.equi[0].acc.Eval(t.ad))
		if !ok {
			// A row with no usable key value matches nothing through this equality, so it
			// contributes no constraint term -- but it also cannot be allowed to narrow
			// this side on its behalf, which it does not.
			continue
		}
		if seen[lit] {
			continue
		}
		seen[lit] = true
		if len(lits) >= maxJoinKeyBatch {
			return nil // more keys than one constraint should carry
		}
		lits = append(lits, lit)
	}
	if len(lits) == 0 {
		return nil
	}
	sort.Strings(lits)
	return lits
}

// batchConstraintFor folds the key literals into one membership constraint on this step's
// attribute.
func (step *joinStep) batchConstraintFor(lits []string) string {
	if len(lits) == 0 {
		return ""
	}
	terms := make([]string, len(lits))
	for i, l := range lits {
		terms[i] = step.batchAttr + " == " + l
	}
	return "(" + strings.Join(terms, " || ") + ")"
}

// minRightRowsPerKey is how many rows the right side must hold per left-side key before
// narrowing its scan to those keys is worth the constraint's own cost.
//
// The constraint is not free: its cost grows with the key count, while the scan it avoids
// grows with the right table's size, so whether it pays is a trade between two quantities
// and not a property of either alone. Measured at 400 keys, batching cost 4.0x MORE against
// a 1,024-row right table and 1.5x more at 4,096 rows, broke even around 8,000, and was 2.0x
// better at 16,384 and 8.0x better at 65,536 -- break-even near 20 rows per key.
//
// A var rather than a const so a test can move it, and so a site whose data disagrees with
// the fixture this was measured on has somewhere to change it. The figure is one shape's
// break-even, not a law.
var minRightRowsPerKey = 20

// keyBatchDecision is why the join did or did not narrow the right side's scan, so EXPLAIN
// can say which and on what basis.
type keyBatchDecision struct {
	constraint string // "" = not batching
	keys       int
	rightRows  int
	knownRows  bool // false = the right side's size could not be learned cheaply
}

// decideKeyBatch chooses whether to narrow the right side's scan to the left side's keys.
//
// Knowing the key count is only half the trade; the other half is how many rows the
// unbatched scan would read. That number is on the server, so it is asked for only when the
// answer can change the decision -- there is something to batch on, and the right side
// carries no constraint of its own, which would make a whole-table count an overestimate of
// what the scan actually reads. When it cannot be learned cheaply the decision falls back to
// the key count alone, which is what this did before the cost was measured.
func (e *Executor) decideKeyBatch(step *joinStep, side joinSide, rows []joinTuple) keyBatchDecision {
	lits := step.keyLiterals(rows)
	d := keyBatchDecision{keys: len(lits)}
	if len(lits) == 0 {
		return d
	}
	if side.where != "" {
		// This side already filters itself, so its whole-table count would overstate the
		// scan being avoided. Keep the pre-measurement behavior rather than decide on a
		// number that does not describe this plan.
		d.constraint = step.batchConstraintFor(lits)
		return d
	}
	n, ok := e.cheapRowCount(side.ref.Table)
	d.rightRows, d.knownRows = n, ok
	if ok && n < len(lits)*minRightRowsPerKey {
		return d // the scan this would avoid is smaller than the constraint costs
	}
	d.constraint = step.batchConstraintFor(lits)
	return d
}

// literalOf renders a scalar value as the ClassAd literal naming it, for the membership
// constraint. Undefined, error and non-scalars have no literal and report false.
func literalOf(v classad.Value) (string, bool) {
	switch {
	case v.IsBool():
		b, _ := v.BoolValue()
		if b {
			return "true", true
		}
		return "false", true
	case v.IsInteger():
		i, _ := v.IntValue()
		return fmt.Sprintf("%d", i), true
	case v.IsReal():
		f, _ := v.RealValue()
		return strconv.FormatFloat(f, 'g', -1, 64), true
	case v.IsString():
		s, _ := v.StringValue()
		return quoteClassAd(s), true
	}
	return "", false
}

// accKey renders the accumulated row's key values into a bucket key, and sideKey the new
// table's. They differ only in which half of each equality they evaluate, and in what they
// evaluate it against: accKey reads the composite row, sideKey the table's own ad.
func (step *joinStep) accKey(row *classad.ClassAd) (string, bool) {
	return step.bucketKey(row, true)
}

func (step *joinStep) sideKey(ad *classad.ClassAd) (string, bool) {
	return step.bucketKey(ad, false)
}

// bucketKey renders one ad's equality-key values into a bucket key.
//
// It must never put two ClassAd-equal values in different buckets, or a matching pair would
// never be considered. So numbers of every kind (int, real, bool) normalize to one float
// form -- ClassAd's == coerces across them -- and strings fold to lower case, because
// ClassAd compares strings case-insensitively. Collisions in the other direction cost
// nothing: a bucket is only a candidate list, and the condition re-checks every pair.
func (step *joinStep) bucketKey(ad *classad.ClassAd, acc bool) (string, bool) {
	var b strings.Builder
	for i, pair := range step.equi {
		ex := pair.side
		if acc {
			ex = pair.acc
		}
		if ex == nil {
			return "", false
		}
		v := ex.Eval(ad)
		switch {
		case v.IsBool():
			x, _ := v.BoolValue()
			if x {
				b.WriteString("n1")
			} else {
				b.WriteString("n0")
			}
		case v.IsInteger():
			n, _ := v.IntValue()
			b.WriteString("n")
			b.WriteString(strconv.FormatFloat(float64(n), 'g', -1, 64))
		case v.IsReal():
			f, _ := v.RealValue()
			b.WriteString("n")
			b.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
		case v.IsString():
			s, _ := v.StringValue()
			b.WriteString("s")
			b.WriteString(strings.ToLower(s))
		default:
			return "", false // undefined, error, list or ad: equals nothing
		}
		if i < len(step.equi)-1 {
			b.WriteByte(0)
		}
	}
	return b.String(), true
}

// tooManyPairs explains a budget refusal in terms of what the writer can change: whether
// the condition gave this step anything to key on, and how big its two inputs were.
func (p *joinPlan) tooManyPairs(step *joinStep, nrows, nads int) error {
	side := p.sides[step.side].ref.Alias
	rowsOf := "the rows so far"
	if step.side == 1 {
		rowsOf = p.sides[0].ref.Alias
	}
	why := fmt.Sprintf("%s has %d rows and %s has %d", rowsOf, nrows, side, nads)
	switch {
	case len(step.equi) == 0:
		return fmt.Errorf("the join would examine more than %d pairs (%s): the ON clause "+
			"joining %s has no equality against the tables above it, so every row is "+
			"compared with every row. Add an equality term, or narrow a side with WHERE",
			maxJoinPairs, why, side)
	default:
		return fmt.Errorf("the join would examine more than %d pairs (%s): the key joining "+
			"%s has too few distinct values to separate the rows, so matching is close to "+
			"a cross product. Join on a more selective key, or narrow a side with WHERE",
			maxJoinPairs, why, side)
	}
}

func isTrue(v classad.Value) bool {
	b, err := v.BoolValue()
	return err == nil && b
}

// --- rendering ---

// renderJoinRows turns the composite rows into the tabular result. Every projected item is
// an expression over the composite ad, so this is the same evaluation the single-table path
// does, with a different ad under it.
func (e *Executor) renderJoinRows(st *Statement, p *joinPlan, rows []*classad.ClassAd) (*Result, error) {
	res := &Result{IsSelect: true, Ads: rows}

	if len(st.Items) == 1 && st.Items[0].Star && st.Items[0].Col == "" {
		res.Columns = joinStarColumns(rows, p)
		res.Star = true
		for _, row := range rows {
			cells := make([]string, len(res.Columns))
			for j, col := range res.Columns {
				cells[j] = valueDisplay(evalPath(row, col))
			}
			res.Rows = append(res.Rows, cells)
		}
		return res, nil
	}

	// Expand any `alias.*` into that side's attributes before compiling, so the column set
	// is fixed for every row.
	items, err := expandJoinStars(st.Items, rows, p)
	if err != nil {
		return nil, err
	}
	compiled := make([]*classad.Expr, len(items))
	for j, it := range items {
		res.Columns = append(res.Columns, it.header())
		src := it.Expr
		if src == "" {
			src = it.Col
		}
		ex, perr := classad.ParseExpr(src)
		if perr != nil {
			return nil, fmt.Errorf("SELECT expression %q: %w", src, perr)
		}
		compiled[j] = ex
	}
	for _, row := range rows {
		cells := make([]string, len(items))
		for j := range items {
			cells[j] = valueDisplay(compiled[j].Eval(row))
		}
		res.Rows = append(res.Rows, cells)
	}
	return res, nil
}

// expandJoinStars replaces each `alias.*` item with one item per attribute that side carries
// in the result, leaving every other item alone.
func expandJoinStars(items []SelectItem, rows []*classad.ClassAd, p *joinPlan) ([]SelectItem, error) {
	out := make([]SelectItem, 0, len(items))
	for _, it := range items {
		if !it.Star {
			out = append(out, it)
			continue
		}
		aliases := p.aliases()
		if it.Col != "" {
			aliases = []string{it.Col}
		}
		for _, alias := range aliases {
			for _, attr := range sideAttrs(rows, alias) {
				q := alias + "." + attr
				out = append(out, SelectItem{Expr: q, Col: q, Alias: q})
			}
		}
	}
	return out, nil
}

// aliases lists the side aliases in FROM-then-JOIN order, which is the order `SELECT *`
// presents their columns in.
func (p *joinPlan) aliases() []string {
	out := make([]string, len(p.sides))
	for i := range p.sides {
		out[i] = p.sides[i].ref.Alias
	}
	return out
}

// joinStarColumns is the column set of `SELECT *` over a join: every attribute of every
// side, qualified by its alias so two sides' like-named attributes stay distinct.
func joinStarColumns(rows []*classad.ClassAd, p *joinPlan) []string {
	var cols []string
	for _, alias := range p.aliases() {
		for _, attr := range sideAttrs(rows, alias) {
			cols = append(cols, alias+"."+attr)
		}
	}
	return cols
}

// sideAttrs lists, in sorted order, every attribute any row carries under one alias.
func sideAttrs(rows []*classad.ClassAd, alias string) []string {
	seen := map[string]bool{}
	var out []string
	for _, row := range rows {
		side := row.EvaluateAttr(alias)
		if !side.IsClassAd() {
			continue
		}
		ad, err := side.ClassAdValue()
		if err != nil || ad == nil {
			continue
		}
		for _, name := range ad.GetAttributes() {
			if k := strings.ToLower(name); !seen[k] {
				seen[k] = true
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out
}

// evalPath evaluates a dotted `alias.Attr` path against a composite row.
func evalPath(row *classad.ClassAd, path string) classad.Value {
	ex, err := classad.ParseExpr(path)
	if err != nil {
		return classad.NewUndefinedValue()
	}
	return ex.Eval(row)
}

// sortJoinAds orders composite rows by the ORDER BY terms, each of which is an expression
// over the row rather than an attribute of it.
func sortJoinAds(rows []*classad.ClassAd, terms []OrderTerm) error {
	compiled := make([]*classad.Expr, len(terms))
	for i, t := range terms {
		if t.Item.IsAggregate() {
			return fmt.Errorf("cannot ORDER BY the aggregate %s in a non-aggregate query", t.Item.header())
		}
		src := t.Item.Expr
		if src == "" {
			src = t.Item.Col
		}
		ex, err := classad.ParseExpr(src)
		if err != nil {
			return fmt.Errorf("ORDER BY %q: %w", src, err)
		}
		compiled[i] = ex
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for k, t := range terms {
			c := compareValues(compiled[k].Eval(rows[i]), compiled[k].Eval(rows[j]))
			if c != 0 {
				if t.Desc {
					return c > 0
				}
				return c < 0
			}
		}
		return false
	})
	return nil
}

// describeJoinPlan reports what the join pushed to each side and how it pairs the rows, so
// EXPLAIN says where the work went rather than only that a join happened.
func (e *Executor) describeJoinPlan(st *Statement) []string {
	p, err := planJoin(st)
	if err != nil {
		return []string{"plan: " + err.Error()}
	}
	side := func(s joinSide) string {
		where := s.where
		if where == "" {
			where = "(none)"
		}
		proj := "whole ads"
		if s.project != nil {
			proj = strings.Join(s.project, ", ")
		}
		return fmt.Sprintf("  %s (%s): constraint %s; attrs %s", s.ref.Alias, s.ref.Table, where, proj)
	}
	out := []string{
		fmt.Sprintf("join of %d tables, left to right: %s", len(p.sides),
			strings.Join(p.aliases(), " -> ")),
		side(p.sides[0]),
	}
	for i := range p.steps {
		step := &p.steps[i]
		s := p.sides[step.side]
		strategy := "nested loop (no equality to hash on)"
		if len(step.equi) > 0 {
			strategy = fmt.Sprintf("hash join on %d equality term(s), build side %s",
				len(step.equi), s.ref.Alias)
		}
		out = append(out,
			fmt.Sprintf("%s %s %s: %s", step.kind, s.ref.Table, s.ref.Alias, strategy),
			side(s),
			fmt.Sprintf("  checked per pair: %d condition term(s)", len(step.cond)))
		if step.batchAttr != "" {
			// The decision needs the keys the rows so far actually hold, which EXPLAIN
			// without ANALYZE has not read, so this reports what is available to batch on
			// and the rule that will be applied to it rather than the outcome.
			out = append(out, fmt.Sprintf("  key batching: %s.%s can be narrowed to the keys "+
				"of the rows so far (at most %d, and only if %s holds at least %d row(s) per key)",
				s.ref.Alias, step.batchAttr, maxJoinKeyBatch, s.ref.Alias, minRightRowsPerKey))
		}
	}
	out = append(out, fmt.Sprintf("after the last join: %d post-filter term(s)", len(p.post)))
	if hasAggregate(st) || len(st.GroupBy) > 0 {
		out = append(out, fmt.Sprintf("  grouping: client-side over the joined rows "+
			"(%d group term(s), %d aggregate(s)); the server aggregate is single-table",
			len(st.GroupBy), len(aggCallOrder(st))))
	}
	if e.serverJoinEligible(st, p) && e.c.SupportsNewer(dbrpc.FeatJoin) {
		out = append(out, "  executed in the store: only the joined rows cross the wire, and one "+
			"side is held rather than both")
	} else {
		out = append(out, "  executed client-side: "+whyNotServerSide(e, st, p))
	}
	return out
}

// whyNotServerSide says which reason a join is being paired here, because "client-side" on
// its own leaves a reader unable to tell a server that cannot join from a statement that
// cannot be sent -- and those have different answers.
func whyNotServerSide(e *Executor, st *Statement, p *joinPlan) string {
	switch {
	case st.AsOf != "":
		return "AS OF reads both sides at one instant, which the join opcode cannot ask for"
	case !p.twoTable():
		return fmt.Sprintf("the join opcode carries two tables and this statement joins %d",
			len(p.sides))
	case p.readsInTx(e):
		return "a read inside a transaction must see that transaction's own writes, and the " +
			"store joins committed state"
	case !e.serverJoin:
		return "the server-side join is not enabled; measured, it moves more data than this " +
			"does whenever the join expands rather than filters"
	case !e.c.SupportsNewer(dbrpc.FeatJoin):
		return "this server does not advertise a server-side join, so no join opcode is sent"
	default:
		return "the plan could not be expressed for the store"
	}
}

// resolveJoinOrderAliases rewrites each ORDER BY term that is a bare name matching a
// projected column's AS alias into that column's expression, so the ordering is by the
// value shown rather than by an attribute of that name. It is idempotent: a term already
// rewritten no longer matches a bare alias.
func resolveJoinOrderAliases(st *Statement) {
	byAlias := map[string]SelectItem{}
	for _, it := range st.Items {
		if it.Alias != "" {
			byAlias[strings.ToLower(it.Alias)] = it
		}
	}
	if len(byAlias) == 0 {
		return
	}
	for i, t := range st.OrderBy {
		if t.Item.Expr != "" || t.Item.Star || t.Item.Col == "" {
			continue
		}
		src, ok := byAlias[strings.ToLower(t.Item.Col)]
		if !ok {
			continue
		}
		st.OrderBy[i].Item.Expr = src.Expr
		if src.Expr == "" {
			st.OrderBy[i].Item.Expr = src.Col
		}
	}
}

// maxJoinPairsForTest lowers the pair budget so a test can exercise the refusal without
// building a fixture big enough to hit the real one, and returns the restore func. The
// budget is a var only for that reason.
func maxJoinPairsForTest(n int) func() {
	prev := maxJoinPairs
	maxJoinPairs = n
	return func() { maxJoinPairs = prev }
}

// aggregateJoinRows groups the joined rows and reduces each group, the client-side
// counterpart of the server's single-table aggregate.
//
// Every group key is an expression over the composite row rather than an attribute of it --
// `GROUP BY j.Owner` reads the Owner of the row's j member -- so each one is compiled once
// and evaluated per row. Past that the machinery is the same as the single-table client-side
// paths use: reduceAggs for the values, the positional projector for HAVING and the column
// order, then ORDER BY and LIMIT over the reduced rows.
func (e *Executor) aggregateJoinRows(st *Statement, rows []*classad.ClassAd) (*Result, error) {
	// Compile each group key once. Parsing per row is what made a grouped CASE query spend
	// most of its time in the ClassAd parser on the single-table path.
	keyExpr := make([]*classad.Expr, len(st.Items))
	for j, it := range st.Items {
		if it.GroupLevel() || it.Star {
			continue
		}
		src := it.Expr
		if src == "" {
			src = it.Col
		}
		ex, err := classad.ParseExpr(src)
		if err != nil {
			return nil, fmt.Errorf("GROUP BY %q: %w", src, err)
		}
		keyExpr[j] = ex
	}

	type group struct {
		vals []string
		rows []*classad.ClassAd
	}
	var order []string
	groups := map[string]*group{}
	for _, row := range rows {
		vals := make([]string, 0, len(st.Items))
		for j, it := range st.Items {
			if it.GroupLevel() || keyExpr[j] == nil {
				continue
			}
			vals = append(vals, valueDisplay(keyExpr[j].Eval(row)))
		}
		key := strings.Join(vals, "\x00")
		g := groups[key]
		if g == nil {
			g = &group{vals: vals}
			groups[key] = g
			order = append(order, key)
		}
		g.rows = append(g.rows, row)
	}

	proj, err := positionalGroupProjector(st)
	if err != nil {
		return nil, err
	}
	res := &Result{IsSelect: true, Columns: proj.columns()}
	for _, key := range order {
		g := groups[key]
		vals := reduceAggs(st, g.rows)
		if !proj.keep(g.vals, vals) {
			continue
		}
		res.Rows = append(res.Rows, proj.row(g.vals, vals))
	}
	if len(st.OrderBy) > 0 {
		if err := sortRows(res, st.OrderBy); err != nil {
			return nil, err
		}
	}
	if st.Limit > 0 && len(res.Rows) > st.Limit {
		res.Rows = res.Rows[:st.Limit]
	}
	return res, nil
}

// joinExprRefs returns the attribute references of an expression over a joined row, each as
// the whole dotted path it was written as.
//
// It exists because vm's ReadAttrs reports the leaf of a select -- `h.Owner` comes back as
// "h" -- which is the right answer for a read plan and the wrong one for asking whether a
// reference is a grouped column. An unqualified reference is returned as written, so a scope
// check still catches it.
func joinExprRefs(src string) []string {
	if strings.TrimSpace(src) == "" {
		return nil
	}
	q, err := vm.Parse(src)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	add := func(path string) {
		if k := strings.ToLower(path); path != "" && !seen[k] {
			seen[k] = true
			out = append(out, path)
		}
	}
	walkAST(q.Expr(), func(n ast.Expr) bool {
		switch v := n.(type) {
		case *ast.SelectExpr:
			if base, ok := unwrapParens(v.Record).(*ast.AttributeReference); ok && base.Scope == ast.NoScope {
				add(base.Name + "." + v.Attr)
				return false
			}
		case *ast.AttributeReference:
			if v.Scope == ast.NoScope {
				add(v.Name)
			}
		}
		return true
	})
	return out
}

// joinServerSide asks the store to do the pairing, returning the joined rows.
//
// ok is false when the join has to happen here instead, which is not a failure: an older
// server, a server holding one database rather than a catalog, or a statement whose shape
// the opcode does not carry. The client-side path is then taken exactly as before.
//
// The plan is already made. What crosses is that plan -- each side's pushed-down constraint
// and projection, the residual condition and filter as ClassAd expression text, and the
// equality halves that drive the hash -- so the server executes a decision this package
// made rather than making its own.
func (e *Executor) joinServerSide(st *Statement, p *joinPlan) ([]*classad.ClassAd, bool, error) {
	if !e.serverJoinEligible(st, p) {
		return nil, false, nil
	}
	left, right, step := p.sides[0], p.sides[1], &p.steps[0]
	req := dbrpc.JoinRequest{Spec: db.JoinSpec{
		Kind:  serverJoinKind(step.kind),
		Left:  db.JoinSide{Table: left.ref.Table, Alias: left.ref.Alias, Constraint: left.where, Project: left.project},
		Right: db.JoinSide{Table: right.ref.Table, Alias: right.ref.Alias, Constraint: right.where, Project: right.project},
		Cond:  step.condSrc,
		Post:  p.postSrc,
	}}
	for _, eq := range step.equiSrc {
		req.Spec.Equi = append(req.Spec.Equi, db.JoinEqui{Left: eq.accStripped, Right: eq.side})
	}
	// The LIMIT only pushes down when nothing here reorders or removes rows afterwards.
	// Sorting, DISTINCT and grouping all decide WHICH rows the limit keeps, so a limit
	// applied before them would keep the wrong ones.
	if st.Limit > 0 && len(st.OrderBy) == 0 && !st.Distinct && !hasAggregate(st) && len(st.GroupBy) == 0 {
		req.Spec.Limit = st.Limit
	}

	var rows []*classad.ClassAd
	err := e.c.Join(e.opCtx(), req, func(row string) bool {
		ad, perr := classad.Parse(row)
		if perr != nil {
			return false
		}
		rows = append(rows, ad)
		return true
	})
	if errors.Is(err, dbrpc.ErrJoinUnsupported) {
		return nil, false, nil // join it here, as before
	}
	if err != nil {
		// Including a limit the store met part way through, which arrives after the rows:
		// a partial join must not be rendered as a complete one.
		return nil, false, err
	}
	return rows, true, nil
}

// serverJoinEligible reports whether this statement's join can run in the store.
//
// Each refusal is a shape the opcode does not carry, not a performance judgement. A
// point-in-time join would have to read both sides at one instant, which the op has no way
// to ask for. A read inside a transaction must see that transaction's own uncommitted
// writes, and the store joins committed state. And a plan whose predicates did not survive
// being rendered to text cannot be sent at all.
func (e *Executor) serverJoinEligible(st *Statement, p *joinPlan) bool {
	if !e.serverJoin {
		return false
	}
	if st.AsOf != "" {
		return false
	}
	// The opcode carries exactly two sides. A longer chain is paired here; the store
	// gaining an n-way join would be a protocol change, not a planner one.
	if !p.twoTable() {
		return false
	}
	if p.readsInTx(e) {
		return false
	}
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

// readsInTx reports whether any side is a table the open transaction has written, whose
// uncommitted writes a read must see and the store's join does not.
func (p *joinPlan) readsInTx(e *Executor) bool {
	for i := range p.sides {
		if e.txReads(p.sides[i].ref.Table) {
			return true
		}
	}
	return false
}

func serverJoinKind(k JoinKind) db.JoinKind {
	switch k {
	case JoinLeft:
		return db.JoinLeft
	case JoinCross:
		return db.JoinCross
	default:
		return db.JoinInner
	}
}
