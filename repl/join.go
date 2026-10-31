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
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
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
	maxJoinOutputRows = 1_000_000
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

// joinPlan is a planned two-table join.
type joinPlan struct {
	kind  JoinKind
	left  joinSide
	right joinSide

	// cond are the ON conjuncts that could not be pushed to one side -- the join condition
	// proper -- compiled over the composite ad. A pair matches when every one is true.
	cond []*classad.Expr

	// post are the WHERE conjuncts that could not be pushed down, compiled over the
	// composite ad and applied after the pair is formed (and, for a LEFT JOIN, after an
	// unmatched left row has been null-extended).
	post []*classad.Expr

	// equi are equality conjuncts of cond whose two halves each read only one side. They
	// drive the hash build/probe. They NARROW candidates only: every surviving pair is
	// still checked against cond in full, so the hash never has to reproduce ClassAd's
	// equality semantics exactly.
	equi []equiPair

	// batchAttr is the right side's bare attribute in a single equi pair, if there is one;
	// it is what key batching builds a membership constraint on.
	batchAttr string
}

// equiPair is one `<left expr> == <right expr>` conjunct, each half stripped of its alias
// so it evaluates against that side's own ad.
type equiPair struct {
	left, right *classad.Expr
}

// --- planning ---

// planJoin resolves the statement's aliases, splits its predicates, and compiles what is
// left into a plan.
func planJoin(st *Statement) (*joinPlan, error) {
	if len(st.Joins) != 1 {
		return nil, fmt.Errorf("a join reads exactly two tables")
	}
	jc := st.Joins[0]
	left := TableRef{Table: st.From[0].Table, Alias: st.From[0].Alias}
	right := TableRef{Table: jc.Table, Alias: jc.Alias}
	aliases := map[string]int{
		strings.ToLower(left.Alias):  0,
		strings.ToLower(right.Alias): 1,
	}

	// ORDER BY may name a projected alias rather than an attribute -- `SELECT j.Cpus * 2 AS
	// twice ... ORDER BY twice`. Resolve those to the expression they stand for before
	// validation, which would otherwise read the alias as an unqualified attribute.
	resolveJoinOrderAliases(st)

	if err := validateJoinClauses(st, aliases, left, right); err != nil {
		return nil, err
	}

	p := &joinPlan{kind: jc.Kind, left: joinSide{ref: left}, right: joinSide{ref: right}}

	// ON. For an INNER (or CROSS) join a single-side conjunct is a filter on that side and
	// pushes down. For a LEFT JOIN only the RIGHT side's conjuncts push: a left row that
	// fails the condition still appears, null-extended, so filtering the left scan with a
	// condition conjunct would drop rows the join must emit.
	onParts, err := conjunctsOf(jc.On)
	if err != nil {
		return nil, fmt.Errorf("JOIN ON: %w", err)
	}
	for _, c := range onParts {
		switch sidesOf(c, aliases) {
		case sideLeft:
			if p.kind == JoinLeft {
				p.cond = append(p.cond, mustCompile(c))
				continue
			}
			p.left.where = andText(p.left.where, stripSide(c, aliases, 0))
		case sideRight:
			p.right.where = andText(p.right.where, stripSide(c, aliases, 1))
		default:
			p.cond = append(p.cond, mustCompile(c))
		}
	}

	// WHERE. The mirror image: a WHERE conjunct on the PRESERVED (left) side of a LEFT JOIN
	// pushes down, while one on the right side must wait until after null-extension -- there
	// it correctly turns the outer join into an inner one only if that is what was written.
	whereParts, err := conjunctsOf(st.Where)
	if err != nil {
		return nil, fmt.Errorf("WHERE: %w", err)
	}
	for _, c := range whereParts {
		switch sidesOf(c, aliases) {
		case sideLeft:
			p.left.where = andText(p.left.where, stripSide(c, aliases, 0))
		case sideRight:
			if p.kind == JoinLeft {
				p.post = append(p.post, mustCompile(c))
				continue
			}
			p.right.where = andText(p.right.where, stripSide(c, aliases, 1))
		default:
			p.post = append(p.post, mustCompile(c))
		}
	}

	p.extractEqui(jc.On, aliases)
	p.left.project, p.right.project = joinProjection(st, aliases)
	return p, nil
}

// extractEqui finds the ON conjuncts of the form `<left-only> == <right-only>` and compiles
// each half against its own side. Used to hash rather than to decide: a pair the hash brings
// together is still checked against the whole condition.
func (p *joinPlan) extractEqui(on string, aliases map[string]int) {
	parts, err := conjunctsOf(on)
	if err != nil {
		return
	}
	for _, c := range parts {
		bo, ok := unwrapParens(c).(*ast.BinaryOp)
		if !ok || bo.Op != "==" {
			continue
		}
		l, r := sidesOf(bo.Left, aliases), sidesOf(bo.Right, aliases)
		var leftHalf, rightHalf ast.Expr
		switch {
		case l == sideLeft && r == sideRight:
			leftHalf, rightHalf = bo.Left, bo.Right
		case l == sideRight && r == sideLeft:
			leftHalf, rightHalf = bo.Right, bo.Left
		default:
			continue
		}
		le, lerr := classad.ParseExpr(stripSide(leftHalf, aliases, 0))
		re, rerr := classad.ParseExpr(stripSide(rightHalf, aliases, 1))
		if lerr != nil || rerr != nil {
			continue
		}
		p.equi = append(p.equi, equiPair{left: le, right: re})
		// Key batching needs a bare attribute on the right so the membership constraint
		// can name it. Anything computed is still hashable, just not batchable.
		if p.batchAttr == "" {
			if ar, ok := unwrapParens(rightHalf).(*ast.SelectExpr); ok {
				if base, attr, ok := qualifiedRef(ar, aliases); ok && base == 1 {
					p.batchAttr = attr
				}
			}
		}
	}
}

// joinProjection returns the attributes each side must fetch, or nil for that side when the
// statement reads attributes this cannot enumerate (a `*`, or an expression that does not
// parse) and the whole ad is needed.
func joinProjection(st *Statement, aliases map[string]int) (left, right []string) {
	seen := [2]map[string]bool{{}, {}}
	out := [2][]string{}
	whole := [2]bool{}
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
			whole[0], whole[1] = true, true
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
			whole[0], whole[1] = true, true
		case it.Star:
			if side, ok := aliases[strings.ToLower(it.Col)]; ok {
				whole[side] = true
			}
		case it.Expr != "":
			scan(it.Expr)
		default:
			// A bare column in a join is refused during validation, so reaching here
			// means an attribute set this cannot reason about; fetch everything.
			whole[0], whole[1] = true, true
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
			whole[0], whole[1] = true, true
		}
	}
	if whole[0] {
		out[0] = nil
	}
	if whole[1] {
		out[1] = nil
	}
	return out[0], out[1]
}

// --- reference resolution ---

const (
	sideNone  = 0 // no reference to either table (a constant, or CurrentTime)
	sideLeft  = 1
	sideRight = 2
	sideBoth  = 3
)

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
			switch v.Scope {
			case ast.MyScope:
				mask |= sideLeft
			case ast.TargetScope:
				mask |= sideRight
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
func validateJoinClauses(st *Statement, aliases map[string]int, left, right TableRef) error {
	check := func(what, src string) error {
		return validateJoinExpr(what, src, aliases, left, right)
	}
	for _, j := range st.Joins {
		if err := check("JOIN ON", j.On); err != nil {
			return err
		}
	}
	if err := check("WHERE", st.Where); err != nil {
		return err
	}
	for _, it := range st.Items {
		if it.Star {
			continue
		}
		if it.Expr == "" {
			return unqualifiedErr("SELECT", it.Col, left, right)
		}
		if err := check("SELECT", it.Expr); err != nil {
			return err
		}
	}
	for _, t := range st.OrderBy {
		if t.Item.Star {
			continue
		}
		if t.Item.Expr == "" {
			return unqualifiedErr("ORDER BY", t.Item.Col, left, right)
		}
		if err := check("ORDER BY", t.Item.Expr); err != nil {
			return err
		}
	}
	return nil
}

func validateJoinExpr(what, src string, aliases map[string]int, left, right TableRef) error {
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
			if v.Scope == ast.NoScope && !magicUnscoped[strings.ToLower(v.Name)] {
				bad = unqualifiedErr(what, v.Name, left, right)
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

func unqualifiedErr(what, name string, left, right TableRef) error {
	return fmt.Errorf("%s: %q is not qualified -- in a join every attribute must say which "+
		"table it comes from, as %s.%s or %s.%s", what, name, left.Alias, name, right.Alias, name)
}

// --- execution ---

// execJoin runs a two-table join entirely in this process, over query ops that predate the
// feature. See the file comment for the strategy.
func (e *Executor) execJoin(st *Statement) (*Result, error) {
	if err := refuseUnsupportedJoin(st); err != nil {
		return nil, err
	}
	p, err := planJoin(st)
	if err != nil {
		return nil, err
	}

	leftAds, err := e.fetchJoinSide(st, p.left, 0)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p.left.ref.Table, err)
	}
	// Key batching: with the left side in hand and an equality on a bare right-side
	// attribute, the right scan can be narrowed to the values the left side actually has,
	// instead of reading the whole table and discarding most of it here.
	right := p.right
	if batch := p.keyBatchConstraint(leftAds); batch != "" {
		right.where = andText(right.where, batch)
	}
	rightAds, err := e.fetchJoinSide(st, right, 1)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p.right.ref.Table, err)
	}

	rows, err := p.join(leftAds, rightAds)
	if err != nil {
		return nil, err
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
	if hasAggregate(st) || len(st.GroupBy) > 0 {
		return fmt.Errorf("aggregates and GROUP BY over a join are not supported yet; " +
			"the aggregate argument grammar takes a bare attribute name, which cannot say " +
			"which table it comes from")
	}
	if hasWindow(st) {
		return fmt.Errorf("window functions over a join are not supported yet")
	}
	if st.Having != "" {
		return fmt.Errorf("HAVING needs GROUP BY, which a join does not support yet")
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

// keyBatchConstraint folds the left side's key values into one membership constraint on the
// right side's attribute. It returns "" when there is no single bare-attribute equality, when
// a left row's key is not a usable literal, or when there are more distinct values than the
// constraint should carry -- in each case the right side is simply scanned as planned.
func (p *joinPlan) keyBatchConstraint(leftAds []*classad.ClassAd) string {
	if p.batchAttr == "" || len(p.equi) != 1 || len(leftAds) == 0 {
		return ""
	}
	seen := map[string]bool{}
	var lits []string
	for _, ad := range leftAds {
		lit, ok := literalOf(p.equi[0].left.Eval(ad))
		if !ok {
			// A left row with no usable key value matches nothing through this equality,
			// so it contributes no constraint term -- but it also cannot be allowed to
			// narrow the right side on its behalf, which it does not.
			continue
		}
		if seen[lit] {
			continue
		}
		seen[lit] = true
		if len(lits) >= maxJoinKeyBatch {
			return "" // too many distinct keys: a plain scan is cheaper than the constraint
		}
		lits = append(lits, lit)
	}
	if len(lits) == 0 {
		return ""
	}
	sort.Strings(lits) // a stable constraint, so the same query explains the same way
	terms := make([]string, len(lits))
	for i, l := range lits {
		terms[i] = p.batchAttr + " == " + l
	}
	return "(" + strings.Join(terms, " || ") + ")"
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

// join pairs the two sides and returns the composite rows, in left-side order.
func (p *joinPlan) join(leftAds, rightAds []*classad.ClassAd) ([]*classad.ClassAd, error) {
	var out []*classad.ClassAd
	pairs := 0
	emit := func(l, r *classad.ClassAd) bool {
		row := p.composite(l, r)
		for _, ex := range p.post {
			if ex == nil || !isTrue(ex.Eval(row)) {
				return false
			}
		}
		out = append(out, row)
		return true
	}

	// Hash the right side on the equality keys when there are any; otherwise every left row
	// is paired against every right row and decided by the condition alone.
	var buckets map[string][]*classad.ClassAd
	if len(p.equi) > 0 {
		buckets = make(map[string][]*classad.ClassAd, len(rightAds))
		for _, r := range rightAds {
			k, ok := p.hashKey(r, false)
			if !ok {
				continue // an undefined key equals nothing: ClassAd == propagates undefined
			}
			buckets[k] = append(buckets[k], r)
		}
	}

	for _, l := range leftAds {
		matched := false
		candidates := rightAds
		if buckets != nil {
			k, ok := p.hashKey(l, true)
			if !ok {
				candidates = nil // an undefined key on the left matches nothing
			} else {
				candidates = buckets[k]
			}
		}
		for _, r := range candidates {
			pairs++
			if pairs > maxJoinPairs {
				return nil, p.tooManyPairs(len(leftAds), len(rightAds))
			}
			// The hash only narrowed the candidates; the condition decides, in full, with
			// ClassAd's own semantics.
			if !p.conditionHolds(l, r) {
				continue
			}
			if emit(l, r) {
				matched = true
			}
			if len(out) > maxJoinOutputRows {
				return nil, fmt.Errorf("the join produced more than %d rows; "+
					"narrow it with WHERE (LIMIT does not help -- the rows are built "+
					"before they are limited)", maxJoinOutputRows)
			}
		}
		if !matched && p.kind == JoinLeft {
			emit(l, nil) // null-extended: every right-side reference reads as undefined
		}
	}
	return out, nil
}

// conditionHolds evaluates the join condition over the pair.
func (p *joinPlan) conditionHolds(l, r *classad.ClassAd) bool {
	if len(p.cond) == 0 {
		return true
	}
	row := p.composite(l, r)
	for _, ex := range p.cond {
		if ex == nil || !isTrue(ex.Eval(row)) {
			return false
		}
	}
	return true
}

// hashKey renders one ad's equality-key values into a bucket key.
//
// It must never put two ClassAd-equal values in different buckets, or a matching pair would
// never be considered. So numbers of every kind (int, real, bool) normalize to one float
// form -- ClassAd's == coerces across them -- and strings fold to lower case, because
// ClassAd compares strings case-insensitively. Collisions in the other direction cost
// nothing: a bucket is only a candidate list, and the condition re-checks every pair.
func (p *joinPlan) hashKey(ad *classad.ClassAd, left bool) (string, bool) {
	var b strings.Builder
	for i, pair := range p.equi {
		ex := pair.right
		if left {
			ex = pair.left
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
		if i < len(p.equi)-1 {
			b.WriteByte(0)
		}
	}
	return b.String(), true
}

// composite builds the joined row: one ClassAd whose members are the two sides under their
// aliases. A nil right side (an unmatched LEFT JOIN row) is simply absent, so every
// reference through its alias evaluates to undefined.
func (p *joinPlan) composite(l, r *classad.ClassAd) *classad.ClassAd {
	row := classad.New()
	if l != nil {
		row.InsertAttrClassAd(p.left.ref.Alias, l)
	}
	if r != nil {
		row.InsertAttrClassAd(p.right.ref.Alias, r)
	}
	return row
}

// tooManyPairs explains a budget refusal in terms of what the writer can change: whether
// the condition gave the join anything to key on, and how big each side was.
func (p *joinPlan) tooManyPairs(nleft, nright int) error {
	why := fmt.Sprintf("%s has %d rows and %s has %d", p.left.ref.Alias, nleft, p.right.ref.Alias, nright)
	switch {
	case len(p.equi) == 0:
		return fmt.Errorf("the join would examine more than %d pairs (%s): its ON clause has "+
			"no equality between the two tables, so every row is compared with every row. "+
			"Add an equality term, or narrow a side with WHERE", maxJoinPairs, why)
	default:
		return fmt.Errorf("the join would examine more than %d pairs (%s): the join key has "+
			"too few distinct values to separate the rows, so matching is close to a cross "+
			"product. Join on a more selective key, or narrow a side with WHERE",
			maxJoinPairs, why)
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
		aliases := []string{p.left.ref.Alias, p.right.ref.Alias}
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

// joinStarColumns is the column set of `SELECT *` over a join: every attribute of either
// side, qualified by its alias so the two sides' like-named attributes stay distinct.
func joinStarColumns(rows []*classad.ClassAd, p *joinPlan) []string {
	var cols []string
	for _, alias := range []string{p.left.ref.Alias, p.right.ref.Alias} {
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
	strategy := "nested loop (no equality to hash on)"
	if len(p.equi) > 0 {
		strategy = fmt.Sprintf("hash join on %d equality term(s), build side %s",
			len(p.equi), p.right.ref.Alias)
	}
	out := []string{
		fmt.Sprintf("%s: %s %s -> %s %s", p.kind,
			p.left.ref.Table, p.left.ref.Alias, p.right.ref.Table, p.right.ref.Alias),
		"  pairing: " + strategy,
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
	out = append(out, side(p.left), side(p.right))
	if p.batchAttr != "" {
		out = append(out, fmt.Sprintf("  key batching: %s.%s narrowed to the left side's keys (max %d)",
			p.right.ref.Alias, p.batchAttr, maxJoinKeyBatch))
	}
	out = append(out, fmt.Sprintf("  checked per pair: %d condition term(s), %d post-filter term(s)",
		len(p.cond), len(p.post)))
	out = append(out, "  executed client-side: no join opcode is sent, so this runs against any server")
	return out
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
