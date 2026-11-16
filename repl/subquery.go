package repl

// SUBQUERY PREDICATES, AND WHY THEY ARE READ ONE CONJUNCT AT A TIME.
//
// `EXISTS (SELECT ...)`, `x IN (SELECT ...)` and `x IN (a, b)` all share one problem with
// BETWEEN, and it is the reason translateSQL refuses IN rather than translating it: ClassAd
// spells membership as a FUNCTION, `member(x, {a, b})`, which takes the left operand as an
// argument -- and by the time a left-to-right translator sees the IN keyword, that operand
// has already been written through to the output verbatim, where it can no longer be moved
// inside a call.
//
// So the WHERE clause is read the way a join's ON clause is read: one top-level conjunct at
// a time, with the capture stopping AT the keyword rather than running past it. The left
// operand is then a value this code holds, and the predicate can be built around it.
//
// WHAT "TOP-LEVEL CONJUNCT" HAS TO MEAN, exactly. The pieces are rejoined with `&&` and the
// text that produces is correct whatever they contain, because ClassAd's `&&` binds tighter
// than `||` exactly as SQL's AND binds tighter than OR -- `a OR b AND c` captures as the two
// pieces `a OR b` and `c`, rejoins as `a || b && c`, and parses back as `a || (b && c)`,
// which is what was written.
//
// A LIFTED predicate is different, and this is the one subtlety here. Pulling a conjunct out
// of the clause and applying it as a separate filter is sound only if it really is a conjunct
// of the whole: with the pieces `a OR EXISTS (...)` and `c`, the clause means
// `a || (EXISTS && c)`, and lifting EXISTS out would answer `(a || c) && EXISTS` instead.
// So a clause that lifts anything must be a pure AND-spine, and one that is not is refused
// rather than silently answered as a different query. orFree below is that check.

import (
	"fmt"
	"strings"
)

// SubqueryKind is the question a subquery predicate asks.
type SubqueryKind int

const (
	// SubqueryExists is `EXISTS (SELECT ...)`: keep the row when the inner query matches
	// anything. It is a semi join.
	SubqueryExists SubqueryKind = iota
	// SubqueryNotExists is `NOT EXISTS (...)`: keep the row when it matches nothing. It is
	// an anti join.
	SubqueryNotExists
	// SubqueryIn is `<expr> IN (SELECT ...)`, which is SubqueryExists with one more
	// equality: the inner query's single column against Expr.
	SubqueryIn
	// SubqueryNotIn is `<expr> NOT IN (SELECT ...)`, the anti join of the same.
	SubqueryNotIn
)

func (k SubqueryKind) String() string {
	switch k {
	case SubqueryNotExists:
		return "NOT EXISTS"
	case SubqueryIn:
		return "IN"
	case SubqueryNotIn:
		return "NOT IN"
	default:
		return "EXISTS"
	}
}

// negated reports whether this kind keeps the rows that DO NOT match -- an anti join.
func (k SubqueryKind) negated() bool {
	return k == SubqueryNotExists || k == SubqueryNotIn
}

// Subquery is one subquery predicate lifted out of a WHERE clause.
type Subquery struct {
	Kind SubqueryKind

	// Expr is the left operand of an IN / NOT IN, as a ClassAd expression in the OUTER
	// query's scope. Empty for EXISTS / NOT EXISTS, which have no left operand.
	Expr string

	// Raw is the predicate as it was written, for error messages and EXPLAIN.
	Raw string

	// Inner is the subquery's own SELECT.
	Inner *Statement
}

// atSubqueryClose reports whether the parser sits at the `)` that closes the subquery being
// parsed. It is folded into the stop predicates of every clause an inner SELECT can contain,
// because those were all written for a statement that ends at the end of the input: without
// it an inner `WHERE Owner == "alice")` would consume the closing parenthesis as part of the
// expression and then fail at the end of the statement, pointing nowhere near the mistake.
func (p *parser) atSubqueryClose() bool {
	return p.inSubquery > 0 && p.atPunct(")")
}

// parseSubquerySelect consumes `( SELECT ... )` and returns the inner statement.
//
// The parser is re-entrant over one token stream, so the inner SELECT is parsed by the same
// function the outer one was; inSubquery is what tells the clause terminators that this
// statement ends at a parenthesis rather than at the end of the input.
func (p *parser) parseSubquerySelect(what string) (*Statement, error) {
	if !p.atPunct("(") {
		return nil, fmt.Errorf("%s takes a parenthesized subquery, e.g. "+
			"`%s (SELECT 1 FROM <table> WHERE ...)`", what, what)
	}
	p.pos++ // (
	if !p.takeKeyword("SELECT") {
		return nil, fmt.Errorf("%s takes a SELECT, and this parenthesis does not open one", what)
	}
	p.inSubquery++
	inner, err := p.parseSelect()
	p.inSubquery--
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	if !p.atPunct(")") {
		return nil, fmt.Errorf("%s: the subquery is missing its closing parenthesis", what)
	}
	p.pos++ // )
	return inner, nil
}

// parseInList consumes the `(...)` after an IN that is NOT a subquery: a list of value
// expressions. It returns the ClassAd list literal text, `{a, b, c}`.
func (p *parser) parseInList() (string, error) {
	p.pos++ // (
	var elems []string
	for {
		if p.atPunct(")") && len(elems) == 0 {
			return "", fmt.Errorf("IN takes at least one value; `IN ()` matches nothing, " +
				"which is better written as a predicate that says so")
		}
		e, _, err := p.captureExpr(func() bool {
			return p.atEnd() || p.atPunct(",") || p.atPunct(")")
		}, nil)
		if err != nil {
			return "", fmt.Errorf("IN: %w", err)
		}
		elems = append(elems, e)
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	if !p.atPunct(")") {
		return "", fmt.Errorf("IN: the value list is missing its closing parenthesis")
	}
	p.pos++ // )
	return "{" + strings.Join(elems, ", ") + "}", nil
}

// whereClause is a WHERE clause read conjunct by conjunct: the conjuncts that stay in the
// predicate, the subquery predicates lifted out of it, and whether any conjunct contained a
// top-level OR (see the file comment).
type whereClause struct {
	conj   []string
	raw    []string
	subs   []Subquery
	hadOr  bool
	rawAll string
}

// expr is the residual predicate: the conjuncts that were not lifted, as one ClassAd
// expression.
func (w *whereClause) expr() string { return strings.Join(w.conj, " && ") }

// rawExpr is the residual predicate as the user typed it, for the dialect lowering. Only the
// conjuncts that stayed are included: a dialect is asked to lower what it can see, and a
// lifted subquery is not part of the predicate any more.
func (w *whereClause) rawExpr() string { return strings.Join(w.raw, " AND ") }

// atWhereConjunctEnd reports whether the parser sits at the end of one top-level conjunct of
// a WHERE clause: where the clause itself ends, at the AND that separates it from the next,
// or at the IN whose left operand it is.
//
// IN appears here for the reason BETWEEN appears in atJoinConjunctEnd: the capture has to
// stop before the keyword so that what it captured can become the call's first argument.
func (p *parser) atWhereConjunctEnd(end func() bool) func() bool {
	return func() bool {
		return end() || p.isKeyword("AND") || p.isKeyword("IN") || p.isKeyword("EXISTS") ||
			(p.isKeyword("NOT") && (p.peekAheadKeyword(1, "IN") ||
				p.peekAheadKeyword(1, "EXISTS")))
	}
}

// parseWhereClause reads a WHERE clause one top-level conjunct at a time, lifting out the
// subquery predicates and translating the IN forms.
//
// end reports where the whole clause stops (the next clause keyword, the end of the
// statement, a set operator, or the parenthesis closing an enclosing subquery).
func (p *parser) parseWhereClause(end func() bool) (*whereClause, error) {
	w := &whereClause{}
	start := p.peek().pos
	last := start
	stop := p.atWhereConjunctEnd(end)
	for {
		piece, err := p.parseWhereConjunct(stop)
		if err != nil {
			return nil, err
		}
		if piece.sub != nil {
			w.subs = append(w.subs, *piece.sub)
		} else {
			w.conj = append(w.conj, piece.expr)
			w.raw = append(w.raw, piece.raw)
		}
		w.hadOr = w.hadOr || piece.hadOr
		if p.pos > 0 {
			last = p.toks[p.pos-1].end
		}
		if !p.takeKeyword("AND") {
			break
		}
	}
	w.rawAll = strings.TrimSpace(p.src[start:last])
	if len(w.subs) > 0 && p.isKeyword("OR") {
		w.hadOr = true
	}
	// Lifting a conjunct out of a clause that is not a pure AND-spine would answer a
	// different query; see the file comment.
	if len(w.subs) > 0 && w.hadOr {
		return nil, fmt.Errorf("%s inside an OR is not supported: it filters rows, so it has "+
			"to be a whole conjunct of WHERE. Rewrite the clause so the subquery is joined "+
			"to the rest with AND", w.subs[0].Kind)
	}
	return w, nil
}

// wherePiece is one conjunct: either an expression that stays in the predicate, or a
// subquery predicate lifted out of it.
type wherePiece struct {
	expr  string
	raw   string
	sub   *Subquery
	hadOr bool
}

// parseWhereConjunct reads one top-level conjunct.
func (p *parser) parseWhereConjunct(stop func() bool) (wherePiece, error) {
	start := p.peek().pos

	// EXISTS and NOT EXISTS take no left operand, so they are recognised before anything is
	// captured. NOT EXISTS is read here rather than left to translateSQL's NOT, which would
	// emit a bare `!` with nothing to apply it to.
	neg := false
	save := p.pos
	if p.isKeyword("NOT") && p.peekAheadKeyword(1, "EXISTS") {
		p.pos++
		neg = true
	}
	if p.isKeyword("EXISTS") {
		p.pos++
		kind := SubqueryExists
		if neg {
			kind = SubqueryNotExists
		}
		inner, err := p.parseSubquerySelect(kind.String())
		if err != nil {
			return wherePiece{}, err
		}
		raw := strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])
		return wherePiece{sub: &Subquery{Kind: kind, Raw: raw, Inner: inner}}, nil
	}
	p.pos = save

	expr, raw, err := p.captureExpr(stop, nil)
	if err != nil {
		return wherePiece{}, err
	}
	// The capture stops at EXISTS, so one sitting here was written inside a larger
	// expression -- `a == 1 OR EXISTS (...)`, or an EXISTS used as a value. Captured as
	// ordinary text it would reach the ClassAd parser as an unknown identifier applied to
	// a parenthesis, which says nothing about what is wrong.
	if err := p.refuseInlineExists(); err != nil {
		return wherePiece{}, err
	}
	hadOr := hasTopLevelOr(expr)

	negIn := p.isKeyword("NOT") && p.peekAheadKeyword(1, "IN")
	if !negIn && !p.isKeyword("IN") {
		return wherePiece{expr: expr, raw: raw, hadOr: hadOr}, nil
	}
	if err := refuseLooseOperand("IN", raw, expr); err != nil {
		return wherePiece{}, err
	}
	if negIn {
		p.pos++ // NOT
	}
	p.pos++ // IN
	if !p.atPunct("(") {
		return wherePiece{}, fmt.Errorf("IN takes a parenthesized list or subquery, e.g. "+
			"`%s IN (1, 2)` or `%s IN (SELECT ...)`", raw, raw)
	}
	// A subquery or a value list: whichever, the left operand is in hand now.
	if p.peekAheadKeyword(1, "SELECT") {
		kind := SubqueryIn
		if negIn {
			kind = SubqueryNotIn
		}
		inner, serr := p.parseSubquerySelect(kind.String())
		if serr != nil {
			return wherePiece{}, serr
		}
		full := strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])
		return wherePiece{sub: &Subquery{Kind: kind, Expr: expr, Raw: full, Inner: inner}}, nil
	}
	list, lerr := p.parseInList()
	if lerr != nil {
		return wherePiece{}, lerr
	}
	out := fmt.Sprintf("member(%s, %s)", expr, list)
	if negIn {
		out = "!" + out
	}
	full := strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])
	return wherePiece{expr: out, raw: full, hadOr: false}, nil
}

// refuseInlineExists reports the error for an EXISTS that is not a whole conjunct of WHERE.
func (p *parser) refuseInlineExists() error {
	kind := SubqueryExists
	switch {
	case p.isKeyword("EXISTS"):
	case p.isKeyword("NOT") && p.peekAheadKeyword(1, "EXISTS"):
		kind = SubqueryNotExists
	default:
		return nil
	}
	return fmt.Errorf("%s filters rows, so it has to be a whole conjunct of WHERE joined to "+
		"the rest with AND; it cannot appear inside a larger expression", kind)
}

// hasTopLevelOr reports whether a captured conjunct contains a `||` outside any parentheses.
//
// It reads the TRANSLATED text rather than the source, so SQL's OR and ClassAd's `||` are
// one spelling by the time it runs. Strings are skipped so that `Owner == "a||b"` is not
// read as a disjunction.
func hasTopLevelOr(expr string) bool {
	depth := 0
	for i := 0; i < len(expr); i++ {
		switch c := expr[i]; c {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case '"':
			for i++; i < len(expr); i++ {
				if expr[i] == '\\' {
					i++
					continue
				}
				if expr[i] == '"' {
					break
				}
			}
		case '|':
			if depth == 0 && i+1 < len(expr) && expr[i+1] == '|' {
				return true
			}
		}
	}
	return false
}

// THE LEFT OPERAND OF A POSTFIX KEYWORD, AND WHY IT HAS TO BE CHECKED.
//
// IN and BETWEEN are both read by stopping the capture AT the keyword and taking what was
// captured as the operand. That is sound only while the capture ends where the operand
// does -- and it does not, because the capture runs to the end of a top-level CONJUNCT while
// both keywords bind tighter than every comparison and than OR.
//
// `a == 1 OR Owner IN ('a')` is the shape that shows it. SQL reads it as
// `(a == 1) OR (Owner IN ('a'))`; the capture hands back `a == 1 || Owner`, and building
// `member(a == 1 || Owner, {"a"})` around that answers a question nobody asked, with no
// symptom. The same text through BETWEEN becomes a window over `a.x == 1 || b.t`.
//
// So an operand carrying a top-level operator that binds LOOSER than the keyword is refused.
// Parenthesizing is not offered as a fix for the OR case because parentheses would not change
// what was meant -- the operand really is just the last term -- so what the message asks for
// is the spelling that says so.

// looserThanPostfix are the operators that bind looser than IN and BETWEEN, in the ClassAd
// spelling the capture has already translated to. Longest first, so that `==` is not read as
// two `=` and `<=` is not read as `<`.
var looserThanPostfix = []string{"=?=", "=!=", "==", "!=", "<=", ">=", "||", "&&", "<", ">", "?"}

// topLevelLooserOp returns the first operator in expr that binds looser than a postfix
// keyword and sits outside every parenthesis, bracket, brace and string; "" when there is
// none.
func topLevelLooserOp(expr string) string {
	depth := 0
	for i := 0; i < len(expr); i++ {
		switch c := expr[i]; c {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			depth--
			continue
		case '"':
			for i++; i < len(expr); i++ {
				if expr[i] == '\\' {
					i++
					continue
				}
				if expr[i] == '"' {
					break
				}
			}
			continue
		}
		if depth != 0 {
			continue
		}
		for _, op := range looserThanPostfix {
			if strings.HasPrefix(expr[i:], op) {
				return op
			}
		}
	}
	return ""
}

// refuseLooseOperand reports the error for a postfix keyword whose captured left operand
// reaches further left than the keyword binds.
func refuseLooseOperand(kw, raw, expr string) error {
	op := topLevelLooserOp(expr)
	if op == "" {
		return nil
	}
	return fmt.Errorf("%s binds tighter than %s, so `%s` is not its left operand: the operand "+
		"is the single value just before %s. Write the comparison and the %s as separate "+
		"conjuncts joined with AND", kw, op, strings.TrimSpace(raw), kw, kw)
}

// walkStatements calls visit on st and on every statement nested inside it -- an EXPLAIN's
// inner statement, a view's defining SELECT, the further branches of a set operation, and
// each subquery predicate's own SELECT. It stops at the first error.
//
// Nesting is why this exists rather than a check at one call site: a subquery inside a
// `CREATE MATERIALIZED VIEW ... AS SELECT`, or inside the second branch of a UNION, is as
// much a subquery as one in a bare SELECT, and a guard that only read the outermost
// statement would let it through.
func walkStatements(st *Statement, visit func(*Statement) error) error {
	if st == nil {
		return nil
	}
	if err := visit(st); err != nil {
		return err
	}
	for _, next := range []*Statement{st.Inner, st.ViewSelect} {
		if err := walkStatements(next, visit); err != nil {
			return err
		}
	}
	for i := range st.SetBranches {
		if err := walkStatements(st.SetBranches[i].Stmt, visit); err != nil {
			return err
		}
	}
	for i := range st.Subqueries {
		if err := walkStatements(st.Subqueries[i].Inner, visit); err != nil {
			return err
		}
	}
	return nil
}

// resolveAllSubqueries answers the subquery predicates of a statement and of every statement
// nested inside it, BEFORE anything is read.
//
// It walks rather than reading the outermost statement alone because a predicate that is
// parsed and not answered is the one outcome worse than one that is refused: the predicate is
// lifted out of its WHERE clause, so the statement would run with a filter silently missing
// and return MORE rows than it asked for, which reads as a successful answer.
func (e *Executor) resolveAllSubqueries(st *Statement) error {
	return walkStatements(st, e.resolveSubqueries)
}
