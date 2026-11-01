// Package ansi lowers an ANSI SQL predicate to a ClassAd constraint.
//
// It exists because Flight SQL carries SQL text, and the SQL a client writes does not mean what
// the same text means to ClassAd. The headline divergence: ClassAd's `==`, `<`, `>` compare
// strings CASE-INSENSITIVELY, matching the C++ reference engine, while SQL's `=` does not. So
// `WHERE Owner = 'bbockelm'` silently means different things in the two languages, with
// plausible results either way and nothing to say which you got.
//
// WHAT THIS DOES NOT DO, and why that is the design rather than a shortfall.
//
// The endpoint that uses this refuses any SQL it cannot lower faithfully, because nothing
// downstream corrects it -- a query half-understood returns rows that look right. This package
// applies the same rule to itself: it lowers what it can prove equivalent and REFUSES the rest,
// naming what it could not do. Getting a refusal wrong costs a missing feature; getting a
// lowering wrong costs wrong answers, and only one of those is recoverable from a bug report.
//
// So the supported grammar is small and will grow, and the omissions are reasoned:
//
//   - String EQUALITY lowers to `=?=`, which is case-sensitive and, better, is the one form the
//     store indexes exactly: probe.go routes it to the categorical index's exact-case postings.
//     It is accepted only in POSITIVE position -- see the polarity note below.
//   - String ORDERING (`<`, `>`, `<=`, `>=`) is refused. ClassAd's fold-order and SQL's
//     byte-order results are neither a subset nor a superset of one another (compare "apple"
//     against "R" under each), and the segment dictionaries are fold-sorted so no index can
//     serve a byte-ordered range anyway. There is no faithful lowering, so there is no lowering.
//   - `<>` on a string is refused for now. The 3VL guard it needs is correct but unverified
//     against the index, and an unindexed inequality on a large table is a surprise better
//     introduced deliberately than discovered.
//   - Numeric and boolean comparisons lower directly: ClassAd compares numbers numerically, as
//     SQL does, so there is nothing to diverge.
//
// POLARITY, and why refusal is the right shape for it. `=?=` returns FALSE where SQL's `=`
// returns NULL (an undefined operand is a type mismatch to identity). Under `AND` and `OR` that
// is unobservable for a WHERE clause -- false and NULL both drop a row, and both keep it when
// the other side decides. It diverges under `NOT`, where SQL drops a NULL row and `!false` keeps
// it, and wherever the value is surfaced rather than filtered on.
//
// An earlier design emitted a guarded form in those positions. This refuses instead. Emitting
// two forms of every predicate means one of them loses the index, and polarity is exactly the
// analysis that is right in review and wrong three levels into a nested expression. Refusing is
// a polarity analysis too, but one whose failure mode is "you cannot write that yet".
package ansi

import (
	"fmt"
	"strconv"
	"strings"
)

// Kind is a column's type, as the schema derivation declared it. It decides which lowering a
// comparison gets, which is what makes the lowering decidable at all: without a declared type
// there is no way to know whether `=` on this column is the case-sensitive problem or an
// ordinary number.
type Kind int

const (
	KindUnknown Kind = iota
	KindInt
	KindReal
	KindString
	KindBool
	KindTimestamp
)

// Columns reports a column's kind. An unknown column is refused rather than guessed: a
// predicate on an attribute the schema does not describe cannot be lowered correctly, because
// which lowering is correct depends on the type.
type Columns func(name string) (Kind, bool)

// KindsFromNames builds a Columns from a map, matching case-insensitively as ClassAd attribute
// names do.
func KindsFromNames(m map[string]Kind) Columns {
	lower := make(map[string]Kind, len(m))
	for k, v := range m {
		lower[strings.ToLower(k)] = v
	}
	return func(name string) (Kind, bool) {
		k, ok := lower[strings.ToLower(name)]
		return k, ok
	}
}

// Unsupported is a refusal: the predicate is valid SQL this package will not lower.
//
// It is a distinct type so a caller can tell "I will not do that" from "that is not SQL". The
// first is a feature gap worth reporting to a user as such; the second is their typo.
type Unsupported struct{ Reason string }

func (e *Unsupported) Error() string { return "unsupported predicate: " + e.Reason }

func unsup(format string, args ...any) error {
	return &Unsupported{Reason: fmt.Sprintf(format, args...)}
}

// Lower parses an ANSI SQL predicate and returns an equivalent ClassAd constraint.
//
// Equivalent means: over any ad, the ClassAd expression is true exactly where the SQL predicate
// would admit the row. It does not mean the two evaluate to the same VALUE -- SQL's NULL and
// ClassAd's false both drop a row, and that is the equivalence a WHERE clause needs.
func Lower(predicate string, cols Columns) (string, error) {
	t, err := LowerTemplate(predicate, cols)
	if err != nil {
		return "", err
	}
	if n := len(t.params); n > 0 {
		return "", unsup("this predicate has %d placeholder(s), and only a prepared "+
			"statement can supply a value for one", n)
	}
	return t.Bind(nil)
}

// LowerTemplate is Lower with placeholders left open, for a prepared statement.
//
// Lower is this function plus a refusal, rather than a second parser, so the two paths cannot
// disagree about what SQL means -- the same reason a prepared statement and a direct one share
// one scan.
func LowerTemplate(predicate string, cols Columns) (*Template, error) {
	if cols == nil {
		cols = func(string) (Kind, bool) { return KindUnknown, false }
	}
	p := &parser{lex: newLexer(predicate), cols: cols}
	p.next()
	out, err := p.parseOr(positive)
	if err != nil {
		return nil, err
	}
	if p.tok.kind != tokEOF {
		return nil, unsup("unexpected %q after the predicate", p.tok.text)
	}
	return &Template{body: out, params: p.params}, nil
}

// polarity tracks whether the current position is negated, which decides whether a comparison
// whose 3VL differs from SQL's may be used.
type polarity int

const (
	positive polarity = iota
	negated
)

func (p polarity) flip() polarity {
	if p == positive {
		return negated
	}
	return positive
}

type parser struct {
	lex    *lexer
	tok    token
	cols   Columns
	params []Param
}

// addParam records a placeholder and returns its index. Order is source order, which is what
// positional binding means.
func (p *parser) addParam(col string, kind Kind) int {
	p.params = append(p.params, Param{Column: col, Kind: kind})
	return len(p.params) - 1
}

func (p *parser) next() { p.tok = p.lex.next() }

func (p *parser) acceptKeyword(kw string) bool {
	if p.tok.kind == tokIdent && strings.EqualFold(p.tok.text, kw) {
		p.next()
		return true
	}
	return false
}

func (p *parser) peekKeyword(kw string) bool {
	return p.tok.kind == tokIdent && strings.EqualFold(p.tok.text, kw)
}

func (p *parser) parseOr(pol polarity) (expr, error) {
	left, err := p.parseAnd(pol)
	if err != nil {
		return expr{}, err
	}
	for p.peekKeyword("OR") {
		p.next()
		right, err := p.parseAnd(pol)
		if err != nil {
			return expr{}, err
		}
		left = seq("(", left, " || ", right, ")")
	}
	return left, nil
}

func (p *parser) parseAnd(pol polarity) (expr, error) {
	left, err := p.parseUnary(pol)
	if err != nil {
		return expr{}, err
	}
	for p.peekKeyword("AND") {
		p.next()
		right, err := p.parseUnary(pol)
		if err != nil {
			return expr{}, err
		}
		left = seq("(", left, " && ", right, ")")
	}
	return left, nil
}

func (p *parser) parseUnary(pol polarity) (expr, error) {
	if p.acceptKeyword("NOT") {
		inner, err := p.parseUnary(pol.flip())
		if err != nil {
			return expr{}, err
		}
		return seq("!(", inner, ")"), nil
	}
	if p.tok.kind == tokLParen {
		p.next()
		inner, err := p.parseOr(pol)
		if err != nil {
			return expr{}, err
		}
		if p.tok.kind != tokRParen {
			return expr{}, unsup("missing closing parenthesis")
		}
		p.next()
		return seq("(", inner, ")"), nil
	}
	return p.parsePredicate(pol)
}

// parsePredicate handles one comparison: the only place a column, an operator and a literal
// meet, and therefore the only place the dialect question arises.
func (p *parser) parsePredicate(pol polarity) (expr, error) {
	if p.tok.kind != tokIdent {
		return expr{}, unsup("expected a column name, got %q", p.tok.text)
	}
	col := p.tok.text
	kind, known := p.cols(col)
	if !known {
		return expr{}, unsup("unknown column %q; a predicate on an attribute the schema does not "+
			"describe cannot be lowered, because which lowering is correct depends on its type", col)
	}
	p.next()

	// IS [NOT] NULL -- the one form that means the same thing in both languages, because
	// ClassAd tests identity against undefined and the store can answer it from an index.
	if p.acceptKeyword("IS") {
		notNull := p.acceptKeyword("NOT")
		if !p.acceptKeyword("NULL") {
			return expr{}, unsup("expected NULL after IS")
		}
		if notNull {
			return fixed(quoteCol(col) + " isnt undefined"), nil
		}
		return fixed(quoteCol(col) + " is undefined"), nil
	}

	if p.peekKeyword("IN") {
		p.next()
		return p.parseIn(col, kind, pol)
	}
	if p.peekKeyword("BETWEEN") {
		p.next()
		return p.parseBetween(col, kind)
	}

	op, err := p.parseCompareOp()
	if err != nil {
		return expr{}, err
	}
	return p.parseOperand(col, kind, op, pol)
}

// parseOperand reads the right-hand side of a comparison -- a literal or a placeholder -- and
// lowers the comparison.
//
// A placeholder is checked for lowerability NOW, with a nil literal, so a client learns at
// prepare time that `NOT Owner = ?` will not be served rather than at the bind. Every refusal
// the inline path gives is therefore given to the parameterised path too, from the same code.
func (p *parser) parseOperand(col string, kind Kind, op string, pol polarity) (expr, error) {
	if p.tok.kind == tokParam {
		p.next()
		if _, err := lowerCompare(col, kind, op, nil, pol); err != nil {
			return expr{}, err
		}
		idx := p.addParam(col, kind)
		param := Param{Column: col, Kind: kind}
		return expr{frags: []frag{{slot: idx, render: func(v any) (string, error) {
			bv, err := normalize(v)
			if err != nil {
				return "", err
			}
			if err := bv.check(param); err != nil {
				return "", err
			}
			if bv.null {
				// SQL compares nothing to NULL successfully, so the row drops.
				// The whole comparison collapses, not the operand -- see param.go.
				return "false", nil
			}
			out, err := lowerCompare(col, kind, op, &literal{kind: bv.kind, text: bv.literal()}, pol)
			if err != nil {
				return "", err
			}
			// A bound comparison has no placeholders of its own, so it is one
			// fragment and this cannot recurse.
			return out.frags[0].text, nil
		}}}}, nil
	}
	lit, err := p.parseLiteral()
	if err != nil {
		return expr{}, err
	}
	return lowerCompare(col, kind, op, &lit, pol)
}

func (p *parser) parseCompareOp() (string, error) {
	if p.tok.kind != tokOp {
		return "", unsup("expected a comparison operator, got %q", p.tok.text)
	}
	op := p.tok.text
	p.next()
	switch op {
	case "=", "<>", "!=", "<", "<=", ">", ">=":
		return op, nil
	}
	return "", unsup("operator %q", op)
}

func (p *parser) parseIn(col string, kind Kind, pol polarity) (expr, error) {
	if p.tok.kind != tokLParen {
		return expr{}, unsup("expected ( after IN")
	}
	p.next()
	var terms []expr
	for {
		// IN is a disjunction of equalities, so each one carries the same lowering -- and
		// the same refusal, if equality on this column cannot be lowered. Each operand
		// may be a placeholder, which is how a client binds a list.
		term, err := p.parseOperand(col, kind, "=", pol)
		if err != nil {
			return expr{}, err
		}
		terms = append(terms, term)
		if p.tok.kind == tokComma {
			p.next()
			continue
		}
		break
	}
	if p.tok.kind != tokRParen {
		return expr{}, unsup("missing ) after an IN list")
	}
	p.next()
	if len(terms) == 1 {
		return terms[0], nil
	}
	parts := []any{"("}
	for i, t := range terms {
		if i > 0 {
			parts = append(parts, " || ")
		}
		parts = append(parts, t)
	}
	return seq(append(parts, ")")...), nil
}

func (p *parser) parseBetween(col string, kind Kind) (expr, error) {
	if kind == KindString {
		return expr{}, unsup("BETWEEN on the string column %q: it is a pair of ordering "+
			"comparisons, and ClassAd orders strings case-insensitively while SQL does not, "+
			"so neither bound can be lowered faithfully", col)
	}
	loTerm, err := p.parseOperand(col, kind, ">=", positive)
	if err != nil {
		return expr{}, err
	}
	if !p.acceptKeyword("AND") {
		return expr{}, unsup("expected AND in BETWEEN")
	}
	hiTerm, err := p.parseOperand(col, kind, "<=", positive)
	if err != nil {
		return expr{}, err
	}
	return seq("(", loTerm, " && ", hiTerm, ")"), nil
}

// literal is a parsed SQL literal, kept with its own kind so a comparison can check it against
// the column's.
type literal struct {
	kind Kind
	text string // already in ClassAd form
}

func (p *parser) parseLiteral() (literal, error) {
	switch p.tok.kind {
	case tokString:
		lit := literal{kind: KindString, text: classAdString(p.tok.text)}
		p.next()
		return lit, nil
	case tokNumber:
		text := p.tok.text
		kind := KindInt
		if strings.ContainsAny(text, ".eE") {
			kind = KindReal
		}
		if _, err := strconv.ParseFloat(text, 64); err != nil {
			return literal{}, unsup("%q is not a number", text)
		}
		p.next()
		return literal{kind: kind, text: text}, nil
	case tokIdent:
		switch strings.ToUpper(p.tok.text) {
		case "TRUE":
			p.next()
			return literal{kind: KindBool, text: "true"}, nil
		case "FALSE":
			p.next()
			return literal{kind: KindBool, text: "false"}, nil
		}
		return literal{}, unsup("expected a literal, got the identifier %q; "+
			"comparing two columns is not supported yet", p.tok.text)
	}
	return literal{}, unsup("expected a literal, got %q", p.tok.text)
}

// lowerCompare is where the dialect divergence is actually decided.
//
// A nil literal means "would this comparison be lowerable?", which is how a placeholder is
// validated at prepare time without a value. Every refusal below is reachable both ways, so a
// parameterised predicate cannot be accepted where its inline twin would be refused.
func lowerCompare(col string, kind Kind, op string, lit *literal, pol polarity) (expr, error) {
	if lit != nil {
		if err := checkLiteralKind(col, kind, *lit); err != nil {
			return expr{}, err
		}
	}
	switch kind {
	case KindString:
		return lowerStringCompare(col, op, lit, pol)
	case KindInt, KindReal, KindTimestamp, KindBool:
		// Numbers and booleans compare the same way in both languages, so the SQL
		// operator maps straight across. ClassAd's `==` on undefined yields undefined,
		// which drops the row exactly as SQL's NULL comparison does -- including under
		// NOT, since `!undefined` is undefined.
		cadOp := op
		switch op {
		case "=":
			cadOp = "=="
		case "<>":
			cadOp = "!="
		}
		if lit == nil {
			return expr{}, nil
		}
		return fixed(quoteCol(col) + " " + cadOp + " " + lit.text), nil
	}
	// Reached when a caller reports the column as present but with no usable type -- an
	// attribute that exists and has no scalar form, such as a policy expression. Saying so
	// beats "unknown column", which would send someone looking for a typo.
	return expr{}, unsup("column %q exists but has no declared scalar type, so a comparison on it "+
		"cannot be lowered; it is available as text in the untyped tail", col)
}

func lowerStringCompare(col, op string, lit *literal, pol polarity) (expr, error) {
	switch op {
	case "=":
		if pol != positive {
			return expr{}, unsup("negated equality on the string column %q: it lowers to "+
				"ClassAd identity, which yields false where SQL yields NULL -- "+
				"unobservable under AND and OR, but not under NOT", col)
		}
		if lit == nil {
			return expr{}, nil
		}
		// `=?=` is case-sensitive AND is the form the store indexes exactly: probe.go
		// routes it to the categorical index's exact-case postings. ClassAd's `==` would
		// be case-INSENSITIVE here, which is the whole divergence this package exists for.
		return fixed(quoteCol(col) + " =?= " + lit.text), nil
	case "<>", "!=":
		return expr{}, unsup("inequality on the string column %q: it needs a three-valued "+
			"guard whose index behaviour is unverified, and an unindexed inequality over a "+
			"large table is better introduced deliberately than discovered", col)
	}
	return expr{}, unsup("ordering comparison %q on the string column %q: ClassAd compares strings "+
		"case-insensitively and SQL does not, and the two orderings are neither a subset nor a "+
		"superset of one another, so there is no faithful lowering", op, col)
}

// checkLiteralKind refuses a comparison whose literal cannot be what the column holds.
//
// Having declared column types is what makes this possible at all: SQL rejects a type-invalid
// comparison when the statement is bound, and doing the same here beats lowering it and letting
// ClassAd's ERROR quietly filter out every row.
func checkLiteralKind(col string, kind Kind, lit literal) error {
	switch kind {
	case KindString:
		if lit.kind != KindString {
			return unsup("comparing the string column %q against a non-string literal", col)
		}
	case KindInt, KindReal, KindTimestamp:
		if lit.kind != KindInt && lit.kind != KindReal {
			return unsup("comparing the numeric column %q against a non-numeric literal", col)
		}
	case KindBool:
		if lit.kind != KindBool {
			return unsup("comparing the boolean column %q against a non-boolean literal", col)
		}
	}
	return nil
}

// quoteCol renders an attribute reference. ClassAd attribute names are bare identifiers, and
// the lexer has already rejected anything that is not one, so there is nothing to escape.
func quoteCol(name string) string { return name }

// classAdString renders a SQL string literal as a ClassAd one: double quotes, with the escapes
// ClassAd reads.
func classAdString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}
