package repl

import (
	"fmt"
	"strings"
)

// Expression capture, and the SQL spellings translated on the way through.
//
// An expression in this language is a ClassAd expression: it is captured from the source and
// handed to the ClassAd engine, so the whole ClassAd operator set is available (=?=, =!=,
// undefined, member(), regexp(), ?:, ...). But the statements around it are SQL, and an
// administrator's fingers type SQL inside them too. So a short list of SQL forms with no
// ClassAd spelling is translated here, at the token level, as the expression is captured:
//
//	CASE WHEN c THEN r [...] [ELSE d] END   ->  c ? (r) : (d)      (and the CASE e WHEN v form)
//	'text'                                  ->  "text"
//	=   <>                                  ->  ==  !=
//	AND  OR  NOT                            ->  &&  ||  !
//	x IS NULL   x IS NOT NULL               ->  x is undefined   x isnt undefined
//
// Everything else is copied through byte for byte, so an expression written in ClassAd
// reaches the engine exactly as typed. All of these forms were previously hard parse errors
// -- except 'text', which ClassAd reads as a quoted ATTRIBUTE NAME and silently evaluated to
// undefined, which is the one behaviour this changes.

// captureExpr walks the expression starting at the current token until stop() holds at the
// top parenthesis level (or input ends), advancing past it. It returns the expression
// translated for the ClassAd engine, and the raw source it came from (for column headers and
// error messages).
//
// When lift is non-nil, aggregate calls are hoisted out into placeholder attributes and
// appended to it, so `2 * COUNT(*)` captures as `2 * __agg_0` (see aggexpr.go). CASE arms are
// captured through this same function, so an aggregate inside one is lifted too.
func (p *parser) captureExpr(stop func() bool, lift *[]AggCall) (expr, raw string, err error) {
	if p.atEnd() || stop() {
		return "", "", fmt.Errorf("empty expression")
	}
	start := p.peek().pos
	end, written := start, start
	var b strings.Builder
	depth := 0
	for !p.atEnd() {
		if depth == 0 && stop() {
			break
		}
		t := p.peek()
		sub, matched, terr := p.translateSQL(lift)
		if terr != nil {
			return "", "", terr
		}
		if matched {
			b.WriteString(p.src[written:t.pos]) // source up to the form we replaced
			b.WriteString(sub)
			end = p.toks[p.pos-1].end
			written = end
			continue
		}
		p.pos++
		end = t.end
		// Nesting: parentheses, and ClassAd list braces. Braces count because a list
		// literal contains commas -- `VALUES ('k', {1, 2, 3})` is two values, not four --
		// and the stop predicate for a VALUES item or a SELECT column fires on a
		// top-level comma. The lexer emits '{' and '}' as single-char operators (it only
		// treats ( ) , as punctuation), so both token kinds have to be considered --
		// but NOT tString, whose text is the unquoted content: `Owner == '{'` must not
		// open a nesting level.
		if t.kind == tPunct || t.kind == tOp {
			switch t.text {
			case "(", "{":
				depth++
			case ")", "}":
				depth--
			}
		}
	}
	b.WriteString(p.src[written:end])
	raw = strings.TrimSpace(p.src[start:end])
	expr = strings.TrimSpace(b.String())
	if raw == "" || expr == "" {
		return "", "", fmt.Errorf("empty expression")
	}
	return expr, raw, nil
}

// translateSQL consumes a SQL form at the current token and returns the ClassAd text to emit
// in its place. matched is false (and nothing consumed) when the token starts no such form,
// which is the common case.
func (p *parser) translateSQL(lift *[]AggCall) (out string, matched bool, err error) {
	t := p.peek()
	switch {
	case lift != nil && t.kind == tIdent && isAggName(strings.ToUpper(t.text)) && p.peekAheadPunct(1, "("):
		call, cerr := p.parseAggCall()
		if cerr != nil {
			return "", false, cerr
		}
		out = fmt.Sprintf("%s%d", aggPlaceholderPrefix, len(*lift))
		*lift = append(*lift, call)
		return out, true, nil

	case p.isKeyword("CASE"):
		s, cerr := p.parseCaseExpr(lift)
		if cerr != nil {
			return "", false, cerr
		}
		return s, true, nil

	// A string literal: the lexer already unquoted it, so re-emit it ClassAd-style. This
	// covers 'text' (SQL) and "text" (ClassAd) alike, and normalizes the escaping.
	case t.kind == tString:
		p.pos++
		return classAdString(t.text), true, nil

	// `x IS NULL` / `x IS NOT NULL` -- ClassAd tests identity against undefined, which the
	// store can also answer from an index (a presence probe).
	case p.isKeyword("IS") && (p.peekKeywordAt(1, "NULL") || (p.peekKeywordAt(1, "NOT") && p.peekKeywordAt(2, "NULL"))):
		p.pos++ // IS
		if p.takeKeyword("NOT") {
			p.pos++ // NULL
			return "isnt undefined", true, nil
		}
		p.pos++ // NULL
		return "is undefined", true, nil

	case p.isKeyword("AND"):
		p.pos++
		return "&&", true, nil
	case p.isKeyword("OR"):
		p.pos++
		return "||", true, nil
	case p.isKeyword("NOT"):
		p.pos++
		return "!", true, nil

	case t.kind == tOp && t.text == "=":
		p.pos++
		return "==", true, nil
	case t.kind == tOp && t.text == "<>":
		p.pos++
		return "!=", true, nil
	}
	return "", false, nil
}

// parseCaseExpr consumes a SQL CASE ... END and returns the ClassAd conditional it means.
// Both SQL forms are accepted:
//
//	CASE WHEN c1 THEN r1 [WHEN c2 THEN r2 ...] [ELSE d] END
//	CASE e WHEN v1 THEN r1 [WHEN v2 THEN r2 ...] [ELSE d] END
//
// The searched form becomes a right-nested conditional chain; the simple form compares the
// operand against each WHEN value first. A missing ELSE yields undefined -- SQL's NULL. The
// result is parenthesized so it composes inside a larger expression (`2 * CASE ... END`).
//
// Arms are captured through captureExpr, so they may themselves contain CASE (a nested END
// is consumed by the inner call and never reaches the outer arm's terminator) and, in a
// SELECT list, aggregates.
func (p *parser) parseCaseExpr(lift *[]AggCall) (string, error) {
	p.pos++ // CASE
	var operand string
	// The simple form puts an operand before the first WHEN. Skip the capture when the next
	// token already ends an arm (`CASE ELSE ...`), so the missing WHEN is reported as such
	// rather than as an empty operand.
	if !p.isKeyword("WHEN") && !p.atCaseArmEnd() {
		op, _, err := p.captureExpr(p.atCaseArmEnd, lift)
		if err != nil {
			return "", fmt.Errorf("CASE operand: %w", err)
		}
		operand = op
	}
	type arm struct{ when, then string }
	var arms []arm
	for p.takeKeyword("WHEN") {
		when, _, err := p.captureExpr(p.atCaseArmEnd, lift)
		if err != nil {
			return "", fmt.Errorf("CASE WHEN: %w", err)
		}
		if !p.takeKeyword("THEN") {
			return "", fmt.Errorf("CASE: expected THEN after WHEN, got %q", p.peek().text)
		}
		then, _, err := p.captureExpr(p.atCaseArmEnd, lift)
		if err != nil {
			return "", fmt.Errorf("CASE THEN: %w", err)
		}
		arms = append(arms, arm{when: when, then: then})
	}
	if len(arms) == 0 {
		return "", fmt.Errorf("CASE: expected at least one WHEN ... THEN")
	}
	out := "undefined" // no ELSE: SQL yields NULL
	if p.takeKeyword("ELSE") {
		els, _, err := p.captureExpr(p.atCaseArmEnd, lift)
		if err != nil {
			return "", fmt.Errorf("CASE ELSE: %w", err)
		}
		out = els
	}
	if !p.takeKeyword("END") {
		return "", fmt.Errorf("CASE: expected END, got %q", p.peek().text)
	}
	for i := len(arms) - 1; i >= 0; i-- {
		cond := "(" + arms[i].when + ")"
		if operand != "" {
			cond = fmt.Sprintf("((%s) == (%s))", operand, arms[i].when)
		}
		out = fmt.Sprintf("%s ? (%s) : (%s)", cond, arms[i].then, out)
	}
	return "(" + out + ")", nil
}

// atCaseArmEnd reports whether the parser sits at a keyword that ends a CASE arm.
func (p *parser) atCaseArmEnd() bool {
	return p.atEnd() || p.isKeyword("WHEN") || p.isKeyword("THEN") ||
		p.isKeyword("ELSE") || p.isKeyword("END")
}

// peekKeywordAt reports whether the token n ahead is the given keyword.
func (p *parser) peekKeywordAt(n int, kw string) bool {
	t := p.peekAt(n)
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

// classAdString renders a string literal in ClassAd syntax. Only the quote and the escape
// character need escaping; every other byte (including UTF-8) passes through, unlike
// strconv.Quote, which would emit \u escapes the ClassAd lexer does not read.
func classAdString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}
