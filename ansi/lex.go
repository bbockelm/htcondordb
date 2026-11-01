package ansi

import "strings"

// A tokeniser for the predicate subset: identifiers, numbers, single-quoted strings,
// comparison operators, parentheses and commas. It is deliberately not a SQL lexer -- there is
// no arithmetic, no function call syntax, no qualified name -- because every token kind it does
// not produce is a construct the lowering would have had to refuse anyway, and refusing at the
// token is a clearer error than refusing at the parse.

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString
	tokOp
	tokLParen
	tokRParen
	tokComma
	tokBad
)

type token struct {
	kind tokKind
	text string
}

type lexer struct {
	src string
	pos int
}

func newLexer(s string) *lexer { return &lexer{src: s} }

func (l *lexer) next() token {
	for l.pos < len(l.src) && isSpace(l.src[l.pos]) {
		l.pos++
	}
	if l.pos >= len(l.src) {
		return token{kind: tokEOF}
	}
	c := l.src[l.pos]
	switch {
	case c == '(':
		l.pos++
		return token{kind: tokLParen, text: "("}
	case c == ')':
		l.pos++
		return token{kind: tokRParen, text: ")"}
	case c == ',':
		l.pos++
		return token{kind: tokComma, text: ","}
	case c == '\'':
		return l.lexString()
	case c == '"':
		// A double-quoted SQL identifier. Accepted so a client that quotes its column
		// names is not refused for a spelling difference.
		return l.lexQuotedIdent()
	case isDigit(c) || (c == '-' && l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1])):
		return l.lexNumber()
	case isIdentStart(c):
		return l.lexIdent()
	}
	return l.lexOp()
}

func (l *lexer) lexString() token {
	l.pos++ // opening quote
	var b strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == '\'' {
			// SQL escapes a quote by doubling it.
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == '\'' {
				b.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++
			return token{kind: tokString, text: b.String()}
		}
		b.WriteByte(c)
		l.pos++
	}
	return token{kind: tokBad, text: "unterminated string"}
}

func (l *lexer) lexQuotedIdent() token {
	l.pos++
	start := l.pos
	for l.pos < len(l.src) && l.src[l.pos] != '"' {
		l.pos++
	}
	if l.pos >= len(l.src) {
		return token{kind: tokBad, text: "unterminated quoted identifier"}
	}
	text := l.src[start:l.pos]
	l.pos++
	if text == "" || !isIdentStart(text[0]) {
		return token{kind: tokBad, text: text}
	}
	for i := 0; i < len(text); i++ {
		if !isIdentPart(text[i]) {
			return token{kind: tokBad, text: text}
		}
	}
	return token{kind: tokIdent, text: text}
}

func (l *lexer) lexNumber() token {
	start := l.pos
	if l.src[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.src) && (isDigit(l.src[l.pos]) || l.src[l.pos] == '.') {
		l.pos++
	}
	// An exponent, so 1e9 is a number rather than 1 followed by the identifier e9.
	if l.pos < len(l.src) && (l.src[l.pos] == 'e' || l.src[l.pos] == 'E') {
		save := l.pos
		l.pos++
		if l.pos < len(l.src) && (l.src[l.pos] == '+' || l.src[l.pos] == '-') {
			l.pos++
		}
		if l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
				l.pos++
			}
		} else {
			l.pos = save
		}
	}
	return token{kind: tokNumber, text: l.src[start:l.pos]}
}

func (l *lexer) lexIdent() token {
	start := l.pos
	for l.pos < len(l.src) && isIdentPart(l.src[l.pos]) {
		l.pos++
	}
	return token{kind: tokIdent, text: l.src[start:l.pos]}
}

func (l *lexer) lexOp() token {
	// Two-character operators first, so <= does not lex as < followed by =.
	if l.pos+1 < len(l.src) {
		two := l.src[l.pos : l.pos+2]
		switch two {
		case "<=", ">=", "<>", "!=":
			l.pos += 2
			return token{kind: tokOp, text: two}
		}
	}
	c := l.src[l.pos]
	switch c {
	case '=', '<', '>':
		l.pos++
		return token{kind: tokOp, text: string(c)}
	}
	l.pos++
	return token{kind: tokBad, text: string(c)}
}

func isSpace(c byte) bool      { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || (c|0x20 >= 'a' && c|0x20 <= 'z') }
func isIdentPart(c byte) bool  { return isIdentStart(c) || isDigit(c) }
