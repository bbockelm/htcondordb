// Package repl implements a small SQL-like query language over the htcondordb
// ClassAd store and an interactive client (REPL) for it.
//
// The store is a single keyed collection of ClassAds -- there are no tables to
// join -- so the language is deliberately the join-free subset of SQL: SELECT
// (with a WHERE filter, column projection, DISTINCT, the COUNT/SUM/AVG/MIN/MAX
// aggregates, GROUP BY over one or more columns, ORDER BY, and LIMIT), INSERT,
// UPDATE, and DELETE, plus CREATE/DROP TABLE, CREATE/DROP INDEX, and MATCH
// (matchmaking between two tables). Aggregation is evaluated server-side
// (hash-map GROUP BY). JOIN and subqueries are intentionally unsupported and
// rejected with a clear error; cross-table work is matchmaking, not a join.
//
// A WHERE clause (and an UPDATE assignment's right-hand side) is a *ClassAd*
// expression, captured verbatim and evaluated by the store's expression engine
// -- the full ClassAd language is available (==, =?=, =!=, undefined, member(),
// regexp(), the ?: operator, ...), not a SQL dialect. String literals use
// double quotes as in ClassAd.
//
// The one table every statement addresses is the ClassAd store itself; the FROM
// / INTO / UPDATE name is accepted for familiarity but is otherwise a label. A
// row's primary key is carried in a key attribute (default "Key", see
// ExecConfig): INSERT stamps it into the ad so that SELECT can display it and
// UPDATE/DELETE can recover the key of every row a WHERE clause matches. WHERE
// and assignment right-hand sides are translated to ClassAd expressions and
// evaluated by the store's expression engine, so the full ClassAd operator set
// is available; the translation only adapts SQL spellings (`=`, `<>`, AND/OR,
// single-quoted strings).
package repl

import (
	"fmt"
	"strings"
)

// StmtKind identifies a parsed statement's type.
type StmtKind int

const (
	StmtSelect StmtKind = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtCreateTable
	StmtDropTable
	StmtCreateIndex
	StmtDropIndex
	StmtMatch
	StmtWatch
)

// Statement is one parsed SQL-like statement.
type Statement struct {
	Kind StmtKind

	// Table is the FROM/INTO/UPDATE target table, or the table a DDL statement
	// acts on.
	Table string

	// IndexKind is "value" or "categorical" for CREATE INDEX.
	IndexKind string

	// InMemory is set by CREATE TABLE <name> MEMORY: create the table as RAM-only
	// (data not persisted across a server restart).
	InMemory bool

	// MatchResource is the resource table for a MATCH statement (Table is the
	// request table); TargetWhere is the pushed-down resource-side filter; Key,
	// if set, matches only that single request key; MatchUsing lists the
	// significant matchmaking attributes for autoclustering (identical requests
	// share one candidate computation).
	MatchResource string
	TargetWhere   string
	Key           string
	MatchUsing    []string
	NoPreempt     bool // MATCH ... NOPREEMPT: exclude already-claimed resources

	// Select fields.
	Items    []SelectItem // projection; a single {Star:true} means "*"
	Distinct bool         // SELECT DISTINCT
	GroupBy  []string     // GROUP BY columns ("" = none)
	OrderBy  []OrderTerm  // ORDER BY terms ("" = unordered)
	Limit    int          // 0 = no limit

	// Insert fields.
	Columns []string // target columns
	Values  []string // ClassAd-literal value expressions, aligned with Columns

	// Update fields.
	Assignments []Assignment

	// Where is the translated ClassAd constraint ("" = match all). Used by
	// SELECT, UPDATE, DELETE.
	Where string

	// Since is the WATCH start point: "now" (default; live changes only) or
	// "beginning" (replay the current contents, then live).
	Since string
}

// SelectItem is one projected column or aggregate. For "*", Star is set. For a
// plain column, Agg is "" and Col is the attribute name. For an aggregate,
// Agg is COUNT/SUM/AVG/MIN/MAX and Col is its argument ("*" for COUNT(*)).
type SelectItem struct {
	Star  bool
	Agg   string // "", "COUNT", "SUM", "AVG", "MIN", "MAX"
	Col   string
	Alias string // display header; defaults to the source text
}

// IsAggregate reports whether this item is an aggregate function.
func (it SelectItem) IsAggregate() bool { return it.Agg != "" }

// Assignment is one UPDATE ... SET column = expr.
type Assignment struct {
	Col  string
	Expr string // a ClassAd expression (captured verbatim)
}

// OrderTerm is one ORDER BY key: a column or aggregate, ascending unless Desc.
type OrderTerm struct {
	Item SelectItem
	Desc bool
}

// header returns the display header for a select item.
func (it SelectItem) header() string {
	if it.Alias != "" {
		return it.Alias
	}
	if it.Star {
		return "*"
	}
	if it.Agg != "" {
		return it.Agg + "(" + it.Col + ")"
	}
	return it.Col
}

// Parse parses one statement. Trailing ';' is allowed. It returns a descriptive
// error for empty input, unsupported constructs (JOIN, GROUP BY, ...), and
// syntax errors.
func Parse(input string) (*Statement, error) {
	toks, err := lex(input)
	if err != nil {
		return nil, err
	}
	if len(toks) == 0 {
		return nil, errEmpty
	}
	p := &parser{toks: toks, src: input}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	if !p.atEnd() {
		return nil, fmt.Errorf("unexpected %q after statement", p.peek().text)
	}
	return stmt, nil
}

var errEmpty = fmt.Errorf("empty statement")

// --- lexer ---

type tokKind int

const (
	tIdent  tokKind = iota // identifier or keyword (text is as-written)
	tNumber                // numeric literal
	tString                // string literal (text is the unquoted content)
	tOp                    // operator: == = != <> < <= > >= + - * / && || ! .
	tPunct                 // ( ) ,
)

type token struct {
	kind tokKind
	text string
	pos  int // start byte offset in the source
	end  int // end byte offset (exclusive), so src[pos:end] is the raw token
}

func lex(s string) ([]token, error) {
	var toks []token
	i, n := 0, len(s)
	emit := func(kind tokKind, text string, start, end int) {
		toks = append(toks, token{kind: kind, text: text, pos: start, end: end})
	}
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == ';':
			// A single trailing terminator is fine; anything after it is caller error.
			i++
		case c == '\'':
			// Single-quoted string; '' is an escaped quote.
			j := i + 1
			var sb strings.Builder
			for j < n {
				if s[j] == '\'' {
					if j+1 < n && s[j+1] == '\'' {
						sb.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				sb.WriteByte(s[j])
				j++
			}
			if j >= n {
				return nil, fmt.Errorf("unterminated string literal")
			}
			emit(tString, sb.String(), i, j+1)
			i = j + 1
		case c == '"':
			// Double-quoted: accepted as a string too (ClassAd-native spelling).
			j := i + 1
			for j < n && s[j] != '"' {
				j++
			}
			if j >= n {
				return nil, fmt.Errorf("unterminated string literal")
			}
			emit(tString, s[i+1:j], i, j+1)
			i = j + 1
		case isDigit(c) || (c == '.' && i+1 < n && isDigit(s[i+1])):
			j := i
			for j < n && (isDigit(s[j]) || s[j] == '.' || s[j] == 'e' || s[j] == 'E' ||
				((s[j] == '+' || s[j] == '-') && j > i && (s[j-1] == 'e' || s[j-1] == 'E'))) {
				j++
			}
			emit(tNumber, s[i:j], i, j)
			i = j
		case isIdentStart(c):
			j := i
			for j < n && isIdentPart(s[j]) {
				j++
			}
			emit(tIdent, s[i:j], i, j)
			i = j
		case c == '(' || c == ')' || c == ',':
			emit(tPunct, string(c), i, i+1)
			i++
		default:
			// ClassAd is-identical / is-not-identical (three chars) first.
			if i+2 < n {
				three := s[i : i+3]
				if three == "=?=" || three == "=!=" {
					emit(tOp, three, i, i+3)
					i += 3
					continue
				}
			}
			two := ""
			if i+1 < n {
				two = s[i : i+2]
			}
			switch two {
			case "==", "!=", "<>", "<=", ">=", "&&", "||":
				emit(tOp, two, i, i+2)
				i += 2
				continue
			}
			// Any remaining operator/punctuation byte is a single-char op. This is
			// deliberately permissive: WHERE and SET right-hand sides are captured
			// verbatim from the source and handed to the ClassAd engine, so the
			// lexer only needs to tokenize the surrounding statement without
			// choking on the full ClassAd operator set (? : % & | ^ ~ etc.).
			emit(tOp, string(c), i, i+1)
			i++
		}
	}
	return toks, nil
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isIdentPart(c byte) bool  { return isIdentStart(c) || isDigit(c) }

// --- parser ---

type parser struct {
	toks []token
	pos  int
	src  string // original source, for verbatim WHERE / SET expression capture
}

func (p *parser) atEnd() bool { return p.pos >= len(p.toks) }

func (p *parser) peek() token {
	if p.atEnd() {
		return token{kind: tIdent}
	}
	return p.toks[p.pos]
}

func (p *parser) next() token {
	t := p.peek()
	p.pos++
	return t
}

// isKeyword reports whether the next token is the given keyword (case-insensitive).
func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

// takeKeyword consumes the next token if it is kw; returns whether it did.
func (p *parser) takeKeyword(kw string) bool {
	if p.isKeyword(kw) {
		p.pos++
		return true
	}
	return false
}

// expectKeyword consumes kw or errors.
func (p *parser) expectKeyword(kw string) error {
	if !p.takeKeyword(kw) {
		return fmt.Errorf("expected %s, got %q", kw, p.peek().text)
	}
	return nil
}

// expectPunct consumes the given punctuation or errors.
func (p *parser) expectPunct(s string) error {
	t := p.peek()
	if t.kind == tPunct && t.text == s {
		p.pos++
		return nil
	}
	return fmt.Errorf("expected %q, got %q", s, t.text)
}

func (p *parser) atPunct(s string) bool {
	t := p.peek()
	return t.kind == tPunct && t.text == s
}

func (p *parser) parseStatement() (*Statement, error) {
	switch {
	case p.takeKeyword("SELECT"):
		return p.parseSelect()
	case p.takeKeyword("INSERT"):
		return p.parseInsert()
	case p.takeKeyword("UPDATE"):
		return p.parseUpdate()
	case p.takeKeyword("DELETE"):
		return p.parseDelete()
	case p.takeKeyword("CREATE"):
		return p.parseCreate()
	case p.takeKeyword("DROP"):
		return p.parseDrop()
	case p.takeKeyword("MATCH"):
		return p.parseMatch()
	case p.takeKeyword("WATCH"):
		return p.parseWatch()
	default:
		return nil, fmt.Errorf("unsupported statement %q (expected SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, MATCH, or WATCH)", p.peek().text)
	}
}

// parseWatch parses:
//
//	WATCH {* | <attr>[, <attr>...]} FROM <table>
//	     [WHERE <constraint>] [SINCE {NOW | BEGINNING}] [LIMIT <n>]
//
// It streams live changes to the table, projecting the named attributes and filtering
// upserts by the WHERE constraint (deletes are always shown). SINCE BEGINNING first
// replays the current contents; the default (NOW) shows only changes from now on.
func (p *parser) parseWatch() (*Statement, error) {
	st := &Statement{Kind: StmtWatch, Since: "now"}
	for { // projection list (mirrors SELECT)
		it, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		if it.IsAggregate() {
			return nil, fmt.Errorf("WATCH does not support aggregates")
		}
		st.Items = append(st.Items, it)
		if !p.atPunct(",") {
			break
		}
		p.pos++ // consume comma
	}
	if len(st.Items) > 1 {
		for _, it := range st.Items {
			if it.Star {
				return nil, fmt.Errorf("`*` cannot be combined with other columns")
			}
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	st.Table = table
	if p.takeKeyword("WHERE") {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		st.Where = where
	}
	if p.takeKeyword("SINCE") {
		switch {
		case p.takeKeyword("NOW"):
			st.Since = "now"
		case p.takeKeyword("BEGINNING"):
			st.Since = "beginning"
		default:
			return nil, fmt.Errorf("SINCE expects NOW or BEGINNING, got %q", p.peek().text)
		}
	}
	if p.takeKeyword("LIMIT") {
		t := p.next()
		if t.kind != tNumber {
			return nil, fmt.Errorf("LIMIT expects a number, got %q", t.text)
		}
		var lim int
		if _, err := fmt.Sscanf(t.text, "%d", &lim); err != nil || lim < 0 {
			return nil, fmt.Errorf("invalid LIMIT %q", t.text)
		}
		st.Limit = lim
	}
	return st, nil
}

// parseCreate parses CREATE TABLE <name> or
// CREATE [VALUE|CATEGORICAL] INDEX ON <table> (<attr>, ...).
func (p *parser) parseCreate() (*Statement, error) {
	if p.takeKeyword("TABLE") {
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		// Optional MEMORY: create the table as RAM-only (non-persistent).
		inMemory := p.takeKeyword("MEMORY")
		return &Statement{Kind: StmtCreateTable, Table: name, InMemory: inMemory}, nil
	}
	// Optional index kind before INDEX; default value.
	kind := "value"
	if p.takeKeyword("VALUE") {
		kind = "value"
	} else if p.takeKeyword("CATEGORICAL") {
		kind = "categorical"
	}
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, fmt.Errorf("expected TABLE or [VALUE|CATEGORICAL] INDEX after CREATE")
	}
	table, cols, err := p.parseIndexTarget()
	if err != nil {
		return nil, err
	}
	return &Statement{Kind: StmtCreateIndex, Table: table, IndexKind: kind, Columns: cols}, nil
}

// parseDrop parses DROP TABLE <name> or DROP INDEX ON <table> (<attr>, ...).
func (p *parser) parseDrop() (*Statement, error) {
	if p.takeKeyword("TABLE") {
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		return &Statement{Kind: StmtDropTable, Table: name}, nil
	}
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, fmt.Errorf("expected TABLE or INDEX after DROP")
	}
	table, cols, err := p.parseIndexTarget()
	if err != nil {
		return nil, err
	}
	return &Statement{Kind: StmtDropIndex, Table: table, Columns: cols}, nil
}

// parseIndexTarget parses "ON <table> (<attr>, ...)".
func (p *parser) parseIndexTarget() (table string, cols []string, err error) {
	if err = p.expectKeyword("ON"); err != nil {
		return "", nil, err
	}
	if table, err = p.parseIdent(); err != nil {
		return "", nil, err
	}
	if err = p.expectPunct("("); err != nil {
		return "", nil, err
	}
	if cols, err = p.parseIdentList(); err != nil {
		return "", nil, err
	}
	return table, cols, nil
}

// parseMatch parses MATCH <requestTable> TO <resourceTable>
// [WHERE <request-filter>] [WHERE TARGET <resource-filter>] [LIMIT k], and the
// single-request form MATCH KEY '<key>' IN <requestTable> TO <resourceTable> ...
func (p *parser) parseMatch() (*Statement, error) {
	st := &Statement{Kind: StmtMatch, Limit: 1}
	if p.takeKeyword("KEY") {
		key, err := p.parseStringLiteral()
		if err != nil {
			return nil, err
		}
		st.Key = key
		if err := p.expectKeyword("IN"); err != nil {
			return nil, err
		}
	}
	req, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	st.Table = req
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	res, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	st.MatchResource = res
	// Optional USING (attrs): significant attributes for autoclustering.
	if p.takeKeyword("USING") {
		if err := p.expectPunct("("); err != nil {
			return nil, err
		}
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		st.MatchUsing = cols
	}
	// NOPREEMPT: only match resources that are not already claimed by a job.
	if p.takeKeyword("NOPREEMPT") {
		st.NoPreempt = true
	}
	// Zero, one, or two WHERE clauses: bare = request-side, WHERE TARGET =
	// resource-side (pushed down).
	for p.takeKeyword("WHERE") {
		if p.takeKeyword("TARGET") {
			expr, err := p.captureRawExpr(matchExprStop(p))
			if err != nil {
				return nil, err
			}
			st.TargetWhere = expr
		} else {
			expr, err := p.captureRawExpr(matchExprStop(p))
			if err != nil {
				return nil, err
			}
			st.Where = expr
		}
	}
	if p.takeKeyword("LIMIT") {
		lim, err := p.parseLimitValue()
		if err != nil {
			return nil, err
		}
		st.Limit = lim
	}
	return st, nil
}

// matchExprStop stops a captured MATCH filter at the next WHERE/LIMIT or end.
func matchExprStop(p *parser) func() bool {
	return func() bool {
		return p.atEnd() || p.isKeyword("WHERE") || p.isKeyword("LIMIT")
	}
}

// parseStringLiteral consumes a string literal, returning its content.
func (p *parser) parseStringLiteral() (string, error) {
	t := p.peek()
	if t.kind != tString {
		return "", fmt.Errorf("expected a quoted string, got %q", t.text)
	}
	p.pos++
	return t.text, nil
}

// parseLimitValue parses a non-negative integer LIMIT value.
func (p *parser) parseLimitValue() (int, error) {
	t := p.next()
	if t.kind != tNumber {
		return 0, fmt.Errorf("LIMIT expects a number, got %q", t.text)
	}
	var lim int
	if _, err := fmt.Sscanf(t.text, "%d", &lim); err != nil || lim < 0 {
		return 0, fmt.Errorf("invalid LIMIT %q", t.text)
	}
	return lim, nil
}

func (p *parser) parseSelect() (*Statement, error) {
	st := &Statement{Kind: StmtSelect}
	if p.takeKeyword("DISTINCT") {
		st.Distinct = true
	}
	// Projection list.
	for {
		it, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		st.Items = append(st.Items, it)
		if !p.atPunct(",") {
			break
		}
		p.pos++ // consume comma
	}
	if len(st.Items) > 1 {
		for _, it := range st.Items {
			if it.Star {
				return nil, fmt.Errorf("`*` cannot be combined with other columns")
			}
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	st.Table = table
	if err := p.rejectJoins(); err != nil {
		return nil, err
	}
	if p.takeKeyword("WHERE") {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		st.Where = where
	}
	if p.takeKeyword("GROUP") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		cols, err := p.parseGroupCols()
		if err != nil {
			return nil, err
		}
		st.GroupBy = cols
	}
	// Validate the projection against the (now known) GROUP BY.
	if err := validateSelect(st); err != nil {
		return nil, err
	}
	if p.takeKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		terms, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		st.OrderBy = terms
	}
	if p.takeKeyword("LIMIT") {
		t := p.next()
		if t.kind != tNumber {
			return nil, fmt.Errorf("LIMIT expects a number, got %q", t.text)
		}
		var lim int
		if _, err := fmt.Sscanf(t.text, "%d", &lim); err != nil || lim < 0 {
			return nil, fmt.Errorf("invalid LIMIT %q", t.text)
		}
		st.Limit = lim
	}
	return st, nil
}

func (p *parser) parseSelectItem() (SelectItem, error) {
	// "*"
	if t := p.peek(); t.kind == tOp && t.text == "*" {
		p.pos++
		return SelectItem{Star: true}, nil
	}
	t := p.peek()
	if t.kind != tIdent {
		return SelectItem{}, fmt.Errorf("expected a column name, got %q", t.text)
	}
	// Aggregate?  IDENT '(' ... ')'
	if agg := strings.ToUpper(t.text); isAggName(agg) && p.peekAheadPunct(1, "(") {
		p.pos += 2 // ident + '('
		var arg string
		if pk := p.peek(); pk.kind == tOp && pk.text == "*" {
			arg = "*"
			p.pos++
		} else {
			col, err := p.parseIdent()
			if err != nil {
				return SelectItem{}, err
			}
			arg = col
		}
		if err := p.expectPunct(")"); err != nil {
			return SelectItem{}, err
		}
		if agg == "COUNT" && arg != "*" {
			// COUNT(col) counts rows where col is defined; we treat it like COUNT(*)
			// for simplicity but keep the argument for the header.
		}
		it := SelectItem{Agg: agg, Col: arg}
		it.Alias = p.parseOptionalAlias()
		return it, nil
	}
	// Plain column.
	p.pos++
	it := SelectItem{Col: t.text}
	it.Alias = p.parseOptionalAlias()
	return it, nil
}

// parseOptionalAlias consumes an optional `AS name` (or bare `name`) alias.
func (p *parser) parseOptionalAlias() string {
	if p.takeKeyword("AS") {
		if t := p.peek(); t.kind == tIdent {
			p.pos++
			return t.text
		}
	}
	return ""
}

func (p *parser) parseInsert() (*Statement, error) {
	st := &Statement{Kind: StmtInsert}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	st.Table = table
	if err := p.expectPunct("("); err != nil {
		return nil, fmt.Errorf("INSERT requires a column list: %w", err)
	}
	cols, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}
	st.Columns = cols
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	vals, err := p.parseValueList()
	if err != nil {
		return nil, err
	}
	st.Values = vals
	if len(st.Columns) != len(st.Values) {
		return nil, fmt.Errorf("INSERT has %d columns but %d values", len(st.Columns), len(st.Values))
	}
	return st, nil
}

func (p *parser) parseUpdate() (*Statement, error) {
	st := &Statement{Kind: StmtUpdate}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	st.Table = table
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	for {
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		if t := p.peek(); !(t.kind == tOp && (t.text == "=" || t.text == "==")) {
			return nil, fmt.Errorf("expected `=` after %s, got %q", col, t.text)
		}
		p.pos++ // '='
		expr, err := p.captureRawExpr(func() bool {
			return p.atPunct(",") || p.isKeyword("WHERE") || p.atEnd()
		})
		if err != nil {
			return nil, err
		}
		st.Assignments = append(st.Assignments, Assignment{Col: col, Expr: expr})
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	if len(st.Assignments) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one assignment")
	}
	if p.takeKeyword("WHERE") {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		st.Where = where
	}
	return st, nil
}

func (p *parser) parseDelete() (*Statement, error) {
	st := &Statement{Kind: StmtDelete}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	table, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	st.Table = table
	if p.takeKeyword("WHERE") {
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		st.Where = where
	}
	return st, nil
}

// parseWhere captures the WHERE clause (up to end/GROUP/ORDER/LIMIT/SINCE) verbatim as
// a ClassAd expression, so the full ClassAd language is available (==, =?=, =!=,
// undefined, member(), regexp(), ?:, ...). SINCE is a WATCH-only terminator; the other
// statements never use it, so listing it here is harmless for them.
func (p *parser) parseWhere() (string, error) {
	return p.captureRawExpr(func() bool {
		return p.atEnd() || p.isKeyword("GROUP") || p.isKeyword("ORDER") ||
			p.isKeyword("LIMIT") || p.isKeyword("SINCE")
	})
}

// captureRawExpr returns the source text of the expression starting at the
// current token and running until stop() is true at the top paren level (or end
// of input), advancing past it. The text is handed to the ClassAd engine
// unchanged -- no SQL-to-ClassAd translation.
func (p *parser) captureRawExpr(stop func() bool) (string, error) {
	if p.atEnd() || stop() {
		return "", fmt.Errorf("empty expression")
	}
	start := p.peek().pos
	end := start
	depth := 0
	for !p.atEnd() {
		if depth == 0 && stop() {
			break
		}
		t := p.next()
		end = t.end
		if t.kind == tPunct {
			if t.text == "(" {
				depth++
			} else if t.text == ")" {
				depth--
			}
		}
	}
	raw := strings.TrimSpace(p.src[start:end])
	if raw == "" {
		return "", fmt.Errorf("empty expression")
	}
	return raw, nil
}

// parseOrderBy parses "term [ASC|DESC] (, term [ASC|DESC])*". Each term is a
// column or aggregate (reusing the SELECT-item grammar).
func (p *parser) parseOrderBy() ([]OrderTerm, error) {
	var terms []OrderTerm
	for {
		it, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		term := OrderTerm{Item: it}
		if p.takeKeyword("DESC") {
			term.Desc = true
		} else {
			p.takeKeyword("ASC")
		}
		terms = append(terms, term)
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	return terms, nil
}

func (p *parser) parseIdent() (string, error) {
	t := p.peek()
	if t.kind != tIdent {
		return "", fmt.Errorf("expected an identifier, got %q", t.text)
	}
	p.pos++
	return t.text, nil
}

func (p *parser) parseIdentList() ([]string, error) {
	var out []string
	for {
		id, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		out = append(out, id)
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	return out, nil
}

// parseValueList parses a VALUES(...) list into ClassAd-literal expressions.
func (p *parser) parseValueList() ([]string, error) {
	var out []string
	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	return out, nil
}

// parseValue parses a single literal value (string, number, bool, null, or a
// signed number) into its ClassAd expression text.
// parseValue parses one INSERT value: a literal (single-quoted strings become
// ClassAd strings) when it is a lone literal token, else a ClassAd expression
// captured verbatim -- so an attribute can be assigned an expression such as
// Requirements = TARGET.Cpus >= RequestCpus or Rank = TARGET.Cpus.
func (p *parser) parseValue() (string, error) {
	if lit, ok := p.tryLiteralValue(); ok {
		return lit, nil
	}
	// Not a lone literal: capture a ClassAd expression up to the next top-level
	// comma or the closing ')'.
	return p.captureRawExpr(func() bool {
		return p.atEnd() || p.atPunct(",") || p.atPunct(")")
	})
}

// tryLiteralValue consumes a lone literal value (string/number/[+-]number/bool/
// null) only when it is immediately followed by ',' or ')'; otherwise it consumes
// nothing and returns ok=false (the value is an expression). Single-quoted (and
// double-quoted) strings are rendered as ClassAd string literals.
func (p *parser) tryLiteralValue() (string, bool) {
	start := p.pos
	lit, ok := p.literalToken()
	if !ok || !(p.atPunct(",") || p.atPunct(")")) {
		p.pos = start // not a lone literal; rewind for expression capture
		return "", false
	}
	return lit, true
}

// literalToken consumes a single literal token and returns its ClassAd rendering.
func (p *parser) literalToken() (string, bool) {
	t := p.next()
	switch t.kind {
	case tString:
		return quoteClassAd(t.text), true
	case tNumber:
		return t.text, true
	case tOp:
		if (t.text == "-" || t.text == "+") && p.peek().kind == tNumber {
			return t.text + p.next().text, true
		}
	case tIdent:
		switch strings.ToUpper(t.text) {
		case "TRUE", "FALSE":
			return strings.ToLower(t.text), true
		case "NULL", "UNDEFINED":
			return "undefined", true
		}
	}
	return "", false
}

// rejectJoins produces a helpful error if a JOIN follows the table name.
func (p *parser) rejectJoins() error {
	for _, kw := range []string{"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL"} {
		if p.isKeyword(kw) {
			return fmt.Errorf("JOINs are not supported (the store is a single ClassAd collection)")
		}
	}
	if p.atPunct(",") {
		return fmt.Errorf("multiple tables / JOINs are not supported")
	}
	return nil
}

func (p *parser) peekAheadPunct(n int, s string) bool {
	if p.pos+n >= len(p.toks) {
		return false
	}
	t := p.toks[p.pos+n]
	return t.kind == tPunct && t.text == s
}

// --- helpers ---

func isAggName(up string) bool {
	switch up {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	}
	return false
}

// parseGroupCols parses the comma-separated GROUP BY column list.
func (p *parser) parseGroupCols() ([]string, error) {
	var cols []string
	for {
		id, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, id)
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	return cols, nil
}

// validateSelect enforces the SELECT/GROUP BY rules: `*` stands alone; without
// GROUP BY, aggregates cannot mix with plain columns; with GROUP BY, every plain
// column must appear in the GROUP BY list and `*` is not allowed.
func validateSelect(st *Statement) error {
	var aggs, plains int
	for _, it := range st.Items {
		switch {
		case it.IsAggregate():
			aggs++
		case !it.Star:
			plains++
		}
	}
	if len(st.GroupBy) == 0 {
		if aggs > 0 && plains > 0 {
			return fmt.Errorf("cannot mix aggregates with plain columns without GROUP BY")
		}
		return nil
	}
	// GROUP BY present.
	inGroup := map[string]bool{}
	for _, g := range st.GroupBy {
		inGroup[strings.ToLower(g)] = true
	}
	for _, it := range st.Items {
		if it.Star {
			return fmt.Errorf("`*` cannot be used with GROUP BY")
		}
		if !it.IsAggregate() && !inGroup[strings.ToLower(it.Col)] {
			return fmt.Errorf("column %q must appear in GROUP BY or be used in an aggregate", it.Col)
		}
	}
	return nil
}

// quoteClassAd renders s as a ClassAd double-quoted string literal.
func quoteClassAd(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		case '\n':
			sb.WriteString("\\n")
		case '\t':
			sb.WriteString("\\t")
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}
