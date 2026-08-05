// Package repl implements a small SQL-like query language over the htcondordb
// ClassAd store and an interactive client (REPL) for it.
//
// The store is a single keyed collection of ClassAds -- there are no tables to
// join -- so the language is deliberately the join-free subset of SQL: SELECT
// (with a WHERE filter, column projection, DISTINCT, the COUNT/SUM/AVG/MIN/MAX
// aggregates, GROUP BY over one or more columns, HAVING, ORDER BY, and LIMIT), INSERT,
// UPDATE, and DELETE, plus CREATE/DROP TABLE, CREATE/DROP INDEX, and MATCH
// (matchmaking between two tables). Aggregation is evaluated server-side
// (hash-map GROUP BY). JOIN and subqueries are intentionally unsupported and
// rejected with a clear error; cross-table work is matchmaking, not a join.
//
// A GROUP BY term may be a computed key rather than an attribute: an expression, or the AS
// alias of a projected one -- SELECT CASE WHEN Memory > 4096 THEN 'big' ELSE 'small' END AS
// sz, COUNT(*) FROM jobs GROUP BY sz. The server aggregate groups by raw attribute values, so
// a computed key is grouped client-side (as time_bucket is), which means every GROUP BY term
// must also be selected.
//
// A GROUP BY term may be time_bucket(<attr>, '<width>'): it floors a unix-epoch
// attribute (e.g. QDate) to a fixed width -- '30s', '5m', '1h', '1d', '1w', or a
// bare integer number of seconds -- aligning buckets to the epoch. Selecting it
// (typically aliased AS time) turns point-in-time rows into a time series, e.g.
// SELECT time_bucket(QDate, '1h') AS time, COUNT(*) AS n FROM jobs
// GROUP BY time_bucket(QDate, '1h'). This grouping is computed client-side (the
// server aggregate groups by raw attribute value only).
//
// CASE WHEN <cond> THEN <v> [WHEN ...] [ELSE <v>] END (and the CASE <expr> WHEN <value>
// form) is accepted anywhere an expression is: it becomes the equivalent ClassAd ?: chain,
// with a missing ELSE yielding undefined (SQL's NULL).
//
// HAVING filters GROUPS after aggregation, where WHERE filters ROWS before it:
// SELECT Owner, SUM(Cpus) FROM jobs GROUP BY Owner HAVING SUM(Cpus) > 100. It accepts the
// same expressions a projected column does (aggregates, CASE, group columns), and applies
// before ORDER BY and LIMIT. With no GROUP BY it filters the single implicit group. A
// reference that is neither grouped nor aggregated is refused rather than silently evaluating
// to undefined and dropping every group.
//
// An aggregate may carry FILTER (WHERE <cond>), restricting THAT aggregate to the rows of
// its group where the condition holds -- so one pass answers several differently-conditioned
// questions:
//
//	SELECT Owner, COUNT(*) AS total,
//	       COUNT(*) FILTER (WHERE JobStatus == 2) AS running,
//	       COUNT(*) FILTER (WHERE JobStatus == 1) AS idle
//	FROM jobs GROUP BY Owner
//
// The portable spelling SUM(CASE WHEN c THEN 1 ELSE 0 END) lowers onto the same thing, as
// does SUM(CASE WHEN c THEN <attr> ELSE 0 END) and the ELSE-less form. Shapes that are not
// provably the same question (a non-zero ELSE, an arithmetic THEN, a second WHEN) are
// refused with a pointer to FILTER rather than guessed at.
//
// A projected column may be any ClassAd expression, and aggregates may appear inside one:
// SELECT 2 * COUNT(*), SELECT SUM(Cpus) / COUNT(*) AS avg, SELECT MAX(m) - MIN(m),
// SELECT COUNT(*) > 1000 ? "busy" : "quiet". The aggregates are lifted out and computed by
// the store as usual; the surrounding expression is evaluated once per group by the ClassAd
// engine, so the whole ClassAd language (?:, strcat, member(), comparisons) is available over
// aggregate results. COUNT(*) is SQL's spelling, not ClassAd's, and is accepted as such.
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
// is available; the translation only adapts SQL spellings that ClassAd has no
// reading for -- `=`, `<>`, AND/OR/NOT, IS [NOT] NULL, single-quoted strings, and
// CASE WHEN ... THEN ... ELSE ... END (see sqlexpr.go). Everything else is passed
// through byte for byte.
package repl

import (
	"fmt"
	"strconv"
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
	StmtCreateView
	StmtDropView
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

	// View fields (CREATE MATERIALIZED VIEW <ViewName> [MAXSERIES n] AS <ViewSelect>).
	// ViewSelect is the embedded SELECT (a StmtSelect) whose GROUP BY + aggregates define
	// the view; ViewMaxSeries is the hard cardinality limit (0 = use the default).
	ViewName      string
	ViewSelect    *Statement
	ViewMaxSeries int
	// Continuous-aggregate options from an optional WITH (...) clause, in seconds (0 =
	// default). ViewGrace delays sealing a time bucket after its window closes;
	// ViewRetention bounds how much sealed history the archive keeps.
	ViewGrace     int64
	ViewRetention int64

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

	// Having is the post-aggregation filter: a ClassAd expression evaluated once per
	// GROUP, over that group's aggregate results and its group columns. HavingAggs are the
	// aggregate calls lifted out of it (as for a SELECT item), which Having refers to as
	// __agg_0, __agg_1, ... continuing the numbering past the projection's own.
	//
	// WHERE filters ROWS before grouping; HAVING filters GROUPS after. Writing one where
	// the other is meant gives a different, plausible-looking answer, which is why the two
	// are kept distinct rather than folded together.
	Having     string
	HavingAggs []AggCall

	// Since is the WATCH start point: "now" (default; live changes only) or
	// "beginning" (replay the current contents, then live).
	Since string

	// AsOf, if set, is the point-in-time ("FOR SYSTEM_TIME AS OF '<ts>'") instant a
	// SELECT reads at -- a timestamp (RFC3339 / "2006-01-02 15:04:05") or a relative
	// look-back like "-1h". Empty means read the current state.
	AsOf string
}

// SelectItem is one projected column or aggregate. For "*", Star is set. For a
// plain column, Agg is "" and Col is the attribute name. For an aggregate,
// Agg is COUNT/SUM/AVG/MIN/MAX and Col is its argument ("*" for COUNT(*)). For a
// time_bucket(col, 'width') grouping expression, Bucket is set: Col is the
// unix-epoch timestamp attribute and BucketWidth is the bucket width in seconds.
type SelectItem struct {
	Star  bool
	Agg   string // "", "COUNT", "SUM", "AVG", "MIN", "MAX"
	Col   string
	Alias string // display header; defaults to the source text

	// Expr is a general ClassAd expression captured verbatim (e.g.
	// "CurrentTime - EnteredHistoryTime"), evaluated per row against each ad. It is
	// empty for a plain column (Col holds the attribute name) and for aggregates.
	Expr string

	// AggFilter is a bare aggregate's `FILTER (WHERE ...)` condition (see AggCall.Filter).
	// An aggregate inside an expression carries its own filter in Aggs instead.
	AggFilter string

	// Aggs are the aggregate calls lifted out of Expr, in source order. Expr refers to
	// each as __agg_0, __agg_1, ... so `2 * COUNT(*)` runs one COUNT per group and
	// evaluates the arithmetic over its result. Empty for a bare aggregate (Agg is set
	// instead) and for a per-row expression.
	Aggs []AggCall

	// Bucket marks a time_bucket(Col, 'width') expression -- a non-aggregate
	// grouping column that floors the epoch-seconds attribute Col to BucketWidth
	// (see parseBucketWidth). It groups a time axis into fixed-width buckets.
	Bucket      bool
	BucketWidth int64 // seconds; >0 when Bucket
}

// AggCall is one aggregate function call: COUNT/SUM/AVG/MIN/MAX over an attribute, or
// COUNT over "*". Filter, when set, is a conditional aggregate -- `FILTER (WHERE ...)`, or
// the `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` spelling lowered onto it -- restricting THIS
// aggregate to the rows of its group where the expression holds.
type AggCall struct {
	Func   string
	Arg    string
	Filter string
}

// aggPlaceholderPrefix names the attribute an aggregate is bound to when it is lifted out of
// a SELECT expression. Double-underscored so it cannot collide with an ad's own attributes.
const aggPlaceholderPrefix = "__agg_"

// IsAggregate reports whether this item is a bare aggregate call, whose value comes straight
// from the aggregate result vector.
func (it SelectItem) IsAggregate() bool { return it.Agg != "" }

// GroupLevel reports whether the item's value is computed per GROUP rather than per row --
// a bare aggregate, or an expression with aggregates lifted out of it. These are the items
// that may not be mixed with plain columns outside a GROUP BY.
func (it SelectItem) GroupLevel() bool { return it.Agg != "" || len(it.Aggs) > 0 }

// aggCalls returns the aggregates this item needs evaluated, whether it is a bare call or an
// expression over several.
func (it SelectItem) aggCalls() []AggCall {
	if it.Agg != "" {
		return []AggCall{{Func: it.Agg, Arg: it.Col, Filter: it.AggFilter}}
	}
	return it.Aggs
}

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
		h := it.Agg + "(" + it.Col + ")"
		if it.AggFilter != "" {
			h += " FILTER (WHERE " + it.AggFilter + ")"
		}
		return h
	}
	if it.Bucket {
		return "time_bucket(" + it.Col + ", " + strconv.FormatInt(it.BucketWidth, 10) + ")"
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
	// inOrderBy marks that the SELECT-item grammar is being reused for an ORDER BY term,
	// where DESC/ASC/LIMIT terminate the term (see atSelectItemEnd).
	inOrderBy bool
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
		if it.GroupLevel() {
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
	if p.takeKeyword("MATERIALIZED") {
		if err := p.expectKeyword("VIEW"); err != nil {
			return nil, fmt.Errorf("expected VIEW after MATERIALIZED")
		}
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		maxSeries := 0 // 0 => the server's default cardinality
		if p.takeKeyword("MAXSERIES") {
			t := p.next()
			if t.kind != tNumber {
				return nil, fmt.Errorf("MAXSERIES expects a number, got %q", t.text)
			}
			if _, err := fmt.Sscanf(t.text, "%d", &maxSeries); err != nil || maxSeries <= 0 {
				return nil, fmt.Errorf("invalid MAXSERIES %q", t.text)
			}
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("SELECT"); err != nil {
			return nil, fmt.Errorf("a materialized view must be defined by a SELECT")
		}
		sel, err := p.parseSelect() // reuses GROUP BY / aggregate / alias parsing
		if err != nil {
			return nil, err
		}
		grace, retention, err := p.parseViewOptions()
		if err != nil {
			return nil, err
		}
		return &Statement{Kind: StmtCreateView, ViewName: name, ViewSelect: sel, ViewMaxSeries: maxSeries,
			ViewGrace: grace, ViewRetention: retention}, nil
	}
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

// parseViewOptions parses an optional continuous-aggregate options clause after a
// materialized view's SELECT: WITH (grace = '<dur>', retention = '<dur>'). Durations use the
// time_bucket width syntax ('30s', '5m', '1h', '1d', ...). Both keys are optional and each
// defaults to 0 (grace 0 = seal at the window's close; retention 0 = keep all history).
func (p *parser) parseViewOptions() (grace, retention int64, err error) {
	if !p.takeKeyword("WITH") {
		return 0, 0, nil
	}
	if err = p.expectPunct("("); err != nil {
		return 0, 0, err
	}
	for {
		key, kerr := p.parseIdent()
		if kerr != nil {
			return 0, 0, kerr
		}
		if t := p.peek(); !(t.kind == tOp && t.text == "=") {
			return 0, 0, fmt.Errorf("expected `=` after %q in WITH (...)", key)
		}
		p.pos++ // consume '='
		val, verr := p.parseStringLiteral()
		if verr != nil {
			return 0, 0, verr
		}
		secs, derr := parseBucketWidth(val)
		if derr != nil {
			return 0, 0, fmt.Errorf("invalid %s duration %q: %w", key, val, derr)
		}
		switch strings.ToLower(key) {
		case "grace":
			grace = secs
		case "retention":
			retention = secs
		default:
			return 0, 0, fmt.Errorf("unknown view option %q (expected grace or retention)", key)
		}
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	if err = p.expectPunct(")"); err != nil {
		return 0, 0, err
	}
	return grace, retention, nil
}

// parseDrop parses DROP TABLE <name> or DROP INDEX ON <table> (<attr>, ...).
func (p *parser) parseDrop() (*Statement, error) {
	if p.takeKeyword("MATERIALIZED") {
		if err := p.expectKeyword("VIEW"); err != nil {
			return nil, fmt.Errorf("expected VIEW after MATERIALIZED")
		}
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		return &Statement{Kind: StmtDropView, ViewName: name}, nil
	}
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
	// Optional temporal clause (SQL:2011 "FOR SYSTEM_TIME AS OF <ts>", or the shorter
	// "AS OF <ts>"), right after the table name -- a point-in-time read.
	if p.takeKeyword("FOR") {
		if err := p.expectKeyword("SYSTEM_TIME"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("OF"); err != nil {
			return nil, err
		}
		if st.AsOf, err = p.parseStringLiteral(); err != nil {
			return nil, err
		}
	} else if p.takeKeyword("AS") {
		if err := p.expectKeyword("OF"); err != nil {
			return nil, err
		}
		if st.AsOf, err = p.parseStringLiteral(); err != nil {
			return nil, err
		}
	}
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
		resolveGroupAliases(st)
	}
	if p.takeKeyword("HAVING") {
		having, aggs, _, err := p.captureSelectExpr(func() bool {
			return p.atEnd() || p.isKeyword("ORDER") || p.isKeyword("LIMIT")
		})
		if err != nil {
			return nil, fmt.Errorf("HAVING: %w", err)
		}
		st.Having, st.HavingAggs = having, aggs
	}
	// Validate the projection against the (now known) GROUP BY and HAVING.
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
		// A leading non-identifier (e.g. "(a - b)", "-x", a literal) begins a general
		// expression rather than a column/aggregate; capture and evaluate it per row.
		return p.parseSelectExpr()
	}
	// Aggregate?  IDENT '(' ... ')'
	if agg := strings.ToUpper(t.text); isAggName(agg) && p.peekAheadPunct(1, "(") {
		save := p.pos
		call, err := p.parseAggCall()
		if err != nil {
			return SelectItem{}, err
		}
		// A BARE aggregate ends the item here. An OPERATOR after it -- `SUM(Cpus) /
		// COUNT(*)`, `MAX(m) - MIN(m)`, `COUNT(*) > 3 ? ...` -- means the call is only the
		// first leaf of an expression, so rewind and parse the whole item as one (lifting
		// every aggregate out of it). Testing for an operator rather than for a terminator
		// keeps ORDER BY working: `SUM(Cpus) DESC` ends at the identifier DESC.
		if p.aggCallContinues() {
			p.pos = save
			return p.parseSelectExpr()
		}
		it := SelectItem{Agg: call.Func, Col: call.Arg, AggFilter: call.Filter}
		it.Alias = p.parseOptionalAlias()
		return it, nil
	}
	// CASE ... END is an expression, not a column: route it to the expression path (a lone
	// identifier would otherwise be taken as a column name and stop at WHEN).
	if p.isKeyword("CASE") {
		return p.parseSelectExpr()
	}
	// time_bucket(attr, 'width') grouping expression.
	if strings.EqualFold(t.text, "time_bucket") && p.peekAheadPunct(1, "(") {
		attr, secs, err := p.parseBucketCall()
		if err != nil {
			return SelectItem{}, err
		}
		it := SelectItem{Bucket: true, Col: attr, BucketWidth: secs}
		it.Alias = p.parseOptionalAlias()
		return it, nil
	}
	// A lone identifier NOT followed by an operator or '(' is a plain column; an identifier
	// followed by an operator (a - b) or '(' (a function call that is not an aggregate/
	// time_bucket, handled above) is a general expression.
	if nt := p.peekAt(1); nt.kind == tOp || (nt.kind == tPunct && nt.text == "(") {
		return p.parseSelectExpr()
	}
	p.pos++
	it := SelectItem{Col: t.text}
	it.Alias = p.parseOptionalAlias()
	return it, nil
}

// parseSelectExpr captures a general ClassAd expression SELECT item (up to a top-level
// comma, FROM, or AS). Aggregate calls inside it are lifted out (see captureSelectExpr), so
// `2 * COUNT(*)` becomes the expression `2 * __agg_0` over one COUNT aggregate. Without any,
// the expression is evaluated per row as before. An alias needs the explicit AS form, since a
// trailing bare identifier would be ambiguous with the expression itself.
func (p *parser) parseSelectExpr() (SelectItem, error) {
	expr, aggs, raw, err := p.captureSelectExpr(p.atSelectItemEnd)
	if err != nil {
		return SelectItem{}, err
	}
	it := SelectItem{Expr: expr, Col: raw, Aggs: aggs}
	it.Alias = p.parseOptionalAlias()
	return it, nil
}

// aggCallContinues reports whether the token after a complete aggregate call carries it into
// a larger expression. Only an operator can: `SUM(a) / COUNT(*)`, `MAX(m) - MIN(m)`,
// `COUNT(*) > 3 ? ...`. An identifier (an AS alias, ORDER BY's DESC, FROM, LIMIT), a comma,
// or end of input ends the item and leaves a bare aggregate.
func (p *parser) aggCallContinues() bool {
	return !p.atEnd() && p.peek().kind == tOp
}

// atSelectItemEnd reports whether the parser sits at the end of a SELECT item: a comma, FROM,
// AS, or end of input. Anything else continues the current expression.
//
// In an ORDER BY the sort direction terminates the term too, so `ORDER BY 2 * COUNT(*) DESC`
// does not swallow DESC into the expression. That is only safe in ORDER BY context, where
// DESC/ASC are keywords rather than possible attribute names.
func (p *parser) atSelectItemEnd() bool {
	if p.atEnd() {
		return true
	}
	pk := p.peek()
	if p.inOrderBy && (p.isKeyword("DESC") || p.isKeyword("ASC") || p.isKeyword("LIMIT")) {
		return true
	}
	return (pk.kind == tPunct && pk.text == ",") || p.isKeyword("FROM") || p.isKeyword("AS")
}

// captureSelectExpr captures a SELECT expression and lifts every aggregate call out of it,
// returning the expression with each call replaced by a placeholder attribute (__agg_0,
// __agg_1, ... in source order), the lifted calls, and the original source text for the
// column header.
//
// The lifting happens at the token level rather than by rewriting the captured string,
// because `COUNT(*)` is not valid ClassAd syntax: handing the raw text to the ClassAd parser
// would fail before anything could be substituted. Consuming each call whole also keeps its
// parentheses out of the depth counter that decides where the item ends.
func (p *parser) captureSelectExpr(stop func() bool) (expr string, aggs []AggCall, raw string, err error) {
	lift := []AggCall{}
	expr, raw, err = p.captureExpr(stop, &lift)
	if err != nil {
		return "", nil, "", err
	}
	if len(lift) == 0 {
		lift = nil
	}
	return expr, lift, raw, nil
}

// parseAggCall consumes an aggregate call `NAME ( * | ident )` and returns it. The caller has
// already established that the next two tokens are an aggregate name and '('.
func (p *parser) parseAggCall() (AggCall, error) {
	name := strings.ToUpper(p.peek().text)
	p.pos += 2 // name + '('
	// A CASE argument is a conditional aggregate in the portable spelling; it lowers onto a
	// filtered one (see parseCaseAggCall) rather than being refused.
	if p.isKeyword("CASE") {
		return p.parseCaseAggCall(name)
	}
	var arg string
	if pk := p.peek(); pk.kind == tOp && pk.text == "*" {
		arg = "*"
		p.pos++
	} else {
		col, err := p.parseIdent()
		if err != nil {
			return AggCall{}, fmt.Errorf("%s(...): %w", name, err)
		}
		arg = col
	}
	// The store aggregates over an ATTRIBUTE, so the argument has to be a bare name: there
	// is no way to push `SUM(a + b)` down to it. Say so plainly rather than reporting a
	// missing parenthesis.
	if !p.atPunct(")") {
		return AggCall{}, fmt.Errorf("%s(...) takes an attribute name, not an expression "+
			"(for a conditional aggregate use %s(...) FILTER (WHERE ...))", name, name)
	}
	if err := p.expectPunct(")"); err != nil {
		return AggCall{}, err
	}
	if arg == "*" && name != "COUNT" {
		return AggCall{}, fmt.Errorf("%s(*) is not valid; %s needs an attribute", name, name)
	}
	call := AggCall{Func: name, Arg: arg}
	filter, err := p.parseAggFilter(name)
	if err != nil {
		return AggCall{}, err
	}
	call.Filter = filter
	return call, nil
}

// parseAggFilter parses an optional `FILTER (WHERE <expr>)` after an aggregate call, the SQL
// spelling of a conditional aggregate: the aggregate sees only the rows of its group where
// the expression holds. The expression is captured with the same SQL-to-ClassAd translation
// as a WHERE clause, so 'text', AND/OR, IS NULL and CASE all read the same way.
func (p *parser) parseAggFilter(name string) (string, error) {
	if !p.takeKeyword("FILTER") {
		return "", nil
	}
	if err := p.expectPunct("("); err != nil {
		return "", fmt.Errorf("%s(...) FILTER: expected (WHERE ...): %w", name, err)
	}
	if !p.takeKeyword("WHERE") {
		return "", fmt.Errorf("%s(...) FILTER: expected WHERE, got %q", name, p.peek().text)
	}
	expr, err := p.captureRawExpr(func() bool { return p.atEnd() || p.atPunct(")") })
	if err != nil {
		return "", fmt.Errorf("%s(...) FILTER (WHERE ...): %w", name, err)
	}
	if err := p.expectPunct(")"); err != nil {
		return "", fmt.Errorf("%s(...) FILTER (WHERE ...): %w", name, err)
	}
	return expr, nil
}

// parseCaseAggCall lowers the portable conditional-aggregate spelling onto a filtered
// aggregate:
//
//	SUM(CASE WHEN c THEN 1 ELSE 0 END)     ->  COUNT(*) FILTER (WHERE c)
//	SUM(CASE WHEN c THEN Attr ELSE 0 END)  ->  SUM(Attr) FILTER (WHERE c)
//	COUNT(CASE WHEN c THEN Attr END)       ->  COUNT(Attr) FILTER (WHERE c)
//
// Only these shapes lower, because only these are provably the same question: one WHEN arm,
// a THEN that is a bare attribute or the literal 1, and an ELSE that is absent or the
// literal 0 (SUM skips undefined, and adding zero changes nothing). Anything else -- a
// second arm, an arithmetic THEN, a non-zero ELSE -- is reported rather than guessed at,
// since a wrong lowering would return a plausible number.
func (p *parser) parseCaseAggCall(name string) (AggCall, error) {
	expr, aggs, raw, err := p.captureSelectExpr(func() bool { return p.atEnd() || p.atPunct(")") })
	if err != nil {
		return AggCall{}, fmt.Errorf("%s(CASE ...): %w", name, err)
	}
	if err := p.expectPunct(")"); err != nil {
		return AggCall{}, err
	}
	if len(aggs) > 0 {
		return AggCall{}, fmt.Errorf("%s(...): an aggregate cannot appear inside another", name)
	}
	cond, then, els, ok := simpleCaseArms(expr)
	if !ok {
		return AggCall{}, fmt.Errorf("%s(%s) is not a conditional aggregate this store can "+
			"push down; use %s(...) FILTER (WHERE ...)", name, raw, name)
	}
	if els != "" && els != "0" && els != "undefined" {
		return AggCall{}, fmt.Errorf("%s(CASE ...) lowers to a filtered aggregate only with "+
			"ELSE 0 (or no ELSE); got ELSE %s", name, els)
	}
	arg := then
	if then == "1" {
		if name != "SUM" && name != "COUNT" {
			return AggCall{}, fmt.Errorf("%s(CASE WHEN ... THEN 1 ...) is only meaningful for SUM or COUNT", name)
		}
		return AggCall{Func: "COUNT", Arg: "*", Filter: cond}, nil
	}
	if !isBareIdent(arg) {
		return AggCall{}, fmt.Errorf("%s(CASE WHEN ... THEN %s ...): THEN must be an attribute "+
			"or the literal 1; use %s(...) FILTER (WHERE ...) instead", name, then, name)
	}
	return AggCall{Func: name, Arg: arg, Filter: cond}, nil
}

// simpleCaseArms decomposes the single-arm conditional the CASE translation produces --
// `((cond) ? (then) : (else))` -- back into its parts. It works on the TRANSLATED text
// (parseCaseExpr's output shape) rather than re-parsing, so anything with a second arm
// nests another conditional in the else slot and is rejected by the callers' checks.
func simpleCaseArms(expr string) (cond, then, els string, ok bool) {
	s := strings.TrimSpace(expr)
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return "", "", "", false
	}
	s = strings.TrimSpace(s[1 : len(s)-1])
	cond, rest, ok := splitParen(s, " ? ")
	if !ok {
		return "", "", "", false
	}
	then, els, ok = splitParen(rest, " : ")
	if !ok {
		return "", "", "", false
	}
	return unwrap(cond), unwrap(then), unwrap(els), true
}

// splitParen splits s at the first occurrence of sep that sits outside parentheses.
func splitParen(s, sep string) (before, after string, ok bool) {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && strings.HasPrefix(s[i:], sep) {
			return strings.TrimSpace(s[:i+1]), strings.TrimSpace(s[i+len(sep):]), true
		}
	}
	return "", "", false
}

// unwrap strips one balanced enclosing paren pair.
func unwrap(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return s
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return s // the parens do not enclose the whole string
			}
		}
	}
	return strings.TrimSpace(s[1 : len(s)-1])
}

// isBareIdent reports whether s is a single attribute name.
func isBareIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isIdentPart(s[i]) {
			return false
		}
	}
	return !isDigit(s[0])
}

// peekAt returns the token n positions ahead (or a zero token past the end).
func (p *parser) peekAt(n int) token {
	if p.pos+n >= len(p.toks) {
		return token{}
	}
	return p.toks[p.pos+n]
}

// parseBucketCall parses "time_bucket ( <attr> , '<width>' )" starting at the
// time_bucket identifier, returning the attribute name and the width in seconds.
func (p *parser) parseBucketCall() (attr string, secs int64, err error) {
	p.pos += 2 // time_bucket + '('
	attr, err = p.parseIdent()
	if err != nil {
		return "", 0, err
	}
	if err = p.expectPunct(","); err != nil {
		return "", 0, fmt.Errorf("time_bucket(attr, 'width') requires a width argument: %w", err)
	}
	width, err := p.parseStringLiteral()
	if err != nil {
		return "", 0, err
	}
	if secs, err = parseBucketWidth(width); err != nil {
		return "", 0, err
	}
	if err = p.expectPunct(")"); err != nil {
		return "", 0, err
	}
	return attr, secs, nil
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
		return p.atEnd() || p.isKeyword("GROUP") || p.isKeyword("HAVING") ||
			p.isKeyword("ORDER") || p.isKeyword("LIMIT") || p.isKeyword("SINCE")
	})
}

// captureRawExpr captures an expression that is NOT a SELECT item -- a WHERE clause, an
// UPDATE assignment's right-hand side, a MATCH filter, an INSERT value. SQL spellings are
// translated for the ClassAd engine (see captureExpr); aggregates are not lifted, since only
// a SELECT list can contain them.
func (p *parser) captureRawExpr(stop func() bool) (string, error) {
	expr, _, err := p.captureExpr(stop, nil)
	return expr, err
}

// parseOrderBy parses "term [ASC|DESC] (, term [ASC|DESC])*". Each term is a
// column or aggregate (reusing the SELECT-item grammar).
func (p *parser) parseOrderBy() ([]OrderTerm, error) {
	p.inOrderBy = true
	defer func() { p.inOrderBy = false }()
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

// parseGroupCols parses the comma-separated GROUP BY term list. Each term is a
// plain column name or a time_bucket(attr, 'width') expression, which is stored
// as its canonical key so validateSelect can match it against a projected item.
func (p *parser) parseGroupCols() ([]string, error) {
	var cols []string
	for {
		t := p.peek()
		switch {
		case strings.EqualFold(t.text, "time_bucket") && p.peekAheadPunct(1, "("):
			attr, secs, err := p.parseBucketCall()
			if err != nil {
				return nil, err
			}
			cols = append(cols, canonicalBucketKey(attr, secs))
		case p.groupTermIsExpr():
			// A computed group key -- `GROUP BY CASE WHEN ... END`. Captured and translated
			// exactly as the matching SELECT item is, so the two texts compare equal and the
			// term can be resolved to the column it groups.
			expr, _, _, err := p.captureSelectExpr(p.atGroupTermEnd)
			if err != nil {
				return nil, fmt.Errorf("GROUP BY: %w", err)
			}
			cols = append(cols, expr)
		default:
			id, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			cols = append(cols, id)
		}
		if p.atPunct(",") {
			p.pos++
			continue
		}
		break
	}
	return cols, nil
}

// canonicalBucketKey is the case-insensitive identity of a time_bucket grouping
// term -- "time_bucket(<attr>,<seconds>)" -- so a SELECT item and a GROUP BY term
// over the same attribute and width compare equal.
func canonicalBucketKey(attr string, secs int64) string {
	return "time_bucket(" + strings.ToLower(attr) + "," + strconv.FormatInt(secs, 10) + ")"
}

// groupTermIsExpr reports whether the GROUP BY term at the cursor is a computed expression
// rather than a plain attribute name: it starts with something other than an identifier, or
// the identifier is followed by an operator or a call. A lone identifier -- possibly a SELECT
// alias -- stays a name, so it can be resolved against the projection.
func (p *parser) groupTermIsExpr() bool {
	t := p.peek()
	if t.kind != tIdent || p.isKeyword("CASE") {
		return true
	}
	nt := p.peekAt(1)
	return nt.kind == tOp || (nt.kind == tPunct && nt.text == "(")
}

// atGroupTermEnd reports whether the parser sits at the end of a GROUP BY term.
func (p *parser) atGroupTermEnd() bool {
	if p.atEnd() {
		return true
	}
	pk := p.peek()
	return (pk.kind == tPunct && pk.text == ",") || p.isKeyword("HAVING") ||
		p.isKeyword("ORDER") || p.isKeyword("LIMIT")
}

// groupItemKey is a select item's identity for GROUP BY matching: the canonical
// bucket key for a time_bucket item, else the lower-cased column name.
func groupItemKey(it SelectItem) string {
	if it.Bucket {
		return canonicalBucketKey(it.Col, it.BucketWidth)
	}
	if it.Expr != "" {
		return strings.ToLower(it.Expr) // the translated text, as parseGroupCols stores it
	}
	return strings.ToLower(it.Col)
}

// resolveGroupAliases rewrites a GROUP BY term that names a projected column's AS alias into
// the thing the alias stands for, so everything downstream sees one spelling: an attribute
// name the server can group by, or the expression the client-side grouping evaluates.
//
// Without this, `SELECT Owner AS o, COUNT(*) FROM jobs GROUP BY o` would pass validation on
// the alias and then ask the server to group by an attribute named "o" -- which no ad has,
// giving one group with an empty key and every row in it. Silently, which is the point.
func resolveGroupAliases(st *Statement) {
	for i, g := range st.GroupBy {
		for _, it := range st.Items {
			if it.GroupLevel() || it.Alias == "" || !strings.EqualFold(g, it.Alias) {
				continue
			}
			st.GroupBy[i] = groupTermFor(it)
			break
		}
	}
}

// groupTermFor is the GROUP BY spelling of a projected item.
func groupTermFor(it SelectItem) string {
	if it.Bucket {
		return canonicalBucketKey(it.Col, it.BucketWidth)
	}
	if it.Expr != "" {
		return it.Expr
	}
	return it.Col
}

// groupItemMatches reports whether a GROUP BY term names this projected item. A term may be
// the attribute name, the item's AS alias, or -- for a computed column -- the expression
// itself, so both of these say the same thing:
//
//	SELECT CASE WHEN Memory > 4096 THEN 'big' ELSE 'small' END AS sz, COUNT(*)
//	  FROM jobs GROUP BY sz
//	  FROM jobs GROUP BY CASE WHEN Memory > 4096 THEN 'big' ELSE 'small' END
func groupItemMatches(it SelectItem, term string) bool {
	t := strings.ToLower(term)
	if t == groupItemKey(it) {
		return true
	}
	return it.Alias != "" && t == strings.ToLower(it.Alias)
}

// groupByHasExpr reports whether any GROUP BY term is a computed key rather than a plain
// attribute -- an expression, or an alias standing for one. The server aggregate groups by
// raw attribute values only, so such a query is grouped client-side (as time_bucket is).
func groupByHasExpr(st *Statement) bool {
	for _, it := range st.Items {
		if it.GroupLevel() || it.Expr == "" {
			continue
		}
		for _, g := range st.GroupBy {
			if groupItemMatches(it, g) {
				return true
			}
		}
	}
	return false
}

// parseBucketWidth parses a time_bucket width literal into whole seconds. It
// accepts a bare integer (seconds) or an integer with a unit suffix: s (second),
// m (minute), h (hour), d (day), w (week) -- e.g. "30s", "5m", "1h", "1d". This
// matches Grafana's interval syntax (its $__interval expands to values like these).
func parseBucketWidth(s string) (int64, error) {
	orig := s
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty time_bucket width")
	}
	mult := int64(1)
	switch s[len(s)-1] {
	case 's', 'S':
		s = s[:len(s)-1]
	case 'm', 'M':
		mult, s = 60, s[:len(s)-1]
	case 'h', 'H':
		mult, s = 3600, s[:len(s)-1]
	case 'd', 'D':
		mult, s = 86400, s[:len(s)-1]
	case 'w', 'W':
		mult, s = 604800, s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid time_bucket width %q", orig)
	}
	return n * mult, nil
}

// validateSelect enforces the SELECT/GROUP BY rules: `*` stands alone; without
// GROUP BY, aggregates cannot mix with plain columns; with GROUP BY, every plain
// column must appear in the GROUP BY list and `*` is not allowed.
func validateSelect(st *Statement) error {
	var aggs, plains, buckets int
	for _, it := range st.Items {
		switch {
		case it.GroupLevel(): // a bare aggregate, or an expression over aggregates
			aggs++
		case it.Bucket:
			buckets++
		case !it.Star:
			plains++
		}
	}
	bucketing := buckets > 0 || groupByHasBucket(st) || groupByHasExpr(st)
	if (buckets > 0 || groupByHasBucket(st)) && len(st.GroupBy) == 0 {
		return fmt.Errorf("time_bucket(...) requires a matching GROUP BY")
	}
	if len(st.GroupBy) == 0 {
		if aggs > 0 && plains > 0 {
			return fmt.Errorf("cannot mix aggregates with plain columns without GROUP BY")
		}
		// HAVING with no GROUP BY filters the single implicit group, so a plain column
		// would have no defined value -- the same rule as mixing one with an aggregate.
		if st.Having != "" && plains > 0 {
			return fmt.Errorf("cannot use HAVING with plain columns without GROUP BY")
		}
		return validateHaving(st)
	}
	if err := validateHaving(st); err != nil {
		return err
	}
	// GROUP BY present.
	inGroup := map[string]bool{}
	for _, g := range st.GroupBy {
		inGroup[strings.ToLower(g)] = true
	}
	itemKeys := map[string]bool{}
	for _, it := range st.Items {
		if it.Star {
			return fmt.Errorf("`*` cannot be used with GROUP BY")
		}
		if it.GroupLevel() {
			continue
		}
		key := groupItemKey(it)
		matched := false
		for _, g := range st.GroupBy {
			if groupItemMatches(it, g) {
				itemKeys[strings.ToLower(g)] = true
				matched = true
			}
		}
		itemKeys[key] = true
		if !matched {
			if it.Bucket {
				return fmt.Errorf("time_bucket(%s, ...) must appear in GROUP BY", it.Col)
			}
			return fmt.Errorf("column %q must appear in GROUP BY or be used in an aggregate", it.Col)
		}
	}
	// A time_bucket query aggregates client-side over the projected group columns
	// (§ Phase 0), so every GROUP BY term must be one of them -- no grouping by a
	// column that isn't selected.
	// The client-side grouping paths (time_bucket, computed keys) group by the PROJECTED
	// non-aggregate items, so every GROUP BY term has to be one of them.
	if bucketing {
		for g := range inGroup {
			if !itemKeys[g] {
				return fmt.Errorf("GROUP BY term %q must also be selected when grouping by "+
					"time_bucket or an expression", g)
			}
		}
	}
	return nil
}

// groupByHasBucket reports whether any GROUP BY term is a time_bucket expression
// (stored as its canonical "time_bucket(...)" key).
func groupByHasBucket(st *Statement) bool {
	for _, g := range st.GroupBy {
		if strings.HasPrefix(strings.ToLower(g), "time_bucket(") {
			return true
		}
	}
	return false
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
