package repl

// Parsing the FROM clause's tables and JOINs.
//
// The join condition is a ClassAd expression, captured verbatim exactly as WHERE is, and the
// row a join produces is itself a ClassAd whose members are the two sides under their
// aliases:
//
//	[ j = [ ClusterId = 42; Owner = "alice"; ... ];
//	  h = [ ClusterId = 42; ExitCode = 0;    ... ] ]
//
// So `j.ClusterId == h.ClusterId` needs no new evaluation rule -- it is the ClassAd select
// operator over a nested ad, which the engine already does. The whole of the rest of the
// statement (projection, WHERE, ORDER BY) is likewise evaluated against that composite ad,
// which is why none of those clauses needed a join-specific parser.

import (
	"fmt"
	"strings"
)

// JoinKind is the kind of a single JOIN clause.
type JoinKind int

const (
	JoinInner JoinKind = iota
	JoinLeft
	JoinCross
)

func (k JoinKind) String() string {
	switch k {
	case JoinLeft:
		return "LEFT JOIN"
	case JoinCross:
		return "CROSS JOIN"
	default:
		return "JOIN"
	}
}

// TableRef is one table named in a FROM clause, with the alias its columns are qualified
// by. Alias is never empty: a table named without one is referred to by its own name, so
// `FROM jobs JOIN history ON jobs.ClusterId == history.ClusterId` works unaliased.
type TableRef struct {
	Table string
	Alias string
}

// JoinClause is one joined table and the condition joining it.
type JoinClause struct {
	Kind  JoinKind
	Table string
	Alias string

	// On is the translated ClassAd join condition, over BOTH sides' aliases. Empty for a
	// CROSS JOIN, which has no condition by definition.
	On string

	// Bands are the ON clause's BETWEEN conjuncts, held apart from On rather than
	// translated into it. See BandClause.
	Bands []BandClause
}

// BandClause is one `<expr> BETWEEN <lo> AND <hi>` conjunct of a join's ON clause.
//
// BETWEEN IS A DECLARATION, WHICH IS WHY IT IS NOT TRANSLATED.
//
// Every other SQL spelling in this language becomes ClassAd text and is answered with
// ClassAd's semantics. BETWEEN cannot be, because what it declares is an ordered WINDOW over
// one attribute, and a window is narrower than the comparison it reads like: it is defined
// over numbers, so a bound or an attribute that evaluates to a string, a list or undefined
// puts the row outside the window rather than being ordered or raising an error. ClassAd's
// own `>=` orders strings against strings.
//
// So `a.X BETWEEN b.Lo AND b.Hi` is NOT stored as `a.X >= b.Lo && a.X <= b.Hi`. Writing it
// that way and recognising the window back out of the text would make the plan depend on how
// the predicate was spelled, and would leave the two spellings meaning different things on
// the same data. Held apart, there is one meaning and both join arms read it from here.
//
// It is also why there is no NOT BETWEEN: the complement of a window is not a window, so
// there is nothing to declare.
type BandClause struct {
	// Attr is the windowed expression, reading the table its JOIN clause folds in.
	Attr string
	// Lo and Hi bound the window, reading the tables joined before that one.
	Lo, Hi string
	// Raw is the conjunct as written, for errors and for EXPLAIN.
	Raw string
}

// IsJoin reports whether the statement's FROM clause names more than one table.
func (s *Statement) IsJoin() bool { return len(s.Joins) > 0 }

// tableRefs returns every table the statement reads, the FROM table first.
func (s *Statement) tableRefs() []TableRef {
	refs := make([]TableRef, 0, 1+len(s.Joins))
	if len(s.From) > 0 {
		refs = append(refs, s.From...)
	} else {
		refs = append(refs, TableRef{Table: s.Table, Alias: s.Table})
	}
	for _, j := range s.Joins {
		refs = append(refs, TableRef{Table: j.Table, Alias: j.Alias})
	}
	return refs
}

// parseTableRef parses `<name> [[AS] <alias>]` with the optional temporal clause on either
// side of the alias, and stamps any AS OF instant it finds onto st.
//
// The alias and the temporal clause both start with AS, so the two are told apart by what
// follows it: `AS OF '<ts>'` is time travel, `AS j` is an alias. Without that lookahead
// `FROM jobs AS OF '-1h'` would bind OF as the table's alias and then fail on the literal.
func (p *parser) parseTableRef(st *Statement) (TableRef, error) {
	name, err := p.parseIdent()
	if err != nil {
		return TableRef{}, err
	}
	ref := TableRef{Table: name}
	if err := p.parseAsOf(st); err != nil {
		return TableRef{}, err
	}
	if p.takeKeyword("AS") {
		alias, err := p.parseIdent()
		if err != nil {
			return TableRef{}, fmt.Errorf("AS: %w", err)
		}
		ref.Alias = alias
	} else if t := p.peek(); t.kind == tIdent && !isClauseKeyword(t.text) {
		p.pos++
		ref.Alias = t.text
	}
	if err := p.parseAsOf(st); err != nil {
		return TableRef{}, err
	}
	if ref.Alias == "" {
		ref.Alias = ref.Table
	}
	return ref, nil
}

// parseAsOf consumes a temporal clause -- SQL:2011's `FOR SYSTEM_TIME AS OF <ts>` or the
// shorter `AS OF <ts>` -- if one is next, and records the instant on st. A statement reads
// every one of its tables at the same instant, so there is one AsOf however many tables it
// names.
func (p *parser) parseAsOf(st *Statement) error {
	switch {
	case p.takeKeyword("FOR"):
		if err := p.expectKeyword("SYSTEM_TIME"); err != nil {
			return err
		}
		if err := p.expectKeyword("AS"); err != nil {
			return err
		}
		if err := p.expectKeyword("OF"); err != nil {
			return err
		}
	case p.isKeyword("AS") && p.peekAheadKeyword(1, "OF"):
		p.pos += 2
	default:
		return nil
	}
	ts, err := p.parseStringLiteral()
	if err != nil {
		return err
	}
	st.AsOf = ts
	return nil
}

// parseJoins consumes every JOIN clause following the FROM table.
func (p *parser) parseJoins(st *Statement) error {
	for {
		// A comma-separated FROM list is an implicit cross join whose condition hides in
		// the WHERE clause. Refuse it with the explicit spelling rather than supporting
		// two syntaxes for one thing.
		if p.atPunct(",") {
			return fmt.Errorf("comma-separated FROM tables are not supported; " +
				"write the join explicitly as `JOIN <table> ON <condition>`")
		}
		kind, ok, err := p.parseJoinKind()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		ref, err := p.parseTableRef(st)
		if err != nil {
			return err
		}
		jc := JoinClause{Kind: kind, Table: ref.Table, Alias: ref.Alias}
		if p.isKeyword("USING") {
			return fmt.Errorf("JOIN ... USING is not supported; name the condition with " +
				"ON, e.g. `ON a.ClusterId == b.ClusterId`")
		}
		if kind == JoinCross {
			if p.isKeyword("ON") {
				return fmt.Errorf("CROSS JOIN takes no ON clause (it pairs every row with every row)")
			}
		} else {
			if !p.takeKeyword("ON") {
				return fmt.Errorf("%s needs an ON clause", kind)
			}
			on, bands, err := p.parseJoinOn()
			if err != nil {
				return fmt.Errorf("JOIN ON: %w", err)
			}
			jc.On, jc.Bands = on, bands
		}
		st.Joins = append(st.Joins, jc)
	}
}

// parseJoinKind consumes a join's leading keywords and reports which kind it is. ok=false
// with no error means the next token does not begin a JOIN at all.
func (p *parser) parseJoinKind() (JoinKind, bool, error) {
	switch {
	case p.takeKeyword("JOIN"):
		return JoinInner, true, nil
	case p.isKeyword("INNER"):
		p.pos++
		if err := p.expectKeyword("JOIN"); err != nil {
			return 0, false, err
		}
		return JoinInner, true, nil
	case p.isKeyword("LEFT"):
		p.pos++
		p.takeKeyword("OUTER") // LEFT OUTER JOIN and LEFT JOIN are the same thing
		if err := p.expectKeyword("JOIN"); err != nil {
			return 0, false, err
		}
		return JoinLeft, true, nil
	case p.isKeyword("CROSS"):
		p.pos++
		if err := p.expectKeyword("JOIN"); err != nil {
			return 0, false, err
		}
		return JoinCross, true, nil
	case p.isKeyword("RIGHT"), p.isKeyword("FULL"):
		kw := strings.ToUpper(p.peek().text)
		return 0, false, fmt.Errorf("%s JOIN is not supported; for %s, swap the two tables "+
			"and use LEFT JOIN", kw, kw)
	case p.isKeyword("NATURAL"):
		return 0, false, fmt.Errorf("NATURAL JOIN is not supported; write the condition " +
			"explicitly with ON (it would otherwise join on whatever attribute names " +
			"happen to coincide)")
	}
	return 0, false, nil
}

// parseJoinOn consumes an ON condition, pulling its BETWEEN conjuncts out as they are read.
//
// The condition is read one top-level conjunct at a time rather than captured whole, which is
// what makes a BETWEEN reachable at all. It cannot be handled where the other SQL spellings
// are, in translateSQL, for the reason IN cannot: by the time the keyword is seen its left
// operand has already been written through to the output verbatim, and a window needs that
// operand in both of its bounds.
//
// Reading conjunct by conjunct also puts BETWEEN's own AND in the one place that can tell it
// from the spine's. SQL binds BETWEEN tighter than AND, so the AND after a lower bound
// belongs to the BETWEEN and the next one ends the conjunct, which is exactly the order this
// reads them in.
//
// What comes back is the remaining conjuncts rejoined as one ClassAd expression -- the same
// text a condition without a BETWEEN has always produced -- and the windows, separately.
func (p *parser) parseJoinOn() (string, []BandClause, error) {
	var conj []string
	var bands []BandClause
	for {
		expr, raw, err := p.captureExpr(p.atJoinConjunctEnd, nil)
		if err != nil {
			return "", nil, err
		}
		switch {
		case p.isKeyword("NOT") && p.peekAheadKeyword(1, "BETWEEN"):
			return "", nil, fmt.Errorf("NOT BETWEEN is not supported in a join condition; "+
				"the complement of a range is not a range, so write the two comparisons "+
				"instead, e.g. `%s < <lo> OR %s > <hi>`", raw, raw)
		case p.takeKeyword("BETWEEN"):
			b, berr := p.parseBandBounds(raw)
			if berr != nil {
				return "", nil, berr
			}
			b.Attr = expr
			bands = append(bands, b)
		default:
			conj = append(conj, expr)
		}
		if !p.takeKeyword("AND") {
			break
		}
	}
	return strings.Join(conj, " && "), bands, nil
}

// parseBandBounds consumes `<lo> AND <hi>` after a BETWEEN whose operand captured as raw.
func (p *parser) parseBandBounds(raw string) (BandClause, error) {
	lo, loRaw, err := p.captureExpr(p.atJoinConjunctEnd, nil)
	if err != nil {
		return BandClause{}, fmt.Errorf("BETWEEN: lower bound: %w", err)
	}
	if !p.takeKeyword("AND") {
		return BandClause{}, fmt.Errorf("BETWEEN names two bounds; `%s BETWEEN %s` has no "+
			"AND and no upper bound", raw, loRaw)
	}
	hi, hiRaw, err := p.captureExpr(p.atJoinConjunctEnd, nil)
	if err != nil {
		return BandClause{}, fmt.Errorf("BETWEEN: upper bound: %w", err)
	}
	return BandClause{
		Lo:  lo,
		Hi:  hi,
		Raw: fmt.Sprintf("%s BETWEEN %s AND %s", raw, loRaw, hiRaw),
	}, nil
}

// atJoinConjunctEnd reports whether the parser sits at the end of one top-level conjunct of
// an ON condition: where the whole condition ends, or at the AND that separates it from the
// next, or at the BETWEEN that makes it a window's operand rather than a conjunct.
func (p *parser) atJoinConjunctEnd() bool {
	return p.atJoinCondEnd() || p.isKeyword("AND") || p.isKeyword("BETWEEN") ||
		(p.isKeyword("NOT") && p.peekAheadKeyword(1, "BETWEEN"))
}

// atJoinCondEnd reports whether the parser sits at the end of an ON condition: the next
// clause, or another JOIN.
func (p *parser) atJoinCondEnd() bool {
	if p.atEnd() {
		return true
	}
	return p.isKeyword("WHERE") || p.isKeyword("GROUP") || p.isKeyword("HAVING") ||
		p.isKeyword("QUALIFY") || p.isKeyword("ORDER") || p.isKeyword("LIMIT") ||
		p.isKeyword("JOIN") || p.isKeyword("INNER") || p.isKeyword("LEFT") ||
		p.isKeyword("RIGHT") || p.isKeyword("FULL") || p.isKeyword("CROSS") ||
		p.isKeyword("NATURAL") || p.atSetOp()
}

// clauseKeywords are the words that may follow a table name without being its alias.
// `FROM jobs WHERE ...` must not read WHERE as an alias.
var clauseKeywords = map[string]bool{
	"where": true, "group": true, "having": true, "qualify": true, "order": true,
	"limit": true, "join": true, "inner": true, "left": true, "right": true,
	"full": true, "cross": true, "natural": true, "on": true, "using": true,
	"as": true, "for": true, "of": true, "since": true, "to": true, "set": true,
	"values": true, "from": true, "asc": true, "desc": true, "and": true, "or": true,
	"not": true, "memory": true,
	// Set operators are refused, but they must not be swallowed as an alias first:
	// `FROM jobs UNION ALL ...` would otherwise read UNION as the table's alias and then
	// fail on ALL, reporting nothing about the set operation.
	"union": true, "intersect": true, "except": true, "offset": true, "window": true,
	"fetch": true,
}

func isClauseKeyword(s string) bool { return clauseKeywords[strings.ToLower(s)] }

// peekAheadKeyword reports whether the token n positions ahead is the given keyword.
func (p *parser) peekAheadKeyword(n int, kw string) bool {
	t := p.peekAt(n)
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

// rejectSetOps catches a set operator trailing a statement that cannot take one (an
// INSERT, say). Between SELECTs it never fires, because parseSelectWithSetOps has already
// consumed it. Without this the parser would report "unexpected ALL after statement",
// which says nothing about what is wrong.
func (p *parser) rejectSetOps() error {
	for _, kw := range []string{"UNION", "INTERSECT", "EXCEPT"} {
		if p.isKeyword(kw) {
			return fmt.Errorf("%s combines SELECT statements; it cannot follow this one", kw)
		}
	}
	return nil
}

// validateJoinTables checks the shape of the FROM clause once it is fully parsed. The only
// rule here is that aliases are distinct; how many tables may be joined is a planner limit
// (maxJoinTables), checked where the plan is built.
func validateJoinTables(st *Statement) error {
	refs := st.tableRefs()
	seen := map[string]string{}
	for _, r := range refs {
		k := strings.ToLower(r.Alias)
		if prev, dup := seen[k]; dup {
			// Self-joins are fine, but each side needs its own alias or a qualified
			// reference could not say which one it meant.
			return fmt.Errorf("duplicate table alias %q (for %s and %s); "+
				"give each table a distinct alias, e.g. `FROM %s a JOIN %s b ON ...`",
				r.Alias, prev, r.Table, prev, r.Table)
		}
		seen[k] = r.Table
	}
	return nil
}

// peekAheadOp reports whether the token n positions ahead is the given operator.
func (p *parser) peekAheadOp(n int, op string) bool {
	t := p.peekAt(n)
	return t.kind == tOp && t.text == op
}

// validateAggArgs refuses a qualified aggregate argument outside a join.
//
// `SUM(h.Cpus)` names the Cpus attribute of the row's h member, which only exists when a
// join put it there. Over a single table the store aggregates over a bare attribute name
// and has nothing to resolve a dotted path against, so the old error -- that the aggregate
// takes an attribute name, not an expression -- is still the right one there.
func validateAggArgs(st *Statement) error {
	if st.IsJoin() {
		return nil
	}
	for _, a := range aggCallOrder(st) {
		if strings.Contains(a.Arg, ".") {
			return fmt.Errorf("%s(%s) is only meaningful in a join, where an attribute has "+
				"to say which table it comes from; over one table an aggregate takes a bare "+
				"attribute name", a.Func, a.Arg)
		}
	}
	return nil
}
