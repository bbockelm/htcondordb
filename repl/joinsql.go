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
			on, err := p.captureRawExpr(p.atJoinCondEnd)
			if err != nil {
				return fmt.Errorf("JOIN ON: %w", err)
			}
			jc.On = on
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

// validateJoinTables checks the shape of the FROM clause once it is fully parsed: aliases
// must be distinct, and v1 joins exactly two tables.
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
	if len(st.Joins) > 1 {
		return fmt.Errorf("joining %d tables is not supported yet; a statement joins at most two",
			len(refs))
	}
	return nil
}

// peekAheadOp reports whether the token n positions ahead is the given operator.
func (p *parser) peekAheadOp(n int, op string) bool {
	t := p.peekAt(n)
	return t.kind == tOp && t.text == op
}
