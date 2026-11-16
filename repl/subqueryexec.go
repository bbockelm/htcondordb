package repl

// RUNNING A SUBQUERY PREDICATE.
//
// All four spellings are one question. `x IN (SELECT y FROM t WHERE p)` asks exactly what
// `EXISTS (SELECT 1 FROM t WHERE p AND t.y == x)` asks, and the NOT forms are the same
// question answered the other way round, so this lowers all of them to ONE shape:
//
//	a table, a filter on it, and an equality joining one of its attributes to an
//	expression over the outer row -- kept (EXISTS, IN) or dropped (NOT EXISTS, NOT IN)
//
// That is a SEMI JOIN, and the store has one. What runs here is the other arm: collect the
// inner side's key values and give the outer scan a membership constraint over them. Both
// arms answer the same rows, which is what TestSubqueryArmsAgree holds them to.
//
// WHY THE KEY-SET ARM READS VALUES AND NOT ADS. The only thing wanted from the inner table is
// one column, so the read is QueryValuesStream: the server evaluates the attribute and ships
// the results column-major, typed. Nothing on this side decodes a record, builds a ClassAd or
// parses text -- which matters because this arm is the one that runs when the inner side is
// large enough to be worth worrying about.
//
// UNDEFINED IS NOT A MEMBER, and both arms agree on that by construction rather than by
// choice. literalOf has no literal for undefined, so an inner row whose key is undefined
// contributes nothing to the set; on the join arm a ClassAd equality against undefined does
// not hold, so such a row matches nothing. The visible consequence is that `NOT IN` over a
// set containing an undefined KEEPS the row, where SQL's three-valued logic would drop every
// row. ClassAd's reading is the one this language has everywhere else, and it is the only one
// the two arms can both produce.

import (
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// defaultMaxInListValues bounds the key set the membership arm will inline.
//
// It is a guard, not a tuned crossover: past it the constraint stops being worth building,
// because `member(k, {...})` folds to a single index probe whose candidate estimate runs over
// as the set grows -- on the archive measured in the store's batchlookup_test.go, 1024 keys
// are estimated at the whole collection where the truth is 8.5% -- and the read degrades to a
// full scan carrying a very large constraint. The store's semi join is what answers a set
// this size; until this cap is measured against it, it is deliberately low.
var defaultMaxInListValues = 1024

// subPlan is one subquery predicate reduced to the shape both arms run.
type subPlan struct {
	sub Subquery

	// innerTable is the table the subquery reads, and innerWhere the conjuncts of its WHERE
	// that read only that table -- the filter pushed into its scan.
	innerTable string
	innerAlias string
	innerWhere string

	// outerWhere are the conjuncts of the subquery's WHERE that read only the OUTER row.
	// They do not depend on the inner table at all, so they filter the outer scan.
	outerWhere string

	// outerKey and innerKey are the two halves of the correlating equality, each written
	// against its own table with no alias qualifier. Both are empty for an uncorrelated
	// EXISTS, which no equality joins to the outer row.
	outerKey string
	innerKey string
}

// correlated reports whether this predicate depends on the outer row.
func (sp *subPlan) correlated() bool { return sp.innerKey != "" }

// planSubqueries reduces every subquery predicate of a statement, refusing the shapes
// neither arm can run.
func planSubqueries(st *Statement) ([]*subPlan, error) {
	plans := make([]*subPlan, 0, len(st.Subqueries))
	for _, sub := range st.Subqueries {
		sp, err := planSubquery(st, sub)
		if err != nil {
			return nil, err
		}
		plans = append(plans, sp)
	}
	return plans, nil
}

// planSubquery reduces one predicate.
func planSubquery(st *Statement, sub Subquery) (*subPlan, error) {
	inner := sub.Inner
	if err := refuseUnsupportedInner(sub); err != nil {
		return nil, err
	}
	outerAlias := st.Table
	if len(st.From) > 0 {
		outerAlias = st.From[0].Alias
	}
	innerAlias := inner.Table
	if len(inner.From) > 0 {
		innerAlias = inner.From[0].Alias
	}
	if strings.EqualFold(outerAlias, innerAlias) {
		return nil, fmt.Errorf("the subquery's table is named %q, the same as the outer "+
			"query's, so a qualified reference could not say which one it means; give one "+
			"of them an alias, e.g. `FROM %s s`", innerAlias, inner.Table)
	}
	sp := &subPlan{sub: sub, innerTable: inner.Table, innerAlias: innerAlias}

	// Side 0 is the outer row, side 1 the subquery's own table. An UNQUALIFIED reference
	// reads side 1: inside the subquery a bare name is its own table's attribute, always.
	// SQL would resolve one outward when the inner table lacks it, which this does not do --
	// ClassAd rows are heterogeneous, so "the inner table lacks it" is not a question with
	// an answer, and a typo would silently become a correlation.
	aliases := map[string]int{
		strings.ToLower(outerAlias): 0,
		strings.ToLower(innerAlias): 1,
	}
	conj, err := conjunctsOf(inner.Where)
	if err != nil {
		return nil, fmt.Errorf("%s: the subquery's WHERE clause did not parse: %w", sub.Kind, err)
	}
	var innerTerms, outerTerms []string
	for _, c := range conj {
		switch mask := sidesOf(c, aliases); mask {
		case sideBit(0):
			// Reads only the outer row: it filters the outer scan, not the inner one.
			s := stripSide(c, aliases, 0)
			if s == "" {
				return nil, refuseSubqueryTerm(sub, c, "it reads the outer row in a way "+
					"this cannot rewrite into a constraint on it")
			}
			outerTerms = append(outerTerms, s)
		case sideBit(1), sideNone:
			// Reads only the subquery's table, or nothing at all (a constant).
			s := stripSide(c, aliases, 1)
			if s == "" {
				return nil, refuseSubqueryTerm(sub, c, "it could not be rewritten into a "+
					"constraint on "+inner.Table)
			}
			innerTerms = append(innerTerms, s)
		default:
			// Reads both: the only form either arm can run is an equality whose halves
			// each read one side.
			l, r, ok := splitCorrelatingEquality(c, aliases)
			if !ok {
				return nil, refuseSubqueryTerm(sub, c, "it relates the two tables by "+
					"something other than an equality, which would pair every row with "+
					"every row")
			}
			if sp.innerKey != "" {
				return nil, fmt.Errorf("%s: the subquery correlates on more than one "+
					"equality, which is not supported yet; correlate on one attribute",
					sub.Kind)
			}
			sp.outerKey, sp.innerKey = l, r
		}
	}
	sp.innerWhere = strings.Join(innerTerms, " && ")
	sp.outerWhere = strings.Join(outerTerms, " && ")

	// IN carries its equality in the syntax rather than in the WHERE clause: the left
	// operand against the subquery's single projected column.
	if sub.Kind == SubqueryIn || sub.Kind == SubqueryNotIn {
		col, cerr := inListColumn(sub)
		if cerr != nil {
			return nil, cerr
		}
		if sp.innerKey != "" {
			return nil, fmt.Errorf("%s: the subquery both selects a column to match and "+
				"correlates on an equality in its WHERE; write one or the other", sub.Kind)
		}
		sp.outerKey, sp.innerKey = sub.Expr, col
	}
	return sp, nil
}

// refuseUnsupportedInner rejects an inner SELECT that is more than a filtered scan.
//
// Each of these is a shape whose meaning inside a membership test is not the obvious one --
// a LIMIT changes WHICH rows the set is drawn from, an aggregate makes the subquery one row
// rather than a set -- so they are refused rather than answered as though the clause were
// not there.
func refuseUnsupportedInner(sub Subquery) error {
	in := sub.Inner
	switch {
	case in.IsSetOp():
		return fmt.Errorf("%s: a subquery combining SELECTs with UNION is not supported", sub.Kind)
	case in.IsJoin():
		return fmt.Errorf("%s: a subquery that joins tables is not supported; the subquery "+
			"reads one table", sub.Kind)
	case len(in.Subqueries) > 0:
		return fmt.Errorf("%s: a subquery inside a subquery is not supported", sub.Kind)
	case len(in.GroupBy) > 0 || hasAggregate(in) || in.Having != "":
		return fmt.Errorf("%s: an aggregate subquery is not supported; %s asks whether a "+
			"row matches, not what the matching rows add up to", sub.Kind, sub.Kind)
	case in.Limit > 0:
		return fmt.Errorf("%s: LIMIT inside a subquery is not supported; it would decide "+
			"WHICH rows the predicate is drawn from, which no order makes well defined", sub.Kind)
	case len(in.OrderBy) > 0:
		return fmt.Errorf("%s: ORDER BY inside a subquery is not supported; a predicate "+
			"asks what the subquery matches, not in what order", sub.Kind)
	case in.AsOf != "":
		return fmt.Errorf("%s: a subquery cannot name its own AS OF instant; a statement "+
			"reads every table at one instant", sub.Kind)
	case in.Qualify != "":
		return fmt.Errorf("%s: QUALIFY inside a subquery is not supported", sub.Kind)
	}
	return nil
}

// inListColumn returns the attribute an IN subquery's single projected column names.
func inListColumn(sub Subquery) (string, error) {
	items := sub.Inner.Items
	if len(items) != 1 {
		return "", fmt.Errorf("%s: the subquery must select exactly one column to match "+
			"against, and this one selects %d", sub.Kind, len(items))
	}
	it := items[0]
	switch {
	case it.Star:
		return "", fmt.Errorf("%s: the subquery must select the one column to match "+
			"against, not `*`", sub.Kind)
	case it.Window != "" || it.Bucket || it.GroupLevel():
		return "", fmt.Errorf("%s: the subquery must select a plain column to match "+
			"against", sub.Kind)
	case it.Col == "" || it.Expr != "":
		return "", fmt.Errorf("%s: the subquery must select a plain attribute to match "+
			"against, not an expression", sub.Kind)
	}
	return it.Col, nil
}

// splitCorrelatingEquality takes an `==` reading both sides apart into the half written
// against the outer row and the half written against the subquery's table, each stripped of
// its alias. ok is false for anything else.
func splitCorrelatingEquality(e ast.Expr, aliases map[string]int) (outer, inner string, ok bool) {
	bin, isBin := unwrapParens(e).(*ast.BinaryOp)
	if !isBin || bin.Op != "==" {
		return "", "", false
	}
	lm, rm := sidesOf(bin.Left, aliases), sidesOf(bin.Right, aliases)
	switch {
	case lm == sideBit(0) && rm == sideBit(1):
		outer, inner = stripSide(bin.Left, aliases, 0), stripSide(bin.Right, aliases, 1)
	case lm == sideBit(1) && rm == sideBit(0):
		outer, inner = stripSide(bin.Right, aliases, 0), stripSide(bin.Left, aliases, 1)
	default:
		return "", "", false
	}
	if outer == "" || inner == "" {
		return "", "", false
	}
	return outer, inner, true
}

// refuseSubqueryTerm reports a conjunct of a subquery's WHERE that neither arm can run.
func refuseSubqueryTerm(sub Subquery, c ast.Expr, why string) error {
	return fmt.Errorf("%s: the subquery's condition `%s` is not supported: %s", sub.Kind, c, why)
}

// resolveSubqueries runs every subquery predicate of a statement and folds the answers into
// its WHERE clause, so that what follows is an ordinary single-table read.
//
// It MUTATES the statement, which is what lets every path downstream -- the scan, the
// aggregate, EXPLAIN, a set operation's branch -- see one constraint without knowing a
// subquery was ever there.
func (e *Executor) resolveSubqueries(st *Statement) error {
	if len(st.Subqueries) == 0 {
		return nil
	}
	plans, err := planSubqueries(st)
	if err != nil {
		return err
	}
	terms := make([]string, 0, len(plans)+1)
	if st.Where != "" {
		terms = append(terms, st.Where)
	}
	for _, sp := range plans {
		term, err := e.subqueryTerm(sp)
		if err != nil {
			return err
		}
		if term == "" {
			continue // the predicate holds for every row; it adds no constraint
		}
		terms = append(terms, term)
		if sp.outerWhere != "" {
			terms = append(terms, sp.outerWhere)
		}
	}
	st.Where = strings.Join(terms, " && ")
	st.Subqueries = nil
	return nil
}

// subqueryTerm answers one predicate as a constraint over the outer row. It returns "" when
// the predicate holds for every row and so constrains nothing.
func (e *Executor) subqueryTerm(sp *subPlan) (string, error) {
	if !sp.correlated() {
		return e.uncorrelatedTerm(sp)
	}
	lits, err := e.innerKeyLiterals(sp)
	if err != nil {
		return "", err
	}
	// An empty key set is not a degenerate case to pass through: `member(x, {})` and its
	// negation would answer correctly, but saying so outright keeps the answer from
	// depending on how the engine reads an empty list.
	if len(lits) == 0 {
		if sp.sub.Kind.negated() {
			return "", nil // nothing to exclude: every row survives
		}
		return "false", nil
	}
	term := fmt.Sprintf("member(%s, {%s})", sp.outerKey, strings.Join(lits, ", "))
	if sp.sub.Kind.negated() {
		term = "!" + term
	}
	return term, nil
}

// uncorrelatedTerm answers a predicate that does not depend on the outer row at all. It is a
// constant: the subquery either matches something or it does not, and one row settles it.
func (e *Executor) uncorrelatedTerm(sp *subPlan) (string, error) {
	rows, err := e.c.QueryTable(e.opCtx(), sp.innerTable, constraint(sp.innerWhere), 1)
	if err != nil {
		return "", fmt.Errorf("%s over %s: %w", sp.sub.Kind, sp.innerTable, err)
	}
	if (len(rows) > 0) != sp.sub.Kind.negated() {
		return "", nil // holds for every row
	}
	return "false", nil
}

// innerKeyLiterals reads the subquery's key column and renders each distinct value as the
// ClassAd literal naming it.
//
// The read is a typed VALUE stream, not a record stream: the server evaluates the one
// attribute and ships the results column-major, so nothing here decodes a record or builds a
// ClassAd. Values with no literal -- undefined, error, a list, a nested ad -- are dropped,
// which is the same answer the join arm gives them (see the file comment).
func (e *Executor) innerKeyLiterals(sp *subPlan) ([]string, error) {
	max := e.maxInListValues()
	seen := make(map[string]bool)
	var lits []string
	var over bool
	var buf []classad.Value
	err := e.c.QueryValuesStream(e.opCtx(), sp.innerTable, constraint(sp.innerWhere),
		[]string{sp.innerKey}, 0, false, func(b *dbrpc.ValueBatch) bool {
			buf = b.Column(0, buf[:0])
			for _, v := range buf {
				lit, ok := literalOf(v)
				if !ok || seen[lit] {
					continue
				}
				if len(lits) >= max {
					over = true
					return false
				}
				seen[lit] = true
				lits = append(lits, lit)
			}
			return true
		})
	if err != nil {
		return nil, fmt.Errorf("%s over %s: %w", sp.sub.Kind, sp.innerTable, err)
	}
	if over {
		return nil, fmt.Errorf("%s over %s matches more than %d distinct %s values, which is "+
			"too many to answer by inlining them into the outer query's constraint; narrow "+
			"the subquery's WHERE clause", sp.sub.Kind, sp.innerTable, max, sp.innerKey)
	}
	return lits, nil
}

// maxInListValues is the cap this session inlines up to.
func (e *Executor) maxInListValues() int {
	if e.maxInList > 0 {
		return e.maxInList
	}
	return defaultMaxInListValues
}
