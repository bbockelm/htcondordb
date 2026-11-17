package repl

// FLATTENING A DERIVED TABLE INTO THE STATEMENT THAT READS IT.
//
// `FROM (SELECT ...) d` is a RELATION, which is the thing this language had no way to hold:
// every other nested construct lowers to a fragment of the outer statement's constraint, and
// a derived table does not. But it only has to be held when something reads its rows more
// than once. A derived table that is a filter and a projection is not read at all -- it is
// the same scan the outer statement was going to run, with one more conjunct and some columns
// renamed. Merging the two is what this file does, and the merged statement is an ordinary
// single-table SELECT or JOIN that every path downstream already knows.
//
// WHY THAT IS THE WHOLE POINT RATHER THAN AN OPTIMIZATION. db.JoinSpec's sides each carry
// their own filter and projection, so
//
//	FROM jobs j JOIN (SELECT ClusterId FROM epochs WHERE Restarts > 0) d ON ...
//
// flattens into exactly the spec the planner builds for a two-table join with a WHERE on one
// side. The derived table is gone before placement is considered, so every store-side path --
// the wire join, the semi join, the reduction -- serves it with no new opcode and no rows
// crossing the wire. A derived table that could not flatten would have to be run here and
// held, and that is a later step.
//
// WHAT CANNOT FLATTEN, AND WHY EACH ONE IS REFUSED RATHER THAN APPROXIMATED. An aggregate,
// GROUP BY, DISTINCT, window function or LIMIT makes the derived table's row set depend on
// its own boundary. Conjoining the outer WHERE into it would then filter a DIFFERENT set of
// rows than the reader wrote: `(SELECT ... LIMIT 10) WHERE q` means "filter those ten", and
// the flattened form would mean "the first ten that satisfy q". Those are different answers,
// not different plans, so each is refused by name until the streaming and materialising modes
// exist to answer it properly.
//
// A bare ORDER BY with no LIMIT is the one clause that is DROPPED rather than refused. A
// derived table is a relation; the outer statement's own ordering is what survives, so
// removing it changes nothing a caller can observe, and ordering a derived table is a common
// enough habit that refusing it would be noise.

import (
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
)

// flattenDerivedTables merges every derived table in the statement's FROM clause into the
// statement itself, and reports why when one cannot be merged.
//
// It runs at parse time so that nothing downstream -- placement, EXPLAIN, the dialect
// lowering -- ever sees a derived table. A construct that is gone before planning starts
// cannot disagree with the planner about anything.
func flattenDerivedTables(st *Statement) error {
	if st == nil {
		return nil
	}
	for i := range st.From {
		if st.From[i].Derived == nil {
			continue
		}
		// The FROM table is the PRESERVED side of every join kind this language has, so a
		// row its own filter rejects is absent from the result whichever way the join goes:
		// WHERE and ON mean the same thing for it, and WHERE is where a single-table
		// statement expects to find its filter.
		if err := flattenOne(st, &st.From[i].Table, st.From[i].Alias, st.From[i].Derived,
			func(f string) { st.Where = conjoin(st.Where, f) }); err != nil {
			return err
		}
		st.From[i].Derived = nil
		// Table shadows From[0].Table for every single-table path, so the two must not
		// drift: a stale name here is a read of the wrong table, not a parse error.
		if i == 0 {
			st.Table = st.From[i].Table
		}
	}
	for i := range st.Joins {
		if st.Joins[i].Derived == nil {
			continue
		}
		j := &st.Joins[i]
		// A JOINED side's filter belongs in its ON clause, NOT in the outer WHERE.
		//
		// For an inner join the two are equivalent. For a LEFT JOIN they are not, and the
		// difference is the whole meaning of the query: the derived table's filter applies
		// to that side BEFORE the pairing, so an outer row with no surviving match still
		// appears with the side undefined. Hoisting it to WHERE would reject that row
		// instead, silently turning the LEFT JOIN into an inner one -- a wrong answer that
		// reads like a plan choice.
		sink := func(f string) { j.On = conjoin(j.On, f) }
		if j.Kind == JoinCross {
			// A CROSS JOIN takes no ON clause, and needs none: it preserves no row, so
			// filtering this side before or after the pairing gives the same set.
			sink = func(f string) { st.Where = conjoin(st.Where, f) }
		}
		if err := flattenOne(st, &j.Table, j.Alias, j.Derived, sink); err != nil {
			return err
		}
		j.Derived = nil
	}
	return nil
}

// flattenOne merges a single derived table, rewriting the outer statement in place.
//
// placeFilter receives the derived table's own WHERE, already qualified; where that lands is
// the caller's decision and is not the same for every position (see flattenDerivedTables).
func flattenOne(st *Statement, table *string, alias string, inner *Statement, placeFilter func(string)) error {
	if why := unflattenableWhy(inner); why != "" {
		return fmt.Errorf("a derived table that %s is not supported yet; "+
			"run it as its own statement, or define it as a materialized view and select "+
			"from that", why)
	}
	cols, star, err := derivedColumns(inner)
	if err != nil {
		return err
	}
	// The outer statement refers to the derived table's columns; the merged statement refers
	// to the inner table's attributes. Every clause that can name a column is rewritten
	// through that map, and a name the derived table does not expose is an error here rather
	// than an undefined value at evaluation time.
	qualified := len(st.Joins) > 0
	if err := rewriteOuterRefs(st, alias, cols, star, qualified); err != nil {
		return err
	}
	*table = inner.Table
	// The inner WHERE names the inner table's attributes bare. In a join every reference has
	// to say which side it reads, so it is qualified with the alias the derived table had.
	where := inner.Where
	if qualified && where != "" {
		where, err = qualifyBareRefs(where, alias)
		if err != nil {
			return fmt.Errorf("a derived table's WHERE clause: %w", err)
		}
	}
	if where != "" {
		placeFilter(where)
	}
	return nil
}

// unflattenableWhy names the clause that stops a derived table merging, and "" when none does.
//
// Every case here is a statement whose ROW SET depends on its own boundary, so the outer
// statement's filter cannot simply be conjoined into it. They are named individually because
// the remedy differs: a grouped derived table becomes a materialized view, while a LIMIT is
// usually a habit the reader can drop.
func unflattenableWhy(inner *Statement) string {
	switch {
	case hasAggregate(inner):
		return "aggregates"
	case len(inner.GroupBy) > 0:
		return "has a GROUP BY"
	case inner.Distinct:
		return "is DISTINCT"
	case hasWindow(inner):
		return "has a window function"
	case inner.Limit > 0:
		return "has a LIMIT"
	case inner.IsSetOp():
		return "is a UNION"
	case len(inner.Joins) > 0:
		return "is itself a join"
	case len(inner.Subqueries) > 0:
		return "carries a subquery predicate"
	case inner.AsOf != "":
		return "reads AS OF an instant"
	}
	return ""
}

// derivedColumns maps each column the derived table exposes to the inner attribute it reads.
//
// star reports `SELECT *`, which exposes whatever the inner table carries and so needs no map:
// a reference through it is already the attribute's own name.
func derivedColumns(inner *Statement) (cols map[string]string, star bool, err error) {
	if isStarSelect(inner) {
		return nil, true, nil
	}
	cols = make(map[string]string, len(inner.Items))
	for _, it := range inner.Items {
		if it.Star {
			return nil, false, fmt.Errorf("a derived table mixing `*` with named columns is " +
				"not supported; name every column it should expose")
		}
		if it.Expr != "" || it.Bucket || it.Window != "" {
			return nil, false, fmt.Errorf("a derived table selecting an expression is not " +
				"supported yet; select the attributes it reads and write the expression in " +
				"the outer statement")
		}
		// Alias defaults to the source text, which for a plain column IS the attribute name.
		name := it.Alias
		if name == "" {
			name = it.Col
		}
		if _, dup := cols[strings.ToLower(name)]; dup {
			return nil, false, fmt.Errorf("a derived table exposes %q twice; "+
				"give one of them a distinct AS name", name)
		}
		cols[strings.ToLower(name)] = it.Col
	}
	return cols, false, nil
}

// rewriteOuterRefs rewrites every clause of the outer statement that can name a column of the
// derived table.
func rewriteOuterRefs(st *Statement, alias string, cols map[string]string, star, qualified bool) error {
	rw := func(src string) (string, error) {
		if src == "" {
			return src, nil
		}
		return rewriteDerivedRefs(src, alias, cols, star, qualified)
	}
	var err error
	if st.Where, err = rw(st.Where); err != nil {
		return err
	}
	rwAgg := func(a *AggCall) error {
		if a.Arg != "" && a.Arg != "*" {
			mapped, ok := mapColumn(a.Arg, alias, cols, star, qualified)
			if !ok {
				return unknownDerivedColumn(a.Arg, alias)
			}
			a.Arg = mapped
		}
		var ferr error
		a.Filter, ferr = rw(a.Filter)
		return ferr
	}
	rwItem := func(it *SelectItem) error {
		// `*` is COUNT(*)'s argument, not a column: it names every row rather than any
		// attribute, so there is nothing to rename and nothing to check it against.
		if it.Col != "" && it.Col != "*" && !it.Star {
			mapped, ok := mapColumn(it.Col, alias, cols, star, qualified)
			if !ok {
				return unknownDerivedColumn(it.Col, alias)
			}
			it.Col = mapped
		}
		var ierr error
		if it.Expr, ierr = rw(it.Expr); ierr != nil {
			return ierr
		}
		if it.AggFilter, ierr = rw(it.AggFilter); ierr != nil {
			return ierr
		}
		for i := range it.Aggs {
			if err := rwAgg(&it.Aggs[i]); err != nil {
				return err
			}
		}
		return nil
	}
	for i := range st.Items {
		if err := rwItem(&st.Items[i]); err != nil {
			return err
		}
	}
	// An ORDER BY term carries a whole SelectItem, so it is rewritten the same way: an
	// ordering that names a derived column must follow the rename too, or it orders by an
	// attribute the merged statement no longer has.
	for i := range st.OrderBy {
		if err := rwItem(&st.OrderBy[i].Item); err != nil {
			return err
		}
	}
	for i := range st.GroupBy {
		if mapped, ok := mapColumn(st.GroupBy[i], alias, cols, star, qualified); ok {
			st.GroupBy[i] = mapped
		} else {
			return unknownDerivedColumn(st.GroupBy[i], alias)
		}
	}
	// HAVING filters groups over the same columns, so it follows the rename too.
	if st.Having, err = rw(st.Having); err != nil {
		return err
	}
	for i := range st.HavingAggs {
		if err := rwAgg(&st.HavingAggs[i]); err != nil {
			return err
		}
	}
	for i := range st.Joins {
		if st.Joins[i].On, err = rw(st.Joins[i].On); err != nil {
			return err
		}
	}
	return nil
}

// mapColumn resolves one column name written against the derived table.
//
// A QUALIFIED name is only this derived table's when the qualifier is its alias. In a join
// the same clause also carries the other sides' columns -- `SELECT j.Owner` beside a derived
// `d` -- and stripping the qualifier before looking the name up would hunt for `Owner` among
// `d`'s columns and reject a reference that was never `d`'s to resolve.
//
// A BARE name is this derived table's only when it is the statement's sole source. In a join
// a bare reference names no side, which the join validator already refuses with a better
// message than this could give, so it passes through untouched.
func mapColumn(name, alias string, cols map[string]string, star, inJoin bool) (string, bool) {
	if name == "" || star {
		return name, true
	}
	if i := strings.LastIndex(name, "."); i >= 0 {
		qual, bare := name[:i], name[i+1:]
		if !strings.EqualFold(qual, alias) {
			return name, true // another table's column
		}
		attr, ok := cols[strings.ToLower(bare)]
		if !ok {
			return "", false
		}
		return qual + "." + attr, true
	}
	if inJoin {
		return name, true
	}
	if attr, ok := cols[strings.ToLower(name)]; ok {
		return attr, true
	}
	// A magic name is not a column of anything and passes through untouched.
	if classad.IsCurrentTimeName(name) {
		return name, true
	}
	return "", false
}

func unknownDerivedColumn(name, alias string) error {
	return fmt.Errorf("%q is not a column of the derived table %q; "+
		"a derived table exposes only the columns it selects", name, alias)
}

// rewriteDerivedRefs rewrites the references in one expression.
//
// Two spellings reach here and both must be handled: `d.col`, which parses as a SelectExpr
// over the alias, and a bare `col`, which is how a single-table outer statement names a
// column. The second is why star is consulted -- with `SELECT *` every bare name is already
// the attribute's own name and nothing should change.
func rewriteDerivedRefs(src, alias string, cols map[string]string, star, qualified bool) (string, error) {
	q, err := vm.Parse(src)
	if err != nil {
		// Not this file's error to report: the clause is parsed for real elsewhere, and a
		// syntax error there names the clause the reader wrote.
		return src, nil
	}
	var bad error
	out := rewriteExpr(q.Expr(), alias, cols, star, qualified, &bad)
	if bad != nil {
		return "", bad
	}
	return out.String(), nil
}

func rewriteExpr(e ast.Expr, alias string, cols map[string]string, star, qualified bool, bad *error) ast.Expr {
	switch n := e.(type) {
	case nil:
		return nil
	case *ast.SelectExpr:
		if base, ok := unwrapParens(n.Record).(*ast.AttributeReference); ok &&
			base.Scope == ast.NoScope && strings.EqualFold(base.Name, alias) {
			if star {
				return n
			}
			attr, found := cols[strings.ToLower(n.Attr)]
			if !found {
				if *bad == nil {
					*bad = unknownDerivedColumn(n.Attr, alias)
				}
				return n
			}
			if qualified {
				return &ast.SelectExpr{Record: base, Attr: attr}
			}
			return &ast.AttributeReference{Name: attr, Scope: ast.NoScope}
		}
		return &ast.SelectExpr{
			Record: rewriteExpr(n.Record, alias, cols, star, qualified, bad),
			Attr:   n.Attr,
		}
	case *ast.AttributeReference:
		// A bare name in a single-table statement is a column of the derived table. In a
		// join it is not: every reference there names its side, and an unqualified one is
		// already refused by the join validator with a better message than this could give.
		if qualified || star || n.Scope != ast.NoScope || classad.IsCurrentTimeName(n.Name) {
			return n
		}
		if attr, found := cols[strings.ToLower(n.Name)]; found {
			return &ast.AttributeReference{Name: attr, Scope: ast.NoScope}
		}
		// A placeholder stands for an aggregate or a subquery result that this statement
		// binds itself; it is not read from any table and so is not the derived table's to
		// expose.
		if strings.HasPrefix(n.Name, aggPlaceholderPrefix) ||
			strings.HasPrefix(n.Name, scalarPlaceholderPrefix) {
			return n
		}
		if *bad == nil {
			*bad = unknownDerivedColumn(n.Name, alias)
		}
		return n
	}
	return rewriteChildren(e, bad, func(c ast.Expr) ast.Expr {
		return rewriteExpr(c, alias, cols, star, qualified, bad)
	})
}

// rewriteChildren rebuilds a container node with each child passed through f.
//
// It covers the same node kinds stripExpr does, with one deliberate difference: an unknown
// kind is an ERROR here rather than a decline. stripExpr is choosing whether to push a filter
// down and may always answer "no"; this is rewriting references the merged statement depends
// on, so a node it cannot walk means a reference it may have missed, and silently returning
// the original would produce a query that reads the wrong attribute.
func rewriteChildren(e ast.Expr, bad *error, f func(ast.Expr) ast.Expr) ast.Expr {
	switch n := e.(type) {
	case *ast.BinaryOp:
		return &ast.BinaryOp{Op: n.Op, Left: f(n.Left), Right: f(n.Right)}
	case *ast.UnaryOp:
		return &ast.UnaryOp{Op: n.Op, Expr: f(n.Expr)}
	case *ast.ParenExpr:
		return &ast.ParenExpr{Inner: f(n.Inner)}
	case *ast.ConditionalExpr:
		return &ast.ConditionalExpr{
			Condition: f(n.Condition), TrueExpr: f(n.TrueExpr), FalseExpr: f(n.FalseExpr)}
	case *ast.ElvisExpr:
		return &ast.ElvisExpr{Left: f(n.Left), Right: f(n.Right)}
	case *ast.FunctionCall:
		args := make([]ast.Expr, len(n.Args))
		for i, a := range n.Args {
			args[i] = f(a)
		}
		return &ast.FunctionCall{Name: n.Name, Args: args}
	case *ast.ListLiteral:
		els := make([]ast.Expr, len(n.Elements))
		for i, el := range n.Elements {
			els[i] = f(el)
		}
		return &ast.ListLiteral{Elements: els}
	case *ast.SubscriptExpr:
		return &ast.SubscriptExpr{Container: f(n.Container), Index: f(n.Index)}
	case *ast.IntegerLiteral, *ast.RealLiteral, *ast.StringLiteral, *ast.BooleanLiteral,
		*ast.UndefinedLiteral, *ast.ErrorLiteral:
		return e
	}
	if *bad == nil {
		*bad = fmt.Errorf("a derived table whose outer statement contains %T is not supported", e)
	}
	return e
}

// qualifyBareRefs prefixes every bare attribute reference with an alias, so an expression
// written against one table reads correctly as one side of a join.
func qualifyBareRefs(src, alias string) (string, error) {
	q, err := vm.Parse(src)
	if err != nil {
		return "", err
	}
	base := &ast.AttributeReference{Name: alias, Scope: ast.NoScope}
	var werr error
	var walk func(ast.Expr) ast.Expr
	walk = func(e ast.Expr) ast.Expr {
		switch n := e.(type) {
		case nil:
			return nil
		case *ast.AttributeReference:
			if n.Scope != ast.NoScope || classad.IsCurrentTimeName(n.Name) {
				return n
			}
			return &ast.SelectExpr{Record: base, Attr: n.Name}
		case *ast.SelectExpr:
			// Already qualified: leave the whole reference alone rather than qualifying
			// its base a second time.
			if b, ok := unwrapParens(n.Record).(*ast.AttributeReference); ok && b.Scope == ast.NoScope {
				return n
			}
			return &ast.SelectExpr{Record: walk(n.Record), Attr: n.Attr}
		}
		return rewriteChildren(e, &werr, walk)
	}
	out := walk(q.Expr())
	if werr != nil {
		return "", werr
	}
	return out.String(), nil
}

// conjoin joins two predicate texts with &&, parenthesising each so that a disjunction in
// either one keeps its own precedence.
func conjoin(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return "(" + a + ") && (" + b + ")"
}
