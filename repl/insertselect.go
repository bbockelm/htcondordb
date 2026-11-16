package repl

import (
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// INSERT ... SELECT, run on the server.
//
// The statement this serves is the roll-up: "summarise what is in that table and store the
// summary in this one", which is the ETL shape -- `INSERT INTO daily_summaries SELECT
// <day>, Owner, COUNT(*), SUM(...) FROM history GROUP BY <day>, Owner`. The scan, the
// reduction and the write all happen in the server and a row count comes back.
//
// Re-running it is the expected operation, not an accident, and it CONVERGES: the target's
// declared PRIMARY KEY derives each row's storage key from the row's own values, so the
// second run replaces the first run's rows instead of adding a second copy of each. That
// is the property the declaration buys, and it is why this refuses to run against a table
// that declares no key rather than falling back to generated ones.

// execInsertSelect answers INSERT INTO <table> [(cols)] SELECT ...
func (e *Executor) execInsertSelect(st *Statement) (*Result, error) {
	sel := st.Source
	if err := checkInsertSelectShape(st, sel); err != nil {
		return nil, err
	}
	groups, aggs, names, err := insertSelectColumns(st, sel)
	if err != nil {
		return nil, err
	}
	n, err := e.c.InsertSelect(e.opCtx(), st.Table, sel.Table, constraint(sel.Where), groups, aggs, names)
	if err != nil {
		return nil, err
	}
	return &Result{Affected: n, Note: fmt.Sprintf("INSERT %d", n)}, nil
}

// checkInsertSelectShape refuses the SELECTs this cannot carry.
//
// Each clause below is refused because the opcode has nowhere to put it, and the refusals
// are written out one at a time rather than collapsed into "unsupported SELECT" because
// the difference matters to whoever is holding the statement: ORDER BY is meaningless
// here and should simply be deleted, while HAVING changes which rows are written and
// deleting it would silently store more of them.
func checkInsertSelectShape(st, sel *Statement) error {
	switch {
	case sel.Kind != StmtSelect:
		return fmt.Errorf("INSERT ... SELECT requires a SELECT")
	case sel.Table == "":
		return fmt.Errorf("INSERT ... SELECT requires a FROM table")
	case strings.EqualFold(sel.Table, st.Table):
		// The target is read and written by one statement. Refused rather than reasoned
		// about: the roll-up's own output would be part of its input, so what it computed
		// would depend on how far it had got.
		return fmt.Errorf("INSERT ... SELECT cannot read and write the same table (%s)", st.Table)
	case len(sel.Items) == 0:
		return fmt.Errorf("INSERT ... SELECT requires a column list")
	case sel.Distinct:
		return fmt.Errorf("INSERT ... SELECT does not support DISTINCT (GROUP BY the same columns instead)")
	case sel.Having != "":
		return fmt.Errorf("INSERT ... SELECT does not support HAVING; it would change which rows are stored")
	case sel.Qualify != "":
		return fmt.Errorf("INSERT ... SELECT does not support QUALIFY")
	case len(sel.OrderBy) > 0:
		return fmt.Errorf("INSERT ... SELECT does not support ORDER BY; stored rows have no order")
	case sel.Limit > 0:
		return fmt.Errorf("INSERT ... SELECT does not support LIMIT")
	case sel.AsOf != "":
		return fmt.Errorf("INSERT ... SELECT does not support FOR SYSTEM_TIME AS OF")
	}
	// A plain row copy is a different operation from a roll-up and is deliberately not
	// served by grouping on every column: grouping COLLAPSES duplicate rows, so
	// `INSERT INTO t2 SELECT Owner, Cmd FROM t1` would store one row per distinct pair and
	// report that as success. Refusing says so instead.
	if !hasAggregate(sel) && len(effectiveGroupBy(sel)) == 0 {
		return fmt.Errorf(
			"INSERT ... SELECT runs a grouped summary and needs GROUP BY or an aggregate; " +
				"a plain row copy is not supported (grouping every column would silently " +
				"collapse duplicate rows)")
	}
	return nil
}

// insertSelectColumns maps the SELECT's items onto the opcode's layout: every group column
// first, then every aggregate, with one target attribute name per column in that same
// order.
//
// The names come from the INSERT's column list when it has one and from each item's own
// header otherwise, and either way they must be legal ClassAd attribute names. That is the
// check that catches the common mistake: `COUNT(*)` is a perfectly good column header and
// cannot be an attribute, so a SELECT whose aggregates are unaliased is refused here with
// the fix named, rather than stored as ads no query can read back.
func insertSelectColumns(st, sel *Statement) ([]dbrpc.GroupCol, []dbrpc.AggSpec, []string, error) {
	var groups []dbrpc.GroupCol
	var groupNames, aggNames []string
	var aggs []dbrpc.AggSpec

	for i, it := range sel.Items {
		name := it.Header()
		if len(st.Columns) > 0 {
			name = st.Columns[i]
		}
		switch {
		case it.Star:
			return nil, nil, nil, fmt.Errorf(
				"INSERT ... SELECT does not support *; name the columns so each one has an attribute to be stored under")
		case it.Window != "":
			return nil, nil, nil, fmt.Errorf("INSERT ... SELECT does not support window functions")
		case len(it.Aggs) > 0:
			// An expression OVER aggregates (SUM(a)/COUNT(*)): the server reduces, it does
			// not evaluate expressions over the reduction, so there is nothing to send.
			return nil, nil, nil, fmt.Errorf(
				"INSERT ... SELECT does not support an expression over aggregates (%s); "+
					"store the aggregates and compute it when reading", it.Header())
		case it.Expr != "":
			return nil, nil, nil, fmt.Errorf(
				"INSERT ... SELECT does not support computed columns (%s)", it.Header())
		}
		if err := checkInsertColumnName(name, it); err != nil {
			return nil, nil, nil, err
		}
		if it.IsAggregate() {
			aggNames = append(aggNames, name)
			continue
		}
		groups = append(groups, dbrpc.GroupCol{Attr: it.Col, BucketWidth: it.BucketWidth})
		groupNames = append(groupNames, name)
	}

	aggs = aggSpecsFor(sel)
	if len(aggs) != len(aggNames) {
		// aggSpecsFor walks the aggregate calls and this walks the items; a mismatch means
		// an item carried a shape the switch above let through, and sending the request
		// would silently pair each aggregate with the wrong name.
		return nil, nil, nil, fmt.Errorf(
			"INSERT ... SELECT: %d aggregate columns but %d aggregate calls", len(aggNames), len(aggs))
	}
	names := append(groupNames, aggNames...)
	if err := checkDistinctNames(names); err != nil {
		return nil, nil, nil, err
	}
	return groups, aggs, names, nil
}

// checkInsertColumnName refuses a column whose name cannot be a ClassAd attribute, naming
// the alias that would fix it.
func checkInsertColumnName(name string, it SelectItem) error {
	if validAttrName(name) {
		return nil
	}
	return fmt.Errorf(
		"INSERT ... SELECT: %q is not a usable attribute name; give the column an alias "+
			"(for example: %s AS <name>)", name, it.Header())
}

// checkDistinctNames refuses two columns written under one attribute.
//
// An ad holds one value per attribute, so two columns sharing a name would not both be
// stored -- the second would overwrite the first and the row would be reported as written.
func checkDistinctNames(names []string) error {
	seen := make(map[string]string, len(names))
	for _, n := range names {
		lower := strings.ToLower(n)
		if first, dup := seen[lower]; dup {
			return fmt.Errorf(
				"INSERT ... SELECT: columns %q and %q are one attribute, so only one could be stored",
				first, n)
		}
		seen[lower] = n
	}
	return nil
}

// validAttrName reports whether name is a legal ClassAd attribute name.
func validAttrName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_':
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
