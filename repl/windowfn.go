package repl

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
)

// Ranking window functions: ROW_NUMBER(), RANK() and DENSE_RANK() over
// PARTITION BY ... ORDER BY ....
//
// This is the one window family with no comfortable alternative here. A running total is a
// Grafana transform or a continuous-aggregate view; LAG/LEAD over a time series is a
// datasource-level rate(). But "the five most recent jobs per owner" otherwise means one
// query per owner, or fetching everything and sorting by hand:
//
//	SELECT Owner, ClusterId, ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate DESC) AS n
//	FROM jobs QUALIFY n <= 5
//
// A window is computed over the whole matched row set, so unlike HAVING or a per-aggregate
// FILTER -- which work on already-small grouped output -- it materializes every matching row.
// That is why LIMIT is not pushed to the server for a windowed query (see execSelectWindow):
// a server-side cap would truncate the rows before they were ranked, and the ranking would be
// of an arbitrary prefix. Constrain a windowed query with WHERE.
//
// No frame clause and no aggregate windows (SUM(x) OVER (...)): those are the parts that pull
// in the machinery this deliberately avoids.

// window function names, as written.
const (
	winRowNumber = "ROW_NUMBER"
	winRank      = "RANK"
	winDenseRank = "DENSE_RANK"
)

func isWindowName(up string) bool {
	switch up {
	case winRowNumber, winRank, winDenseRank:
		return true
	}
	return false
}

// hasWindow reports whether any projected item is a window function.
func hasWindow(st *Statement) bool {
	for _, it := range st.Items {
		if it.Window != "" {
			return true
		}
	}
	return false
}

// parseWindowItem parses `<FUNC>() OVER (PARTITION BY ... ORDER BY ...)`. The caller has
// established that the current token is a window function name followed by '('.
//
// ORDER BY inside OVER is REQUIRED. SQL permits omitting it, but then the numbering is over an
// arbitrary row order, so the same query can return different answers over the same data --
// not a useful thing to hand someone.
func (p *parser) parseWindowItem() (SelectItem, error) {
	name := strings.ToUpper(p.peek().text)
	p.pos += 2 // name + '('
	if err := p.expectPunct(")"); err != nil {
		return SelectItem{}, fmt.Errorf("%s() takes no arguments: %w", name, err)
	}
	if !p.takeKeyword("OVER") {
		return SelectItem{}, fmt.Errorf("%s() must be followed by OVER (...)", name)
	}
	if err := p.expectPunct("("); err != nil {
		return SelectItem{}, fmt.Errorf("%s() OVER: %w", name, err)
	}
	it := SelectItem{Window: name}
	if p.takeKeyword("PARTITION") {
		if err := p.expectKeyword("BY"); err != nil {
			return SelectItem{}, err
		}
		cols, err := p.parsePartitionCols()
		if err != nil {
			return SelectItem{}, err
		}
		it.WinPartition = cols
	}
	if !p.takeKeyword("ORDER") {
		return SelectItem{}, fmt.Errorf("%s() OVER (...) requires ORDER BY: without it the "+
			"numbering would depend on an arbitrary row order", name)
	}
	if err := p.expectKeyword("BY"); err != nil {
		return SelectItem{}, err
	}
	terms, err := p.parseOrderBy()
	if err != nil {
		return SelectItem{}, fmt.Errorf("%s() OVER (... ORDER BY ...): %w", name, err)
	}
	it.WinOrder = terms
	if err := p.expectPunct(")"); err != nil {
		return SelectItem{}, fmt.Errorf("%s() OVER (...): %w", name, err)
	}
	it.Col = it.windowText()
	it.Alias = p.parseOptionalAlias()
	return it, nil
}

// parsePartitionCols parses the attribute list after PARTITION BY.
func (p *parser) parsePartitionCols() ([]string, error) {
	var cols []string
	for {
		id, err := p.parseIdent()
		if err != nil {
			return nil, fmt.Errorf("PARTITION BY: %w", err)
		}
		cols = append(cols, id)
		if p.atPunct(",") {
			p.pos++
			continue
		}
		return cols, nil
	}
}

// windowText renders the window call the way it was written, for the default column header.
func (it SelectItem) windowText() string {
	var b strings.Builder
	b.WriteString(it.Window)
	b.WriteString("() OVER (")
	if len(it.WinPartition) > 0 {
		b.WriteString("PARTITION BY ")
		b.WriteString(strings.Join(it.WinPartition, ", "))
		b.WriteString(" ")
	}
	b.WriteString("ORDER BY ")
	for i, t := range it.WinOrder {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(t.Item.header())
		if t.Desc {
			b.WriteString(" DESC")
		}
	}
	b.WriteString(")")
	return b.String()
}

// validateWindow enforces the rules a windowed SELECT has to obey.
func validateWindow(st *Statement) error {
	if !hasWindow(st) {
		if st.Qualify != "" {
			return fmt.Errorf("QUALIFY filters window function results, but this SELECT has none " +
				"(did you mean WHERE?)")
		}
		return nil
	}
	// A window is per row; an aggregate collapses rows. Mixing them means a window over
	// grouped output, which needs the machinery this deliberately leaves out.
	if hasAggregate(st) || len(effectiveGroupBy(st)) > 0 {
		return fmt.Errorf("a window function cannot be combined with aggregates or GROUP BY")
	}
	for _, it := range st.Items {
		if it.Star {
			return fmt.Errorf("`*` cannot be used with a window function; project explicit columns")
		}
	}
	if err := rejectWindowInWhere(st); err != nil {
		return err
	}
	return validateQualify(st)
}

// rejectWindowInWhere refuses a WHERE clause that reads a window column.
//
// `WHERE n <= 5` is what everyone reaches for to get "top five per group", and it is not legal
// SQL: WHERE runs before the window is computed, so there is no such column yet. Here it is
// worse than illegal -- the constraint goes to the store, which finds no attribute by that
// name, and the query returns NOTHING rather than complaining. So it is caught here and
// pointed at QUALIFY, which is the clause that does what the user meant.
func rejectWindowInWhere(st *Statement) error {
	if st.Where == "" {
		return nil
	}
	q, err := vm.Parse(st.Where)
	if err != nil {
		return nil // not our error to report; the store will reject it
	}
	named := map[string]string{}
	for _, it := range st.Items {
		if it.Window == "" {
			continue
		}
		if it.Alias != "" {
			named[strings.ToLower(it.Alias)] = it.Alias
		}
		named[strings.ToLower(it.header())] = it.header()
	}
	for _, a := range q.ReadAttrs() {
		if name, ok := named[strings.ToLower(a)]; ok {
			return fmt.Errorf("WHERE cannot reference the window column %q: WHERE selects rows "+
				"before the window is computed. Use QUALIFY %s ... instead", name, a)
		}
	}
	return nil
}

// validateQualify checks that QUALIFY only reads window columns. A row-level condition belongs
// in WHERE, where the store applies it before the rows are ever fetched -- so pointing there is
// both the correct answer and the faster one.
func validateQualify(st *Statement) error {
	if st.Qualify == "" {
		return nil
	}
	q, err := vm.Parse(st.Qualify)
	if err != nil {
		return fmt.Errorf("QUALIFY is not a valid expression: %w", err)
	}
	inScope := map[string]bool{}
	for _, it := range st.Items {
		if it.Window == "" {
			continue
		}
		inScope[strings.ToLower(it.header())] = true
		if it.Alias != "" {
			inScope[strings.ToLower(it.Alias)] = true
		}
	}
	for _, a := range q.ReadAttrs() {
		if !inScope[strings.ToLower(a)] {
			return fmt.Errorf("QUALIFY: %q is not a window column; a condition on the rows "+
				"themselves belongs in WHERE", a)
		}
	}
	return nil
}

// windowValues computes each window column's value for every ad, returned as one slice per
// ad aligned with st.Items (empty at non-window positions).
//
// Rows are partitioned by the PARTITION BY tuple, each partition is sorted by the window's own
// ORDER BY, and the numbering is assigned within it. The input order of ads is preserved in
// the output, so the caller's own ORDER BY still decides how rows are presented.
func windowValues(st *Statement, ads []*classad.ClassAd) ([][]string, error) {
	out := make([][]string, len(ads))
	for i := range out {
		out[i] = make([]string, len(st.Items))
	}
	for j, it := range st.Items {
		if it.Window == "" {
			continue
		}
		// Render the ordering key of every ad up front, so the per-partition sorts below just
		// compare strings.
		keys, err := orderKeys(ads, it.WinOrder)
		if err != nil {
			return nil, err
		}
		// Index the ads by partition so each partition can be ordered independently.
		parts := map[string][]int{}
		var order []string
		for i, ad := range ads {
			key := partitionKey(ad, it.WinPartition)
			if _, seen := parts[key]; !seen {
				order = append(order, key)
			}
			parts[key] = append(parts[key], i)
		}
		for _, key := range order {
			idx := parts[key]
			sort.SliceStable(idx, func(a, b int) bool {
				return keysLess(keys[idx[a]], keys[idx[b]], it.WinOrder)
			})
			assignRanks(it.Window, idx, keys, out, j)
		}
	}
	return out, nil
}

// assignRanks writes the window value for one ordered partition.
//
//	ROW_NUMBER  1,2,3,4  -- always distinct
//	RANK        1,1,3,4  -- ties share a number, and skip the ones they consumed
//	DENSE_RANK  1,1,2,3  -- ties share a number, and the next is the next integer
func assignRanks(fn string, idx []int, keys [][]string, out [][]string, col int) {
	rank, dense := 0, 0
	for pos, adIdx := range idx {
		tied := pos > 0 && sameOrderKey(keys[idx[pos-1]], keys[adIdx])
		switch fn {
		case winRowNumber:
			rank = pos + 1
		case winRank:
			if !tied {
				rank = pos + 1
			}
		case winDenseRank:
			if !tied {
				dense++
			}
			rank = dense
		}
		out[adIdx][col] = strconv.Itoa(rank)
	}
}

// partitionKey renders an ad's PARTITION BY tuple. No partition columns means one partition
// holding every row.
func partitionKey(ad *classad.ClassAd, cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = valueDisplay(ad.EvaluateAttr(c))
	}
	return strings.Join(parts, "\x00")
}

// sameOrderKey reports whether two precomputed ORDER BY tuples compare equal -- a tie, which
// RANK and DENSE_RANK give the same number.
func sameOrderKey(a, b []string) bool {
	for k := range a {
		if compareCells(a[k], b[k]) != 0 {
			return false
		}
	}
	return true
}

// orderKeys renders each ad's window ORDER BY tuple, once.
//
// Computing them inside the comparator instead cost O(n log n) attribute evaluations and
// string renders per partition -- and, for an expression term, an O(n log n) ClassAd parse,
// since the term was compiled afresh on every comparison. Decorating first makes it O(n).
func orderKeys(ads []*classad.ClassAd, terms []OrderTerm) ([][]string, error) {
	compiled := make([]*classad.Expr, len(terms))
	for k, t := range terms {
		if t.Item.Expr == "" {
			continue
		}
		ex, err := classad.ParseExpr(t.Item.Expr)
		if err != nil {
			return nil, fmt.Errorf("window ORDER BY %q: %w", t.Item.Expr, err)
		}
		compiled[k] = ex
	}
	keys := make([][]string, len(ads))
	cells := make([]string, len(ads)*len(terms)) // one backing array, re-sliced per ad
	for i, ad := range ads {
		row := cells[i*len(terms) : (i+1)*len(terms) : (i+1)*len(terms)]
		for k, t := range terms {
			if compiled[k] != nil {
				row[k] = valueDisplay(compiled[k].Eval(ad))
			} else {
				row[k] = valueDisplay(ad.EvaluateAttr(t.Item.Col))
			}
		}
		keys[i] = row
	}
	return keys, nil
}

// keysLess orders two precomputed ORDER BY tuples.
func keysLess(a, b []string, terms []OrderTerm) bool {
	for k, t := range terms {
		c := compareCells(a[k], b[k])
		if c == 0 {
			continue
		}
		if t.Desc {
			return c > 0
		}
		return c < 0
	}
	return false
}

// qualifyKeep filters rows by the QUALIFY expression, which reads only window columns (see
// validateQualify) and is therefore evaluated against a scope holding just those.
func qualifyKeep(st *Statement, ads []*classad.ClassAd, win [][]string) ([]*classad.ClassAd, [][]string, error) {
	if st.Qualify == "" {
		return ads, win, nil
	}
	ex, err := classad.ParseExpr(st.Qualify)
	if err != nil {
		return nil, nil, fmt.Errorf("QUALIFY %q: %w", st.Qualify, err)
	}
	keptAds := make([]*classad.ClassAd, 0, len(ads))
	keptWin := make([][]string, 0, len(win))
	for i, ad := range ads {
		scope := classad.New()
		for j, it := range st.Items {
			if it.Window == "" {
				continue
			}
			setLiteral(scope, it.header(), win[i][j])
			if it.Alias != "" {
				setLiteral(scope, it.Alias, win[i][j])
			}
		}
		if b, berr := ex.Eval(scope).BoolValue(); berr == nil && b {
			keptAds = append(keptAds, ad)
			keptWin = append(keptWin, win[i])
		}
	}
	return keptAds, keptWin, nil
}

// execSelectWindow runs a SELECT carrying a ranking window column.
//
// It fetches every matching row: LIMIT is deliberately NOT pushed to the server, because a
// server-side cap truncates before the ranking happens, which would rank an arbitrary prefix
// and quietly return the wrong rows. QUALIFY narrows the result after the numbering, and the
// statement's own ORDER BY / LIMIT apply last -- so `ORDER BY n` sorts by the window column,
// as it should.
func (e *Executor) execSelectWindow(st *Statement) (*Result, error) {
	ads, err := e.queryAdsForClientScan(st)
	if err != nil {
		return nil, err
	}
	win, err := windowValues(st, ads)
	if err != nil {
		return nil, err
	}
	ads, win, err = qualifyKeep(st, ads, win)
	if err != nil {
		return nil, err
	}

	res := &Result{IsSelect: true}
	compiled := make([]*classad.Expr, len(st.Items))
	for j, it := range st.Items {
		res.Columns = append(res.Columns, it.header())
		if it.Window == "" && it.Expr != "" {
			ex, perr := classad.ParseExpr(it.Expr)
			if perr != nil {
				return nil, fmt.Errorf("SELECT expression %q: %w", it.Expr, perr)
			}
			compiled[j] = ex
		}
	}
	// Render each row paired with its ad, so a sort keeps Result.Ads (which backs the JSON and
	// ClassAd output formats, in display order after LIMIT) in step with Result.Rows.
	type wrow struct {
		cells []string
		ad    *classad.ClassAd
	}
	rows := make([]wrow, len(ads))
	for i, ad := range ads {
		cells := make([]string, len(st.Items))
		for j, it := range st.Items {
			switch {
			case it.Window != "":
				cells[j] = win[i][j]
			case compiled[j] != nil:
				cells[j] = valueDisplay(compiled[j].Eval(ad))
			default:
				cells[j] = valueDisplay(ad.EvaluateAttr(it.Col))
			}
		}
		rows[i] = wrow{cells: cells, ad: ad}
	}

	if len(st.OrderBy) > 0 {
		idxs := make([]int, len(st.OrderBy))
		for k, t := range st.OrderBy {
			idx := columnIndex(res.Columns, t.Item.header())
			if idx < 0 {
				return nil, fmt.Errorf("ORDER BY %s is not a selected column", t.Item.header())
			}
			idxs[k] = idx
		}
		sort.SliceStable(rows, func(a, b int) bool {
			for k, t := range st.OrderBy {
				c := compareCells(rows[a].cells[idxs[k]], rows[b].cells[idxs[k]])
				if c == 0 {
					continue
				}
				if t.Desc {
					return c > 0
				}
				return c < 0
			}
			return false
		})
	}
	if st.Limit > 0 && len(rows) > st.Limit {
		rows = rows[:st.Limit]
	}
	for _, r := range rows {
		res.Rows = append(res.Rows, r.cells)
		res.Ads = append(res.Ads, r.ad)
	}
	return res, nil
}
