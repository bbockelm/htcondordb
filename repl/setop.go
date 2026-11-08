package repl

// Set operations: UNION and UNION ALL.
//
// A row here is a ClassAd, not a tuple of fixed arity, which makes `SELECT * FROM jobs
// UNION ALL SELECT * FROM history` the natural spelling of a question the store is asked
// constantly -- one user's live jobs together with their completed ones -- and makes it
// cheap: concatenating two ad streams. The rendering already copes with ads that do not
// carry the same attributes, filling the gaps with undefined, so unioning two tables whose
// shapes differ needs no schema reconciliation.
//
// This is NOT a join, and the distinction is worth keeping sharp because the two questions
// sound alike. A join PAIRS rows side by side and produces a composite ad; a union STACKS
// them and produces rows of the same kind as its inputs. A job is in the queue or in
// history, never both, so pairing those two tables finds nothing to pair while stacking
// them answers the question.
//
// Each branch is executed by the ordinary SELECT path, so every pushdown a single-table
// SELECT gets, a branch gets. Only the combination happens here.

import (
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// SetOpKind is which set operation joins two branches.
type SetOpKind int

const (
	SetUnion SetOpKind = iota
)

// SetBranch is one further SELECT of a set operation.
type SetBranch struct {
	Kind SetOpKind
	// All is UNION ALL: keep duplicates. Without it the operation de-duplicates, as SQL's
	// bare UNION does.
	All  bool
	Stmt *Statement
}

// IsSetOp reports whether the statement combines several SELECTs.
func (s *Statement) IsSetOp() bool { return len(s.SetBranches) > 0 }

// parseSelectWithSetOps parses a SELECT and any UNION branches following it.
//
// ORDER BY and LIMIT bind to the WHOLE operation, not to the branch they follow, which is
// what SQL says and what a reader expects: `... UNION ALL SELECT ... ORDER BY QDate` orders
// the combined rows. Since each branch is parsed by the ordinary SELECT parser, the last
// branch is the one that consumes them, and they are lifted to the operation here. A branch
// that is not the last may not carry them -- there is nothing sensible it could mean.
func (p *parser) parseSelectWithSetOps() (*Statement, error) {
	first, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	prev := first
	for {
		if kw := p.setOpKeyword(); kw != "" {
			if kw != "UNION" {
				return nil, fmt.Errorf("%s is not supported; only UNION and UNION ALL are", kw)
			}
		} else {
			break
		}
		p.pos++ // UNION
		all := p.takeKeyword("ALL")
		if len(prev.OrderBy) > 0 || prev.Limit > 0 {
			return nil, fmt.Errorf("ORDER BY and LIMIT apply to the whole UNION, " +
				"so they belong after the last SELECT, not on a branch of it")
		}
		if !p.takeKeyword("SELECT") {
			return nil, fmt.Errorf("UNION must be followed by SELECT")
		}
		next, perr := p.parseSelect()
		if perr != nil {
			return nil, perr
		}
		first.SetBranches = append(first.SetBranches, SetBranch{Kind: SetUnion, All: all, Stmt: next})
		prev = next
	}
	if !first.IsSetOp() {
		return first, nil
	}
	// The last branch consumed the operation's ORDER BY / LIMIT; lift them to it.
	last := prev
	first.OrderBy, last.OrderBy = last.OrderBy, nil
	first.Limit, last.Limit = last.Limit, 0
	if err := validateSetOp(first); err != nil {
		return nil, err
	}
	return first, nil
}

// atSetOp reports whether a set operator is at the cursor. Every clause whose expression is
// captured verbatim has to stop here, or the expression swallows `UNION ALL SELECT ...` and
// the ClassAd parser reports a syntax error pointing at the operator.
func (p *parser) atSetOp() bool { return p.setOpKeyword() != "" }

// setOpKeyword names the set operator at the cursor, or "" if there is none.
func (p *parser) setOpKeyword() string {
	for _, kw := range []string{"UNION", "INTERSECT", "EXCEPT"} {
		if p.isKeyword(kw) {
			return kw
		}
	}
	return ""
}

// validateSetOp refuses branch shapes that cannot be combined, naming what differs.
func validateSetOp(st *Statement) error {
	branches := st.setBranchStmts()
	firstStar := isStarSelect(st)
	for i, b := range branches[1:] {
		if isStarSelect(b) != firstStar {
			return fmt.Errorf("UNION branch %d mixes SELECT * with named columns; "+
				"either every branch selects * or none does", i+2)
		}
		if b.IsSetOp() {
			return fmt.Errorf("a UNION branch may not itself be a UNION")
		}
		if !firstStar && len(b.Items) != len(st.Items) {
			return fmt.Errorf("UNION branch %d selects %d columns but the first selects %d; "+
				"every branch must select the same number", i+2, len(b.Items), len(st.Items))
		}
	}
	for _, b := range branches {
		if hasWindow(b) {
			return fmt.Errorf("window functions in a UNION branch are not supported")
		}
	}
	// UNION and UNION ALL in one chain would need SQL's left-associative, per-step
	// de-duplication to mean anything precise. Refuse the mix rather than pick a reading.
	for i, b := range st.SetBranches[1:] {
		if b.All != st.SetBranches[0].All {
			return fmt.Errorf("UNION and UNION ALL are mixed at branch %d; "+
				"use one or the other throughout, or run the halves separately", i+2)
		}
	}
	return nil
}

// setBranchStmts returns every branch, this statement first.
func (s *Statement) setBranchStmts() []*Statement {
	out := make([]*Statement, 0, 1+len(s.SetBranches))
	out = append(out, s)
	for _, b := range s.SetBranches {
		out = append(out, b.Stmt)
	}
	return out
}

// isStarSelect reports whether a branch projects whole ads.
func isStarSelect(st *Statement) bool {
	return len(st.Items) == 1 && st.Items[0].Star && st.Items[0].Col == ""
}

// --- execution ---

// execSetOp runs each branch and combines the results.
//
// The branches run as ordinary SELECTs, each with its own pushdowns; the operation's ORDER
// BY and LIMIT are applied to the combined rows, never to a branch, since a branch's first
// k rows are not the operation's first k. The one exception is UNION ALL with no ordering,
// where the result IS the branches in order and a branch can stop early.
//
// A branch's typed columns travel with its rows, so a named-column UNION serializes the
// columns it asked for rather than the ads the scan happened to match, and a cell keeps the
// type it evaluated to. Everything that is per-row -- the rows, the projected ads, the
// matched ads -- is reordered, de-duplicated and truncated together, because they are three
// views of one row and a reader may consult any of them.
func (e *Executor) execSetOp(st *Statement) (*Result, error) {
	branches := st.setBranchStmts()
	dedupe := !st.SetBranches[0].All // validateSetOp has made every branch agree
	// Push the remaining limit into each branch only when the output is the branches
	// concatenated in order: no reordering and no row-removal may happen afterwards.
	pushLimit := st.Limit > 0 && len(st.OrderBy) == 0 && !dedupe

	star := isStarSelect(st)
	var combinedAds []*classad.ClassAd
	var res *Result
	// Whether every branch so far has handed back one projected ad per row, and one
	// matched ad per row. See settleCombinedAds: one branch that carries neither drops that
	// kind of ad for the whole operation.
	projected, withAds := true, true
	for i, b := range branches {
		sub := *b // run the branch without the operation's ORDER BY / LIMIT
		sub.SetBranches = nil
		sub.OrderBy = nil
		sub.Limit = 0
		if pushLimit {
			if remaining := st.Limit - len(combinedAds) - rowCount(res); remaining <= 0 {
				break
			} else {
				sub.Limit = remaining
			}
		}
		br, err := e.Exec(&sub)
		if err != nil {
			return nil, fmt.Errorf("UNION branch %d: %w", i+1, err)
		}
		if star {
			// Whole ads: combine the ads themselves and render once, so the column set is
			// the union of what the branches carry rather than the first branch's.
			combinedAds = append(combinedAds, br.Ads...)
			continue
		}
		if res == nil {
			res = &Result{IsSelect: true, Columns: br.Columns}
		} else if len(br.Columns) != len(res.Columns) {
			return nil, fmt.Errorf("UNION branch %d produced %d columns, the first produced %d",
				i+1, len(br.Columns), len(res.Columns))
		}
		p, a := combineBranch(res, br)
		projected = projected && p
		withAds = withAds && a
	}

	if star {
		if dedupe {
			combinedAds = dedupeAds(combinedAds)
		}
		res = e.renderStarRows(combinedAds)
	} else if res == nil {
		res = &Result{IsSelect: true}
	} else {
		settleCombinedAds(res, projected, withAds)
		if dedupe {
			keep := dedupeRowIndexes(res.Rows)
			res.Rows = pickRows(res.Rows, keep)
			res.Projected = pickAds(res.Projected, keep)
			// The matched ads are dropped rather than picked: two rows that render alike
			// can come from ads that differ in the attributes the SELECT did not name, so
			// there is no one ad that stands for the row that survives. The projected ads
			// hold only the named columns, which are what the rows compared equal on, so
			// the surviving row's own projection is still exactly right.
			res.Ads = nil
		}
	}

	if len(st.OrderBy) > 0 {
		if err := sortRows(res, st.OrderBy); err != nil {
			return nil, err
		}
	}
	if st.Limit > 0 && len(res.Rows) > st.Limit {
		res.Rows = res.Rows[:st.Limit]
		if len(res.Ads) > st.Limit {
			res.Ads = res.Ads[:st.Limit]
		}
		if len(res.Projected) > st.Limit {
			res.Projected = res.Projected[:st.Limit]
		}
	}
	return res, nil
}

// combineBranch appends one branch's rows to the combined result, together with the per-row
// ads that travel beside them, and reports whether the branch carried a projected ad for
// every row and a matched ad for every row.
//
// The branch's typed columns are worth carrying because without them the ad-shaped formats
// and the cursor fall back to the branch's whole matched ads -- which answer a different
// question than the SELECT asked, and for a computed column answer nothing at all -- or,
// where there are no ads either, to re-reading the display text, under which the string
// "123" comes back as the number 123.
//
// A branch that is short of either kind contributes none of it, so a slice that survives is
// a prefix of the rows rather than a mixture: one branch's ads cannot slide under another
// branch's rows. Whether what survives is kept at all is settleCombinedAds's decision.
func combineBranch(res, br *Result) (projected, withAds bool) {
	res.Rows = append(res.Rows, br.Rows...)
	if projected = len(br.Projected) == len(br.Rows); projected {
		res.Projected = append(res.Projected, alignProjection(br, res.Columns)...)
	}
	if withAds = len(br.Ads) == len(br.Rows); withAds {
		res.Ads = append(res.Ads, br.Ads...)
	}
	return projected, withAds
}

// settleCombinedAds keeps the combined result's per-row ads only when every branch supplied
// them.
//
// PER-ROW ADS ARE ALL OR NOTHING, the same rule an aggregate result follows. A format that
// reads them reads them for every row, and a short slice does not degrade to display text:
// the ad formats serialize fewer rows than the table prints, silently, and a client that
// types its cells from ad i reads some other row's ad under this row's columns. An
// aggregate branch beside a row-scan branch is the ordinary way this happens -- the
// aggregate has no matched ads behind its rows at all -- so dropping the kind that is short
// leaves every row exactly as the rows themselves say it is.
func settleCombinedAds(res *Result, projected, withAds bool) {
	if !projected {
		res.Projected = nil
	}
	if !withAds {
		res.Ads = nil
	}
}

// alignProjection re-keys one branch's projected ads onto the operation's column names.
//
// A UNION matches its branches BY POSITION -- the i'th item of every branch feeds the i'th
// output column, which takes its name from the first branch -- while a projected ad is
// addressed by name. A branch that spells its i'th column differently, which `SELECT
// Machine FROM a UNION ALL SELECT Name FROM b` does and SQL allows, therefore holds its
// value under a name no output column carries. Handing those ads to the formats unchanged
// would print one branch's rows under one set of attribute names and the other branch's
// under another, while the table printed both under the first branch's -- so the rows and
// the ads would disagree about what the result even is.
//
// A branch whose names already are the operation's -- always the first, and in practice
// most of the rest -- is passed through without copying.
func alignProjection(br *Result, columns []string) []*classad.ClassAd {
	if sameColumns(br.Columns, columns) {
		return br.Projected
	}
	out := make([]*classad.ClassAd, 0, len(br.Projected))
	for _, proj := range br.Projected {
		ad := classad.New()
		for j, col := range columns {
			insertValue(ad, col, proj.EvaluateAttr(br.Columns[j]))
		}
		out = append(out, ad)
	}
	return out
}

// sameColumns reports whether two header lists name the same columns in the same order.
// Attribute names are case-insensitive, so headers that differ only in case name the same
// attribute and need no re-keying.
func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

// pickRows and pickAds take the entries named by keep, in keep's order. A nil or
// short input stays nil, so a result carrying no projection keeps carrying none.
func pickRows(rows [][]string, keep []int) [][]string {
	if len(rows) == 0 {
		return rows
	}
	out := make([][]string, 0, len(keep))
	for _, i := range keep {
		out = append(out, rows[i])
	}
	return out
}

func pickAds(ads []*classad.ClassAd, keep []int) []*classad.ClassAd {
	if ads == nil {
		return nil
	}
	out := make([]*classad.ClassAd, 0, len(keep))
	for _, i := range keep {
		out = append(out, ads[i])
	}
	return out
}

func rowCount(r *Result) int {
	if r == nil {
		return 0
	}
	return len(r.Rows)
}

// renderStarRows renders whole ads under the union of their attribute names, so a row from
// a branch that does not carry an attribute shows it as undefined rather than shifting the
// columns.
func (e *Executor) renderStarRows(ads []*classad.ClassAd) *Result {
	res := &Result{IsSelect: true, Star: true, Ads: ads, Columns: e.starColumns(ads)}
	for _, ad := range ads {
		row := make([]string, len(res.Columns))
		for j, col := range res.Columns {
			row[j] = valueDisplay(ad.EvaluateAttr(col))
		}
		res.Rows = append(res.Rows, row)
	}
	return res
}

// dedupeRowIndexes names the rows to keep: the first of each distinct row, in first-seen
// order.
//
// It compares RENDERED rows, which is what makes two undefined values de-duplicate as
// equal -- SQL's DISTINCT treats NULLs as alike even though `NULL = NULL` is unknown, and
// ClassAd's `undefined == undefined` is likewise undefined. Comparing values with == here
// would keep every undefined row.
//
// It returns indexes rather than rows because the rows are not the only thing in row order:
// a result that carries projected ads carries one per row, and they have to lose the same
// entries the rows do. Comparing on the rendered text while keeping the typed ad does mean
// that two rows whose values differ only in a way the display hides -- the string "123" and
// the number 123 -- collapse to whichever came first, which is the same answer the table
// gives and the price of de-duplicating on what the reader sees.
func dedupeRowIndexes(rows [][]string) []int {
	seen := make(map[string]bool, len(rows))
	keep := make([]int, 0, len(rows))
	for i, r := range rows {
		k := strings.Join(r, "\x00")
		if seen[k] {
			continue
		}
		seen[k] = true
		keep = append(keep, i)
	}
	return keep
}
