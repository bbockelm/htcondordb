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
func (e *Executor) execSetOp(st *Statement) (*Result, error) {
	branches := st.setBranchStmts()
	dedupe := !st.SetBranches[0].All // validateSetOp has made every branch agree
	// Push the remaining limit into each branch only when the output is the branches
	// concatenated in order: no reordering and no row-removal may happen afterwards.
	pushLimit := st.Limit > 0 && len(st.OrderBy) == 0 && !dedupe

	star := isStarSelect(st)
	var combinedAds []*classad.ClassAd
	var res *Result
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
		res.Rows = append(res.Rows, br.Rows...)
		res.Ads = append(res.Ads, br.Ads...)
	}

	if star {
		if dedupe {
			combinedAds = dedupeAds(combinedAds)
		}
		res = e.renderStarRows(combinedAds)
	} else if res == nil {
		res = &Result{IsSelect: true}
	} else if dedupe {
		res.Rows = dedupeRows(res.Rows)
		res.Ads = nil // the ads no longer line up with the rows one for one
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
	}
	return res, nil
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

// dedupeRows drops rows equal to an earlier one, preserving first-seen order.
//
// It compares RENDERED rows, which is what makes two undefined values de-duplicate as
// equal -- SQL's DISTINCT treats NULLs as alike even though `NULL = NULL` is unknown, and
// ClassAd's `undefined == undefined` is likewise undefined. Comparing values with == here
// would keep every undefined row.
func dedupeRows(rows [][]string) [][]string {
	seen := make(map[string]bool, len(rows))
	out := rows[:0:0]
	for _, r := range rows {
		k := strings.Join(r, "\x00")
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, r)
	}
	return out
}
