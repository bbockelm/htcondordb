package repl

import (
	"fmt"

	"github.com/PelicanPlatform/classad/classad"
)

// RowStreamer prepares a SELECT for row-at-a-time delivery: it returns the column headers and
// a function that produces one row's cell values from a matched ad. Pair it with StreamSelect,
// which supplies the ads, to walk a result without materializing it.
//
// It returns (nil, nil, nil) -- not an error -- when the statement's rows cannot be produced
// from matched ads alone, in which case the caller must run Exec and read Result.Rows instead.
// Three shapes are like that, and the reason is the same each time: the row is a property of
// the result set rather than of any one ad.
//
//   - SELECT * : the header is the union of every matched ad's attributes (starColumns), so it
//     is not known until the last ad has been seen. Streaming it would mean a column list that
//     grows underneath the caller.
//   - An aggregate or GROUP BY : rows are synthesized from group tuples. The ads StreamSelect
//     yields for those are themselves synthesized, and evaluating COUNT(*) against one is
//     meaningless.
//   - A window function : ROW_NUMBER and friends rank a row against the others, so no ad
//     carries the answer.
//
// ORDER BY and DISTINCT are deliberately absent from that list. Both need the whole result
// before the first row is correct, but their rows still come from ads, so StreamSelect
// collects internally and delivers through the same path. The caller gets identical values
// either way and only the memory differs -- which is exactly the distinction that should not
// leak into a public API.
func (e *Executor) RowStreamer(st *Statement) ([]string, func(*classad.ClassAd) []classad.Value, error) {
	if st.Kind != StmtSelect {
		return nil, nil, fmt.Errorf("only SELECT produces rows")
	}
	if len(st.GroupBy) > 0 || hasAggregate(st) {
		return nil, nil, nil
	}
	for _, it := range st.Items {
		if it.Star || it.Window != "" || it.Bucket {
			return nil, nil, nil
		}
	}

	// Compile expression columns once, as the materializing path does, so the per-row work is
	// an evaluation rather than a parse.
	columns := make([]string, len(st.Items))
	compiled := make([]*classad.Expr, len(st.Items))
	for j, it := range st.Items {
		columns[j] = it.header()
		if it.Expr != "" {
			ex, err := classad.ParseExpr(it.Expr)
			if err != nil {
				return nil, nil, fmt.Errorf("SELECT expression %q: %w", it.Expr, err)
			}
			compiled[j] = ex
		}
	}

	items := st.Items
	row := func(ad *classad.ClassAd) []classad.Value {
		values := make([]classad.Value, len(items))
		for j, it := range items {
			if compiled[j] != nil {
				values[j] = compiled[j].Eval(ad)
			} else {
				values[j] = ad.EvaluateAttr(it.Col)
			}
		}
		return values
	}
	return columns, row, nil
}
