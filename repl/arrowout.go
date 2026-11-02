package repl

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Arrow as a REPL output format.
//
// The encoder and the type contract exist for the Flight endpoint, and once they do this is
// nearly free -- which is the only reason it is here. Its value is not a new capability: it is
// that a query's result reaches DuckDB, pandas or polars with its TYPES rather than as text
// somebody's loader has to guess at, and that the guess the CLI used to leave to a loader is
// now the same declaration the endpoint publishes.
//
// Three shapes, three sources, and they are different because the honest typed form of each
// result is different:
//
//   - `SELECT *` encodes the matched ADS against the table's own schema, tail column included,
//     exactly as the endpoint's `SELECT *` does. The projection is the table.
//   - A plain projection evaluates each item per ad, through the same RowStreamer the other
//     output formats use, so a cell reaches Arrow as a classad.Value and is never rendered.
//   - An aggregate has no ads -- its rows are per GROUP -- so its cells are parsed back from
//     the executor's display text against their DECLARED types (ArrowCellValue). That is a
//     text round trip, and it is confined to the one shape that cannot avoid one.
//
// BINARY OUTPUT, so this refuses a terminal. An Arrow IPC stream written to a tty is a
// screenful of control characters and a confused user, and the mistake is easy to make because
// every other format here is text.

// WriteArrow encodes a SELECT's result as an Arrow IPC stream.
//
// The statement is needed as well as the result, because the result does not say what its
// columns MEAN -- `Result.Columns` is display headers, and `COUNT(*)` is a header, not a type.
func (e *Executor) WriteArrow(w io.Writer, st *Statement, r *Result) error {
	if st == nil || !r.IsSelect {
		return fmt.Errorf("arrow output is for SELECT results")
	}
	table, err := e.tableDecisions(st.Table)
	if err != nil {
		return err
	}

	var decisions []arrowtype.Decision
	if r.Star {
		// Every typed column the table has, plus the tail, which is what makes the
		// file a faithful dump rather than a lossy one.
		decisions = withTail(table)
	} else {
		if decisions, err = ArrowDecisions(st, table); err != nil {
			return err
		}
	}

	enc := arrowtype.NewEncoder(decisions, arrowtype.EncodeOptions{})
	defer enc.Release()

	switch {
	case r.Star:
		for _, ad := range r.Ads {
			enc.Append(ad)
		}
	case r.Ads != nil:
		// A per-row projection: evaluate each item against the ad, which is what the
		// other formats do, so the same query prints and encodes the same values.
		_, row, rerr := e.RowStreamer(st)
		if rerr != nil {
			return rerr
		}
		if row == nil {
			// RowStreamer declines a shape whose rows are not a property of any one
			// ad. Those have no Ads either, so this is unreachable -- checked
			// because reaching it would silently encode an empty file.
			return fmt.Errorf("this result's rows cannot be produced from its ads")
		}
		for _, ad := range r.Ads {
			if err := enc.AppendValues(row(ad)); err != nil {
				return err
			}
		}
	default:
		vals := make([]classad.Value, len(decisions))
		for _, cells := range r.Rows {
			for i := range decisions {
				cell := ""
				if i < len(cells) {
					cell = cells[i]
				}
				vals[i] = ArrowCellValue(cell, decisions[i])
			}
			if err := enc.AppendValues(vals); err != nil {
				return err
			}
		}
	}

	rec := enc.NewRecord()
	defer rec.Release()
	wr := ipc.NewWriter(w, ipc.WithSchema(rec.Schema()))
	if err := wr.Write(rec); err != nil {
		wr.Close()
		return err
	}
	return wr.Close()
}

// withTail is the table's own decisions with the tail column appended, which is the schema a
// `SELECT *` carries: every typed column, and one more holding what none of them took.
func withTail(table []arrowtype.Decision) []arrowtype.Decision {
	out := make([]arrowtype.Decision, 0, len(table)+1)
	out = append(out, table...)
	return append(out, arrowtype.TailDecision())
}

// tableDecisions derives a table's Arrow columns, the same way the ANSI dialect and the Flight
// endpoint do. One derivation, so a column means the same thing however it is reached.
func (e *Executor) tableDecisions(table string) ([]arrowtype.Decision, error) {
	if table == "" {
		return nil, fmt.Errorf("no table to read types from")
	}
	rep, err := e.c.AttrTypeProfiles(e.opCtx(), table, ansiSampleMax)
	if err != nil {
		return nil, fmt.Errorf("reading %s's attribute types: %w", table, err)
	}
	// A sample short of its cap saw the whole table, which is the only sound basis for
	// declaring a column non-nullable.
	complete := rep.Sampled < ansiSampleMax
	ms := arrowtype.FromProfilesComplete(rep.Profiles, rep.Sampled, complete)
	_, decisions := arrowtype.Schema(ms, arrowtype.Options{})
	return decisions, nil
}
