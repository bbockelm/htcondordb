package flightsvc

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/repl"
)

// The analytic path: the statements the scan cannot answer by itself.
//
// `SELECT <cols> FROM t [WHERE] [LIMIT]` is a scan with a filter, so the endpoint serves it by
// scanning (statement.go). An aggregate, a GROUP BY, an ORDER BY or a DISTINCT is not: the
// value of a row depends on the other rows, so somebody has to hold the result set. That
// somebody already exists -- `repl`'s executor runs all of it today, with server-side TopK and
// the archive's own GROUP BY underneath -- and until now the Flight endpoint refused the lot,
// so a client wanting `COUNT(*) GROUP BY Owner` pulled every row and counted them locally,
// over an endpoint whose store could have answered.
//
// TWO THINGS MAKE THIS MORE THAN ROUTING, and both are about the schema.
//
// **The result schema is not the table's.** `COUNT(*)` is an int64 column that no attribute
// produced; `AVG(x)` is a float64 whatever `x` is. So the columns are typed from the SELECT
// LIST, against the table's declared types -- which is the one thing that makes answering
// GetFlightInfo without executing possible at all, and that is a protocol requirement rather
// than an optimization.
//
// **The executor returns display text.** `repl.Result.Rows` is `[][]string`, so a cell is
// parsed on the way into Arrow. That is the text round trip the scan path just finished
// deleting, and it is acceptable HERE and not there for a reason worth stating: an aggregate
// result is group rows rather than table rows -- tens or thousands, not millions -- and the
// parse is against a DECLARED type rather than an inferred one, so it cannot silently retype a
// column the way inference does. If an analytic result ever becomes large enough for the parse
// to matter, the fix is a typed row API out of `repl`, not a heuristic here.
//
// READ-ONLY, structurally. Only `StmtSelect` reaches the executor, checked before anything
// else: the executor also runs INSERT, UPDATE, DELETE and DDL, and the endpoint's read-only
// promise must not depend on the ANSI front end happening to refuse those spellings.

// analytic is a statement the simple recogniser refused and `repl`'s parser accepted.
type analytic struct {
	st *repl.Statement
}

// unsupportedShape marks a statement that parsed as SQL but is not one this endpoint serves.
// It is distinguished from a parse error so the caller can report whichever message is more
// use: a syntax error names the typo, and this names the capability.
type unsupportedShape struct{ reason string }

func (e *unsupportedShape) Error() string { return e.reason }

// parseAnalytic parses a statement this endpoint's scan recogniser refused, and decides
// whether the executor can serve it.
func parseAnalytic(q string) (*analytic, error) {
	st, err := repl.Parse(q)
	if err != nil {
		return nil, fmt.Errorf("unsupported SQL: %w", err)
	}
	if st.Kind != repl.StmtSelect {
		// Everything that is not a SELECT, named rather than lumped together, because
		// "this endpoint is read-only" is the answer to a different question from "this
		// endpoint does not do EXPLAIN yet".
		return nil, &unsupportedShape{reason: "this endpoint is read-only and does not run " +
			"statements that write, define or explain; " + unsupportedHint}
	}
	if len(st.Joins) > 0 || len(st.SetBranches) > 0 {
		// The same refusal the REPL's ANSI dialect gives, for the same reason: a bare
		// column could belong to either side, and typing it from the first table's
		// declarations would type it from the wrong one.
		return nil, &unsupportedShape{reason: "a JOIN or a set operation is not served here: a " +
			"bare column could belong to either side, and this endpoint types columns from " +
			"one table's declarations; " + unsupportedHint}
	}
	if st.AsOf != "" {
		return nil, &unsupportedShape{reason: "FOR SYSTEM_TIME AS OF is not served here; " + unsupportedHint}
	}
	if st.Table == "" {
		return nil, &unsupportedShape{reason: "a statement with no FROM table is not served here; " + unsupportedHint}
	}
	for _, it := range st.Items {
		if it.Star {
			// SELECT * reaches here only alongside something analytic, since a plain
			// one is the scan's. Mixed with a GROUP BY it is not a legal statement
			// anyway, and mixed with an ORDER BY it would want the tail column, which
			// the executor does not produce.
			return nil, &unsupportedShape{reason: "`*` cannot be combined with an aggregate, " +
				"a GROUP BY or an ORDER BY here; name the columns. " + unsupportedHint}
		}
	}
	return &analytic{st: st}, nil
}

// runAnalytic executes the statement and appends its rows to the encoder.
//
// The whole result is materialized by the executor before the first cell reaches Arrow,
// because that is what an aggregate IS -- there is no first row until the last input row has
// been seen. The stream cap and the byte budget still apply around it, so a client cannot run
// many of these at once, but the executor's own working set is not bounded by them. A GROUP BY
// over a high-cardinality attribute is therefore the shape to watch here, the same way it is
// in the REPL.
func (s *Server) runAnalytic(ctx context.Context, db *dbrpc.Client, an *analytic,
	enc *arrowtype.Encoder, decisions []arrowtype.Decision) error {
	ex := repl.NewExecutor(db, repl.ExecConfig{})
	// ANSI, always: this endpoint speaks one dialect, and the predicate the client wrote
	// means what SQL says it means rather than what ClassAd would read it as.
	ex.SetDialect(repl.DialectANSI)
	restore := ex.SetOpContext(ctx)
	defer restore()

	res, err := ex.Exec(an.st)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if !res.IsSelect {
		// Unreachable: parseAnalytic admits only StmtSelect. Checked anyway, because
		// the thing being guarded is the read-only promise, and an unreachable check on
		// that is cheaper than discovering the path that reached it.
		return status.Error(codes.Internal, "the executor ran something that was not a query")
	}
	if len(res.Columns) != len(decisions) {
		// The schema was promised before this ran. If the executor produced a different
		// shape, serving the rows would put values under the wrong column names.
		return status.Errorf(codes.Internal,
			"the result has %d columns and the promised schema has %d",
			len(res.Columns), len(decisions))
	}
	vals := make([]classad.Value, len(decisions))
	for _, row := range res.Rows {
		for i := range decisions {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			vals[i] = repl.ArrowCellValue(cell, decisions[i])
		}
		if err := enc.AppendValues(vals); err != nil {
			return err
		}
	}
	return nil
}
