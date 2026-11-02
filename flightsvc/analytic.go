package flightsvc

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
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

// analyticDecisions types the result columns from the SELECT list, against the table's own
// declarations. This is what lets the schema be answered before anything runs.
//
// Everything here is NULLABLE except a count. A group with no rows contributing to a MIN, a
// column that is absent in some ads, an expression that evaluates to undefined -- all of them
// produce a null cell, and a non-nullable Arrow column that receives one is a lie the builder
// cannot catch. COUNT is the exception because it is defined for every group, zero included.
func analyticDecisions(st *repl.Statement, table []arrowtype.Decision) ([]arrowtype.Decision, error) {
	byName := make(map[string]arrowtype.Decision, len(table))
	for _, d := range table {
		byName[strings.ToLower(d.Name)] = d
	}
	// Every column the statement NAMES is resolved, not only the ones it projects.
	// A GROUP BY, an ORDER BY or an aggregate's argument reaches an attribute just as a
	// projected column does -- `SELECT COUNT(*) ... GROUP BY ClaimId` returns one row per
	// distinct value of it -- so a column that is not in this reader's declarations has to
	// be refused wherever it is named, not only where it would be printed.
	for _, name := range st.GroupBy {
		if _, err := sourceColumn(name, byName, "GROUP BY"); err != nil {
			return nil, err
		}
	}
	for _, term := range st.OrderBy {
		if err := checkReferenced(term.Item, byName, "ORDER BY"); err != nil {
			return nil, err
		}
	}
	for _, agg := range st.HavingAggs {
		if agg.Arg != "" && agg.Arg != "*" {
			if _, err := sourceColumn(agg.Arg, byName, "HAVING"); err != nil {
				return nil, err
			}
		}
	}
	out := make([]arrowtype.Decision, 0, len(st.Items))
	for _, it := range st.Items {
		d, err := analyticColumn(it, byName)
		if err != nil {
			return nil, err
		}
		d.Name = it.Header()
		d.Field.Name = d.Name
		d.Field.Nullable = d.Field.Nullable || !isCount(it)
		out = append(out, d)
	}
	return out, nil
}

// checkReferenced resolves the attribute a term names, without typing the term itself. An
// ORDER BY key is not a result column, so its type does not matter -- but the attribute it
// reaches is one the reader has to be entitled to.
func checkReferenced(it repl.SelectItem, byName map[string]arrowtype.Decision, within string) error {
	if it.Col == "" || it.Col == "*" || it.Expr != "" || it.Window != "" {
		// An expression or a window term orders by something computed. Its own column
		// references go through the executor, which resolves them against this same
		// session, so there is nothing to check here that would not be checked there.
		return nil
	}
	_, err := sourceColumn(it.Col, byName, within)
	return err
}

func isCount(it repl.SelectItem) bool {
	return strings.HasPrefix(strings.ToUpper(it.Agg), "COUNT")
}

// analyticColumn types one select item. The Name and nullability are set by the caller.
func analyticColumn(it repl.SelectItem, byName map[string]arrowtype.Decision) (arrowtype.Decision, error) {
	// A window function ranks a row against the others in its partition: always a count.
	if it.Window != "" {
		return intColumn("window-function", "a ranking function numbers rows"), nil
	}
	switch {
	case isCount(it):
		// A count is an int64 whatever it counted -- but it still has to be counting
		// something this reader may name. COUNT(DISTINCT x) reports the cardinality of
		// x and COUNT(x) reports how many rows have it, so skipping the check here
		// because the RESULT type does not depend on the column was a disclosure: an
		// unprivileged peer could count a private attribute it cannot read.
		if it.Col != "" && it.Col != "*" {
			if _, err := sourceColumn(it.Col, byName, "count"); err != nil {
				return arrowtype.Decision{}, err
			}
		}
		return intColumn("aggregate-count", "a count of rows, which is an integer whatever it counted"), nil
	case it.Agg == "AVG":
		// A mean is not the column's type even when the column is an integer, and
		// declaring it int64 would truncate every result that is not whole.
		return realColumn("aggregate-avg", "a mean is a real number whatever it averaged"), nil
	case it.Agg == "SUM", it.Agg == "MIN", it.Agg == "MAX":
		src, err := sourceColumn(it.Col, byName, strings.ToLower(it.Agg))
		if err != nil {
			return arrowtype.Decision{}, err
		}
		if it.Agg == "SUM" && src.Field.Type.ID() == arrow.TIMESTAMP {
			// Summing timestamps is not a timestamp. It is also not a question
			// anybody means to ask, so refusing beats inventing a unit for it.
			return arrowtype.Decision{}, fmt.Errorf("SUM(%s) adds timestamps together, which has no type", it.Col)
		}
		return src, nil
	case it.Bucket:
		// time_bucket floors a timestamp, so the bucket is the same kind of thing the
		// column was -- a timestamp stays a timestamp, which is what makes the result
		// graph on a time axis downstream rather than as a large integer.
		return sourceColumn(it.Col, byName, "time_bucket")
	case it.Expr != "":
		// A general expression. Its value cannot be typed from a measurement of the
		// table -- nothing measured `CurrentTime - QDate` -- so it takes the same
		// answer the tail column takes for an attribute no column fits: text, which is
		// faithful for any ClassAd value. Refusing instead would make this endpoint
		// strictly less capable than the REPL for a shape the REPL answers correctly.
		return textColumn("expression", "a computed expression, which no measurement of the table types"), nil
	case it.Col != "":
		return sourceColumn(it.Col, byName, "")
	}
	return arrowtype.Decision{}, fmt.Errorf("cannot type the selected term %q", it.Header())
}

// sourceColumn resolves a select term that takes an attribute's own declared type.
func sourceColumn(col string, byName map[string]arrowtype.Decision, within string) (arrowtype.Decision, error) {
	d, ok := byName[strings.ToLower(col)]
	in := ""
	if within != "" {
		in = " in " + within + "()"
	}
	if !ok {
		return arrowtype.Decision{}, fmt.Errorf("no such column %q%s in this table", col, in)
	}
	if d.Verdict != arrowtype.Typed {
		return arrowtype.Decision{}, fmt.Errorf("column %q%s has no typed form (%s: %s)",
			col, in, d.Reason, d.Detail)
	}
	d.Tail = false
	return d, nil
}

func intColumn(reason, detail string) arrowtype.Decision {
	return arrowtype.Decision{Verdict: arrowtype.Typed, Reason: reason, Detail: detail,
		Field: arrow.Field{Type: arrow.PrimitiveTypes.Int64}}
}

func realColumn(reason, detail string) arrowtype.Decision {
	return arrowtype.Decision{Verdict: arrowtype.Typed, Reason: reason, Detail: detail,
		Field: arrow.Field{Type: arrow.PrimitiveTypes.Float64}}
}

func textColumn(reason, detail string) arrowtype.Decision {
	return arrowtype.Decision{Verdict: arrowtype.Typed, Reason: reason, Detail: detail,
		Field: arrow.Field{Type: arrow.BinaryTypes.String, Nullable: true}}
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
			vals[i] = cellValue(cell, decisions[i])
		}
		if err := enc.AppendValues(vals); err != nil {
			return err
		}
	}
	return nil
}

// cellValue reads one display cell as the value its DECLARED column type says it is.
//
// This is a parse against a declaration, not an inference from the text, and the difference is
// the whole reason it is acceptable. Inference is what the Grafana plugin and the DB-API
// cursor do today -- look at the characters, guess a type, and disagree with each other about
// the same column. Here the type was decided before the query ran, from a measurement of the
// table, and the cell is read as that or as null. A cell that does not parse as its declared
// type is a null rather than a silent retype, which keeps the column honest; the encoder's own
// counters record it.
func cellValue(cell string, d arrowtype.Decision) classad.Value {
	switch cell {
	case "", "undefined":
		return classad.NewUndefinedValue()
	case "error":
		return classad.NewErrorValue()
	}
	switch d.Field.Type.ID() {
	case arrow.INT64, arrow.TIMESTAMP:
		if n, err := strconv.ParseInt(cell, 10, 64); err == nil {
			return classad.NewIntValue(n)
		}
		// An integral real: the aggregate path renders an averaged-looking value the
		// same way ClassAd does, without a decimal point when it is whole.
		if f, err := strconv.ParseFloat(cell, 64); err == nil {
			return classad.NewRealValue(f)
		}
		return classad.NewUndefinedValue()
	case arrow.FLOAT64:
		if f, err := strconv.ParseFloat(cell, 64); err == nil {
			return classad.NewRealValue(f)
		}
		return classad.NewUndefinedValue()
	case arrow.BOOL:
		switch cell {
		case "true":
			return classad.NewBoolValue(true)
		case "false":
			return classad.NewBoolValue(false)
		}
		return classad.NewUndefinedValue()
	default:
		return classad.NewStringValue(cell)
	}
}
