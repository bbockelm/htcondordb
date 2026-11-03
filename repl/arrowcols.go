package repl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Typing a SELECT's result columns for Arrow.
//
// This lives here, beside SelectItem, because TWO front doors need it and a second copy would
// let them disagree about what a query returns -- the same reason arrowtype.Columns is shared
// rather than duplicated. The Flight SQL endpoint types a result before executing it, because
// the protocol promises a shape in one RPC and delivers rows in another; the REPL's Arrow
// output types the same result on the way to a file.
//
// The columns are NOT the table's. COUNT(*) is an int64 no attribute produced, AVG is a
// float64 whatever it averaged, and a window function is a count. A table's declarations
// answer only the terms that reach an attribute: MIN, MAX, SUM, time_bucket and a plain
// column.

// ArrowDecisions types the result columns from the SELECT list, against the table's own
// declarations. This is what lets the schema be answered before anything runs.
//
// Everything here is NULLABLE except a count. A group with no rows contributing to a MIN, a
// column that is absent in some ads, an expression that evaluates to undefined -- all of them
// produce a null cell, and a non-nullable Arrow column that receives one is a lie the builder
// cannot catch. COUNT is the exception because it is defined for every group, zero included.
func ArrowDecisions(st *Statement, table []arrowtype.Decision) ([]arrowtype.Decision, error) {
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
		d, err := arrowColumn(it, byName)
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
func checkReferenced(it SelectItem, byName map[string]arrowtype.Decision, within string) error {
	if it.Col == "" || it.Col == "*" || it.Expr != "" || it.Window != "" {
		// An expression or a window term orders by something computed. Its own column
		// references go through the executor, which resolves them against this same
		// session, so there is nothing to check here that would not be checked there.
		return nil
	}
	_, err := sourceColumn(it.Col, byName, within)
	return err
}

func isCount(it SelectItem) bool {
	return strings.HasPrefix(strings.ToUpper(it.Agg), "COUNT")
}

// arrowColumn types one select item. The Name and nullability are set by the caller.
func arrowColumn(it SelectItem, byName map[string]arrowtype.Decision) (arrowtype.Decision, error) {
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

// ArrowCellValue reads one display cell as the value its DECLARED column type says it is.
//
// This is a parse against a declaration, not an inference from the text, and the difference is
// the whole reason it is acceptable. Inference is what the Grafana plugin and the DB-API
// cursor do today -- look at the characters, guess a type, and disagree with each other about
// the same column. Here the type was decided before the query ran, from a measurement of the
// table, and the cell is read as that or as null. A cell that does not parse as its declared
// type is a null rather than a silent retype, which keeps the column honest; the encoder's own
// counters record it.
func ArrowCellValue(cell string, d arrowtype.Decision) classad.Value {
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
