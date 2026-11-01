package flightsvc

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reading bound parameters off the wire.
//
// Flight SQL sends a prepared statement's parameters as an Arrow stream: one COLUMN per
// placeholder, one ROW per parameter set. The values arrive in whatever width the client's
// driver chose, which is not necessarily the width this endpoint advertised -- a driver given a
// Python int may send int32 where the parameter schema said int64 -- so extraction converts to a
// Go value and lets the ansi package check it against the declared kind. One type check, in the
// place that knows what the column holds.

// readBoundParameters drains the parameter stream and returns one value per placeholder.
func readBoundParameters(r flight.MessageReader) ([]any, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "no parameter data was sent")
	}
	var vals []any
	rows := 0
	for r.Next() {
		rec := r.Record()
		if rec.NumRows() == 0 {
			continue
		}
		rows += int(rec.NumRows())
		if rows > 1 {
			// Several parameter SETS would be several queries, and a DoGet returns one
			// result set. Refused rather than silently running the first: a client that
			// meant executemany would otherwise get one answer and no indication that
			// the rest were dropped.
			return nil, status.Errorf(codes.InvalidArgument,
				"%d parameter sets were bound; a SELECT returns one result set, so bind one "+
					"set per execution rather than batching them", rows)
		}
		vals = make([]any, rec.NumCols())
		for i, col := range rec.Columns() {
			v, err := scalarAt(col, 0)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument,
					"parameter %d: %v", i+1, err)
			}
			vals[i] = v
		}
	}
	if err := r.Err(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "reading the bound parameters: %v", err)
	}
	if vals == nil {
		// An empty stream is not the same as binding nothing: a client that sent no rows
		// has not told us its values, and running the statement unparameterised would
		// return rows that look like an answer.
		return nil, status.Error(codes.InvalidArgument,
			"the parameter stream held no rows; bind one row of values per execution")
	}
	return vals, nil
}

// scalarAt reads one value out of an Arrow array as a Go value.
//
// A NULL comes back as a nil, which the lowering turns into a dropped row rather than a
// comparison against nothing.
func scalarAt(col arrow.Array, row int) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch a := col.(type) {
	case *array.Null:
		return nil, nil
	case *array.Int64:
		return a.Value(row), nil
	case *array.Int32:
		return a.Value(row), nil
	case *array.Int16:
		return a.Value(row), nil
	case *array.Int8:
		return a.Value(row), nil
	case *array.Uint64:
		return a.Value(row), nil
	case *array.Uint32:
		return a.Value(row), nil
	case *array.Uint16:
		return int32(a.Value(row)), nil
	case *array.Uint8:
		return int32(a.Value(row)), nil
	case *array.Float64:
		return a.Value(row), nil
	case *array.Float32:
		return a.Value(row), nil
	case *array.String:
		return a.Value(row), nil
	case *array.LargeString:
		return a.Value(row), nil
	case *array.Boolean:
		return a.Value(row), nil
	case *array.Timestamp:
		// The column this binds against holds unix-epoch seconds as a plain integer, and
		// the encoder emits them as timestamp[s] without rescaling -- so a timestamp[s]
		// is numerically the stored value. Any other unit is rescaled rather than
		// reinterpreted, because a millisecond count compared against a second count
		// matches nothing and looks like an empty result.
		dt, ok := a.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("a timestamp with no unit cannot be bound")
		}
		secs, err := timestampSeconds(int64(a.Value(row)), dt.Unit)
		if err != nil {
			return nil, err
		}
		return secs, nil
	}
	return nil, fmt.Errorf("a %s value cannot be bound as a parameter", col.DataType())
}

// timestampSeconds rescales to the epoch-second count the stored attribute holds.
//
// It TRUNCATES toward zero for the sub-second units, which is the only honest option: the
// attribute has no sub-second part, so a client binding 10:00:00.500 is asking about a value
// that cannot exist. Truncation makes `>= 10:00:00.500` behave as `>= 10:00:00`, admitting one
// extra second's rows; rounding would move the boundary in whichever direction the fraction
// happened to fall, which is harder to reason about from a query's results.
func timestampSeconds(v int64, u arrow.TimeUnit) (int64, error) {
	switch u {
	case arrow.Second:
		return v, nil
	case arrow.Millisecond:
		return v / 1e3, nil
	case arrow.Microsecond:
		return v / 1e6, nil
	case arrow.Nanosecond:
		return v / 1e9, nil
	}
	return 0, fmt.Errorf("a timestamp in %v cannot be bound", u)
}
