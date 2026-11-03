package arrowtype

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Appending a whole COLUMN at a time, for a caller that already has one.
//
// AppendValues takes a row of classad.Values, which is right for a caller holding records and
// wrong for one holding columns. The store now answers a read as dense typed arrays, and the
// relay carries them that way; boxing each cell into a classad.Value so this package can unbox it
// again is a round trip through a representation neither end wanted. Measured on a six-column
// read it is most of what the encode still costs.
//
// SO THIS IS A FAST PATH, NOT A SECOND SEMANTICS. It appends only where the column's values can
// go into the builder AS THEY ARE -- an int64 column into an int64 or timestamp builder, a float
// into a float, a string into a string, a bool into a bool. Anything needing a conversion, a
// width check or a value-by-value decision is refused, and the caller falls back to the per-cell
// path that already knows how to do all of it. One coercion rule, in coerce, where it has always
// been.
//
// ALL OR NOTHING PER BATCH. Arrow builders must end a batch the same length, so a half-taken
// batch would leave the record ragged and NewRecord would panic on it. Eligibility is therefore
// decided for every column before any of them is appended.

// NullCell is one row with no value, and why.
type NullCell struct {
	Row   int
	Cause Cause
}

// ColumnValues is one column of a batch in the dense, typed form a columnar relay produces:
// exactly one of the value slices is non-nil, holding only the rows that HAVE a value, and Valid
// says which rows those are -- bit i set means row i takes the next dense value.
//
// Nulls lists the rows with no value. A caller that cannot say why may use CauseUndefined, which
// is what a projection yields for an absent attribute anyway.
type ColumnValues struct {
	Ints  []int64
	Reals []float64
	Bools []bool
	Strs  []string
	Valid []byte
	Nulls []NullCell
}

// dense reports how many values the column carries, and that exactly one slice holds them.
func (c *ColumnValues) dense() (int, bool) {
	n, kinds := 0, 0
	if c.Ints != nil {
		n, kinds = len(c.Ints), kinds+1
	}
	if c.Reals != nil {
		n, kinds = len(c.Reals), kinds+1
	}
	if c.Bools != nil {
		n, kinds = len(c.Bools), kinds+1
	}
	if c.Strs != nil {
		n, kinds = len(c.Strs), kinds+1
	}
	return n, kinds == 1
}

// AppendColumns appends one batch of rows, a column at a time.
//
// ok=false means some column cannot be appended as it is and NOTHING was appended; the caller
// uses the per-cell path for the whole batch. An error is a strict-mode refusal or a null in a
// non-nullable column, and leaves the batch appended, exactly as AppendValues does.
func (e *Encoder) AppendColumns(rows int, cols []ColumnValues) (bool, error) {
	if e.tail >= 0 {
		// The tail is the complement of a whole record, which columns alone do not describe.
		return false, nil
	}
	if len(cols) != len(e.attrs) || rows < 0 {
		return false, nil
	}
	for i := range cols {
		if !e.columnFits(i, rows, &cols[i]) {
			return false, nil
		}
	}
	var firstErr error
	for i := range cols {
		if err := e.appendColumn(i, rows, &cols[i]); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.rows += rows
	e.colBatches++
	return true, firstErr
}

// ColumnBatches is how many batches this encoder took column-wise rather than per cell.
//
// It is here because the fast path is INVISIBLE when it stops working: a column that stops
// fitting, a kind that stops being dense, and the encoder still produces exactly the right
// record by the slow path. Nothing in the output says which one ran, so a test that means to
// exercise this has to be able to ask.
func (e *Encoder) ColumnBatches() int { return e.colBatches }

// columnFits reports whether this column's values can go into its builder unchanged.
func (e *Encoder) columnFits(i, rows int, c *ColumnValues) bool {
	n, one := c.dense()
	if !one || n+len(c.Nulls) != rows {
		// Every row is either dense or null. A column that does not account for all of
		// them is one this path has misread, and appending it would misalign the batch.
		return false
	}
	if len(c.Nulls) > 0 && !e.schema.Field(i).Nullable {
		// A null in a non-nullable column is an error the per-cell path reports per row,
		// naming the attribute and the cause. Keeping that one report in one place is
		// worth more than appending this batch quickly.
		return false
	}
	switch e.builder.Field(i).(type) {
	case *array.Int64Builder:
		return c.Ints != nil
	case *array.Float64Builder:
		return c.Reals != nil
	case *array.StringBuilder:
		return c.Strs != nil
	case *array.BooleanBuilder:
		return c.Bools != nil
	case *array.TimestampBuilder:
		// An epoch in seconds as an integer is the declared shape; a real one is lossy
		// and goes to coerce, which counts it.
		return c.Ints != nil
	}
	return false
}

// appendColumn appends one column that columnFits has already accepted.
func (e *Encoder) appendColumn(i, rows int, c *ColumnValues) error {
	st := &e.stats[i]
	valid := e.validScratch(rows)
	for r := range rows {
		valid[r] = bitSet(c.Valid, r)
	}
	n, _ := c.dense()
	st.Direct += n
	for _, nc := range c.Nulls {
		st.Nulls[nc.Cause]++
	}

	switch bld := e.builder.Field(i).(type) {
	case *array.Int64Builder:
		bld.AppendValues(spread(e.i64Scratch(rows), c.Ints, valid), valid)
	case *array.Float64Builder:
		bld.AppendValues(spread(e.f64Scratch(rows), c.Reals, valid), valid)
	case *array.StringBuilder:
		bld.AppendValues(spread(e.strScratch(rows), c.Strs, valid), valid)
	case *array.BooleanBuilder:
		bld.AppendValues(spread(e.boolScratch(rows), c.Bools, valid), valid)
	case *array.TimestampBuilder:
		out := e.tsScratch(rows)
		d := 0
		for r := range rows {
			if valid[r] {
				out[r] = arrow.Timestamp(c.Ints[d])
				d++
			} else {
				out[r] = 0
			}
		}
		bld.AppendValues(out, valid)
	default:
		return fmt.Errorf("arrowtype: %s has no column append", e.attrs[i])
	}
	return nil
}

// spread expands a dense slice into one value per row, leaving the zero value where the row has
// none. Arrow's AppendValues wants a value for every row and a parallel validity; the dense form
// has one only for the rows that have a value.
func spread[T any](out []T, dense []T, valid []bool) []T {
	d := 0
	var zero T
	for r := range out {
		if valid[r] {
			out[r] = dense[d]
			d++
		} else {
			out[r] = zero
		}
	}
	return out
}

// bitSet reads bit i of a packed bitmap, treating a short one as all-unset.
func bitSet(b []byte, i int) bool {
	j := i >> 3
	return j < len(b) && b[j]&(1<<(uint(i)&7)) != 0
}

// The scratch buffers below are the point of appending column-wise: one buffer per kind, grown to
// the widest batch and reused, rather than a value boxed per cell.
func (e *Encoder) validScratch(n int) []bool {
	if cap(e.scValid) < n {
		e.scValid = make([]bool, n)
	}
	return e.scValid[:n]
}

func (e *Encoder) i64Scratch(n int) []int64 {
	if cap(e.scI64) < n {
		e.scI64 = make([]int64, n)
	}
	return e.scI64[:n]
}

func (e *Encoder) f64Scratch(n int) []float64 {
	if cap(e.scF64) < n {
		e.scF64 = make([]float64, n)
	}
	return e.scF64[:n]
}

func (e *Encoder) strScratch(n int) []string {
	if cap(e.scStr) < n {
		e.scStr = make([]string, n)
	}
	return e.scStr[:n]
}

func (e *Encoder) boolScratch(n int) []bool {
	if cap(e.scBool) < n {
		e.scBool = make([]bool, n)
	}
	return e.scBool[:n]
}

func (e *Encoder) tsScratch(n int) []arrow.Timestamp {
	if cap(e.scTS) < n {
		e.scTS = make([]arrow.Timestamp, n)
	}
	return e.scTS[:n]
}
