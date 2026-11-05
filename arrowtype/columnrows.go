package arrowtype

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Appending a whole column from a producer that is IN THIS PROCESS.
//
// AppendColumns takes the dense form a relay produces: the values that exist, packed, with a
// bitmap saying which rows they belong to. That is the right shape for a WIRE, where the rows
// with no value should cost nothing to send. It is the wrong shape for Arrow, which wants one
// entry per row and a parallel validity -- which is why appendColumn ends in spread, undoing the
// packing a row at a time.
//
// A producer inside this process never needed the packing. It holds its values one per row
// already, so densifying them for AppendColumns only to have spread expand them again is two
// passes and a pair of buffers to arrive back where it started. This path takes the per-row form
// as it is: when the values need no conversion they go to the builder in ONE bulk copy, and the
// only per-row work left is the validity, which Arrow needs either way.
//
// SAME FAST-PATH RULES AS AppendColumns. It appends only where the values can enter the builder
// as they are, refuses the whole batch otherwise, and leaves every conversion to the per-cell
// path. Eligibility is decided for every column before any is appended, because Arrow builders
// must end a batch the same length.

// ColumnRows is one column of a batch in the PER-ROW form an in-process producer holds: exactly
// one of the value slices is non-nil and has one entry per row, including the rows with no value.
//
// Valid says which rows carry a value, one bool per row; nil means every row does. A row that is
// not valid may hold anything in the value slice -- Arrow ignores it -- so a producer whose own
// storage is already per-row can pass that storage directly rather than copying around its gaps.
//
// Nulls lists the rows with no value and why, and must agree with Valid. It carries the cause for
// the statistics only; Arrow learns nothing from it beyond what Valid already says.
//
// A COLUMN WITH NO VALUE SLICE AT ALL is every row null, whatever the field's type -- the shape a
// projection yields for an attribute no row in the batch has. It is spelled by leaving all four
// value slices nil and letting Nulls name every row, and it is the one shape here that does not
// have to agree with a particular builder, because there is nothing to put in one.
type ColumnRows struct {
	Ints  []int64
	Reals []float64
	Bools []bool
	Strs  []string
	Valid []bool
	Nulls []NullCell
}

// allNull reports the no-value-slice shape: every row null, for any builder.
func (c *ColumnRows) allNull(rows int) bool {
	return c.Ints == nil && c.Reals == nil && c.Bools == nil && c.Strs == nil && len(c.Nulls) == rows
}

// rows reports how many rows the column carries, and that exactly one slice holds them.
func (c *ColumnRows) rowsHeld() (int, bool) {
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

// AppendColumnRows appends one batch of rows, a column at a time, from the per-row form.
//
// ok=false means some column cannot be appended as it is and NOTHING was appended; the caller
// uses the per-cell path for the whole batch. An error is a strict-mode refusal or a null in a
// non-nullable column, and leaves the batch appended, exactly as AppendValues does.
func (e *Encoder) AppendColumnRows(rows int, cols []ColumnRows) (bool, error) {
	if e.tail >= 0 {
		// The tail is the complement of a whole record, which columns alone do not describe.
		return false, nil
	}
	if len(cols) != len(e.attrs) || rows < 0 {
		return false, nil
	}
	for i := range cols {
		if !e.columnRowsFit(i, rows, &cols[i]) {
			return false, nil
		}
	}
	var firstErr error
	for i := range cols {
		if err := e.appendColumnRows(i, rows, &cols[i]); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.rows += rows
	e.colBatches++
	return true, firstErr
}

// columnRowsFit reports whether this column's values can go into its builder unchanged.
func (e *Encoder) columnRowsFit(i, rows int, c *ColumnRows) bool {
	if c.allNull(rows) {
		// Nothing to put in a builder, so the field's type does not come into it. Only
		// whether the field may be null at all.
		return e.schema.Field(i).Nullable
	}
	n, one := c.rowsHeld()
	if !one || n != rows {
		// One entry per row is the whole premise of this form. A column that does not
		// account for all of them is one this path has misread.
		return false
	}
	if c.Valid != nil && len(c.Valid) != rows {
		// Arrow panics on a validity that is neither empty nor the values' length, and a
		// batch half appended cannot be taken back.
		return false
	}
	if len(c.Nulls) > 0 && !e.schema.Field(i).Nullable {
		// A null in a non-nullable column is an error the per-cell path reports per row,
		// naming the attribute and the cause. Keeping that one report in one place is
		// worth more than appending this batch quickly.
		return false
	}
	if c.Valid == nil && len(c.Nulls) > 0 {
		// "Every row is valid" and "these rows are not" cannot both be true. Refusing is
		// cheaper than deciding which one the caller meant.
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

// appendColumnRows appends one column that columnRowsFit has already accepted.
func (e *Encoder) appendColumnRows(i, rows int, c *ColumnRows) error {
	st := &e.stats[i]
	st.Direct += rows - len(c.Nulls)
	for _, nc := range c.Nulls {
		st.Nulls[nc.Cause]++
	}
	if c.allNull(rows) {
		// AppendNulls is on the Builder interface, so this needs no type switch -- which is
		// the point: a column no row has a value for is the same append whatever its type.
		e.builder.Field(i).AppendNulls(rows)
		return nil
	}

	switch bld := e.builder.Field(i).(type) {
	case *array.Int64Builder:
		bld.AppendValues(c.Ints, c.Valid)
	case *array.Float64Builder:
		bld.AppendValues(c.Reals, c.Valid)
	case *array.StringBuilder:
		bld.AppendValues(c.Strs, c.Valid)
	case *array.BooleanBuilder:
		bld.AppendValues(c.Bools, c.Valid)
	case *array.TimestampBuilder:
		// The one kind that cannot go in as it is: arrow.Timestamp is a distinct type, so
		// the integers have to be retyped into a buffer of their own.
		out := e.tsScratch(rows)
		for r := range rows {
			out[r] = arrow.Timestamp(c.Ints[r])
		}
		bld.AppendValues(out, c.Valid)
	default:
		return fmt.Errorf("arrowtype: %s has no column append", e.attrs[i])
	}
	return nil
}
