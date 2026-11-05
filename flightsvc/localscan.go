package flightsvc

import (
	"context"
	"fmt"
	"math"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Reading the store's columns without the loopback round trip.
//
// scanInto's framed path asks the loopback for typed columns, which the server encodes into a
// frame and this process decodes back. In process the producer's columns are already here, and
// they are already PER ROW -- which is the form the Arrow builders want. So for the two kinds
// that need no conversion the column goes to the builder as it stands, in one bulk copy, and the
// only per-row work left is the validity Arrow needs either way.

// localColumn is the buffers this path reuses across batches, one set per column of the
// projection. Only the two kinds that cannot enter a builder as they are need one.
type localColumn struct {
	valid []bool
	reals []float64
	bools []bool
	nulls []arrowtype.NullCell
}

// scanLocal fills enc from the store's own columns. It returns dbrpc.ErrLocalUnsupported, having
// appended nothing, when this table cannot serve a column scan -- the caller then takes the framed
// path, which answers the same question from records.
func (s *Server) scanLocal(ctx context.Context, local *dbrpc.LocalSession, enc *arrowtype.Encoder,
	table, constraint string, flush func() bool) error {
	cols := make([]arrowtype.ColumnRows, len(enc.Attrs()))
	scratch := make([]localColumn, len(enc.Attrs()))
	declined, appended := false, false
	// redact false for the reason scanInto gives: it asks for redaction BEYOND the session's,
	// and the session already redacts whatever an unprivileged reader asks.
	err := local.ScanColumns(ctx, table, constraint, enc.Attrs(), false,
		func(b *collections.Batch) bool {
			// A batch this shortcut cannot describe is not a reason to STOP: returning false
			// here ends the scan, and the rows already appended would ship as a complete
			// answer that is short. The framed path has a per-cell encoder to fall back to;
			// this one does not yet, so it refuses the whole read and lets scanInto take the
			// framed path, which does.
			if len(b.Cols) != len(cols) {
				declined = true
				return false
			}
			for i := range b.Cols {
				if !columnRowsOfBatch(&b.Cols[i], b.N, &scratch[i], &cols[i]) {
					declined = true
					return false
				}
			}
			took, _ := enc.AppendColumnRows(b.N, cols)
			if !took {
				declined = true
				return false
			}
			appended = true
			return enc.Rows() < batchRows || flush()
		})
	if err != nil {
		return err
	}
	if declined {
		if appended {
			// Rows are already in the encoder and some have probably been flushed, so the
			// framed path cannot retry from the start without duplicating them. Fail the
			// read rather than ship a short or doubled answer; a column this shortcut
			// cannot describe is a reason to grow it, not to guess.
			return fmt.Errorf("flightsvc: the in-process column read cannot encode a batch of "+
				"%s after %d rows were already appended", table, enc.Rows())
		}
		return dbrpc.ErrLocalUnsupported // nothing appended: the caller retries framed
	}
	return nil
}

// columnRowsOfBatch describes one producer column to the Arrow encoder, reporting false when it
// holds something the column path cannot describe.
//
// The producer's column is per CELL -- a kind, and a number or a string, for every row -- and so
// is what Arrow takes, so for an integer or a string column there is nothing to reshape: the
// producer's own slice IS the values, including at the rows that have none, which Arrow ignores.
// A real and a bool are stored as bits and as 0/1 and have to be read out into a slice of their
// own, which is one pass and one reused buffer.
//
// It refuses a column that mixes kinds, exactly as the framed path's columnValuesOf refuses a
// batch carrying anything the fast path does not describe: the caller then has the whole batch
// read the other way, so the coercion rules stay in one place and this stays a shortcut rather
// than a second semantics.
func columnRowsOfBatch(c *collections.Column, rows int, sc *localColumn, out *arrowtype.ColumnRows) bool {
	kind, ok := soleKind(c, rows)
	if !ok {
		return false
	}
	// Undefined and error are tested BEFORE kind, because when the column holds no typed value
	// at all soleKind reports undefined as the kind, and a row matching it there would be
	// counted as having a value it does not have.
	sc.nulls = sc.nulls[:0]
	for i := 0; i < rows; i++ {
		switch c.Kind[i] {
		case collections.ValueUndefined:
			sc.nulls = append(sc.nulls, arrowtype.NullCell{Row: i, Cause: arrowtype.CauseUndefined})
		case collections.ValueError:
			sc.nulls = append(sc.nulls, arrowtype.NullCell{Row: i, Cause: arrowtype.CauseError})
		case kind:
		default:
			// A scalar of another kind: whether it coerces is coerce's decision, not this
			// function's, and the caller has a path that asks it.
			return false
		}
	}
	var valid []bool
	if len(sc.nulls) > 0 {
		sc.valid = growSlice(sc.valid, rows)
		valid = sc.valid
		for i := range valid {
			valid[i] = true
		}
		for _, nc := range sc.nulls {
			valid[nc.Row] = false
		}
	}
	if kind == collections.ValueUndefined {
		// No row has a typed value. That is a column of nulls whatever the field's type,
		// and it is routine -- an attribute only some ads carry is absent from whole
		// batches -- so it must be describable rather than send the read to the slow path.
		*out = arrowtype.ColumnRows{Nulls: sc.nulls}
		return true
	}
	*out = arrowtype.ColumnRows{Valid: valid, Nulls: sc.nulls}

	switch kind {
	case collections.ValueInt:
		out.Ints = c.Num[:rows]
	case collections.ValueString:
		if !emptyAtNulls(c.Str, sc.nulls) {
			// The producer leaves "" in Str for every non-string cell, and the string
			// builder copies the bytes of EVERY element before it applies the validity --
			// so a row with no value whose Str was left holding some earlier cell's text
			// would put that text in the record's data buffer. The row reads as null and
			// the bytes still ship. Refusing costs this batch; not checking would cost
			// whatever the stale cell was.
			return false
		}
		out.Strs = c.Str[:rows]
	case collections.ValueReal:
		sc.reals = growSlice(sc.reals, rows)
		out.Reals = sc.reals
		for i := 0; i < rows; i++ {
			// Num carries a real as its bits. A row with no value reads as 0 here and
			// Arrow ignores it, so this needs no per-row test for which rows those are.
			out.Reals[i] = math.Float64frombits(uint64(c.Num[i]))
		}
	case collections.ValueBool:
		sc.bools = growSlice(sc.bools, rows)
		out.Bools = sc.bools
		for i := 0; i < rows; i++ {
			out.Bools[i] = c.Num[i] != 0
		}
	default:
		return false
	}
	return true
}

// emptyAtNulls reports that every row with no value holds no string, which is what lets the
// producer's own slice go to the builder rather than a copy with the gaps cleared.
func emptyAtNulls(str []string, nulls []arrowtype.NullCell) bool {
	for _, nc := range nulls {
		if str[nc.Row] != "" {
			return false
		}
	}
	return true
}

// growSlice returns a slice of exactly n, reusing s when it is already big enough.
func growSlice[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
}

// soleKind is the one packable kind in the column, or false when it holds more than one.
//
// Undefined and error do not count against it: they become nulls, they are much the commonest
// thing in a projection, and refusing over an absent attribute would give the fast path back on
// exactly the columns a projection asks for. A column with nothing but those has no kind, which
// is reported as undefined and taken as a column of nulls.
func soleKind(c *collections.Column, rows int) (collections.ValueKind, bool) {
	kind := collections.ValueUndefined
	for i := 0; i < rows; i++ {
		switch k := c.Kind[i]; k {
		case collections.ValueUndefined, collections.ValueError:
		default:
			if kind != collections.ValueUndefined && k != kind {
				return kind, false
			}
			kind = k
		}
	}
	return kind, true
}
