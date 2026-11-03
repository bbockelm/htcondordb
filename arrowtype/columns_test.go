package arrowtype

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/PelicanPlatform/classad/classad"
)

// col is a typed Decision, so these tests say which builder a column gets without restating
// what derive already knows.
func colDec(name string, dt arrow.DataType) Decision {
	return Decision{Name: name, Verdict: Typed,
		Field: arrow.Field{Name: name, Type: dt, Nullable: true}}
}

// colTestSchema is one column of each kind the fast path claims, plus a timestamp, which is the
// one that takes a conversion and still counts as direct.
func colTestSchema() []Decision {
	return []Decision{
		colDec("I", arrow.PrimitiveTypes.Int64),
		colDec("F", arrow.PrimitiveTypes.Float64),
		colDec("S", arrow.BinaryTypes.String),
		colDec("B", arrow.FixedWidthTypes.Boolean),
		colDec("T", arrow.FixedWidthTypes.Timestamp_s),
	}
}

func setValid(b []byte, i int) []byte {
	for len(b) <= i>>3 {
		b = append(b, 0)
	}
	b[i>>3] |= 1 << (uint(i) & 7)
	return b
}

// TestAppendColumnsMatchesAppendValues: the column path is a shortcut, not a second semantics.
// Both must build the same record -- same values, same nulls, same stats -- or a read answers
// differently depending on a decision the caller never made.
func TestAppendColumnsMatchesAppendValues(t *testing.T) {
	rows := 7
	// Row 3 is null in every column; everything else has a value.
	var valid []byte
	for r := range rows {
		if r != 3 {
			valid = setValid(valid, r)
		}
	}
	cols := []ColumnValues{
		{Ints: []int64{10, 11, 12, 14, 15, 16}, Valid: valid},
		{Reals: []float64{1.5, 2.5, 3.5, 5.5, 6.5, 7.5}, Valid: valid},
		{Strs: []string{"a", "b", "c", "e", "f", "g"}, Valid: valid},
		{Bools: []bool{true, false, true, false, true, false}, Valid: valid},
		{Ints: []int64{100, 101, 102, 104, 105, 106}, Valid: valid},
	}
	for i := range cols {
		cols[i].Nulls = []NullCell{{Row: 3, Cause: CauseUndefined}}
	}

	colEnc := NewEncoder(colTestSchema(), EncodeOptions{})
	took, err := colEnc.AppendColumns(rows, cols)
	if err != nil {
		t.Fatalf("AppendColumns: %v", err)
	}
	if !took {
		t.Fatal("the column path refused a batch it describes exactly")
	}
	if colEnc.ColumnBatches() != 1 {
		t.Fatalf("ColumnBatches = %d, want 1", colEnc.ColumnBatches())
	}

	rowEnc := NewEncoder(colTestSchema(), EncodeOptions{})
	dense := 0
	for r := range rows {
		if r == 3 {
			vals := make([]classad.Value, 5)
			for i := range vals {
				vals[i] = classad.NewUndefinedValue()
			}
			if err := rowEnc.AppendValues(vals); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := rowEnc.AppendValues([]classad.Value{
			classad.NewIntValue(cols[0].Ints[dense]),
			classad.NewRealValue(cols[1].Reals[dense]),
			classad.NewStringValue(cols[2].Strs[dense]),
			classad.NewBoolValue(cols[3].Bools[dense]),
			classad.NewIntValue(cols[4].Ints[dense]),
		}); err != nil {
			t.Fatal(err)
		}
		dense++
	}
	if rowEnc.ColumnBatches() != 0 {
		t.Errorf("the per-cell path counted %d column batches", rowEnc.ColumnBatches())
	}

	byCol, byRow := colEnc.NewRecord(), rowEnc.NewRecord()
	defer byCol.Release()
	defer byRow.Release()
	if byCol.NumRows() != byRow.NumRows() || byCol.NumRows() != int64(rows) {
		t.Fatalf("column path %d rows, per-cell %d, want %d", byCol.NumRows(), byRow.NumRows(), rows)
	}
	for c := range int(byCol.NumCols()) {
		a, b := byCol.Column(c), byRow.Column(c)
		if a.String() != b.String() {
			t.Errorf("column %d differs:\n  columns  %s\n  per-cell %s", c, a.String(), b.String())
		}
	}
	for i := range colEnc.Stats() {
		cs, rs := colEnc.Stats()[i], rowEnc.Stats()[i]
		if cs.Direct != rs.Direct || cs.Nulls[CauseUndefined] != rs.Nulls[CauseUndefined] {
			t.Errorf("column %d stats differ: columns %+v, per-cell %+v", i, cs, rs)
		}
	}
}

// TestAppendColumnsRefusesWhatItCannotDoExactly: everything needing a conversion or a per-row
// decision must go to coerce, which is where the rules live. A fast path that guessed at one of
// them would be a second answer to the same question.
func TestAppendColumnsRefusesWhatItCannotDoExactly(t *testing.T) {
	one := setValid(nil, 0)
	for _, tc := range []struct {
		name string
		col  ColumnValues
	}{
		{"a real into an int column", ColumnValues{Reals: []float64{1}, Valid: one}},
		{"an int into a string column", ColumnValues{Ints: []int64{1}, Valid: one}},
		{"no dense values at all", ColumnValues{Valid: one}},
		{"two kinds at once", ColumnValues{Ints: []int64{1}, Strs: []string{"x"}, Valid: one}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewEncoder([]Decision{colDec("I", arrow.PrimitiveTypes.Int64)}, EncodeOptions{})
			if tc.name == "an int into a string column" {
				e = NewEncoder([]Decision{colDec("S", arrow.BinaryTypes.String)}, EncodeOptions{})
			}
			if took, _ := e.AppendColumns(1, []ColumnValues{tc.col}); took {
				t.Error("took a batch it cannot append exactly")
			}
			if e.Rows() != 0 {
				t.Errorf("refused the batch and still appended %d rows", e.Rows())
			}
		})
	}
}

// TestAppendColumnsRefusesAllOrNothing: Arrow builders must end a batch the same length, so a
// batch half-taken would leave the record ragged and NewRecord would panic on it. The second
// column here is fine and the third is not; none of them may be appended.
func TestAppendColumnsRefusesAllOrNothing(t *testing.T) {
	one := setValid(nil, 0)
	e := NewEncoder([]Decision{
		colDec("A", arrow.PrimitiveTypes.Int64),
		colDec("B", arrow.PrimitiveTypes.Int64),
		colDec("C", arrow.PrimitiveTypes.Int64),
	}, EncodeOptions{})
	took, _ := e.AppendColumns(1, []ColumnValues{
		{Ints: []int64{1}, Valid: one},
		{Ints: []int64{2}, Valid: one},
		{Strs: []string{"no"}, Valid: one},
	})
	if took {
		t.Fatal("took a batch whose third column does not fit")
	}
	if e.Rows() != 0 {
		t.Fatalf("appended %d rows of a refused batch", e.Rows())
	}
	// And the builders are still equal length, which is the thing a ragged append breaks.
	rec := e.NewRecord()
	defer rec.Release()
	for c := range int(rec.NumCols()) {
		if rec.Column(c).Len() != 0 {
			t.Errorf("column %d has %d values after a refused batch", c, rec.Column(c).Len())
		}
	}
}

// TestAppendColumnsRefusesANullInANonNullableColumn: the per-cell path reports that per row,
// naming the attribute and the cause. Keeping one report in one place is worth more than
// appending this batch quickly.
func TestAppendColumnsRefusesANullInANonNullableColumn(t *testing.T) {
	e := NewEncoder([]Decision{{Name: "A", Verdict: Typed,
		Field: arrow.Field{Name: "A", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}}, EncodeOptions{})
	took, _ := e.AppendColumns(2, []ColumnValues{{
		Ints: []int64{1}, Valid: setValid(nil, 0),
		Nulls: []NullCell{{Row: 1, Cause: CauseUndefined}},
	}})
	if took {
		t.Error("took a batch with a null in a non-nullable column")
	}
}

// TestAppendColumnsTimestampsAreDirect: an epoch in seconds as an integer is the declared shape,
// so it is not a coercion and must not be counted as one -- the counts are what an operator reads
// to decide a column has stopped describing its data.
func TestAppendColumnsTimestampsAreDirect(t *testing.T) {
	e := NewEncoder([]Decision{colDec("T", arrow.FixedWidthTypes.Timestamp_s)}, EncodeOptions{})
	valid := setValid(setValid(nil, 0), 1)
	if took, err := e.AppendColumns(2, []ColumnValues{{Ints: []int64{1700000000, 1700000060}, Valid: valid}}); !took || err != nil {
		t.Fatalf("took=%v err=%v", took, err)
	}
	st := e.Stats()[0]
	if st.Direct != 2 || st.CoercedExact != 0 || st.CoercedLossy != 0 {
		t.Errorf("timestamp stats %+v, want 2 direct and nothing coerced", st)
	}
	rec := e.NewRecord()
	defer rec.Release()
	ts, ok := rec.Column(0).(*array.Timestamp)
	if !ok {
		t.Fatalf("column is %T", rec.Column(0))
	}
	if ts.Value(0) != arrow.Timestamp(1700000000) || ts.Value(1) != arrow.Timestamp(1700000060) {
		t.Errorf("timestamps %v %v", ts.Value(0), ts.Value(1))
	}
}
