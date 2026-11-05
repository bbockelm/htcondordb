package arrowtype

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/PelicanPlatform/classad/classad"
)

// rowsFixture is the same batch as the dense fixture in columns_test.go, in per-row form: one
// entry per row including row 3, which has no value in any column.
func rowsFixture(rows, nullRow int) ([]ColumnRows, []bool) {
	valid := make([]bool, rows)
	for r := range rows {
		valid[r] = r != nullRow
	}
	cols := []ColumnRows{
		{Ints: []int64{10, 11, 12, 13, 14, 15, 16}},
		{Reals: []float64{1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5}},
		{Strs: []string{"a", "b", "c", "d", "e", "f", "g"}},
		{Bools: []bool{true, false, true, true, false, true, false}},
		{Ints: []int64{100, 101, 102, 103, 104, 105, 106}},
	}
	for i := range cols {
		cols[i].Valid = valid
		cols[i].Nulls = []NullCell{{Row: nullRow, Cause: CauseUndefined}}
	}
	return cols, valid
}

// TestAppendColumnRowsMatchesTheOtherTwoPaths: there are now three ways into a record and they
// must be one semantics. The per-cell path is the authority -- it is where coerce lives -- so
// both column paths are held to IT rather than to each other, which is what keeps a read from
// answering differently depending on where its producer happened to live.
func TestAppendColumnRowsMatchesTheOtherTwoPaths(t *testing.T) {
	const rows, nullRow = 7, 3
	cols, _ := rowsFixture(rows, nullRow)

	rowsEnc := NewEncoder(colTestSchema(), EncodeOptions{})
	took, err := rowsEnc.AppendColumnRows(rows, cols)
	if err != nil {
		t.Fatalf("AppendColumnRows: %v", err)
	}
	if !took {
		t.Fatal("the per-row column path refused a batch it describes exactly")
	}
	if rowsEnc.ColumnBatches() != 1 {
		t.Fatalf("ColumnBatches = %d, want 1", rowsEnc.ColumnBatches())
	}

	// The dense path over the same logical batch: the values that exist, packed, plus a bitmap.
	var bits []byte
	for r := range rows {
		if r != nullRow {
			bits = setValid(bits, r)
		}
	}
	dense := []ColumnValues{
		{Ints: []int64{10, 11, 12, 14, 15, 16}, Valid: bits},
		{Reals: []float64{1.5, 2.5, 3.5, 5.5, 6.5, 7.5}, Valid: bits},
		{Strs: []string{"a", "b", "c", "e", "f", "g"}, Valid: bits},
		{Bools: []bool{true, false, true, false, true, false}, Valid: bits},
		{Ints: []int64{100, 101, 102, 104, 105, 106}, Valid: bits},
	}
	for i := range dense {
		dense[i].Nulls = []NullCell{{Row: nullRow, Cause: CauseUndefined}}
	}
	denseEnc := NewEncoder(colTestSchema(), EncodeOptions{})
	if took, err := denseEnc.AppendColumns(rows, dense); err != nil || !took {
		t.Fatalf("AppendColumns took=%v err=%v", took, err)
	}

	cellEnc := NewEncoder(colTestSchema(), EncodeOptions{})
	for r := range rows {
		if r == nullRow {
			undef := make([]classad.Value, len(cols))
			for i := range undef {
				undef[i] = classad.NewUndefinedValue()
			}
			if err := cellEnc.AppendValues(undef); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := cellEnc.AppendValues([]classad.Value{
			classad.NewIntValue(cols[0].Ints[r]),
			classad.NewRealValue(cols[1].Reals[r]),
			classad.NewStringValue(cols[2].Strs[r]),
			classad.NewBoolValue(cols[3].Bools[r]),
			classad.NewIntValue(cols[4].Ints[r]),
		}); err != nil {
			t.Fatal(err)
		}
	}

	byRows, byDense, byCell := rowsEnc.NewRecord(), denseEnc.NewRecord(), cellEnc.NewRecord()
	defer byRows.Release()
	defer byDense.Release()
	defer byCell.Release()
	for _, got := range []struct {
		name string
		rec  arrow.Record
	}{{"per-row columns", byRows}, {"dense columns", byDense}} {
		if got.rec.NumRows() != byCell.NumRows() {
			t.Fatalf("%s: %d rows, per-cell %d", got.name, got.rec.NumRows(), byCell.NumRows())
		}
		for c := range int(byCell.NumCols()) {
			a, b := got.rec.Column(c), byCell.Column(c)
			if a.String() != b.String() {
				t.Errorf("%s column %d differs:\n  got      %s\n  per-cell %s",
					got.name, c, a.String(), b.String())
			}
		}
	}
	for i := range cellEnc.Stats() {
		rs, ds, cs := rowsEnc.Stats()[i], denseEnc.Stats()[i], cellEnc.Stats()[i]
		if rs.Direct != cs.Direct || rs.Nulls[CauseUndefined] != cs.Nulls[CauseUndefined] {
			t.Errorf("column %d: per-row stats %+v, per-cell %+v", i, rs, cs)
		}
		if rs.Direct != ds.Direct || rs.Nulls[CauseUndefined] != ds.Nulls[CauseUndefined] {
			t.Errorf("column %d: per-row stats %+v, dense %+v", i, rs, ds)
		}
	}
}

// TestAppendColumnRowsAllValidNeedsNoValidity: the hot shape. A column with a value in every row
// passes nil validity, which is Arrow's "all valid" and the whole reason this path can hand a
// producer's own storage to the builder without walking it.
func TestAppendColumnRowsAllValidNeedsNoValidity(t *testing.T) {
	const rows = 4
	cols := []ColumnRows{
		{Ints: []int64{1, 2, 3, 4}},
		{Reals: []float64{1, 2, 3, 4}},
		{Strs: []string{"a", "b", "c", "d"}},
		{Bools: []bool{true, false, true, false}},
		{Ints: []int64{9, 8, 7, 6}},
	}
	enc := NewEncoder(colTestSchema(), EncodeOptions{})
	took, err := enc.AppendColumnRows(rows, cols)
	if err != nil || !took {
		t.Fatalf("took=%v err=%v", took, err)
	}
	rec := enc.NewRecord()
	defer rec.Release()
	if rec.NumRows() != rows {
		t.Fatalf("%d rows, want %d", rec.NumRows(), rows)
	}
	for c := range int(rec.NumCols()) {
		if n := rec.Column(c).NullN(); n != 0 {
			t.Errorf("column %d has %d nulls with no validity given", c, n)
		}
	}
	for i, st := range enc.Stats() {
		if st.Direct != rows {
			t.Errorf("column %d counted %d direct, want %d", i, st.Direct, rows)
		}
	}
}

// TestAppendColumnRowsTakesAColumnNoRowHasAValueFor: the shape a projection yields for an
// attribute absent from every row of a batch. It must APPEND, not refuse: a sparse attribute is
// undefined across a whole batch routinely, and refusing would hand the entire read back to the
// slow path over a batch that is in fact trivially describable.
func TestAppendColumnRowsTakesAColumnNoRowHasAValueFor(t *testing.T) {
	const rows = 5
	// Every field of the fixture schema, each with no values at all: the shape must not
	// depend on what the field's type would have been.
	cols := make([]ColumnRows, len(colTestSchema()))
	for i := range cols {
		for r := range rows {
			cols[i].Nulls = append(cols[i].Nulls, NullCell{Row: r, Cause: CauseUndefined})
		}
	}
	enc := NewEncoder(colTestSchema(), EncodeOptions{})
	took, err := enc.AppendColumnRows(rows, cols)
	if err != nil {
		t.Fatalf("AppendColumnRows: %v", err)
	}
	if !took {
		t.Fatal("refused a column that no row has a value for")
	}
	rec := enc.NewRecord()
	defer rec.Release()
	if rec.NumRows() != rows {
		t.Fatalf("%d rows, want %d", rec.NumRows(), rows)
	}
	for c := range int(rec.NumCols()) {
		if n := rec.Column(c).NullN(); n != rows {
			t.Errorf("column %d has %d nulls, want all %d", c, n, rows)
		}
	}
	for i, st := range enc.Stats() {
		if st.Direct != 0 || st.Nulls[CauseUndefined] != rows {
			t.Errorf("column %d: %+v, want 0 direct and %d undefined", i, st, rows)
		}
	}
}

// TestAppendColumnRowsRefusesAnAllNullColumnThatCannotBeNull: the one thing that shape still has
// to agree with. A non-nullable field has no way to take it, and the per-cell path is where that
// gets reported.
func TestAppendColumnRowsRefusesAnAllNullColumnThatCannotBeNull(t *testing.T) {
	enc := NewEncoder([]Decision{{Name: "I", Verdict: Typed,
		Field: arrow.Field{Name: "I", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}},
		EncodeOptions{})
	took, err := enc.AppendColumnRows(2, []ColumnRows{{
		Nulls: []NullCell{{Row: 0, Cause: CauseUndefined}, {Row: 1, Cause: CauseUndefined}},
	}})
	if err != nil {
		t.Fatalf("refusal must not be an error: %v", err)
	}
	if took {
		t.Fatal("took an all-null column into a non-nullable field")
	}
}

// TestAppendColumnRowsDoesNotDisturbTheProducersSlices: the point of this path is that the
// caller's storage goes to the builder as it is. If the builder or this code wrote through it,
// a producer reusing its buffers across batches -- which is why it has them -- would find them
// changed underneath it, and the damage would show up a batch later, somewhere else.
func TestAppendColumnRowsDoesNotDisturbTheProducersSlices(t *testing.T) {
	const rows, nullRow = 7, 3
	cols, valid := rowsFixture(rows, nullRow)
	ints := append([]int64(nil), cols[0].Ints...)
	reals := append([]float64(nil), cols[1].Reals...)
	strs := append([]string(nil), cols[2].Strs...)
	bools := append([]bool(nil), cols[3].Bools...)
	validWas := append([]bool(nil), valid...)

	enc := NewEncoder(colTestSchema(), EncodeOptions{})
	if took, err := enc.AppendColumnRows(rows, cols); err != nil || !took {
		t.Fatalf("took=%v err=%v", took, err)
	}
	rec := enc.NewRecord()
	defer rec.Release()

	for i := range ints {
		if cols[0].Ints[i] != ints[i] || cols[4].Ints[i] != 100+int64(i) {
			t.Fatalf("an int column was written through at row %d", i)
		}
		if cols[1].Reals[i] != reals[i] || cols[2].Strs[i] != strs[i] || cols[3].Bools[i] != bools[i] {
			t.Fatalf("a value column was written through at row %d", i)
		}
		if valid[i] != validWas[i] {
			t.Fatalf("the validity was written through at row %d", i)
		}
	}
}

// TestAppendColumnRowsRefusesWhatItCannotDoExactly: same contract as the dense path. Anything
// needing a conversion or a per-row decision belongs to coerce, and a batch that does not
// describe itself consistently must be refused WHOLE -- a half-appended batch leaves the record
// ragged and NewRecord panics on it.
func TestAppendColumnRowsRefusesWhatItCannotDoExactly(t *testing.T) {
	allValid := []bool{true, true, true, true}
	for _, tc := range []struct {
		name string
		col  ColumnRows
		why  string
	}{
		{"short values", ColumnRows{Ints: []int64{1, 2}},
			"two values cannot fill four rows"},
		{"two kinds at once", ColumnRows{Ints: []int64{1, 2, 3, 4}, Strs: []string{"a", "b", "c", "d"}},
			"which one is the column is not this path's guess to make"},
		{"no kind at all", ColumnRows{Valid: allValid},
			"a column with no values is not a column"},
		{"validity of the wrong length", ColumnRows{Ints: []int64{1, 2, 3, 4}, Valid: []bool{true, true}},
			"Arrow panics on this, and a half-appended batch cannot be taken back"},
		{"nulls without a validity", ColumnRows{Ints: []int64{1, 2, 3, 4},
			Nulls: []NullCell{{Row: 1, Cause: CauseUndefined}}},
			"nil validity says every row has a value, and Nulls says one does not"},
		{"a real where the schema says integer", ColumnRows{Reals: []float64{1, 2, 3, 4}},
			"the width check belongs to coerce"},
		{"a string where the schema says integer", ColumnRows{Strs: []string{"1", "2", "3", "4"}},
			"parsing a string is a per-row decision"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// One integer column, so the refusal is about the column and not the schema.
			enc := NewEncoder([]Decision{colDec("I", arrow.PrimitiveTypes.Int64)}, EncodeOptions{})
			took, err := enc.AppendColumnRows(4, []ColumnRows{tc.col})
			if err != nil {
				t.Fatalf("refusal must not be an error: %v", err)
			}
			if took {
				t.Fatalf("took a batch it should have refused: %s", tc.why)
			}
			if enc.Rows() != 0 {
				t.Fatalf("refused the batch but appended %d rows", enc.Rows())
			}
		})
	}
}

// TestAppendColumnRowsRefusesANullInANonNullableColumn: the per-cell path reports this per row,
// naming the attribute and the cause. One report in one place is worth more than a fast append.
func TestAppendColumnRowsRefusesANullInANonNullableColumn(t *testing.T) {
	enc := NewEncoder([]Decision{{Name: "I", Verdict: Typed,
		Field: arrow.Field{Name: "I", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}},
		EncodeOptions{})
	took, err := enc.AppendColumnRows(2, []ColumnRows{{
		Ints:  []int64{1, 0},
		Valid: []bool{true, false},
		Nulls: []NullCell{{Row: 1, Cause: CauseUndefined}},
	}})
	if err != nil {
		t.Fatalf("refusal must not be an error: %v", err)
	}
	if took {
		t.Fatal("took a null into a non-nullable column")
	}
}
