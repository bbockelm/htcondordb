package flightsvc

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// TestColumnValuesOfTranslatesEachKind: the relay's dense arrays go to the encoder as they are.
// Anything copied here would be the copy this path exists to avoid, so the test asserts the
// slices are the SAME ones, not merely equal.
func TestColumnValuesOfTranslatesEachKind(t *testing.T) {
	ints := []int64{1, 2}
	reals := []float64{1.5}
	strs := []string{"a"}
	bools := []bool{true}
	for _, tc := range []struct {
		name string
		in   dbrpc.ValueColumn
		want func(*arrowtype.ColumnValues) bool
	}{
		{"int", dbrpc.ValueColumn{Kind: dbrpc.KindInt, Ints: ints},
			func(o *arrowtype.ColumnValues) bool { return &o.Ints[0] == &ints[0] }},
		{"real", dbrpc.ValueColumn{Kind: dbrpc.KindReal, Reals: reals},
			func(o *arrowtype.ColumnValues) bool { return &o.Reals[0] == &reals[0] }},
		{"string", dbrpc.ValueColumn{Kind: dbrpc.KindString, Strs: strs},
			func(o *arrowtype.ColumnValues) bool { return &o.Strs[0] == &strs[0] }},
		{"bool", dbrpc.ValueColumn{Kind: dbrpc.KindBool, Bools: bools},
			func(o *arrowtype.ColumnValues) bool { return &o.Bools[0] == &bools[0] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out arrowtype.ColumnValues
			if !columnValuesOf(&tc.in, 2, &out) {
				t.Fatal("refused a column it describes")
			}
			if !tc.want(&out) {
				t.Error("the dense values were copied rather than passed through")
			}
		})
	}
}

// TestColumnValuesOfTurnsNullExceptionsIntoNulls: undefined and error cells travel in the
// exception list and are much the commonest thing in it -- an attribute a projection asks for and
// most records do not have. Refusing the batch over one would give the fast path back on exactly
// the columns it is for.
func TestColumnValuesOfTurnsNullExceptionsIntoNulls(t *testing.T) {
	in := dbrpc.ValueColumn{
		Kind: dbrpc.KindInt,
		Ints: []int64{7},
		Exceptions: []dbrpc.ValueException{
			{Row: 1, Value: classad.NewUndefinedValue()},
			{Row: 2, Value: classad.NewErrorValue()},
		},
	}
	var out arrowtype.ColumnValues
	if !columnValuesOf(&in, 3, &out) {
		t.Fatal("refused a column whose only exceptions are nulls")
	}
	if len(out.Nulls) != 2 {
		t.Fatalf("got %d nulls, want 2", len(out.Nulls))
	}
	if out.Nulls[0].Cause != arrowtype.CauseUndefined || out.Nulls[1].Cause != arrowtype.CauseError {
		t.Errorf("causes %v and %v; undefined and error are different answers to "+
			"why a column is sparse", out.Nulls[0].Cause, out.Nulls[1].Cause)
	}
}

// TestColumnValuesOfRefusesWhatCoerceOwns: a scalar of another kind may coerce or may not, and
// deciding that is coerce's job. A fast path that guessed would be a second set of rules.
func TestColumnValuesOfRefusesWhatCoerceOwns(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   dbrpc.ValueColumn
	}{
		{"a string where the column packs ints", dbrpc.ValueColumn{
			Kind: dbrpc.KindInt, Ints: []int64{1},
			Exceptions: []dbrpc.ValueException{{Row: 1, Value: classad.NewStringValue("x")}},
		}},
		{"a non-scalar", dbrpc.ValueColumn{
			Kind: dbrpc.KindInt, Ints: []int64{1},
			Exceptions: []dbrpc.ValueException{{Row: 1, Value: classad.NewStringValue("{1,2}"), NonScalar: true}},
		}},
		{"a column with no dense kind", dbrpc.ValueColumn{Kind: dbrpc.KindNone}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out arrowtype.ColumnValues
			if columnValuesOf(&tc.in, 2, &out) {
				t.Error("took a column whose values coerce decides")
			}
		})
	}
}

// TestColumnValuesOfReusesItsNullSlice: the translation runs per column per batch, so allocating
// a null list each time would be a per-batch cost in the path that exists to remove per-cell ones.
func TestColumnValuesOfReusesItsNullSlice(t *testing.T) {
	in := dbrpc.ValueColumn{Kind: dbrpc.KindInt, Ints: []int64{1},
		Exceptions: []dbrpc.ValueException{{Row: 1, Value: classad.NewUndefinedValue()}}}
	var out arrowtype.ColumnValues
	if !columnValuesOf(&in, 2, &out) {
		t.Fatal("refused")
	}
	first := &out.Nulls[0]
	if !columnValuesOf(&in, 2, &out) {
		t.Fatal("refused on the second pass")
	}
	if &out.Nulls[0] != first {
		t.Error("the null list was reallocated between batches")
	}
}
