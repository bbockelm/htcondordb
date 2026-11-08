package repl

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/arrowtype"
)

func rowValueDecisions(types ...arrow.DataType) []arrowtype.Decision {
	out := make([]arrowtype.Decision, len(types))
	for i, dt := range types {
		out[i] = arrowtype.Decision{Verdict: arrowtype.Typed,
			Field: arrow.Field{Type: dt, Nullable: true}}
	}
	return out
}

func projAd(t *testing.T, pairs ...any) *classad.ClassAd {
	t.Helper()
	ad := classad.New()
	for i := 0; i+1 < len(pairs); i += 2 {
		insertValue(ad, pairs[i].(string), pairs[i+1].(classad.Value))
	}
	return ad
}

// TestArrowRowValues covers the cases a whole query cannot reach on its own: a result
// assembled without a projection, a projection that is short of the rows, a row shorter than
// its columns, and a projected value that disagrees with the column it is going into.
func TestArrowRowValues(t *testing.T) {
	str, in64 := arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64

	t.Run("prefers the projected value over the cell", func(t *testing.T) {
		// The cell reads back as UNDEFINED whatever the column is declared to be,
		// because "undefined" is how a display row spells a missing value. The value
		// is the string of that name, and only the projection can say so.
		r := &Result{IsSelect: true, Columns: []string{"S"},
			Rows:      [][]string{{"undefined"}},
			Projected: []*classad.ClassAd{projAd(t, "S", classad.NewStringValue("undefined"))}}
		out := make([]classad.Value, 1)
		ArrowRowValues(r, 0, rowValueDecisions(str), out)
		got, err := out[0].StringValue()
		if err != nil || got != "undefined" {
			t.Fatalf("the cell is %v (%v), want the string undefined", out[0], err)
		}
	})

	t.Run("more decisions than columns", func(t *testing.T) {
		// A caller whose decisions outrun the result's headers. The extra column has
		// no header to look the projection up by and no cell to read, so it is
		// undefined -- the answer a nullable column takes for a cell nothing fills.
		r := &Result{IsSelect: true, Columns: []string{"A"},
			Rows:      [][]string{{"1"}},
			Projected: []*classad.ClassAd{projAd(t, "A", classad.NewIntValue(1))}}
		out := make([]classad.Value, 2)
		ArrowRowValues(r, 0, rowValueDecisions(in64, in64), out)
		if !out[1].IsUndefined() {
			t.Fatalf("the extra column is %v, want undefined", out[1])
		}
	})

	t.Run("no projection falls to the cell", func(t *testing.T) {
		// A result assembled by hand, or one whose projection was dropped because a
		// row could not be projected. The display text is all there is, and it is read
		// against the DECLARED type rather than guessed at.
		r := &Result{IsSelect: true, Columns: []string{"N"}, Rows: [][]string{{"7"}}}
		out := make([]classad.Value, 1)
		ArrowRowValues(r, 0, rowValueDecisions(in64), out)
		n, err := out[0].IntValue()
		if err != nil || n != 7 {
			t.Fatalf("the cell is %v (%v), want the integer 7", out[0], err)
		}
	})

	t.Run("a projection short of the rows leaves later rows to the cells", func(t *testing.T) {
		r := &Result{IsSelect: true, Columns: []string{"N"},
			Rows:      [][]string{{"7"}, {"8"}},
			Projected: []*classad.ClassAd{projAd(t, "N", classad.NewIntValue(7))}}
		out := make([]classad.Value, 1)
		ArrowRowValues(r, 1, rowValueDecisions(in64), out)
		n, err := out[0].IntValue()
		if err != nil || n != 8 {
			t.Fatalf("row 1 is %v (%v), want the integer 8 -- a projection that does not "+
				"reach this row must not be read for it", out[0], err)
		}
	})

	t.Run("a column the projection cannot name falls to the cell", func(t *testing.T) {
		r := &Result{IsSelect: true, Columns: []string{"A", "B"},
			Rows:      [][]string{{"1", "2"}},
			Projected: []*classad.ClassAd{projAd(t, "A", classad.NewIntValue(1))}}
		out := make([]classad.Value, 2)
		ArrowRowValues(r, 0, rowValueDecisions(in64, in64), out)
		n, err := out[1].IntValue()
		if err != nil || n != 2 {
			t.Fatalf("B is %v (%v), want the integer 2 from its cell", out[1], err)
		}
	})

	t.Run("a row shorter than its columns", func(t *testing.T) {
		r := &Result{IsSelect: true, Columns: []string{"A", "B"}, Rows: [][]string{{"1"}}}
		out := make([]classad.Value, 2)
		ArrowRowValues(r, 0, rowValueDecisions(in64, in64), out)
		if !out[1].IsUndefined() {
			t.Fatalf("the missing cell is %v, want undefined", out[1])
		}
	})

	t.Run("a repeated header falls to the cells", func(t *testing.T) {
		// The projected ad holds one x, the later item's. Both columns reading it
		// would give the first one the second one's VALUE.
		r := &Result{IsSelect: true, Columns: []string{"x", "x"},
			Rows:      [][]string{{"u0", "100"}},
			Projected: []*classad.ClassAd{projAd(t, "x", classad.NewStringValue("100"))}}
		out := make([]classad.Value, 2)
		ArrowRowValues(r, 0, rowValueDecisions(str, str), out)
		s, _ := out[0].StringValue()
		if s != "u0" {
			t.Fatalf("the first x is %q, want u0", s)
		}
	})

	// THE COLUMN'S TYPE IS NOT THE VALUE'S. A column is typed from a measurement of the
	// table, so a value can disagree with it: an attribute the profile's sample saw only
	// as integers, or one an operator declared as epoch seconds (the Flight endpoint does
	// both). The value is passed on as it is and the encoder decides -- a counted null
	// here, a faithful rendering in a text column. Retyping it to fit the declaration is
	// the guess this path exists to delete, and it is what the cell reading does: the
	// string "1700000000" parses as that integer against an int64 or timestamp column.
	t.Run("a value that disagrees with its column is not retyped", func(t *testing.T) {
		r := &Result{IsSelect: true, Columns: []string{"QDate"},
			Rows:      [][]string{{"1700000000"}},
			Projected: []*classad.ClassAd{projAd(t, "QDate", classad.NewStringValue("1700000000"))}}
		out := make([]classad.Value, 1)
		ArrowRowValues(r, 0, rowValueDecisions(arrow.FixedWidthTypes.Timestamp_s), out)
		if !out[0].IsString() {
			t.Fatalf("the cell is %v, want the string it actually holds", out[0].Type())
		}
		// The premise: reading the cell instead is what produces the number.
		if v := ArrowCellValue("1700000000", rowValueDecisions(arrow.FixedWidthTypes.Timestamp_s)[0]); !v.IsInteger() {
			t.Fatalf("the display cell reads as %v, so this test proves nothing", v.Type())
		}
	})
}
