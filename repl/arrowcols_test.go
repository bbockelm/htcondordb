package repl

import (
	"testing"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// TestCellValueReadsTheDeclaredTypeOrNull is the unit test for the one place this path parses
// text. The rule is that a cell is read as the type the schema DECLARED or as null -- never as
// whatever the characters happen to look like, which is the inference this endpoint exists to
// replace.
func TestCellValueReadsTheDeclaredTypeOrNull(t *testing.T) {
	i64 := intColumn("t", "t")
	f64 := realColumn("t", "t")
	str := textColumn("t", "t")

	for _, c := range []struct {
		cell string
		d    arrowtype.Decision
		want string // classad.Value.Type().String() is not stable; compare the rendering
	}{
		{"42", i64, "42"},
		{"-1", i64, "-1"},
		{"1019.5", f64, "1019.5"},
		{"hello", str, `"hello"`},
		// The cases that must NOT retype: a non-numeric cell in a numeric column is a
		// null, not a string column, and "undefined"/"error"/"" are the executor's own
		// spellings for the two ClassAd non-values.
		{"hello", i64, "undefined"},
		{"", i64, "undefined"},
		{"undefined", str, "undefined"},
		{"error", i64, "error"},
	} {
		got := ArrowCellValue(c.cell, c.d).String()
		if got != c.want {
			t.Errorf("cell %q as %v: got %s, want %s", c.cell, c.d.Field.Type, got, c.want)
		}
	}
}
