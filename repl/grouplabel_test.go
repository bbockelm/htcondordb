package repl

import (
	"sort"
	"strings"
	"testing"
)

// A GROUP BY key decides which rows are ONE row of the answer, so it has to be the value and
// not how the value prints. The store used to group on the rendering, which put the integer 4
// and the string "4" in one group: a single row whose count was the sum of two, under a label
// naming one of them, with nothing to say so.
//
// The store now groups on the value and reports a label faithful enough to tell them apart --
// a string arrives quoted. This pins both halves as the executor sees them: the counts are
// per value, and the cell is still display text, where a string appears unquoted as it does
// in every other result row.
func TestGroupByKeysOnTheValueNotItsRendering(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE vals")
	for _, v := range []string{
		"('a', 4)",      // integer
		"('b', '4')",    // the string that prints the same
		"('c', '4')",    //
		"('d', true)",   // boolean
		"('e', 'true')", //
		"('f', 'true')", //
		"('g', 'true')", //
	} {
		mustExec(t, e, "INSERT INTO vals (Key, V) VALUES "+v)
	}

	r := mustExec(t, e, "SELECT V, COUNT(*) FROM vals GROUP BY V")
	var got []string
	for _, row := range r.Rows {
		got = append(got, row[0]+"="+row[1])
	}
	sort.Strings(got)
	// Four groups, not two: the counts are 1 and 2 for the two spellings of 4, and 1 and 3
	// for the two spellings of true. The LABELS collide, which is what display text does --
	// a plain SELECT of these values renders them the same way -- but the rows do not merge.
	want := "4=1 4=2 true=1 true=3"
	if strings.Join(got, " ") != want {
		t.Errorf("GROUP BY V = %v, want %s.\nA merged group reports one row whose count is the "+
			"sum of two values that are not the same value.", got, want)
	}
}

// And the ordinary case: a string group column reaches the result unquoted, like every other
// string cell. The store's label carries quotes because it doubles as the group key.
func TestStringGroupLabelReachesTheRowUnquoted(t *testing.T) {
	e, cleanup := aggExprExec(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner")
	for _, row := range r.Rows {
		if strings.HasPrefix(row[0], `"`) {
			t.Errorf("group cell %q is the store's quoted label, not display text", row[0])
		}
	}
}

// groupDisplay must unquote through the ClassAd string rules rather than by trimming, or a
// value holding a quote or a backslash comes back mangled -- and it must leave everything
// that is not a quoted string exactly as the store wrote it.
func TestGroupDisplay(t *testing.T) {
	for _, tc := range []struct{ label, want string }{
		{`"alice"`, "alice"},
		{`"he said \"hi\""`, `he said "hi"`},
		{`"c:\\tmp"`, `c:\tmp`},
		{`""`, ""},
		{"4", "4"},
		{"-1.5", "-1.5"},
		{"true", "true"},
		{"undefined", "undefined"},
		{"error", "error"},
		{"", ""},
		// Not a terminated string literal: pass it through rather than guess.
		{`"unclosed`, `"unclosed`},
	} {
		if got := groupDisplay(tc.label); got != tc.want {
			t.Errorf("groupDisplay(%q) = %q, want %q", tc.label, got, tc.want)
		}
	}
}
