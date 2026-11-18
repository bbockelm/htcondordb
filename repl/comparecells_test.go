package repl

import (
	"fmt"
	"testing"
)

// INTEGERS PAST 2^53 DO NOT SURVIVE A ROUND TRIP THROUGH float64.
//
// compareCells parsed both cells as float64, so two int64s that differ but share a float64
// came back EQUAL. That is not an ordering bug alone: compareCells also backs sameOrderKey,
// the tie detector for RANK and DENSE_RANK, so two distinct values would be reported tied and
// handed the same rank -- a visibly wrong answer, not a reordering of equals.
//
// 2^53 is 9007199254740992. Every integer above it that is not a multiple of 2 is unrepresentable
// as a float64 and rounds to a neighbour.
func TestCompareCellsOrdersLargeIntegersExactly(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		// The boundary itself and its neighbours.
		{"9007199254740993", "9007199254740992", 1},
		{"9007199254740992", "9007199254740993", -1},
		{"9007199254740993", "9007199254740993", 0},
		// Far past it, where whole runs of integers share a float64.
		{"9223372036854775807", "9223372036854775806", 1},
		{"-9223372036854775808", "-9223372036854775807", -1},
		// And the ordinary cases must not change.
		{"1", "2", -1},
		{"2", "1", 1},
		{"2", "2", 0},
		{"-5", "3", -1},
		{"1.5", "1.25", 1},
		{"2.0", "2", 0},
		{"abc", "abd", -1},
		{"abc", "abc", 0},
		{"10", "9", 1},
		{"x10", "x9", -1}, // not numbers: lexical, so "1" < "9"
	} {
		t.Run(fmt.Sprintf("%s_vs_%s", tc.a, tc.b), func(t *testing.T) {
			if got := compareCells(tc.a, tc.b); got != tc.want {
				t.Errorf("compareCells(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestCompareCellsIsAntisymmetric pins the property a sort depends on: swapping the arguments
// negates the answer. A comparator that says both "a < b" and "b < a" -- or neither, for values
// that differ -- can order a slice differently depending on the input order, which is how a
// rounding bug shows up as an unstable ranking rather than as a wrong number.
func TestCompareCellsIsAntisymmetric(t *testing.T) {
	cells := []string{
		"9007199254740992", "9007199254740993", "9007199254740994",
		"9223372036854775806", "9223372036854775807",
		"0", "1", "-1", "1.5", "2.0", "abc", "", "1e300",
	}
	for _, a := range cells {
		for _, b := range cells {
			ab, ba := compareCells(a, b), compareCells(b, a)
			if ab != -ba {
				t.Errorf("compareCells(%q,%q)=%d but compareCells(%q,%q)=%d", a, b, ab, b, a, ba)
			}
			if a == b && ab != 0 {
				t.Errorf("compareCells(%q,%q)=%d, want 0 for equal cells", a, b, ab)
			}
		}
	}
}
