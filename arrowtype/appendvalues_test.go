package arrowtype

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/apache/arrow-go/v18/arrow"
)

func intColumn(name string) Decision {
	return Decision{
		Name: name, Verdict: Typed,
		Field: arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}
}

// TestAppendValuesCountsWhyEachNullIsNull is the property the undefined and error branches exist
// for, and it is invisible from the data: all three of undefined, error and a value of the wrong
// shape become a null, so a column encoded without the branches looks identical.
//
// What differs is the breakdown an operator reads to find out WHY a column is sparse. Reporting
// "uncoercible" for a cell that simply had no value says the data is the wrong shape when it is
// not there at all, which points an investigation at the schema instead of at the source.
func TestAppendValuesCountsWhyEachNullIsNull(t *testing.T) {
	enc := NewEncoder([]Decision{intColumn("N")}, EncodeOptions{})
	defer enc.Release()

	for _, v := range []classad.Value{
		classad.NewIntValue(7),
		classad.NewUndefinedValue(),
		classad.NewErrorValue(),
		classad.NewStringValue("not a number"),
	} {
		if err := enc.AppendValues([]classad.Value{v}); err != nil && !strings.Contains(err.Error(), "cannot be stored") {
			t.Fatalf("AppendValues(%v): %v", v, err)
		}
	}
	rec := enc.NewRecord()
	defer rec.Release()
	if rec.NumRows() != 4 {
		t.Fatalf("%d rows, want 4", rec.NumRows())
	}

	st := enc.Stats()[0]
	if st.Direct != 1 {
		t.Errorf("Direct = %d, want 1", st.Direct)
	}
	for cause, want := range map[Cause]int{
		CauseUndefined:   1,
		CauseError:       1,
		CauseUncoercible: 1,
		CauseAbsent:      0,
	} {
		if got := st.Nulls[cause]; got != want {
			t.Errorf("%v = %d, want %d (all of %v)", cause, got, want, st.Nulls)
		}
	}
}

// TestAppendValuesRefusesATailEncoder: the tail is the complement of a whole record, and values
// alone are neither the whole record nor its stored form. Filling it with anything would be an
// invention, and skipping it would leave the batch ragged -- which NewRecord panics on rather
// than reporting, so the refusal has to be here.
func TestAppendValuesRefusesATailEncoder(t *testing.T) {
	enc := NewEncoder([]Decision{intColumn("N"), TailDecision()}, EncodeOptions{})
	defer enc.Release()
	err := enc.AppendValues([]classad.Value{classad.NewIntValue(1), classad.NewUndefinedValue()})
	if err == nil {
		t.Fatal("an encoder with a tail accepted values")
	}
	if !strings.Contains(err.Error(), "tail") {
		t.Errorf("refusal %q does not say the tail is the problem", err)
	}
	if enc.Rows() != 0 {
		t.Errorf("the refused row was counted: Rows = %d", enc.Rows())
	}
}

// TestAppendValuesMatchesAppend: the two entry points are the same encoder, so a row that
// arrives as values and the same row that arrives as an ad have to land identically. They
// differ only in where the evaluation happened, which is not something a reader should be able
// to see.
func TestAppendValuesMatchesAppend(t *testing.T) {
	decisions := []Decision{
		intColumn("ClusterId"),
		{Name: "Owner", Verdict: Typed,
			Field: arrow.Field{Name: "Owner", Type: arrow.BinaryTypes.String, Nullable: true}},
		intColumn("Mem"),
	}
	src := "ClusterId = 12\nOwner = \"alice\"\nMem = ClusterId * 1024\n"
	ad, err := classad.ParseOld(src)
	if err != nil {
		t.Fatal(err)
	}

	fromAd := NewEncoder(decisions, EncodeOptions{})
	defer fromAd.Release()
	if err := fromAd.Append(ad); err != nil {
		t.Fatal(err)
	}
	recAd := fromAd.NewRecord()
	defer recAd.Release()

	fromVals := NewEncoder(decisions, EncodeOptions{})
	defer fromVals.Release()
	vals := make([]classad.Value, len(fromVals.Attrs()))
	for i, name := range fromVals.Attrs() {
		vals[i] = ad.EvaluateAttr(name)
	}
	if err := fromVals.AppendValues(vals); err != nil {
		t.Fatal(err)
	}
	recVals := fromVals.NewRecord()
	defer recVals.Release()

	for c := range int(recAd.NumCols()) {
		a, v := recAd.Column(c).ValueStr(0), recVals.Column(c).ValueStr(0)
		if a != v {
			t.Errorf("%s: from the ad %q, from values %q", decisions[c].Name, a, v)
		}
	}
}
