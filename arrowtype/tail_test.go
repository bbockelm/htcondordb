package arrowtype

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// tailDecisions is one typed column plus the tail, which is the smallest shape that can show
// the tail carrying the complement rather than the whole row.
func tailDecisions() []Decision {
	return []Decision{
		{
			Name: "Owner", Verdict: Typed,
			Field: arrow.Field{Name: "Owner", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		TailDecision(),
	}
}

func tailOf(t *testing.T, rec arrow.Record, row int) (string, bool) {
	t.Helper()
	i := rec.Schema().FieldIndices(TailColumnName)
	if len(i) != 1 {
		t.Fatalf("the record has %d columns named %s, want 1", len(i), TailColumnName)
	}
	col, ok := rec.Column(i[0]).(*array.String)
	if !ok {
		t.Fatalf("%s is %T, want a string array", TailColumnName, rec.Column(i[0]))
	}
	if col.IsNull(row) {
		return "", false
	}
	return col.Value(row), true
}

func appendAds(t *testing.T, enc *Encoder, srcs ...string) arrow.Record {
	t.Helper()
	for _, src := range srcs {
		ad, err := classad.ParseOld(src)
		if err != nil {
			t.Fatal(err)
		}
		_ = enc.Append(ad)
	}
	return enc.NewRecord()
}

// TestTailHoldsTheComplement is the column's whole contract: what a typed column took is not
// repeated here, and what no column took is not lost.
//
// Requirements is the attribute this exists for. Its text is the only useful form of it -- the
// boolean it evaluates to says nothing without a machine to match against -- so no measurement
// will ever give it a column, and without the tail it is simply invisible over this endpoint.
func TestTailHoldsTheComplement(t *testing.T) {
	enc := NewEncoder(tailDecisions(), EncodeOptions{})
	defer enc.Release()
	rec := appendAds(t, enc, "Owner = \"alice\"\nClusterId = 7\nRequirements = TARGET.Cpus >= 4\n")
	defer rec.Release()

	text, ok := tailOf(t, rec, 0)
	if !ok {
		t.Fatal("the tail is null although the row has attributes no column took")
	}
	if strings.Contains(text, "Owner") {
		t.Errorf("the tail repeats a typed column: %s", text)
	}
	for _, want := range []string{"ClusterId", "Requirements"} {
		if !strings.Contains(text, want) {
			t.Errorf("the tail does not carry %s: %s", want, text)
		}
	}

	// It is a ClassAd, not a rendering that happens to look like one: a reader gets the
	// values back out with an ordinary parse.
	ad, err := classad.Parse(text)
	if err != nil {
		t.Fatalf("the tail does not parse as a ClassAd: %v (%s)", err, text)
	}
	if _, found := ad.Lookup("Requirements"); !found {
		t.Errorf("the parsed tail has no Requirements: %s", text)
	}
}

// TestTailKeepsTheStoredExpression is why the column is text. An attribute that lands here is
// one whose evaluated form was not worth a column; evaluating it on the way into the tail would
// throw away the only thing the tail was for.
func TestTailKeepsTheStoredExpression(t *testing.T) {
	enc := NewEncoder(tailDecisions(), EncodeOptions{})
	defer enc.Release()
	rec := appendAds(t, enc, "Owner = \"alice\"\nCpus = 4\nNeeded = Cpus * 2\n")
	defer rec.Release()

	text, _ := tailOf(t, rec, 0)
	if !strings.Contains(text, "Cpus * 2") {
		t.Errorf("the tail does not hold Needed's stored expression: %s", text)
	}
}

// TestTailIsNullWithNothingLeftOver keeps "there was nothing else" distinguishable from "there
// was something and it was empty". A reader filtering on the tail should not have to parse an
// empty ad to learn the row had no extras.
func TestTailIsNullWithNothingLeftOver(t *testing.T) {
	enc := NewEncoder(tailDecisions(), EncodeOptions{})
	defer enc.Release()
	rec := appendAds(t, enc, "Owner = \"alice\"\n")
	defer rec.Release()

	if text, ok := tailOf(t, rec, 0); ok {
		t.Errorf("the tail is %q for a row whose every attribute is a column; want null", text)
	}
}

// TestSchemaAppendsTheTailLast pins the position, because a client reads a record by position:
// putting the tail anywhere but the end would shift every column after it the moment the option
// is turned on.
func TestSchemaAppendsTheTailLast(t *testing.T) {
	ms := []Measured{
		{Name: "Owner", Present: 10, Sampled: 10, ValueClasses: map[string]int{"string": 10}},
		{Name: "ClusterId", Present: 10, Sampled: 10, ValueClasses: map[string]int{"int": 10}},
	}
	plain, _ := Schema(ms, Options{})
	withTail, decisions := Schema(ms, Options{Tail: true})

	if withTail.NumFields() != plain.NumFields()+1 {
		t.Fatalf("the tail added %d fields, want 1", withTail.NumFields()-plain.NumFields())
	}
	for i := range plain.NumFields() {
		if !withTail.Field(i).Equal(plain.Field(i)) {
			t.Fatalf("field %d moved when the tail was added: %v vs %v", i, withTail.Field(i), plain.Field(i))
		}
	}
	last := withTail.Field(withTail.NumFields() - 1)
	if last.Name != TailColumnName || last.Type.ID() != arrow.STRING || !last.Nullable {
		t.Fatalf("the last field is %v, want a nullable utf8 %s", last, TailColumnName)
	}
	if d := decisions[len(decisions)-1]; !d.Tail {
		t.Fatalf("the last decision is %+v, want the tail", d)
	}
}
