package arrowtype

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/apache/arrow-go/v18/arrow"
)

// A record batch is columnar, so a row must reach every builder or none. Append used to stop
// at the first failing cell, leaving the later columns a row short; the next NewRecord then
// panicked on the ragged result ("some fields have excessive number of rows") instead of
// returning anything. A panic is not a reportable error -- in a daemon it is an outage -- and
// it was reachable from the documented Strict option on ordinary data.
func TestAppendKeepsTheRowAlignedWhenACellFails(t *testing.T) {
	ds := []Decision{
		{Name: "A", Verdict: Typed, Field: arrow.Field{Name: "A", Type: arrow.PrimitiveTypes.Int64, Nullable: true}},
		{Name: "B", Verdict: Typed, Field: arrow.Field{Name: "B", Type: arrow.PrimitiveTypes.Int64, Nullable: true}},
		{Name: "C", Verdict: Typed, Field: arrow.Field{Name: "C", Type: arrow.PrimitiveTypes.Int64, Nullable: true}},
	}
	for _, strict := range []bool{false, true} {
		enc := NewEncoder(ds, EncodeOptions{Strict: strict})
		ad := classad.New()
		ad.InsertAttr("A", 1)
		ad.InsertAttrString("B", "not-an-int") // uncoercible into an int column
		ad.InsertAttr("C", 3)

		err := enc.Append(ad)
		if strict && err == nil {
			t.Errorf("strict: Append reported no error for an uncoercible cell")
		}
		rec := enc.NewRecord() // panicked here before the row was completed on every column
		for i := 0; i < int(rec.NumCols()); i++ {
			if got := rec.Column(i).Len(); got != 1 {
				t.Errorf("strict=%v: column %q holds %d rows, want 1; the batch is ragged",
					strict, rec.Schema().Field(i).Name, got)
			}
		}
		if rec.NumRows() != 1 {
			t.Errorf("strict=%v: NumRows = %d, want 1", strict, rec.NumRows())
		}
		rec.Release()
		enc.Release()
	}
}

// Declaring a column non-nullable and then handing it a null produced a record whose data
// contradicted its own schema, and said nothing: Append returned nil even under Strict. A
// consumer is entitled to skip the validity bitmap of a non-nullable field, so this is not a
// cosmetic disagreement.
func TestNonNullableColumnReportsANull(t *testing.T) {
	no := false
	m := Measured{Name: "N", Sampled: 10, Present: 10, ValueClasses: map[string]int{"int": 10}}
	d := Derive(m, Options{Overrides: map[string]Override{"n": {Nullable: &no}}})
	if d.Field.Nullable {
		t.Fatalf("the declaration did not take: field is nullable")
	}

	enc := NewEncoder([]Decision{d}, EncodeOptions{})
	defer enc.Release()
	if err := enc.Append(classad.New()); err == nil {
		t.Errorf("Append reported no error for a null in a column declared non-nullable")
	}
	st := enc.Stats()[0]
	if st.Nulls[CauseNonNullable] != 1 {
		t.Errorf("CauseNonNullable count = %d, want 1; the violation must be countable, "+
			"not only returned once", st.Nulls[CauseNonNullable])
	}
	// The row is still complete, so the batch stays well-formed for a caller that ignores
	// the error -- wrong about that column, but not ragged.
	rec := enc.NewRecord()
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Errorf("NumRows = %d, want 1", rec.NumRows())
	}
}

// An override is a declaration, not a hint. TimeAttrs used to convert a declared int column to
// a timestamp, which left no way to say "this is epoch seconds and I want the raw number" --
// the one reason to declare int on a time attribute at all.
func TestDeclaredKindOutranksTimeAttrs(t *testing.T) {
	m := Measured{Name: "QDate", Sampled: 10, Present: 10, ValueClasses: map[string]int{"int": 10}}

	d := Derive(m, Options{
		Overrides: map[string]Override{"qdate": {Kind: "int"}},
		TimeAttrs: map[string]bool{"qdate": true},
	})
	if d.Field.Type.ID() != arrow.INT64 {
		t.Errorf("declared Kind=int with TimeAttrs set gave %v, want int64", d.Field.Type)
	}

	// Without a declaration, TimeAttrs still governs.
	d2 := Derive(m, Options{TimeAttrs: map[string]bool{"qdate": true}})
	if d2.Field.Type.ID() != arrow.TIMESTAMP {
		t.Errorf("TimeAttrs alone gave %v, want a timestamp", d2.Field.Type)
	}
	// And a declared timestamp is still reachable.
	d3 := Derive(m, Options{Overrides: map[string]Override{"qdate": {Kind: "timestamp"}}})
	if d3.Field.Type.ID() != arrow.TIMESTAMP {
		t.Errorf("declared Kind=timestamp gave %v, want a timestamp", d3.Field.Type)
	}
}

// ClassAd names are case-insensitive, so two measurements differing only in case are one
// attribute. Emitting a column for each put two columns in the schema for it, with conflicting
// types -- a disagreement published as two facts.
func TestSchemaDoesNotEmitTwoColumnsForOneAttribute(t *testing.T) {
	sc, ds := Schema([]Measured{
		{Name: "Owner", Sampled: 5, Present: 5, ValueClasses: map[string]int{"string": 5}},
		{Name: "owner", Sampled: 5, Present: 5, ValueClasses: map[string]int{"int": 5}},
	}, Options{})

	if n := len(sc.Fields()); n != 1 {
		names := make([]string, 0, n)
		for _, f := range sc.Fields() {
			names = append(names, f.Name)
		}
		t.Errorf("schema has %d fields %v, want 1: the two names are one attribute", n, names)
	}
	if len(ds) != 2 {
		t.Fatalf("got %d decisions, want one per input", len(ds))
	}
	if ds[1].Verdict != Untyped || ds[1].Reason != "duplicate-name" {
		t.Errorf("the second measurement decided %v/%q, want Untyped/duplicate-name; a caller "+
			"has to be able to see that it was folded away", ds[1].Verdict, ds[1].Reason)
	}
}
