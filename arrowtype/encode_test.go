package arrowtype

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/PelicanPlatform/classad/classad"
)

func ad(t *testing.T, src string) *classad.ClassAd {
	t.Helper()
	a, err := classad.ParseOld(src)
	if err != nil {
		t.Fatalf("ParseOld(%q): %v", src, err)
	}
	return a
}

// col builds a one-column encoder of the given Arrow type, bypassing Derive so a coercion can
// be tested independently of whether Derive would ever produce that pairing.
func col(t *testing.T, name string, dt arrow.DataType, opts EncodeOptions) *Encoder {
	t.Helper()
	return NewEncoder([]Decision{{
		Name: name, Verdict: Typed,
		Field: arrow.Field{Name: name, Type: dt, Nullable: true},
	}}, opts)
}

func encodeOne(t *testing.T, e *Encoder, src string) (arrow.Record, ColumnStats) {
	t.Helper()
	if err := e.Append(ad(t, src)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return e.NewRecord(), e.Stats()[0]
}

// TestEncodeEvaluates is the first rule: an attribute held as an expression is a value once
// evaluated. On a real AP this is most of what looks like heterogeneity -- MemoryUsage is an
// expression in 1996 of 2000 history ads and an integer in all 2000 once evaluated.
func TestEncodeEvaluates(t *testing.T) {
	e := col(t, "X", arrow.PrimitiveTypes.Int64, EncodeOptions{})
	defer e.Release()
	rec, st := encodeOne(t, e, "X = 20 + 22")
	defer rec.Release()

	got := rec.Column(0).(*array.Int64)
	if got.IsNull(0) || got.Value(0) != 42 {
		t.Errorf("X = %v (null=%v), want 42 -- the expression must be evaluated", got.Value(0), got.IsNull(0))
	}
	if st.Direct != 1 {
		t.Errorf("Direct=%d, want 1 (an evaluated integer is not a coercion)", st.Direct)
	}
}

// TestEncodeNullCauses separates the three ways a cell becomes null. They are different facts
// about the data -- the attribute was not there, it was there with no value, or a value could
// not be produced -- and collapsing them would make a profile unable to tell which.
func TestEncodeNullCauses(t *testing.T) {
	cases := []struct {
		src  string
		want Cause
	}{
		{"Y = 1", CauseAbsent},            // X not in the ad at all
		{"X = undefined", CauseUndefined}, // present, no value
		{"X = error", CauseError},         // present, could not be produced
		{`X = "text"`, CauseUncoercible},  // present, wrong kind for the column
	}
	for _, c := range cases {
		e := col(t, "X", arrow.PrimitiveTypes.Int64, EncodeOptions{})
		rec, st := encodeOne(t, e, c.src)
		if !rec.Column(0).IsNull(0) {
			t.Errorf("%s: not null, want null", c.src)
		}
		if st.Nulls[c.want] != 1 {
			t.Errorf("%s: nulls %v, want one %v", c.src, st.Nulls, c.want)
		}
		rec.Release()
		e.Release()
	}
}

// TestEncodeStringIntoNumberIsRefused pins the one coercion deliberately absent. A stringified
// number written into a numeric attribute is a bug class this tree has hit; parsing it here
// would make that type error look like clean data.
func TestEncodeStringIntoNumberIsRefused(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float64} {
		e := col(t, "X", dt, EncodeOptions{})
		rec, st := encodeOne(t, e, `X = "42"`)
		if !rec.Column(0).IsNull(0) {
			t.Errorf("%v: parsed \"42\"; it must stay uncoercible", dt)
		}
		if st.Nulls[CauseUncoercible] != 1 {
			t.Errorf("%v: nulls %v, want one uncoercible", dt, st.Nulls)
		}
		rec.Release()
		e.Release()
	}
}

func TestEncodeExactCoercions(t *testing.T) {
	cases := []struct {
		name string
		dt   arrow.DataType
		src  string
		want float64 // compared numerically; strings checked separately
	}{
		{"bool into int", arrow.PrimitiveTypes.Int64, "X = true", 1},
		{"bool false into int", arrow.PrimitiveTypes.Int64, "X = false", 0},
		{"integral real into int", arrow.PrimitiveTypes.Int64, "X = 7.0", 7},
		{"int into float", arrow.PrimitiveTypes.Float64, "X = 7", 7},
		{"bool into float", arrow.PrimitiveTypes.Float64, "X = true", 1},
	}
	for _, c := range cases {
		e := col(t, "X", c.dt, EncodeOptions{})
		rec, st := encodeOne(t, e, c.src)
		if rec.Column(0).IsNull(0) {
			t.Errorf("%s: null, want %v", c.name, c.want)
		} else {
			var got float64
			switch a := rec.Column(0).(type) {
			case *array.Int64:
				got = float64(a.Value(0))
			case *array.Float64:
				got = a.Value(0)
			}
			if got != c.want {
				t.Errorf("%s: %v, want %v", c.name, got, c.want)
			}
		}
		if st.CoercedExact != 1 || st.CoercedLossy != 0 {
			t.Errorf("%s: exact=%d lossy=%d, want exact=1 lossy=0", c.name, st.CoercedExact, st.CoercedLossy)
		}
		rec.Release()
		e.Release()
	}
}

// TestEncodeFractionalRealIntoIntIsNotTruncated: truncating would silently change the value,
// which is the opposite of what coercing-where-exact means.
func TestEncodeFractionalRealIntoIntIsNotTruncated(t *testing.T) {
	e := col(t, "X", arrow.PrimitiveTypes.Int64, EncodeOptions{})
	defer e.Release()
	rec, st := encodeOne(t, e, "X = 7.5")
	defer rec.Release()
	if !rec.Column(0).IsNull(0) {
		t.Errorf("7.5 stored as %v; it must not be truncated", rec.Column(0).(*array.Int64).Value(0))
	}
	if st.Nulls[CauseUncoercible] != 1 {
		t.Errorf("nulls %v, want one uncoercible", st.Nulls)
	}
}

// TestEncodeLossyIntIntoFloat is the 2^53 case counted rather than hidden. Derive refuses to
// declare a float column when the measurement saw such a value, so reaching here means the data
// moved since -- which is exactly what the counter is for.
func TestEncodeLossyIntIntoFloat(t *testing.T) {
	e := col(t, "X", arrow.PrimitiveTypes.Float64, EncodeOptions{})
	defer e.Release()
	if err := e.Append(ad(t, "X = 9007199254740993")); err != nil { // 2^53 + 1
		t.Fatal(err)
	}
	if err := e.Append(ad(t, "X = 9007199254740992")); err != nil { // 2^53, exact
		t.Fatal(err)
	}
	rec := e.NewRecord()
	defer rec.Release()
	st := e.Stats()[0]
	if st.CoercedLossy != 1 {
		t.Errorf("lossy=%d, want 1 (only the value past 2^53)", st.CoercedLossy)
	}
	if st.CoercedExact != 1 {
		t.Errorf("exact=%d, want 1 (2^53 itself is representable)", st.CoercedExact)
	}
}

func TestEncodeTextColumnTakesAnything(t *testing.T) {
	for _, src := range []string{`X = "hi"`, "X = 42", "X = 4.5", "X = true", "X = {1, 2}", "X = [a = 1]"} {
		e := col(t, "X", arrow.BinaryTypes.String, EncodeOptions{})
		rec, st := encodeOne(t, e, src)
		if rec.Column(0).IsNull(0) {
			t.Errorf("%s: null in a text column, which should render anything", src)
		}
		if st.Nulls[CauseUncoercible] != 0 {
			t.Errorf("%s: uncoercible in a text column", src)
		}
		rec.Release()
		e.Release()
	}
}

func TestEncodeBoolColumnRefusesNumbers(t *testing.T) {
	e := col(t, "X", arrow.FixedWidthTypes.Boolean, EncodeOptions{})
	defer e.Release()
	rec, st := encodeOne(t, e, "X = 1")
	defer rec.Release()
	if !rec.Column(0).IsNull(0) {
		t.Error("1 became a boolean; a bool column getting a number means the data disagrees with the measurement")
	}
	if st.Nulls[CauseUncoercible] != 1 {
		t.Errorf("nulls %v, want one uncoercible", st.Nulls)
	}
}

func TestEncodeTimestamp(t *testing.T) {
	e := col(t, "QDate", arrow.FixedWidthTypes.Timestamp_s, EncodeOptions{})
	defer e.Release()
	if err := e.Append(ad(t, "QDate = 1700000000")); err != nil {
		t.Fatal(err)
	}
	if err := e.Append(ad(t, "QDate = 1700000000.5")); err != nil { // sub-second: lossy
		t.Fatal(err)
	}
	rec := e.NewRecord()
	defer rec.Release()
	got := rec.Column(0).(*array.Timestamp)
	if int64(got.Value(0)) != 1700000000 {
		t.Errorf("row 0 = %v, want 1700000000", got.Value(0))
	}
	st := e.Stats()[0]
	if st.CoercedLossy != 1 {
		t.Errorf("lossy=%d, want 1 -- sub-second precision is dropped and must be counted", st.CoercedLossy)
	}
}

func TestEncodeStrictFailsInsteadOfNulling(t *testing.T) {
	strict := EncodeOptions{Strict: true}

	e := col(t, "X", arrow.PrimitiveTypes.Int64, strict)
	defer e.Release()
	if err := e.Append(ad(t, `X = "text"`)); err == nil {
		t.Error("strict: uncoercible value accepted, want an error")
	}

	e2 := col(t, "X", arrow.PrimitiveTypes.Float64, strict)
	defer e2.Release()
	if err := e2.Append(ad(t, "X = 9007199254740993")); err == nil {
		t.Error("strict: lossy coercion accepted, want an error")
	}

	// A null from absence or undefined is NOT a failure: the column is nullable and a
	// missing value is ordinary. Strict is about values that would be altered, not absent.
	e3 := col(t, "X", arrow.PrimitiveTypes.Int64, strict)
	defer e3.Release()
	if err := e3.Append(ad(t, "Y = 1")); err != nil {
		t.Errorf("strict: absence rejected (%v); absence is not a coercion", err)
	}
}

// TestEncoderSkipsUntypedAndHoldsSchema covers the two structural contracts: an untyped
// decision contributes no column, and the schema is fixed for the encoder's life -- a consumer
// cannot accept a column changing type partway through a stream.
func TestEncoderSkipsUntypedAndHoldsSchema(t *testing.T) {
	decisions := []Decision{
		{Name: "Owner", Verdict: Typed, Field: arrow.Field{Name: "Owner", Type: arrow.BinaryTypes.String, Nullable: true}},
		{Name: "Requirements", Verdict: Untyped, Reason: "expression-valued"},
		{Name: "Cpus", Verdict: Typed, Field: arrow.Field{Name: "Cpus", Type: arrow.PrimitiveTypes.Int64, Nullable: true}},
	}
	e := NewEncoder(decisions, EncodeOptions{})
	defer e.Release()
	if n := e.Schema().NumFields(); n != 2 {
		t.Fatalf("%d fields, want 2", n)
	}

	for i := 0; i < 3; i++ {
		if err := e.Append(ad(t, `Owner = "alice"`+"\nCpus = 4\nRequirements = Cpus > 2")); err != nil {
			t.Fatal(err)
		}
	}
	rec1 := e.NewRecord()
	defer rec1.Release()
	if rec1.NumRows() != 3 {
		t.Errorf("%d rows, want 3", rec1.NumRows())
	}
	// A second batch carries the identical schema.
	if err := e.Append(ad(t, `Owner = "bob"`+"\nCpus = 8")); err != nil {
		t.Fatal(err)
	}
	rec2 := e.NewRecord()
	defer rec2.Release()
	if !rec1.Schema().Equal(rec2.Schema()) {
		t.Error("batches carry different schemas")
	}
}

// TestStatsAccountForEveryCell: a cell that fell through the switch without being counted would
// leave the stats quietly short, so the totals are checked against the rows rather than trusted.
func TestStatsAccountForEveryCell(t *testing.T) {
	e := col(t, "X", arrow.PrimitiveTypes.Int64, EncodeOptions{})
	defer e.Release()
	srcs := []string{"X = 1", "X = true", "X = 2.0", "X = 2.5", `X = "s"`, "X = undefined", "X = error", "Y = 9"}
	for _, s := range srcs {
		if err := e.Append(ad(t, s)); err != nil {
			t.Fatal(err)
		}
	}
	rec := e.NewRecord()
	defer rec.Release()
	if got := e.Stats()[0].Total(); got != len(srcs) {
		t.Errorf("stats total %d, want %d -- every cell must be accounted for", got, len(srcs))
	}
	if int(rec.NumRows()) != len(srcs) {
		t.Errorf("%d rows, want %d", rec.NumRows(), len(srcs))
	}
}
