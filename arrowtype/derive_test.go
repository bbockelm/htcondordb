package arrowtype

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func m(name string, sampled, present int, classes map[string]int) Measured {
	return Measured{Name: name, Sampled: sampled, Present: present, ValueClasses: classes}
}

func TestDeriveSingleClass(t *testing.T) {
	cases := []struct {
		class string
		want  arrow.Type
	}{
		{"int", arrow.INT64},
		{"real", arrow.FLOAT64},
		{"bool", arrow.BOOL},
		{"string", arrow.STRING},
	}
	for _, c := range cases {
		d := Derive(m("X", 100, 100, map[string]int{c.class: 100}), Options{})
		if d.Verdict != Typed {
			t.Errorf("%s: verdict %v, want Typed (%s)", c.class, d.Verdict, d.Detail)
			continue
		}
		if d.Field.Type.ID() != c.want {
			t.Errorf("%s: type %v, want %v", c.class, d.Field.Type.ID(), c.want)
		}
		if d.Field.Nullable {
			t.Errorf("%s: nullable, want not -- present in every record with no nulls", c.class)
		}
		if d.Reason != "single-class" {
			t.Errorf("%s: reason %q, want single-class", c.class, d.Reason)
		}
	}
}

// TestDeriveNullability separates the two independent ways a column admits nulls. They are
// different facts -- absent from the record, versus present and holding undefined -- and a
// derivation that noticed only one would produce a non-nullable column that then has to hold a
// null.
func TestDeriveNullability(t *testing.T) {
	full := Derive(m("X", 100, 100, map[string]int{"int": 100}), Options{})
	if full.Field.Nullable {
		t.Error("fully present, no nulls: nullable, want not")
	}

	absent := Derive(m("X", 100, 60, map[string]int{"int": 60}), Options{})
	if !absent.Field.Nullable {
		t.Error("absent from 40 records: not nullable, want nullable")
	}

	stored := Measured{Name: "X", Sampled: 100, Present: 100, Nulls: 10,
		ValueClasses: map[string]int{"int": 90}}
	if d := Derive(stored, Options{}); !d.Field.Nullable {
		t.Error("present but holding undefined: not nullable, want nullable")
	}
}

// TestDeriveIntRealWidening is the case that matters most on real data: ClassAd writes an
// integral float without a decimal point, so a float-valued attribute arrives as a mix of int
// and real nodes. BytesRecvd was 67 int to 1933 real on a real history table.
func TestDeriveIntRealWidening(t *testing.T) {
	d := Derive(m("BytesRecvd", 2000, 2000, map[string]int{"int": 67, "real": 1933}), Options{})
	if d.Verdict != Typed || d.Field.Type.ID() != arrow.FLOAT64 {
		t.Fatalf("verdict %v type %v, want Typed float64 (%s)", d.Verdict, d.Field.Type, d.Detail)
	}
	if d.Reason != "int-real-widened" {
		t.Errorf("reason %q, want int-real-widened", d.Reason)
	}
}

// TestDeriveIntRealLossyRefusesFloat is the guard that makes the widening above safe. An
// integer past 2^53 shares a float64 with its neighbour, so widening would alter values rather
// than round them -- the store keeps an int64 payload specifically to preserve these.
func TestDeriveIntRealLossyRefusesFloat(t *testing.T) {
	lossy := Measured{Name: "BytesSent", Sampled: 2000, Present: 2000,
		ValueClasses: map[string]int{"int": 1000, "real": 1000}, LossyAsFloat: 1}
	d := Derive(lossy, Options{})
	if d.Field.Type.ID() == arrow.FLOAT64 {
		t.Fatal("widened to float64 despite a value past 2^53")
	}
	if d.Reason != "int-real-lossy" {
		t.Errorf("reason %q, want int-real-lossy", d.Reason)
	}
	if d.Verdict != Typed || d.Field.Type.ID() != arrow.STRING {
		t.Errorf("verdict %v type %v, want a text column by default", d.Verdict, d.Field.Type)
	}
}

func TestDeriveUntypedCases(t *testing.T) {
	cases := []struct {
		name       string
		in         Measured
		wantReason string
	}{
		{
			name:       "nothing but nulls",
			in:         Measured{Name: "X", Sampled: 100, Present: 100, Nulls: 100, ValueClasses: map[string]int{}},
			wantReason: "no-value-observed",
		},
		{
			// Requirements evaluated to a boolean in 1685 of 2000 real ads, plus 141
			// errors -- a match expression with no machine to match against. Its text
			// is the useful form.
			name:       "predominantly an expression",
			in:         m("Requirements", 2000, 2000, map[string]int{"expr": 1995, "bool": 5}),
			wantReason: "expression-valued",
		},
		{
			name:       "list values",
			in:         m("Tags", 100, 100, map[string]int{"list": 100}),
			wantReason: "composite-value",
		},
		{
			name:       "nested ad values",
			in:         m("Nested", 100, 100, map[string]int{"classad": 100}),
			wantReason: "composite-value",
		},
	}
	for _, c := range cases {
		d := Derive(c.in, Options{})
		if d.Verdict != Untyped {
			t.Errorf("%s: verdict %v, want Untyped", c.name, d.Verdict)
		}
		if d.Reason != c.wantReason {
			t.Errorf("%s: reason %q, want %q", c.name, d.Reason, c.wantReason)
		}
		if d.Detail == "" {
			t.Errorf("%s: no detail; an omitted column must say why", c.name)
		}
	}
}

// TestDeriveStrayExpressionDoesNotRetype is a regression test for a real bug, found by running
// the rules over production ads rather than over invented cases -- which is why it is worth
// doing that routinely.
//
// RequestMemory is an integer in 1994 of 1995 records on a live AP and an expression in ONE;
// RequestDisk the same; LeaveJobInQueue is a boolean in 1999 of 2000 and an expression in one.
// Counting that single record as a competing kind made each of them a TEXT column -- an integer
// attribute a reader cannot do arithmetic on, with nothing anywhere to say why. An expression
// is not a type: it is an unevaluated form that becomes one, and the encoder evaluates it.
func TestDeriveStrayExpressionDoesNotRetype(t *testing.T) {
	cases := []struct {
		name string
		in   Measured
		want arrow.Type
	}{
		{"RequestMemory", m("RequestMemory", 1995, 1995, map[string]int{"int": 1994, "expr": 1}), arrow.INT64},
		{"RequestDisk", m("RequestDisk", 1995, 1995, map[string]int{"int": 1994, "expr": 1}), arrow.INT64},
		{"LeaveJobInQueue", m("LeaveJobInQueue", 2000, 2000, map[string]int{"bool": 1999, "expr": 1}), arrow.BOOL},
		// And the int/real widening still applies through a stray expression.
		{"BytesRecvd", m("BytesRecvd", 2000, 2000, map[string]int{"int": 67, "real": 1932, "expr": 1}), arrow.FLOAT64},
	}
	for _, c := range cases {
		d := Derive(c.in, Options{})
		if d.Verdict != Typed {
			t.Errorf("%s: verdict %v, want Typed (%s)", c.name, d.Verdict, d.Detail)
			continue
		}
		if d.Field.Type.ID() != c.want {
			t.Errorf("%s: type %v, want %v -- one expression must not retype the column",
				c.name, d.Field.Type.ID(), c.want)
		}
	}
}

// TestDeriveExpressionShareStillDecidesWhetherToType: dropping expressions from the KIND
// decision must not drop them from the decision of whether to type the attribute at all. A
// formula whose text is the point still gets no column.
func TestDeriveExpressionShareStillDecidesWhetherToType(t *testing.T) {
	// Requirements: 1995 expressions to 5 booleans. Predominantly a formula.
	req := Derive(m("Requirements", 2000, 2000, map[string]int{"expr": 1995, "bool": 5}), Options{})
	if req.Verdict != Untyped || req.Reason != "expression-valued" {
		t.Errorf("Requirements: %v/%s, want Untyped/expression-valued", req.Verdict, req.Reason)
	}

	// Every value an expression and nothing else observed: still nothing to type, and the
	// reason must say it was expressions rather than nulls.
	all := Derive(m("PeriodicHold", 100, 100, map[string]int{"expr": 100}), Options{})
	if all.Verdict != Untyped || all.Reason != "expression-valued" {
		t.Errorf("PeriodicHold: %v/%s, want Untyped/expression-valued", all.Verdict, all.Reason)
	}
}

func TestDeriveMixedKinds(t *testing.T) {
	in := m("Mixed", 100, 100, map[string]int{"int": 90, "string": 10})
	d := Derive(in, Options{})
	if d.Verdict != Typed || d.Field.Type.ID() != arrow.STRING {
		t.Fatalf("verdict %v type %v, want a text column", d.Verdict, d.Field.Type)
	}
	if d.Reason != "mixed-kinds" {
		t.Errorf("reason %q, want mixed-kinds", d.Reason)
	}

	strict := Derive(in, Options{StrictMixedKinds: true})
	if strict.Verdict != Untyped {
		t.Errorf("strict: verdict %v, want Untyped", strict.Verdict)
	}
}

// TestDeriveEpochDeclaration pins that epoch-to-timestamp is a DECLARATION and not a guess: it
// applies to the names given and to no others, and only where the underlying type is numeric.
func TestDeriveEpochDeclaration(t *testing.T) {
	opts := Options{TimeAttrs: map[string]bool{"qdate": true, "completiondate": true}}

	d := Derive(m("QDate", 100, 100, map[string]int{"int": 100}), opts)
	if d.Field.Type.ID() != arrow.TIMESTAMP {
		t.Errorf("QDate: type %v, want timestamp", d.Field.Type)
	}

	// Attribute names are case-insensitive in ClassAd, so the declaration must be too.
	if d2 := Derive(m("qDaTe", 100, 100, map[string]int{"int": 100}), opts); d2.Field.Type.ID() != arrow.TIMESTAMP {
		t.Errorf("qDaTe: type %v, want timestamp (names are case-insensitive)", d2.Field.Type)
	}

	// An undeclared numeric attribute stays numeric: this is the mechanism replacing the
	// hardcoded guessing the Grafana plugin does today.
	if d3 := Derive(m("Cpus", 100, 100, map[string]int{"int": 100}), opts); d3.Field.Type.ID() != arrow.INT64 {
		t.Errorf("Cpus: type %v, want int64 -- not declared a time attribute", d3.Field.Type)
	}

	// A declared name whose values are strings is NOT a timestamp; the declaration says how
	// to read a number, not how to parse text.
	strs := Derive(m("CompletionDate", 100, 100, map[string]int{"string": 100}), opts)
	if strs.Field.Type.ID() != arrow.STRING {
		t.Errorf("CompletionDate (strings): type %v, want utf8", strs.Field.Type)
	}
}

func TestDeriveMetadataCarriesReasonAndClasses(t *testing.T) {
	d := Derive(m("BytesRecvd", 2000, 2000, map[string]int{"int": 67, "real": 1933}), Options{})
	md := d.Field.Metadata
	got := map[string]string{}
	for i, k := range md.Keys() {
		got[k] = md.Values()[i]
	}
	if got["htcondor:reason"] != "int-real-widened" {
		t.Errorf("metadata reason %q", got["htcondor:reason"])
	}
	if got["htcondor:classes"] != "int,real" {
		t.Errorf("metadata classes %q, want \"int,real\" (sorted, so it is stable)", got["htcondor:classes"])
	}
}

// TestSchemaOrderAndOmissions pins the two things a consumer depends on: field order follows
// the input (which the store reports most-present-first), and an attribute that got no column
// is still reported so the caller can route it to the tail rather than discover the omission
// later.
func TestSchemaOrderAndOmissions(t *testing.T) {
	in := []Measured{
		m("Owner", 100, 100, map[string]int{"string": 100}),
		m("Requirements", 100, 100, map[string]int{"expr": 100}),
		m("Cpus", 100, 100, map[string]int{"int": 100}),
	}
	s, decisions := Schema(in, Options{})
	if got := s.NumFields(); got != 2 {
		t.Fatalf("%d fields, want 2 (Requirements is untyped)", got)
	}
	if s.Field(0).Name != "Owner" || s.Field(1).Name != "Cpus" {
		t.Errorf("field order %q,%q, want Owner,Cpus", s.Field(0).Name, s.Field(1).Name)
	}
	if len(decisions) != 3 {
		t.Fatalf("%d decisions, want one per input attribute", len(decisions))
	}
	if decisions[1].Verdict != Untyped || decisions[1].Name != "Requirements" {
		t.Errorf("decision[1] = %+v, want Requirements untyped", decisions[1])
	}
}

// TestDeriveAgainstMeasuredShapes runs the rules over the attribute shapes actually measured on
// a production AP (2000 ads from each of a live queue and a history table), so the rules are
// pinned against real data rather than only against cases invented to exercise them.
func TestDeriveAgainstMeasuredShapes(t *testing.T) {
	opts := Options{TimeAttrs: map[string]bool{"qdate": true}}
	cases := []struct {
		in       Measured
		wantType arrow.Type
		wantKind Verdict
	}{
		// history: BytesRecvd 67 int / 1933 real -- a float attribute written without
		// decimal points.
		{m("BytesRecvd", 2000, 2000, map[string]int{"int": 67, "real": 1933}), arrow.FLOAT64, Typed},
		// history: CpusUsage 2 int / 1998 real.
		{m("CpusUsage", 2000, 2000, map[string]int{"int": 2, "real": 1998}), arrow.FLOAT64, Typed},
		// jobs: Rank 1 int / 1999 real.
		{m("Rank", 2000, 2000, map[string]int{"int": 1, "real": 1999}), arrow.FLOAT64, Typed},
		// history: MemoryUsage is an expression in 1996 of 2000 ads; it evaluates to an
		// integer, but the profile measures stored form, so the typed column comes from
		// the evaluated profile, not this one.
		{m("MemoryUsage", 2000, 2000, map[string]int{"expr": 1996, "int": 4}), arrow.NULL, Untyped},
		// history: MachineAttrGLIDEIN_Site0 is 25 strings against 1975 undefined -- a
		// uniformly string attribute that usually has no value.
		{Measured{Name: "MachineAttrGLIDEIN_Site0", Sampled: 2000, Present: 2000, Nulls: 1975,
			ValueClasses: map[string]int{"string": 25}}, arrow.STRING, Typed},
		// A declared epoch attribute.
		{m("QDate", 2000, 2000, map[string]int{"int": 2000}), arrow.TIMESTAMP, Typed},
	}
	for _, c := range cases {
		d := Derive(c.in, opts)
		if d.Verdict != c.wantKind {
			t.Errorf("%s: verdict %v, want %v (%s)", c.in.Name, d.Verdict, c.wantKind, d.Detail)
			continue
		}
		if d.Verdict == Typed && d.Field.Type.ID() != c.wantType {
			t.Errorf("%s: type %v, want %v", c.in.Name, d.Field.Type.ID(), c.wantType)
		}
	}
}

// ptrBool is a helper for the Nullable override, which is a *bool so that "not declared" and
// "declared false" are distinguishable.
func ptrBool(b bool) *bool { return &b }

// TestOverrideDecidesWhatInferenceCannot is the case this mechanism exists for. MemoryUsage and
// Requirements are indistinguishable by shape on a real AP -- both predominantly expressions,
// both evaluating to one clean kind -- and differ only in what the formula MEANS: MemoryUsage's
// value is the content, Requirements' text is. No measurement separates them.
func TestOverrideDecidesWhatInferenceCannot(t *testing.T) {
	measured := map[string]int{"expr": 1996, "int": 4}
	opts := Options{Overrides: map[string]Override{
		"memoryusage":  {Kind: "int"},
		"requirements": {Exclude: true},
	}}

	// Without the declaration, both are dropped as expression-valued.
	if d := Derive(m("MemoryUsage", 2000, 2000, measured), Options{}); d.Verdict != Untyped {
		t.Fatalf("precondition: MemoryUsage already typed without a declaration (%v)", d.Verdict)
	}

	got := Derive(m("MemoryUsage", 2000, 2000, measured), opts)
	if got.Verdict != Typed || got.Field.Type.ID() != arrow.INT64 {
		t.Errorf("MemoryUsage: %v %v, want a typed int64 column", got.Verdict, got.Field.Type)
	}
	if got.Reason != "override-kind" {
		t.Errorf("MemoryUsage: reason %q, want override-kind so a report shows it was declared", got.Reason)
	}
	// The detail must record what the measurement saw, so a declaration that contradicts the
	// data is visible as a disagreement rather than inferred from an empty column.
	if !strings.Contains(got.Detail, "expr") {
		t.Errorf("MemoryUsage: detail %q does not say what was measured", got.Detail)
	}

	req := Derive(m("Requirements", 2000, 2000, map[string]int{"expr": 1995, "bool": 5}), opts)
	if req.Verdict != Untyped || req.Reason != "override-excluded" {
		t.Errorf("Requirements: %v/%s, want Untyped/override-excluded", req.Verdict, req.Reason)
	}
}

// TestOverrideWinsOverTheMeasurement: a declaration is not a hint. Forcing a kind the data does
// not hold is allowed, because the alternative is an admin who cannot correct a bad inference --
// and it is not silent, because the encoder counts every value it then cannot store.
func TestOverrideWinsOverTheMeasurement(t *testing.T) {
	opts := Options{Overrides: map[string]Override{"x": {Kind: "int"}}}
	d := Derive(m("X", 100, 100, map[string]int{"string": 100}), opts)
	if d.Verdict != Typed || d.Field.Type.ID() != arrow.INT64 {
		t.Fatalf("X: %v %v, want the declared int64", d.Verdict, d.Field.Type)
	}
	if !strings.Contains(d.Detail, "string") {
		t.Errorf("detail %q must record that the measurement saw strings", d.Detail)
	}
}

func TestOverrideNullabilityAndValidation(t *testing.T) {
	// Nullability alone: the kind still comes from the measurement.
	opts := Options{Overrides: map[string]Override{"x": {Nullable: ptrBool(true)}}}
	d := Derive(m("X", 100, 100, map[string]int{"int": 100}), opts)
	if d.Field.Type.ID() != arrow.INT64 {
		t.Errorf("type %v, want int64 from the measurement", d.Field.Type.ID())
	}
	if !d.Field.Nullable {
		t.Error("not nullable, but nullability was declared true")
	}
	if !strings.Contains(d.Reason, "override-nullable") {
		t.Errorf("reason %q should record the declaration", d.Reason)
	}

	// A declared kind that is not a kind is refused rather than silently ignored: ignoring
	// it would leave the admin believing a column was fixed when it was not.
	bad := Options{Overrides: map[string]Override{"x": {Kind: "int64"}}}
	if d := Derive(m("X", 100, 100, map[string]int{"int": 100}), bad); d.Reason != "override-invalid" {
		t.Errorf("reason %q, want override-invalid for an unknown kind", d.Reason)
	}
}

func TestOverrideNameIsCaseInsensitive(t *testing.T) {
	opts := Options{Overrides: map[string]Override{"memoryusage": {Exclude: true}}}
	if d := Derive(m("MeMoRyUsAgE", 100, 100, map[string]int{"int": 100}), opts); d.Verdict != Untyped {
		t.Error("override missed: ClassAd attribute names are case-insensitive")
	}
}
