package arrowtype

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func fpCol(name string, dt arrow.DataType, nullable bool) Decision {
	return Decision{
		Name:    name,
		Verdict: Typed,
		Field:   arrow.Field{Name: name, Type: dt, Nullable: nullable},
		Reason:  "single-class",
		Detail:  "every sampled value was the same kind",
	}
}

// TestFingerprintIsContentAddressed is the property the whole mechanism rests on: deriving the
// same schema twice produces the same name. If it did not -- if the hash picked up anything
// that varies between two derivations of an unchanged table -- then every DoGet would refuse
// and the pin would be a denial of service rather than a guarantee.
func TestFingerprintIsContentAddressed(t *testing.T) {
	a := []Decision{
		fpCol("Owner", arrow.BinaryTypes.String, true),
		fpCol("ClusterId", arrow.PrimitiveTypes.Int64, false),
		TailDecision(),
	}
	b := []Decision{
		fpCol("Owner", arrow.BinaryTypes.String, true),
		fpCol("ClusterId", arrow.PrimitiveTypes.Int64, false),
		TailDecision(),
	}
	if Fingerprint(a) != Fingerprint(b) {
		t.Fatalf("two equal schemas got different names: %s vs %s", Fingerprint(a), Fingerprint(b))
	}

	// And the prose does NOT enter it. Reason and Detail are built from counts, so they move
	// with the sample on a table nobody has retyped; hashing them would make an identical
	// schema look like a different one every time a row was written.
	c := append([]Decision(nil), a...)
	c[0].Reason = "int-real-widened"
	c[0].Detail = "1 of 2000 values was a real"
	if Fingerprint(c) != Fingerprint(a) {
		t.Error("the explanation of a decision changed its fingerprint; only the shape should")
	}
}

// TestFingerprintSeparatesWhatAClientReadsByPosition enumerates the changes that must produce a
// different name. Each one of these, delivered under a schema promised before it, is a client
// reading the wrong column or the wrong type out of a buffer.
func TestFingerprintSeparatesWhatAClientReadsByPosition(t *testing.T) {
	base := []Decision{
		fpCol("Owner", arrow.BinaryTypes.String, true),
		fpCol("ClusterId", arrow.PrimitiveTypes.Int64, false),
	}
	want := Fingerprint(base)

	cases := []struct {
		what string
		d    []Decision
	}{
		{"column order", []Decision{base[1], base[0]}},
		{"a column's type", []Decision{base[0], fpCol("ClusterId", arrow.PrimitiveTypes.Float64, false)}},
		{"a timestamp's unit", []Decision{
			fpCol("QDate", &arrow.TimestampType{Unit: arrow.Second}, true),
			fpCol("QDate2", &arrow.TimestampType{Unit: arrow.Millisecond}, true),
		}},
		{"nullability", []Decision{base[0], fpCol("ClusterId", arrow.PrimitiveTypes.Int64, true)}},
		{"a column's name", []Decision{fpCol("owner", arrow.BinaryTypes.String, true), base[1]}},
		{"an extra column", append(append([]Decision(nil), base...), fpCol("Site", arrow.BinaryTypes.String, true))},
		{"a missing column", base[:1]},
		{"a column losing its type", []Decision{base[0], {Name: "ClusterId", Verdict: Untyped, Reason: "mixed-kinds"}}},
		{"the tail", append(append([]Decision(nil), base...), TailDecision())},
	}
	seen := map[string]string{want: "the base schema"}
	for _, c := range cases {
		got := Fingerprint(c.d)
		if c.what == "a timestamp's unit" {
			// This pair is self-contained: both entries are in the same slice, so it
			// is checked against itself rather than against the base.
			if Fingerprint(c.d[:1]) == Fingerprint(c.d[1:]) {
				t.Error("a timestamp in seconds and one in milliseconds got the same name")
			}
			continue
		}
		if got == want {
			t.Errorf("changing %s did not change the fingerprint", c.what)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("changing %s collided with %s", c.what, prev)
		}
		seen[got] = c.what
	}
}

// TestFingerprintIsInjectiveAcrossFieldBoundaries is why the hash is length-prefixed rather
// than delimiter-joined. Two schemas whose concatenated text is identical must still get
// different names, because the split between one field and the next is part of the content.
func TestFingerprintIsInjectiveAcrossFieldBoundaries(t *testing.T) {
	a := []Decision{fpCol("AB", arrow.BinaryTypes.String, true)}
	b := []Decision{fpCol("A", arrow.BinaryTypes.String, true), fpCol("B", arrow.BinaryTypes.String, true)}
	if Fingerprint(a) == Fingerprint(b) {
		t.Error("one column named AB and two named A and B got the same fingerprint")
	}

	// The same question one level down: a name that contains what a delimiter would be.
	c := []Decision{fpCol("A\x00B", arrow.BinaryTypes.String, true)}
	if Fingerprint(c) == Fingerprint(b) {
		t.Error("a NUL inside a name collided with the boundary between two names")
	}

	// And the case that is a genuine collision under concatenation rather than merely a
	// suspicious one. The tail flag is hashed as a marker word, so a column whose field
	// name happens to begin with that word produces the same bytes as a TAIL column
	// without it -- two schemas that an encoder fills by completely different means, one
	// from the row's residue and one from an attribute lookup.
	tail := []Decision{{Name: "A", Verdict: Typed, Tail: true,
		Field: arrow.Field{Name: "B", Type: arrow.BinaryTypes.String}}}
	notTail := []Decision{{Name: "A", Verdict: Typed,
		Field: arrow.Field{Name: "tailB", Type: arrow.BinaryTypes.String}}}
	if Fingerprint(tail) == Fingerprint(notTail) {
		t.Error("a tail column collided with an ordinary one whose field name absorbed the marker")
	}
}

// TestFingerprintOfNothingIsStable: an empty projection is a legitimate schema to promise (a
// table every attribute of which went untyped), and it must have a name like any other rather
// than an empty string that compares equal to "no fingerprint at all".
func TestFingerprintOfNothingIsStable(t *testing.T) {
	empty := Fingerprint(nil)
	if empty == "" {
		t.Fatal("the empty schema has no name, so it cannot be told from an unpinned ticket")
	}
	if empty != Fingerprint([]Decision{}) {
		t.Error("nil and empty decisions got different names")
	}
	if empty == Fingerprint([]Decision{fpCol("Owner", arrow.BinaryTypes.String, true)}) {
		t.Error("the empty schema collided with a one-column one")
	}
}

// TestDefaultTimeAttrsIsFreshAndLowercase pins the two things callers rely on without saying
// so. A caller ADDS its configured attributes to this map, so handing out a shared one would
// let one consumer's configuration leak into another's -- and the Grafana plugin does exactly
// that, adding its own "time" alias. And every lookup folds the name first, so an entry with a
// capital in it would be an entry nothing matches.
func TestDefaultTimeAttrsIsFreshAndLowercase(t *testing.T) {
	a := DefaultTimeAttrs()
	b := DefaultTimeAttrs()
	if len(a) == 0 {
		t.Fatal("no epoch attributes at all")
	}
	a["mycustomtime"] = true
	if b["mycustomtime"] {
		t.Error("two callers share one map, so one's configuration reaches the other")
	}
	for name := range b {
		if name != strings.ToLower(name) {
			t.Errorf("%q is not lowercased, so a folded lookup will never find it", name)
		}
	}
	// The well-known ones a job ad actually carries, since an empty-but-present map would
	// pass everything above.
	for _, want := range []string{"qdate", "completiondate", "jobstartdate"} {
		if !b[want] {
			t.Errorf("%s is missing", want)
		}
	}
}
