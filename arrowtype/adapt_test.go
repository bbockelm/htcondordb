package arrowtype

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestFromProfileDropsNullClasses pins the one thing the adapter does beyond copying fields:
// undefined and error reach the derivation as a null COUNT, never as value classes. Counting
// them as classes is what would send a uniformly-string attribute to a fallback type.
func TestFromProfileDropsNullClasses(t *testing.T) {
	p := collections.AttrTypeProfile{
		Name:    "Site",
		Present: 2000,
		Classes: map[string]int{"string": 25, "undefined": 1974, "error": 1},
	}
	m := FromProfile(p, 2000)

	if got := len(m.ValueClasses); got != 1 {
		t.Errorf("value classes %v, want only string", m.ValueClasses)
	}
	if m.ValueClasses["string"] != 25 {
		t.Errorf("string count %d, want 25", m.ValueClasses["string"])
	}
	if m.Nulls != 1975 {
		t.Errorf("nulls %d, want 1975 (undefined + error)", m.Nulls)
	}
	// And the consequence: it types as a nullable string, not as a fallback.
	d := Derive(m, Options{})
	if d.Verdict != Typed || d.Field.Type.ID() != arrow.STRING || !d.Field.Nullable {
		t.Errorf("Site: %v %v nullable=%v, want a nullable utf8 column",
			d.Verdict, d.Field.Type, d.Field.Nullable)
	}
}

// TestFromProfilePropagatesLossyCount covers the field the whole widening rule leans on. If the
// adapter dropped IntsLossyAsFloat, every int/real column would widen to float64 including the
// ones whose integers do not survive it -- values altered, silently, with the measurement that
// should have prevented it sitting right there unused. (Found by mutation: zeroing the field in
// the adapter passed the entire suite until this existed.)
func TestFromProfilePropagatesLossyCount(t *testing.T) {
	p := collections.AttrTypeProfile{
		Name:             "BytesSent",
		Present:          100,
		Classes:          map[string]int{"int": 50, "real": 50},
		IntsLossyAsFloat: 3,
	}
	m := FromProfile(p, 100)
	if m.LossyAsFloat != 3 {
		t.Fatalf("LossyAsFloat %d, want 3", m.LossyAsFloat)
	}
	// And the consequence: the column must NOT be declared float64.
	if d := Derive(m, Options{}); d.Field.Type.ID() == arrow.FLOAT64 {
		t.Errorf("BytesSent widened to float64 despite %d values past 2^53", m.LossyAsFloat)
	}

	// The contrast, so this does not pass by refusing everything: with no lossy values the
	// same shape does widen.
	p.IntsLossyAsFloat = 0
	if d := Derive(FromProfile(p, 100), Options{}); d.Field.Type.ID() != arrow.FLOAT64 {
		t.Errorf("with no lossy values: type %v, want float64", d.Field.Type.ID())
	}
}

func TestFromProfilesPreservesOrder(t *testing.T) {
	in := []collections.AttrTypeProfile{
		{Name: "A", Present: 10, Classes: map[string]int{"int": 10}},
		{Name: "B", Present: 5, Classes: map[string]int{"int": 5}},
		{Name: "C", Present: 1, Classes: map[string]int{"int": 1}},
	}
	got := FromProfiles(in, 10)
	for i, want := range []string{"A", "B", "C"} {
		if got[i].Name != want {
			t.Fatalf("order %v, want A,B,C", []Measured{got[0], got[1], got[2]})
		}
		if got[i].Sampled != 10 {
			t.Errorf("%s: sampled %d, want 10 on every entry", want, got[i].Sampled)
		}
	}
}

// TestEndToEndStoreToArrow runs the whole chain for the first time: put ads in a real store,
// measure what the attributes hold, derive a schema from the measurement, encode the ads
// against it, and check the Arrow batch. Each piece is unit-tested on its own; this is the one
// that would catch them disagreeing about what they hand each other.
//
// The ad shapes are the ones measured on a production AP, so the chain is exercised on what it
// will actually meet rather than on a tidy invention.
func TestEndToEndStoreToArrow(t *testing.T) {
	c := collections.New(collections.Options{Shards: 1, SegmentSize: 1 << 16})
	defer c.Close()

	const n = 200
	for i := 0; i < n; i++ {
		src := fmt.Sprintf(`Owner = "user%d"
ClusterId = %d
RequestMemory = %d
Requirements = TARGET.Cpus >= RequestCpus
QDate = %d
`, i%5, 1000+i, 2048, 1700000000+i)
		// CpusUsage is the real-data pattern: a float attribute that ClassAd writes
		// without a decimal point when the value happens to be integral.
		if i%10 == 0 {
			src += "CpusUsage = 1\n"
		} else {
			src += fmt.Sprintf("CpusUsage = %d.5\n", i%4)
		}
		// Site is present only sometimes, and explicitly undefined the rest of the time.
		if i%4 == 0 {
			src += `Site = "CHTC"` + "\n"
		} else {
			src += "Site = undefined\n"
		}
		ad, err := classad.ParseOld(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), ad); err != nil {
			t.Fatal(err)
		}
	}

	profs, sampled := c.AttrTypeProfiles(1000)
	if sampled != n {
		t.Fatalf("sampled %d, want %d", sampled, n)
	}
	opts := Options{TimeAttrs: map[string]bool{"qdate": true}}
	schema, decisions := Schema(FromProfiles(profs, sampled), opts)

	byName := map[string]arrow.Field{}
	for _, f := range schema.Fields() {
		byName[f.Name] = f
	}
	want := map[string]arrow.Type{
		"Owner":         arrow.STRING,
		"ClusterId":     arrow.INT64,
		"RequestMemory": arrow.INT64,
		"CpusUsage":     arrow.FLOAT64, // int/real mix, every integer within 2^53
		"Site":          arrow.STRING,  // string + undefined is a nullable string
		"QDate":         arrow.TIMESTAMP,
	}
	for name, wantType := range want {
		f, ok := byName[name]
		if !ok {
			t.Errorf("%s: no column", name)
			continue
		}
		if f.Type.ID() != wantType {
			t.Errorf("%s: type %v, want %v", name, f.Type.ID(), wantType)
		}
	}
	if _, ok := byName["Requirements"]; ok {
		t.Error("Requirements got a typed column; it is expression-valued and belongs in the tail")
	}
	if f := byName["Site"]; !f.Nullable {
		t.Error("Site: not nullable, but it is undefined in most records")
	}

	// Encode every ad against the derived schema.
	enc := NewEncoder(decisions, EncodeOptions{})
	defer enc.Release()
	for _, adText := range mustQueryAll(t, c) {
		enc.Append(adText)
	}
	rec := enc.NewRecord()
	defer rec.Release()

	if int(rec.NumRows()) != n {
		t.Fatalf("%d rows, want %d", rec.NumRows(), n)
	}
	if !rec.Schema().Equal(schema) {
		t.Error("batch schema differs from the derived schema")
	}

	// Site is a string column that is null wherever the ad stored undefined. Getting this
	// right end to end is the point: the store said "25%% string, 75%% undefined", the
	// derivation said "nullable utf8", and the encoder has to produce exactly that.
	siteIdx := rec.Schema().FieldIndices("Site")[0]
	site := rec.Column(siteIdx).(*array.String)
	nulls := 0
	for i := 0; i < site.Len(); i++ {
		if site.IsNull(i) {
			nulls++
		}
	}
	if nulls != n-n/4 {
		t.Errorf("Site: %d nulls, want %d", nulls, n-n/4)
	}

	// And the stats account for every cell of every column.
	for _, st := range enc.Stats() {
		if st.Total() != n {
			t.Errorf("%s: stats total %d, want %d", st.Name, st.Total(), n)
		}
	}
}

// mustQueryAll reads every ad back out of the store, which is how the encoder will receive
// them in service -- from a scan, not from the values that were put in.
func mustQueryAll(t *testing.T, c *collections.Collection) []*classad.ClassAd {
	t.Helper()
	var out []*classad.ClassAd
	for i := 0; ; i++ {
		ad, ok := c.Get([]byte(fmt.Sprintf("k%d", i)))
		if !ok {
			break
		}
		out = append(out, ad)
	}
	if len(out) == 0 {
		t.Fatal("read nothing back out of the store")
	}
	return out
}
