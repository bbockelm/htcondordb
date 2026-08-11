package main

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/htcondordb/repl"
)

// parseCell types a display string. The interesting cases are the ones where a wrong guess
// would corrupt a report: a string that looks numeric, and the float spellings Go's parser
// accepts but ClassAd never emits.
func TestParseCell(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want any
	}{
		{"undefined", nil},
		{"error", nil},
		{"true", true},
		{"false", false},
		{"42", int64(42)},
		{"-7", int64(-7)},
		{"+5", int64(5)},
		{"0", int64(0)},
		{"3.5", 3.5},
		{"-0.25", -0.25},
		{"1e3", float64(1000)},
		{"1.5E-2", 0.015},
		// ParseFloat accepts these; ClassAd does not render them, so they are strings.
		{"inf", "inf"},
		{"Inf", "Inf"},
		{"NaN", "NaN"},
		{"0x1p-2", "0x1p-2"},
		// Not numbers at all.
		{"", ""},
		{"abc", "abc"},
		{"1.2.3", "1.2.3"},
		{"12abc", "12abc"},
		{".", "."},
		{"-", "-"},
		{"1e", "1e"},
		{"True", "True"},   // ClassAd renders bools lowercase; this is a string
		{" 42", " 42"},     // ParseInt rejects the space, and so do we
		{"1_000", "1_000"}, // Go's underscore digit separators are not ClassAd
	} {
		if got := parseCell(tc.in); got != tc.want {
			t.Errorf("parseCell(%q) = %#v (%T), want %#v (%T)", tc.in, got, got, tc.want, tc.want)
		}
	}
}

// A cell backed by an ad takes the ad's real ClassAd type, so a string attribute whose text
// looks numeric survives as a string -- the fidelity parseCell alone cannot give.
func TestCellValuesPrefersAdTypes(t *testing.T) {
	ad := classad.New()
	ad.InsertAttrString("Owner", "alice")
	ad.InsertAttrString("Ticket", "0042") // numeric-looking string
	ad.InsertAttrString("Version", "1.5") // real-looking string
	ad.InsertAttrString("Flag", "true")   // bool-looking string
	ad.InsertAttr("Memory", 4096)
	ad.InsertAttrFloat("Load", 0.75)
	ad.InsertAttrBool("Wanted", true)

	r := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Owner", "Ticket", "Version", "Flag", "Memory", "Load", "Wanted", "Missing"},
		Rows:     [][]string{{"alice", "0042", "1.5", "true", "4096", "0.75", "true", "undefined"}},
		Ads:      []*classad.ClassAd{ad},
	}

	got := cellValues(r, 0)
	want := []any{"alice", "0042", "1.5", "true", int64(4096), 0.75, true, nil}
	if len(got) != len(want) {
		t.Fatalf("got %d cells, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("cell %d (%s) = %#v (%T), want %#v (%T)",
				i, r.Columns[i], got[i], got[i], want[i], want[i])
		}
	}
}

// With no ad behind the row -- an aggregate, whose values the server already reduced to
// strings -- cells fall back to parsing the display string.
func TestCellValuesAggregateFallback(t *testing.T) {
	r := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Owner", "n", "avg_mem"},
		Rows:     [][]string{{"alice", "3", "2048.5"}},
	}
	got := cellValues(r, 0)
	want := []any{"alice", int64(3), 2048.5}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("cell %d = %#v (%T), want %#v (%T)", i, got[i], got[i], want[i], want[i])
		}
	}
}

// A row shorter than the column list leaves the missing cells null rather than panicking.
func TestCellValuesShortRow(t *testing.T) {
	r := &repl.Result{
		IsSelect: true,
		Columns:  []string{"a", "b", "c"},
		Rows:     [][]string{{"1"}},
	}
	got := cellValues(r, 0)
	if len(got) != 3 {
		t.Fatalf("got %d cells, want 3", len(got))
	}
	if got[0] != int64(1) || got[1] != nil || got[2] != nil {
		t.Errorf("got %#v, want [1 <nil> <nil>]", got)
	}
}

// Composite ClassAd values keep their ClassAd text: a caller would have to parse them
// anyway, and JSON has no spelling for a nested expression.
func TestValueJSONComposites(t *testing.T) {
	ad := classad.New()
	ad.InsertAttrClassAd("Inner", classad.New())
	if v := valueJSON(ad.EvaluateAttr("Inner")); v == nil {
		t.Error("nested ad rendered as null, want its ClassAd text")
	}
	if _, ok := valueJSON(ad.EvaluateAttr("Inner")).(string); !ok {
		t.Error("nested ad did not render as a string")
	}
}

// The JSON document is this library's contract with non-Go callers: an empty result still
// carries columns and rows as arrays (never null), so a client can index them unguarded.
func TestBuildResultEmptySelect(t *testing.T) {
	doc := buildResult(&repl.Result{IsSelect: true}, 0, false)
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round struct {
		Select  bool   `json:"select"`
		Columns []any  `json:"columns"`
		Rows    []any  `json:"rows"`
		Ads     []any  `json:"ads"`
		Note    string `json:"note"`
	}
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !round.Select {
		t.Error("select = false, want true")
	}
	if round.Columns == nil || round.Rows == nil {
		t.Errorf("columns/rows marshaled as null: %s", b)
	}
	if round.Ads != nil {
		t.Errorf("ads present without the hcdbSQLAds option: %s", b)
	}
}

// A DML result reports its affected count and note, and is not a SELECT.
func TestBuildResultDML(t *testing.T) {
	doc := buildResult(&repl.Result{Affected: 3, Note: "UPDATE 3", Duration: 250 * time.Microsecond}, 0, false)
	if doc.Select {
		t.Error("select = true for a DML result")
	}
	if doc.Affected != 3 || doc.Note != "UPDATE 3" {
		t.Errorf("affected/note = %d/%q, want 3/\"UPDATE 3\"", doc.Affected, doc.Note)
	}
	if doc.DurationNS != 250_000 {
		t.Errorf("duration_ns = %d, want 250000", doc.DurationNS)
	}
}

// The ads option adds old-format ClassAd text, one entry per result ad, and only when asked.
func TestBuildResultAdsOption(t *testing.T) {
	ad := classad.New()
	ad.InsertAttrString("Owner", "alice")
	r := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Owner"},
		Rows:     [][]string{{"alice"}},
		Ads:      []*classad.ClassAd{ad},
	}
	if doc := buildResult(r, 0, false); doc.Ads != nil {
		t.Errorf("ads emitted without the option: %v", doc.Ads)
	}
	doc := buildResult(r, hcdbSQLAds, false)
	if len(doc.Ads) != 1 {
		t.Fatalf("got %d ads, want 1", len(doc.Ads))
	}
	if doc.Ads[0] == "" {
		t.Error("ad text is empty")
	}
}

// JSON cannot represent NaN or Infinity, and encoding/json refuses to marshal them -- so without
// a guard one non-finite cell fails the whole batch instead of just itself. Null is lossy but
// bounded; losing a report's entire query to one value is not.
func TestValueJSONNonFinite(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    float64
	}{
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := valueJSON(classad.NewRealValue(tc.f))
			if got != nil {
				t.Fatalf("valueJSON(%v) = %#v, want nil", tc.f, got)
			}
			// The point of the guard: the row it lives in still marshals.
			if _, err := json.Marshal([]any{got, "alice"}); err != nil {
				t.Errorf("a row holding %v failed to marshal: %v", tc.f, err)
			}
		})
	}

	// A finite real is untouched.
	if got := valueJSON(classad.NewRealValue(1.5)); got != 1.5 {
		t.Errorf("valueJSON(1.5) = %#v, want 1.5", got)
	}
}
