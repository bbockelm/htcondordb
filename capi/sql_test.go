package main

import (
	"encoding/json"
	"math"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
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

// cellFixture is a real store behind a real executor: what these tests exercise is the
// typed value the executor carries alongside each row, which only a real SELECT builds.
func cellFixture(t *testing.T) (*repl.Executor, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	// Four strings whose text reads as some other type, and one of every type a cell can
	// hold, so a wrong guess shows up as a wrong Go type rather than a near miss.
	const text = `Owner = "alice"
IntStr = "123"
FloatStr = "3.5"
BoolStr = "true"
UndefStr = "undefined"
ErrStr = "error"
Memory = 4096
Load = 0.75
Wanted = true
Items = {1, 2, 3}
Inner = [ a = 1 ]
`
	if err := d.UpdateOld("k1", text); err != nil {
		t.Fatal(err)
	}
	srv := dbrpc.NewServer(d)
	a, b := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(a)) }()
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(b))
	return repl.NewExecutor(cl, repl.ExecConfig{}), func() { _ = cl.Close(); srv.Close(); _ = d.Close() }
}

// firstRow runs sql and returns its headers and the typed cells of row 0.
func firstRow(t *testing.T, ex *repl.Executor, sql string) ([]string, []any) {
	t.Helper()
	r, err := ex.ExecString(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(r.Rows) == 0 {
		t.Fatalf("%s: no rows", sql)
	}
	return r.Columns, cellValues(r, 0)
}

// An aliased or computed column is typed from the value the SELECT evaluated, not from the
// text the table prints. The string-that-looks-like-a-number cases are the point: `Owner AS
// who` over the string "123" has to stay a string, because nothing downstream can tell a
// number that was always a number from one a display string was guessed into.
func TestCellValuesAliasedColumnsKeepExactTypes(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	for _, tc := range []struct {
		name string
		sql  string
		want any
	}{
		{"string looks like an integer", "SELECT IntStr AS t FROM " + tbl, "123"},
		{"string looks like a real", "SELECT FloatStr AS t FROM " + tbl, "3.5"},
		{"string looks like a bool", "SELECT BoolStr AS t FROM " + tbl, "true"},
		{"string reads undefined", "SELECT UndefStr AS t FROM " + tbl, "undefined"},
		{"string reads error", "SELECT ErrStr AS t FROM " + tbl, "error"},
		{"integer", "SELECT Memory AS t FROM " + tbl, int64(4096)},
		{"real", "SELECT Load AS t FROM " + tbl, 0.75},
		{"bool", "SELECT Wanted AS t FROM " + tbl, true},
		{"list keeps its ClassAd text", "SELECT Items AS t FROM " + tbl, "[1 2 3]"},
		{"nested ad keeps its ClassAd text", "SELECT Inner AS t FROM " + tbl, "[a = 1]"},
		// UNDEFINED and ERROR are both a missing cell to a reporting client, and JSON has
		// one spelling for that.
		{"undefined attribute", "SELECT NoSuchAttr AS t FROM " + tbl, nil},
		{"expression evaluates to error", "SELECT Owner + 1 AS t FROM " + tbl, nil},
		// Computed columns, aliased and not: the header is whatever the SELECT named, and
		// the arithmetic result is an integer rather than text that parses as one.
		{"computed column with alias", "SELECT Memory * 2 AS d FROM " + tbl, int64(8192)},
		{"computed column keeps expression header", "SELECT Memory * 2 FROM " + tbl, int64(8192)},
		{"computed string result", `SELECT strcat(IntStr, "4") AS t FROM ` + tbl, "1234"},
		// A plain column was already exact and stays so.
		{"plain column", "SELECT IntStr FROM " + tbl, "123"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cells := firstRow(t, ex, tc.sql)
			if len(cells) != 1 {
				t.Fatalf("got %d cells, want 1", len(cells))
			}
			if cells[0] != tc.want {
				t.Errorf("cell = %#v (%T), want %#v (%T)", cells[0], cells[0], tc.want, tc.want)
			}
		})
	}
}

// The projection answers the question the SELECT asked; the matched ad answers the one the
// scan answered. An alias may reuse an attribute name the matched ad also carries under a
// different value, and the aliased value is the one the caller selected.
//
// The expression column is what makes the two disagree: a SELECT of plain columns alone is
// served by a projected fetch, so the matched ad carries only the attributes the SELECT
// named and has nothing to disagree with. An expression column takes the whole-ad path.
func TestCellValuesProjectionBeatsMatchedAd(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	cols, cells := firstRow(t, ex, `SELECT strcat(Owner, "") AS c, IntStr AS Owner FROM `+dbrpc.DefaultTable)
	if len(cols) != 2 || cols[1] != "Owner" {
		t.Fatalf("headers = %q, want the second to be Owner", cols)
	}
	if cells[1] != "123" {
		t.Errorf("Owner cell = %#v (%T), want %#v -- the matched ad's own Owner is \"alice\"",
			cells[1], cells[1], "123")
	}
}

// An aggregate's rows were never ads: the server reduced them to strings before they
// reached the client, so there is no typed value to read and the display string is all
// there is. A numeric-looking string grouped on still arrives as a string only because
// GROUP BY keys are rendered from the key value -- an aggregate output cannot be exact, and
// this test exists so that gap is written down rather than discovered.
func TestCellValuesAggregatesFallBackToDisplayStrings(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	r, err := ex.ExecString("SELECT COUNT(*) AS n, AVG(Memory) AS m FROM " + tbl)
	if err != nil {
		t.Fatal(err)
	}
	if r.Projected != nil {
		t.Fatalf("aggregate carries a projection; this test no longer covers the fallback")
	}
	cells := cellValues(r, 0)
	if cells[0] != int64(1) || cells[1] != int64(4096) {
		t.Errorf("aggregate cells = %#v, want [1 4096]", cells)
	}

	// Grouping on a numeric-looking string: the group key is parsed back out of its
	// display string, so it arrives as a number. That is the remaining gap.
	_, grouped := firstRow(t, ex, "SELECT IntStr, COUNT(*) AS n FROM "+tbl+" GROUP BY IntStr")
	if grouped[0] != int64(123) {
		t.Errorf("group key = %#v (%T); the aggregate path parses display strings, so a "+
			"numeric-looking key is expected to arrive as int64(123)", grouped[0], grouped[0])
	}
}

// SELECT * is unchanged: it carries no projection (its matched ads already are the
// projection), so every cell comes from the matched ad, exactly as before.
func TestCellValuesStarReadsMatchedAds(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	r, err := ex.ExecString("SELECT * FROM " + dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	if r.Projected != nil {
		t.Fatal("SELECT * carries a projection; this test no longer covers the matched-ad path")
	}
	cells := cellValues(r, 0)
	want := map[string]any{
		"Owner": "alice", "IntStr": "123", "FloatStr": "3.5", "BoolStr": "true",
		"UndefStr": "undefined", "ErrStr": "error", "Memory": int64(4096),
		"Load": 0.75, "Wanted": true,
	}
	for j, col := range r.Columns {
		w, ok := want[col]
		if !ok {
			continue
		}
		if cells[j] != w {
			t.Errorf("SELECT * cell %s = %#v (%T), want %#v (%T)", col, cells[j], cells[j], w, w)
		}
	}
}

// Two items may share a header, and the projected ad holds one attribute of that name --
// the later item's value. Cells are positional, so an ambiguous header is typed from the
// row instead, which keeps each cell its own value.
func TestCellValuesAmbiguousHeaderKeepsPerCellValues(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	_, cells := firstRow(t, ex, "SELECT IntStr AS a, Memory AS a FROM "+dbrpc.DefaultTable)
	if len(cells) != 2 {
		t.Fatalf("got %d cells, want 2", len(cells))
	}
	if cells[0] != int64(123) || cells[1] != int64(4096) {
		t.Errorf("cells = %#v, want [123 4096]: each cell keeps its own item's value", cells)
	}
}

// A header the projection does not carry as an attribute reads as undefined, which is
// indistinguishable from a column that evaluated to UNDEFINED. Both fall through to the row
// cell, which is right for either.
func TestCellValuesUncarriedHeaderFallsThroughToRow(t *testing.T) {
	proj := classad.New()
	proj.InsertAttr("kept", 1)
	r := &repl.Result{
		IsSelect:  true,
		Columns:   []string{"kept", "absent"},
		Rows:      [][]string{{"1", "42"}},
		Projected: []*classad.ClassAd{proj},
	}
	cells := cellValues(r, 0)
	if cells[0] != int64(1) {
		t.Errorf("carried cell = %#v, want 1", cells[0])
	}
	if cells[1] != int64(42) {
		t.Errorf("uncarried cell = %#v (%T), want int64(42) from the row cell", cells[1], cells[1])
	}
}

// A column that evaluated to UNDEFINED reaches null whichever route it takes: the projected
// lookup reports undefined, the matched ad has no such attribute, and the display string is
// "undefined". ERROR is found as a value and renders null too. The two agree on purpose --
// a reporting client sees one missing cell either way.
func TestCellValuesUndefinedAndErrorAgreeOnNull(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable
	for _, sql := range []string{
		"SELECT NoSuchAttr AS t FROM " + tbl,
		"SELECT NoSuchAttr FROM " + tbl,
		"SELECT Owner + 1 AS t FROM " + tbl,
	} {
		_, cells := firstRow(t, ex, sql)
		if cells[0] != nil {
			t.Errorf("%s: cell = %#v (%T), want nil", sql, cells[0], cells[0])
		}
	}
}
