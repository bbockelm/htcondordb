package main

import (
	"encoding/json"
	"errors"
	"math"
	"net"
	"strconv"
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

// With no ad behind the row at all, cells fall back to parsing the display string. That is
// the last resort, not the aggregate path -- a Result built by hand here, because an
// aggregate the executor produced carries a projection.
func TestCellValuesNoAdFallsBackToDisplayStrings(t *testing.T) {
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
	ex, _, done := cellFixtureOpts(t)
	return ex, done
}

// cellFixtureOpts is cellFixture with client options, so a test can say what the peer
// advertised, and hands back the client so it can check that the option took.
func cellFixtureOpts(t *testing.T, opts ...dbrpc.ClientOption) (*repl.Executor, *dbrpc.Client, func()) {
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
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(b), opts...)
	return repl.NewExecutor(cl, repl.ExecConfig{}), cl,
		func() { _ = cl.Close(); srv.Close(); _ = d.Close() }
}

// typedGroupFixture is cellFixture against a client that was told the peer sends typed
// group cells. Over a pipe nothing is advertised, so the plain fixture exercises the
// fallback; this one exercises the feature, and the two must agree.
func typedGroupFixture(t *testing.T) (*repl.Executor, func()) {
	t.Helper()
	ex, cl, done := cellFixtureOpts(t, dbrpc.WithPeerSessionAttrs(map[string]string{
		dbrpc.SessionAttrProtocol: strconv.Itoa(dbrpc.ProtocolVersion),
		dbrpc.SessionAttrFeatures: string(dbrpc.FeatTypedGroups),
	}))
	// Checked rather than assumed: if the advertisement stopped taking, this fixture would
	// quietly become a second copy of the plain one and the feature would go untested.
	if !cl.SupportsNewer(dbrpc.FeatTypedGroups) {
		done()
		t.Fatal("the fixture's client does not take the peer's typed-groups advertisement")
	}
	return ex, done
}

// firstValue is a user-defined aggregate returning the group's first defined argument value
// WHATEVER KIND IT IS, which is what makes it able to return a string where every built-in
// returns a number.
type firstValue struct{}

type firstValueAcc struct{ v classad.Value }

func (firstValue) NewAccumulator() db.Accumulator { return &firstValueAcc{} }

func (a *firstValueAcc) Add(v classad.Value) {
	if a.v.IsUndefined() && !v.IsUndefined() {
		a.v = v
	}
}

func (a *firstValueAcc) Merge(other db.Accumulator) {
	if o, ok := other.(*firstValueAcc); ok && a.v.IsUndefined() {
		a.v = o.v
	}
}

func (a *firstValueAcc) Result() classad.Value { return a.v }

// registerFirstValue installs it. The registry is append-only and process-wide, so a second
// call is a no-op rather than a failure.
func registerFirstValue(t *testing.T) {
	t.Helper()
	if err := db.RegisterAggregate("firstvalue", firstValue{}); err != nil &&
		!errors.Is(err, db.ErrAggregateExists) {
		t.Fatal(err)
	}
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

// An aggregate carries a projection of its own, so its cells are typed from values rather
// than guessed from text. A group key is exact: dbrpc fills AggRow.GroupValues on every
// reply, so the string "123" grouped on arrives as the string "123".
//
// The aggregate's OWN output is the part that cannot be exact. It crosses as rendered text,
// not as a ClassAd literal -- a string result arrives bare, where a string group key
// arrives quoted -- so it is read the way the table reads it. COUNT, SUM, AVG, MIN and MAX
// only ever produce a number, `undefined` or `error`, so the gap needs a user-defined
// aggregate to reach; TestCellValuesUserAggregateOutputIsReadAsText reaches it.
func TestCellValuesAggregateGroupKeysAreExact(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	r, err := ex.ExecString("SELECT COUNT(*) AS n, AVG(Memory) AS m FROM " + tbl)
	if err != nil {
		t.Fatal(err)
	}
	cells := cellValues(r, 0)
	if cells[0] != int64(1) || cells[1] != int64(4096) {
		t.Errorf("aggregate cells = %#v, want [1 4096]", cells)
	}

	// The three reproductions: a string that reads as an integer, one that reads as a
	// bool, and one that reads as neither. All three are the string the ad holds.
	for _, tc := range []struct {
		col  string
		want any
	}{
		{"IntStr", "123"},
		{"BoolStr", "true"},
		{"Owner", "alice"},
	} {
		t.Run(tc.col, func(t *testing.T) {
			_, cells := firstRow(t, ex,
				"SELECT "+tc.col+", COUNT(*) AS n FROM "+tbl+" GROUP BY "+tc.col)
			if cells[0] != tc.want {
				t.Errorf("group key = %#v (%T), want %#v (%T)",
					cells[0], cells[0], tc.want, tc.want)
			}
			if cells[1] != int64(1) {
				t.Errorf("count = %#v (%T), want int64(1)", cells[1], cells[1])
			}
		})
	}
}

// Every kind a group key can be, through cellValues. A list and a nested ad group by their
// canonical text rather than by a relation, but that text is a ClassAd literal, so they
// arrive as the values they are and not as strings standing in for them -- which is why
// they are here beside the scalars. Every want below is what the SAME column gives as a
// plain SELECT (see TestCellValuesAliasedColumnsKeepExactTypes), which is the whole claim:
// grouping by a column does not change what that column is.
func TestCellValuesAggregateGroupKeyKinds(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	for _, tc := range []struct {
		col  string
		want any
	}{
		{"Owner", "alice"},
		{"Memory", int64(4096)},
		{"Load", 0.75},
		{"Wanted", true},
		{"NoSuchAttr", nil},
		{"Items", "[1 2 3]"},
		{"Inner", "[a = 1]"},
	} {
		t.Run(tc.col, func(t *testing.T) {
			_, cells := firstRow(t, ex,
				"SELECT "+tc.col+", COUNT(*) AS n FROM "+tbl+" GROUP BY "+tc.col)
			if cells[0] != tc.want {
				t.Errorf("group key = %#v (%T), want %#v (%T)",
					cells[0], cells[0], tc.want, tc.want)
			}
		})
	}
}

// TWO GROUP COLUMNS, so each item has to read ITS OWN position in the group tuple. The two
// are of different kinds on purpose: a projection that read the tuple off by one would hand
// the string column the number and still produce a plausible-looking row.
func TestCellValuesAggregateTwoGroupColumns(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	cols, cells := firstRow(t, ex,
		"SELECT IntStr, Memory, COUNT(*) AS n FROM "+dbrpc.DefaultTable+" GROUP BY IntStr, Memory")
	if len(cols) != 3 {
		t.Fatalf("headers = %q, want three", cols)
	}
	if cells[0] != "123" || cells[1] != int64(4096) || cells[2] != int64(1) {
		t.Errorf("cells = %#v, want [\"123\" 4096 1]", cells)
	}
}

// A computed item beside a group key: the group key comes from its value and the expression
// from its own evaluation, so neither is read out of the other's text.
func TestCellValuesAggregateComputedItem(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	cols, cells := firstRow(t, ex,
		`SELECT IntStr, COUNT(*) * 2 AS twice FROM `+dbrpc.DefaultTable+` GROUP BY IntStr`)
	if len(cols) != 2 {
		t.Fatalf("headers = %q, want two", cols)
	}
	if cells[0] != "123" {
		t.Errorf("group key = %#v (%T), want the string \"123\"", cells[0], cells[0])
	}
	if cells[1] != int64(2) {
		t.Errorf("computed cell = %#v (%T), want int64(2)", cells[1], cells[1])
	}
}

// Two aggregate items may share a header, and an ad holds one attribute of that name. Cells
// are positional, so a repeated header is typed from the row instead -- the same rule the
// row-scan path follows, for the same reason.
func TestCellValuesAggregateRepeatedHeader(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	cols, cells := firstRow(t, ex,
		`SELECT COUNT(*) AS n, AVG(Memory) AS n FROM `+dbrpc.DefaultTable)
	if len(cols) != 2 || cols[0] != "n" || cols[1] != "n" {
		t.Fatalf("headers = %q, want both to be n", cols)
	}
	if cells[0] != int64(1) || cells[1] != int64(4096) {
		t.Errorf("cells = %#v, want [1 4096] -- each cell its own value", cells)
	}
}

// A SERVER THAT SENDS TYPED GROUP CELLS answers exactly as one that does not. The fixture
// above reaches the server over a pipe that advertises nothing, which is the fallback: the
// group's text is read back into a value by the client. This one advertises the feature, so
// the group cells arrive as tagged values instead. Both are the same group, and a caller
// never has to ask which kind of server answered -- so the assertions are the same ones.
func TestCellValuesAggregateGroupKeysTypedServer(t *testing.T) {
	ex, done := typedGroupFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	for _, tc := range []struct {
		col  string
		want any
	}{
		{"IntStr", "123"},
		{"BoolStr", "true"},
		{"Owner", "alice"},
		{"Memory", int64(4096)},
		{"Load", 0.75},
		{"Wanted", true},
		{"NoSuchAttr", nil},
		{"Items", "[1 2 3]"},
		{"Inner", "[a = 1]"},
	} {
		t.Run(tc.col, func(t *testing.T) {
			_, cells := firstRow(t, ex,
				"SELECT "+tc.col+", COUNT(*) AS n FROM "+tbl+" GROUP BY "+tc.col)
			if cells[0] != tc.want {
				t.Errorf("group key = %#v (%T), want %#v (%T)",
					cells[0], cells[0], tc.want, tc.want)
			}
		})
	}
}

// AN AGGREGATE'S OWN OUTPUT IS RENDERED TEXT, and this is the gap that leaves. A group key
// crosses as its faithful text -- a string quoted -- but an aggregate's result crosses as
// the table would print it, so a string result arrives bare and is read back as whatever
// its text spells. The built-ins cannot show this: COUNT, SUM, AVG, MIN and MAX produce a
// number, `undefined` or `error` and nothing else. A user-defined aggregate can, and does.
func TestCellValuesUserAggregateOutputIsReadAsText(t *testing.T) {
	registerFirstValue(t)
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	_, cells := firstRow(t, ex,
		"SELECT Owner, firstvalue(IntStr) AS f FROM "+tbl+" GROUP BY Owner")
	if cells[0] != "alice" {
		t.Errorf("group key = %#v (%T), want the string \"alice\"", cells[0], cells[0])
	}
	// The aggregate returned the STRING "123"; it crossed as the text `123` and reads
	// back as the number. Closing this needs a typed aggregate cell on the wire.
	if cells[1] != int64(123) {
		t.Errorf("aggregate output = %#v (%T), want int64(123) -- an aggregate's output "+
			"crosses as rendered text, so a string result that spells a number is read "+
			"back as that number", cells[1], cells[1])
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

// A UNION's cells are typed exactly too. Its branches are ordinary SELECTs and each hands
// back the values it evaluated, so the operation carries them on rather than handing the
// caller the branches' whole matched ads -- which answer the question the scan answered,
// and for a computed column answer nothing at all.
//
// The fixture has one table, so each branch here reads it; what varies is the SELECT, which
// is what decides the typing. The string that reads as a number is the case that separates
// a real projection from a lucky parse of the display text.
func TestCellValuesUnionKeepsExactTypes(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable
	both := func(items string) string {
		return "SELECT " + items + " FROM " + tbl + " UNION ALL SELECT " + items + " FROM " + tbl
	}

	for _, tc := range []struct {
		name string
		sql  string
		want []any
	}{
		{"plain column", both("IntStr"), []any{"123"}},
		{"two plain columns", both("IntStr, Memory"), []any{"123", int64(4096)}},
		{"aliased plain column", both("IntStr AS who"), []any{"123"}},
		{"plain and computed", both("IntStr, Memory * 2 AS d"), []any{"123", int64(8192)}},
		{"computed only", both("Memory * 2 AS d"), []any{int64(8192)}},
		{"computed keeps its expression header", both("Memory * 2"), []any{int64(8192)}},
		{"string result of a computed column", both(`strcat(IntStr, "4") AS t`), []any{"1234"}},
		{"de-duplicated", "SELECT IntStr FROM " + tbl + " UNION SELECT IntStr FROM " + tbl,
			[]any{"123"}},
		{"over aggregates", "SELECT IntStr, COUNT(*) AS n FROM " + tbl + " GROUP BY IntStr " +
			"UNION ALL SELECT IntStr, COUNT(*) AS n FROM " + tbl + " GROUP BY IntStr",
			[]any{"123", int64(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cells := firstRow(t, ex, tc.sql)
			if len(cells) != len(tc.want) {
				t.Fatalf("got %d cells, want %d", len(cells), len(tc.want))
			}
			for j := range tc.want {
				if cells[j] != tc.want[j] {
					t.Errorf("cell %d = %#v (%T), want %#v (%T)",
						j, cells[j], cells[j], tc.want[j], tc.want[j])
				}
			}
		})
	}
}

// ORDER BY reorders a UNION's rows, and the typed cells have to move with them. A cell is
// typed from the ad beside its row, so a sort that moved only the rows would type row i
// from some other row's values -- and the C API would report a table the REPL never
// printed.
func TestCellValuesUnionOrderByStaysWithItsRows(t *testing.T) {
	ex, done := cellFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	r, err := ex.ExecString("SELECT Owner, Memory FROM " + tbl +
		" UNION ALL SELECT IntStr AS Owner, Load AS Memory FROM " + tbl + " ORDER BY Owner")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(r.Rows))
	}
	// "123" sorts before "alice", so the second branch's row comes first.
	want := [][]any{{"123", 0.75}, {"alice", int64(4096)}}
	for i := range want {
		cells := cellValues(r, i)
		for j := range want[i] {
			if cells[j] != want[i][j] {
				t.Errorf("row %d cell %d = %#v (%T), want %#v (%T)",
					i, j, cells[j], cells[j], want[i][j], want[i][j])
			}
		}
	}
}
