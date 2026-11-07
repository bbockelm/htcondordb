package repl

import (
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// The aggregate path's own projection, through the formats that read it.
//
// A GROUP BY key is the cell with no other source: the table shows a label, and a label
// cannot tell the string "4" from the number 4. The value is carried beside the label on
// every reply, so these assert the KIND each column reaches JSON and the ClassAd formats
// as, not just its text.

// aggFixture holds one ad of every kind a group key can be, plus strings whose text reads
// as some other type -- a wrong guess shows up as a wrong rendering rather than a near miss.
//
// Written as an ad rather than through INSERT because a list and a nested ad are among the
// kinds, and those are what the group path has no other way to be handed.
func aggFixture(t *testing.T) (*Executor, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	const text = `Owner = "alice"
IntStr = "123"
BoolStr = "true"
Memory = 4096
Load = 0.75
Wanted = true
Items = {1, 2, 3}
Inner = [ a = 1 ]
`
	if err := d.UpdateOld("a", text); err != nil {
		t.Fatal(err)
	}
	srv := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(sp)) }()
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return NewExecutor(cl, ExecConfig{}), func() { _ = cl.Close(); srv.Close(); _ = d.Close() }
}

// Every kind of group key, as the ad formats write it. The string cases are the point: a
// string that spells an integer stays a JSON string, and the number stays a number.
func TestAggregateGroupKeyKindsInAdFormats(t *testing.T) {
	e, cleanup := aggFixture(t)
	defer cleanup()

	for _, tc := range []struct {
		col   string
		json  string
		adNew string
	}{
		{"Owner", `{"COUNT(*)":1,"Owner":"alice"}`, `['COUNT(*)' = 1; Owner = "alice"]`},
		{"IntStr", `{"COUNT(*)":1,"IntStr":"123"}`, `['COUNT(*)' = 1; IntStr = "123"]`},
		{"BoolStr", `{"BoolStr":"true","COUNT(*)":1}`, `[BoolStr = "true"; 'COUNT(*)' = 1]`},
		{"Memory", `{"COUNT(*)":1,"Memory":4096}`, `['COUNT(*)' = 1; Memory = 4096]`},
		{"Load", `{"COUNT(*)":1,"Load":0.75}`, `['COUNT(*)' = 1; Load = 0.75]`},
		{"Wanted", `{"COUNT(*)":1,"Wanted":true}`, `['COUNT(*)' = 1; Wanted = true]`},
		{"NoSuch", `{"COUNT(*)":1,"NoSuch":null}`, `['COUNT(*)' = 1; NoSuch = undefined]`},
		{"Items", `{"COUNT(*)":1,"Items":[1,2,3]}`, `['COUNT(*)' = 1; Items = {1, 2, 3}]`},
		{"Inner", `{"COUNT(*)":1,"Inner":{"a":1}}`, `['COUNT(*)' = 1; Inner = [a = 1]]`},
	} {
		t.Run(tc.col, func(t *testing.T) {
			r := mustExec(t, e, "SELECT "+tc.col+", COUNT(*) FROM ads GROUP BY "+tc.col)
			if got := strings.TrimSpace(formatString(r, FormatJSON)); got != tc.json {
				t.Errorf("JSON gave %s, want %s", got, tc.json)
			}
			if got := strings.TrimSpace(formatString(r, FormatClassAdNew)); got != tc.adNew {
				t.Errorf("classad-new gave %s, want %s", got, tc.adNew)
			}
			assertNamesMatchColumns(t, r)
		})
	}
}

// A computed item beside a group key. Each is evaluated once and lands twice, so the ad
// carries the arithmetic's result as a number and the key as the string it is.
func TestAggregateComputedItemInAdFormats(t *testing.T) {
	e, cleanup := aggFixture(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT IntStr, COUNT(*) * 2 AS twice FROM ads GROUP BY IntStr")
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)),
		`{"IntStr":"123","twice":2}`; got != want {
		t.Errorf("JSON gave %s, want %s", got, want)
	}
	assertNamesMatchColumns(t, r)
}

// A header repeated across two items cannot round-trip through an ad: an ad holds one
// attribute of a name, so the second item's value is the one that survives. The table keeps
// both cells, since cells are positional -- so the projection is the lossy shape here, and
// what it drops is a column the table still prints. The ad formats were already lossy for
// this, by the same rule and in the same direction, so nothing is traded away: what this
// pins is that the names still match the columns and the surviving value is a real one.
func TestAggregateRepeatedHeaderKeepsOneAttribute(t *testing.T) {
	e, cleanup := aggFixture(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT COUNT(*) AS n, MAX(Memory) AS n FROM ads")
	if len(r.Columns) != 2 || r.Columns[0] != "n" || r.Columns[1] != "n" {
		t.Fatalf("columns = %q, want both to be n", r.Columns)
	}
	if len(r.Rows[0]) != 2 || r.Rows[0][0] != "1" || r.Rows[0][1] != "4096" {
		t.Errorf("row = %q, want each cell its own value", r.Rows[0])
	}
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)), `{"n":4096}`; got != want {
		t.Errorf("JSON gave %s, want %s -- an ad holds one attribute of a name", got, want)
	}
}

// ORDER BY and LIMIT reorder and truncate the rows; the projected ads are a parallel slice
// and have to move with them, or a row's columns would be served from another row's values.
func TestAggregateOrderAndLimitMoveTheProjection(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	for _, tc := range []struct{ key, owner string }{{"a", "carol"}, {"b", "alice"}, {"c", "bob"}} {
		mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('"+tc.key+"', '"+tc.owner+"')")
	}

	r := mustExec(t, e, "SELECT Owner, COUNT(*) AS n FROM ads GROUP BY Owner ORDER BY Owner")
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("projected %d ads for %d rows", len(r.Projected), len(r.Rows))
	}
	assertRowsMatchProjection(t, r)
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)),
		"{\"n\":1,\"Owner\":\"alice\"}\n{\"n\":1,\"Owner\":\"bob\"}\n{\"n\":1,\"Owner\":\"carol\"}"; got != want {
		t.Errorf("JSON gave %s, want %s", got, want)
	}

	r = mustExec(t, e, "SELECT Owner, COUNT(*) AS n FROM ads GROUP BY Owner ORDER BY Owner DESC LIMIT 1")
	if len(r.Projected) != 1 {
		t.Fatalf("projected %d ads for %d rows", len(r.Projected), len(r.Rows))
	}
	assertRowsMatchProjection(t, r)
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)),
		`{"n":1,"Owner":"carol"}`; got != want {
		t.Errorf("JSON gave %s, want %s", got, want)
	}
}

// assertRowsMatchProjection checks that each projected ad belongs to the row beside it: the
// ad's attribute for a column renders to the cell the table prints in that column. It is
// the check a sort or a truncation that moved only one of the two would fail.
func assertRowsMatchProjection(t *testing.T, r *Result) {
	t.Helper()
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("projected %d ads for %d rows", len(r.Projected), len(r.Rows))
	}
	dup := map[string]int{}
	for _, c := range r.Columns {
		dup[strings.ToLower(c)]++
	}
	for i, row := range r.Rows {
		for j, col := range r.Columns {
			if dup[strings.ToLower(col)] > 1 {
				continue // one attribute of a name: the cells cannot both be in the ad
			}
			if got := valueDisplay(r.Projected[i].EvaluateAttr(col)); got != row[j] {
				t.Errorf("row %d column %s: the ad says %q, the table says %q",
					i, col, got, row[j])
			}
		}
	}
}

// Grouping on a column whose values are strings that read as numbers is the defect this
// path had: the rows are distinct groups and must stay distinct, each reported as the
// string it is.
func TestAggregateNumericLookingStringsStayDistinctStrings(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Tag) VALUES ('a', '1')")
	mustExec(t, e, "INSERT INTO ads (Key, Tag) VALUES ('b', '01')")
	mustExec(t, e, "INSERT INTO ads (Key, Tag) VALUES ('c', 1)")

	r := mustExec(t, e, "SELECT Tag, COUNT(*) AS n FROM ads GROUP BY Tag ORDER BY Tag")
	if len(r.Rows) != 3 {
		t.Fatalf("got %d groups, want 3: %q", len(r.Rows), r.Rows)
	}
	got := strings.TrimSpace(formatString(r, FormatJSON))
	for _, want := range []string{`"Tag":"1"`, `"Tag":"01"`, `"Tag":1`} {
		if !strings.Contains(got, want) {
			t.Errorf("JSON %s is missing %s", got, want)
		}
	}
}

// THE CLIENT-SIDE GROUPING PATHS CARRY THE SAME VALUES. Not every aggregate is reduced by
// the server: a time_bucket series and a grouped join are grouped here, from ads this
// process evaluated. Those keep the value each key was rendered from, so a string key is a
// string on those paths too -- the hazard being that a path that kept only the label would
// look right in the table and wrong in every other format.
func TestBucketedAggregateCarriesTypedKeys(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Done) VALUES ('a', '123', 1000)")
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Done) VALUES ('b', '123', 1200)")

	r := mustExec(t, e, `SELECT time_bucket(Done, '1h') AS t, Owner, COUNT(*) AS n
		FROM ads GROUP BY time_bucket(Done, '1h'), Owner`)
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("projected %d ads for %d rows", len(r.Projected), len(r.Rows))
	}
	assertRowsMatchProjection(t, r)
	// The bucket floor is the number the series is indexed by; the owner is the string it
	// is, though its text spells a number.
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)),
		`{"n":2,"Owner":"123","t":0}`; got != want {
		t.Errorf("JSON gave %s, want %s", got, want)
	}
	assertNamesMatchColumns(t, r)
}

func TestJoinAggregateCarriesTypedKeys(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "CREATE TABLE hist")
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner) VALUES ('1', 1, '123')`)
	mustExec(t, e, `INSERT INTO hist (Key, ClusterId, ExitCode) VALUES ('h1', 1, 0)`)

	r := mustExec(t, e, `SELECT j.Owner, COUNT(*) AS n FROM jobs j
		JOIN hist h ON j.ClusterId == h.ClusterId GROUP BY j.Owner`)
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("projected %d ads for %d rows", len(r.Projected), len(r.Rows))
	}
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)),
		`{"j.Owner":"123","n":1}`; got != want {
		t.Errorf("JSON gave %s, want %s", got, want)
	}
	assertNamesMatchColumns(t, r)
}

// A COMPUTED GROUP KEY cannot be pushed down -- the server groups by raw attribute values --
// so this is the client-side bucketing path, grouping over ads evaluated here. The key is
// the expression's own value, which is what keeps a string-valued expression a string.
func TestComputedGroupKeyCarriesItsValue(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Memory) VALUES ('a', 4096)")
	mustExec(t, e, "INSERT INTO ads (Key, Memory) VALUES ('b', 64)")

	r := mustExec(t, e, `SELECT ifThenElse(Memory > 1024, "123", "small") AS size, COUNT(*) AS n
		FROM ads GROUP BY ifThenElse(Memory > 1024, "123", "small") ORDER BY size`)
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("projected %d ads for %d rows", len(r.Projected), len(r.Rows))
	}
	assertRowsMatchProjection(t, r)
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)),
		"{\"n\":1,\"size\":\"123\"}\n{\"n\":1,\"size\":\"small\"}"; got != want {
		t.Errorf("JSON gave %s, want %s", got, want)
	}
}

// An aggregate read inside a transaction reduces over the transaction's own rows rather
// than the committed store, which is the fetch-and-reduce path AS OF also takes. It keeps
// the value each key was rendered from, like the other client-side paths.
func TestInTransactionAggregateCarriesTypedKeys(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('a', '123')")

	mustExec(t, e, "BEGIN")
	defer func() { mustExec(t, e, "ROLLBACK") }()
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('b', '123')")

	r := mustExec(t, e, "SELECT Owner, COUNT(*) AS n FROM ads GROUP BY Owner")
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("projected %d ads for %d rows", len(r.Projected), len(r.Rows))
	}
	assertRowsMatchProjection(t, r)
	if got, want := strings.TrimSpace(formatString(r, FormatJSON)),
		`{"n":2,"Owner":"123"}`; got != want {
		t.Errorf("JSON gave %s, want %s -- the uncommitted row counts and the key is a string", got, want)
	}
}
