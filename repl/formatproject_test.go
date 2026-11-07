package repl

import (
	"bytes"
	"sort"
	"strings"
	"testing"
)

// formatString renders a result the way the REPL would.
func formatString(r *Result, f Format) string {
	var b strings.Builder
	FormatResult(&b, r, f)
	return b.String()
}

// adNames collects the attribute names an ad-shaped format would write for a result, as the
// formats themselves pick their ads.
func adNames(r *Result) []string {
	ads := resultAds(r)
	if len(ads) == 0 {
		return nil
	}
	names := append([]string(nil), ads[0].GetAttributes()...)
	sort.Strings(names)
	return names
}

// assertNamesMatchColumns pins the relationship that keeps the two output shapes from
// drifting: whatever the table prints as a header, the ad formats write as an attribute.
// Asserting each format's exact text separately would let both drift together.
func assertNamesMatchColumns(t *testing.T, r *Result) {
	t.Helper()
	want := append([]string(nil), r.Columns...)
	sort.Strings(want)
	got := adNames(r)
	if len(got) != len(want) {
		t.Fatalf("ads carry %q but the table prints columns %q", got, want)
	}
	for i := range want {
		if !strings.EqualFold(got[i], want[i]) {
			t.Fatalf("ads carry %q but the table prints columns %q", got, want)
		}
	}
}

// Every format answers the question the SELECT asked: the columns it named, under the names
// it gave them. An alias is a name, and a computed column is a column.
func TestAdFormatsHonourTheProjection(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used, Total) VALUES ('a', 'm1', 3, 8)")

	for _, tc := range []struct {
		name    string
		query   string
		columns []string
		json    string
		old     string
		adNew   string
	}{
		{
			name:    "plain column",
			query:   "SELECT Machine FROM ads",
			columns: []string{"Machine"},
			json:    "{\"Machine\":\"m1\"}\n",
			old:     "Machine = \"m1\"\n\n",
			adNew:   "[Machine = \"m1\"]\n",
		},
		{
			name:    "two plain columns",
			query:   "SELECT Machine, Used FROM ads",
			columns: []string{"Machine", "Used"},
			json:    "{\"Machine\":\"m1\",\"Used\":3}\n",
			old:     "Machine = \"m1\"\nUsed = 3\n\n",
			adNew:   "[Machine = \"m1\"; Used = 3]\n",
		},
		{
			name:    "aliased plain column",
			query:   "SELECT Machine AS host FROM ads",
			columns: []string{"host"},
			json:    "{\"host\":\"m1\"}\n",
			old:     "host = \"m1\"\n\n",
			adNew:   "[host = \"m1\"]\n",
		},
		{
			name:    "plain and computed",
			query:   "SELECT Machine, Used * 2 AS d FROM ads",
			columns: []string{"Machine", "d"},
			json:    "{\"d\":6,\"Machine\":\"m1\"}\n",
			old:     "d = 6\nMachine = \"m1\"\n\n",
			adNew:   "[d = 6; Machine = \"m1\"]\n",
		},
		{
			name:    "computed only",
			query:   "SELECT Used * 2 AS d FROM ads",
			columns: []string{"d"},
			json:    "{\"d\":6}\n",
			old:     "d = 6\n\n",
			adNew:   "[d = 6]\n",
		},
		{
			// SELECT * is the one shape whose projection IS the whole ad.
			name:    "star",
			query:   "SELECT * FROM ads",
			columns: []string{"Key", "Machine", "Total", "Used"},
			json:    "{\"Key\":\"a\",\"Machine\":\"m1\",\"Total\":8,\"Used\":3}\n",
			old:     "Key = \"a\"\nMachine = \"m1\"\nTotal = 8\nUsed = 3\n\n",
			adNew:   "[Key = \"a\"; Machine = \"m1\"; Total = 8; Used = 3]\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := mustExec(t, e, tc.query)
			if strings.Join(r.Columns, ",") != strings.Join(tc.columns, ",") {
				t.Fatalf("columns %q, want %q", r.Columns, tc.columns)
			}
			for _, f := range []struct {
				format Format
				want   string
			}{
				{FormatJSON, tc.json},
				{FormatClassAdOld, tc.old},
				{FormatClassAdNew, tc.adNew},
			} {
				if got := formatString(r, f.format); got != f.want {
					t.Errorf("%s format gave %q, want %q", f.format, got, f.want)
				}
			}
			assertNamesMatchColumns(t, r)
		})
	}
}

// The table format is the one that was always right, so it is the reference the others are
// held to -- and it must not move while they are brought into line with it.
func TestTableFormatUnchangedByProjection(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used, Total) VALUES ('a', 'm1', 3, 8)")

	r := mustExec(t, e, "SELECT Machine AS host, Used * 2 AS d FROM ads")
	got := formatString(r, FormatTable)
	want := "host | d\n-----+--\nm1   | 6\n"
	if i := strings.Index(got, "\n(1 row"); i >= 0 {
		got = got[:i+1]
	}
	if got != want {
		t.Errorf("table format gave %q, want %q", got, want)
	}
}

// `SELECT *` keeps serializing the whole ad: there the projection is the ad, and an
// attribute the table happens not to have a header for must still reach the reader.
func TestStarStillSerializesWholeAds(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used, Total) VALUES ('a', 'm1', 3, 8)")

	r := mustExec(t, e, "SELECT * FROM ads")
	if r.Projected != nil {
		t.Fatalf("SELECT * built projected ads; its own ads are the projection")
	}
	for _, attr := range []string{"Key", "Machine", "Used", "Total"} {
		if !strings.Contains(formatString(r, FormatJSON), "\""+attr+"\"") {
			t.Errorf("SELECT * JSON dropped %s", attr)
		}
	}
}

// Type fidelity is the reason the projected ad is built where the evaluated value still
// exists: a number that reached JSON through the display cell would arrive as "6".
func TestProjectedValuesKeepTheirTypes(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used, Ratio, Up, Digits) "+
		"VALUES ('a', 'm1', 3, 1.5, true, '123')")

	r := mustExec(t, e, "SELECT Used * 2 AS d, Ratio, Up, Digits, Machine FROM ads")
	want := "{\"d\":6,\"Digits\":\"123\",\"Machine\":\"m1\",\"Ratio\":1.5,\"Up\":true}\n"
	if got := formatString(r, FormatJSON); got != want {
		t.Errorf("JSON gave %q, want %q -- a stringified cell shows up here as quotes", got, want)
	}
}

// UNDEFINED and ERROR keep their column. The query named it, so dropping it would hand back
// a different column set; the ad holds the ClassAd keyword, which JSON renders as null and
// as the error expression.
func TestProjectedUndefinedAndErrorKeepTheirColumns(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used) VALUES ('a', 'm1', 3)")

	r := mustExec(t, e, "SELECT NoSuch AS miss, Machine + 1 AS bad, Machine FROM ads")
	if got, want := formatString(r, FormatJSON),
		"{\"bad\":\"\\/Expr(error)\\/\",\"Machine\":\"m1\",\"miss\":null}\n"; got != want {
		t.Errorf("JSON gave %q, want %q", got, want)
	}
	if got, want := formatString(r, FormatClassAdNew),
		"[bad = error; Machine = \"m1\"; miss = undefined]\n"; got != want {
		t.Errorf("classad-new gave %q, want %q", got, want)
	}
	assertNamesMatchColumns(t, r)
}

// An aggregate has no ads -- its rows belong to groups, not to any one ad -- so the ad
// formats keep synthesizing an ad per group row from the display cells.
func TestAggregateFormattingUnchanged(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used) VALUES ('a', 'm1', 3)")
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used) VALUES ('b', 'm1', 4)")

	r := mustExec(t, e, "SELECT Machine, COUNT(*) FROM ads GROUP BY Machine")
	if r.Ads != nil || r.Projected != nil {
		t.Fatalf("an aggregate result carries ads")
	}
	if got, want := formatString(r, FormatJSON),
		"{\"COUNT(*)\":\"2\",\"Machine\":\"m1\"}\n"; got != want {
		t.Errorf("JSON gave %q, want %q", got, want)
	}
	if got, want := formatString(r, FormatClassAdOld),
		"COUNT(*) = \"2\"\nMachine = \"m1\"\n\n"; got != want {
		t.Errorf("classad gave %q, want %q", got, want)
	}
}

// Arrow reaches the same columns by its own route -- it re-evaluates the items through
// RowStreamer rather than reading Result -- so this is the check that the route agrees.
func TestArrowHonoursTheProjection(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Machine, Used) VALUES ('a', 'm1', 3)")

	st, err := Parse("SELECT Machine AS host, Used * 2 AS d FROM ads")
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec(st)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.WriteArrow(&buf, st, res); err != nil {
		t.Fatal(err)
	}
	recs, schema := readArrow(t, buf.Bytes())
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()
	if schema.NumFields() != 2 {
		t.Fatalf("%d fields, want 2", schema.NumFields())
	}
	if schema.Field(0).Name != "host" || schema.Field(1).Name != "d" {
		t.Errorf("fields are %q/%q, want host/d", schema.Field(0).Name, schema.Field(1).Name)
	}
	if len(recs) != 1 || recs[0].NumRows() != 1 {
		t.Fatalf("got %d batches, want one row", len(recs))
	}
	// Read the cells through ValueStr: this table declares no column types, so Arrow's
	// cell TYPE here comes from the table's schema and is a separate question from
	// whether the right columns, under the right names, arrived at all.
	if got := recs[0].Column(0).ValueStr(0); got != "m1" {
		t.Errorf("host = %q, want m1", got)
	}
	if got := recs[0].Column(1).ValueStr(0); got != "6" {
		t.Errorf("d = %q, want 6", got)
	}
}

// A join reaches the ad formats by its own rendering path, over a composite row that carries
// both sides' attributes. Without the projection it would serialize all of them, which is
// the same defect with more to leak.
func TestJoinAdFormatsHonourTheProjection(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT j.Owner AS who, h.ExitCode + 1 AS next FROM jobs j "+
		"JOIN hist h ON j.ClusterId == h.ClusterId WHERE j.Owner == \"bob\"")
	if got, want := formatString(r, FormatJSON), "{\"next\":2,\"who\":\"bob\"}\n"; got != want {
		t.Errorf("JSON gave %q, want %q", got, want)
	}
	assertNamesMatchColumns(t, r)
}
