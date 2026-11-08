package repl

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// unionFixture is two tables whose rows differ in every column, so a row that ends up under
// the wrong ad shows as a wrong value rather than as a coincidence. Digits is a STRING that
// reads as a number, which is the case that tells a projection apart from a lucky guess at
// the display text.
func unionFixture(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE a")
	mustExec(t, e, "CREATE TABLE b")
	mustExec(t, e, `INSERT INTO a (Key, Machine, Used, Digits) VALUES ('1', 'm5', 5, '123')`)
	mustExec(t, e, `INSERT INTO b (Key, Machine, Used, Digits) VALUES ('2', 'm3', 3, '42')`)
	return e, cleanup
}

// Every format answers the question the UNION asked: the columns its first branch named,
// under the names it gave them, for the rows of every branch. These are the shapes the
// single-table projection is held to, unioned with themselves.
func TestUnionAdFormatsHonourTheProjection(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

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
			query:   "SELECT Machine FROM a UNION ALL SELECT Machine FROM b",
			columns: []string{"Machine"},
			json:    "{\"Machine\":\"m5\"}\n{\"Machine\":\"m3\"}\n",
			old:     "Machine = \"m5\"\n\nMachine = \"m3\"\n\n",
			adNew:   "[Machine = \"m5\"]\n[Machine = \"m3\"]\n",
		},
		{
			name:    "two plain columns",
			query:   "SELECT Machine, Used FROM a UNION ALL SELECT Machine, Used FROM b",
			columns: []string{"Machine", "Used"},
			json:    "{\"Machine\":\"m5\",\"Used\":5}\n{\"Machine\":\"m3\",\"Used\":3}\n",
			old:     "Machine = \"m5\"\nUsed = 5\n\nMachine = \"m3\"\nUsed = 3\n\n",
			adNew:   "[Machine = \"m5\"; Used = 5]\n[Machine = \"m3\"; Used = 3]\n",
		},
		{
			name:    "aliased plain column",
			query:   "SELECT Machine AS host FROM a UNION ALL SELECT Machine AS host FROM b",
			columns: []string{"host"},
			json:    "{\"host\":\"m5\"}\n{\"host\":\"m3\"}\n",
			old:     "host = \"m5\"\n\nhost = \"m3\"\n\n",
			adNew:   "[host = \"m5\"]\n[host = \"m3\"]\n",
		},
		{
			name: "plain and computed",
			query: "SELECT Machine, Used * 2 AS d FROM a " +
				"UNION ALL SELECT Machine, Used * 2 AS d FROM b",
			columns: []string{"Machine", "d"},
			json:    "{\"d\":10,\"Machine\":\"m5\"}\n{\"d\":6,\"Machine\":\"m3\"}\n",
			old:     "d = 10\nMachine = \"m5\"\n\nd = 6\nMachine = \"m3\"\n\n",
			adNew:   "[d = 10; Machine = \"m5\"]\n[d = 6; Machine = \"m3\"]\n",
		},
		{
			name:    "computed only",
			query:   "SELECT Used * 2 AS d FROM a UNION ALL SELECT Used * 2 AS d FROM b",
			columns: []string{"d"},
			json:    "{\"d\":10}\n{\"d\":6}\n",
			old:     "d = 10\n\nd = 6\n\n",
			adNew:   "[d = 10]\n[d = 6]\n",
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

// The string that reads as a number is what separates a projection from a coincidence: a
// cell routed through the display text comes back as 123, and nothing downstream can tell
// it from a number that was always a number.
func TestUnionProjectedValuesKeepTheirTypes(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT Digits AS d FROM a UNION ALL SELECT Digits AS d FROM b")
	want := "{\"d\":\"123\"}\n{\"d\":\"42\"}\n"
	if got := formatString(r, FormatJSON); got != want {
		t.Errorf("JSON gave %q, want %q -- a stringified cell shows up here without quotes", got, want)
	}
	assertRowsAndAdsCorrespond(t, r)
	for i, ad := range r.Projected {
		if v := ad.EvaluateAttr("d"); !v.IsString() {
			t.Errorf("row %d: Digits is %v, want the string it was stored as", i, v)
		}
	}
}

// A de-duplicated UNION keeps its projection, and keeps it aligned: the rows that survive
// are the ones whose ads survive.
func TestUnionDedupeKeepsProjectionAligned(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()
	mustExec(t, e, `INSERT INTO b (Key, Machine, Used, Digits) VALUES ('3', 'm5', 5, '123')`)

	r := mustExec(t, e, "SELECT Machine, Digits FROM a UNION SELECT Machine, Digits FROM b")
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want b's duplicate of a's row removed: %v", len(r.Rows), r.Rows)
	}
	assertRowsAndAdsCorrespond(t, r)
	if got, want := formatString(r, FormatJSON),
		"{\"Digits\":\"123\",\"Machine\":\"m5\"}\n{\"Digits\":\"42\",\"Machine\":\"m3\"}\n"; got != want {
		t.Errorf("JSON gave %q, want %q", got, want)
	}
}

// ORDER BY reorders the rows, and whatever a reader consults beside a row has to move with
// it. The ad formats read the projected ads and the cursor reads the matched ads, so a sort
// that moved only the rows would hand either of them another row's values under this row's
// columns -- in the table, ordered; everywhere else, in scan order.
func TestUnionOrderByKeepsRowsAndAdsInCorrespondence(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()
	mustExec(t, e, `INSERT INTO a (Key, Machine, Used, Digits) VALUES ('4', 'm1', 1, '7')`)

	for _, q := range []string{
		"SELECT Machine, Used FROM a UNION ALL SELECT Machine, Used FROM b ORDER BY Machine",
		"SELECT Machine, Used FROM a UNION ALL SELECT Machine, Used FROM b ORDER BY Machine LIMIT 2",
		"SELECT Machine, Used FROM a UNION SELECT Machine, Used FROM b ORDER BY Machine DESC",
	} {
		t.Run(q, func(t *testing.T) {
			r := mustExec(t, e, q)
			assertRowsAndAdsCorrespond(t, r)
			if len(r.Ads) != 0 && len(r.Ads) != len(r.Rows) {
				t.Fatalf("%d matched ads for %d rows", len(r.Ads), len(r.Rows))
			}
			for i, ad := range r.Ads {
				if got := valueDisplay(ad.EvaluateAttr("Machine")); got != r.Rows[i][0] {
					t.Errorf("row %d prints Machine %q but its matched ad holds %q",
						i, r.Rows[i][0], got)
				}
			}
		})
	}
}

// `SELECT *` over a UNION is the shape whose matched ads ARE the projection, so the ads the
// formats serialize are the matched ones -- and they are ordered with the rows.
func TestUnionStarOrderByOrdersItsAds(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT * FROM a UNION ALL SELECT * FROM b ORDER BY Machine")
	if r.Projected != nil {
		t.Fatal("SELECT * built projected ads; its own ads are the projection")
	}
	if len(r.Ads) != len(r.Rows) {
		t.Fatalf("%d ads for %d rows", len(r.Ads), len(r.Rows))
	}
	idx := columnIndex(r.Columns, "Machine")
	for i, ad := range r.Ads {
		if got := valueDisplay(ad.EvaluateAttr("Machine")); got != r.Rows[i][idx] {
			t.Errorf("row %d prints Machine %q but its ad holds %q", i, r.Rows[i][idx], got)
		}
	}
}

// Branches match BY POSITION and the first branch names the columns, so a branch that
// spells its column differently still reports under the operation's name. The table has
// always done this; the ads have to say the same thing.
func TestUnionRekeysBranchColumnNames(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT Machine FROM a UNION ALL SELECT Digits FROM b")
	if got, want := formatString(r, FormatJSON),
		"{\"Machine\":\"m5\"}\n{\"Machine\":\"42\"}\n"; got != want {
		t.Errorf("JSON gave %q, want %q -- the second branch reports under the first's name", got, want)
	}
	assertNamesMatchColumns(t, r)
	assertRowsAndAdsCorrespond(t, r)
}

// An aggregate branch has no matched ads behind its rows, so the combined result cannot
// carry one per row and carries none -- while the projection, which both branches do
// supply, survives and keeps the group key's type.
func TestUnionOverAggregates(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	r := mustExec(t, e, "SELECT Digits, COUNT(*) AS n FROM a GROUP BY Digits "+
		"UNION ALL SELECT Digits, COUNT(*) AS n FROM b GROUP BY Digits")
	if got, want := formatString(r, FormatJSON),
		"{\"Digits\":\"123\",\"n\":1}\n{\"Digits\":\"42\",\"n\":1}\n"; got != want {
		t.Errorf("JSON gave %q, want %q -- a group key keeps the kind it grouped on", got, want)
	}
	assertRowsAndAdsCorrespond(t, r)

	// An aggregate branch beside a row-scan branch: the row-scan branch has matched ads and
	// the aggregate has none, so keeping what arrived would leave one branch's ads under
	// the other branch's rows.
	mixed := mustExec(t, e, "SELECT COUNT(*) AS n FROM a UNION ALL SELECT Used AS n FROM b")
	if mixed.Ads != nil {
		t.Errorf("%d matched ads for %d rows: a branch that has none makes them all unusable",
			len(mixed.Ads), len(mixed.Rows))
	}
	if got, want := formatString(mixed, FormatJSON), "{\"n\":1}\n{\"n\":3}\n"; got != want {
		t.Errorf("JSON gave %q, want %q", got, want)
	}
}

// The projection is all or nothing. A branch that cannot project drops it for the whole
// operation, because a projection shorter than the rows does not degrade to display text --
// the ad formats serialize that many rows and silently stop.
func TestSettleCombinedAdsIsAllOrNothing(t *testing.T) {
	newRes := func() *Result {
		return &Result{
			IsSelect:  true,
			Columns:   []string{"Machine"},
			Rows:      [][]string{{"m5"}, {"m3"}},
			Projected: []*classad.ClassAd{classad.New()},
			Ads:       []*classad.ClassAd{classad.New()},
		}
	}
	r := newRes()
	settleCombinedAds(r, false, true)
	if r.Projected != nil {
		t.Errorf("a branch without a projection left %d projected ads for %d rows",
			len(r.Projected), len(r.Rows))
	}
	if r.Ads == nil {
		t.Error("the matched ads were dropped along with the projection")
	}

	r = newRes()
	settleCombinedAds(r, true, false)
	if r.Ads != nil {
		t.Errorf("a branch without matched ads left %d of them for %d rows",
			len(r.Ads), len(r.Rows))
	}
	if r.Projected == nil {
		t.Error("the projection was dropped along with the matched ads")
	}

	r = newRes()
	settleCombinedAds(r, true, true)
	if r.Projected == nil || r.Ads == nil {
		t.Error("whole branches lost their ads")
	}
}

// assertRowsAndAdsCorrespond checks the invariant every per-row slice is held to: one
// projected ad per row, holding this row's cells under this row's columns.
func assertRowsAndAdsCorrespond(t *testing.T, r *Result) {
	t.Helper()
	if r.Projected == nil {
		t.Fatal("the result carries no projection")
	}
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("%d projected ads for %d rows", len(r.Projected), len(r.Rows))
	}
	for i, ad := range r.Projected {
		for j, col := range r.Columns {
			if got := valueDisplay(ad.EvaluateAttr(col)); got != r.Rows[i][j] {
				t.Errorf("row %d prints %s = %q but its ad holds %q", i, col, r.Rows[i][j], got)
			}
		}
	}
}
