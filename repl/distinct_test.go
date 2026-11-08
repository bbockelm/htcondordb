package repl

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// distinctExec is three rows chosen so that every DISTINCT shape has something to
// de-duplicate and something to keep apart:
//
//   - Machine repeats, so a plain column has a duplicate to drop.
//   - Used repeats across two rows and differs in the third, so `Used * 2` has one
//     duplicate value and one unique one -- and its two spellings, 10 and 10, come from
//     rows that are otherwise different, which is what makes the de-duplication visible.
//   - Txt holds the string "4", the number 4 and the string "x": the first two print the
//     same and are not the same value.
func distinctRowsExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE a")
	mustExec(t, e, `INSERT INTO a (Key, Machine, Used, Txt) VALUES ('1', 'h1', 5, '4')`)
	mustExec(t, e, `INSERT INTO a (Key, Machine, Used, Txt) VALUES ('2', 'h1', 5, 4)`)
	mustExec(t, e, `INSERT INTO a (Key, Machine, Used, Txt) VALUES ('3', 'h2', 7, 'x')`)
	return e, cleanup
}

// distinctRowSet renders a result's rows as a set of pipe-joined cells, for comparing an answer
// whose ROW ORDER is unspecified -- a DISTINCT without ORDER BY returns its groups in
// whatever order they were first seen, which is a property of the scan and not of the
// question.
func distinctRowSet(r *Result) map[string]int {
	out := map[string]int{}
	for _, row := range r.Rows {
		out[strings.Join(row, "|")]++
	}
	return out
}

func wantRowSet(t *testing.T, r *Result, want ...string) {
	t.Helper()
	got := distinctRowSet(r)
	if len(got) != len(r.Rows) {
		t.Errorf("rows %v contain a duplicate, which DISTINCT exists to remove", r.Rows)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows %v, want %d: %v", len(r.Rows), r.Rows, len(want), want)
	}
	for _, w := range want {
		if got[w] == 0 {
			t.Fatalf("rows %v, want %v", r.Rows, want)
		}
	}
}

// THE BUG: a computed SELECT item under DISTINCT.
//
// `SELECT DISTINCT a, b` is `GROUP BY a, b`, and that is how the executor runs it so the
// server can group by those attributes rather than ship every matched ad. A computed item
// has no attribute to name: `Used * 2` is source text, and asking the store to group by an
// attribute of that name gave one group whose key was undefined for every row. The result
// was a single row reading `undefined` -- for a SELECT that prints 10 and 14 without the
// word DISTINCT in front of it -- with no error anywhere to say the question had not been
// answered.
//
// Every shape in the family is pinned here rather than just the reproduction, because the
// routing decision is per statement and a shape that takes a different branch is a shape
// this says nothing about.
func TestDistinctOverComputedColumns(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	for _, tc := range []struct {
		name  string
		query string
		cols  []string
		rows  []string
	}{
		{
			name:  "computed column",
			query: `SELECT DISTINCT Used * 2 FROM a`,
			cols:  []string{"Used * 2"},
			rows:  []string{"10", "14"},
		},
		{
			name:  "aliased computed column",
			query: `SELECT DISTINCT Used * 2 AS d FROM a`,
			cols:  []string{"d"},
			rows:  []string{"10", "14"},
		},
		{
			name:  "plain column",
			query: `SELECT DISTINCT Machine FROM a`,
			cols:  []string{"Machine"},
			rows:  []string{"h1", "h2"},
		},
		{
			name:  "aliased plain column",
			query: `SELECT DISTINCT Machine AS host FROM a`,
			cols:  []string{"host"},
			rows:  []string{"h1", "h2"},
		},
		{
			// Only one of the two items is computed. The plain one used to come out right
			// beside a column of undefined, which reads as a gap in the data rather than
			// as a broken query.
			name:  "one of two items computed",
			query: `SELECT DISTINCT Machine, Used * 2 FROM a`,
			cols:  []string{"Machine", "Used * 2"},
			rows:  []string{"h1|10", "h2|14"},
		},
		{
			name:  "function call",
			query: `SELECT DISTINCT strcat(Machine, "!") AS tag FROM a`,
			cols:  []string{"tag"},
			rows:  []string{"h1!", "h2!"},
		},
		{
			// A computed item over the same attribute as a plain one: the two columns are
			// grouped together, so the pair is the key.
			name:  "computed over a column also selected plain",
			query: `SELECT DISTINCT Used, Used * 2 FROM a`,
			cols:  []string{"Used", "Used * 2"},
			rows:  []string{"5|10", "7|14"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := mustExec(t, e, tc.query)
			if strings.Join(r.Columns, ",") != strings.Join(tc.cols, ",") {
				t.Fatalf("columns %q, want %q", r.Columns, tc.cols)
			}
			wantRowSet(t, r, tc.rows...)
			assertProjectionComplete(t, r)
		})
	}
}

// The computed item evaluates to the same thing it does without DISTINCT. Pinning the two
// against each other is what catches a DISTINCT that answers a different question from the
// SELECT it de-duplicates, which is how the bug presented: both queries ran, only one was
// right, and nothing compared them.
func TestDistinctAgreesWithTheUndistinctSelect(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	plain := mustExec(t, e, `SELECT Used * 2 FROM a`)
	distinct := mustExec(t, e, `SELECT DISTINCT Used * 2 FROM a`)

	// Three ads, three rows, one of the two values repeated: that is what DISTINCT is
	// handed, and the shape the comparison needs.
	seen := distinctRowSet(plain)
	if len(plain.Rows) != 3 || seen["10"] != 2 || seen["14"] != 1 {
		t.Fatalf("the undistinct SELECT gave %v, want 10 twice and 14 once", plain.Rows)
	}
	wantRowSet(t, distinct, "10", "14")
	for _, row := range distinct.Rows {
		if seen[strings.Join(row, "|")] == 0 {
			t.Errorf("DISTINCT produced the row %v, which the same SELECT without it never "+
				"produces: %v", row, plain.Rows)
		}
	}
}

// ORDER BY and LIMIT over a de-duplicated result.
//
// Both reach a DISTINCT result AFTER the rows have been reduced, which is the only reading
// that makes sense: ordering the duplicates and then dropping them would order a different
// set, and limiting before de-duplication would cap the scan rather than the answer.
func TestDistinctWithOrderByAndLimit(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`SELECT DISTINCT Used * 2 FROM a ORDER BY Used * 2`, []string{"10", "14"}},
		{`SELECT DISTINCT Used * 2 FROM a ORDER BY Used * 2 DESC`, []string{"14", "10"}},
		{`SELECT DISTINCT Used * 2 AS d FROM a ORDER BY d`, []string{"10", "14"}},
		{`SELECT DISTINCT Used * 2 AS d FROM a ORDER BY d DESC`, []string{"14", "10"}},
		{`SELECT DISTINCT Machine FROM a ORDER BY Machine`, []string{"h1", "h2"}},
		{`SELECT DISTINCT Machine FROM a ORDER BY Machine DESC`, []string{"h2", "h1"}},
		// LIMIT caps the de-duplicated rows: there are three matching ads but only two
		// distinct doubles, so LIMIT 2 is the whole answer and LIMIT 1 is half of it.
		{`SELECT DISTINCT Used * 2 AS d FROM a ORDER BY d LIMIT 1`, []string{"10"}},
		{`SELECT DISTINCT Used * 2 AS d FROM a ORDER BY d DESC LIMIT 1`, []string{"14"}},
		{`SELECT DISTINCT Used * 2 AS d FROM a ORDER BY d LIMIT 2`, []string{"10", "14"}},
		{`SELECT DISTINCT Used * 2 AS d FROM a ORDER BY d LIMIT 5`, []string{"10", "14"}},
		{`SELECT DISTINCT Machine FROM a ORDER BY Machine LIMIT 1`, []string{"h1"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			r := mustExec(t, e, tc.query)
			var got []string
			for _, row := range r.Rows {
				got = append(got, strings.Join(row, "|"))
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("rows %v, want %v", got, tc.want)
			}
			assertProjectionComplete(t, r)
			assertRowsAndProjectionAgree(t, r)
		})
	}
}

// A DISTINCT alongside an explicit GROUP BY. The grouping has already made the rows
// distinct, so the keyword adds nothing -- but it must not take anything away either, and
// it decides nothing about the routing: the GROUP BY does.
func TestDistinctOverGroupBy(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT DISTINCT Machine FROM a GROUP BY Machine`)
	wantRowSet(t, r, "h1", "h2")

	r = mustExec(t, e, `SELECT DISTINCT Machine, COUNT(*) FROM a GROUP BY Machine`)
	wantRowSet(t, r, "h1|2", "h2|1")

	// A computed group key with DISTINCT in front of it: the GROUP BY routes it, and the
	// counts are per computed value.
	r = mustExec(t, e, `SELECT DISTINCT Used * 2 AS d, COUNT(*) FROM a GROUP BY d`)
	wantRowSet(t, r, "10|2", "14|1")
}

// `SELECT DISTINCT *` de-duplicates whole ads rather than a column tuple, which is a
// different mechanism (dedupeAds) and so a different thing to pin. The fixture's three rows
// differ in Key, so none of them merge; a fourth row identical to an earlier one but for
// its key shows the ads being compared whole.
func TestDistinctStar(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT DISTINCT * FROM a`)
	if len(r.Rows) != 3 {
		t.Fatalf("got %d rows %v, want the three ads, which differ in Key", len(r.Rows), r.Rows)
	}
	if !r.Star {
		t.Error("Star is not set on a SELECT * result")
	}
	if len(r.Ads) != len(r.Rows) {
		t.Errorf("%d ads for %d rows: SELECT * carries one ad per row", len(r.Ads), len(r.Rows))
	}
	// Rows that are equal as ADS merge. The key is an attribute like any other, so two ads
	// that agree on everything including it are one row.
	mustExec(t, e, "CREATE TABLE b_dup")
	mustExec(t, e, `INSERT INTO b_dup (Key, Machine) VALUES ('1', 'h1')`)
	mustExec(t, e, `INSERT INTO b_dup (Key, Machine) VALUES ('1', 'h1')`)
	r = mustExec(t, e, `SELECT DISTINCT * FROM b_dup`)
	if len(r.Rows) != 1 {
		t.Errorf("got %d rows %v, want 1: the same key written twice is one ad", len(r.Rows), r.Rows)
	}
}

// WHAT DISTINCT COMPARES: values, not the text they print as.
//
// The string "4" and the number 4 render identically, and a result that de-duplicated on
// the rendering would merge them into one row under a label naming one of them -- the same
// class of wrong answer as a group key read back out of display text. They are two rows.
//
// This holds for the plain-column case, which the store groups, AND for a tuple containing
// a computed item, which is grouped here: both spellings of DISTINCT have to agree, or the
// answer would depend on whether some other column in the SELECT happened to be computed.
func TestDistinctComparesValuesNotTheirRendering(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	// Three rows: the string "4", the number 4 and the string "x". The first two print
	// alike -- the LABELS collide, as display text does -- and stay separate rows.
	r := mustExec(t, e, `SELECT DISTINCT Txt FROM a`)
	if len(r.Rows) != 3 {
		t.Fatalf("got %d rows %v, want 3: the string \"4\" and the number 4 are not one value",
			len(r.Rows), r.Rows)
	}
	kinds := map[string]bool{}
	for _, ad := range r.Projected {
		v := ad.EvaluateAttr("Txt")
		switch {
		case v.IsString():
			s, _ := v.StringValue()
			kinds["string:"+s] = true
		case v.IsInteger():
			kinds["int"] = true
		}
	}
	for _, want := range []string{`string:4`, `string:x`, "int"} {
		if !kinds[want] {
			t.Errorf("the projected values are %v, missing %q", kinds, want)
		}
	}

	// The same column beside a computed one, which moves the grouping client-side. Still
	// three rows, because the key is the value either way.
	r = mustExec(t, e, `SELECT DISTINCT Txt, Used * 2 FROM a`)
	if len(r.Rows) != 3 {
		t.Fatalf("got %d rows %v, want 3: a computed column beside Txt must not merge the "+
			"string \"4\" with the number 4", len(r.Rows), r.Rows)
	}
	// Two rows PRINT alike here, so the assertion is on the counts: a set of rendered rows
	// would lose exactly the distinction under test.
	counts := distinctRowSet(r)
	if counts["4|10"] != 2 || counts["x|14"] != 1 {
		t.Errorf("rows %v, want the pair 4|10 twice -- once for the string, once for the "+
			"number -- and x|14 once", r.Rows)
	}
}

// A boolean and the string that spells it, for the same reason. `true` and "true" are two
// values; a text-keyed de-duplication merges them.
func TestDistinctKeepsBoolsApartFromTheirSpelling(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE flags")
	mustExec(t, e, `INSERT INTO flags (Key, V, N) VALUES ('1', true, 1)`)
	mustExec(t, e, `INSERT INTO flags (Key, V, N) VALUES ('2', 'true', 1)`)

	// Grouped here, because N * 1 is computed.
	r := mustExec(t, e, `SELECT DISTINCT V, N * 1 AS n FROM flags`)
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows %v, want 2: the boolean true and the string \"true\" are not "+
			"one value", len(r.Rows), r.Rows)
	}
}

// UNDEFINED and ERROR are values, and two rows that are both undefined are ONE distinct row.
//
// That is SQL's reading of DISTINCT over NULL -- the rows are alike for the purpose of
// de-duplication even though `NULL = NULL` is unknown -- and it is what the store already
// does for a plain column, so the client-side grouping follows it rather than inventing a
// second answer. ClassAd's `undefined == undefined` is likewise undefined, so a comparison
// with == would keep every one of them and `SELECT DISTINCT Missing` would print one row
// per ad.
//
// undefined and error do NOT merge with each other: they are different values and print
// differently.
func TestDistinctCollapsesUndefinedAndError(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	for _, tc := range []struct{ query, want string }{
		// Three ads, none carrying the attribute: one row.
		{`SELECT DISTINCT Missing FROM a`, "undefined"},
		// Computed from an attribute no ad carries: still one row, now through the
		// client-side grouping.
		{`SELECT DISTINCT Missing * 2 FROM a`, "undefined"},
		// An expression that errors for every row: one row, and it says error rather
		// than undefined -- the two are not the same answer.
		{`SELECT DISTINCT Machine + 1 FROM a`, "error"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			r := mustExec(t, e, tc.query)
			if len(r.Rows) != 1 || r.Rows[0][0] != tc.want {
				t.Fatalf("rows %v, want one row reading %q", r.Rows, tc.want)
			}
			assertProjectionComplete(t, r)
		})
	}

	// undefined and error in one column are two rows, not one.
	mustExec(t, e, `INSERT INTO a (Key, Machine, Used) VALUES ('4', 'h3', 'notanumber')`)
	r := mustExec(t, e, `SELECT DISTINCT Used * 2 FROM a`)
	got := distinctRowSet(r)
	if got["error"] == 0 || got["undefined"] != 0 {
		t.Errorf("rows %v: a row whose arithmetic errors must read error", r.Rows)
	}
}

// assertProjectionComplete pins Result.Projected's all-or-nothing rule: a format that reads
// it reads it for EVERY row, so a partly-filled one serializes two shapes under one column
// set and the rows that fell back are silently short. The count is the whole test.
func assertProjectionComplete(t *testing.T, r *Result) {
	t.Helper()
	if r.Star {
		return // SELECT * has no projection; its ads are one
	}
	if len(r.Projected) != len(r.Rows) {
		t.Fatalf("%d projected ads for %d rows: the projection is all or nothing",
			len(r.Projected), len(r.Rows))
	}
}

// assertRowsAndProjectionAgree pins the parallel slices to each other cell by cell. A sort
// or a LIMIT that moved one and not the other would leave every length check passing while
// handing the ad formats another row's values under this row's columns.
func assertRowsAndProjectionAgree(t *testing.T, r *Result) {
	t.Helper()
	if len(r.Projected) != len(r.Rows) {
		return // assertProjectionComplete reports this
	}
	for i, row := range r.Rows {
		for j, col := range r.Columns {
			if j >= len(row) {
				continue
			}
			if got := valueDisplay(r.Projected[i].EvaluateAttr(col)); got != row[j] {
				t.Errorf("row %d column %q: the table prints %q and the projected ad holds "+
					"%q -- the two slices are out of step", i, col, row[j], got)
			}
		}
	}
}

// The de-duplication removes rows from every parallel slice at once. UNION ALL ... ORDER BY
// desynchronized exactly this way: the rows moved and the ads behind them did not, so the
// ad-shaped formats answered with a different row's values and every length check passed.
//
// The check is run over the shapes that take different branches -- plain, computed, mixed,
// with and without an ORDER BY and a LIMIT -- because each assembles its rows separately.
func TestDistinctKeepsParallelSlicesInStep(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	for _, q := range []string{
		`SELECT DISTINCT Machine FROM a`,
		`SELECT DISTINCT Used * 2 FROM a`,
		`SELECT DISTINCT Used * 2 AS d FROM a`,
		`SELECT DISTINCT Machine, Used * 2 FROM a`,
		`SELECT DISTINCT Txt, Used * 2 FROM a`,
		`SELECT DISTINCT Machine, Used * 2 FROM a ORDER BY Machine DESC`,
		`SELECT DISTINCT Machine, Used * 2 FROM a ORDER BY Machine DESC LIMIT 1`,
		`SELECT DISTINCT Used * 2 AS d FROM a ORDER BY d DESC LIMIT 1`,
	} {
		t.Run(q, func(t *testing.T) {
			r := mustExec(t, e, q)
			assertProjectionComplete(t, r)
			assertRowsAndProjectionAgree(t, r)
			if len(r.Ads) != 0 && len(r.Ads) != len(r.Rows) {
				t.Errorf("%d matched ads for %d rows", len(r.Ads), len(r.Rows))
			}
		})
	}
}

// Every output format answers the question the SELECT asked. The table's cells, the JSON
// document and both ClassAd renderings come from one result, so a projection that is right
// in one and wrong in another is the drift this catches -- and the computed column is where
// it was wrong in all of them at once.
func TestDistinctFormats(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE f")
	mustExec(t, e, `INSERT INTO f (Key, Machine, Used) VALUES ('1', 'm1', 3)`)
	mustExec(t, e, `INSERT INTO f (Key, Machine, Used) VALUES ('2', 'm1', 3)`)

	for _, tc := range []struct {
		name          string
		query         string
		table         string
		json          string
		old, adNew    string
		columnsWanted []string
	}{
		{
			name:          "computed column",
			query:         `SELECT DISTINCT Used * 2 AS d FROM f`,
			columnsWanted: []string{"d"},
			table:         "d\n-\n6\n",
			json:          "{\"d\":6}\n",
			old:           "d = 6\n\n",
			adNew:         "[d = 6]\n",
		},
		{
			name:          "plain and computed",
			query:         `SELECT DISTINCT Machine, Used * 2 AS d FROM f`,
			columnsWanted: []string{"Machine", "d"},
			table:         "Machine | d\n--------+--\nm1      | 6\n",
			json:          "{\"d\":6,\"Machine\":\"m1\"}\n",
			old:           "d = 6\nMachine = \"m1\"\n\n",
			adNew:         "[d = 6; Machine = \"m1\"]\n",
		},
		{
			name:          "plain column",
			query:         `SELECT DISTINCT Machine AS host FROM f`,
			columnsWanted: []string{"host"},
			table:         "host\n----\nm1  \n",
			json:          "{\"host\":\"m1\"}\n",
			old:           "host = \"m1\"\n\n",
			adNew:         "[host = \"m1\"]\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := mustExec(t, e, tc.query)
			if strings.Join(r.Columns, ",") != strings.Join(tc.columnsWanted, ",") {
				t.Fatalf("columns %q, want %q", r.Columns, tc.columnsWanted)
			}
			for _, f := range []struct {
				format Format
				want   string
			}{
				{FormatTable, tc.table},
				{FormatJSON, tc.json},
				{FormatClassAdOld, tc.old},
				{FormatClassAdNew, tc.adNew},
			} {
				got := formatString(r, f.format)
				if f.format == FormatTable {
					// The table's footer carries the execution time, which no
					// assertion can pin; everything above it is the answer.
					if i := strings.Index(got, "\n(1 row"); i >= 0 {
						got = got[:i+1]
					}
				}
				if got != f.want {
					t.Errorf("%s format gave %q, want %q", f.format, got, f.want)
				}
			}
			assertNamesMatchColumns(t, r)
		})
	}
}

// A streamed DISTINCT carries the same values a materialized one does.
//
// StreamSelect cannot deliver a DISTINCT row as it arrives -- no row is known to be new
// until the last ad has been seen -- so it collects and streams what the executor produced.
// What it streams has to be the ANSWER: the ads behind a DISTINCT are gone by then, and the
// ones synthesized in their place used to be typed by reading the display cells back, which
// turned a group key that is the string "4" into the number 4.
func TestDistinctStreamsTypedValues(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	st, err := Parse(`SELECT DISTINCT Txt FROM a`)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	if serr := e.StreamSelect(st, func(ad *classad.ClassAd) bool {
		v := ad.EvaluateAttr("Txt")
		switch {
		case v.IsString():
			s, _ := v.StringValue()
			kinds = append(kinds, "string:"+s)
		case v.IsInteger():
			kinds = append(kinds, "int")
		default:
			kinds = append(kinds, "other")
		}
		return true
	}); serr != nil {
		t.Fatal(serr)
	}
	want := map[string]bool{"string:4": true, "string:x": true, "int": true}
	if len(kinds) != 3 {
		t.Fatalf("streamed %v, want three rows", kinds)
	}
	for _, k := range kinds {
		if !want[k] {
			t.Errorf("streamed %v, which is not one of the three values", kinds)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Errorf("streamed %v; never saw %v", kinds, want)
	}
}

// A computed DISTINCT column cannot be produced from a streamed ad at all, so RowStreamer
// declines rather than evaluating the expression against an ad that no longer holds its
// inputs -- which read undefined, silently, for every row.
func TestRowStreamerDeclinesDistinct(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	for _, q := range []string{
		`SELECT DISTINCT Used * 2 FROM a`,
		`SELECT DISTINCT Used * 2 AS d FROM a`,
		`SELECT DISTINCT Machine FROM a`,
	} {
		st, err := Parse(q)
		if err != nil {
			t.Fatal(err)
		}
		cols, row, serr := e.RowStreamer(st)
		if serr != nil {
			t.Fatalf("%s: %v", q, serr)
		}
		if row != nil || cols != nil {
			t.Errorf("%s: RowStreamer accepted a DISTINCT; its rows are a property of the "+
				"result set, not of any one ad", q)
		}
	}
}

// Two selected items can share a header (`SELECT a AS x, b AS x`). A projected ad holds one
// attribute of that name -- the later item's value -- so reading the stream's cells out of
// it by header would give both the same wrong answer, under two derived attribute names
// that are already made distinct. The display strings stay per cell and are right for both,
// so an ambiguous header falls back to them.
func TestDistinctStreamAmbiguousHeaderFallsBackToCells(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	st, err := Parse(`SELECT DISTINCT Machine AS x, Used AS x FROM a`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if serr := e.StreamSelect(st, func(ad *classad.ClassAd) bool {
		got = append(got, valueDisplay(ad.EvaluateAttr("x"))+"|"+valueDisplay(ad.EvaluateAttr("x_2")))
		return true
	}); serr != nil {
		t.Fatal(serr)
	}
	want := map[string]bool{"h1|5": true, "h2|7": true}
	if len(got) != 2 {
		t.Fatalf("streamed %v, want two rows", got)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("streamed %v: the first column's value was read from the second's "+
				"attribute", got)
		}
	}
}

// distinctHasExpr is the routing decision, and what it means is narrow: this statement's
// DISTINCT is the thing doing the grouping AND one of the keys is computed. The shapes it
// must say no to are the ones another predicate already owns -- an explicit GROUP BY is
// groupByHasExpr's to answer, and `*` and an aggregate are not DISTINCT-grouped at all --
// so a yes there would route a query twice on one reason and leave the predicate's name
// describing something it no longer tests.
func TestDistinctHasExpr(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
		why  string
	}{
		{`SELECT DISTINCT Used * 2 FROM a`, true, "DISTINCT over a computed item"},
		{`SELECT DISTINCT Used * 2 AS d FROM a`, true, "an alias does not make it plain"},
		{`SELECT DISTINCT Machine, Used * 2 FROM a`, true, "one computed item is enough"},
		{`SELECT DISTINCT strcat(Machine, "!") FROM a`, true, "a call is computed"},

		{`SELECT DISTINCT Machine FROM a`, false, "a plain column: the store groups it"},
		{`SELECT DISTINCT Machine AS host FROM a`, false, "an aliased plain column"},
		{`SELECT Used * 2 FROM a`, false, "no DISTINCT: nothing is grouped"},
		{`SELECT DISTINCT * FROM a`, false, "`*` de-duplicates whole ads, not a tuple"},
		{`SELECT DISTINCT Used * 2 AS d, COUNT(*) FROM a GROUP BY d`, false,
			"an explicit GROUP BY owns the decision; groupByHasExpr answers for it"},
		{`SELECT DISTINCT COUNT(*) FROM a`, false, "an aggregate is not DISTINCT-grouped"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			st, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := distinctHasExpr(st); got != tc.want {
				t.Errorf("distinctHasExpr = %v, want %v (%s)", got, tc.want, tc.why)
			}
		})
	}
}

// An undefined column is spelled by LEAVING THE ATTRIBUTE OUT, which is what a ClassAd does
// and what every other synthesized ad in the stream does. Writing the value in instead would
// put a literal `undefined` in the ad text a reader gets, where the same column of the same
// query without DISTINCT carries nothing at all. An error is a value and is written.
func TestDistinctStreamSpellsUndefinedByOmission(t *testing.T) {
	e, cleanup := distinctRowsExec(t)
	defer cleanup()

	for _, tc := range []struct{ query, want string }{
		{`SELECT DISTINCT Nope FROM a`, "[]"},
		{`SELECT DISTINCT Nope * 2 FROM a`, "[]"},
		{`SELECT DISTINCT Machine + 1 AS bad FROM a`, "[bad = error]"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			st, err := Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			if serr := e.StreamSelect(st, func(ad *classad.ClassAd) bool {
				got = append(got, ad.String())
				return true
			}); serr != nil {
				t.Fatal(serr)
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("streamed %v, want one ad %q", got, tc.want)
			}
		})
	}
}
