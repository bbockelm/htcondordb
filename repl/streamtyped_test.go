package repl

import (
	"sort"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// adAttrNames lists a streamed ad's attribute names, sorted, so a test can say what the
// cursor emits without depending on insertion order.
func adAttrNames(ad *classad.ClassAd) string {
	names := append([]string(nil), ad.GetAttributes()...)
	sort.Strings(names)
	return strings.Join(names, ",")
}

// THE NAMES THE CURSOR EMITS ARE THE CONTRACT, and they are pinned here because the values
// behind them are not the names: a header is whatever the user wrote, an attribute name has
// to be legal, and the two are reconciled by folding the header (see rowAd). A caller walks
// these ads by name, so the folding has to stay put no matter where the value came from.
func TestStreamAttrNamesAreTheFoldedHeaders(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	for _, tc := range []struct {
		query string
		names string
	}{
		// An aggregate call is not an attribute name; the fold runs the function and its
		// argument together, and drops COUNT's star.
		{"SELECT Digits, COUNT(*) FROM a GROUP BY Digits", "Count,Digits"},
		{"SELECT SUM(Used) FROM a", "SumUsed"},
		{"SELECT SUM(Used) AS total FROM a", "total"},
		// A computed header is not a name either: its illegal characters come off.
		{"SELECT Used * 2 FROM a UNION SELECT Used * 2 FROM b", "Used2"},
		{"SELECT Used * 2 AS d FROM a UNION SELECT Used * 2 AS d FROM b", "d"},
		// A plain column and an alias are already legal and are left exactly as written.
		{"SELECT DISTINCT Machine FROM a", "Machine"},
		{"SELECT Machine FROM a UNION SELECT Machine FROM b", "Machine"},
		{"SELECT Machine AS host FROM a UNION SELECT Machine AS host FROM b", "host"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			ads := collectStream(t, e, tc.query)
			if len(ads) == 0 {
				t.Fatal("no ads")
			}
			if got := adAttrNames(ads[0]); got != tc.names {
				t.Errorf("cursor emitted %q, want %q", got, tc.names)
			}
		})
	}
}

// A cell whose value survives reaches the cursor as that value. Reading it back out of the
// display text cannot tell the string "123" from the number 123, and these are the shapes
// with no ads of their own behind them, so the text used to be all there was.
func TestStreamKeepsCellTypes(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	for _, tc := range []struct {
		name  string
		query string
		attr  string
	}{
		{"group key", "SELECT Digits, COUNT(*) FROM a GROUP BY Digits", "Digits"},
		{"distinct", "SELECT DISTINCT Digits FROM a", "Digits"},
		{"union", "SELECT Digits FROM a UNION SELECT Digits FROM b", "Digits"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ads := collectStream(t, e, tc.query)
			if len(ads) == 0 {
				t.Fatal("no ads")
			}
			for i, ad := range ads {
				v := ad.EvaluateAttr(tc.attr)
				if !v.IsString() {
					t.Errorf("ad %d: %s is %v, want the string it was stored as", i, tc.attr, v)
				}
			}
		})
	}
}

// An aggregate's own output still arrives as rendered text and is still read the way the
// table reads it -- the projection holds the same recovery, so nothing about a count
// changes.
func TestStreamAggregateOutputStaysANumber(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	ads := collectStream(t, e, "SELECT COUNT(*) AS n, SUM(Used) AS s FROM a")
	if len(ads) != 1 {
		t.Fatalf("got %d ads, want one", len(ads))
	}
	if v := ads[0].EvaluateAttr("n"); !v.IsInteger() {
		t.Errorf("n is %v, want an integer", v)
	}
	if v := ads[0].EvaluateAttr("s"); !v.IsInteger() {
		t.Errorf("s is %v, want an integer", v)
	}
}

// A column that is UNDEFINED has no attribute, which is how a ClassAd spells it. The
// projection holds the undefined value under that name, so routing values through it must
// not start emitting an attribute where there was none.
func TestStreamUndefinedColumnHasNoAttribute(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	ads := collectStream(t, e, "SELECT DISTINCT Machine, NoSuchAttr FROM a")
	if len(ads) != 1 {
		t.Fatalf("got %d ads, want one", len(ads))
	}
	if got := adAttrNames(ads[0]); got != "Machine" {
		t.Errorf("cursor emitted %q, want Machine alone -- an undefined column is left out", got)
	}
}

// Two items may share a header, and one ad holds one attribute of that name. Cells are
// positional, so an ambiguous header is typed from the row instead and each column keeps
// its own item's value -- the same rule the C API's cells follow.
func TestStreamAmbiguousHeaderKeepsPerCellValues(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	ads := collectStream(t, e, "SELECT DISTINCT Machine AS x, Used AS x FROM a")
	if len(ads) != 1 {
		t.Fatalf("got %d ads, want one", len(ads))
	}
	if got, want := attrOf(t, ads[0], "x"), "m5"; got != want {
		t.Errorf("x = %q, want %q: the first item's cell, not the second's value", got, want)
	}
	if got, want := attrOf(t, ads[0], "x_2"), "5"; got != want {
		t.Errorf("x_2 = %q, want %q", got, want)
	}
}

// A SET OPERATION IS NOT ITS FIRST BRANCH. The statement carries the first branch's table
// and predicate, so streaming it as an ordinary SELECT hands back that branch alone and
// calls it the union -- rows silently missing, with nothing to tell the caller.
func TestStreamSelectUnionStreamsEveryBranch(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"SELECT Machine FROM a UNION SELECT Machine FROM b", []string{"m3", "m5"}},
		{"SELECT Machine FROM a UNION ALL SELECT Machine FROM b", []string{"m3", "m5"}},
		{"SELECT * FROM a UNION ALL SELECT * FROM b", []string{"m3", "m5"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			var got []string
			for _, ad := range collectStream(t, e, tc.query) {
				got = append(got, attrOf(t, ad, "Machine"))
			}
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("cursor streamed %v, want %v -- every branch's rows", got, tc.want)
			}
		})
	}
}

// A streamed UNION stops at the operation's LIMIT, not at a branch's.
func TestStreamSelectUnionHonoursTheLimit(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	ads := collectStream(t, e, "SELECT Machine FROM a UNION ALL SELECT Machine FROM b LIMIT 1")
	if len(ads) != 1 {
		t.Fatalf("got %d ads, want 1", len(ads))
	}
}

// The tuple cursor builds its cells by evaluating the SELECT's items against each streamed
// ad, which a set operation has no single answer for: a de-duplicated UNION has no ads
// behind its rows at all, and a branch may spell its columns differently from the headers
// the operation reports. RowStreamer therefore declines it and the caller runs the
// statement whole, where the rows and their typed columns are already settled.
func TestRowStreamerDeclinesSetOps(t *testing.T) {
	e, cleanup := unionFixture(t)
	defer cleanup()

	for _, q := range []string{
		"SELECT Machine FROM a UNION SELECT Machine FROM b",
		"SELECT Machine AS host FROM a UNION SELECT Machine AS host FROM b",
		"SELECT Used * 2 AS d FROM a UNION ALL SELECT Used * 2 AS d FROM b",
	} {
		t.Run(q, func(t *testing.T) {
			st, err := Parse(q)
			if err != nil {
				t.Fatal(err)
			}
			cols, rowOf, err := e.RowStreamer(st)
			if err != nil {
				t.Fatal(err)
			}
			if cols != nil || rowOf != nil {
				t.Errorf("RowStreamer took the statement (columns %q), which would evaluate "+
					"the first branch's items against every branch's rows", cols)
			}
		})
	}
}
