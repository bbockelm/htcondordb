package flightsvc

import (
	"fmt"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// oddStrings are the strings a display row cannot spell apart from something else: three that
// spell a number, and the three cells a row writes for a missing value, a failed one and an
// empty one. Every one of them is a STRING in the fixture.
var oddStrings = []string{"100", "101", "102", "undefined", "error", ""}

// TestAnalyticGroupKeysKeepTheirValues: an aggregate and a DISTINCT have no ads behind their
// rows, so this endpoint used to re-read the executor's rendered text to fill the batch --
// under which half of this column arrives as a null and a quoted number would arrive as a
// number. The typed values the executor already evaluated are what cross instead.
func TestAnalyticGroupKeysKeepTheirValues(t *testing.T) {
	cl, _, d, done := harnessWithStore(t, 0, arrowtype.Options{})
	defer done()
	seedOddStrings(t, d)

	for _, q := range []string{
		"SELECT DISTINCT S FROM " + dbrpc.DefaultTable,
		"SELECT S FROM " + dbrpc.DefaultTable + " GROUP BY S",
	} {
		recs, schema := readAll(t, cl, q)
		if got := schema.Field(0).Type.ID(); got != arrow.STRING {
			t.Fatalf("%s: S is %v, want utf8", q, got)
		}
		var got []string
		for _, rec := range recs {
			col := rec.Column(0).(*array.String)
			for i := 0; i < int(rec.NumRows()); i++ {
				if col.IsNull(i) {
					got = append(got, "<null>")
					continue
				}
				got = append(got, col.Value(i))
			}
			rec.Release()
		}
		sort.Strings(got)
		want := append([]string(nil), oddStrings...)
		sort.Strings(want)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s gave %q, want %q", q, got, want)
		}
	}
}

func seedOddStrings(t *testing.T, d *db.DB) {
	t.Helper()
	for i, s := range oddStrings {
		ad, err := classad.ParseOld(fmt.Sprintf(
			"Owner = \"user%d\"\nClusterId = %d\nS = %q\n", i%2, 1000+i, s))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Put(fmt.Sprintf("odd%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
}
