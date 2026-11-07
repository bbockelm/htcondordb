package main

import (
	"fmt"
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/htcondordb/repl"
)

// distinctFixture is three ads chosen so a DISTINCT has something to de-duplicate, something
// to compute, and a pair of values that print alike without being alike: the string "4" and
// the number 4 both live in Txt.
func distinctFixture(t *testing.T) (*repl.Executor, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ key, text string }{
		{"k1", "Machine = \"h1\"\nUsed = 5\nTxt = \"4\"\n"},
		{"k2", "Machine = \"h1\"\nUsed = 5\nTxt = 4\n"},
		{"k3", "Machine = \"h2\"\nUsed = 7\nTxt = \"x\"\n"},
	} {
		if uerr := d.UpdateOld(row.key, row.text); uerr != nil {
			t.Fatal(uerr)
		}
	}
	srv := dbrpc.NewServer(d)
	a, b := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(a)) }()
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(b))
	return repl.NewExecutor(cl, repl.ExecConfig{}),
		func() { _ = cl.Close(); srv.Close(); _ = d.Close() }
}

// distinctCells runs sql and returns every row's typed cells, keyed by the cells rendered
// with their Go types so two rows that print alike can still be told apart.
func distinctCells(t *testing.T, ex *repl.Executor, sql string) map[string]int {
	t.Helper()
	r, err := ex.ExecString(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := map[string]int{}
	for i := range r.Rows {
		out[fmt.Sprintf("%#v", cellValues(r, i))]++
	}
	return out
}

// A DISTINCT column is typed from the value the SELECT evaluated, exactly as the same
// column without DISTINCT is -- a computed one included.
//
// `SELECT DISTINCT Used * 2` used to answer `undefined`: de-duplication is grouping, and the
// executor asked the store to group by an attribute named "Used * 2", which no ad carries.
// cellValues reads the typed value out of the row's projected ad, so the wrong value
// reached a C caller as a JSON null with nothing to say the arithmetic had never run.
func TestCellValuesDistinctColumns(t *testing.T) {
	ex, done := distinctFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	for _, tc := range []struct {
		name string
		sql  string
		want map[string]int
	}{
		{
			name: "computed column",
			sql:  "SELECT DISTINCT Used * 2 FROM " + tbl,
			want: map[string]int{`[]interface {}{10}`: 1, `[]interface {}{14}`: 1},
		},
		{
			name: "aliased computed column",
			sql:  "SELECT DISTINCT Used * 2 AS d FROM " + tbl,
			want: map[string]int{`[]interface {}{10}`: 1, `[]interface {}{14}`: 1},
		},
		{
			name: "plain column",
			sql:  "SELECT DISTINCT Machine FROM " + tbl,
			want: map[string]int{`[]interface {}{"h1"}`: 1, `[]interface {}{"h2"}`: 1},
		},
		{
			name: "aliased plain column",
			sql:  "SELECT DISTINCT Machine AS host FROM " + tbl,
			want: map[string]int{`[]interface {}{"h1"}`: 1, `[]interface {}{"h2"}`: 1},
		},
		{
			name: "one of two items computed",
			sql:  "SELECT DISTINCT Machine, Used * 2 FROM " + tbl,
			want: map[string]int{
				`[]interface {}{"h1", 10}`: 1,
				`[]interface {}{"h2", 14}`: 1,
			},
		},
		{
			// The two rows that print alike are two rows, and their types say which is
			// which: a C caller sees the string "4" and the number 4, not two 4s.
			name: "a string and the number it prints as",
			sql:  "SELECT DISTINCT Txt FROM " + tbl,
			want: map[string]int{
				`[]interface {}{"4"}`: 1,
				`[]interface {}{4}`:   1,
				`[]interface {}{"x"}`: 1,
			},
		},
		{
			// The same, with a computed column beside it, which groups the rows here
			// rather than in the store. The answer must not depend on that.
			name: "a string and its number, beside a computed column",
			sql:  "SELECT DISTINCT Txt, Used * 2 FROM " + tbl,
			want: map[string]int{
				`[]interface {}{"4", 10}`: 1,
				`[]interface {}{4, 10}`:   1,
				`[]interface {}{"x", 14}`: 1,
			},
		},
		{
			// undefined converts to null, and the three ads that lack the attribute are
			// one row rather than three.
			name: "undefined is one row",
			sql:  "SELECT DISTINCT Missing * 2 FROM " + tbl,
			want: map[string]int{`[]interface {}{interface {}(nil)}`: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := distinctCells(t, ex, tc.sql)
			if len(got) != len(tc.want) {
				t.Fatalf("cells %v, want %v", got, tc.want)
			}
			for k, n := range tc.want {
				if got[k] != n {
					t.Errorf("cells %v, want %v", got, tc.want)
					break
				}
			}
		})
	}
}

// The cells survive ORDER BY and LIMIT in the right order. cellValues addresses a row BY
// INDEX into two parallel slices, so a sort that moved the rows without their projected ads
// would hand the caller another row's values under this row's columns -- with every length
// check still passing.
func TestCellValuesDistinctOrderedAndLimited(t *testing.T) {
	ex, done := distinctFixture(t)
	defer done()
	tbl := dbrpc.DefaultTable

	for _, tc := range []struct {
		sql  string
		want [][]any
	}{
		{
			sql:  "SELECT DISTINCT Machine, Used * 2 AS d FROM " + tbl + " ORDER BY d",
			want: [][]any{{"h1", int64(10)}, {"h2", int64(14)}},
		},
		{
			sql:  "SELECT DISTINCT Machine, Used * 2 AS d FROM " + tbl + " ORDER BY d DESC",
			want: [][]any{{"h2", int64(14)}, {"h1", int64(10)}},
		},
		{
			sql:  "SELECT DISTINCT Machine, Used * 2 AS d FROM " + tbl + " ORDER BY d DESC LIMIT 1",
			want: [][]any{{"h2", int64(14)}},
		},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			r, err := ex.ExecString(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != len(tc.want) {
				t.Fatalf("%d rows %v, want %d", len(r.Rows), r.Rows, len(tc.want))
			}
			for i, want := range tc.want {
				got := cellValues(r, i)
				if len(got) != len(want) {
					t.Fatalf("row %d: %d cells, want %d", i, len(got), len(want))
				}
				for j := range want {
					if got[j] != want[j] {
						t.Errorf("row %d cell %d = %#v (%T), want %#v (%T)",
							i, j, got[j], got[j], want[j], want[j])
					}
				}
			}
		})
	}
}
