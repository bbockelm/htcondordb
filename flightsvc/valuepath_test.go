package flightsvc

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

// columnStrings runs q and returns every requested column's values, rendered, in row order.
func columnStrings(t *testing.T, cl *flightsql.Client, q string, names []string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	info, err := cl.Execute(ctx, q)
	if err != nil {
		t.Fatalf("Execute(%s): %v", q, err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	defer rdr.Release()

	out := map[string][]string{}
	for rdr.Next() {
		rec := rdr.Record()
		for _, name := range names {
			idx := rec.Schema().FieldIndices(name)
			if len(idx) != 1 {
				t.Fatalf("%s: %d columns named it, want 1", name, len(idx))
			}
			col := rec.Column(idx[0])
			for row := range int(rec.NumRows()) {
				out[name] = append(out[name], col.ValueStr(row))
			}
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	return out
}

// TestValuePathAgreesWithTheTextPath is the differential that makes the fast path safe to take.
//
// An explicit projection is served by the typed value stream and SELECT * by the text
// projection -- two different store ops, two different encoder entry points, the same data. The
// values they produce have to be identical, because which one a query gets is decided by the
// shape of the query and not by anything the user asked for.
//
// MemoryUsage is in the projection deliberately: it is an expression in every ad, so the value
// path gets it evaluated in the store and the text path evaluates it here. That is the pair
// most likely to disagree, and the one that decides whether an expression-valued attribute
// gets a typed column at all.
func TestValuePathAgreesWithTheTextPath(t *testing.T) {
	cl, _, done := harness(t, 60)
	defer done()

	names := []string{"Owner", "ClusterId", "QDate", "CpusUsage", "MemoryUsage", "RequestMemory", "Ratio"}
	projected := columnStrings(t, cl, "SELECT Owner, ClusterId, QDate, CpusUsage, MemoryUsage, "+
		"RequestMemory, Ratio FROM "+dbrpc.DefaultTable, names)
	star := columnStrings(t, cl, "SELECT * FROM "+dbrpc.DefaultTable, names)

	// The premise for Ratio: it must actually be null in some rows and not others, or the
	// error branch it exists to exercise is not reached and the comparison below is vacuous.
	nulls := 0
	for _, v := range projected["Ratio"] {
		if v == "(null)" || v == "" {
			nulls++
		}
	}
	if nulls == 0 || nulls == len(projected["Ratio"]) {
		t.Fatalf("Ratio is null in %d of %d rows; the fixture must make it an error in SOME of them",
			nulls, len(projected["Ratio"]))
	}

	for _, name := range names {
		p, s := projected[name], star[name]
		if len(p) != 60 || len(s) != 60 {
			t.Fatalf("%s: %d rows projected, %d from SELECT *, want 60 of each", name, len(p), len(s))
		}
		for i := range p {
			if p[i] != s[i] {
				t.Fatalf("%s row %d: the value stream gave %q and the text path %q",
					name, i, p[i], s[i])
			}
		}
	}
}

// TestValuePathKeepsTheDeclaredTypes: the schema is promised before the rows are fetched, and
// the two paths must not reach it differently. A column that arrived as int64 one way and
// float64 the other would be a schema that depends on how the query was phrased.
func TestValuePathKeepsTheDeclaredTypes(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()

	projected, err := clientSchemaOf(ctx, cl, "SELECT Owner, ClusterId, MemoryUsage FROM "+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	star, err := clientSchemaOf(ctx, cl, "SELECT * FROM "+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Owner", "ClusterId", "MemoryUsage"} {
		pi, si := projected.FieldIndices(name), star.FieldIndices(name)
		if len(pi) != 1 || len(si) != 1 {
			t.Fatalf("%s: %d/%d columns", name, len(pi), len(si))
		}
		if p, s := projected.Field(pi[0]), star.Field(si[0]); !typeEqual(p, s) {
			t.Errorf("%s is %v when projected and %v from SELECT *", name, p.Type, s.Type)
		}
	}
}

func typeEqual(a, b arrow.Field) bool { return arrow.TypeEqual(a.Type, b.Type) }
