package arrowstream

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// serve stands up a real dbrpc server over a pipe, so the export runs against the actual RPC
// surface rather than against a stand-in. The type profile and the scan are two separate round
// trips, and a stub would hide a disagreement between them.
func serve(t *testing.T) (*dbrpc.Client, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServer(d)
	cconn, sconn := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sconn), dbrpc.ServeOptions{Privileged: true, IncludePrivate: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cconn))
	return c, func() { c.Close(); s.Close(); d.Close() }
}

// seed writes ads whose shapes are the ones measured on a production AP: an integer attribute
// with one stray expression, a float attribute that ClassAd writes without a decimal point when
// the value is integral, an attribute that is usually undefined, and a policy expression.
func seed(t *testing.T, c *dbrpc.Client, n int) {
	t.Helper()
	ctx := context.Background()
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		src := fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nQDate = %d\n", i%4, 1000+i, 1700000000+i)
		if i == 0 {
			src += "RequestMemory = RequestCpus * 1024\n" // the one stray expression
		} else {
			src += "RequestMemory = 2048\n"
		}
		if i%10 == 0 {
			src += "CpusUsage = 1\n"
		} else {
			src += fmt.Sprintf("CpusUsage = %d.25\n", i%3)
		}
		if i%4 == 0 {
			src += "Site = \"CHTC\"\n"
		} else {
			src += "Site = undefined\n"
		}
		src += "Requirements = TARGET.Cpus >= RequestCpus\n"
		if err := tx.NewClassAd(ctx, fmt.Sprintf("k%d", i), src); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func readBack(t *testing.T, buf *bytes.Buffer) ([]arrow.Record, *arrow.Schema) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("the stream is not readable Arrow IPC: %v", err)
	}
	defer r.Release()
	var recs []arrow.Record
	for r.Next() {
		rec := r.Record()
		rec.Retain()
		recs = append(recs, rec)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	return recs, r.Schema()
}

// TestExportRoundTrip is the whole read path: profile over RPC, derive, scan over RPC, encode,
// Arrow IPC out, and read back with an independent Arrow reader. Reading it back matters -- a
// stream this package alone can parse would prove nothing about whether DuckDB or pandas can.
func TestExportRoundTrip(t *testing.T) {
	c, cleanup := serve(t)
	defer cleanup()
	const n = 100
	seed(t, c, n)

	var buf bytes.Buffer
	res, err := Export(context.Background(), c, &buf, optsFor())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if res.Rows != n {
		t.Errorf("rows %d, want %d", res.Rows, n)
	}
	if res.Unparsed != 0 {
		t.Errorf("%d rows did not parse", res.Unparsed)
	}
	if res.Batches == 0 {
		t.Error("no batches written")
	}

	recs, schema := readBack(t, &buf)
	total := 0
	for _, rec := range recs {
		total += int(rec.NumRows())
		rec.Release()
	}
	if total != n {
		t.Errorf("read back %d rows, want %d", total, n)
	}

	byName := map[string]arrow.DataType{}
	for _, f := range schema.Fields() {
		byName[f.Name] = f.Type
	}
	// The shapes that matter, each one a rule this chain is supposed to apply.
	want := map[string]arrow.Type{
		"Owner":         arrow.STRING,
		"ClusterId":     arrow.INT64,
		"RequestMemory": arrow.INT64,     // one stray expression must not retype it
		"CpusUsage":     arrow.FLOAT64,   // int/real mix widens
		"Site":          arrow.STRING,    // string + undefined is a nullable string
		"QDate":         arrow.TIMESTAMP, // declared epoch
	}
	for name, wantType := range want {
		dt, ok := byName[name]
		if !ok {
			t.Errorf("%s: no column in the streamed schema", name)
			continue
		}
		if dt.ID() != wantType {
			t.Errorf("%s: %v, want %v", name, dt.ID(), wantType)
		}
	}
	if _, ok := byName["Requirements"]; ok {
		t.Error("Requirements got a column; it is a policy expression and belongs in the tail")
	}
	if got := res.Untyped()["Requirements"]; got != "expression-valued" {
		t.Errorf("Requirements untyped reason %q, want expression-valued", got)
	}
}

func optsFor() Options {
	return Options{
		BatchRows: 32, // several batches over 100 rows, so batching is exercised
		Derive:    arrowtype.Options{TimeAttrs: map[string]bool{"qdate": true}},
	}
}

// TestExportBatching pins that a multi-batch stream carries one schema throughout. A consumer
// cannot accept a column changing type partway through, and a stream assembled per batch could
// drift without anyone noticing until a reader rejected it.
func TestExportBatching(t *testing.T) {
	c, cleanup := serve(t)
	defer cleanup()
	seed(t, c, 100)

	var buf bytes.Buffer
	res, err := Export(context.Background(), c, &buf, optsFor())
	if err != nil {
		t.Fatal(err)
	}
	if res.Batches < 3 {
		t.Fatalf("%d batches at 32 rows over 100 rows, want several", res.Batches)
	}
	recs, schema := readBack(t, &buf)
	for i, rec := range recs {
		if !rec.Schema().Equal(schema) {
			t.Errorf("batch %d carries a different schema", i)
		}
		rec.Release()
	}
}

// TestExportConstraintAndLimit: the schema is derived from a sample of the WHOLE table, not
// from the constraint's matches, so a filtered export still carries the table's schema. A
// schema that varied by query would make a cached catalog untrustworthy.
func TestExportConstraintAndLimit(t *testing.T) {
	c, cleanup := serve(t)
	defer cleanup()
	seed(t, c, 100)

	var all bytes.Buffer
	full, err := Export(context.Background(), c, &all, optsFor())
	if err != nil {
		t.Fatal(err)
	}

	opts := optsFor()
	opts.Constraint = `Owner == "user1"`
	var some bytes.Buffer
	got, err := Export(context.Background(), c, &some, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows == 0 || got.Rows >= full.Rows {
		t.Errorf("constrained export returned %d of %d rows", got.Rows, full.Rows)
	}
	if !got.Schema.Equal(full.Schema) {
		t.Error("the constrained export carries a different schema from the full one")
	}

	opts.Constraint = ""
	opts.Limit = 7
	var few bytes.Buffer
	lim, err := Export(context.Background(), c, &few, opts)
	if err != nil {
		t.Fatal(err)
	}
	if lim.Rows != 7 {
		t.Errorf("limit 7 returned %d rows", lim.Rows)
	}
}

// TestExportStatsAccountForEveryCell: the per-column accounting has to survive the trip, or the
// one signal that a column quietly nulled part of itself is lost.
func TestExportStatsAccountForEveryCell(t *testing.T) {
	c, cleanup := serve(t)
	defer cleanup()
	const n = 100
	seed(t, c, n)

	var buf bytes.Buffer
	res, err := Export(context.Background(), c, &buf, optsFor())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stats) == 0 {
		t.Fatal("no column stats reported")
	}
	for _, st := range res.Stats {
		if st.Total() != n {
			t.Errorf("%s: stats total %d, want %d", st.Name, st.Total(), n)
		}
	}
	// Site is undefined in three quarters of the ads, and that has to show as nulls rather
	// than as empty strings.
	for _, st := range res.Stats {
		if st.Name != "Site" {
			continue
		}
		if st.Nulls[arrowtype.CauseUndefined] != n-n/4 {
			t.Errorf("Site: %d undefined nulls, want %d", st.Nulls[arrowtype.CauseUndefined], n-n/4)
		}
	}
}

// TestExportEmptyTable: a table with no rows is a valid stream with a schema and no batches,
// not an error and not an unreadable buffer.
func TestExportEmptyTable(t *testing.T) {
	c, cleanup := serve(t)
	defer cleanup()

	var buf bytes.Buffer
	res, err := Export(context.Background(), c, &buf, optsFor())
	if err != nil {
		t.Fatalf("empty table: %v", err)
	}
	if res.Rows != 0 || res.Batches != 0 {
		t.Errorf("rows=%d batches=%d, want 0/0", res.Rows, res.Batches)
	}
	recs, _ := readBack(t, &buf)
	if len(recs) != 0 {
		t.Errorf("%d batches read back from an empty export", len(recs))
	}
}
