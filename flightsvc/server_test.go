package flightsvc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// harness runs the whole stack the way it will actually run: a real store, a real dbrpc
// session, a real gRPC listener, and arrow-go's own Flight SQL client over it. A stub at any
// layer would hide the layers disagreeing, and the point of this test is that they do not.
func harness(t *testing.T, rows int) (*flightsql.Client, *Server, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	seed(t, d, rows)

	dbSrv := dbrpc.NewServer(d)
	cc, sc := net.Pipe()
	go func() {
		_ = dbSrv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{Privileged: true, IncludePrivate: true})
	}()
	dbc := dbrpc.NewClient(dbrpc.NewStreamConn(cc))

	svc, err := NewServer(dbc, arrowtype.Options{
		TimeAttrs: map[string]bool{"qdate": true},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gsrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(gsrv, NewFlightServer(svc, nil))
	go func() { _ = gsrv.Serve(ln) }()

	cl, err := flightsql.NewClient(ln.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return cl, svc, func() { cl.Close(); gsrv.Stop(); dbc.Close(); dbSrv.Close(); d.Close() }
}

func seed(t *testing.T, d *db.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		src := fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nQDate = %d\n", i%4, 1000+i, 1700000000+i)
		if i == 0 {
			src += "RequestMemory = RequestCpus * 1024\n" // one stray expression
		} else {
			src += "RequestMemory = 2048\n"
		}
		if i%10 == 0 {
			src += "CpusUsage = 1\n"
		} else {
			src += fmt.Sprintf("CpusUsage = %d.25\n", i%3)
		}
		src += "Requirements = TARGET.Cpus >= RequestCpus\n"
		ad, err := classad.ParseOld(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCatalogSurface(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()

	// Tables: the listing a catalog browser builds its tree from.
	info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: false})
	if err != nil {
		t.Fatal(err)
	}
	names := readStringColumn(t, cl, info, "table_name")
	if len(names) == 0 {
		t.Fatal("no tables listed")
	}
	found := false
	for _, n := range names {
		if n == dbrpc.DefaultTable {
			found = true
		}
	}
	if !found {
		t.Errorf("tables %v do not include %q", names, dbrpc.DefaultTable)
	}

	// Table types and schemas: small, fixed, and a client reads them at connect.
	tt, err := cl.GetTableTypes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := readStringColumn(t, cl, tt, "table_type"); len(got) != 1 || got[0] != tableType {
		t.Errorf("table types %v, want [%s]", got, tableType)
	}
	sch, err := cl.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if got := readStringColumn(t, cl, sch, "db_schema_name"); len(got) != 1 || got[0] != defaultSchemaName {
		t.Errorf("db schemas %v, want [%s]", got, defaultSchemaName)
	}
}

// TestGetTablesWithSchema is the one that makes the catalog worth having: a client can see
// each table's COLUMN TYPES without running a query, and those types are the measured ones.
func TestGetTablesWithSchema(t *testing.T) {
	cl, _, done := harness(t, 50)
	defer done()
	ctx := context.Background()

	info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true})
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()

	var serialized []byte
	for rdr.Next() {
		rec := rdr.Record()
		names := rec.Column(2).(*array.String)
		blobs := rec.Column(4).(*array.Binary)
		for i := 0; i < int(rec.NumRows()); i++ {
			if names.Value(i) == dbrpc.DefaultTable && !blobs.IsNull(i) {
				serialized = append([]byte(nil), blobs.Value(i)...)
			}
		}
	}
	if serialized == nil {
		t.Fatal("no schema returned for the default table")
	}
	schema, err := flight.DeserializeSchema(serialized, nil)
	if err != nil {
		t.Fatalf("the catalog's schema blob does not deserialize: %v", err)
	}
	assertSeededTypes(t, schema)
}

// TestSelectAllRoundTrip is the path an ADBC client takes: ask for the result's schema, then
// fetch the rows, and get the same schema both times.
func TestSelectAllRoundTrip(t *testing.T) {
	const n = 50
	cl, _, done := harness(t, n)
	defer done()
	ctx := context.Background()

	q := "SELECT * FROM " + dbrpc.DefaultTable

	// The schema is answerable WITHOUT running the query, which is why the measurement
	// has to be separable from the scan.
	declaredRes, err := cl.GetExecuteSchema(ctx, q)
	if err != nil {
		t.Fatalf("GetExecuteSchema: %v", err)
	}
	declared, err := flight.DeserializeSchema(declaredRes.Schema, nil)
	if err != nil {
		t.Fatalf("the declared schema does not deserialize: %v", err)
	}
	assertSeededTypes(t, declared)

	info, err := cl.Execute(ctx, q)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	defer rdr.Release()

	rows := 0
	for rdr.Next() {
		rec := rdr.Record()
		rows += int(rec.NumRows())
		if !rec.Schema().Equal(rdr.Schema()) {
			t.Error("a batch carries a different schema from the stream")
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	if rows != n {
		t.Errorf("read %d rows, want %d", rows, n)
	}
	if !rdr.Schema().Equal(declared) {
		t.Error("the schema the rows arrived with differs from the one GetExecuteSchema promised")
	}
}

// assertSeededTypes checks the rules the whole stack is supposed to apply, on the shapes that
// made them necessary.
func assertSeededTypes(t *testing.T, schema *arrow.Schema) {
	t.Helper()
	want := map[string]arrow.Type{
		"Owner":         arrow.STRING,
		"ClusterId":     arrow.INT64,
		"RequestMemory": arrow.INT64,     // one stray expression must not make it text
		"CpusUsage":     arrow.FLOAT64,   // int/real mix widens
		"QDate":         arrow.TIMESTAMP, // declared epoch
	}
	for name, wantType := range want {
		i := schema.FieldIndices(name)
		if len(i) == 0 {
			t.Errorf("%s: no column", name)
			continue
		}
		if got := schema.Field(i[0]).Type.ID(); got != wantType {
			t.Errorf("%s: %v, want %v", name, got, wantType)
		}
	}
	if len(schema.FieldIndices("Requirements")) != 0 {
		t.Error("Requirements got a column; it is a policy expression and belongs in the tail")
	}
}

// TestUnsupportedSQLIsRefused is the contract that makes shipping a partial SQL surface safe.
// Nothing downstream corrects this server, so a query it half-understands would return rows
// that look right and are not. Every one of these must be refused, and the refusal must say
// what IS accepted.
func TestUnsupportedSQLIsRefused(t *testing.T) {
	cl, _, done := harness(t, 10)
	defer done()
	ctx := context.Background()

	for _, q := range []string{
		`SELECT * FROM ads WHERE Owner = 'alice'`,
		`SELECT Owner FROM ads`,
		`SELECT COUNT(*) FROM ads`,
		`SELECT * FROM a JOIN b ON a.x = b.x`,
		`DELETE FROM ads`,
		`SELECT * FROM ads LIMIT 10`,
		`nonsense`,
	} {
		_, err := cl.Execute(ctx, q)
		if err == nil {
			t.Errorf("%q: accepted, want a refusal", q)
			continue
		}
		if !strings.Contains(err.Error(), "SELECT * FROM") {
			t.Errorf("%q: refusal %q does not say what is accepted", q, err)
		}
	}
}

func TestUnknownTableIsAnError(t *testing.T) {
	cl, _, done := harness(t, 5)
	defer done()
	if _, err := cl.Execute(context.Background(), "SELECT * FROM nosuchtable"); err == nil {
		t.Error("an unknown table was accepted")
	}
}

func readStringColumn(t *testing.T, cl *flightsql.Client, info *flight.FlightInfo, col string) []string {
	t.Helper()
	rdr, err := cl.DoGet(context.Background(), info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	var out []string
	for rdr.Next() {
		rec := rdr.Record()
		idx := rec.Schema().FieldIndices(col)
		if len(idx) == 0 {
			t.Fatalf("no column %q", col)
		}
		arr := rec.Column(idx[0]).(*array.String)
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestCappedSampleCannotDeclareDensity reaches the soundness rule that a quick fixture
// otherwise hides. With the default cap every small table is sampled completely, so "measured
// everything" and "measured a prefix" never differ and the rule goes unexercised -- which is
// how a mutation that dropped it passed the whole suite.
//
// Capped, a column present in every sampled record still cannot be declared non-nullable: the
// unsampled remainder may not have it, and a non-nullable column the encoder then nulls is a
// promise broken to the reader.
func TestCappedSampleCannotDeclareDensity(t *testing.T) {
	cl, svc, done := harness(t, 50)
	defer done()
	ctx := context.Background()
	q := "SELECT * FROM " + dbrpc.DefaultTable

	// Complete sample: the dense columns are provably dense.
	full, err := schemaOf(ctx, cl, q)
	if err != nil {
		t.Fatal(err)
	}
	if nullableCount(full) == full.NumFields() {
		t.Fatal("precondition: nothing is non-nullable even with a complete sample")
	}

	// Capped below the table size: nothing can be declared dense any more.
	svc.SetSampleMax(10)
	capped, err := schemaOf(ctx, cl, q)
	if err != nil {
		t.Fatal(err)
	}
	if n := nullableCount(capped); n != capped.NumFields() {
		t.Errorf("%d of %d columns nullable under a capped sample; a prefix cannot prove density",
			n, capped.NumFields())
	}
}

func schemaOf(ctx context.Context, cl *flightsql.Client, q string) (*arrow.Schema, error) {
	res, err := cl.GetExecuteSchema(ctx, q)
	if err != nil {
		return nil, err
	}
	return flight.DeserializeSchema(res.Schema, nil)
}

func nullableCount(s *arrow.Schema) int {
	n := 0
	for _, f := range s.Fields() {
		if f.Nullable {
			n++
		}
	}
	return n
}
