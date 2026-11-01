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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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
// TestUnsupportedSQLIsRefused covers the two kinds of refusal, which say different things on
// purpose. A statement whose SHAPE is unsupported names the accepted shape, because the user
// needs to know what to write instead. A predicate the lowering will not translate explains
// THAT predicate's problem -- restating the grammar there would be useless, since the shape was
// fine and the semantics were not.
func TestUnsupportedSQLIsRefused(t *testing.T) {
	cl, _, done := harness(t, 10)
	defer done()
	ctx := context.Background()

	// Shape refusals: must name what is accepted.
	for _, q := range []string{
		`SELECT COUNT(*) FROM ads`,
		`SELECT Owner AS who FROM ads`,
		`SELECT upper(Owner) FROM ads`,
		`SELECT * FROM ads ORDER BY QDate`,
		`SELECT * FROM a JOIN b ON a.x = b.x`,
		`DELETE FROM ads`,
		`nonsense`,
	} {
		_, err := cl.Execute(ctx, q)
		if err == nil {
			t.Errorf("%q: accepted, want a refusal", q)
			continue
		}
		if !strings.Contains(err.Error(), "FROM <table>") {
			t.Errorf("%q: refusal %q does not say what shape is accepted", q, err)
		}
	}

	// Predicate refusals: must say the WHERE clause is the problem, and why. Each of these
	// is valid SQL whose ClassAd meaning would differ, so it is declined rather than
	// approximated.
	for _, c := range []struct{ q, reason string }{
		{`SELECT * FROM ads WHERE Owner > 'R'`, "case-insensitively"},
		{`SELECT * FROM ads WHERE Owner <> 'alice'`, "three-valued"},
		{`SELECT * FROM ads WHERE NOT Owner = 'alice'`, "negated equality"},
		{`SELECT * FROM ads WHERE Nope = 'x'`, "unknown column"},
		{`SELECT * FROM ads WHERE Requirements = TRUE`, "no declared scalar type"},
		{`SELECT * FROM ads WHERE Owner = 5`, "non-string literal"},
	} {
		_, err := cl.Execute(ctx, c.q)
		if err == nil {
			t.Errorf("%q: accepted, want a refusal", c.q)
			continue
		}
		if !strings.Contains(err.Error(), "WHERE clause") {
			t.Errorf("%q: refusal %q does not locate the problem in the WHERE clause", c.q, err)
		}
		if !strings.Contains(err.Error(), c.reason) {
			t.Errorf("%q: refusal %q does not explain the reason (%s)", c.q, err, c.reason)
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
	full, err := clientSchemaOf(ctx, cl, q)
	if err != nil {
		t.Fatal(err)
	}
	if nullableCount(full) == full.NumFields() {
		t.Fatal("precondition: nothing is non-nullable even with a complete sample")
	}

	// Capped below the table size: nothing can be declared dense any more.
	svc.SetSampleMax(10)
	capped, err := clientSchemaOf(ctx, cl, q)
	if err != nil {
		t.Fatal(err)
	}
	if n := nullableCount(capped); n != capped.NumFields() {
		t.Errorf("%d of %d columns nullable under a capped sample; a prefix cannot prove density",
			n, capped.NumFields())
	}
}

func clientSchemaOf(ctx context.Context, cl *flightsql.Client, q string) (*arrow.Schema, error) {
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

// TestStreamBudgetRefusesRatherThanGrows is the containment this endpoint needs because it
// runs in the daemon. memory.DefaultAllocator grows until the process cannot; in a gateway
// that is a dead gateway, in process it is a dead daemon taking the job queue's reader with
// it. The query must fail instead, and it must say so as a resource limit rather than as an
// internal fault, because the client's recourse is a narrower query.
func TestStreamBudgetRefusesRatherThanGrows(t *testing.T) {
	cl, svc, done := harness(t, 200)
	defer done()
	svc.SetStreamBudget(2048) // far too small for 200 rows of several columns
	ctx := context.Background()

	info, err := cl.Execute(ctx, "SELECT * FROM "+dbrpc.DefaultTable)
	if err != nil {
		t.Fatalf("Execute should still plan; only the stream is budgeted: %v", err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err == nil {
		for rdr.Next() {
		}
		err = rdr.Err()
		rdr.Release()
	}
	if err == nil {
		t.Fatal("a 2 KiB budget served 200 rows; the budget is not reaching the encoder")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("code %v, want ResourceExhausted -- a configured limit is not an internal fault (%v)", got, err)
	}
	if !strings.Contains(err.Error(), "budget") {
		t.Errorf("error %q does not mention the budget, so an operator cannot tell what to raise", err)
	}
}

// TestStreamBudgetGenerousEnoughStillWorks keeps the test above honest: it must fail because
// the budget is small, not because a budget was set at all.
func TestStreamBudgetGenerousEnoughStillWorks(t *testing.T) {
	cl, svc, done := harness(t, 200)
	defer done()
	svc.SetStreamBudget(64 << 20)
	ctx := context.Background()

	info, err := cl.Execute(ctx, "SELECT * FROM "+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	rows := 0
	for rdr.Next() {
		rows += int(rdr.Record().NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("a 64 MiB budget refused 200 rows: %v", err)
	}
	if rows != 200 {
		t.Errorf("read %d rows, want 200", rows)
	}
}

// TestMaxStreamsRefusesWithoutQueueing covers the cap's semantics directly rather than over
// gRPC, and the reason is worth recording: a client cannot hold a slot open by declining to
// read. The server's scan goroutine fills the channel and finishes -- for any result small
// enough to keep a test fast, that happens before the client reads a byte -- and releasing the
// slot then is correct, because the work it accounts for is done. Demonstrating contention
// through the client would need a fixture big enough to block the goroutine on a full channel,
// which buys nothing this does not already pin.
//
// What matters is that the cap REFUSES rather than queues. A queue of scans inside the daemon
// is precisely what the cap exists to prevent: a refused client can retry, a client left
// hanging cannot tell whether to.
func TestMaxStreamsRefusesWithoutQueueing(t *testing.T) {
	_, svc, done := harness(t, 10)
	defer done()

	svc.SetMaxStreams(2)
	first, ok := svc.acquireStream()
	if !ok {
		t.Fatal("the first slot was refused")
	}
	second, ok := svc.acquireStream()
	if !ok {
		t.Fatal("the second slot was refused with a cap of two")
	}
	if _, ok := svc.acquireStream(); ok {
		t.Error("a third slot was admitted with a cap of two")
	}

	// Releasing admits another: a limit on concurrency, not a permanent ceiling.
	first()
	third, ok := svc.acquireStream()
	if !ok {
		t.Error("no slot after one was released")
	}

	// Releasing twice must not manufacture capacity, or a double release would raise the
	// effective cap -- the same class of bug as accounting that goes negative.
	third()
	third()
	second()
	for i := 0; i < 2; i++ {
		if _, ok := svc.acquireStream(); !ok {
			t.Fatalf("slot %d unavailable after every release", i)
		}
	}
	if _, ok := svc.acquireStream(); ok {
		t.Error("capacity exceeded the cap after a double release")
	}
}

// TestMaxStreamsUnlimited: an unconfigured endpoint is uncapped, which is what it did before
// the cap existed, and the zero value has to mean that rather than "no streams allowed".
func TestMaxStreamsUnlimited(t *testing.T) {
	_, svc, done := harness(t, 10)
	defer done()
	for _, n := range []int{0, -1} {
		svc.SetMaxStreams(n)
		for i := 0; i < 50; i++ {
			if _, ok := svc.acquireStream(); !ok {
				t.Fatalf("SetMaxStreams(%d): refused slot %d, want unlimited", n, i)
			}
		}
	}
}

// TestProjection is the grammar's first widening, and it widened by exactly the argument that
// made SELECT * safe: a column list contains no expression, so it means the same thing in every
// dialect and nothing about it can be reinterpreted when the ANSI front end lands.
func TestProjection(t *testing.T) {
	cl, _, done := harness(t, 60)
	defer done()
	ctx := context.Background()

	// Order is as asked for, not as the table reports it: a client reads by position.
	schema, err := clientSchemaOf(ctx, cl, `SELECT CpusUsage, Owner, ClusterId FROM `+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"CpusUsage", "Owner", "ClusterId"}
	if schema.NumFields() != len(want) {
		t.Fatalf("%d columns, want %d", schema.NumFields(), len(want))
	}
	for i, name := range want {
		if got := schema.Field(i).Name; got != name {
			t.Errorf("column %d is %q, want %q -- projection order must be preserved", i, got, name)
		}
	}
	// And the types are still the measured ones, not re-guessed.
	if schema.Field(0).Type.ID() != arrow.FLOAT64 {
		t.Errorf("CpusUsage is %v, want float64", schema.Field(0).Type.ID())
	}

	// Names are case-insensitive, as ClassAd attribute names are.
	if _, err := clientSchemaOf(ctx, cl, `SELECT oWnEr FROM `+dbrpc.DefaultTable); err != nil {
		t.Errorf("a differently-cased column name was refused: %v", err)
	}

	// The rows come back projected too, not just the schema.
	info, err := cl.Execute(ctx, `SELECT Owner, ClusterId FROM `+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	rows := 0
	for rdr.Next() {
		rec := rdr.Record()
		if rec.NumCols() != 2 {
			t.Fatalf("a batch has %d columns, want 2", rec.NumCols())
		}
		rows += int(rec.NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 60 {
		t.Errorf("read %d rows, want 60", rows)
	}
}

// TestProjectionRefusesAColumnItCannotServe: silently returning fewer columns than asked for
// is the failure this endpoint exists not to have. A client reads by position, so an omitted
// column does not shift one value -- it shifts every value after it.
func TestProjectionRefusesAColumnItCannotServe(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()

	if _, err := cl.Execute(ctx, `SELECT NoSuchAttribute FROM `+dbrpc.DefaultTable); err == nil {
		t.Error("a column the table does not have was accepted")
	} else if !strings.Contains(err.Error(), "no such column") {
		t.Errorf("refusal %q does not say the column is unknown", err)
	}

	// Requirements exists but has no TYPED column -- it is a policy expression. Asking for
	// it must be refused with that reason, not silently dropped.
	_, err := cl.Execute(ctx, `SELECT Requirements FROM `+dbrpc.DefaultTable)
	if err == nil {
		t.Fatal("an untyped attribute was served as a column")
	}
	if !strings.Contains(err.Error(), "no typed form") {
		t.Errorf("refusal %q does not explain why the column is unavailable", err)
	}
}

func TestLimit(t *testing.T) {
	cl, _, done := harness(t, 100)
	defer done()
	ctx := context.Background()

	info, err := cl.Execute(ctx, `SELECT * FROM `+dbrpc.DefaultTable+` LIMIT 7`)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	rows := 0
	for rdr.Next() {
		rows += int(rdr.Record().NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 7 {
		t.Errorf("LIMIT 7 returned %d rows", rows)
	}
}

func TestParseQueryShapes(t *testing.T) {
	for _, c := range []struct {
		q     string
		table string
		cols  []string
		limit int
	}{
		{`SELECT * FROM jobs`, "jobs", nil, 0},
		{`select * from jobs;`, "jobs", nil, 0},
		{`SELECT * FROM "jobs"`, "jobs", nil, 0},
		{`SELECT a FROM jobs`, "jobs", []string{"a"}, 0},
		{`SELECT a, b , c FROM jobs`, "jobs", []string{"a", "b", "c"}, 0},
		{`SELECT "a", b FROM jobs`, "jobs", []string{"a", "b"}, 0},
		{`SELECT * FROM jobs LIMIT 5`, "jobs", nil, 5},
		{`SELECT a FROM jobs limit 5 ;`, "jobs", []string{"a"}, 5},
		{`SELECT * FROM archives/history`, "archives/history", nil, 0},
	} {
		got, err := parseQuery(c.q)
		if err != nil {
			t.Errorf("%q: %v", c.q, err)
			continue
		}
		if got.table != c.table || got.limit != c.limit || len(got.cols) != len(c.cols) {
			t.Errorf("%q: got %+v, want table=%s cols=%v limit=%d", c.q, got, c.table, c.cols, c.limit)
			continue
		}
		for i := range c.cols {
			if got.cols[i] != c.cols[i] {
				t.Errorf("%q: col %d = %q, want %q", c.q, i, got.cols[i], c.cols[i])
			}
		}
	}
}

// TestWhereClause is the dialect reaching the endpoint. The case-sensitivity check is the one
// that matters: ClassAd's == would match both spellings, and SQL's = must match one.
func TestWhereClause(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	count := func(q string) int {
		t.Helper()
		info, err := cl.Execute(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rdr.Release()
		n := 0
		for rdr.Next() {
			n += int(rdr.Record().NumRows())
		}
		if err := rdr.Err(); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}

	all := count(`SELECT * FROM ` + dbrpc.DefaultTable)
	if all != 40 {
		t.Fatalf("unfiltered count %d, want 40", all)
	}

	// The seed writes Owner = user0..user3, so a quarter match.
	if got := count(`SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE Owner = 'user1'`); got != 10 {
		t.Errorf("Owner = 'user1' matched %d rows, want 10", got)
	}
	// Case-sensitively: ClassAd's == would match these, SQL's = must not.
	if got := count(`SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE Owner = 'USER1'`); got != 0 {
		t.Errorf("Owner = 'USER1' matched %d rows; = must be case-sensitive", got)
	}
	// Numeric, boolean composition, IN and BETWEEN all reach the store.
	if got := count(`SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE ClusterId >= 1020`); got != 20 {
		t.Errorf("ClusterId >= 1020 matched %d rows, want 20", got)
	}
	if got := count(`SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE Owner = 'user1' AND ClusterId >= 1020`); got != 5 {
		t.Errorf("the conjunction matched %d rows, want 5", got)
	}
	if got := count(`SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE Owner IN ('user1', 'user2')`); got != 20 {
		t.Errorf("IN matched %d rows, want 20", got)
	}
	if got := count(`SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE ClusterId BETWEEN 1000 AND 1009`); got != 10 {
		t.Errorf("BETWEEN matched %d rows, want 10", got)
	}
	// WHERE composes with projection and LIMIT.
	if got := count(`SELECT Owner FROM ` + dbrpc.DefaultTable + ` WHERE Owner = 'user1' LIMIT 3`); got != 3 {
		t.Errorf("WHERE + projection + LIMIT matched %d rows, want 3", got)
	}
}

// TestWhereDoesNotChangeTheSchema: a predicate selects ROWS. The columns a table offers are a
// property of the table, and a schema that varied by query would make a cached catalog
// worthless.
func TestWhereDoesNotChangeTheSchema(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()

	plain, err := clientSchemaOf(ctx, cl, `SELECT * FROM `+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := clientSchemaOf(ctx, cl, `SELECT * FROM `+dbrpc.DefaultTable+` WHERE ClusterId > 1005`)
	if err != nil {
		t.Fatal(err)
	}
	if !plain.Equal(filtered) {
		t.Error("a WHERE clause changed the schema")
	}
}

// TestWhereRefusalIsReportedBeforeRowsAreFetched: a predicate the endpoint cannot serve must
// fail when the schema is asked for, not after a client has committed to reading a stream.
func TestWhereRefusalIsReportedBeforeRowsAreFetched(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()
	q := `SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE Owner > 'R'`

	if _, err := cl.GetExecuteSchema(ctx, q); err == nil {
		t.Error("GetExecuteSchema accepted a predicate the endpoint cannot serve")
	}
	if _, err := cl.Execute(ctx, q); err == nil {
		t.Error("Execute accepted it too")
	} else if !strings.Contains(err.Error(), "WHERE clause") {
		t.Errorf("refusal %q does not say the WHERE clause is the problem", err)
	}
}
