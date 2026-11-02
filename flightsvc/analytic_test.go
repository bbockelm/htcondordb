package flightsvc

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/dbrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// readAll runs a query to completion and returns its batches, so a test can assert on values
// rather than only on the fact that something came back.
func readAll(t *testing.T, cl *flightsql.Client, q string) ([]arrow.Record, *arrow.Schema) {
	t.Helper()
	ctx := context.Background()
	info, err := cl.Execute(ctx, q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rdr.Release()
	var out []arrow.Record
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		out = append(out, rec)
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return out, rdr.Schema()
}

func totalRows(recs []arrow.Record) int {
	n := 0
	for _, r := range recs {
		n += int(r.NumRows())
	}
	return n
}

// TestAggregateIsAnsweredByTheStore is the capability this path exists for: before it, a client
// wanting a count per owner pulled every row and counted them itself, over an endpoint whose
// store already had a GROUP BY.
//
// It asserts the VALUES, not the row count. A GROUP BY that returned one row per record would
// have the right column types and the wrong answer.
func TestAggregateIsAnsweredByTheStore(t *testing.T) {
	cl, _, done := harnessWith(t, 40, arrowtype.Options{})
	defer done()

	recs, schema := readAll(t, cl, "SELECT Owner, COUNT(*) FROM "+dbrpc.DefaultTable+" GROUP BY Owner")
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	// The fixture is 40 ads over 4 owners, so the answer is four groups of ten. Stated as
	// the premise it is: a fixture with one owner would pass a broken GROUP BY.
	if got := totalRows(recs); got != 4 {
		t.Fatalf("%d group rows, want 4 (one per owner in the fixture)", got)
	}
	if schema.NumFields() != 2 {
		t.Fatalf("schema has %d fields, want Owner and the count", schema.NumFields())
	}
	if f := schema.Field(1); f.Type.ID() != arrow.INT64 || f.Nullable {
		t.Errorf("the count is %v nullable=%v, want a non-nullable int64", f.Type, f.Nullable)
	}
	counts := recs[0].Column(1).(*array.Int64)
	for i := 0; i < counts.Len(); i++ {
		if counts.Value(i) != 10 {
			t.Errorf("group %d counted %d, want 10", i, counts.Value(i))
		}
	}
}

// TestAnalyticSchemaIsAnsweredWithoutExecuting is a protocol requirement rather than a nicety:
// Flight promises a shape in one RPC and delivers rows in another, so an aggregate has to be
// typed from its SELECT list before anything runs. A path that could only learn its types by
// executing could not implement GetSchema at all.
func TestAnalyticSchemaIsAnsweredWithoutExecuting(t *testing.T) {
	cl, _, done := harnessWith(t, 40, arrowtype.Options{})
	defer done()

	res, err := cl.GetExecuteSchema(context.Background(),
		"SELECT Owner, COUNT(*), AVG(ClusterId) FROM "+dbrpc.DefaultTable+" GROUP BY Owner")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := flight.DeserializeSchema(res.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []arrow.Type{arrow.STRING, arrow.INT64, arrow.FLOAT64}
	if schema.NumFields() != len(want) {
		t.Fatalf("%d fields, want %d", schema.NumFields(), len(want))
	}
	for i, w := range want {
		if got := schema.Field(i).Type.ID(); got != w {
			t.Errorf("field %d (%s) is %v, want %v", i, schema.Field(i).Name, got, w)
		}
	}
}

// TestAnalyticTypesComeFromTheSelectList pins the rules that are NOT the table's types,
// because that is where this path differs from the scan. A count is an integer whatever it
// counted; a mean is a real whatever it averaged; a MIN or MAX is the column's own type.
//
// AVG is the one worth having first: declaring it int64 because its column is int64 would
// truncate every result that is not whole, silently, in a column whose whole purpose is the
// fractional part.
func TestAnalyticTypesComeFromTheSelectList(t *testing.T) {
	cl, _, done := harnessWith(t, 40, arrowtype.Options{})
	defer done()

	for _, c := range []struct {
		sel  string
		want arrow.Type
	}{
		{"COUNT(*)", arrow.INT64},
		{"COUNT(Owner)", arrow.INT64},
		{"COUNT(DISTINCT Owner)", arrow.INT64},
		{"AVG(ClusterId)", arrow.FLOAT64}, // an int column, averaged
		{"SUM(ClusterId)", arrow.INT64},   // an int column, summed
		{"MIN(ClusterId)", arrow.INT64},   // the column's own type
		{"MAX(MemoryUsage)", arrow.INT64}, // an expression-valued attribute, evaluated
		{"MAX(Owner)", arrow.STRING},      // not every aggregate is numeric
		{"AVG(CpusUsage)", arrow.FLOAT64}, // an int/real column
	} {
		_, schema := readAll(t, cl, "SELECT "+c.sel+" FROM "+dbrpc.DefaultTable)
		if got := schema.Field(0).Type.ID(); got != c.want {
			t.Errorf("%s: %v, want %v", c.sel, got, c.want)
		}
	}

	// An AVG really does carry its fractional part rather than being floored.
	recs, _ := readAll(t, cl, "SELECT AVG(ClusterId) FROM "+dbrpc.DefaultTable)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()
	avg := recs[0].Column(0).(*array.Float64).Value(0)
	// 40 ads with ClusterId 1000..1039: the mean is 1019.5, which an int64 column could
	// not hold.
	if avg != 1019.5 {
		t.Errorf("AVG(ClusterId) = %v, want 1019.5", avg)
	}
}

// TestAnalyticUsesTheAnsiDialect is the one that would be easiest to get wrong and hardest to
// notice. The endpoint speaks ANSI, where `=` on a string is case-SENSITIVE; ClassAd's `==` is
// case-insensitive, and the executor's default dialect is ClassAd. So an analytic query whose
// predicate took the default would quietly answer a different question -- same columns, same
// types, more rows -- and no type assertion anywhere would catch it.
func TestAnalyticUsesTheAnsiDialect(t *testing.T) {
	cl, _, done := harnessWith(t, 40, arrowtype.Options{})
	defer done()

	lower, _ := readAll(t, cl,
		"SELECT COUNT(*) FROM "+dbrpc.DefaultTable+" WHERE Owner = 'user1'")
	upper, _ := readAll(t, cl,
		"SELECT COUNT(*) FROM "+dbrpc.DefaultTable+" WHERE Owner = 'USER1'")
	defer func() {
		for _, r := range append(lower, upper...) {
			r.Release()
		}
	}()

	got := lower[0].Column(0).(*array.Int64).Value(0)
	// The premise: the fixture stores lowercase owners and ten ads carry this one, so a
	// case-sensitive match finds them and there is something for the uppercase form to
	// wrongly find.
	if got != 10 {
		t.Fatalf("WHERE Owner = 'user1' counted %d, want 10; the fixture is not posing the question", got)
	}
	if got := upper[0].Column(0).(*array.Int64).Value(0); got != 0 {
		t.Errorf("WHERE Owner = 'USER1' counted %d, want 0: the predicate was read as ClassAd, "+
			"which folds case, not as ANSI", got)
	}
}

// TestAnalyticOrderByIsApplied asserts the ORDER the client asked for, not just that three rows
// came back. A LIMIT over an unordered scan returns three rows too, and they are the wrong
// three.
func TestAnalyticOrderByIsApplied(t *testing.T) {
	cl, _, done := harnessWith(t, 40, arrowtype.Options{})
	defer done()

	recs, _ := readAll(t, cl,
		"SELECT ClusterId FROM "+dbrpc.DefaultTable+" ORDER BY ClusterId DESC LIMIT 3")
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()
	if got := totalRows(recs); got != 3 {
		t.Fatalf("%d rows, want 3", got)
	}
	ids := recs[0].Column(0).(*array.Int64)
	want := []int64{1039, 1038, 1037}
	for i, w := range want {
		if ids.Value(i) != w {
			t.Errorf("row %d is %d, want %d (descending from the highest)", i, ids.Value(i), w)
		}
	}
}

// TestFlightServesNothingButSelect is the read-only promise, stated where it is structural.
// The executor underneath runs INSERT, UPDATE, DELETE and DDL, so "this endpoint is read-only"
// must not rest on the ANSI front end happening to refuse those spellings -- it rests on only
// a SELECT ever reaching the executor at all.
func TestFlightServesNothingButSelect(t *testing.T) {
	cl, _, done := harnessWith(t, 20, arrowtype.Options{})
	defer done()
	ctx := context.Background()

	for _, q := range []string{
		"INSERT INTO " + dbrpc.DefaultTable + " (Owner) VALUES ('mallory')",
		"UPDATE " + dbrpc.DefaultTable + " SET Owner = 'mallory'",
		"DELETE FROM " + dbrpc.DefaultTable,
		"DROP TABLE " + dbrpc.DefaultTable,
		"CREATE TABLE evil",
		"TRUNCATE TABLE " + dbrpc.DefaultTable,
	} {
		if _, err := cl.Execute(ctx, q); err == nil {
			t.Errorf("%s: accepted by a read-only endpoint", q)
		} else if !strings.Contains(err.Error(), "read-only") &&
			!strings.Contains(err.Error(), "unsupported SQL") {
			t.Errorf("%s: refused, but not as a read-only endpoint should: %v", q, err)
		}
	}

	// And the store is untouched: a refusal that happened after the write would pass the
	// assertion above and still have written.
	recs, _ := readAll(t, cl, "SELECT COUNT(*) FROM "+dbrpc.DefaultTable)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()
	if got := recs[0].Column(0).(*array.Int64).Value(0); got != 20 {
		t.Errorf("the table holds %d rows, want the 20 it was seeded with", got)
	}
}

// TestAnalyticRefusesWhatItCannotType names the column rather than returning it as text or
// omitting it. A client reads results by position, so a column quietly missing from an
// aggregate's output shifts every value after it.
func TestAnalyticRefusesWhatItCannotType(t *testing.T) {
	cl, _, done := harnessWith(t, 20, arrowtype.Options{})
	defer done()
	ctx := context.Background()

	for _, c := range []struct{ q, want string }{
		{"SELECT MAX(NoSuchColumn) FROM " + dbrpc.DefaultTable, "no such column"},
		{"SELECT NoSuchColumn, COUNT(*) FROM " + dbrpc.DefaultTable + " GROUP BY NoSuchColumn", "no such column"},
		{"SELECT a.Owner, COUNT(*) FROM " + dbrpc.DefaultTable + " a JOIN " + dbrpc.DefaultTable + " b ON a.Owner = b.Owner GROUP BY a.Owner", "JOIN"},
	} {
		_, err := cl.Execute(ctx, c.q)
		if err == nil {
			t.Errorf("%s: accepted", c.q)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: refused with %v, which does not mention %q", c.q, err, c.want)
		}
	}
}

// TestAnalyticPinIsEnforced: the analytic path promises a schema in one RPC and produces rows
// in another exactly as the scan does, so it needs the same guarantee. Easy to miss, because
// the fingerprint is computed in the planner and the second half is a different function.
func TestAnalyticPinIsEnforced(t *testing.T) {
	ctx := context.Background()
	const stmt = "SELECT Owner, FreshAttribute FROM " + dbrpc.DefaultTable + " ORDER BY Owner"
	_, svc, d, done := harnessWithStore(t, 20, arrowtype.Options{})
	defer done()

	// The column does not exist yet, so the statement cannot even be planned...
	if _, err := svc.planStatement(ctx, stmt); err == nil {
		t.Fatal("a column the table does not have was planned")
	}
	retype(t, d)
	// ...and once it does, it plans. Now drift the OTHER way: plan, then make the column
	// change type under it.
	pl, err := svc.planStatement(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.doGetQuery(ctx, stmt, nil, pl.fp); err != nil {
		t.Fatalf("an undisturbed table refused its own promised schema: %v", err)
	}
	if _, _, err := svc.doGetQuery(ctx, stmt, nil, "not-the-schema-you-were-promised"); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a stale pin on an analytic statement gave %v, want FailedPrecondition", err)
	}
}

// TestAnalyticPreparesButTakesNoParameters: an aggregate prepares, because preparing is how a
// client learns its statement will work and this one will. What it cannot do is carry a bound
// value -- the executor parses once and holds a ClassAd predicate thereafter, so a placeholder
// would have to be substituted before parsing, and the schema is promised before that.
//
// The refusal has to come from the bind rather than from the prepare, and it has to say THAT,
// not "unsupported SQL": a client told its statement is unsupported, by the same endpoint that
// just prepared it, has no way to act on the answer.
func TestAnalyticPreparesButTakesNoParameters(t *testing.T) {
	cl, _, done := harnessWith(t, 20, arrowtype.Options{})
	defer done()
	ctx := context.Background()

	ps, err := cl.Prepare(ctx, "SELECT Owner, COUNT(*) FROM "+dbrpc.DefaultTable+" GROUP BY Owner")
	if err != nil {
		t.Fatalf("an aggregate did not prepare: %v", err)
	}
	defer ps.Close(ctx)

	// It reports no parameters, which is the statement a client should believe.
	if n := ps.ParameterSchema().NumFields(); n != 0 {
		t.Errorf("parameter schema has %d fields, want none", n)
	}

	// And it executes on the prepared handle, which is a different code path from the
	// direct one and the one most ADBC clients actually use.
	info, err := ps.Execute(ctx)
	if err != nil {
		t.Fatalf("prepared execute: %v", err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("prepared fetch: %v", err)
	}
	rows := 0
	for rdr.Next() {
		rows += int(rdr.Record().NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	rdr.Release()
	if rows != 4 {
		t.Errorf("%d group rows from the prepared handle, want 4", rows)
	}
}

// TestAnalyticExpressionsBecomeText records a real limitation rather than asserting a success.
// A computed expression's VALUE cannot be typed from a measurement of the table -- nothing
// measured `ClusterId * 2` -- so it takes the answer the tail column takes for an attribute no
// column fits: text, which is faithful for any ClassAd value.
//
// Refusing instead would make this endpoint strictly less capable than the REPL for a shape
// the REPL answers correctly. The cost is that arithmetic downstream sees a string, which is
// what the declared type map would fix.
func TestAnalyticExpressionsBecomeText(t *testing.T) {
	cl, _, done := harnessWith(t, 20, arrowtype.Options{})
	defer done()

	recs, schema := readAll(t, cl, "SELECT ClusterId * 2 FROM "+dbrpc.DefaultTable+" ORDER BY ClusterId LIMIT 1")
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()
	if got := schema.Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("a computed expression is %v, want utf8", got)
	}
	// The VALUE is still right, which is the half that matters: this is a type
	// limitation, not a wrong answer.
	if got := recs[0].Column(0).(*array.String).Value(0); got != "2000" {
		t.Errorf("ClusterId * 2 on the lowest row is %q, want \"2000\"", got)
	}
}

// TestAnalyticAcceptsWhatItUsedToRefuse is the inverse of the refusal test, and it is here
// because a capability that is only asserted by its absence elsewhere is one nobody notices
// regressing. Each of these was refused by the scan recogniser and is now served.
func TestAnalyticAcceptsWhatItUsedToRefuse(t *testing.T) {
	cl, _, done := harnessWith(t, 20, arrowtype.Options{})
	defer done()

	for _, q := range []string{
		"SELECT COUNT(*) FROM " + dbrpc.DefaultTable,
		"SELECT Owner AS who FROM " + dbrpc.DefaultTable,
		"SELECT upper(Owner) FROM " + dbrpc.DefaultTable,
		"SELECT Owner, COUNT(*) FROM " + dbrpc.DefaultTable + " GROUP BY Owner HAVING COUNT(*) > 1",
		"SELECT DISTINCT Owner FROM " + dbrpc.DefaultTable,
		"SELECT Owner FROM " + dbrpc.DefaultTable + " ORDER BY Owner DESC",
		"SELECT JobStatus, COUNT(*) FROM " + dbrpc.DefaultTable + " WHERE ClusterId > 1005 GROUP BY JobStatus",
	} {
		recs, _ := readAll(t, cl, q)
		if totalRows(recs) == 0 {
			t.Errorf("%s: served, but returned no rows", q)
		}
		for _, r := range recs {
			r.Release()
		}
	}

	// An alias really is the column's name, not its source text: a client reads the
	// header it asked for.
	_, schema := readAll(t, cl, "SELECT Owner AS who FROM "+dbrpc.DefaultTable)
	if got := schema.Field(0).Name; got != "who" {
		t.Errorf("the aliased column is named %q, want who", got)
	}
}

// peerClient builds a Flight client authorized at the given levels against one store, so a
// test can ask the same question as two different principals. It is the setup the tail's
// privacy test does inline; the analytic path is a SECOND way into the store, and a privacy
// property that holds on one path and not the other is the kind that gets found in production.
func peerClient(t *testing.T, cat Catalog, levels ...string) *flightsql.Client {
	t.Helper()
	sessions, err := NewSessions(cat, fixedAuth{user: "alice"},
		allowLevels("alice", levels...), arrowtype.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sessions.Close)
	fsrv, err := NewSessionFlightServer(sessions, nil, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gsrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(gsrv, fsrv)
	go func() { _ = gsrv.Serve(ln) }()
	t.Cleanup(gsrv.Stop)
	cl, err := flightsql.NewClient(ln.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

// TestAnalyticCannotGroupByAPrivateAttribute: an aggregate is a disclosure channel of its own
// shape. Nobody has to see a value to learn it -- `SELECT ClaimId, COUNT(*) GROUP BY ClaimId`
// names every distinct value in the column as a group key, and `MIN`/`MAX` report two of them
// exactly. So the analytic path has to be closed to a private attribute the same way the scan
// is, and it is, at the same place: an unprivileged peer's type report does not carry the
// attribute, so there is no column to type the SELECT list against.
//
// The DAEMON arm is the premise. Without it a green READ arm would prove only that the fixture
// has no private attribute to withhold.
func TestAnalyticCannotGroupByAPrivateAttribute(t *testing.T) {
	cat, cleanup := store(t)
	defer cleanup()
	ctx := context.Background()

	queries := []string{
		"SELECT ClaimId, COUNT(*) FROM " + dbrpc.DefaultTable + " GROUP BY ClaimId",
		"SELECT MAX(ClaimId) FROM " + dbrpc.DefaultTable,
		// A count does not need its argument's TYPE, which is how it came to be
		// accepted without resolving the argument at all. It still reports the
		// cardinality of a column the reader may not name.
		"SELECT COUNT(DISTINCT ClaimId) FROM " + dbrpc.DefaultTable,
		"SELECT COUNT(ClaimId) FROM " + dbrpc.DefaultTable,
		// The column is named but never projected, so a check that walked only the
		// SELECT list would pass this.
		"SELECT COUNT(*) FROM " + dbrpc.DefaultTable + " GROUP BY ClaimId",
		"SELECT Owner FROM " + dbrpc.DefaultTable + " ORDER BY ClaimId",
		"SELECT Owner, COUNT(*) FROM " + dbrpc.DefaultTable + " GROUP BY Owner HAVING MAX(ClaimId) > 'a'",
	}

	daemon := peerClient(t, cat, "READ", "WRITE", "DAEMON")
	for _, q := range queries {
		if _, err := daemon.Execute(ctx, q); err != nil {
			t.Fatalf("%s: a DAEMON peer was refused (%v), so the READ arm below proves nothing", q, err)
		}
	}

	read := peerClient(t, cat, "READ")
	for _, q := range queries {
		_, err := read.Execute(ctx, q)
		if err == nil {
			t.Errorf("%s: a READ peer grouped on a private attribute", q)
			continue
		}
		// The refusal says the column is not there, which is the same thing it says
		// about a column that genuinely is not there. Saying "that one is private"
		// would answer the question the refusal exists to refuse.
		if !strings.Contains(err.Error(), "no such column") {
			t.Errorf("%s: refused with %v; want the same answer an absent column gets", q, err)
		}
		if strings.Contains(strings.ToLower(err.Error()), "private") {
			t.Errorf("%s: the refusal says the attribute is private, which confirms it exists: %v", q, err)
		}
	}
}
