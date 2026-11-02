package flightsvc

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
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

// A harness with an ARCHIVE in it, and a record of what the store was actually asked.
//
// The plain harness holds one mutable table, which is why it could not see that every read
// of an archive failed: the endpoint listed `history`, derived its schema, and then errored
// on the rows, and no test went that far. A fixture that cannot distinguish the two kinds of
// table cannot test the one place they differ -- the same mistake as a test that creates a
// PLAIN table named "history" and concludes history works.
//
// The query log is here for the other half: whether the PROJECTION reached the store is a
// decision with no visible effect on the rows, since the schema projects them either way. The
// log is where the store says what it was asked for, so the test asserts on that rather than
// on something that would pass regardless.
type catHarness struct {
	cl  *flightsql.Client
	svc *Server

	mu   sync.Mutex
	logs []dbrpc.QueryLog
}

// queries returns the logged reads for one table, newest last.
func (h *catHarness) queries(table string) []dbrpc.QueryLog {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []dbrpc.QueryLog
	for _, q := range h.logs {
		if q.Table == table {
			out = append(out, q)
		}
	}
	return out
}

func (h *catHarness) lastScan(t *testing.T, table string) dbrpc.QueryLog {
	t.Helper()
	// AttrTypeProfiles is not a streamed query and so is not logged; what is logged for a
	// table is the scan, which is what these tests are about.
	qs := h.queries(table)
	if len(qs) == 0 {
		t.Fatalf("no query logged for %q", table)
	}
	return qs[len(qs)-1]
}

func catalogHarness(t *testing.T) (*catHarness, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	// MemoryUsage is an expression in a MINORITY of ads, so the derivation still types the
	// column as an integer (an expression-dominant attribute is ruled untyped and gets no
	// column at all, which would test nothing). The expression is written over RawMem, a
	// sibling nobody selects -- so a projection that does not chase references loses it and
	// the column comes back null where it should hold 100.
	for i := 0; i < 10; i++ {
		src := fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nRawMem = 50\n", i%2, 1000+i)
		if i < 2 {
			src += "MemoryUsage = RawMem * 2\n"
		} else {
			src += "MemoryUsage = 100\n"
		}
		ad, perr := classad.ParseOld(src)
		if perr != nil {
			t.Fatal(perr)
		}
		if err := jobs.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}

	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId"},
		ZoneAttrs:  []string{"CompletionDate"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		ad, perr := classad.Parse(fmt.Sprintf(
			`[ ClusterId = %d; Owner = "user%d"; JobStatus = 4; CompletionDate = %d ]`,
			2000+i, i%2, 1700000000+i))
		if perr != nil {
			t.Fatal(perr)
		}
		if aerr := arch.Append(ad); aerr != nil {
			t.Fatal(aerr)
		}
	}

	h := &catHarness{}
	dbSrv := dbrpc.NewServerCatalog(cat)
	cc, sc := net.Pipe()
	go func() {
		_ = dbSrv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{
			Privileged:     true,
			IncludePrivate: true,
			QueryLog: func(q dbrpc.QueryLog) {
				h.mu.Lock()
				h.logs = append(h.logs, q)
				h.mu.Unlock()
			},
		})
	}()
	dbc := dbrpc.NewClient(dbrpc.NewStreamConn(cc))

	svc, err := NewServer(dbc, arrowtype.Options{}, nil)
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

	cl, err := flightsql.NewClient(ln.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	h.cl, h.svc = cl, svc
	return h, func() { cl.Close(); gsrv.Stop(); dbc.Close(); dbSrv.Close(); cat.Close() }
}

// rows runs a query to completion and returns the records, so a test asserts on values
// rather than on the absence of an error.
func (h *catHarness) rows(t *testing.T, q string) []arrow.Record {
	t.Helper()
	ctx := context.Background()
	info, err := h.cl.Execute(ctx, q)
	if err != nil {
		t.Fatalf("Execute(%q): %v", q, err)
	}
	rdr, err := h.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("DoGet(%q): %v", q, err)
	}
	defer rdr.Release()
	var out []arrow.Record
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		out = append(out, rec)
	}
	if rerr := rdr.Err(); rerr != nil {
		t.Fatalf("read(%q): %v", q, rerr)
	}
	return out
}

func countRows(recs []arrow.Record) int {
	n := 0
	for _, r := range recs {
		n += int(r.NumRows())
	}
	return n
}

func stringsOf(t *testing.T, recs []arrow.Record, field string) []string {
	t.Helper()
	var out []string
	for _, r := range recs {
		idx := r.Schema().FieldIndices(field)
		if len(idx) == 0 {
			t.Fatalf("no field %q in %v", field, r.Schema().Fields())
		}
		col, ok := r.Column(idx[0]).(*array.String)
		if !ok {
			t.Fatalf("field %q is %T, not a string column", field, r.Column(idx[0]))
		}
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out = append(out, "<null>")
				continue
			}
			out = append(out, col.Value(i))
		}
	}
	sort.Strings(out)
	return out
}

func int64sOf(t *testing.T, recs []arrow.Record, field string) []int64 {
	t.Helper()
	var out []int64
	for _, r := range recs {
		idx := r.Schema().FieldIndices(field)
		if len(idx) == 0 {
			t.Fatalf("no field %q in %v", field, r.Schema().Fields())
		}
		col, ok := r.Column(idx[0]).(*array.Int64)
		if !ok {
			t.Fatalf("field %q is %T, not an int64 column", field, r.Column(idx[0]))
		}
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				t.Errorf("%s row %d is NULL", field, i)
				continue
			}
			out = append(out, col.Value(i))
		}
	}
	return out
}

func release(recs []arrow.Record) {
	for _, r := range recs {
		r.Release()
	}
}

// TestSelectFromArchiveTable is the regression for an endpoint that advertised a table it
// could not read. The catalog lists `history`, the schema derives, and before this the rows
// failed with "Query is not served from an archive".
func TestSelectFromArchiveTable(t *testing.T) {
	h, cleanup := catalogHarness(t)
	defer cleanup()

	recs := h.rows(t, "SELECT * FROM history")
	defer release(recs)
	if got := countRows(recs); got != 4 {
		t.Fatalf("SELECT * FROM history: %d rows, want 4", got)
	}
	if got := stringsOf(t, recs, "Owner"); len(got) != 4 || got[0] != "user0" || got[3] != "user1" {
		t.Errorf("owners = %v", got)
	}
}

// TestArchiveIsListedAndReadable pins the two halves together. A client that can see a table
// in the catalog and get its schema must be able to read it; the failure this guards against
// was precisely the three disagreeing.
func TestArchiveIsListedAndReadable(t *testing.T) {
	h, cleanup := catalogHarness(t)
	defer cleanup()
	ctx := context.Background()

	info, err := h.cl.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true})
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := h.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for rdr.Next() {
		rec := rdr.Record()
		names := rec.Column(2).(*array.String)
		for i := 0; i < names.Len(); i++ {
			listed[names.Value(i)] = true
		}
	}
	rdr.Release()
	if !listed["history"] {
		t.Fatalf("history not listed; listed = %v", listed)
	}

	if _, err := h.cl.GetExecuteSchema(ctx, "SELECT * FROM history"); err != nil {
		t.Fatalf("schema for a listed table: %v", err)
	}
	recs := h.rows(t, "SELECT ClusterId FROM history")
	defer release(recs)
	if got := countRows(recs); got != 4 {
		t.Errorf("SELECT ClusterId FROM history: %d rows, want 4", got)
	}
}

// TestArchivePredicateAndLimit checks the archive path carries the constraint and the row cap,
// not just the rows -- a scan that ignored either would still look like a working archive read.
func TestArchivePredicateAndLimit(t *testing.T) {
	h, cleanup := catalogHarness(t)
	defer cleanup()

	recs := h.rows(t, `SELECT ClusterId FROM history WHERE Owner = 'user0'`)
	defer release(recs)
	if got := countRows(recs); got != 2 {
		t.Errorf("WHERE Owner = 'user0': %d rows, want 2", got)
	}

	capped := h.rows(t, "SELECT ClusterId FROM history LIMIT 1")
	defer release(capped)
	if got := countRows(capped); got != 1 {
		t.Errorf("LIMIT 1: %d rows, want 1", got)
	}
	if q := h.lastScan(t, "history"); q.Limit != 1 {
		t.Errorf("the store was asked for Limit %d, want 1 -- the cap was applied client-side", q.Limit)
	}
}

// TestProjectionReachesTheStore asserts the pushdown itself. The rows look identical whether
// or not the projection crossed the wire -- the schema narrows them either way -- so the
// assertion is on what the store was asked for.
func TestProjectionReachesTheStore(t *testing.T) {
	h, cleanup := catalogHarness(t)
	defer cleanup()

	recs := h.rows(t, "SELECT Owner, ClusterId FROM jobs")
	release(recs)
	q := h.lastScan(t, "jobs")
	if q.Op != "QueryRawProjectRefs" {
		t.Errorf("scan op = %q, want QueryRawProjectRefs", q.Op)
	}
	if q.Projection != 2 {
		t.Errorf("projection = %d attributes, want 2; 0 means every attribute of every ad crossed the wire", q.Projection)
	}

	// SELECT * asks for no projection, which is the honest request: the tail is not a
	// fixed list, so narrowing it would be guessing.
	star := h.rows(t, "SELECT * FROM jobs")
	release(star)
	if q := h.lastScan(t, "jobs"); q.Projection != 0 {
		t.Errorf("SELECT *: projection = %d, want 0", q.Projection)
	}
}

// TestProjectedExpressionStillEvaluates is why the REFS-chasing op and not the plain one.
//
// MemoryUsage is written over RawMem in two of the ten ads. Nobody selects RawMem, so a
// projection that returns exactly what was asked for strips it, and the expression then
// evaluates to undefined -- a null in what should be a uniform integer column. Correct for a
// relay forwarding text; wrong for an encoder that evaluates.
func TestProjectedExpressionStillEvaluates(t *testing.T) {
	h, cleanup := catalogHarness(t)
	defer cleanup()

	recs := h.rows(t, "SELECT MemoryUsage FROM jobs")
	defer release(recs)
	got := int64sOf(t, recs, "MemoryUsage")
	if len(got) != 10 {
		t.Fatalf("%d non-null values, want 10", len(got))
	}
	for i, v := range got {
		if v != 100 {
			t.Errorf("row %d: MemoryUsage = %d, want 100", i, v)
		}
	}
}
