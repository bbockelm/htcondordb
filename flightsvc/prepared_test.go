package flightsvc

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestPreparedStatementRoundTrip is the path an ADBC client takes when it prepares: the
// statement is validated and its schema known before any row is fetched, which is most of what
// preparing is for.
func TestPreparedStatementRoundTrip(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner, ClusterId FROM `+dbrpc.DefaultTable+` WHERE Owner = 'user1'`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close(ctx)

	// The schema is known from preparing alone.
	sch := stmt.DatasetSchema()
	if sch == nil || sch.NumFields() != 2 {
		t.Fatalf("dataset schema %v, want 2 fields", sch)
	}
	if sch.Field(0).Name != "Owner" || sch.Field(1).Name != "ClusterId" {
		t.Errorf("schema fields %q,%q, want Owner,ClusterId", sch.Field(0).Name, sch.Field(1).Name)
	}
	// It takes no parameters, and the schema says so rather than leaving a client to infer
	// it from an absent field.
	if ps := stmt.ParameterSchema(); ps == nil {
		t.Error("parameter schema is nil; an empty one states that the statement takes none")
	} else if ps.NumFields() != 0 {
		t.Errorf("parameter schema has %d fields, want 0", ps.NumFields())
	}

	info, err := stmt.Execute(ctx)
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
		if rec.NumCols() != 2 {
			t.Fatalf("a batch has %d columns, want 2", rec.NumCols())
		}
		rows += int(rec.NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	// The seed writes Owner = user0..user3 over 40 rows, so a quarter match -- and the
	// WHERE clause has to survive the prepare, not just the parse.
	if rows != 10 {
		t.Errorf("read %d rows, want 10", rows)
	}
}

// TestPreparedStatementValidatesAtPrepareTime: the point of preparing is learning early. A
// statement this endpoint will not serve must fail at Prepare rather than at the first fetch.
func TestPreparedStatementValidatesAtPrepareTime(t *testing.T) {
	cl, _, done := harness(t, 10)
	defer done()
	ctx := context.Background()

	for _, q := range []string{
		`SELECT COUNT(*) FROM ` + dbrpc.DefaultTable,
		`SELECT * FROM ` + dbrpc.DefaultTable + ` WHERE Owner > 'R'`,
		`SELECT NoSuchColumn FROM ` + dbrpc.DefaultTable,
		`SELECT * FROM nosuchtable`,
	} {
		stmt, err := cl.Prepare(ctx, q)
		if err == nil {
			stmt.Close(ctx)
			t.Errorf("%q: prepared, want a refusal at prepare time", q)
		}
	}
}

// TestPreparedAndDirectAgree: a prepared statement and the same SQL run directly must return
// the same rows. They share one scan implementation precisely so they cannot drift, and a
// prepared statement quietly returning different rows is the kind of bug nobody reports
// because nobody compares.
func TestPreparedAndDirectAgree(t *testing.T) {
	cl, _, done := harness(t, 50)
	defer done()
	ctx := context.Background()
	q := `SELECT Owner FROM ` + dbrpc.DefaultTable + ` WHERE ClusterId >= 1025`

	direct, err := cl.Execute(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	directRows := drain(t, cl, direct.Endpoint[0].Ticket.Ticket)

	stmt, err := cl.Prepare(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	prep, err := stmt.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prepRows := drain(t, cl, prep.Endpoint[0].Ticket.Ticket)

	if directRows != prepRows {
		t.Errorf("direct returned %d rows, prepared returned %d", directRows, prepRows)
	}
	if directRows == 0 {
		t.Error("both returned nothing; the comparison proves nothing")
	}
}

// TestPreparedHandleIsBoundToItsOwner: handles are random so they cannot be enumerated, and
// checked against the caller so they cannot be borrowed. An unknown handle and somebody else's
// report the same error on purpose -- distinguishing them would make the handle space an oracle
// for which handles exist.
func TestPreparedHandleIsBoundToItsOwner(t *testing.T) {
	cat, cleanup := store(t)
	defer cleanup()

	alice, err := NewSessions(cat, fixedAuth{user: "alice"}, allowLevels("alice", "READ"), deriveNone())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewSessionServer(alice, deriveNone(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := srv.CreatePreparedStatement(ctx, fakeCreate{q: `SELECT * FROM ` + dbrpc.DefaultTable})
	if err != nil {
		t.Fatalf("alice could not prepare: %v", err)
	}
	// Alice can use her own handle.
	if _, err := srv.preparedQuery(ctx, fakeHandle{h: res.Handle}); err != nil {
		t.Errorf("the owner could not use her own handle: %v", err)
	}

	// Bob, on a server whose identity is bob, must not -- same table, same handle bytes.
	bobSessions, err := NewSessions(cat, fixedAuth{user: "bob"}, allowLevels("bob", "READ"), deriveNone())
	if err != nil {
		t.Fatal(err)
	}
	bobSrv, err := NewSessionServer(bobSessions, deriveNone(), nil)
	if err != nil {
		t.Fatal(err)
	}
	bobSrv.prep = srv.prep // share the table, so only ownership can refuse him
	if _, err := bobSrv.preparedQuery(ctx, fakeHandle{h: res.Handle}); err == nil {
		t.Error("another identity used a handle it did not create")
	} else if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code %v, want NotFound -- the same answer an unknown handle gets, so the "+
			"handle space is not an oracle", got)
	}

	// An unknown handle reports identically.
	if _, err := srv.preparedQuery(ctx, fakeHandle{h: []byte("0123456789abcdef")}); err == nil {
		t.Error("an unknown handle was accepted")
	} else if got := status.Code(err); got != codes.NotFound {
		t.Errorf("unknown handle code %v, want NotFound", got)
	}
}

// TestPreparedTableIsBounded: a client that never closes its statements would otherwise grow
// the daemon's memory without limit, which is the same failure the stream cap exists to
// prevent and gets the same answer -- refuse past the cap rather than accumulate.
func TestPreparedTableIsBounded(t *testing.T) {
	p := newPrepared()
	p.max = 3
	for i := 0; i < 3; i++ {
		if _, err := p.add("SELECT * FROM t", "alice"); err != nil {
			t.Fatalf("statement %d refused below the cap: %v", i, err)
		}
	}
	_, err := p.add("SELECT * FROM t", "alice")
	if err == nil {
		t.Fatal("a statement past the cap was accepted")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("code %v, want ResourceExhausted", got)
	}

	// Closing one makes room: a cap on live statements, not a permanent ceiling.
	for k := range p.byKey {
		if err := p.remove([]byte(k), "alice"); err != nil {
			t.Fatalf("removing a statement: %v", err)
		}
		break
	}
	if _, err := p.add("SELECT * FROM t", "alice"); err != nil {
		t.Errorf("no room after closing one: %v", err)
	}
}

// TestPreparedEntriesExpire: a disconnected client cannot close anything, so without expiry its
// handles are leaked for the daemon's lifetime.
func TestPreparedEntriesExpire(t *testing.T) {
	p := newPrepared()
	p.ttl = time.Millisecond
	h, err := p.add("SELECT * FROM t", "alice")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := p.get(h, "alice"); err == nil {
		t.Error("a statement past its TTL was still served")
	}
	if n := p.len(); n != 0 {
		t.Errorf("%d entries left after expiry, want 0", n)
	}
}

// TestBindingParametersIsRefused: accepting a bind and ignoring the values would run the
// unparameterised query and return rows that look like an answer.
func TestBindingParametersIsRefused(t *testing.T) {
	_, svc, done := harness(t, 10)
	defer done()
	_, err := svc.DoPutPreparedStatementQuery(context.Background(), fakeHandle{h: []byte("x")}, nil, nil)
	if err == nil {
		t.Fatal("a parameter bind was accepted")
	}
	if got := status.Code(err); got != codes.Unimplemented {
		t.Errorf("code %v, want Unimplemented", got)
	}
}

// --- small fakes for the server-side calls, which the flightsql client cannot reach directly ---

type fakeCreate struct{ q string }

func (f fakeCreate) GetQuery() string         { return f.q }
func (f fakeCreate) GetTransactionId() []byte { return nil }

type fakeHandle struct{ h []byte }

func (f fakeHandle) GetPreparedStatementHandle() []byte { return f.h }

// deriveNone is the empty derivation: no epoch declarations and no overrides, which is what
// these tests want -- they are about handles, not types.
func deriveNone() arrowtype.Options { return arrowtype.Options{} }

// drain reads a ticket to exhaustion and returns the row count.
func drain(t *testing.T, cl *flightsql.Client, ticket []byte) int {
	t.Helper()
	rdr, err := cl.DoGet(context.Background(), &flight.Ticket{Ticket: ticket})
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	defer rdr.Release()
	n := 0
	for rdr.Next() {
		n += int(rdr.Record().NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reading: %v", err)
	}
	return n
}

// TestPreparedHandlesAreUnguessable: ownership is one defence and unguessability is the other.
// Sequential or derived handles would let an identity enumerate the space and discover that
// somebody else's statement exists, which the matching NotFound errors exist to prevent.
func TestPreparedHandlesAreUnguessable(t *testing.T) {
	p := newPrepared()
	const n = 64
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		// The SAME query each time: a handle derived from its content would collide.
		h, err := p.add("SELECT * FROM t", "alice")
		if err != nil {
			t.Fatal(err)
		}
		if len(h) < 32 {
			t.Fatalf("handle is %d bytes; too short to resist guessing", len(h))
		}
		if seen[string(h)] {
			t.Fatalf("handle %q issued twice", h)
		}
		seen[string(h)] = true
	}
	// Not sequential: consecutive handles must not differ in only their last character,
	// which is what a counter would produce.
	var prev string
	adjacent := 0
	for h := range seen {
		if prev != "" && len(h) == len(prev) && h[:len(h)-1] == prev[:len(prev)-1] {
			adjacent++
		}
		prev = h
	}
	if adjacent > 0 {
		t.Errorf("%d handles differ only in their last character; they look generated rather than random", adjacent)
	}
}
