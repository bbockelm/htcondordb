package flightsvc

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// TestPinnedHandleRoundTrip covers the encoding on its own, including the two cases that are
// not a round trip: a handle written before pins existed, which must still run, and a damaged
// one, which must not run as SQL.
func TestPinnedHandleRoundTrip(t *testing.T) {
	const q = "SELECT Owner FROM " + dbrpc.DefaultTable + " WHERE Owner = 'a\x00b'"
	fp, got := parsePinnedHandle(pinnedHandle("abc123", q))
	if fp != "abc123" || got != q {
		t.Errorf("round trip gave (%q, %q), want (abc123, %q)", fp, got, q)
	}

	// An unpinned handle is the query itself. A client may be holding one from before this
	// existed, and an embedder calling the scan directly passes no pin at all.
	if fp, got := parsePinnedHandle(q); fp != "" || got != q {
		t.Errorf("an unpinned handle gave (%q, %q), want (\"\", the query)", fp, got)
	}

	// Truncated after the marker: there is no query in it, and the remainder must NOT be
	// run as one. Returning it would execute text the client never wrote.
	if fp, got := parsePinnedHandle(pinPrefix + "deadbeef"); fp != "" || got != "" {
		t.Errorf("a truncated handle gave (%q, %q), want two empty strings", fp, got)
	}
}

// TestCheckPinAcceptsOnlyTheSchemaItNamed is the comparison itself: it passes when nothing was
// promised, passes on the schema that was, and refuses anything else with a code a client can
// branch on.
func TestCheckPinAcceptsOnlyTheSchemaItNamed(t *testing.T) {
	decisions := []arrowtype.Decision{arrowtype.TailDecision()}
	fp := arrowtype.Fingerprint(decisions)

	if err := checkPin("", decisions); err != nil {
		t.Errorf("an unpinned fetch was refused: %v", err)
	}
	if err := checkPin(fp, decisions); err != nil {
		t.Errorf("the promised schema was refused: %v", err)
	}
	err := checkPin("not-the-schema", decisions)
	if err == nil {
		t.Fatal("a fingerprint naming a different schema was accepted")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("code %v, want FailedPrecondition so a client can tell this from a bad query", got)
	}
	if !strings.Contains(err.Error(), "re-plan") {
		t.Errorf("the refusal does not say what to do about it: %v", err)
	}
}

// retype adds a record carrying an attribute the table did not have, which is how a table's
// derived schema changes under a client that is between its two RPCs. This is the real shape of
// the hazard rather than a contrived one: a pool starts setting a new job attribute, and every
// `SELECT *` planned before it gains a column at the fetch.
func retype(t *testing.T, d *db.DB) {
	t.Helper()
	ad, err := classad.ParseOld("Owner = \"newcomer\"\nClusterId = 99999\nFreshAttribute = 7\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put("retyped", ad); err != nil {
		t.Fatal(err)
	}
}

// TestSchemaChangeBetweenPlanAndFetchIsRefused is the whole point of the mechanism. A client
// plans a statement, the table grows an attribute, and the fetch must not quietly deliver rows
// under a shape the client was never told about -- a client reads an Arrow batch by position,
// so one extra column at the front does not move one value, it moves every value after it.
//
// The premise is asserted first: the same sequence WITHOUT the retype has to succeed, or this
// test would pass against a server that refuses every fetch.
func TestSchemaChangeBetweenPlanAndFetchIsRefused(t *testing.T) {
	ctx := context.Background()
	const stmt = "SELECT * FROM " + dbrpc.DefaultTable

	// Premise: plan, do not disturb the table, fetch. Rows.
	_, svc, _, done := harnessWithStore(t, 20, arrowtype.Options{})
	defer done()
	pl, err := svc.planStatement(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.doGetQuery(ctx, stmt, nil, pl.fp); err != nil {
		t.Fatalf("an undisturbed table refused its own promised schema: %v", err)
	}

	// The hazard: the same sequence with the table growing a column in between.
	_, svc2, d2, done2 := harnessWithStore(t, 20, arrowtype.Options{})
	defer done2()
	pl2, err := svc2.planStatement(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	retype(t, d2)
	after, err := svc2.planStatement(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	// Without this the test proves nothing: if the fixture's change did not reach the
	// derivation, the refusal below could never fire and a green run would mean nothing.
	if after.fp == pl2.fp {
		t.Fatal("the fixture did not change the derived schema, so there is no drift to detect")
	}
	if after.schema.NumFields() <= pl2.schema.NumFields() {
		t.Fatalf("expected a new column; fields went %d -> %d",
			pl2.schema.NumFields(), after.schema.NumFields())
	}
	_, _, err = svc2.doGetQuery(ctx, stmt, nil, pl2.fp)
	if err == nil {
		t.Fatal("a fetch delivered rows under a schema that had been superseded")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("code %v, want FailedPrecondition", got)
	}
}

// TestAForgedFingerprintRefusesItsOwnRequest is the security half of carrying a name rather
// than a schema. The ticket is in the client's hands, so it can put anything in it; what it
// must NOT be able to do is make the server produce a shape of the client's choosing. The only
// reachable outcome of an invented name is a refusal of the request that carried it.
func TestAForgedFingerprintRefusesItsOwnRequest(t *testing.T) {
	cl, _, done := harnessWith(t, 20, arrowtype.Options{})
	defer done()
	ctx := context.Background()

	ticket, err := flightsql.CreateStatementQueryTicket(
		[]byte(pinnedHandle("0000000000000000deadbeefdeadbeef", "SELECT Owner FROM "+dbrpc.DefaultTable)))
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := cl.DoGet(ctx, &flight.Ticket{Ticket: ticket})
	if err == nil {
		// Some drivers surface the error only once the stream is read.
		for rdr.Next() {
		}
		err = rdr.Err()
		rdr.Release()
	}
	if err == nil {
		t.Fatal("a ticket naming a schema this server never promised returned rows")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("code %v, want FailedPrecondition", got)
	}
}

// TestTheTicketAFlightInfoHandsOutIsAcceptedByItsOwnFetch is the end-to-end contract, over real
// gRPC with arrow-go's own client: whatever GetFlightInfo puts in the ticket, DoGet takes.
// Encoding the pin in the handle is exactly the kind of change that works in a unit test and
// breaks the wire, because the handle crosses a protobuf and a client that never looks inside.
func TestTheTicketAFlightInfoHandsOutIsAcceptedByItsOwnFetch(t *testing.T) {
	cl, _, done := harnessWith(t, 20, arrowtype.Options{})
	defer done()
	ctx := context.Background()

	for _, q := range []string{"SELECT * FROM " + dbrpc.DefaultTable, "SELECT Owner, ClusterId FROM " + dbrpc.DefaultTable + " LIMIT 5"} {
		info, err := cl.Execute(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			t.Fatalf("%s: the ticket its own GetFlightInfo produced was refused: %v", q, err)
		}
		rows := 0
		for rdr.Next() {
			rows += int(rdr.Record().NumRows())
		}
		if err := rdr.Err(); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		rdr.Release()
		if rows == 0 {
			t.Errorf("%s: no rows", q)
		}
	}
}

// TestAPreparedStatementRepinsWhenItReportsASchema covers the one place the pin is deliberately
// NOT fixed at the moment of promising.
//
// A prepared statement lives for half an hour and a schema derivation is a measurement of a
// moving table, so a pin fixed at prepare would start refusing a working statement as soon as
// the pool set a new attribute -- a worse failure than the one being prevented, and one the
// client cannot fix without noticing. So every call that REPORTS a schema re-pins, and the
// guarantee is the one that was wanted all along: what the client was last told is what it
// gets.
func TestAPreparedStatementRepinsWhenItReportsASchema(t *testing.T) {
	ctx := context.Background()
	const stmt = "SELECT * FROM " + dbrpc.DefaultTable

	// A fetch against a stale pin is refused...
	cl, _, d, done := harnessWithStore(t, 20, arrowtype.Options{})
	defer done()
	ps, err := cl.Prepare(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	retype(t, d)
	info, err := ps.Execute(ctx)
	if err == nil {
		var rdr *flight.Reader
		rdr, err = cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err == nil {
			for rdr.Next() {
			}
			err = rdr.Err()
			rdr.Release()
		}
	}
	// ...unless the Execute itself re-pinned, which is what GetFlightInfo does. arrow-go's
	// Execute calls GetFlightInfo, so this path SHOULD succeed, and that is the behaviour
	// under test: a client that asks again gets the new shape rather than a refusal.
	if err != nil {
		t.Fatalf("a prepared statement refused a fetch it had just re-promised a schema for: %v", err)
	}

	// And the statement is still usable afterwards, with the new column in it.
	res, err := ps.GetSchema(ctx)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := flight.DeserializeSchema(res.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range schema.Fields() {
		if f.Name == "FreshAttribute" {
			found = true
		}
	}
	if !found {
		t.Error("the re-derived schema does not carry the attribute the table gained, so the fixture proved nothing")
	}
	if err := ps.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestAPreparedFetchWithoutARepinIsRefused is the other half: the re-pin above is what makes a
// drifted statement work, so the pin must genuinely be enforced when nothing re-promised. This
// drives the server directly, because the point is the handle's stored pin rather than any
// sequence a client can issue.
func TestAPreparedFetchWithoutARepinIsRefused(t *testing.T) {
	ctx := context.Background()
	const stmt = "SELECT * FROM " + dbrpc.DefaultTable
	_, svc, d, done := harnessWithStore(t, 20, arrowtype.Options{})
	defer done()

	pl, err := svc.planStatement(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := svc.prep.add(stmt, "fixed-client", pl.fp)
	if err != nil {
		t.Fatal(err)
	}
	retype(t, d)
	st, err := svc.prep.get(handle, "fixed-client")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.doGetQuery(ctx, st.query, &args{}, st.fp); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a fetch on a stale pin gave %v, want FailedPrecondition", err)
	}

	// Re-pinning is what a GetSchema or a GetFlightInfo on this handle does, and after it
	// the same fetch runs.
	after, err := svc.planStatement(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	svc.prep.repin(handle, "fixed-client", after.fp)
	st, err = svc.prep.get(handle, "fixed-client")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.doGetQuery(ctx, st.query, &args{}, st.fp); err != nil {
		t.Fatalf("a re-pinned statement was still refused: %v", err)
	}
}

// TestTheTicketCarriesThePinOverTheWire is the end-to-end version of the refusal, and it is a
// separate test because the unit one drives doGetQuery directly and so cannot see the ticket at
// all. If GetFlightInfo stopped putting the fingerprint in the handle -- or the handle stopped
// surviving the protobuf, which is the kind of thing that works in process and breaks on the
// wire -- every unit test here would still pass and the guarantee would be gone.
func TestTheTicketCarriesThePinOverTheWire(t *testing.T) {
	ctx := context.Background()
	cl, _, d, done := harnessWithStore(t, 20, arrowtype.Options{})
	defer done()

	info, err := cl.Execute(ctx, "SELECT * FROM "+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	retype(t, d)
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err == nil {
		for rdr.Next() {
		}
		err = rdr.Err()
		rdr.Release()
	}
	if err == nil {
		t.Fatal("rows were delivered over gRPC under a schema the table no longer has")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("code %v, want FailedPrecondition; error was %v", got, err)
	}
}
