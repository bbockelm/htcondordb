package flightsvc

import (
	"context"
	"net"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// adbcGetInfoIDs are the SqlInfo entries a real ADBC driver requests when a caller invokes
// adbc_get_info(). An unregistered entry comes back as NOT_FOUND rather than as an empty value,
// which fails the WHOLE call -- so one missing entry makes the endpoint look broken to every
// ADBC client, whatever else works.
//
// This list is not a guess: adbc_driver_flightsql 1.13.0 failed against this endpoint with
// "no information for sql info number 1" until FLIGHT_SQL_SERVER_VERSION was registered.
var adbcGetInfoIDs = []flightsql.SqlInfo{
	flightsql.SqlInfoFlightSqlServerName,
	flightsql.SqlInfoFlightSqlServerVersion,
	flightsql.SqlInfoFlightSqlServerArrowVersion,
}

// TestSqlInfoAnswersWhatADBCAsks is the regression for a bug no other test in this package
// could reach: every test here drives arrow-go's own flightsql.Client, which never calls
// GetSqlInfo, so a missing entry was invisible until a real driver connected.
func TestSqlInfoAnswersWhatADBCAsks(t *testing.T) {
	cl, _, done := harness(t, 5)
	defer done()
	ctx := context.Background()

	info, err := cl.GetSqlInfo(ctx, adbcGetInfoIDs)
	if err != nil {
		t.Fatalf("GetSqlInfo: %v", err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	defer rdr.Release()

	got := map[uint32]bool{}
	for rdr.Next() {
		rec := rdr.Record()
		ids, ok := rec.Column(0).(*array.Uint32)
		if !ok {
			t.Fatalf("the info_name column is a %T, want uint32", rec.Column(0))
		}
		for i := 0; i < ids.Len(); i++ {
			got[ids.Value(i)] = true
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range adbcGetInfoIDs {
		if !got[uint32(id)] {
			t.Errorf("sql info %v (%d) is not answered; an ADBC client's adbc_get_info() "+
				"fails the whole call on one missing entry", id, uint32(id))
		}
	}
}

// TestSetVersionIsReported: the daemon passes its build stamp, and a client that asks must get
// that rather than the placeholder -- otherwise every deployment reports the same version and
// nobody can tell them apart from the client side.
func TestSetVersionIsReported(t *testing.T) {
	cl, svc, done := harness(t, 5)
	defer done()
	ctx := context.Background()

	const want = "v9.9.9-test"
	if err := svc.SetVersion(want); err != nil {
		t.Fatal(err)
	}
	if got := sqlInfoString(t, cl, ctx, flightsql.SqlInfoFlightSqlServerVersion); got != want {
		t.Errorf("server version = %q, want %q", got, want)
	}

	// An empty version is a no-op, not an erasure: a host that does not know its own
	// version should leave the placeholder rather than register an empty string, which a
	// client would display as blank.
	if err := svc.SetVersion(""); err != nil {
		t.Fatal(err)
	}
	if got := sqlInfoString(t, cl, ctx, flightsql.SqlInfoFlightSqlServerVersion); got != want {
		t.Errorf("after SetVersion(\"\") the version is %q, want it unchanged at %q", got, want)
	}
}

// sqlInfoString reads one string-valued SqlInfo entry.
func sqlInfoString(t *testing.T, cl *flightsql.Client, ctx context.Context, id flightsql.SqlInfo) string {
	t.Helper()
	info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{id})
	if err != nil {
		t.Fatalf("GetSqlInfo(%v): %v", id, err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		if rec.NumRows() == 0 {
			continue
		}
		// The value column is a dense union; the string variant is child 0.
		u, ok := rec.Column(1).(*array.DenseUnion)
		if !ok {
			t.Fatalf("the value column is a %T, want a dense union", rec.Column(1))
		}
		strs, ok := u.Field(0).(*array.String)
		if !ok {
			t.Fatalf("union child 0 is a %T, want string", u.Field(0))
		}
		return strs.Value(int(u.ValueOffset(0)))
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("no value came back for %v", id)
	return ""
}

// TestSettingsVersionReachesTheServer drives the path the DAEMON uses:
// NewSessionFlightServer with a Settings carrying the build stamp. That is a different thing
// from SetVersion working -- a Settings field that lands in the struct and is never applied is a
// silent no-op, the version silently reports the placeholder, and every deployment looks
// identical from the client side. A mutation that dropped the SetVersion call inside
// NewSessionFlightServer went undetected until this existed.
func TestSettingsVersionReachesTheServer(t *testing.T) {
	cat, cleanup := store(t)
	defer cleanup()

	sessions, err := NewSessions(cat, fixedAuth{user: "alice"}, allowLevels("alice", "READ"), deriveNone())
	if err != nil {
		t.Fatal(err)
	}
	defer sessions.Close()

	const want = "v1.2.3-from-settings"
	fsrv, err := NewSessionFlightServer(sessions, nil, Settings{Version: want})
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
	defer gsrv.Stop()

	cl, err := flightsql.NewClient(ln.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	if got := sqlInfoString(t, cl, context.Background(), flightsql.SqlInfoFlightSqlServerVersion); got != want {
		t.Errorf("the version a client sees is %q, want %q -- Settings.Version did not reach "+
			"the server", got, want)
	}
}
