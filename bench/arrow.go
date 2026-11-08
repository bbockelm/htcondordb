package bench

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The Arrow Flight SQL arm.
//
// It is the EVALUATED form over a different transport: typed columns, column-major batches, the
// shape a consumer with a schema receives. Measuring it beside the ClassAd transports is the
// point of the tool -- it is the arm most consumers of a history table will actually use, and the
// one whose cost is least like the others'.
//
// THE QUERY LANGUAGES DIFFER, AND THE TOOL WILL NOT PAPER OVER IT. The other arms take a ClassAd
// constraint; this one takes SQL. `JobStatus == 3` is not SQL and `JobStatus = 3` is not a
// ClassAd constraint, and silently translating between them is how a sweep ends up comparing two
// different questions and reporting the difference as a transport's. So:
//
//   - An empty or trivially-true constraint needs no WHERE, and the arm runs.
//   - Otherwise the caller supplies Spec.SQLWhere, asserting the two express the same filter.
//   - With neither, the arm DECLINES with a note saying why, rather than running an unfiltered
//     query beside the others' filtered one and looking fast.
//
// Bytes come from the socket, as for every other transport: a column-major batch has no per-row
// length, and the gRPC framing is part of what a consumer receives.

// Arrow is the Flight SQL transport.
const Arrow Transport = "arrow/flight-sql"

// DialFlight opens a Flight SQL client with its socket bytes counted.
func DialFlight(addr string) (*Conn, error) {
	var bytesFn func() int64
	dialer := func(ctx context.Context, a string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", a)
		if err != nil {
			return nil, err
		}
		counted, fn := NewCountingConn(c)
		bytesFn = fn
		return counted, nil
	}
	fs, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer))
	if err != nil {
		return nil, fmt.Errorf("dialing flight sql at %s: %w", addr, err)
	}
	return &Conn{
		Flight: fs,
		Close:  func() { _ = fs.Close() },
		// Read through a closure because the dialer runs lazily, on the first RPC: capturing
		// bytesFn directly here would capture a nil.
		Bytes: func() int64 {
			if bytesFn == nil {
				return 0
			}
			return bytesFn()
		},
	}, nil
}

// arrowQuery builds the SELECT for a spec, or explains why it cannot.
func arrowQuery(spec Spec, limit int) (string, error) {
	cols := "*"
	if len(spec.Projection) > 0 {
		cols = strings.Join(spec.Projection, ", ")
	}
	q := fmt.Sprintf("SELECT %s FROM %s", cols, spec.Table)
	switch {
	case spec.SQLWhere != "":
		q += " WHERE " + spec.SQLWhere
	case trivialConstraint(spec.Constraint):
		// No filter either way: the two arms ask the same question.
	default:
		return "", fmt.Errorf("the other arms filter with the ClassAd constraint %q, which is not SQL; "+
			"set Spec.SQLWhere to the equivalent so this arm asks the same question, or the comparison "+
			"would be between an unfiltered Arrow read and a filtered one", spec.Constraint)
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return q, nil
}

func trivialConstraint(c string) bool {
	c = strings.TrimSpace(strings.ToLower(c))
	return c == "" || c == "true"
}

// readArrow runs one Flight SQL query and reports the rows received.
func readArrow(ctx context.Context, cn *Conn, spec Spec, limit int) (rows int64, err error) {
	if cn.Flight == nil {
		return 0, fmt.Errorf("no Flight SQL endpoint for this connection")
	}
	q, err := arrowQuery(spec, limit)
	if err != nil {
		return 0, err
	}
	info, err := cn.Flight.Execute(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("executing %q: %w", q, err)
	}
	if len(info.Endpoint) == 0 {
		return 0, fmt.Errorf("query %q returned no endpoint", q)
	}
	rdr, err := cn.Flight.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return 0, fmt.Errorf("fetching %q: %w", q, err)
	}
	defer rdr.Release()
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	if rerr := rdr.Err(); rerr != nil {
		return rows, rerr
	}
	return rows, nil
}
