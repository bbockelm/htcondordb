package flightsvc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// The statement surface: one shape, and a refusal for everything else.
//
// `SELECT * FROM <table>` is the whole accepted grammar today. It is chosen because it is the
// only statement that means the same thing in every SQL dialect -- it contains no expression,
// so there is nothing for the ClassAd/ANSI divergence to bite. Anything with a WHERE, a
// projection, a join or an aggregate is refused until the ANSI front end can lower it
// faithfully.
//
// REFUSE, NEVER APPROXIMATE. Nothing downstream corrects this server: a query we
// half-understand returns rows that look right and are not. A refusal is a bug report the user
// can act on; a wrong answer is one nobody files. The error names the shape that IS accepted,
// so a refusal teaches rather than just blocks.

// selectAllRe matches `SELECT * FROM <table>`, with an optional trailing semicolon and
// optional double-quoted identifier. Deliberately a recogniser and not a parser: it accepts a
// shape or refuses, with no partial understanding in between. A real parser arrives with the
// ANSI front end, and until then pattern-matching one statement is honest about how little is
// supported.
// Go's regexp is RE2 and has no backreferences, so the quoted and unquoted forms are spelled
// as an alternation rather than as a captured quote matched again.
var selectAllRe = regexp.MustCompile(`(?is)^\s*select\s+\*\s+from\s+(?:"([A-Za-z_][A-Za-z0-9_./-]*)"|([A-Za-z_][A-Za-z0-9_./-]*))\s*;?\s*$`)

// parseSelectAll returns the table a statement reads, or an error naming what is supported.
func parseSelectAll(query string) (string, error) {
	m := selectAllRe.FindStringSubmatch(query)
	if m == nil {
		return "", fmt.Errorf(
			"unsupported SQL: this endpoint accepts `SELECT * FROM <table>` and nothing else yet "+
				"(got %q). Filters, projections, aggregates and joins are refused rather than "+
				"approximated, because a partly-understood query returns rows that look right",
			strings.TrimSpace(query))
	}
	if m[1] != "" {
		return m[1], nil // the double-quoted form
	}
	return m[2], nil
}

// tableSchema derives a table's Arrow schema from a measurement of what its attributes hold.
// It returns the decisions too, so a caller can report which attributes got no column.
func (s *Server) tableSchema(ctx context.Context, table string) (*arrow.Schema, []arrowtype.Decision, error) {
	db, release, err := s.dbFor(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer release()
	return s.tableSchemaWith(ctx, db, table)
}

// tableSchemaWith is tableSchema on a client the caller already holds, so a handler that makes
// several calls on one session does not open a session per call.
func (s *Server) tableSchemaWith(ctx context.Context, db *dbrpc.Client, table string) (*arrow.Schema, []arrowtype.Decision, error) {
	max := s.sampleMax()
	rep, err := db.AttrTypeProfiles(ctx, table, max)
	if err != nil {
		return nil, nil, err
	}
	// A sample that came back short of its cap saw the whole table, which is the only
	// sound basis for declaring a column non-nullable.
	complete := rep.Sampled < max
	ms := arrowtype.FromProfilesComplete(rep.Profiles, rep.Sampled, complete)
	schema, decisions := arrowtype.Schema(ms, s.derive)
	return schema, decisions, nil
}

// defaultSampleMax is how many records a schema derivation looks at. Matches arrowstream's
// default: the schema a client sees through the catalog and the schema it gets from a query
// have to agree, and they will not if the two paths sample differently.
const defaultSampleMax = 10000

// sampleMax is the server's effective cap. It is a field rather than a constant so a test can
// make the CAPPED case reachable: with the default, any fixture small enough to run quickly is
// also small enough to be sampled completely, so the difference between "measured everything"
// and "measured a prefix" never arises and the soundness rule that depends on it goes
// unexercised.
func (s *Server) sampleMax() int {
	if s.sample > 0 {
		return s.sample
	}
	return defaultSampleMax
}

func (s *Server) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	table, err := parseSelectAll(cmd.GetQuery())
	if err != nil {
		return nil, err
	}
	schema, _, err := s.tableSchema(ctx, table)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.alloc)}, nil
}

// GetFlightInfoStatement answers with the schema BEFORE running anything, which is the whole
// reason the type measurement has to be separable from the scan: a client is entitled to know
// a result's shape without paying for its rows.
//
// The ticket carries the query text rather than a server-side handle. A handle would mean
// holding per-query state between two RPCs that may not even arrive on the same connection,
// and the query is what the handle would look up anyway.
func (s *Server) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	table, err := parseSelectAll(cmd.GetQuery())
	if err != nil {
		return nil, err
	}
	schema, _, err := s.tableSchema(ctx, table)
	if err != nil {
		return nil, err
	}
	ticket, err := flightsql.CreateStatementQueryTicket([]byte(cmd.GetQuery()))
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// DoGetStatement scans the table and encodes it against the schema.
//
// The schema is derived again here rather than carried in the ticket. That is a second profile
// round trip, and it is the correct trade: a schema in the ticket would be a schema the client
// could edit, and this one has to match what the encoder will actually produce.
func (s *Server) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := string(ticket.GetStatementHandle())
	table, err := parseSelectAll(query)
	if err != nil {
		return nil, nil, err
	}
	// A stream slot first: refusing here costs nothing, while refusing after the schema
	// derivation would have spent a profile round trip on a request we decline anyway.
	slot, ok := s.acquireStream()
	if !ok {
		return nil, nil, status.Errorf(codes.ResourceExhausted,
			"too many concurrent Flight streams (limit %d); retry shortly", cap(s.streams))
	}
	db, release, err := s.dbFor(ctx)
	if err != nil {
		slot()
		return nil, nil, err
	}
	schema, decisions, err := s.tableSchemaWith(ctx, db, table)
	if err != nil {
		release()
		slot()
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk, 2)
	go func() {
		// The session and the stream slot outlive this call: DoGetStatement returns as
		// soon as the stream exists, and the scan runs afterwards. Releasing on return
		// would close the loopback under the goroutine still reading through it, and
		// would free the slot while the work it accounts for is still running.
		defer slot()
		defer release()
		defer close(ch)
		// A panic here would otherwise take the daemon down with it: this runs in
		// process, and one query must not be an outage. The recover is in this
		// goroutine deliberately -- a helper that wrapped recover() would catch
		// nothing, which is a mistake already made once in this tree.
		//
		// It is also the channel the memory budget refuses through: memory.Allocator
		// cannot return an error, so arrowtype.Budget panics, and this is what turns
		// that into a failed request rather than a dead daemon.
		budget := arrowtype.NewBudget(s.alloc, s.streamBudget)
		defer func() {
			if r := recover(); r != nil {
				ch <- flight.StreamChunk{Err: streamPanicError(r, budget)}
			}
		}()

		enc := arrowtype.NewEncoder(decisions, arrowtype.EncodeOptions{Alloc: budget})
		defer enc.Release()

		flush := func() bool {
			if enc.Rows() == 0 {
				return true
			}
			select {
			case ch <- flight.StreamChunk{Data: enc.NewRecord()}:
				return true
			case <-ctx.Done():
				return false
			}
		}
		scanErr := db.QueryTableStream(ctx, table, "true", 0, func(row string) bool {
			// QueryTable streams BRACKETED new-ClassAd text, not old.
			ad, perr := classad.Parse(row)
			if perr != nil {
				return true
			}
			_ = enc.Append(ad) // non-strict: coercions and nulls are counted, not fatal
			if enc.Rows() >= batchRows {
				return flush()
			}
			return true
		})
		if scanErr != nil {
			ch <- flight.StreamChunk{Err: scanErr}
			return
		}
		flush()
	}()
	return schema, ch, nil
}

// batchRows matches arrowstream's default, for the same reason sampleMax does.
const batchRows = 2048

// NewFlightServer wraps a service as a Flight server ready to register with gRPC.
func NewFlightServer(s *Server, alloc memory.Allocator) flight.FlightServer {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	return flightsql.NewFlightServerWithAllocator(s, alloc)
}

// NewSessionFlightServer is NewFlightServer for a service that resolves a per-identity session
// on every RPC. One Flight server serves every connection; what differs per RPC is the
// authorization its reads go through.
func NewSessionFlightServer(sessions *Sessions, alloc memory.Allocator) (flight.FlightServer, error) {
	srv, err := NewSessionServer(sessions, sessions.Derive(), alloc)
	if err != nil {
		return nil, err
	}
	return NewFlightServer(srv, alloc), nil
}

// streamPanicError turns a recovered panic into the error a client sees.
//
// A budget refusal is reported as ResourceExhausted with the numbers in it, because it is an
// ordinary resource limit and the client's recourse is a narrower query or a smaller batch --
// quite different from an internal fault, which is what anything else is. Reporting both the
// same way would have an operator hunting a bug when the answer is a configured limit.
func streamPanicError(r any, budget *arrowtype.Budget) error {
	var over *arrowtype.ErrBudgetExceeded
	if err, ok := r.(error); ok && errors.As(err, &over) {
		return status.Errorf(codes.ResourceExhausted,
			"this query needs more Arrow memory than the per-stream budget allows (%d bytes): %v",
			over.Limit, over)
	}
	return status.Errorf(codes.Internal, "the query failed internally (peak Arrow use %d bytes): %v",
		budget.Peak(), r)
}
