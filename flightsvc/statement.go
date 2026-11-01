package flightsvc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/ansi"
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
// The accepted grammar: SELECT <cols> FROM <table> [LIMIT n].
//
// The column list and the row count contain NO EXPRESSION, so they mean the same thing in
// every SQL dialect. The WHERE clause does not -- it is where the ClassAd/ANSI divergence
// lives -- so it goes through the ansi package, which lowers what it can prove equivalent and
// refuses the rest. An aggregate, an alias, a function or an ORDER BY still have semantics
// nothing here can get right, so they remain refused.
//
// Still a recogniser and not a parser. It accepts a shape or declines; there is no partial
// understanding in between, which is the property that makes a narrow surface safe rather than
// merely small.

var selectRe = regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s+from\s+(?:"([A-Za-z_][A-Za-z0-9_./-]*)"|([A-Za-z_][A-Za-z0-9_./-]*))(?:\s+where\s+(.+?))?(?:\s+limit\s+(\d+))?\s*;?\s*$`)

// identRe is one bare or double-quoted column name, and nothing else: no expression, no alias,
// no qualification. A list of these is the whole of what a projection may be.
var identRe = regexp.MustCompile(`^(?:"([A-Za-z_][A-Za-z0-9_]*)"|([A-Za-z_][A-Za-z0-9_]*))$`)

// query is a recognised statement.
type query struct {
	table string
	// cols is the projection in the order asked for, or nil for SELECT *. Order matters:
	// `SELECT b, a` must return b then a, because a client reads by position.
	cols []string
	// where is the predicate as written, still SQL. It is lowered against the table's
	// declared types, which the recogniser does not have -- so recognising and lowering
	// are separate steps.
	where string
	limit int
}

const unsupportedHint = "this endpoint accepts `SELECT <columns|*> FROM <table> [WHERE <predicate>] " +
	"[LIMIT n]`. An aggregate, an alias, a function or an ORDER BY is refused rather than " +
	"approximated, because a partly-understood query returns rows that look right"

// parseQuery recognises a statement, or returns an error naming what is supported.
func parseQuery(q string) (query, error) {
	m := selectRe.FindStringSubmatch(q)
	if m == nil {
		return query{}, fmt.Errorf("unsupported SQL: %s (got %q)", unsupportedHint, strings.TrimSpace(q))
	}
	out := query{table: m[2]}
	if out.table == "" {
		out.table = m[3]
	}
	out.where = strings.TrimSpace(m[4])
	if lim := m[5]; lim != "" {
		n, err := strconv.Atoi(lim)
		if err != nil {
			return query{}, fmt.Errorf("unsupported SQL: LIMIT %q is not a row count", lim)
		}
		out.limit = n
	}

	sel := strings.TrimSpace(m[1])
	if sel == "*" {
		return out, nil
	}
	for _, part := range strings.Split(sel, ",") {
		part = strings.TrimSpace(part)
		im := identRe.FindStringSubmatch(part)
		if im == nil {
			// Naming the offending term is the difference between a refusal somebody
			// can act on and one they have to bisect.
			return query{}, fmt.Errorf(
				"unsupported SQL: %q is not a plain column name; %s", part, unsupportedHint)
		}
		if im[1] != "" {
			out.cols = append(out.cols, im[1])
		} else {
			out.cols = append(out.cols, im[2])
		}
	}
	if len(out.cols) == 0 {
		return query{}, fmt.Errorf("unsupported SQL: no columns selected")
	}
	return out, nil
}

// project narrows a table's decisions to the requested columns, in the requested order.
//
// A requested column with no typed column is REFUSED, naming it and why, rather than omitted.
// Silently returning fewer columns than asked for is the failure this endpoint exists not to
// have: a client reads results by position, so a missing column does not shift one value, it
// shifts every value after it.
func project(decisions []arrowtype.Decision, cols []string) ([]arrowtype.Decision, error) {
	if len(cols) == 0 {
		return decisions, nil
	}
	byName := make(map[string]arrowtype.Decision, len(decisions))
	for _, d := range decisions {
		byName[strings.ToLower(d.Name)] = d
	}
	out := make([]arrowtype.Decision, 0, len(cols))
	for _, want := range cols {
		d, ok := byName[strings.ToLower(want)]
		if !ok {
			return nil, fmt.Errorf("no such column %q in this table", want)
		}
		if d.Verdict != arrowtype.Typed {
			return nil, fmt.Errorf("column %q has no typed form (%s: %s); it is available as text in the untyped tail",
				want, d.Reason, d.Detail)
		}
		out = append(out, d)
	}
	return out, nil
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
	schema, _, err := s.planSchema(ctx, cmd.GetQuery())
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.alloc)}, nil
}

// planSchema recognises a query and returns the schema its rows will carry, projection applied.
// It is what lets the schema be answered without running anything.
//
// A WHERE clause does NOT change the schema: it selects rows, and the columns a table offers
// are a property of the table. Deriving the schema from a predicate's matches instead would
// make it vary by query and a consumer's cached catalog worthless. It is still lowered here,
// so a predicate this endpoint cannot serve is refused when the schema is asked for rather
// than surfacing only once rows are fetched.
func (s *Server) planSchema(ctx context.Context, q string) (*arrow.Schema, query, error) {
	parsed, err := parseQuery(q)
	if err != nil {
		return nil, query{}, err
	}
	_, decisions, err := s.tableSchema(ctx, parsed.table)
	if err != nil {
		return nil, query{}, err
	}
	if _, err := constraintFor(parsed, decisions); err != nil {
		return nil, query{}, err
	}
	picked, err := project(decisions, parsed.cols)
	if err != nil {
		return nil, query{}, err
	}
	return schemaOf(picked), parsed, nil
}

// constraintFor lowers the query's WHERE clause to a ClassAd constraint, against the types the
// schema derivation declared. "true" -- everything -- when there is no predicate.
//
// The types are what make the lowering decidable: whether `=` on a column is the
// case-sensitive problem or an ordinary number is a question only the declared type answers.
func constraintFor(q query, decisions []arrowtype.Decision) (string, error) {
	if q.where == "" {
		return "true", nil
	}
	lowered, err := ansi.Lower(q.where, columnsOf(decisions))
	if err != nil {
		return "", fmt.Errorf("in the WHERE clause: %w", err)
	}
	return lowered, nil
}

// columnsOf exposes the derivation's decisions as column kinds.
//
// An attribute with no TYPED column is reported as present-but-untyped rather than absent, so a
// predicate on `Requirements` is refused for the right reason -- it is a policy expression with
// no scalar form -- instead of being reported as a column that does not exist.
func columnsOf(decisions []arrowtype.Decision) ansi.Columns {
	kinds := make(map[string]ansi.Kind, len(decisions))
	for _, d := range decisions {
		if d.Verdict != arrowtype.Typed {
			kinds[d.Name] = ansi.KindUnknown
			continue
		}
		switch d.Field.Type.ID() {
		case arrow.INT64:
			kinds[d.Name] = ansi.KindInt
		case arrow.FLOAT64:
			kinds[d.Name] = ansi.KindReal
		case arrow.STRING:
			kinds[d.Name] = ansi.KindString
		case arrow.BOOL:
			kinds[d.Name] = ansi.KindBool
		case arrow.TIMESTAMP:
			kinds[d.Name] = ansi.KindTimestamp
		default:
			kinds[d.Name] = ansi.KindUnknown
		}
	}
	return ansi.KindsFromNames(kinds)
}

// schemaOf builds the Arrow schema of the typed decisions, in their given order.
func schemaOf(decisions []arrowtype.Decision) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(decisions))
	for _, d := range decisions {
		if d.Verdict == arrowtype.Typed {
			fields = append(fields, d.Field)
		}
	}
	return arrow.NewSchema(fields, nil)
}

// GetFlightInfoStatement answers with the schema BEFORE running anything, which is the whole
// reason the type measurement has to be separable from the scan: a client is entitled to know
// a result's shape without paying for its rows.
//
// The ticket carries the query text rather than a server-side handle. A handle would mean
// holding per-query state between two RPCs that may not even arrive on the same connection,
// and the query is what the handle would look up anyway.
func (s *Server) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema, _, err := s.planSchema(ctx, cmd.GetQuery())
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
	return s.doGetQuery(ctx, string(ticket.GetStatementHandle()))
}

// doGetQuery is the one scan implementation, shared by a direct statement and a prepared one.
// Two copies would drift, and the drift would be invisible: a prepared statement returning
// subtly different rows from the same SQL run directly is the kind of bug nobody reports
// because nobody compares.
func (s *Server) doGetQuery(ctx context.Context, stmt string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	parsed, err := parseQuery(stmt)
	if err != nil {
		return nil, nil, err
	}
	table := parsed.table
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
	_, allDecisions, err := s.tableSchemaWith(ctx, db, table)
	if err != nil {
		release()
		slot()
		return nil, nil, err
	}
	constraint, err := constraintFor(parsed, allDecisions)
	if err != nil {
		release()
		slot()
		return nil, nil, err
	}
	decisions, err := project(allDecisions, parsed.cols)
	if err != nil {
		release()
		slot()
		return nil, nil, err
	}
	schema := schemaOf(decisions)

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
		scanErr := db.QueryTableStream(ctx, table, constraint, parsed.limit, func(row string) bool {
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

// Limits are the resource bounds an in-process endpoint needs. Both zero means unlimited,
// which is what the endpoint did before the limits existed.
type Limits struct {
	// StreamBudgetBytes caps the Arrow memory one DoGet stream may hold.
	StreamBudgetBytes int64
	// MaxStreams caps concurrent DoGet streams.
	MaxStreams int
}

// NewSessionFlightServer is NewFlightServer for a service that resolves a per-identity session
// on every RPC. One Flight server serves every connection; what differs per RPC is the
// authorization its reads go through.
func NewSessionFlightServer(sessions *Sessions, alloc memory.Allocator, limits Limits) (flight.FlightServer, error) {
	srv, err := NewSessionServer(sessions, sessions.Derive(), alloc)
	if err != nil {
		return nil, err
	}
	srv.SetStreamBudget(limits.StreamBudgetBytes)
	srv.SetMaxStreams(limits.MaxStreams)
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
