package flightsvc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Prepared statements, which many ADBC clients use whether or not they bind a parameter.
//
// A prepared statement here is a RECOGNISED QUERY AND ITS SCHEMA, nothing more. Preparing
// validates the statement -- recognises the shape, lowers the predicate, derives the schema --
// so a client learns at prepare time that its SQL will work, which is most of what preparing
// is for. It caches no plan and holds no cursor, because the scan is streamed fresh on each
// DoGet and there is nothing to keep between them.
//
// PARAMETERS are bound by a DoPut and kept on the statement, because that is the sequence the
// protocol specifies: a client prepares, binds, then fetches on the same handle. The parameter
// schema is reported from the lowered predicate, typed from the columns the placeholders compare
// against, so a client binds what it would read -- and a statement with no placeholders reports
// an EMPTY schema rather than omitting the field, so a client can see that it takes none instead
// of guessing.
//
// A bind mutates the statement, so two fetches racing a rebind on one handle see whichever bind
// won. That is inherent to a protocol where the values live on the server between two calls, not
// something this file can fix; a driver binds and fetches in sequence on one statement object,
// which is the only usage the shape makes sense for.
//
// SERVER-SIDE STATE, which this endpoint otherwise has none of, and the two things that makes
// necessary:
//
//   - A HANDLE IS BOUND TO THE IDENTITY THAT CREATED IT. Handles are random, so they cannot be
//     enumerated, and checked against the caller, so they cannot be borrowed. The data a reused
//     handle would return is still authorized per RPC through the caller's own session -- so
//     the leak would be the query TEXT, not rows -- but a statement naming a table is still
//     something an identity should not learn from another's handle.
//   - THE TABLE IS BOUNDED AND ENTRIES EXPIRE. A client that never closes its statements would
//     otherwise grow the daemon's memory without limit, which is the same failure the stream
//     cap exists to prevent and deserves the same answer: refuse past the cap rather than
//     accumulate, and expire what a disconnected client left behind.

const (
	// defaultMaxPrepared bounds live statements across all identities. Generous for real
	// use -- a client holds a handful -- and small enough that a leaking client is refused
	// long before the daemon notices.
	defaultMaxPrepared = 256
	// defaultPreparedTTL expires a statement a client never closed. A disconnected client
	// cannot close anything, so without expiry its handles are leaked for the daemon's
	// lifetime.
	defaultPreparedTTL = 30 * time.Minute
)

type preparedStmt struct {
	query   string
	owner   string
	created time.Time
	// bound is the values a DoPut supplied, or nil if the client has not bound any.
	// Nil is meaningfully different from empty: see the args type.
	bound *args
}

// prepared is the live statement table.
type prepared struct {
	mu    sync.Mutex
	byKey map[string]preparedStmt
	max   int
	ttl   time.Duration
}

func newPrepared() *prepared {
	return &prepared{byKey: map[string]preparedStmt{}, max: defaultMaxPrepared, ttl: defaultPreparedTTL}
}

// add stores a statement and returns its handle. It expires stale entries first, so a cap is
// reached only by statements that are genuinely live rather than by accumulated debris.
func (p *prepared) add(query, owner string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireLocked()
	if len(p.byKey) >= p.max {
		return nil, status.Errorf(codes.ResourceExhausted,
			"too many prepared statements (limit %d); close the ones you are finished with", p.max)
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return nil, status.Errorf(codes.Internal, "generating a statement handle: %v", err)
	}
	key := hex.EncodeToString(raw)
	p.byKey[key] = preparedStmt{query: query, owner: owner, created: time.Now()}
	return []byte(key), nil
}

// get returns the statement a handle names, provided the caller owns it.
//
// An unknown handle and one owned by somebody else report the SAME error on purpose: telling
// them apart would turn the handle space into an oracle for which handles exist.
func (p *prepared) get(handle []byte, caller string) (preparedStmt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireLocked()
	st, ok := p.byKey[string(handle)]
	if !ok || st.owner != caller {
		return preparedStmt{}, status.Error(codes.NotFound, "no such prepared statement")
	}
	return st, nil
}

// bind attaches values to a statement. It replaces any previous binding rather than appending,
// so a client reusing one statement does not accumulate the values of its earlier executions.
func (p *prepared) bind(handle []byte, caller string, vals []any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireLocked()
	key := string(handle)
	st, ok := p.byKey[key]
	if !ok || st.owner != caller {
		return status.Error(codes.NotFound, "no such prepared statement")
	}
	st.bound = &args{vals: vals}
	p.byKey[key] = st
	return nil
}

func (p *prepared) remove(handle []byte, caller string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := string(handle)
	st, ok := p.byKey[key]
	if !ok || st.owner != caller {
		return status.Error(codes.NotFound, "no such prepared statement")
	}
	delete(p.byKey, key)
	return nil
}

// expireLocked drops statements past the TTL. Called on every access rather than from a timer:
// a table nobody touches needs no sweeping, and a timer would be a goroutine outliving the
// endpoint it serves.
func (p *prepared) expireLocked() {
	if p.ttl <= 0 {
		return
	}
	cutoff := time.Now().Add(-p.ttl)
	for k, st := range p.byKey {
		if st.created.Before(cutoff) {
			delete(p.byKey, k)
		}
	}
}

func (p *prepared) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byKey)
}

// identity returns who this RPC is, for handle ownership. A server with a fixed client (the
// single-identity form used by tests and by a non-Flight caller) has one owner, named so the
// ownership check is still exercised rather than bypassed.
func (s *Server) identity(ctx context.Context) (string, error) {
	if s.sessions == nil {
		return "fixed-client", nil
	}
	return s.sessions.Identity(ctx)
}

// CreatePreparedStatement validates the query and returns a handle.
//
// Validation is the point: a client learns here that its SQL is one this endpoint will serve,
// rather than at the first fetch. That means doing the schema derivation now -- a profile round
// trip -- which is what preparing costs and what it buys.
func (s *Server) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	var res flightsql.ActionCreatePreparedStatementResult
	owner, err := s.identity(ctx)
	if err != nil {
		return res, err
	}
	schema, params, _, err := s.planStatement(ctx, req.GetQuery())
	if err != nil {
		return res, err
	}
	handle, err := s.prep.add(req.GetQuery(), owner)
	if err != nil {
		return res, err
	}
	isUpdate := false
	return flightsql.ActionCreatePreparedStatementResult{
		Handle:          handle,
		DatasetSchema:   schema,
		ParameterSchema: params,
		IsUpdate:        &isUpdate,
	}, nil
}

func (s *Server) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	owner, err := s.identity(ctx)
	if err != nil {
		return err
	}
	return s.prep.remove(req.GetPreparedStatementHandle(), owner)
}

func (s *Server) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	st, err := s.preparedStatement(ctx, cmd)
	if err != nil {
		return nil, err
	}
	schema, _, err := s.planSchema(ctx, st.query)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.alloc)}, nil
}

func (s *Server) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	st, err := s.preparedStatement(ctx, cmd)
	if err != nil {
		return nil, err
	}
	schema, _, err := s.planSchema(ctx, st.query)
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// DoGetPreparedStatement serves the prepared query's rows. It resolves the handle to its SQL
// and runs the ordinary statement path, so a prepared statement and a direct one cannot drift:
// there is one scan implementation, not two.
func (s *Server) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	st, err := s.preparedStatement(ctx, cmd)
	if err != nil {
		return nil, nil, err
	}
	bound := st.bound
	if bound == nil {
		// Prepared but never bound. This passes an EMPTY args rather than nil, which is
		// the difference between "a prepared statement with nothing bound yet" and "a
		// direct statement": both are refused if the predicate has placeholders, but a
		// client that prepared and forgot to bind should be told to bind, not told to use
		// a prepared statement it is already using.
		bound = &args{}
	}
	return s.doGetQuery(ctx, st.query, bound)
}

func (s *Server) preparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (preparedStmt, error) {
	owner, err := s.identity(ctx)
	if err != nil {
		return preparedStmt{}, err
	}
	return s.prep.get(cmd.GetPreparedStatementHandle(), owner)
}

// DoPutPreparedStatementQuery binds parameters to a statement.
//
// The values are checked against the declared column types HERE rather than at the fetch, so a
// client learns its bind was wrong at the call that made it. The same handle comes back, which
// is what the client goes on to fetch with.
func (s *Server) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, r flight.MessageReader, _ flight.MetadataWriter) ([]byte, error) {
	owner, err := s.identity(ctx)
	if err != nil {
		return nil, err
	}
	handle := cmd.GetPreparedStatementHandle()
	st, err := s.prep.get(handle, owner)
	if err != nil {
		return nil, err
	}
	vals, err := readBoundParameters(r)
	if err != nil {
		return nil, err
	}
	// Validate before storing, so a rejected bind leaves the statement as it was rather
	// than half-updated. This repeats the lowering, which is cheap next to the profile
	// round trip the derivation needs -- and the derivation is what types the check.
	if _, _, err := s.bindCheck(ctx, st.query, vals); err != nil {
		return nil, err
	}
	if err := s.prep.bind(handle, owner, vals); err != nil {
		return nil, err
	}
	return handle, nil
}

// bindCheck lowers the statement with the values applied, returning the constraint it produced.
// It exists so a bad bind is refused at the DoPut that sent it; the constraint is recomputed at
// the fetch rather than cached, because the fetch runs against a fresh derivation anyway.
func (s *Server) bindCheck(ctx context.Context, stmt string, vals []any) (string, query, error) {
	parsed, err := parseQuery(stmt)
	if err != nil {
		return "", query{}, err
	}
	_, decisions, err := s.tableSchema(ctx, parsed.table)
	if err != nil {
		return "", query{}, err
	}
	constraint, err := constraintFor(parsed, decisions, &args{vals: vals})
	if err != nil {
		return "", query{}, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return constraint, parsed, nil
}
