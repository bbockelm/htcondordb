// Package flightsvc serves htcondordb over Arrow Flight SQL, which is what ADBC clients speak.
//
// WHAT IT ACCEPTS, AND WHY SO LITTLE. The catalog surface is complete: an ADBC client can
// connect, list catalogs, schemas and tables, and read each table's Arrow schema -- derived
// from a measurement of what its attributes actually hold, not guessed. The STATEMENT surface
// accepts exactly one shape, `SELECT * FROM <table>`, and refuses everything else with a clear
// error naming what it understood.
//
// That is deliberate, and it is the inverse of how the DuckDB/Airport path would work. There,
// filters we cannot translate may be DROPPED, because the client re-filters and a superset is
// corrected downstream. Here nothing downstream corrects us: a query we half-understand
// returns wrong rows that look right. So the rule inverts -- REFUSE, never approximate -- and
// that is exactly what makes shipping a partial SQL surface safe. The accepted grammar grows
// as the ANSI front end does; what it has not learned, it declines.
//
// DIALECT. Flight SQL carries SQL text, so the dialect is part of the contract the moment the
// endpoint is live. This endpoint is ANSI-only by construction: it does not accept the
// ClassAd-flavoured dialect at all, so no query can change meaning later when the ANSI front
// end lands. The one shape accepted today has no expressions in it, so it means the same thing
// in either dialect -- which is why it is a safe place to start.
package flightsvc

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// tableType is what every htcondordb table reports as. Flight SQL's table_type is the
// JDBC/ODBC vocabulary ("TABLE", "VIEW", "SYSTEM TABLE"); a mutable table and an archive are
// both plain tables to a reader, and the difference -- whether it can be written -- is not
// something this read-only endpoint exposes.
const tableType = "TABLE"

// defaultSchemaName is the single db_schema this endpoint presents. The store has tables and
// no schemas above them, and inventing a hierarchy it does not have would make a client's
// three-part names mean nothing.
const defaultSchemaName = "htcondordb"

// Server is a read-only Flight SQL server over one htcondordb daemon.
//
// The dbrpc client is resolved PER RPC rather than held, because one Flight server serves every
// connection while each connection's reads must go through a session carrying its own
// identity's authorization. dbFor is that resolution: a fixed client for a single-identity
// server, or a freshly authorized loopback session for a multi-identity one.
//
// Per RPC, not per connection, and deliberately: gRPC offers no stable per-connection handle in
// a handler's context without a stats handler, and a session per RPC is a pipe and a goroutine
// whose cost is small beside the ad text already crossing it. Rung 0 is not the fast path.
type Server struct {
	flightsql.BaseServer

	// dbFor returns a client for this RPC, whether that client's session sees private
	// attributes, and a release to call when the RPC's work -- including any stream it
	// started -- is finished.
	//
	// The privilege bit comes back with the client because it is decided by the same act
	// of authorizing it, and because it is the only identity-dependent input to a schema
	// derivation: an unprivileged type report has the private attributes filtered out, so
	// a derived schema may not be shared across that line (see schemaCache).
	dbFor func(context.Context) (*dbrpc.Client, bool, func(), error)
	alloc memory.Allocator
	// derive carries the type declarations -- epoch attributes, and the overrides for
	// attributes whose type inference cannot decide (see arrowtype.Override).
	derive arrowtype.Options
	// sample caps how many records a schema derivation reads; 0 uses the default.
	sample int
	// streamBudget caps the Arrow bytes one DoGet stream may hold; 0 is unlimited.
	streamBudget int64
	// sessions resolves a per-identity session, and is nil for a fixed-client server.
	// Prepared-statement handles are bound to the identity it reports.
	sessions *Sessions
	// prep is the live prepared-statement table.
	prep *prepared
	// schemas holds recently derived table schemas. Nil or a zero TTL derives every time.
	schemas *schemaCache
	// streams admits at most one token per concurrent DoGet stream. Nil means unlimited.
	//
	// A cap exists because this runs in the daemon: concurrent wide scans compete for CPU
	// and heap with the store's own goroutines and its maintenance, and the failure mode of
	// having no cap is not a slow endpoint but a slow DATABASE. Refusing the eleventh
	// stream is a client's problem; starving the store is everyone's.
	streams chan struct{}
}

// SetStreamBudget caps the Arrow bytes a single DoGet stream may hold. Zero is unlimited,
// which is memory.DefaultAllocator's behaviour and what an unconfigured endpoint gets.
func (s *Server) SetStreamBudget(bytes int64) { s.streamBudget = bytes }

// SetMaxStreams caps concurrent DoGet streams. Zero or less is unlimited.
func (s *Server) SetMaxStreams(n int) {
	if n <= 0 {
		s.streams = nil
		return
	}
	s.streams = make(chan struct{}, n)
}

// acquireStream takes a slot without waiting. Blocking would turn a cap into a queue, and a
// queue of scans inside the daemon is the thing the cap exists to prevent -- a client told
// "too many streams" can retry, a client left hanging cannot tell whether to.
func (s *Server) acquireStream() (func(), bool) {
	if s.streams == nil {
		return func() {}, true
	}
	select {
	case s.streams <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-s.streams }) }, true
	default:
		return nil, false
	}
}

// SetSchemaTTL caps how long a derived table schema is reused. Zero disables the cache, which
// derives one per RPC -- what this served before the cache existed.
func (s *Server) SetSchemaTTL(ttl time.Duration) {
	if ttl <= 0 {
		s.schemas = nil
		return
	}
	s.schemas = newSchemaCache(ttl)
}

// SetSampleMax caps how many records a schema derivation reads. Zero restores the default.
//
// Lowering it is how a test reaches the CAPPED case, where the sample is a prefix of the table
// rather than the whole of it -- the case in which a column cannot soundly be declared
// non-nullable. Any fixture small enough to run quickly is otherwise sampled completely, so
// that rule would never be exercised.
func (s *Server) SetSampleMax(n int) { s.sample = n }

// SetVersion declares the daemon's version to a client, which ADBC asks for by name at connect:
// adbc_get_info() requests FLIGHT_SQL_SERVER_VERSION, and an unregistered entry comes back as
// NOT_FOUND rather than as an empty string -- so the whole call fails and the client reports a
// broken server. The default below keeps that from happening when nobody calls this; the daemon
// passes its build stamp.
func (s *Server) SetVersion(v string) error {
	if v == "" {
		return nil
	}
	return s.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, v)
}

// NewServer builds the Flight SQL service. The dbrpc client carries the connection's
// authorization, so every read this serves is already scoped to what that connection may see;
// serving one connection's server to a less privileged reader would defeat that.
func NewServer(db *dbrpc.Client, derive arrowtype.Options, alloc memory.Allocator) (*Server, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	s := &Server{
		// A fixed client is one session with one authorization, so its privilege is
		// whatever that session was served with. This constructor does not know it --
		// the caller chose it when it built the client -- so the scope is reported as
		// unprivileged, which is the conservative key: it never shares an entry with a
		// privileged derivation, and with a single client there is nobody to share with.
		dbFor: func(context.Context) (*dbrpc.Client, bool, func(), error) { return db, false, func() {}, nil },
		alloc: alloc, derive: derive, prep: newPrepared(),
	}
	if err := s.registerSqlInfo(); err != nil {
		return nil, err
	}
	return s, nil
}

// NewSessionServer builds a Flight SQL service that resolves a per-identity session for every
// RPC, which is how one server serves many identities without any of them seeing another's
// authorization.
func NewSessionServer(sessions *Sessions, derive arrowtype.Options, alloc memory.Allocator) (*Server, error) {
	if sessions == nil {
		return nil, fmt.Errorf("flightsvc: no sessions")
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	s := &Server{dbFor: sessions.ForScoped, alloc: alloc, derive: derive,
		sessions: sessions, prep: newPrepared()}
	if err := s.registerSqlInfo(); err != nil {
		return nil, err
	}
	return s, nil
}

// registerSqlInfo answers what an ADBC client asks at connect.
//
// The entries that describe the DIALECT matter more than the version strings: a client that
// does not ask still assumes, and the assumptions it would make are wrong here in ways that
// change results. Flight SQL has no field for value-comparison collation, which is the one
// divergence that most needs declaring, so it is stated in the server name instead -- a poor
// channel, but better than silence.
func (s *Server) registerSqlInfo() error {
	for _, e := range []struct {
		id  flightsql.SqlInfo
		val interface{}
	}{
		{flightsql.SqlInfoFlightSqlServerName, "htcondordb"},
		// A version is REQUIRED in practice, not optional: ADBC's adbc_get_info()
		// requests it by name, and an unregistered entry is NOT_FOUND rather than an
		// empty string, which fails the whole call. The daemon overwrites this with its
		// build stamp through SetVersion; this placeholder is what keeps a Server nobody
		// configures from looking broken to a real client. Found by connecting one --
		// arrow-go's own client never asks, so no test in this package had reached it.
		{flightsql.SqlInfoFlightSqlServerVersion, "unknown"},
		{flightsql.SqlInfoFlightSqlServerArrowVersion, arrow.PkgVersion},
		// This endpoint takes SQL, not Substrait plans. A client that asks and gets
		// NOT_FOUND learns nothing; one that is told can pick the right path first time.
		{flightsql.SqlInfoFlightSqlServerSql, true},
		{flightsql.SqlInfoFlightSqlServerSubstrait, false},
		// No transactions and no cancellation, said explicitly so a client does not
		// offer either. SqlTransactionNone is the enum's "not supported" value.
		{flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionNone)},
		{flightsql.SqlInfoFlightSqlServerCancel, false},
		// Read-only: there is no DML surface at all, and a client that knows this will
		// not offer an editing UI it cannot use.
		{flightsql.SqlInfoFlightSqlServerReadOnly, true},
		{flightsql.SqlInfoDDLCatalog, false},
		{flightsql.SqlInfoDDLSchema, false},
		{flightsql.SqlInfoDDLTable, false},
		// ClassAd attribute names are case-insensitive and are not folded on the way in,
		// so an unquoted identifier keeps its spelling and matches regardless of case.
		{flightsql.SqlInfoIdentifierCase, int32(flightsql.SqlCaseSensitivityCaseInsensitive)},
		{flightsql.SqlInfoQuotedIdentifierCase, int32(flightsql.SqlCaseSensitivityCaseInsensitive)},
		{flightsql.SqlInfoIdentifierQuoteChar, `"`},
	} {
		if err := s.RegisterSqlInfo(e.id, e.val); err != nil {
			return fmt.Errorf("sql info %v: %w", e.id, err)
		}
	}
	return nil
}

// --- catalog ---

// chunk sends one record as a single-chunk stream. Every catalog answer here is small --
// counts of tables, not rows of data -- so building it whole is simpler than streaming it and
// costs nothing that matters.
func chunk(rec arrow.Record) <-chan flight.StreamChunk {
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: rec}
	close(ch)
	return ch
}

func (s *Server) stringRecord(schema *arrow.Schema, cols [][]string) arrow.Record {
	b := array.NewRecordBuilder(s.alloc, schema)
	defer b.Release()
	for i, vals := range cols {
		fb := b.Field(i).(*array.StringBuilder)
		for _, v := range vals {
			fb.Append(v)
		}
	}
	return b.NewRecord()
}

func (s *Server) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return infoFor(desc, schema_ref.Catalogs), nil
}

// DoGetCatalogs reports no catalogs. The store has tables, not a catalog hierarchy, and an
// empty catalog list is how Flight SQL says that -- inventing one would give a client
// three-part names that address nothing.
func (s *Server) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.Catalogs, chunk(s.stringRecord(schema_ref.Catalogs, [][]string{{}})), nil
}

func (s *Server) GetFlightInfoSchemas(_ context.Context, _ flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return infoFor(desc, schema_ref.DBSchemas), nil
}

func (s *Server) DoGetDBSchemas(context.Context, flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	rec := s.stringRecord(schema_ref.DBSchemas, [][]string{{""}, {defaultSchemaName}})
	return schema_ref.DBSchemas, chunk(rec), nil
}

func (s *Server) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return infoFor(desc, schema_ref.TableTypes), nil
}

func (s *Server) DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	rec := s.stringRecord(schema_ref.TableTypes, [][]string{{tableType}})
	return schema_ref.TableTypes, chunk(rec), nil
}

func (s *Server) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if cmd.GetIncludeSchema() {
		return infoFor(desc, schema_ref.TablesWithIncludedSchema), nil
	}
	return infoFor(desc, schema_ref.Tables), nil
}

// DoGetTables lists the daemon's tables, optionally with each one's Arrow schema.
//
// With IncludeSchema the schema is DERIVED per table, which costs a profile round trip each.
// That is the expensive form and a client asks for it explicitly, which is why it is worth
// honouring rather than refusing: a catalog browser that cannot see column types is not much
// of a catalog browser.
func (s *Server) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	db, private, release, err := s.dbFor(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer release()
	names, err := db.Tables(ctx)
	if err != nil {
		return nil, nil, err
	}
	names = filterTables(names, cmd)

	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	b := array.NewRecordBuilder(s.alloc, schema)
	defer b.Release()
	for _, name := range names {
		b.Field(0).(*array.StringBuilder).Append("")
		b.Field(1).(*array.StringBuilder).Append(defaultSchemaName)
		b.Field(2).(*array.StringBuilder).Append(name)
		b.Field(3).(*array.StringBuilder).Append(tableType)
		if !cmd.GetIncludeSchema() {
			continue
		}
		sch, _, derr := s.tableSchemaWith(ctx, db, name, private)
		if derr != nil {
			// A table whose schema cannot be derived is still a table; reporting it
			// with no schema is more useful than failing the whole listing, which
			// would make one bad table hide every good one.
			b.Field(4).(*array.BinaryBuilder).AppendNull()
			continue
		}
		b.Field(4).(*array.BinaryBuilder).Append(flight.SerializeSchema(sch, s.alloc))
	}
	return schema, chunk(b.NewRecord()), nil
}

// filterTables applies the catalog/schema/name/type filters Flight SQL allows. A filter that
// names a catalog or schema this endpoint does not have matches nothing, which is the honest
// answer -- it is not an error to ask about a catalog that is not here.
func filterTables(names []string, cmd flightsql.GetTables) []string {
	if c := cmd.GetCatalog(); c != nil && *c != "" {
		return nil
	}
	if sp := cmd.GetDBSchemaFilterPattern(); sp != nil && !likeMatch(*sp, defaultSchemaName) {
		return nil
	}
	if types := cmd.GetTableTypes(); len(types) > 0 {
		found := false
		for _, t := range types {
			if strings.EqualFold(t, tableType) {
				found = true
			}
		}
		if !found {
			return nil
		}
	}
	pat := cmd.GetTableNameFilterPattern()
	if pat == nil || *pat == "" {
		return names
	}
	var out []string
	for _, n := range names {
		if likeMatch(*pat, n) {
			out = append(out, n)
		}
	}
	return out
}

// likeMatch implements the SQL LIKE patterns Flight SQL's filters use: % for any run, _ for
// one character. Matching is case-insensitive, because the identifiers being matched are
// ClassAd/table names and those are.
func likeMatch(pattern, s string) bool {
	var b strings.Builder
	b.WriteString("(?i)^")
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(s)
}

// infoFor builds a FlightInfo whose single endpoint carries the descriptor's own command as
// its ticket, which is how the Flight SQL machinery routes a DoGet back to the right handler.
func infoFor(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}
