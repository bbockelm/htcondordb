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
type Server struct {
	flightsql.BaseServer

	db    *dbrpc.Client
	alloc memory.Allocator
	// derive carries the type declarations -- epoch attributes, and the overrides for
	// attributes whose type inference cannot decide (see arrowtype.Override).
	derive arrowtype.Options
	// sample caps how many records a schema derivation reads; 0 uses the default.
	sample int
}

// SetSampleMax caps how many records a schema derivation reads. Zero restores the default.
//
// Lowering it is how a test reaches the CAPPED case, where the sample is a prefix of the table
// rather than the whole of it -- the case in which a column cannot soundly be declared
// non-nullable. Any fixture small enough to run quickly is otherwise sampled completely, so
// that rule would never be exercised.
func (s *Server) SetSampleMax(n int) { s.sample = n }

// NewServer builds the Flight SQL service. The dbrpc client carries the connection's
// authorization, so every read this serves is already scoped to what that connection may see;
// serving one connection's server to a less privileged reader would defeat that.
func NewServer(db *dbrpc.Client, derive arrowtype.Options, alloc memory.Allocator) (*Server, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	s := &Server{db: db, alloc: alloc, derive: derive}
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
		{flightsql.SqlInfoFlightSqlServerArrowVersion, arrow.PkgVersion},
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
	names, err := s.db.Tables(ctx)
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
		sch, _, derr := s.tableSchema(ctx, name)
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
