package flightsvc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

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
	"github.com/bbockelm/htcondordb/repl"
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

const unsupportedHint = "this endpoint accepts a read-only SELECT over one table: columns, " +
	"aggregates, GROUP BY, HAVING, ORDER BY, DISTINCT and LIMIT, as in " +
	"`SELECT <columns|*> FROM <table> [WHERE <predicate>] [LIMIT n]`. A JOIN, a set " +
	"operation, and anything that writes are refused rather than approximated, because a " +
	"partly-understood query returns rows that look right"

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
			return nil, fmt.Errorf("column %q has no typed form (%s: %s); it is available as text in %s",
				want, d.Reason, d.Detail, arrowtype.TailColumnName)
		}
		out = append(out, d)
	}
	return out, nil
}

// tableSchema derives a table's Arrow schema from a measurement of what its attributes hold.
// It returns the decisions too, so a caller can report which attributes got no column.
func (s *Server) tableSchema(ctx context.Context, table string) (*arrow.Schema, []arrowtype.Decision, error) {
	db, private, release, err := s.dbFor(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer release()
	return s.tableSchemaWith(ctx, db, table, private)
}

// tableSchemaWith is tableSchema on a client the caller already holds, so a handler that makes
// several calls on one session does not open a session per call.
func (s *Server) tableSchemaWith(ctx context.Context, db *dbrpc.Client, table string, private bool) (*arrow.Schema, []arrowtype.Decision, error) {
	key := schemaKey{table: table, private: private}
	if schema, decisions, ok := s.schemas.get(key); ok {
		return schema, decisions, nil
	}
	max := s.sampleMax()
	// The EVALUATED classes, because this endpoint's encoder evaluates: an attribute a pool
	// stores as an expression is an integer in the column, and typing it from the stored
	// classes rules it untypeable and gives it no column at all. MemoryUsage is that
	// attribute on a real history table.
	//
	// A store too old to measure them answers the plain request, successfully, with the
	// stored classes -- which is a different answer, not a lesser one, so it is taken only
	// when the peer has said it cannot do better. The endpoint still works against such a
	// store; it just publishes the schema it published before.
	rep, err := db.AttrTypeProfilesEvaluated(ctx, table, max)
	if errors.Is(err, dbrpc.ErrEvalTypesUnsupported) {
		rep, err = db.AttrTypeProfiles(ctx, table, max)
	}
	if err != nil {
		return nil, nil, err
	}
	// A sample that came back short of its cap saw the whole table, which is the only
	// sound basis for declaring a column non-nullable.
	complete := rep.Sampled < max
	ms := arrowtype.FromProfilesComplete(rep.Profiles, rep.Sampled, complete)
	// The tail is this endpoint's contract rather than the host's choice. Without it an
	// attribute the measurement declined to type is simply absent from the schema, and a
	// client cannot tell that from an attribute the table does not have -- so a host could
	// not turn it off without making the schema lie, and there is no reason to let it.
	derive := s.derive
	derive.Tail = true
	_, decisions := arrowtype.Schema(ms, derive)
	// What the endpoint has settled on for this table outranks a single sample, and the
	// memory appends to itself whatever the measurement found that it had not seen
	// (columntypes.go). Without this the published schema moves with the sample: a column
	// retypes on one stray value, a new attribute lands in the middle of the order, and a
	// client holding a cached catalog is holding something that was true once. A type that
	// has genuinely changed still moves -- after successive derivations agree -- and when it
	// does, the generation below says so.
	decisions = s.types.reconcile(table, decisions, private)
	schema := tableSchemaOf(decisions, table, s.types.generation(table))
	s.schemas.put(key, schema, decisions)
	// Off the read path: a derivation that extended the declaration writes it, and a
	// failure to write is reported rather than failing the read, because the file is a
	// stability optimization and never a correctness dependency.
	s.types.flush()
	return schema, decisions, nil
}

// defaultSampleMax is how many records a schema derivation looks at: the shared default, so the
// schema a client sees through the catalog, the one it gets from a query, and the one the REPL
// derives are all the same declaration about the same table.
const defaultSampleMax = arrowtype.DefaultSampleMax

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
	p, err := s.planStatement(ctx, cmd.GetQuery())
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(p.schema, s.alloc)}, nil
}

// plan is everything a statement can be answered with before any rows are read: the shape of
// its result, the shape of its parameters, the query it came from, and the NAME of that result
// shape (see pin.go), which is what lets a later fetch prove it is delivering what was promised.
type plan struct {
	schema *arrow.Schema
	params *arrow.Schema
	parsed query
	// fp names schema. Carried to the client in the ticket and checked back at the fetch.
	fp string
	// an is set when the simple recogniser refused the statement and the executor took
	// it instead (analytic.go). Its decisions are typed from the SELECT list rather than
	// from the table, so they travel with it.
	an        *analytic
	decisions []arrowtype.Decision
}

// planStatement recognises a query and returns the schema its rows will carry, projection
// applied. It is what lets the schema be answered without running anything.
//
// A WHERE clause does NOT change the schema: it selects rows, and the columns a table offers
// are a property of the table. Deriving the schema from a predicate's matches instead would
// make it vary by query and a consumer's cached catalog worthless. It is still lowered here,
// so a predicate this endpoint cannot serve is refused when the schema is asked for rather
// than surfacing only once rows are fetched.
//
// One implementation, so a prepared statement cannot disagree with a direct one about what a
// query means or which columns it returns. The parameter schema only a prepare needs, and it
// is built from the DECLARED column types, so a client binds what it would read: a placeholder
// against a timestamp column takes a timestamp, not the integer underneath.
func (s *Server) planStatement(ctx context.Context, q string) (plan, error) {
	parsed, err := parseQuery(q)
	if err != nil {
		// The scan recogniser refused. That is not the same as "not SQL": an
		// aggregate, a GROUP BY or an ORDER BY is a statement the executor serves,
		// so it gets a second reading before the refusal stands (analytic.go).
		an, aerr := parseAnalytic(q)
		if aerr != nil {
			if _, shape := aerr.(*unsupportedShape); shape {
				// It IS SQL, and it is a shape this endpoint does not serve.
				// More use to a reader than "a regexp did not match".
				return plan{}, aerr
			}
			return plan{}, err
		}
		// From here the statement is the executor's, so ITS complaints are the ones
		// worth reporting -- "no such column Frob" rather than the scan recogniser's
		// list of shapes, which is an answer to a question the client did not ask.
		return s.planAnalytic(ctx, an)
	}
	_, decisions, err := s.tableSchema(ctx, parsed.table)
	if err != nil {
		return plan{}, err
	}
	tmpl, err := templateFor(parsed, decisions)
	if err != nil {
		return plan{}, err
	}
	picked, err := project(decisions, parsed.cols)
	if err != nil {
		return plan{}, err
	}
	// The fingerprint names the RESULT columns rather than the table's whole decision list.
	// That is what the client was told and what it reads by position; a column appearing
	// elsewhere in the table changes neither, and refusing over it would make a busy table
	// unqueryable for the sake of a promise nobody made.
	return plan{schema: schemaOf(picked), params: paramSchema(tmpl, decisions), parsed: parsed,
		fp: arrowtype.Fingerprint(picked), decisions: picked}, nil
}

// planAnalytic plans a statement the scan recogniser refused, by typing the SELECT list
// against the table's own declarations.
//
// It takes no parameters. A prepared statement with a `?` in an aggregate query would need
// the bound value to reach `repl`'s parser, which parses once and holds a ClassAd predicate
// thereafter -- so binding would mean re-planning per execution, and the schema is promised
// before that. Refusing is honest and the client can inline the value.
func (s *Server) planAnalytic(ctx context.Context, an *analytic) (plan, error) {
	_, table, err := s.tableSchema(ctx, an.st.Table)
	if err != nil {
		return plan{}, err
	}
	decisions, err := repl.ArrowDecisions(an.st, table)
	if err != nil {
		return plan{}, err
	}
	return plan{
		schema:    schemaOf(decisions),
		params:    arrow.NewSchema(nil, nil),
		parsed:    query{table: an.st.Table, limit: an.st.Limit},
		fp:        arrowtype.Fingerprint(decisions),
		an:        an,
		decisions: decisions,
	}, nil
}

// paramSchema describes the placeholders, in order.
//
// EMPTY RATHER THAN NIL when a statement takes none: saying "no parameters" beats leaving a
// client to infer it from an absent field.
func paramSchema(tmpl *ansi.Template, decisions []arrowtype.Decision) *arrow.Schema {
	if tmpl == nil {
		return arrow.NewSchema(nil, nil)
	}
	params := tmpl.Params()
	byName := make(map[string]arrow.DataType, len(decisions))
	for _, d := range decisions {
		if d.Verdict == arrowtype.Typed {
			byName[strings.ToLower(d.Name)] = d.Field.Type
		}
	}
	fields := make([]arrow.Field, 0, len(params))
	for i, p := range params {
		dt, ok := byName[strings.ToLower(p.Column)]
		if !ok {
			// Unreachable from a lowered template -- an untyped column cannot carry a
			// placeholder, because its comparison is refused. Typed rather than
			// skipped so the field count still matches the placeholder count if that
			// ever stops being true.
			dt = arrow.BinaryTypes.String
		}
		fields = append(fields, arrow.Field{
			// Named for the column and its position, because a client showing a bind
			// error needs to know WHICH placeholder, and two may share a column.
			Name:     fmt.Sprintf("%s_%d", p.Column, i+1),
			Type:     dt,
			Nullable: true, // binding NULL is allowed; it drops the row
		})
	}
	return arrow.NewSchema(fields, nil)
}

// constraintFor lowers the query's WHERE clause to a ClassAd constraint, against the types the
// schema derivation declared. "true" -- everything -- when there is no predicate.
//
// The types are what make the lowering decidable: whether `=` on a column is the
// case-sensitive problem or an ordinary number is a question only the declared type answers.
func constraintFor(q query, decisions []arrowtype.Decision, bound *args) (string, error) {
	tmpl, err := templateFor(q, decisions)
	if err != nil {
		return "", err
	}
	if tmpl == nil {
		// No predicate at all. Values bound to it are a client bug worth naming rather
		// than ignoring: ignoring them would run a query that selects everything.
		if bound != nil && len(bound.vals) > 0 {
			return "", status.Errorf(codes.InvalidArgument,
				"%d value(s) were bound to a statement that has no parameters", len(bound.vals))
		}
		return "true", nil
	}
	if bound == nil {
		// The direct path, which has no way to supply a value -- so a placeholder is
		// refused here with ansi's own message, naming prepared statements.
		//
		// A PREPARED statement that has not been bound arrives with an empty args rather
		// than nil, so it falls past this to Bind's parameter-count check and is told to
		// bind its values. Same refusal either way; the one a client can act on differs.
		if n := len(tmpl.Params()); n > 0 {
			return "", fmt.Errorf("in the WHERE clause: %w", &ansi.Unsupported{
				Reason: fmt.Sprintf("this predicate has %d placeholder(s), and only a "+
					"prepared statement can supply a value for one", n)})
		}
		bound = &args{}
	}
	lowered, err := tmpl.Bind(bound.vals)
	if err != nil {
		return "", fmt.Errorf("in the WHERE clause: %w", err)
	}
	return lowered, nil
}

// args are the values bound to a prepared statement.
//
// A NIL args means a DIRECT statement, which is not the same as an empty one: a prepared
// statement with no placeholders binds nothing and is still prepared, and only the direct path
// refuses a `?`.
type args struct{ vals []any }

// templateFor lowers the WHERE clause to a template with its placeholders open. A nil template
// means the query has no predicate.
func templateFor(q query, decisions []arrowtype.Decision) (*ansi.Template, error) {
	if q.where == "" {
		return nil, nil
	}
	tmpl, err := ansi.LowerTemplate(q.where, columnsOf(decisions))
	if err != nil {
		return nil, fmt.Errorf("in the WHERE clause: %w", err)
	}
	return tmpl, nil
}

// columnsOf exposes the derivation's decisions as column kinds.
//
// The mapping lives in arrowtype because the REPL's ANSI dialect needs the same one, and two
// copies would let this endpoint and the REPL disagree about a column's type -- and so about
// what a query means.
func columnsOf(decisions []arrowtype.Decision) ansi.Columns {
	return arrowtype.Columns(decisions)
}

// hasTail reports whether these decisions include the tail column.
func hasTail(decisions []arrowtype.Decision) bool {
	for _, d := range decisions {
		if d.Tail {
			return true
		}
	}
	return false
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

// tableSchemaOf is schemaOf for a TABLE's schema, carrying the table's identity and schema
// generation in the Arrow schema's own metadata.
//
// It rides in the metadata because that is the one place every client already reads. A consumer
// that cached a catalog can tell "this is the schema I have" from "this moved" by comparing one
// integer, on a fetch it was making anyway -- no side call, no second endpoint to learn, and it
// survives the Arrow IPC stream to whatever reads the file at the other end.
//
// The generation answers a different question from the fingerprint (fingerprint.go). The
// fingerprint names a schema's CONTENT, which is what the pin needs: it agrees across processes
// and restarts and says nothing about history. The generation counts ADOPTED CHANGES to the
// table, which is what a person asking "did this change, and when" needs, and only exists
// because the endpoint now remembers the table's columns and therefore knows.
func tableSchemaOf(decisions []arrowtype.Decision, table string, generation int64) *arrow.Schema {
	base := schemaOf(decisions)
	md := arrow.MetadataFrom(map[string]string{
		"htcondordb.table":             table,
		"htcondordb.schema_generation": strconv.FormatInt(generation, 10),
		"htcondordb.schema_version":    arrowtype.Fingerprint(decisions),
	})
	return arrow.NewSchema(base.Fields(), &md)
}

// GetFlightInfoStatement answers with the schema BEFORE running anything, which is the whole
// reason the type measurement has to be separable from the scan: a client is entitled to know
// a result's shape without paying for its rows.
//
// The ticket carries the query text rather than a server-side handle. A handle would mean
// holding per-query state between two RPCs that may not even arrive on the same connection,
// and the query is what the handle would look up anyway.
func (s *Server) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	p, err := s.planStatement(ctx, cmd.GetQuery())
	if err != nil {
		return nil, err
	}
	// The handle carries the query AND the name of the schema just promised, so the fetch
	// can prove it is delivering that schema rather than a later measurement of a table
	// that moved in between (pin.go).
	ticket, err := flightsql.CreateStatementQueryTicket([]byte(pinnedHandle(p.fp, cmd.GetQuery())))
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(p.schema, s.alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// DoGetStatement scans the table and encodes it against the schema.
//
// The schema is derived again here rather than carried in the ticket. That is a second profile
// round trip, and it is the correct trade: a schema in the ticket would be a schema the client
// could edit, and this one has to match what the encoder will actually produce. What the ticket
// does carry is the NAME of the schema the client was promised, which it cannot usefully edit
// -- an invented name refuses its own request and nothing else (pin.go).
func (s *Server) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	fp, stmt := parsePinnedHandle(string(ticket.GetStatementHandle()))
	return s.doGetQuery(ctx, stmt, nil, fp)
}

// doGetQuery is the one scan implementation, shared by a direct statement and a prepared one.
// Two copies would drift, and the drift would be invisible: a prepared statement returning
// subtly different rows from the same SQL run directly is the kind of bug nobody reports
// because nobody compares.
func (s *Server) doGetQuery(ctx context.Context, stmt string, bound *args, pinned string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	parsed, perr := parseQuery(stmt)
	var an *analytic
	if perr != nil {
		// Same two-reading rule as the plan: a shape the scan cannot answer may still
		// be one the executor can (analytic.go).
		var aerr error
		if an, aerr = parseAnalytic(stmt); aerr != nil {
			if _, shape := aerr.(*unsupportedShape); shape {
				return nil, nil, aerr
			}
			return nil, nil, perr
		}
		parsed = query{table: an.st.Table, limit: an.st.Limit}
	}
	table := parsed.table
	// A stream slot first: refusing here costs nothing, while refusing after the schema
	// derivation would have spent a profile round trip on a request we decline anyway.
	slot, ok := s.acquireStream()
	if !ok {
		return nil, nil, status.Errorf(codes.ResourceExhausted,
			"too many concurrent Flight streams (limit %d); retry shortly", cap(s.streams))
	}
	db, private, release, err := s.dbFor(ctx)
	if err != nil {
		slot()
		return nil, nil, err
	}
	_, allDecisions, err := s.tableSchemaWith(ctx, db, table, private)
	if err != nil {
		release()
		slot()
		return nil, nil, err
	}
	var constraint string
	var decisions []arrowtype.Decision
	if an != nil {
		// A bound value cannot reach the executor: it parses the statement once and
		// holds a ClassAd predicate thereafter, so a placeholder would have to be
		// substituted before parsing and the schema is promised before that. The
		// prepare reports an EMPTY parameter schema, so a client that bound anything
		// bound it to a statement that said it takes none -- which is a client bug
		// worth naming rather than a value worth ignoring.
		if bound != nil && len(bound.vals) > 0 {
			release()
			slot()
			return nil, nil, status.Errorf(codes.InvalidArgument,
				"%d value(s) were bound to an aggregate or ordered statement, which takes "+
					"no parameters here; inline the value", len(bound.vals))
		}
		// The executor carries its own predicate, lowered by the ANSI dialect when it
		// runs, so there is no constraint to build here and the columns come from the
		// SELECT list rather than from the table.
		decisions, err = repl.ArrowDecisions(an.st, allDecisions)
	} else {
		constraint, err = constraintFor(parsed, allDecisions, bound)
		if err == nil {
			decisions, err = project(allDecisions, parsed.cols)
		}
	}
	if err != nil {
		release()
		slot()
		return nil, nil, err
	}
	// What was promised is what gets delivered, or nothing is. This is the last point at
	// which the two can still be compared: past it the encoder is built and the client is
	// reading buffers by position.
	if err := checkPin(pinned, decisions); err != nil {
		release()
		slot()
		return nil, nil, err
	}
	schema := schemaOf(decisions)
	// The tail is built from what the typed columns did NOT take, so it can only be filled
	// from a whole record: a pushed-down projection would deliver exactly the attributes
	// that are already columns and leave the tail empty in every row, which is a wrong
	// answer rather than a slow one. Asking for it therefore turns the projection off.
	cols := parsed.cols
	if hasTail(decisions) {
		cols = nil
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
		var scanErr error
		if an != nil {
			// One batch: an aggregate's whole result is materialized by the
			// executor before any of it can be correct, so there is nothing to
			// stream and a flush per batch would only split a small table.
			scanErr = s.runAnalytic(ctx, db, an, enc, decisions)
		} else {
			scanErr = s.scanInto(ctx, db, enc, table, constraint, cols, parsed.limit, flush)
		}
		if scanErr != nil {
			ch <- flight.StreamChunk{Err: scanErr}
			return
		}
		flush()
	}()
	return schema, ch, nil
}

// scanInto fills enc from the store, flushing a batch whenever one is full.
//
// TWO PRODUCERS, and which one runs is decided by whether the encoder has a tail column.
//
// The typed value stream is the one worth having: it evaluates in the store, where the record's
// reference closure and its precomputed values already are, and ships results column-major. The
// text projection renders every record to ClassAd source for this process to parse straight
// back, which measured as the large majority of a read's CPU -- work that is not work, only the
// cost of a relay between two halves of the same daemon.
//
// It cannot serve a TAIL, and that is a property of the op rather than a gap in the wiring: the
// value stream returns one column per REQUESTED attribute, as values, and the tail is the
// complement of a whole record in its stored form. Neither the set nor the form is available
// there. So SELECT * keeps the text path for as long as its schema carries a tail, and an
// explicit projection -- which is what a client aggregating or joining actually issues -- takes
// the fast one.
//
// No fallback for a server that lacks the opcode. Every Flight read goes through the in-process
// dbrpc loopback, so the server is this same binary; a fallback would be untestable code
// guarding an impossible case, which is the mistake the refs-chasing projection already
// documents here.
//
// redact is false because it asks for redaction BEYOND the session's: an unprivileged
// connection is redacted whatever it passes, and a privileged one is entitled to what it reads.
func (s *Server) scanInto(ctx context.Context, db *dbrpc.Client, enc *arrowtype.Encoder,
	table, constraint string, cols []string, limit int, flush func() bool) error {
	if !enc.HasTail() && len(enc.Attrs()) > 0 {
		vals := make([]classad.Value, len(enc.Attrs()))
		cells := make([][]classad.Value, len(enc.Attrs()))
		colv := make([]arrowtype.ColumnValues, len(enc.Attrs()))
		// ASK FOR DICTIONARY STRING COLUMNS. A pool's Owner, a site name, a status text:
		// storage holds these as a per-block dictionary and the plain wire form expanded
		// them to one string per row, on both sides. The decoder expands the dictionary
		// into the same slice, so nothing below this had to change except knowing the kind.
		//
		// It is a request, not a demand: against a server too old to advertise it the flag
		// is not set and the plain form arrives, which this reads just as well.
		return db.QueryValuesStreamOpts(ctx, table, constraint, enc.Attrs(), limit, false,
			dbrpc.ValueStreamOptions{Dictionary: true},
			func(b *dbrpc.ValueBatch) bool {
				// COLUMN-WISE where the values can go into the builders as they are: the
				// relay already carries dense typed arrays, and boxing each cell into a
				// classad.Value so the encoder can unbox it again is a round trip through
				// a representation neither end wanted.
				//
				// It refuses a batch that needs any conversion or any per-row decision,
				// and the per-cell path below takes it -- so the coercion rules stay in
				// one place and this is a shortcut rather than a second semantics.
				if len(b.Cols) == len(colv) {
					ok := true
					for col := range b.Cols {
						if !columnValuesOf(&b.Cols[col], b.Rows, &colv[col]) {
							ok = false
							break
						}
					}
					if ok {
						if took, _ := enc.AppendColumns(b.Rows, colv); took {
							return enc.Rows() < batchRows || flush()
						}
					}
				}
				// Column-major, one forward pass each, rather than ValueAt per cell.
				// ValueAt recounts the validity bitmap from row 0 every call, so reading a
				// whole batch through it is quadratic in the batch size -- measured at
				// 6257 ns/row against 2254 for this, on a read whose store side is 2213.
				// The entire difference was the accessor, not the work.
				for col := range b.Cols {
					cells[col] = unpackValueColumn(&b.Cols[col], b.Rows, cells[col][:0])
				}
				for row := range b.Rows {
					for col := range b.Cols {
						vals[col] = cells[col][row]
					}
					_ = enc.AppendValues(vals) // non-strict: counted, not fatal
					if enc.Rows() >= batchRows && !flush() {
						return false
					}
				}
				return true
			})
	}
	// The REFS-CHASING projection op, for three reasons that happen to coincide.
	//
	// It reaches an ARCHIVE. The plain query op does not -- it refuses one by design and says
	// so -- so reading `history`, the table most queries are about, used to fail after the
	// endpoint had already listed it and derived its schema.
	//
	// It PUSHES THE PROJECTION DOWN, inside the decode walk: a non-projected attribute is
	// never rendered, and a projection the hot header covers is served from the hot header
	// alone.
	//
	// And it chases the projected expressions' REFERENCES, which this encoder needs because
	// it EVALUATES. MemoryUsage projected without the siblings it is written over evaluates
	// to undefined, so the plain projection op would turn a perfectly good integer column
	// into nulls -- correct for a relay forwarding text, wrong for anything that evaluates.
	// cols is nil for SELECT *, which the op reads as "no projection": every attribute.
	return db.QueryRawProjectRefsStream(ctx, table, constraint, cols, limit, func(row string) bool {
		// The projection ops stream OLD-ClassAd text, not the bracketed form.
		ad, perr := classad.ParseOld(row)
		if perr != nil {
			return true
		}
		_ = enc.Append(ad) // non-strict: coercions and nulls are counted, not fatal
		if enc.Rows() >= batchRows {
			return flush()
		}
		return true
	})
}

// unpackValueColumn expands one packed column into per-row values in a single forward pass,
// carrying a running dense index and a running exception index rather than searching for each.
//
// It belongs upstream beside the batch it unpacks, and should be deleted when dbrpc offers a
// sequential reader; it lives here because the quadratic accessor is the only one that exists
// and the cost of using it is most of a read.
//
// dst is appended to and returned so the caller can reuse one slice per column across batches.
func unpackValueColumn(c *dbrpc.ValueColumn, rows int, dst []classad.Value) []classad.Value {
	dense, exc := 0, 0
	for row := range rows {
		if row>>3 >= len(c.Valid) || c.Valid[row>>3]&(1<<(uint(row)&7)) == 0 {
			// Exceptions are in ascending Row order, so the cursor only moves forward.
			for exc < len(c.Exceptions) && c.Exceptions[exc].Row < row {
				exc++
			}
			if exc < len(c.Exceptions) && c.Exceptions[exc].Row == row {
				dst = append(dst, c.Exceptions[exc].Value)
			} else {
				dst = append(dst, classad.NewUndefinedValue())
			}
			continue
		}
		switch c.Kind {
		case dbrpc.KindInt:
			dst = append(dst, classad.NewIntValue(c.Ints[dense]))
		case dbrpc.KindReal:
			dst = append(dst, classad.NewRealValue(c.Reals[dense]))
		case dbrpc.KindBool:
			dst = append(dst, classad.NewBoolValue(c.Bools[dense]))
		case dbrpc.KindString:
			dst = append(dst, classad.NewStringValue(c.Strs[dense]))
		default:
			dst = append(dst, classad.NewErrorValue())
		}
		dense++
	}
	return dst
}

// columnValuesOf renders one relay column as the encoder's dense form. ok=false when the column
// carries anything the fast path does not describe -- an exception that is a value rather than a
// null, or a kind with no dense array at all -- and the caller then takes the per-cell path.
//
// An exception that IS a null becomes one here: the relay sends undefined and error cells in the
// exception list, they are much the commonest thing in it, and refusing the batch over an absent
// attribute would give the fast path back on exactly the columns a projection asks for.
func columnValuesOf(c *dbrpc.ValueColumn, rows int, out *arrowtype.ColumnValues) bool {
	*out = arrowtype.ColumnValues{Valid: c.Valid, Nulls: out.Nulls[:0]}
	switch c.Kind {
	case dbrpc.KindInt:
		out.Ints = c.Ints
	case dbrpc.KindReal:
		out.Reals = c.Reals
	case dbrpc.KindBool:
		out.Bools = c.Bools
	case dbrpc.KindString, dbrpc.KindStringDict:
		// Both forms land in the same Strs slice: the dictionary is expanded at decode by
		// copying string HEADERS into it, so the only difference upstream is how many
		// distinct strings were ever allocated.
		out.Strs = c.Strs
	default:
		return false
	}
	for i := range c.Exceptions {
		x := &c.Exceptions[i]
		if x.NonScalar {
			return false
		}
		switch x.Value.Type() {
		case classad.UndefinedValue:
			out.Nulls = append(out.Nulls, arrowtype.NullCell{Row: x.Row, Cause: arrowtype.CauseUndefined})
		case classad.ErrorValue:
			out.Nulls = append(out.Nulls, arrowtype.NullCell{Row: x.Row, Cause: arrowtype.CauseError})
		default:
			// A scalar of another kind: it may coerce, it may not, and deciding that is
			// coerce's job rather than this function's.
			return false
		}
	}
	return true
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

// Settings are what an in-process endpoint's host configures. Zero values keep the behaviour
// the endpoint had before each setting existed.
type Settings struct {
	// StreamBudgetBytes caps the Arrow memory one DoGet stream may hold. Zero is
	// unlimited.
	StreamBudgetBytes int64
	// MaxStreams caps concurrent DoGet streams. Zero is unlimited.
	MaxStreams int
	// SchemaTTL is how long a derived table schema is reused. Zero derives one per RPC,
	// which is what this served before the cache existed; a host that wants the cache has
	// to ask for it, because the right answer depends on how fast its tables change.
	SchemaTTL time.Duration
	// ColumnTypeFile is where the endpoint remembers each table's measured column types
	// across restarts (columntypes.go). Empty remembers in memory for this process only,
	// which is what an in-memory store gets and is still enough to stop a schema moving
	// between two RPCs of one session. A path gets the half a cache cannot do: a column
	// keeps its position and its settled type after a restart, so a consumer's cached
	// catalog stays valid.
	ColumnTypeFile string
	// Warn reports a condition an operator should know about but that must not fail a
	// request -- an unwritable column-type file, a column kind a newer build wrote. Nil
	// discards them, which is what an embedder with no logger gets.
	//
	// Note reports an ordinary event worth seeing go past rather than a problem: a table's
	// schema generation moving because a column retyped. Separate from Warn because a host
	// logs the two at different levels, and a schema change logged as a warning is one an
	// operator learns to ignore.
	Warn func(string)
	Note func(string)
	// Version is the host's own version, reported to a client as
	// FLIGHT_SQL_SERVER_VERSION. Empty leaves the placeholder, which matters because
	// ADBC's adbc_get_info() asks for this entry by name and reads an unregistered one as
	// NOT_FOUND rather than as empty -- failing the whole call.
	Version string
}

// Limits is the former name of Settings, kept so the rename does not break a caller.
//
// Deprecated: use Settings.
type Limits = Settings

// NewSessionFlightServer is NewFlightServer for a service that resolves a per-identity session
// on every RPC. One Flight server serves every connection; what differs per RPC is the
// authorization its reads go through.
func NewSessionFlightServer(sessions *Sessions, alloc memory.Allocator, set Settings) (flight.FlightServer, error) {
	srv, err := NewSessionServer(sessions, sessions.Derive(), alloc)
	if err != nil {
		return nil, err
	}
	if err := applySettings(srv, set); err != nil {
		return nil, err
	}
	return NewFlightServer(srv, alloc), nil
}

// applySettings is separate, and tested separately, because a setting that parses and never
// reaches what it configures is a silent no-op: the host looks configured, every request keeps
// the old behaviour, and nothing says so. That has happened twice in this work -- a CLI flag
// and this struct's own Version field -- so the wiring is a named function with a test that
// reads every field back off the server rather than a sequence of calls inside a constructor
// whose return value hides them.
func applySettings(srv *Server, set Settings) error {
	srv.SetStreamBudget(set.StreamBudgetBytes)
	srv.SetMaxStreams(set.MaxStreams)
	srv.SetSchemaTTL(set.SchemaTTL)
	srv.SetColumnTypeMemory(set.ColumnTypeFile, set.Warn, set.Note)
	return srv.SetVersion(set.Version)
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
