package flightsvc

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// paramRec builds a one-row parameter batch against a schema. vals holds one Go value per
// field; a nil binds NULL.
func paramRec(t *testing.T, schema *arrow.Schema, vals ...any) arrow.RecordBatch {
	t.Helper()
	if schema.NumFields() != len(vals) {
		t.Fatalf("the schema has %d fields and %d values were given", schema.NumFields(), len(vals))
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bld.Release()
	for i, v := range vals {
		f := bld.Field(i)
		if v == nil {
			f.AppendNull()
			continue
		}
		switch b := f.(type) {
		case *array.Int64Builder:
			b.Append(v.(int64))
		case *array.Int32Builder:
			b.Append(v.(int32))
		case *array.Float64Builder:
			b.Append(v.(float64))
		case *array.StringBuilder:
			b.Append(v.(string))
		case *array.BooleanBuilder:
			b.Append(v.(bool))
		case *array.TimestampBuilder:
			b.Append(arrow.Timestamp(v.(int64)))
		default:
			t.Fatalf("field %d is a %T, which this helper does not build", i, f)
		}
	}
	return bld.NewRecordBatch()
}

// TestBoundParameterFiltersRows is the path an ADBC client takes for `cursor.execute(sql,
// params)`: prepare, bind, fetch.
func TestBoundParameterFiltersRows(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner, ClusterId FROM `+dbrpc.DefaultTable+` WHERE Owner = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close(ctx)

	// The parameter schema is known from preparing, and typed from the column -- which is
	// what lets a client build a binding at all.
	ps := stmt.ParameterSchema()
	if ps == nil || ps.NumFields() != 1 {
		t.Fatalf("parameter schema %v, want 1 field", ps)
	}
	if ps.Field(0).Type.ID() != arrow.STRING {
		t.Errorf("parameter type %v, want utf8 for a string column", ps.Field(0).Type)
	}
	if !strings.HasPrefix(ps.Field(0).Name, "Owner") {
		t.Errorf("parameter named %q; it should name the column it binds against", ps.Field(0).Name)
	}

	rec := paramRec(t, ps, "user1")
	defer rec.Release()
	stmt.SetParameters(rec)
	info, err := stmt.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// The seed writes Owner = user0..user3 over 40 rows, so a quarter match.
	if n := drain(t, cl, info.Endpoint[0].Ticket.Ticket); n != 10 {
		t.Errorf("read %d rows, want 10", n)
	}
}

// TestBoundParameterEqualsInlineOverTheWire: binding a value must select exactly the rows
// writing it inline selects. The ansi package proves this about the lowered text; this proves it
// about the rows that come back, through the real client and the real store.
func TestBoundParameterEqualsInlineOverTheWire(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	inline, err := cl.Execute(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable+` WHERE ClusterId >= 1025`)
	if err != nil {
		t.Fatal(err)
	}
	want := drain(t, cl, inline.Endpoint[0].Ticket.Ticket)
	if want == 0 {
		t.Fatal("the inline query returned nothing; the comparison would prove nothing")
	}

	stmt, err := cl.Prepare(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable+` WHERE ClusterId >= ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	rec := paramRec(t, stmt.ParameterSchema(), int64(1025))
	defer rec.Release()
	stmt.SetParameters(rec)
	info, err := stmt.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, cl, info.Endpoint[0].Ticket.Ticket); got != want {
		t.Errorf("the bound query returned %d rows, the inline one %d", got, want)
	}
}

// TestRebindingChangesTheRows: a statement reused with new values must answer the new query. A
// server that cached the first binding would return the previous execution's rows, which looks
// like a result and is not one.
func TestRebindingChangesTheRows(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable+` WHERE ClusterId >= ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	ps := stmt.ParameterSchema()

	counts := map[int64]int{}
	for _, bound := range []int64{1000, 1020, 1039, 9999} {
		rec := paramRec(t, ps, bound)
		stmt.SetParameters(rec)
		info, err := stmt.Execute(ctx)
		rec.Release()
		if err != nil {
			t.Fatalf("binding %d: %v", bound, err)
		}
		counts[bound] = drain(t, cl, info.Endpoint[0].Ticket.Ticket)
	}
	// ClusterId runs 1000..1039, so these are known quantities, not just "different".
	for bound, want := range map[int64]int{1000: 40, 1020: 20, 1039: 1, 9999: 0} {
		if counts[bound] != want {
			t.Errorf("bound %d returned %d rows, want %d", bound, counts[bound], want)
		}
	}
}

// TestUnboundPlaceholderIsRefused is the one that matters most. Running a parameterised
// statement with nothing bound would select every row and look exactly like a successful query
// -- so it must fail instead.
//
// The refusal may land at the plan or at the fetch, and this does not care which. It does care
// that one HAPPENS and that it is about the parameters: an earlier version returned early on any
// error, so it would have passed on a refusal that came from somewhere unrelated.
func TestUnboundPlaceholderIsRefused(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable+` WHERE Owner = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)

	refusal := func() error {
		info, err := stmt.Execute(ctx) // no SetParameters
		if err != nil {
			return err
		}
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			return err
		}
		defer rdr.Release()
		rows := 0
		for rdr.Next() {
			rows += int(rdr.Record().NumRows())
		}
		if err := rdr.Err(); err != nil {
			return err
		}
		t.Fatalf("an unbound parameterised statement returned %d rows; it must be refused, "+
			"because selecting everything looks like a successful query", rows)
		return nil
	}()
	if refusal == nil {
		t.Fatal("no refusal")
	}
	if !strings.Contains(refusal.Error(), "parameter") {
		t.Errorf("the refusal %q is not about the unbound parameters", refusal)
	}
}

// TestDirectStatementWithAPlaceholderIsRefused: the direct path has no way to supply a value, so
// a `?` must be refused with a message that says where placeholders DO work. The parameter-count
// check would also stop this statement, but it would report that nothing was bound -- which to
// someone who never tried to bind reads as a bug in the server. The wording is asserted here so
// the guard that produces it cannot go dead.
func TestDirectStatementWithAPlaceholderIsRefused(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()
	const q = `SELECT Owner FROM ` + dbrpc.DefaultTable + ` WHERE Owner = ?`

	refusal := func() error {
		info, err := cl.Execute(ctx, q)
		if err != nil {
			return err
		}
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			return err
		}
		defer rdr.Release()
		for rdr.Next() {
		}
		if err := rdr.Err(); err != nil {
			return err
		}
		t.Fatal("a direct statement containing a placeholder returned rows")
		return nil
	}()
	if refusal == nil {
		t.Fatal("no refusal")
	}
	if !strings.Contains(refusal.Error(), "prepared statement") {
		t.Errorf("the refusal %q does not say that placeholders need a prepared statement", refusal)
	}
}

// TestBoundNullReturnsNothing: SQL compares nothing to NULL successfully, so the result is
// empty.
//
// The predicates are chosen so that a NULL read as its type's ZERO VALUE would return every row
// instead of none. Binding NULL to `Owner = ?` would not show that: an empty string matches no
// Owner in the seed, so it returns nothing either way, and an earlier version of this test
// passed with the null check removed.
func TestBoundNullReturnsNothing(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	for _, q := range []string{
		// ClusterId runs 1000..1039, so `>= 0` would admit all 40.
		`SELECT Owner FROM ` + dbrpc.DefaultTable + ` WHERE ClusterId >= ?`,
		// QDate likewise: every row is after the epoch.
		`SELECT Owner FROM ` + dbrpc.DefaultTable + ` WHERE QDate > ?`,
		// And the string case, for completeness.
		`SELECT Owner FROM ` + dbrpc.DefaultTable + ` WHERE Owner = ?`,
	} {
		stmt, err := cl.Prepare(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		rec := paramRec(t, stmt.ParameterSchema(), nil)
		stmt.SetParameters(rec)
		info, err := stmt.Execute(ctx)
		rec.Release()
		if err != nil {
			stmt.Close(ctx)
			t.Errorf("%s: %v", q, err)
			continue
		}
		if n := drain(t, cl, info.Endpoint[0].Ticket.Ticket); n != 0 {
			t.Errorf("%s: a NULL binding returned %d rows, want 0", q, n)
		}
		stmt.Close(ctx)
	}
}

// TestTimestampParameterBindsAsTheColumnReads: a client binds what it would read. QDate is
// declared a time attribute, so it reads as timestamp[s] and must bind as one -- not as the
// integer underneath, which the client has no reason to know about.
func TestTimestampParameterBindsAsTheColumnReads(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT QDate FROM `+dbrpc.DefaultTable+` WHERE QDate > ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	ps := stmt.ParameterSchema()
	if ps.NumFields() != 1 || ps.Field(0).Type.ID() != arrow.TIMESTAMP {
		t.Fatalf("parameter type %v, want a timestamp", ps.Field(0).Type)
	}

	// The seed writes QDate = 1700000000+i for i in 0..39.
	rec := paramRec(t, ps, int64(1700000010))
	defer rec.Release()
	stmt.SetParameters(rec)
	info, err := stmt.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := drain(t, cl, info.Endpoint[0].Ticket.Ticket); n != 29 {
		t.Errorf("read %d rows, want 29 (i = 11..39)", n)
	}
}

// TestBadBindIsRefusedAtTheBind, not at the fetch: a client learns its values are wrong at the
// call that sent them, which is the only moment it can still say which ones.
func TestBadBindIsRefusedAtTheBind(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable+` WHERE ClusterId = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)

	// A string where the column holds a number. Built against a schema of our own, since
	// the point is that the server checks rather than trusting the declared schema.
	wrong := arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	rec := paramRec(t, wrong, "not a number")
	defer rec.Release()
	stmt.SetParameters(rec)
	if _, err := stmt.Execute(ctx); err == nil {
		t.Fatal("a string bound to a numeric column was accepted")
	} else if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code %v, want InvalidArgument", got)
	}

	// Too many values.
	two := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	rec2 := paramRec(t, two, int64(1), int64(2))
	defer rec2.Release()
	stmt.SetParameters(rec2)
	if _, err := stmt.Execute(ctx); err == nil {
		t.Fatal("two values bound to a one-placeholder statement were accepted")
	} else if !strings.Contains(err.Error(), "takes 1 parameter") {
		t.Errorf("error %q does not say how many parameters the statement takes", err)
	}
}

// TestBindToAStatementWithNoParametersIsRefused: ignoring the values would run a query that
// selects everything, which is not what a client that bothered to bind asked for.
func TestBindToAStatementWithNoParametersIsRefused(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	if ps := stmt.ParameterSchema(); ps == nil || ps.NumFields() != 0 {
		t.Errorf("parameter schema %v, want an empty one", ps)
	}
	one := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	rec := paramRec(t, one, int64(1))
	defer rec.Release()
	stmt.SetParameters(rec)
	if _, err := stmt.Execute(ctx); err == nil {
		t.Error("a value bound to a statement with no parameters was accepted")
	}
}

// TestSeveralParameterSetsAreRefused: a SELECT with N parameter sets is N queries, and a DoGet
// returns one result set. Running the first and dropping the rest would answer a different
// question than the one asked.
func TestSeveralParameterSetsAreRefused(t *testing.T) {
	cl, _, done := harness(t, 20)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable+` WHERE ClusterId = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)

	ps := stmt.ParameterSchema()
	bld := array.NewRecordBuilder(memory.DefaultAllocator, ps)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).AppendValues([]int64{1000, 1001}, nil)
	rec := bld.NewRecordBatch()
	defer rec.Release()
	stmt.SetParameters(rec)
	if _, err := stmt.Execute(ctx); err == nil {
		t.Error("two parameter sets were accepted for a SELECT")
	} else if !strings.Contains(err.Error(), "one result set") {
		t.Errorf("error %q does not explain why several sets cannot be served", err)
	}
}

// TestMultipleParametersBindInOrder: positional binding means source order, and a server that
// mixed two placeholders up would return plausible rows for the wrong query.
func TestMultipleParametersBindInOrder(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx,
		`SELECT Owner, ClusterId FROM `+dbrpc.DefaultTable+` WHERE Owner = ? AND ClusterId >= ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	ps := stmt.ParameterSchema()
	if ps.NumFields() != 2 {
		t.Fatalf("%d parameters, want 2", ps.NumFields())
	}
	if ps.Field(0).Type.ID() != arrow.STRING || ps.Field(1).Type.ID() != arrow.INT64 {
		t.Fatalf("parameter types %v, %v; want utf8 then int64 -- the order the placeholders appear",
			ps.Field(0).Type, ps.Field(1).Type)
	}
	rec := paramRec(t, ps, "user1", int64(1020))
	defer rec.Release()
	stmt.SetParameters(rec)
	info, err := stmt.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Owner = user1 is i%4 == 1, so i = 1,5,...,37; ClusterId >= 1020 is i >= 20; both is
	// i = 21,25,29,33,37.
	if n := drain(t, cl, info.Endpoint[0].Ticket.Ticket); n != 5 {
		t.Errorf("read %d rows, want 5", n)
	}
}

// TestBoundStringIsCaseSensitiveOverTheWire: the divergence the ansi package exists for, reached
// through a bound parameter and the real store rather than through an evaluated expression.
func TestBoundStringIsCaseSensitiveOverTheWire(t *testing.T) {
	cl, _, done := harness(t, 40)
	defer done()
	ctx := context.Background()

	stmt, err := cl.Prepare(ctx, `SELECT Owner FROM `+dbrpc.DefaultTable+` WHERE Owner = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	ps := stmt.ParameterSchema()

	for _, c := range []struct {
		bind string
		want int
	}{{"user1", 10}, {"USER1", 0}, {"User1", 0}} {
		rec := paramRec(t, ps, c.bind)
		stmt.SetParameters(rec)
		info, err := stmt.Execute(ctx)
		rec.Release()
		if err != nil {
			t.Fatalf("binding %q: %v", c.bind, err)
		}
		if n := drain(t, cl, info.Endpoint[0].Ticket.Ticket); n != c.want {
			t.Errorf("binding %q returned %d rows, want %d -- SQL's = does not fold case",
				c.bind, n, c.want)
		}
	}
}

// TestTimestampIsRescaledNotReinterpreted: the parameter schema says timestamp[s], but a client
// is free to send another unit and some drivers do. A millisecond count compared against a
// second count matches nothing, which looks like an empty result rather than a bug -- so the
// value is rescaled.
func TestTimestampIsRescaledNotReinterpreted(t *testing.T) {
	const sec = int64(1700000010)
	for _, c := range []struct {
		unit arrow.TimeUnit
		v    int64
	}{
		{arrow.Second, sec},
		{arrow.Millisecond, sec * 1e3},
		{arrow.Microsecond, sec * 1e6},
		{arrow.Nanosecond, sec * 1e9},
	} {
		dt := &arrow.TimestampType{Unit: c.unit}
		bld := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
		bld.Append(arrow.Timestamp(c.v))
		arr := bld.NewArray()
		got, err := scalarAt(arr, 0)
		arr.Release()
		bld.Release()
		if err != nil {
			t.Errorf("%v: %v", c.unit, err)
			continue
		}
		if got != sec {
			t.Errorf("a %v timestamp of %d read as %v, want %d epoch seconds",
				c.unit, c.v, got, sec)
		}
	}
	// A sub-second part truncates toward zero rather than rounding: the attribute has no
	// sub-second part, so rounding would move a predicate's boundary by whichever way the
	// fraction happened to fall.
	dt := &arrow.TimestampType{Unit: arrow.Millisecond}
	bld := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
	bld.Append(arrow.Timestamp(sec*1e3 + 999))
	arr := bld.NewArray()
	defer arr.Release()
	defer bld.Release()
	got, err := scalarAt(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != sec {
		t.Errorf("%d.999 read as %v, want %d -- truncated, not rounded", sec, got, sec)
	}
}

// TestEmptyParameterStreamIsRefused: a client that sent no rows has not told us its values, and
// running the statement unparameterised would return rows that look like an answer.
func TestEmptyParameterStreamIsRefused(t *testing.T) {
	if _, err := readBoundParameters(nil); err == nil {
		t.Error("a nil parameter stream was accepted")
	} else if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code %v, want InvalidArgument", got)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	for _, c := range []struct {
		name string
		recs []arrow.RecordBatch
	}{
		{"no batches at all", nil},
		{"a batch with no rows", []arrow.RecordBatch{emptyBatch(t, schema)}},
	} {
		rdr := &fakeMessageReader{schema: schema, recs: c.recs}
		_, err := readBoundParameters(rdr)
		for _, r := range c.recs {
			r.Release()
		}
		if err == nil {
			t.Errorf("%s: accepted, want a refusal", c.name)
		} else if !strings.Contains(err.Error(), "row") {
			t.Errorf("%s: error %q does not say what was missing", c.name, err)
		}
	}
}

// TestUnbindableValueTypeIsNamed: a type this endpoint cannot bind must say so rather than
// become something else.
func TestUnbindableValueTypeIsNamed(t *testing.T) {
	bld := array.NewDate32Builder(memory.DefaultAllocator)
	bld.Append(19000)
	arr := bld.NewArray()
	defer arr.Release()
	defer bld.Release()
	if _, err := scalarAt(arr, 0); err == nil {
		t.Error("a date32 was bound")
	} else if !strings.Contains(err.Error(), "date32") {
		t.Errorf("error %q does not name the type it could not bind", err)
	}
}

func emptyBatch(t *testing.T, schema *arrow.Schema) arrow.RecordBatch {
	t.Helper()
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bld.Release()
	return bld.NewRecordBatch()
}

// fakeMessageReader replays a fixed list of batches. The flightsql client cannot be made to send
// an empty stream, so the reader is faked to reach the path that handles one.
type fakeMessageReader struct {
	schema *arrow.Schema
	recs   []arrow.RecordBatch
	i      int
}

func (f *fakeMessageReader) Retain()                                          {}
func (f *fakeMessageReader) Release()                                         {}
func (f *fakeMessageReader) Schema() *arrow.Schema                            { return f.schema }
func (f *fakeMessageReader) Next() bool                                       { f.i++; return f.i <= len(f.recs) }
func (f *fakeMessageReader) RecordBatch() arrow.RecordBatch                   { return f.recs[f.i-1] }
func (f *fakeMessageReader) Record() arrow.RecordBatch                        { return f.recs[f.i-1] }
func (f *fakeMessageReader) Err() error                                       { return nil }
func (f *fakeMessageReader) Chunk() flight.StreamChunk                        { return flight.StreamChunk{} }
func (f *fakeMessageReader) LatestFlightDescriptor() *flight.FlightDescriptor { return nil }
func (f *fakeMessageReader) LatestAppMetadata() []byte                        { return nil }

// Read satisfies arrio.Reader, which MessageReader embeds. readBoundParameters iterates with
// Next/Record, so this is here only to satisfy the interface.
func (f *fakeMessageReader) Read() (arrow.RecordBatch, error) {
	if !f.Next() {
		return nil, io.EOF
	}
	return f.RecordBatch(), nil
}
