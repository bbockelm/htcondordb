package bench

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The Flight endpoint serves the generated data to an unauthenticated client.
func TestFlightServesAnonymously(t *testing.T) {
	srv, _, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"),
		Rows: 3000, Shapes: []Shape{ShapeHistory}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr, err := srv.ServeFlight(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := flightsql.NewClient(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fs.Close() }()
	ctx := context.Background()
	info, err := fs.Execute(ctx, "SELECT ClusterId, Owner, JobStatus FROM history")
	if err != nil {
		t.Fatalf("anonymous Execute refused: %v", err)
	}
	rdr, err := fs.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("anonymous DoGet refused: %v", err)
	}
	defer rdr.Release()
	rows := 0
	for rdr.Next() {
		rows += int(rdr.RecordBatch().NumRows())
	}
	if rerr := rdr.Err(); rerr != nil {
		t.Fatal(rerr)
	}
	if rows != 3000 {
		t.Fatalf("%d rows over Flight, want 3000", rows)
	}
}

// The expression-valued attribute must have a typed, POPULATED Arrow column.
//
// MemoryUsage is an expression in every generated ad, as in almost every real one, and a typed
// consumer cannot receive an expression -- so if it arrived as nulls, or not at all, every Arrow
// measurement would be of a narrower result than a real consumer asks for while being reported
// as the consumer's. The profile evaluates, so this holds without the declared override; the
// override is pinned separately below for what it does change.
func TestFlightTypesTheExpressionValuedAttribute(t *testing.T) {
	srv, _, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"),
		Rows: 2000, Shapes: []Shape{ShapeHistory}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr, err := srv.ServeFlight(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := flightsql.NewClient(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fs.Close() }()
	ctx := context.Background()
	info, err := fs.Execute(ctx, "SELECT ClusterId, MemoryUsage FROM history")
	if err != nil {
		t.Fatalf("selecting the expression-valued attribute failed: %v", err)
	}
	rdr, err := fs.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	schema := rdr.Schema()
	var names []string
	for _, f := range schema.Fields() {
		names = append(names, strings.ToLower(f.Name))
	}
	found := false
	var kind string
	for i, n := range names {
		if n == "memoryusage" {
			found = true
			kind = schema.Field(i).Type.String()
		}
	}
	if !found {
		t.Fatalf("MemoryUsage has no Arrow column; schema = %v. The derive overrides are not reaching the server", names)
	}
	if !strings.Contains(kind, "int") {
		t.Errorf("MemoryUsage typed as %s, want an integer column", kind)
	}
	rows := 0
	nonNull := 0
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rows += int(rec.NumRows())
		col := rec.Column(1)
		for i := 0; i < col.Len(); i++ {
			if col.IsValid(i) {
				nonNull++
			}
		}
	}
	if rerr := rdr.Err(); rerr != nil {
		t.Fatal(rerr)
	}
	if rows != 2000 {
		t.Fatalf("%d rows, want 2000", rows)
	}
	// A column of nulls would satisfy "the column exists" while carrying nothing.
	if nonNull != rows {
		t.Fatalf("MemoryUsage was null in %d of %d rows; the column exists but the expression was not evaluated",
			rows-nonNull, rows)
	}
}

// The TimeAttrs declaration is the half of the derive options that changes the schema: an
// epoch-second attribute is an integer to the profile, and only a declaration makes it a
// timestamp. A consumer's date functions have nothing to work on otherwise.
//
// This is the assertion with a live mutation behind it -- dropping TimeAttrs fails it -- where
// asserting the MemoryUsage override does not, because the profile produces that column anyway.
func TestFlightDeclaresTimestampColumns(t *testing.T) {
	srv, _, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"),
		Rows: 2000, Shapes: []Shape{ShapeHistory}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr, err := srv.ServeFlight(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := flightsql.NewClient(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fs.Close() }()
	info, err := fs.Execute(context.Background(), "SELECT CompletionDate, QDate FROM history")
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := fs.DoGet(context.Background(), info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	for _, f := range rdr.Schema().Fields() {
		if !strings.Contains(strings.ToLower(f.Type.String()), "timestamp") {
			t.Errorf("%s arrived as %s, want a timestamp; the TimeAttrs declaration is not reaching the server",
				f.Name, f.Type)
		}
	}
}
