package bench

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PelicanPlatform/classad/classad"
)

// The benchmark's Flight endpoint has to be configured the way a daemon's is, and the schema
// cache is the setting that decides the number.
//
// A schema is derived by SAMPLING the table, and a statement needs one twice -- GetFlightInfo
// answers the shape, DoGet builds the encoder -- so an endpoint without the cache pays two
// profile scans per query. Benchmark mode built its server through the constructor that applies
// no settings, and the Arrow arm reported 14.9 MB/s against the value stream's 240 on the same
// 200000 rows. With the cache asked for, the same sweep reports 226 against 224: the arm had
// been measuring schema derivation and reporting it as the cost of delivering Arrow.
//
// ASSERTED THROUGH THE CACHE'S OWN CONTRACT, which is that a cached schema is a STALE one: a
// column that appears in the table does not appear in the schema until the entry ages out. That
// needs no clock and no package internals.
//
// An endpoint with no cache re-derives, and does it TWICE PER STATEMENT -- so against a table
// that just changed it either answers with the new column or fails the fetch outright, because
// the schema it planned with is not the one it would encode with. Both are the same defect, and
// this treats them alike rather than asserting only the first.
func TestBenchmarkFlightAsksForTheSchemaCache(t *testing.T) {
	srv, _, err := Build(ServeConfig{
		Dir: filepath.Join(t.TempDir(), "scratch"), Rows: 300, Seed: 11,
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, err := srv.ServeFlight(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fc, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fc.Close() }()

	const probe = "BenchCacheProbe"
	const q = "SELECT * FROM jobs"
	first, err := schemaOfQuery(ctx, fc, q)
	if err != nil {
		t.Fatalf("the first query failed: %v", err)
	}
	if has(first, probe) {
		t.Fatalf("%s was in the schema before any ad carried it", probe)
	}

	// Enough of them that a sampler cannot miss the attribute: the point is that the schema
	// does not move, not that the attribute is rare.
	tbl, ok := srv.Tables[ShapeJobs]
	if !ok {
		t.Fatal("no jobs table")
	}
	for i := range 200 {
		ad, perr := classad.ParseOld(fmt.Sprintf(
			"ClusterId = %d\nOwner = \"probe\"\nJobStatus = 4\n%s = %d\n", 900000+i, probe, i))
		if perr != nil {
			t.Fatal(perr)
		}
		if err := tbl.Append(ad); err != nil {
			t.Fatal(err)
		}
	}

	second, err := schemaOfQuery(ctx, fc, q)
	switch {
	case err != nil:
		t.Errorf("the query failed once the table changed (%v), which is what an endpoint "+
			"that derives a schema per RPC does: the shape it planned with is not the one "+
			"it encodes with. It is not asking for the cache", err)
	case has(second, probe):
		t.Errorf("%s appeared in the schema straight after it was written, so the endpoint "+
			"derived a fresh schema: it is not asking for the cache, and every query pays "+
			"two profile scans", probe)
	}
}

func has(s *arrow.Schema, name string) bool { return s != nil && len(s.FieldIndices(name)) > 0 }

// schemaOfQuery runs the query and returns the schema the server answered with. It returns the
// error rather than failing, because a failed fetch is one of the two things this test is
// looking for.
func schemaOfQuery(ctx context.Context, fc *flightsql.Client, q string) (*arrow.Schema, error) {
	info, err := fc.Execute(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("executing %q: %w", q, err)
	}
	if len(info.Endpoint) == 0 {
		return nil, fmt.Errorf("query %q returned no endpoint", q)
	}
	rdr, err := fc.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, fmt.Errorf("fetching %q: %w", q, err)
	}
	defer rdr.Release()
	s := rdr.Schema()
	for rdr.Next() {
	}
	if rerr := rdr.Err(); rerr != nil {
		return s, fmt.Errorf("reading %q: %w", q, rerr)
	}
	return s, nil
}
