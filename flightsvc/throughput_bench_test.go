package flightsvc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// How many MB/s of query results Flight SQL can hand to a client.
//
// BenchmarkLocalVsFramedOverGRPC answers "which of the two server-side paths is cheaper per
// row". This answers the different question of how fast results LEAVE, which needs bytes
// rather than rows: the per-row cost says nothing about the rate a consumer sees without
// knowing how wide a row is, and an Arrow row of six columns is not comparable to an
// old-ClassAd ad of forty-five attributes except as bytes per second.
//
// Bytes are counted on the SOCKET, server side, so the number includes Arrow IPC framing and
// gRPC's own framing -- everything the consumer actually has to receive. Counting the Arrow
// buffers instead would report a rate no client can observe.

// countingListener counts every byte the server writes to an accepted connection.
type countingListener struct {
	net.Listener
	n *atomic.Int64
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: c, n: l.n}, nil
}

type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.n.Add(int64(n))
	return n, err
}

// BenchmarkArrowResultThroughput reports the delivered rate for both server-side paths, at
// several result sizes because the per-RPC session setup is a fixed cost a small read pays in
// full.
func BenchmarkArrowResultThroughput(b *testing.B) {
	for _, rows := range []int{2000, 20000, 200000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			cat, cleanup := convergedBenchCatalog(b, rows)
			defer cleanup()
			ctx := context.Background()
			q := "SELECT " + strings.Join(benchProjection, ", ") + " FROM history"

			for _, arm := range []struct {
				name  string
				local bool
			}{{"framed", false}, {"local", true}} {
				b.Run(arm.name, func(b *testing.B) {
					srv := dbrpc.NewServerCatalog(cat)
					defer srv.Close()
					var catalog Catalog = srv
					if !arm.local {
						catalog = framedOnlyCatalog{srv}
					}
					sessions, err := NewSessions(catalog, fixedAuth{user: "alice"},
						allowLevels("alice", "READ"), benchDerive)
					if err != nil {
						b.Fatal(err)
					}
					svc, err := NewSessionServer(sessions, benchDerive, nil)
					if err != nil {
						b.Fatal(err)
					}
					svc.SetSchemaTTL(time.Hour)
					// Each arm proves which path it is on before the timer runs, exactly as
					// the per-row benchmark does; without it a local arm that fell through
					// reports the framed path's rate under the local name.
					assertArmTakesItsPath(b, svc, sessions, arm.local)

					var wire atomic.Int64
					rawLn, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						b.Fatal(err)
					}
					ln := countingListener{Listener: rawLn, n: &wire}
					gsrv := grpc.NewServer()
					flight.RegisterFlightServiceServer(gsrv, NewFlightServer(svc, nil))
					go func() { _ = gsrv.Serve(ln) }()
					defer gsrv.Stop()
					fs, err := flightsql.NewClient(rawLn.Addr().String(), nil, nil,
						grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = fs.Close() }()

					drain := func() {
						info, derr := fs.Execute(ctx, q)
						if derr != nil {
							b.Fatal(derr)
						}
						rdr, derr := fs.DoGet(ctx, info.Endpoint[0].Ticket)
						if derr != nil {
							b.Fatal(derr)
						}
						n := 0
						for rdr.Next() {
							n += int(rdr.Record().NumRows())
						}
						if rerr := rdr.Err(); rerr != nil {
							b.Fatal(rerr)
						}
						rdr.Release()
						if n != rows {
							b.Fatalf("%d rows over gRPC, want %d", n, rows)
						}
					}
					// One warm pass: the archive is mmap'd and the first read pays page-in
					// that no later read pays, and the schema derivation caches on first use.
					drain()
					wire.Store(0)

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						drain()
					}
					b.StopTimer()
					got := wire.Load()
					b.SetBytes(got / int64(b.N))
					b.ReportMetric(float64(got)/b.Elapsed().Seconds()/(1<<20), "MB/s")
					b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
					b.ReportMetric(float64(got)/float64(rows)/float64(b.N), "B/row")
				})
			}
		})
	}
}

// convergedBenchCatalog is localBenchCatalog with the columnar form CONVERGED.
//
// A schema-scan pass columnarizes a bounded number of sealed segments -- 64 by default, so that
// enabling the accelerator over an existing archive does not stall on hours of I/O -- and
// localBenchCatalog calls it once. On a table past that budget one call leaves most records
// being rebuilt from the arena on every read: measured at 87% on a 478-segment table, while the
// accelerator reported every one of those segments covered.
//
// Every earlier number from this benchmark described that unconverged state and not Arrow. The
// assertion below is what makes the difference visible rather than assumed, since the coverage
// figure does not show it.
func convergedBenchCatalog(b *testing.B, rows int) (*db.Catalog, func()) {
	b.Helper()
	cat, cleanup := localBenchCatalog(b, rows)
	arch, ok := cat.ArchiveTable("history")
	if !ok {
		cleanup()
		b.Fatal("the fixture has no history table")
	}
	sealed := arch.SchemaScanInfo().SealedSegments
	for pass := 0; pass < sealed/64+2; pass++ {
		if !arch.BuildAndEnableSchemaScan(2000, 8) {
			break
		}
	}
	var conv collections.ScanStats
	seq, cerr := arch.QueryProjectStats("true", benchProjection, &conv)
	if cerr != nil {
		cleanup()
		b.Fatal(cerr)
	}
	for range seq {
	}
	if conv.RecordsVisited == 0 {
		cleanup()
		b.Fatal("the convergence probe visited nothing")
	}
	esc := float64(conv.RecordsReassembled) / float64(conv.RecordsVisited)
	b.Logf("fixture: %d rows, %d sealed segments, %.2f%% of records still reassembled", rows, sealed, 100*esc)
	// The ACTIVE segment is never columnarized, so a small table has an irreducible residue --
	// at 4 sealed segments it was 15%, which is the active segment's share and not a failure to
	// converge. The assertion is for tables large enough that the sealed segments dominate,
	// which are the ones where one pass is not enough and the measurement goes wrong.
	if sealed >= 16 && esc > 0.05 {
		cleanup()
		b.Fatalf("the fixture did not converge: %.1f%% rebuilt from the arena over %d sealed segments, so this would measure the record path",
			100*esc, sealed)
	}
	return cat, cleanup
}
