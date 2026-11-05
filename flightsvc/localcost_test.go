package flightsvc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// What the in-process column path is worth to a client, over a real socket.
//
// Measured anywhere short of the endpoint this is easy to overstate. The scan, the Arrow encode
// and the client's decode OVERLAP, so a saving on one of them is only worth what it is worth to
// whichever is the longest -- and a delivery-only benchmark cannot see that. Both arms here are
// a Flight SQL client calling Execute and DoGet against a gRPC server on a TCP socket, draining
// every record, so the number is the one a caller waits for.
//
// The arms differ in ONE thing: whether the catalog the server was built on offers a
// LocalSession. That is exactly the production difference -- openSession takes the in-process
// path when its catalog has one -- rather than a switch that exists for the benchmark.

// framedOnlyCatalog is a catalog WITHOUT the optional in-process half. It cannot embed the
// server, because embedding would promote LocalSession and defeat the whole point.
type framedOnlyCatalog struct{ srv *dbrpc.Server }

func (c framedOnlyCatalog) ServeConnOpts(conn dbrpc.MsgConn, opts dbrpc.ServeOptions) error {
	return c.srv.ServeConnOpts(conn, opts)
}

// localBenchCatalog is the columnar archive both arms read. Columnar deliberately: an archive
// whose segments have not been rewritten is read from records either way, and both arms would
// measure the same path.
func localBenchCatalog(b *testing.B, rows int) (*db.Catalog, func()) {
	b.Helper()
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId", "JobStatus"}, ZoneAttrs: []string{"CompletionDate"},
		// The same size benchArchive uses, so this number can be read beside
		// BenchmarkReadStages' columnar arm rather than only against itself.
		SegmentSize: 256 << 10,
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		// No sparse attribute here, unlike the correctness fixture. An attribute too rare
		// for the derived columnar schema is read from the RECORDS for every row, which
		// drags the whole read back to the record path and measures that instead -- the
		// first draft of this benchmark did exactly that and reported 18120 ns/row.
		ad, perr := classad.ParseOld(benchAd(i))
		if perr != nil {
			b.Fatal(perr)
		}
		if aerr := arch.Append(ad); aerr != nil {
			b.Fatal(aerr)
		}
	}
	if !arch.BuildAndEnableSchemaScan(2000, 8) {
		b.Fatal("nothing to sample; the columnar accelerator did not build")
	}
	info := arch.SchemaScanInfo()
	if !info.Enabled || info.SealedSegments == 0 || info.CoveredSegments < info.SealedSegments {
		b.Fatalf("columnar accelerator not covering the data: enabled=%v sealed=%d covered=%d; "+
			"both arms would measure the record path", info.Enabled, info.SealedSegments,
			info.CoveredSegments)
	}
	return cat, func() { cat.Close() }
}

func BenchmarkLocalVsFramedOverGRPC(b *testing.B) {
	// Several sizes, because the two arms do not scale alike. The session machinery is per
	// RPC -- a fresh loopback pipe, its serve goroutine and a client, built whether or not
	// anything crosses it -- so it is a fixed cost that a small read pays in full and a large
	// one amortizes away. A single large size reports the best case for both arms and hides
	// which one a short interactive query actually gets.
	for _, rows := range []int{200, 2000, 20000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			benchLocalVsFramed(b, rows)
		})
	}
}

func benchLocalVsFramed(b *testing.B, rows int) {
	cat, cleanup := localBenchCatalog(b, rows)
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
			// On, because that is how it serves: an uncached derivation puts a
			// sampling scan inside every iteration and swamps what this measures.
			svc.SetSchemaTTL(time.Hour)
			assertArmTakesItsPath(b, svc, sessions, arm.local)

			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			gsrv := grpc.NewServer()
			flight.RegisterFlightServiceServer(gsrv, NewFlightServer(svc, nil))
			go func() { _ = gsrv.Serve(ln) }()
			defer gsrv.Stop()
			fs, err := flightsql.NewClient(ln.Addr().String(), nil, nil,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				b.Fatal(err)
			}
			defer fs.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				info, err := fs.Execute(ctx, q)
				if err != nil {
					b.Fatal(err)
				}
				rdr, err := fs.DoGet(ctx, info.Endpoint[0].Ticket)
				if err != nil {
					b.Fatal(err)
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
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(rows), "ns/row")
		})
	}
}

// assertArmTakesItsPath makes each arm prove which path it is measuring, BEFORE the timer runs.
//
// Without it the comparison is unfalsifiable: a local arm that fell through to the framed path --
// because the table declined a column scan, or a batch held something the shortcut cannot
// describe -- measures the framed path and reports it as the local one, and the honest conclusion
// "no difference" is indistinguishable from the bug.
//
// It asks scanLocal DIRECTLY rather than reading Encoder.ColumnBatches, because that counter does
// not separate these two arms: the framed path decodes dense columns off the wire and appends
// those column-wise too, so it is nonzero either way. The first draft of this check asserted on
// it and failed the framed arm for taking a column path it is supposed to take.
func assertArmTakesItsPath(b *testing.B, svc *Server, sessions *Sessions, wantLocal bool) {
	b.Helper()
	ctx := context.Background()
	client, local, _, release, err := sessions.ForScoped(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer release()
	if (local != nil) != wantLocal {
		b.Fatalf("this arm has local=%v, want %v", local != nil, wantLocal)
	}
	if !wantLocal {
		return // no in-process session: scanInto cannot take the local path at all
	}
	_, decisions, err := svc.tableSchema(ctx, "history")
	if err != nil {
		b.Fatal(err)
	}
	projected, err := project(decisions, benchProjection)
	if err != nil {
		b.Fatal(err)
	}
	enc := arrowtype.NewEncoder(projected, arrowtype.EncodeOptions{Alloc: memory.DefaultAllocator})
	defer enc.Release()
	flush := func() bool {
		if enc.Rows() > 0 {
			enc.NewRecord().Release()
		}
		return true
	}
	// scanInto would swallow a refusal by falling back, which is the whole thing being
	// checked, so this asks the local path itself.
	if err := svc.scanLocal(ctx, local, enc, "history", "true", flush); err != nil {
		b.Fatalf("the local arm cannot serve this read in process, so it would measure the "+
			"framed path: %v", err)
	}
	flush()
	if enc.ColumnBatches() == 0 {
		b.Fatal("the local path served the read without appending a single batch column-wise")
	}
	_ = client
}
