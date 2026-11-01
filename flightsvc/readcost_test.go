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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Where a Flight read's time actually goes, and how much is left for a columnar producer.
//
// This exists to answer one question before anybody spends weeks on it. The plan has always
// been to emit Arrow batches built straight from columnar segments rather than from reassembled
// ads -- but what that would REPLACE is the render, the text parse and the per-attribute
// evaluate, so if the store's scan dominates all three then the ceiling is low and the work is
// not worth its risk.
//
// THREE DESIGN POINTS, each of which a previous version of this file got wrong.
//
// 1. THE STAGES ARE MEASURED IN ISOLATION, NOT CUMULATIVELY. The dbrpc loopback serves from its
//    own goroutine, so a "scan then parse in the yield" arm has the scan and the parse running
//    concurrently on two cores. Subtracting such arms attributed nothing: the cumulative arms
//    came out NON-MONOTONE, with the full gRPC read faster than a strict subset of its own work,
//    which is the only reason the error was caught at all. So the parse and the encode run over
//    material collected beforehand, and the end-to-end arm is reported separately rather than
//    subtracted -- it overlaps by design, and that is a feature of the endpoint, not a cost.
//
// 2. THE ARCHIVE IS WHERE THE COLUMNAR LAYOUT LIVES, AND IT HAS TO BE TURNED ON. A default
//    fixture reports `colscan: enabled=false`, and a mutable table in one has NO SEALED SEGMENTS
//    at all -- so the accelerator cannot apply to it even when enabled. Measured without
//    checking, the "archive" arm measured the record path and would have been published as a
//    columnar comparison. The fixture now asserts coverage, and the benchmark names the tier it
//    measured, for the same reason the storage layer's own vector scan counts the blocks that
//    declined: a scan that silently fell back looks exactly like a fast one that is mysteriously
//    no faster.
//
// 3. THE A/B IS THE ACCELERATOR, NOT THE TABLE KIND. Comparing a mutable table against an
//    archive compares two different datasets' storage. Comparing one archive with the columnar
//    accelerator on and off isolates the thing in question.

// benchAd is a job-shaped ad: enough attributes that per-attribute work is visible, with a
// realistic mix of kinds and one expression, since an expression is what makes the encoder
// evaluate rather than read.
func benchAd(i int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Owner = \"user%d\"\n", i%64)
	fmt.Fprintf(&b, "ClusterId = %d\nProcId = %d\nJobStatus = %d\n", 1000+i/10, i%10, i%6)
	fmt.Fprintf(&b, "QDate = %d\nJobStartDate = %d\nCompletionDate = %d\n",
		1700000000+i, 1700000100+i, 1700000900+i)
	fmt.Fprintf(&b, "RequestCpus = %d\nRequestMemory = %d\nRequestDisk = %d\n",
		1+i%8, 2048+i%4096, 10240+i%8192)
	fmt.Fprintf(&b, "RemoteWallClockTime = %d.5\nCpusUsage = %d.25\nBytesSent = %d\nBytesRecvd = %d\n",
		i%7200, i%4, i*131, i*17)
	fmt.Fprintf(&b, "Cmd = \"/usr/bin/payload-%d\"\nIwd = \"/home/user%d/run/%d\"\n", i%32, i%64, i)
	fmt.Fprintf(&b, "GlobalJobId = \"ap%d.example.edu#%d.%d#%d\"\n", i%4, 1000+i/10, i%10, 1700000000+i)
	fmt.Fprintf(&b, "LastRemoteHost = \"slot1_%d@node%d.example.edu\"\n", 1+i%8, i%512)
	fmt.Fprintf(&b, "ProjectName = \"proj%d\"\nAccountingGroup = \"group%d.user%d\"\n", i%16, i%4, i%64)
	fmt.Fprintf(&b, "ExitCode = %d\nNumJobStarts = %d\nNumShadowStarts = %d\n", i%2, 1+i%3, 1+i%3)
	fmt.Fprintf(&b, "ImageSize = %d\nResidentSetSize = %d\nDiskUsage = %d\n",
		100000+i, 90000+i, 50000+i)
	fmt.Fprintf(&b, "TransferInputSizeMB = %d\nJobUniverse = 5\nMinHosts = 1\nMaxHosts = 1\n", i%100)
	fmt.Fprintf(&b, "WantCheckpoint = false\nOnExitHold = false\nLeaveJobInQueue = false\n")
	fmt.Fprintf(&b, "Requirements = TARGET.Arch == \"X86_64\" && TARGET.Memory >= RequestMemory\n")
	// MemoryUsage as an expression over a sibling: the case that makes the encoder evaluate,
	// and the reason the scan has to chase references.
	fmt.Fprintf(&b, "MemoryUsage = (ResidentSetSize + 1023) / 1024\n")
	return b.String()
}

// benchProjection is the common read: a handful of columns, not the whole ad.
var benchProjection = []string{"Owner", "ClusterId", "JobStatus", "QDate", "MemoryUsage", "CpusUsage"}

// benchDerive declares MemoryUsage, which is a finding and not a fixture detail: it is an
// expression in every ad here, as it is in 1996 of 2000 on a real AP, and the profile
// classifies the STORED form and evaluates nothing -- so the measurement rules it untyped and
// the column does not exist. The encoder would evaluate it to an integer perfectly well. Until
// the profile evaluates too, an override is the only way to have the column at all, and this
// benchmark wants it because an attribute the encoder must EVALUATE is exactly the
// per-attribute cost being weighed.
var benchDerive = arrowtype.Options{
	TimeAttrs: map[string]bool{"qdate": true, "jobstartdate": true, "completiondate": true},
	Overrides: map[string]arrowtype.Override{"memoryusage": {Kind: "int"}},
}

type benchStack struct {
	client *dbrpc.Client
	svc    *Server
	fs     *flightsql.Client
	tier   string
	close  func()
}

// benchArchive builds an archive of rows ads and, when columnar is set, turns the per-segment
// columnar accelerator on and covers every sealed segment with it.
//
// Production reaches that state by demand -- maintenance builds the schema from accumulated
// read demand and covers a budget of segments per pass -- which a fixture gets none of, so it
// is asked for directly. tier records what was actually achieved, because "enabled" and
// "covering the segments you are about to read" are different claims.
func benchArchive(b *testing.B, rows int, columnar bool) *benchStack {
	b.Helper()
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId", "JobStatus"},
		ZoneAttrs:  []string{"CompletionDate"},
		// Small enough that the archive actually SEALS rather than sitting in one
		// unsealed segment: the columnar layout applies only to sealed ones, so a
		// default-sized fixture would measure the record path and call it columnar.
		SegmentSize: 256 << 10,
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		ad, perr := classad.ParseOld(benchAd(i))
		if perr != nil {
			b.Fatal(perr)
		}
		if aerr := arch.Append(ad); aerr != nil {
			b.Fatal(aerr)
		}
	}

	tier := "record path (columnar accelerator off)"
	if columnar {
		if !arch.BuildAndEnableSchemaScan(2000, 8) {
			b.Fatal("nothing to sample; the columnar accelerator did not build")
		}
		info := arch.SchemaScanInfo()
		if !info.Enabled || info.SealedSegments == 0 || info.CoveredSegments < info.SealedSegments {
			b.Fatalf("columnar accelerator not covering the data: enabled=%v sealed=%d covered=%d; "+
				"this arm would measure the record path and report it as columnar",
				info.Enabled, info.SealedSegments, info.CoveredSegments)
		}
		tier = fmt.Sprintf("columnar (%d fields, %d/%d sealed segments covered)",
			info.SchemaFields, info.CoveredSegments, info.SealedSegments)
	} else if info := arch.SchemaScanInfo(); info.Enabled {
		b.Fatalf("the columnar accelerator is on in the control arm (covered=%d)", info.CoveredSegments)
	}

	srv := dbrpc.NewServerCatalog(cat)
	cc, sc := net.Pipe()
	go func() {
		_ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc),
			dbrpc.ServeOptions{ReadOnly: true, Privileged: true, IncludePrivate: true})
	}()
	client := dbrpc.NewClient(dbrpc.NewStreamConn(cc))

	svc, err := NewServer(client, benchDerive, nil)
	if err != nil {
		b.Fatal(err)
	}
	// The cache on, because that is how it serves: an uncached derivation would put a
	// sampling scan inside every iteration and swamp what this measures.
	svc.SetSchemaTTL(time.Hour)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	gsrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(gsrv, NewFlightServer(svc, nil))
	go func() { _ = gsrv.Serve(ln) }()
	fs, err := flightsql.NewClient(ln.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	return &benchStack{client: client, svc: svc, fs: fs, tier: tier, close: func() {
		fs.Close()
		gsrv.Stop()
		client.Close()
		srv.Close()
		cat.Close()
	}}
}

// BenchmarkReadStages measures each stage of a Flight read on its own, with the columnar
// accelerator off and on.
func BenchmarkReadStages(b *testing.B) {
	const rows = 20000
	for _, columnar := range []bool{false, true} {
		name := "record"
		if columnar {
			name = "columnar"
		}
		b.Run(name, func(b *testing.B) { benchReadStages(b, rows, columnar) })
	}
}

func benchReadStages(b *testing.B, rows int, columnar bool) {
	st := benchArchive(b, rows, columnar)
	defer st.close()
	ctx := context.Background()
	b.Log("tier:", st.tier)

	_, decisions, err := st.svc.tableSchema(ctx, "history")
	if err != nil {
		b.Fatal(err)
	}
	projected, err := project(decisions, benchProjection)
	if err != nil {
		b.Fatal(err)
	}

	// Collected once, outside every timer: the parse and encode arms work over material that
	// is already in hand, so neither overlaps the store's goroutine.
	var texts []string
	if err := st.client.QueryRawProjectRefsStream(ctx, "history", "true", benchProjection, 0,
		func(row string) bool { texts = append(texts, row); return true }); err != nil {
		b.Fatal(err)
	}
	if len(texts) != rows {
		b.Fatalf("collected %d rows, want %d", len(texts), rows)
	}
	ads := make([]*classad.ClassAd, 0, len(texts))
	for _, t := range texts {
		ad, perr := classad.ParseOld(t)
		if perr != nil {
			b.Fatal(perr)
		}
		ads = append(ads, ad)
	}

	// A benchmark of a failing path measures nothing, and "every row encoded" would be
	// satisfied by a column of nulls. So encode once, outside the timer, and require the
	// expression-valued column to hold real values -- which is also the check that the
	// projection carried the siblings MemoryUsage is written over.
	func() {
		enc := arrowtype.NewEncoder(projected, arrowtype.EncodeOptions{})
		defer enc.Release()
		for _, ad := range ads {
			if err := enc.Append(ad); err != nil {
				b.Fatal(err)
			}
			if enc.Rows() >= batchRows {
				break
			}
		}
		rec := enc.NewRecord()
		defer rec.Release()
		idx := rec.Schema().FieldIndices("MemoryUsage")
		if len(idx) == 0 {
			b.Fatal("no MemoryUsage column; the encode arm would measure the wrong work")
		}
		col := rec.Column(idx[0])
		if col.NullN() != 0 {
			b.Fatalf("MemoryUsage is %d/%d null: the expression did not evaluate, so this "+
				"benchmark is timing a failing path", col.NullN(), col.Len())
		}
		// Total() counts every cell accounted for, including the ones stored cleanly, so
		// what matters here is the two kinds of damage: a lossy coercion and a null. A
		// first pass asserted Total()==0 and "failed" on healthy columns.
		for _, cs := range enc.Stats() {
			nulls := 0
			for _, v := range cs.Nulls {
				nulls += v
			}
			if cs.CoercedLossy != 0 || nulls != 0 {
				b.Fatalf("column %s: %d lossy coercions and %d nulls out of %d cells; "+
					"this benchmark would be timing a path that is not producing the data",
					cs.Name, cs.CoercedLossy, nulls, cs.Total())
			}
			if cs.Direct+cs.CoercedExact == 0 {
				b.Fatalf("column %s stored no values at all", cs.Name)
			}
		}
	}()

	perRow := func(b *testing.B) {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(rows), "ns/row")
	}

	// scan: the store's scan, its projection, the render, and the loopback transfer. The
	// yield only counts, so nothing of ours overlaps it.
	b.Run("scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			if err := st.client.QueryRawProjectRefsStream(ctx, "history", "true",
				benchProjection, 0, func(string) bool { n++; return true }); err != nil {
				b.Fatal(err)
			}
			if n != rows {
				b.Fatalf("%d rows, want %d", n, rows)
			}
		}
		perRow(b)
	})

	// parse: text back into a ClassAd. Unaffected by the storage tier -- it is the same text
	// either way -- and measured in both arms as a check on exactly that.
	b.Run("parse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, t := range texts {
				if ad, perr := classad.ParseOld(t); perr != nil || ad == nil {
					b.Fatal("parse failed")
				}
			}
		}
		perRow(b)
	})

	// encode: evaluate each projected attribute and append it to an Arrow builder. This and
	// the parse are what a columnar producer would remove outright.
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			enc := arrowtype.NewEncoder(projected, arrowtype.EncodeOptions{})
			n := 0
			for _, ad := range ads {
				_ = enc.Append(ad)
				if enc.Rows() >= batchRows {
					rec := enc.NewRecord()
					n += int(rec.NumRows())
					rec.Release()
				}
			}
			if enc.Rows() > 0 {
				rec := enc.NewRecord()
				n += int(rec.NumRows())
				rec.Release()
			}
			enc.Release()
			if n != rows {
				b.Fatalf("%d encoded, want %d", n, rows)
			}
		}
		perRow(b)
	})

	// e2e: the whole endpoint over gRPC. NOT a sum of the arms above and not comparable to
	// them by subtraction -- the scan, the encode and the client's decode run concurrently,
	// which is why this is the number a user feels and the others are the CPU it costs.
	b.Run("e2e-over-grpc", func(b *testing.B) {
		b.ReportAllocs()
		q := "SELECT " + strings.Join(benchProjection, ", ") + " FROM history"
		for i := 0; i < b.N; i++ {
			info, err := st.fs.Execute(ctx, q)
			if err != nil {
				b.Fatal(err)
			}
			rdr, err := st.fs.DoGet(ctx, info.Endpoint[0].Ticket)
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
		perRow(b)
	})
}
