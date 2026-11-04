package flightsvc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/collections/wire"
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

// benchValueOpts is what the Flight service asks for, so the value arms measure the form that
// actually ships. Without it they take the plain string column, which allocates a Go string per
// cell on the client where the dictionary allocates one per distinct value -- so the arms would
// charge the client for work the service does not do, and the comparison they exist to make would
// be against a path nobody takes.
var benchValueOpts = dbrpc.ValueStreamOptions{Dictionary: true}

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

// probeColumnScan reads the whole fixture through the column path and reports what the scan
// actually touched. RecordReads -- rows whose bytes had to be materialized even so -- is the
// measurement that matters here, and the one counter that distinguishes a converged archive from
// one whose segments merely carry blocks.
//
// Outside every timer, and deliberately a direct store read rather than one through the service:
// it is asking what the storage layer has, not what the Flight path costs.
func probeColumnScan(b *testing.B, arch *db.ArchiveTable) collections.ColumnScanStats {
	b.Helper()
	var scan collections.ColumnScanStats
	seq, err := arch.QueryColumnsStats("true", benchProjection, false, &scan)
	if err != nil {
		b.Fatalf("column probe: %v", err)
	}
	for range seq {
	}
	return scan
}

// benchArchive builds an archive of rows ads and, when columnar is set, turns the per-segment
// columnar accelerator on and runs maintenance until reads stop falling back to whole records.
//
// Production reaches that state by demand -- maintenance builds the schema from accumulated
// read demand and rewrites a budget of segments per pass -- which a fixture gets none of, so it
// is asked for directly. tier records what was actually achieved, because "enabled", "covering
// the segments you are about to read" and "reading them from columns" are three different
// claims, and this benchmark previously checked the weakest one.
//
// Coverage is the weak check: a sidecar columnar block is built for every sealed segment on one
// schema scan, so CoveredSegments reaches SealedSegments immediately and says nothing about how
// the records are stored. The rewrite that moves attributes out of the records is bounded per
// pass, so after the single pass this used to do, most segments were still row-native and the
// "columnar" arm was measuring the record path. The convergence loop below settles that from
// the read side, where ColumnScanStats counts what the scan actually touched.
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
		// Converge on the read itself, because every other counter is blind to this. One
		// maintenance pass rewrites a bounded number of segments into columnar-native form, so
		// a fixture bigger than that budget keeps serving most rows from whole records for
		// several passes -- while CoveredSegments, StaleSegments and RowWindows all sit at
		// "fully columnar" from the first pass, since the blocks exist and every one of them
		// vectorizes. Measured at 320k rows: 766 sealed, 766 covered, 0 stale, 1 row window,
		// and 293,107 of 320,000 rows still reading a record. Only RecordReads moves, by one
		// segment budget per pass.
		//
		// RecordReads never reaches zero: the segment still being appended to has no block, so
		// its rows always cost a record. The loop therefore ends on a pass that wins nothing.
		passes, scan := 1, probeColumnScan(b, arch)
		for scan.RecordReads > 0 {
			arch.BuildAndEnableSchemaScan(2000, 8)
			passes++
			next := probeColumnScan(b, arch)
			if next.RecordReads >= scan.RecordReads {
				scan = next
				break
			}
			scan = next
		}
		if scan.Rows == 0 {
			b.Fatal("the column probe read no rows; it is not measuring this fixture")
		}
		// Well above the unsealed-segment floor (1.2% of rows here) and far below what an
		// unconverged fixture reads (92%), so this separates the two without being tuned to
		// either.
		if scan.RecordReads*10 > scan.Rows {
			b.Fatalf("after %d maintenance pass(es) %d of %d rows still read a whole record: "+
				"this arm would measure the record path and report it as columnar",
				passes, scan.RecordReads, scan.Rows)
		}
		tier = fmt.Sprintf("columnar (%d fields, %d/%d sealed segments covered, %d maintenance "+
			"pass(es), %d of %d rows still read a whole record)",
			info.SchemaFields, info.CoveredSegments, info.SealedSegments,
			passes, scan.RecordReads, scan.Rows)
	} else if info := arch.SchemaScanInfo(); info.Enabled {
		b.Fatalf("the columnar accelerator is on in the control arm (covered=%d)", info.CoveredSegments)
	}

	srv := dbrpc.NewServerCatalog(cat)
	cc, sc := net.Pipe()
	go func() {
		_ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc),
			dbrpc.ServeOptions{ReadOnly: true, Privileged: true, IncludePrivate: true})
	}()
	// WITH the peer's advertisement, as the production loopback builds it
	// (loopbackPeerAttrs). Without it the client reads every feature that postdates
	// advertisement as absent and silently takes the fallback -- so a feature-gated
	// improvement measures as exactly zero here while working in the daemon.
	client := dbrpc.NewClient(dbrpc.NewStreamConn(cc),
		dbrpc.WithPeerSessionAttrs(dbrpc.ServerSessionAttrs()))

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

	// values-scan: the same store work on the TYPED path -- the scan, its projection, the
	// evaluation, the column packing and the loopback transfer, with no text in it. Against
	// "scan" above this is what deleting the render was worth inside the store; against
	// "values-encode" below it is how the remaining CPU divides between the store and this
	// process, which is the number that decides whether a columnar producer has anything
	// left to win.
	b.Run("values-scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			if err := st.client.QueryValuesStreamOpts(ctx, "history", "true", benchProjection, 0, false,
				benchValueOpts,
				func(vb *dbrpc.ValueBatch) bool { n += vb.Rows; return true }); err != nil {
				b.Fatal(err)
			}
			if n != rows {
				b.Fatalf("%d rows, want %d", n, rows)
			}
		}
		perRow(b)
	})

	// values-encode: the scan above PLUS this process turning those values into Arrow. The
	// gap between the two is everything a columnar producer could still remove on this side
	// of the loopback -- the per-cell ValueAt and the builder appends -- and it is the whole
	// of the upside, because the store's own work is the other arm and a producer does not
	// make the storage read cheaper.
	b.Run("values-encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			enc := arrowtype.NewEncoder(projected, arrowtype.EncodeOptions{})
			n := 0
			if err := st.client.QueryValuesStreamOpts(ctx, "history", "true", benchProjection, 0, false,
				benchValueOpts,
				func(vb *dbrpc.ValueBatch) bool {
					for row := range vb.Rows {
						vals := make([]classad.Value, len(vb.Cols))
						for col := range vb.Cols {
							vals[col] = vb.ValueAt(col, row)
						}
						_ = enc.AppendValues(vals)
						if enc.Rows() >= batchRows {
							rec := enc.NewRecord()
							n += int(rec.NumRows())
							rec.Release()
						}
					}
					return true
				}); err != nil {
				b.Fatal(err)
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

	// values-encode-seq: the same work as values-encode, reading each batch COLUMN-MAJOR with
	// a running dense index instead of calling ValueAt per cell.
	//
	// ValueAt recounts the validity bitmap from row 0 on every call, so consuming a whole
	// batch through it is quadratic in the batch size. That is a property of the accessor and
	// not of the data, and the gap between these two arms is the whole of it -- which is why
	// it is measured rather than argued, and why the first split of this cost attributed to
	// "turning values into Arrow" what was really an O(n^2) walk.
	b.Run("values-encode-seq", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			enc := arrowtype.NewEncoder(projected, arrowtype.EncodeOptions{})
			n := 0
			vals := make([]classad.Value, len(benchProjection))
			cells := make([][]classad.Value, len(benchProjection))
			if err := st.client.QueryValuesStreamOpts(ctx, "history", "true", benchProjection, 0, false,
				benchValueOpts,
				func(vb *dbrpc.ValueBatch) bool {
					for col := range vb.Cols {
						cells[col] = unpackColumn(&vb.Cols[col], vb.Rows, cells[col][:0])
					}
					for row := range vb.Rows {
						for col := range vb.Cols {
							vals[col] = cells[col][row]
						}
						_ = enc.AppendValues(vals)
						if enc.Rows() >= batchRows {
							rec := enc.NewRecord()
							n += int(rec.NumRows())
							rec.Release()
						}
					}
					return true
				}); err != nil {
				b.Fatal(err)
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

// benchLiterals is benchProjection without MemoryUsage, which this fixture stores as an
// expression in every ad and which therefore always falls through to an evaluator.
var benchLiterals = []string{"Owner", "ClusterId", "JobStatus", "QDate", "CpusUsage"}

// BenchmarkWireNativeRead measures the read that never materializes text.
//
// A wire-form row is an inline-names ad: wire.Ad.LookupByName finds one attribute's node bytes
// and wire.LiteralValue decodes a scalar from them, so a value reaches an Arrow builder with no
// render and no ClassAd in between. Against BenchmarkReadStages this is what separates the
// storage read from the text round trip built on top of it -- and that separation is the whole
// reason this benchmark exists, because an earlier version of it attributed the render to the
// scan and concluded the scan was irreducible.
//
// WHAT IT DOES NOT DO: evaluate. A node that is not a literal gets a null here, and the
// fellthrough/row metric counts those. A real encoder must evaluate such a cell, so the figures
// below are the ceiling for a projection of literal-valued attributes and not a universal
// number -- which is why the literals-only arm is reported separately rather than instead.
func BenchmarkWireNativeRead(b *testing.B) {
	const rows = 20000
	st := benchArchive(b, rows, true)
	defer st.close()
	ctx := context.Background()

	// The wire op refuses an archive in classad up to v0.34.x, which is the gap this
	// benchmark was written to size. Skip rather than fail, naming why, so the arm starts
	// reporting the moment the dependency carries the archive case.
	if err := st.client.QueryRawWireRefsStream(ctx, "history", "true", benchProjection, 1, false,
		func([]byte) bool { return false }); err != nil {
		b.Skipf("the wire op does not serve an archive in this classad: %v", err)
	}

	b.Run("scan", func(b *testing.B) {
		for _, arm := range []struct {
			name string
			run  func() error
		}{
			// The text projection first, as the thing being compared against.
			{"text-projection", func() error {
				n := 0
				err := st.client.QueryRawProjectRefsStream(ctx, "history", "true", benchProjection, 0,
					func(string) bool { n++; return true })
				if n != rows {
					b.Fatalf("%d rows, want %d", n, rows)
				}
				return err
			}},
			{"wire", func() error {
				n := 0
				err := st.client.QueryRawWireRefsStream(ctx, "history", "true", benchProjection, 0, false,
					func([]byte) bool { n++; return true })
				if n != rows {
					b.Fatalf("%d rows, want %d", n, rows)
				}
				return err
			}},
		} {
			b.Run(arm.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := arm.run(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(rows), "ns/row")
			})
		}
	})

	b.Run("extract", func(b *testing.B) {
		for _, proj := range [][]string{benchProjection, benchLiterals} {
			name := "with-expression-column"
			if len(proj) == len(benchLiterals) {
				name = "literals-only"
			}
			b.Run(name, func(b *testing.B) { benchWireExtract(b, st, proj, rows) })
		}
	})
}

func benchWireExtract(b *testing.B, st *benchStack, proj []string, rows int) {
	ctx := context.Background()
	_, decisions, err := st.svc.tableSchema(ctx, "history")
	if err != nil {
		b.Fatal(err)
	}
	projected, err := project(decisions, proj)
	if err != nil {
		b.Fatal(err)
	}
	schema := schemaOf(projected)

	// Collected once, outside the timer, as the other stage arms are.
	var raws [][]byte
	if err := st.client.QueryRawWireRefsStream(ctx, "history", "true", proj, 0, false,
		func(row []byte) bool {
			raws = append(raws, append([]byte(nil), row...))
			return true
		}); err != nil {
		b.Fatal(err)
	}
	if len(raws) != rows {
		b.Fatalf("collected %d wire rows, want %d", len(raws), rows)
	}

	fell := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		fell = 0
		for _, raw := range raws {
			ad := wire.Ad(raw)
			for fi, f := range schema.Fields() {
				node, ok := ad.LookupByName(f.Name)
				if !ok {
					bld.Field(fi).AppendNull()
					continue
				}
				lit, ok := wire.LiteralValue(node)
				if !ok {
					fell++ // an expression: a real encoder evaluates here
					bld.Field(fi).AppendNull()
					continue
				}
				appendLiteral(bld.Field(fi), lit)
			}
		}
		rec := bld.NewRecord()
		if int(rec.NumRows()) != rows {
			b.Fatalf("%d rows built, want %d", rec.NumRows(), rows)
		}
		rec.Release()
		bld.Release()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(rows), "ns/row")
	b.ReportMetric(float64(fell)/float64(rows), "fellthrough/row")
}

// appendLiteral puts one wire literal into a builder, coercing between int and real the way
// arrowtype.Encoder does so the comparison is like for like.
func appendLiteral(bld array.Builder, lit wire.Literal) {
	switch b := bld.(type) {
	case *array.Int64Builder:
		switch lit.Kind {
		case wire.LitInt:
			b.Append(lit.Int)
		case wire.LitReal:
			b.Append(int64(lit.Real))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch lit.Kind {
		case wire.LitReal:
			b.Append(lit.Real)
		case wire.LitInt:
			b.Append(float64(lit.Int))
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		if lit.Kind == wire.LitString {
			b.Append(lit.Str)
		} else {
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if lit.Kind == wire.LitBool {
			b.Append(lit.Bool)
		} else {
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch lit.Kind {
		case wire.LitInt:
			b.Append(arrow.Timestamp(lit.Int))
		case wire.LitReal:
			b.Append(arrow.Timestamp(int64(lit.Real)))
		default:
			b.AppendNull()
		}
	default:
		bld.AppendNull()
	}
}

// unpackColumn expands one packed column into per-row values in a single forward pass, keeping
// a running dense index rather than recounting the bitmap for each row.
func unpackColumn(c *dbrpc.ValueColumn, rows int, dst []classad.Value) []classad.Value {
	dense, exc := 0, 0
	for row := range rows {
		if c.Valid[row>>3]&(1<<(uint(row)&7)) == 0 {
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
