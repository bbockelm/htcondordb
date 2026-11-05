package flightsvc

import (
	"context"
	"maps"
	"net"
	"strconv"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// localScanFixture is a COLUMNAR archive plus both ways of reading it. Columnar deliberately: an
// archive whose segments have not sealed is read from records whichever path is taken, and a
// comparison between the two would be a comparison of the framed path with itself.
func localScanFixtureB(t testing.TB, rows int) (*dbrpc.Client, *dbrpc.LocalSession, func()) {
	// Two rows in three, spread evenly, so no batch is wholly without it.
	return localScanFixtureSparse(t, rows, func(i int) bool { return i%3 != 0 })
}

// localScanFixtureSparse is localScanFixtureB with WHERE the sparse attribute lands left to the
// caller, which is what decides whether a batch has some of it or none of it at all.
func localScanFixtureSparse(t testing.TB, rows int, sparse func(int) bool) (*dbrpc.Client, *dbrpc.LocalSession, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId", "JobStatus"}, ZoneAttrs: []string{"CompletionDate"},
		SegmentSize: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		text := benchAd(i)
		// A SPARSE attribute. Without one, every cell of every projected column has a
		// value, the undefined branch never runs, and a path that attributed its nulls to
		// the wrong cause would agree with the other one anyway.
		if sparse(i) {
			text += "\nSparseAttr = " + strconv.Itoa(i%11) + "\n"
		}
		ad, perr := classad.ParseOld(text)
		if perr != nil {
			t.Fatal(perr)
		}
		if aerr := arch.Append(ad); aerr != nil {
			t.Fatal(aerr)
		}
	}
	for i := 0; i < 24; i++ {
		arch.BuildAndEnableSchemaScan(2000, 8)
	}
	info := arch.SchemaScanInfo()
	if info.ColumnarSegments == 0 {
		t.Fatalf("fixture has 0 of %d sealed segments columnar: it would be read from records "+
			"either way", info.SealedSegments)
	}
	srv := dbrpc.NewServerCatalog(cat)
	opts := dbrpc.ServeOptions{ReadOnly: true, Privileged: true, IncludePrivate: true}
	cc, sc := net.Pipe()
	go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc), opts) }()
	client := dbrpc.NewClient(dbrpc.NewStreamConn(cc), dbrpc.WithPeerSessionAttrs(dbrpc.ServerSessionAttrs()))
	return client, srv.LocalSession(opts), func() { client.Close(); srv.Close(); cat.Close() }
}

// localProjection is benchProjection plus the sparse attribute, so the read carries undefined
// cells as well as values.
var localProjection = append(append([]string{}, benchProjection...), "SparseAttr")

// TestLocalScanMatchesTheFramedPath: the in-process read and the loopback read must produce the
// same Arrow records. They are two ways of answering one query, and nothing downstream would
// notice them diverging -- both produce a well-formed record batch of the declared schema.
func TestLocalScanMatchesTheFramedPath(t *testing.T) {
	const rows = 20000
	client, local, cleanup := localScanFixtureB(t, rows)
	defer cleanup()
	ctx := context.Background()

	read := func(withLocal bool) ([]arrow.Record, int, []arrowtype.ColumnStats, int) {
		svc, err := NewServer(client, benchDerive, nil)
		if err != nil {
			t.Fatal(err)
		}
		svc.SetSchemaTTL(0)
		_, decisions, err := svc.tableSchema(ctx, "history")
		if err != nil {
			t.Fatal(err)
		}
		projected, err := project(decisions, localProjection)
		if err != nil {
			t.Fatal(err)
		}
		enc := arrowtype.NewEncoder(projected, arrowtype.EncodeOptions{Alloc: memory.DefaultAllocator})
		defer enc.Release()
		var recs []arrow.Record
		n := 0
		flush := func() bool {
			if enc.Rows() == 0 {
				return true
			}
			r := enc.NewRecord()
			n += int(r.NumRows())
			recs = append(recs, r)
			return true
		}
		ls := local
		if !withLocal {
			ls = nil
		}
		if err := svc.scanInto(ctx, client, ls, enc, "history", "true", localProjection, 0, flush); err != nil {
			t.Fatal(err)
		}
		flush()
		return recs, n, append([]arrowtype.ColumnStats(nil), enc.Stats()...), enc.ColumnBatches()
	}

	framedRecs, framedN, framedStats, _ := read(false)
	localRecs, localN, localStats, localBatches := read(true)
	// Without this the comparison below is vacuous: a local read that silently fell through to
	// the framed path agrees with the framed path perfectly, and says nothing.
	if localBatches == 0 {
		t.Fatal("the local read took no batch column-wise, so this compares the framed path with itself")
	}
	defer func() {
		for _, r := range framedRecs {
			r.Release()
		}
		for _, r := range localRecs {
			r.Release()
		}
	}()
	if framedN != rows || localN != rows {
		t.Fatalf("framed read %d rows, local %d, want %d each", framedN, localN, rows)
	}
	// The null CAUSE is not visible in an Arrow record -- a null is a null -- but it is counted
	// per column, and it is the difference between "the attribute was undefined" and "it
	// evaluated to an error". Two paths that agree on the values can still disagree here.
	if len(localStats) != len(framedStats) {
		t.Fatalf("stats length %d vs %d", len(localStats), len(framedStats))
	}
	for col := range framedStats {
		if !maps.Equal(localStats[col].Nulls, framedStats[col].Nulls) {
			t.Fatalf("column %d (%s): local nulls by cause %v, framed %v",
				col, localProjection[col], localStats[col].Nulls, framedStats[col].Nulls)
		}
	}

	framed := digestRecords(t, framedRecs)
	got := digestRecords(t, localRecs)
	for col, want := range framed {
		if got[col] != want {
			t.Fatalf("column %d (%s): local digest %016x, framed %016x",
				col, localProjection[col], got[col], want)
		}
	}
}

// digestRecords folds each COLUMN across the whole read, independent of where the batch
// boundaries fall -- the two paths batch differently, and a digest taken batch by batch would
// differ with every value identical.
func digestRecords(t *testing.T, recs []arrow.Record) []uint64 {
	t.Helper()
	if len(recs) == 0 {
		t.Fatal("no records")
	}
	h := make([]uint64, recs[0].NumCols())
	for i := range h {
		h[i] = 1469598103934665603
	}
	for _, r := range recs {
		for c := 0; c < int(r.NumCols()); c++ {
			col := r.Column(c)
			for row := 0; row < int(r.NumRows()); row++ {
				var s string
				if col.IsNull(row) {
					s = "\x00null"
				} else {
					s = col.ValueStr(row)
				}
				for j := 0; j < len(s); j++ {
					h[c] = (h[c] ^ uint64(s[j])) * 1099511628211
				}
				h[c] = (h[c] ^ 0xff) * 1099511628211
			}
		}
	}
	return h
}

// TestLocalScanTakesABatchWithNoneOfASparseAttribute: an attribute in the table's schema that no
// row of SOME batch carries. That is a column of nulls, not a column this path cannot read.
//
// Before the per-row form it was the latter: the reshape reported every row VALID with no values
// at all, the encoder refused the batch it could not place, and one such batch sent the ENTIRE
// read to the framed path. A sparse attribute does this routinely, so the fast path would have
// been lost silently on exactly the projections that ask for one.
func TestLocalScanTakesABatchWithNoneOfASparseAttribute(t *testing.T) {
	const rows = 4000
	// Only the first 200 ads carry it, so it is in the derived schema -- an attribute NO ad
	// has never gets a column -- while the great majority of batches hold none of it.
	client, local, cleanup := localScanFixtureSparse(t, rows, func(i int) bool { return i < 200 })
	defer cleanup()
	ctx := context.Background()

	svc, err := NewServer(client, benchDerive, nil)
	if err != nil {
		t.Fatal(err)
	}
	svc.SetSchemaTTL(0)
	_, decisions, err := svc.tableSchema(ctx, "history")
	if err != nil {
		t.Fatal(err)
	}
	projected, err := project(decisions, localProjection)
	if err != nil {
		t.Fatal(err)
	}
	enc := arrowtype.NewEncoder(projected, arrowtype.EncodeOptions{Alloc: memory.DefaultAllocator})
	defer enc.Release()
	n := 0
	flush := func() bool {
		if enc.Rows() == 0 {
			return true
		}
		r := enc.NewRecord()
		n += int(r.NumRows())
		r.Release()
		return true
	}
	if err := svc.scanInto(ctx, client, local, enc, "history", "true", localProjection, 0, flush); err != nil {
		t.Fatal(err)
	}
	flush()
	if n != rows {
		t.Fatalf("read %d rows, want %d", n, rows)
	}
	if enc.ColumnBatches() == 0 {
		t.Fatal("a batch with none of the sparse attribute sent the read off the column path")
	}
	sparse := enc.Stats()[len(localProjection)-1]
	if sparse.Direct != 200 || sparse.Nulls[arrowtype.CauseUndefined] != rows-200 {
		t.Errorf("the sparse attribute read as %+v, want 200 direct and %d undefined",
			sparse, rows-200)
	}
}
