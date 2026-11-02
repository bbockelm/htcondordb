package historyimport

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/wire"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// deepAd is a history record holding one value nested deeper than the archive can store.
func deepAd(t *testing.T, cluster int, schedd string) *classad.ClassAd {
	t.Helper()
	ad := histAd(cluster, 0, schedd)
	e, err := classad.ParseExpr(strings.Repeat("x || ", wire.MaxDepth+10) + "x")
	if err != nil {
		t.Fatal(err)
	}
	ad.InsertExpr("Deep", e)
	return ad
}

// rpcWriter is the out-of-process writer the history-import runner uses, over a real dbrpc
// connection to an in-process server, so the refusal crosses the wire as it does in production.
func rpcWriter(t *testing.T, cat *db.Catalog) Writer {
	t.Helper()
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	t.Cleanup(func() { _ = c.Close(); s.Close() })
	return &DBWriter{Client: c}
}

// One record the archive refuses as too deep must not stop a schedd's import. Returned as a
// cycle error it left the cursor short of the record, so every later cycle stopped on it again
// and nothing newer from that schedd was ever imported.
func TestTooDeepRecordDoesNotWedgeImporter(t *testing.T) {
	for _, tc := range []struct {
		name string
		w    func(*testing.T, *db.Catalog) Writer
	}{
		{"in-process", func(_ *testing.T, cat *db.Catalog) Writer { return &ArchiveWriter{Cat: cat} }},
		{"dbrpc", rpcWriter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, err := db.OpenCatalog(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cat.Close() })
			w := tc.w(t, cat)

			hist := []*classad.ClassAd{histAd(1, 0, "schedA"), deepAd(t, 2, "schedA"), histAd(3, 0, "schedA")}
			src := fakeSource{hist: map[string][]*classad.ClassAd{"schedA": hist}}
			cur := fakeCursors{}
			im := newImporter(fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}}, src, w, cur)
			job := Job{Name: "j1", Pool: "p", Table: "history"}
			ctx := context.Background()

			st, err := im.RunJob(ctx, job)
			if err != nil {
				t.Fatal(err)
			}
			if st.Failures != 0 || st.Imported != 2 || st.SkippedTooDeep != 1 {
				t.Fatalf("stats = %+v, want 2 imported, 1 skipped too deep, 0 failures", st)
			}
			if v, _ := cur.Get("j1", "schedA"); v != "3.0" {
				t.Fatalf("cursor = %q, want 3.0: it did not advance past the refused record", v)
			}

			// The next cycle imports what arrived since and does not meet the refused record again.
			src.hist["schedA"] = append(hist, histAd(4, 0, "schedA"))
			st, err = im.RunJob(ctx, job)
			if err != nil {
				t.Fatal(err)
			}
			if st.Failures != 0 || st.Imported != 1 || st.SkippedTooDeep != 0 {
				t.Fatalf("second cycle stats = %+v, want 1 imported, nothing skipped", st)
			}
			arch, ok := cat.ArchiveTable("history")
			if !ok {
				t.Fatal("archive table not created")
			}
			if n := arch.Count(); n != 3 {
				t.Fatalf("archive holds %d records, want 3 (1.0, 3.0, 4.0)", n)
			}
		})
	}
}

// The epoch import has its own append loop and cursor; the same refusal must not wedge it.
func TestTooDeepEpochRecordDoesNotWedgeImporter(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	deep := epochAd(2, 0, 1, 200)
	e, err := classad.ParseExpr(strings.Repeat("x || ", wire.MaxDepth+10) + "x")
	if err != nil {
		t.Fatal(err)
	}
	deep.InsertExpr("Deep", e)
	recs := []*classad.ClassAd{epochAd(3, 0, 1, 300), deep, epochAd(1, 0, 1, 100)} // newest first
	cur := fakeCursors{}
	im := newImporter(fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}},
		fakeEpochSource{recs: recs}, &ArchiveWriter{Cat: cat}, cur)
	st, err := im.RunJob(context.Background(), Job{Name: "j", Pool: "p", Table: "epoch_history", Source: SourceEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if st.Failures != 0 || st.Imported != 2 || st.SkippedTooDeep != 1 {
		t.Fatalf("stats = %+v, want 2 imported, 1 skipped too deep, 0 failures", st)
	}
	v, _ := cur.Get("j", "schedA")
	if c := decodeEpochCursor(v); c.T != 300 {
		t.Fatalf("epoch cursor = %+v, want T=300", c)
	}
}

// The count reaches the status file the daemon's importer manager reads.
func TestStatusAccumulatesSkippedTooDeep(t *testing.T) {
	b := NewStatusBeater(t.TempDir() + "/status")
	b.RecordCycle(Stats{Imported: 2, SkippedTooDeep: 1}, nil)
	b.RecordCycle(Stats{Imported: 1, SkippedTooDeep: 2}, nil)
	s, ok := ReadStatusFile(b.path)
	if !ok {
		t.Fatal("status file unreadable")
	}
	if s.SkippedTooDeepTotal != 3 {
		t.Errorf("SkippedTooDeepTotal = %d, want 3", s.SkippedTooDeepTotal)
	}
}
