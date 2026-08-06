package repl

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// archiveExecBucketed builds an Executor over a "history" archive shaped for the
// per-group-per-day query: Owner categorically indexed, CompletionDate zone-mapped, and
// enough small segments that most fall wholly inside one day bucket.
func archiveExecBucketed(t *testing.T, days, perDay int) (*Executor, func(), map[string]int64) {
	t.Helper()
	const day = 86400
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		SegmentSize:      1 << 13,
		CategoricalAttrs: []string{"Owner"},
		ZoneAttrs:        []string{"CompletionDate"},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1700000000) / day * day
	owners := []string{"alice", "bob", "carol"}
	want := map[string]int64{} // "bucket|owner" -> count
	for d := 0; d < days; d++ {
		for i := 0; i < perDay; i++ {
			o := owners[i%len(owners)]
			ts := base + int64(d)*day + int64(i)*(day/int64(perDay))
			ad, _ := classad.Parse(fmt.Sprintf(`[ CompletionDate = %d; Owner = "%s"; JobStatus = 4 ]`, ts, o))
			if err := arch.Append(ad); err != nil {
				t.Fatal(err)
			}
			want[fmt.Sprintf("%d|%s", ts/day*day, o)]++
		}
	}
	srv := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	return e, func() { c.Close(); srv.Close(); cat.Close() }, want
}

// TestArchiveBucketedGroupBy is the end-to-end "jobs per group per day" query over history.
// Before the archive gained a bucketed aggregate opcode this fell back to shipping every
// matched row to the client and reducing locally; it must now push down and still produce
// exactly the same numbers.
func TestArchiveBucketedGroupBy(t *testing.T) {
	const days, perDay = 4, 60
	e, cleanup, want := archiveExecBucketed(t, days, perDay)
	defer cleanup()

	res, err := e.ExecString(
		`SELECT Owner, time_bucket(CompletionDate, '1d') AS day, COUNT(*) ` +
			`FROM history GROUP BY Owner, time_bucket(CompletionDate, '1d')`)
	if err != nil {
		t.Fatalf("bucketed archive GROUP BY: %v", err)
	}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d (owner,day) rows, want %d", len(res.Rows), len(want))
	}
	var total int64
	for _, r := range res.Rows {
		if len(r) != 3 {
			t.Fatalf("row %v: want 3 columns", r)
		}
		n, err := strconv.ParseInt(r[2], 10, 64)
		if err != nil {
			t.Fatalf("non-numeric count %q", r[2])
		}
		key := r[1] + "|" + r[0]
		if want[key] != n {
			t.Errorf("%s = %d, want %d", key, n, want[key])
		}
		total += n
	}
	if total != int64(days*perDay) {
		t.Errorf("counts sum to %d, want %d", total, days*perDay)
	}
}

// TestArchiveBucketedPushdownAvailable pins that the bucketed archive aggregate is actually
// served by the server rather than silently reduced on the client. The SELECT test above
// compares numbers, which both paths get right, so it cannot tell a pushdown from a
// regression back to fetch-and-reduce; this calls the pushdown directly and requires it to
// answer.
func TestArchiveBucketedPushdownAvailable(t *testing.T) {
	const days, perDay = 3, 30
	e, cleanup, want := archiveExecBucketed(t, days, perDay)
	defer cleanup()

	groups := []dbrpc.GroupCol{{Attr: "Owner"}, {Attr: "CompletionDate", BucketWidth: 86400}}
	rows, err := e.c.ArchiveAggregateBucketed(context.Background(), "history", "true", groups,
		[]dbrpc.AggSpec{{Func: dbrpc.AggCount, Arg: "*"}})
	if err != nil {
		if bucketPushdownUnsupported(err) {
			t.Fatalf("server declined the bucketed archive aggregate, so the repl would silently "+
				"fall back to client-side reduction: %v", err)
		}
		t.Fatalf("ArchiveAggregateBucketed: %v", err)
	}
	if len(rows) != len(want) {
		t.Fatalf("pushdown returned %d (owner,day) rows, want %d", len(rows), len(want))
	}
	var total int64
	for _, r := range rows {
		n, err := strconv.ParseInt(r.Values[0], 10, 64)
		if err != nil {
			t.Fatalf("non-numeric count %q", r.Values[0])
		}
		if got := want[r.Group[1]+"|"+r.Group[0]]; got != n {
			t.Errorf("%s|%s = %d, want %d", r.Group[1], r.Group[0], n, got)
		}
		total += n
	}
	if total != int64(days*perDay) {
		t.Errorf("pushdown counts sum to %d, want %d", total, days*perDay)
	}
}
