package main

import (
	"context"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// A whole query, from a real database through a real dbrpc server to the bytes
// the schedd puts on its client's socket. The CEDAR session and the cgo handles
// are the only things left out: everything between the stored ad and the wire
// framing is the shipping code.
func newTestServer(t *testing.T, ads map[string]string) *dbrpc.Client {
	t.Helper()
	d, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	for key, text := range ads {
		if err := d.UpdateOld(key, text); err != nil {
			t.Fatalf("storing %s: %v", key, err)
		}
	}

	srv := dbrpc.NewServer(d)
	a, b := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(a)) }()
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(b))
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// runStream drives one request to completion and returns the ads it produced (as
// their expression lists) and the counts text.
func runStream(t *testing.T, cl *dbrpc.Client, constraint string, attrs []string,
	limit int, b *adRecordBuilder) ([][]string, string) {
	t.Helper()
	r := &queueRequest{
		client: &queueClient{notifyFD: -1},
		chunks: make(chan queueChunk, 16),
	}
	r.wg.Add(1)
	go r.stream(context.Background(), cl, dbrpc.DefaultTable, constraint, attrs, limit, b, true)
	go func() {
		r.wg.Wait()
		close(r.chunks)
		r.closed.Store(true)
	}()

	var ads [][]string
	for ch := range r.chunks {
		rest := ch.data
		for len(rest) > 0 {
			var exprs []string
			exprs, _, rest = decodeRecord(t, rest)
			ads = append(ads, exprs)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		t.Fatalf("stream failed: %v", r.err)
	}
	return ads, r.counts
}

func sortedLines(s string) []string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	sort.Strings(lines)
	return lines
}

func TestStreamEndToEnd(t *testing.T) {
	cl := newTestServer(t, map[string]string{
		"1.0": "ClusterId = 1\nProcId = 0\nOwner = \"alice\"\nJobStatus = 1\nJobUniverse = 5\nClaimId = \"secret\"",
		"1.1": "ClusterId = 1\nProcId = 1\nOwner = \"alice\"\nJobStatus = 2\nJobUniverse = 5",
		"2.0": "ClusterId = 2\nProcId = 0\nOwner = \"bob\"\nJobStatus = 1\nJobUniverse = 7",
	})

	ads, counts := runStream(t, cl, "true", nil, -1, &adRecordBuilder{emit: true})
	if len(ads) != 3 {
		t.Fatalf("got %d ads, want 3: %q", len(ads), ads)
	}
	for _, ad := range ads {
		for _, e := range ad {
			if strings.HasPrefix(e, "ClaimId ") {
				t.Errorf("a private attribute reached the wire: %q", e)
			}
		}
	}
	want := []string{"5 1 1", "5 2 1", "7 1 1"}
	got := sortedLines(counts)
	if len(got) != len(want) {
		t.Fatalf("counts = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("counts = %q, want %q", got, want)
		}
	}
}

func TestStreamProjectionStillCounts(t *testing.T) {
	// The case a second, grouped query would have hidden: the client projects
	// JobStatus away, and the summary must still be right.
	cl := newTestServer(t, map[string]string{
		"1.0": "ClusterId = 1\nProcId = 0\nOwner = \"alice\"\nJobStatus = 1\nJobUniverse = 5",
		"1.1": "ClusterId = 1\nProcId = 1\nOwner = \"alice\"\nJobStatus = 5\nJobUniverse = 5",
	})

	attrs, suppress := withCountingAttrs([]string{"ClusterId", "ProcId"})
	ads, counts := runStream(t, cl, "true", attrs, -1, &adRecordBuilder{emit: true, suppress: suppress})

	if len(ads) != 2 {
		t.Fatalf("got %d ads, want 2", len(ads))
	}
	for _, ad := range ads {
		for _, e := range ad {
			name := attrNameOf(e)
			if name != "ClusterId" && name != "ProcId" {
				t.Errorf("unprojected attribute %q reached the wire", e)
			}
		}
	}
	got := sortedLines(counts)
	want := []string{"5 1 1", "5 5 1"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("counts = %q, want %q", got, want)
		}
	}
}

func TestStreamSummaryOnlyProducesNoAds(t *testing.T) {
	cl := newTestServer(t, map[string]string{
		"1.0": "ClusterId = 1\nProcId = 0\nJobStatus = 1\nJobUniverse = 5",
		"1.1": "ClusterId = 1\nProcId = 1\nJobStatus = 1\nJobUniverse = 5",
	})
	ads, counts := runStream(t, cl, "true",
		[]string{attrJobStatus, attrJobUniverse}, -1, &adRecordBuilder{emit: false})
	if len(ads) != 0 {
		t.Fatalf("a summary-only query produced %d ads", len(ads))
	}
	if strings.TrimSpace(counts) != "5 1 2" {
		t.Fatalf("counts = %q, want \"5 1 2\"", counts)
	}
}

func TestStreamConstraintAndServerTime(t *testing.T) {
	cl := newTestServer(t, map[string]string{
		"1.0": "ClusterId = 1\nProcId = 0\nOwner = \"alice\"\nJobStatus = 1\nJobUniverse = 5",
		"2.0": "ClusterId = 2\nProcId = 0\nOwner = \"bob\"\nJobStatus = 1\nJobUniverse = 5",
	})
	b := &adRecordBuilder{emit: true, serverTime: serverTimeExpr(1700000000)}
	ads, counts := runStream(t, cl, `Owner == "bob"`, nil, -1, b)
	if len(ads) != 1 {
		t.Fatalf("got %d ads, want only bob's: %q", len(ads), ads)
	}
	var sawServerTime, sawOwner bool
	for _, e := range ads[0] {
		if e == "ServerTime = 1700000000" {
			sawServerTime = true
		}
		if e == `Owner = "bob"` {
			sawOwner = true
		}
	}
	if !sawOwner {
		t.Errorf("the constraint matched the wrong ad: %q", ads[0])
	}
	if !sawServerTime {
		t.Errorf("ServerTime missing from %q", ads[0])
	}
	if strings.TrimSpace(counts) != "5 1 1" {
		t.Errorf("counts = %q, want only the matching ad", counts)
	}
}

func TestStreamLimitCountsOnlyWhatItReturned(t *testing.T) {
	// The schedd's own counter stops with the walk, so a limited query's summary
	// describes the ads returned. Counting from the rows reproduces that; a
	// grouped query over the whole constraint would not.
	ads := map[string]string{}
	for i := 0; i < 10; i++ {
		ads[string(rune('a'+i))+".0"] = "ClusterId = " + string(rune('0'+i)) + "\nJobStatus = 1\nJobUniverse = 5"
	}
	cl := newTestServer(t, ads)

	got, counts := runStream(t, cl, "true", nil, 4, &adRecordBuilder{emit: true})
	if len(got) != 4 {
		t.Fatalf("got %d ads, want the 4 asked for", len(got))
	}
	if strings.TrimSpace(counts) != "5 1 4" {
		t.Fatalf("counts = %q, want the 4 returned", counts)
	}
}
