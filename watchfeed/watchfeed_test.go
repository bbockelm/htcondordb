package watchfeed

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// feedExec wires a client to a server over a pipe. persistent selects a catalog on disk,
// which is what the wire-form feed needs; an in-memory one exercises the text fallback.
func feedExec(t *testing.T, persistent bool) (*dbrpc.Client, *db.Catalog, func()) {
	t.Helper()
	dir := ""
	if persistent {
		dir = t.TempDir()
	}
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return c, cat, func() { c.Close(); s.Close(); cat.Close() }
}

// nextUpsert waits for the next upsert, skipping the control events (reset, synced).
func nextUpsert(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("feed closed before an upsert arrived")
			}
			if ev.Kind != 0 { // db.WatchUpsert
				continue
			}
			return ev
		case <-deadline:
			t.Fatal("timed out waiting for an upsert")
		}
	}
}

// TestWatchDecodesOverBothTransports is the point of the package: whichever feed the
// server offers, a consumer gets the same decoded ad. The persistent case takes the
// wire-form feed; there is no in-repo way to get an old server, so the text path is
// covered by the fallback test below and by every consumer's own tests.
func TestWatchDecodesOverBothTransports(t *testing.T) {
	c, cat, cleanup := feedExec(t, true)
	defer cleanup()
	tbl, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	head, err := c.WatchHead(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	ch, stop, wireForm, err := Watch(ctx, c, "jobs", head)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if !wireForm {
		t.Log("server served the text feed; the decode below still has to hold")
	}

	ad, err := classad.Parse(`[ Owner = "alice"; JobStatus = 2; Memory = 2048; Note = "a \"quoted\" word" ]`)
	if err != nil {
		t.Fatal(err)
	}
	if err := tbl.Put("k1", ad); err != nil {
		t.Fatal(err)
	}

	ev := nextUpsert(t, ch)
	if ev.Err != nil {
		t.Fatalf("event carried an error: %v", ev.Err)
	}
	if ev.Key != "k1" {
		t.Errorf("key = %q, want k1", ev.Key)
	}
	if ev.Ad == nil {
		t.Fatal("upsert carried no ad")
	}
	if owner, _ := ev.Ad.EvaluateAttrString("Owner"); owner != "alice" {
		t.Errorf("Owner = %q, want alice", owner)
	}
	if mem, ok := ev.Ad.EvaluateAttrNumber("Memory"); !ok || mem != 2048 {
		t.Errorf("Memory = %v (ok=%v), want 2048", mem, ok)
	}
	if note, _ := ev.Ad.EvaluateAttrString("Note"); note != `a "quoted" word` {
		t.Errorf("Note = %q, want the embedded quotes intact", note)
	}
}

// TestWatchFallsBackForMemoryTable covers the text path end to end. An in-memory catalog
// cannot serve wire rows for queries; the watch feed is a different op, so this mainly
// pins that a RAM table's feed still delivers decoded ads whichever way it is served.
func TestWatchFallsBackForMemoryTable(t *testing.T) {
	c, cat, cleanup := feedExec(t, false)
	defer cleanup()
	tbl, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	head, err := c.WatchHead(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	ch, stop, _, err := Watch(ctx, c, "jobs", head)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	ad, err := classad.Parse(`[ Owner = "bob"; Cpus = 4 ]`)
	if err != nil {
		t.Fatal(err)
	}
	if err := tbl.Put("k1", ad); err != nil {
		t.Fatal(err)
	}
	ev := nextUpsert(t, ch)
	if ev.Err != nil {
		t.Fatalf("event carried an error: %v", ev.Err)
	}
	if ev.Ad == nil {
		t.Fatal("upsert carried no ad")
	}
	if owner, _ := ev.Ad.EvaluateAttrString("Owner"); owner != "bob" {
		t.Errorf("Owner = %q, want bob", owner)
	}
}

// TestTextFeedFormatContract pins the format the text feed actually renders, because
// getting it wrong is silent. The Grafana plugin parsed these events with ParseOld -- the
// OLD-ClassAd reader -- and discarded the error, so every column it rendered came out
// blank; its test passed because the fixture was hand-written in the old format rather
// than the one the server sends.
func TestTextFeedFormatContract(t *testing.T) {
	ad, err := classad.Parse(`[ Owner = "alice"; Cpus = 8 ]`)
	if err != nil {
		t.Fatal(err)
	}
	feedText := ad.String() // exactly what the server writes for a text watch event

	if _, err := classad.ParseOld(feedText); err == nil {
		t.Error("ParseOld now accepts the text feed's format; the reason this package parses " +
			"with Parse should be revisited")
	}
	got, err := classad.Parse(feedText)
	if err != nil {
		t.Fatalf("Parse rejected the text feed's own output: %v", err)
	}
	if owner, _ := got.EvaluateAttrString("Owner"); owner != "alice" {
		t.Errorf("Owner = %q, want alice", owner)
	}
}
