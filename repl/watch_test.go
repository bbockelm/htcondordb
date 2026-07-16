package repl

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// TestWatchSinceBeginning: SINCE BEGINNING replays the table's current contents as
// upserts (projected + filtered), and LIMIT stops the stream.
func TestWatchSinceBeginning(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, `INSERT INTO ads (Key, Owner, Memory) VALUES ('1.0', 'alice', 4096)`)
	mustExec(t, e, `INSERT INTO ads (Key, Owner, Memory) VALUES ('2.0', 'bob', 8192)`)
	mustExec(t, e, `INSERT INTO ads (Key, Owner, Memory) VALUES ('3.0', 'alice', 2048)`)

	s := &session{exec: e, table: "ads"}
	st, err := Parse(`WATCH Owner, Memory FROM ads WHERE Owner == "alice" SINCE BEGINNING LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() { s.runWatch(context.Background(), &buf, st); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop at LIMIT")
	}
	out := buf.String()
	// Both alice rows replayed (bob filtered out), stopped at LIMIT 2.
	if got := strings.Count(out, "UPSERT"); got != 2 {
		t.Errorf("want 2 UPSERT rows (alice only), got %d\n%s", got, out)
	}
	if strings.Contains(out, "bob") {
		t.Errorf("bob should be filtered by WHERE:\n%s", out)
	}
	if !strings.Contains(out, "limit reached") {
		t.Errorf("expected a limit-reached summary:\n%s", out)
	}
}

// TestWatchNowLive: the default (NOW) suppresses the initial replay and shows only
// changes made after the watch starts.
func TestWatchNowLive(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, `INSERT INTO ads (Key, Owner) VALUES ('old', 'preexisting')`)

	s := &session{exec: e, table: "ads"}
	st, err := Parse(`WATCH * FROM ads`) // SINCE NOW (default), no limit
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() { s.runWatch(ctx, &buf, st); close(done) }()

	// Give the watch a moment to reach the live phase, then make a change.
	time.Sleep(150 * time.Millisecond)
	mustExec(t, e, `INSERT INTO ads (Key, Owner) VALUES ('new', 'live')`)
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop on cancel")
	}
	out := buf.String()
	if !strings.Contains(out, "new") || !strings.Contains(out, "live") {
		t.Errorf("live change 'new' should appear:\n%s", out)
	}
	if strings.Contains(out, "preexisting") {
		t.Errorf("NOW mode must suppress the pre-existing ad (replay):\n%s", out)
	}
}
