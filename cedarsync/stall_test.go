package cedarsync

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db/replicate"
)

// pump releases every change the gateSink holds, so a session runs through to Synced unimpeded.
// It stops when ctx is cancelled.
func pump(ctx context.Context, s *gateSink) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.got:
			}
			select {
			case <-ctx.Done():
				return
			case s.release <- struct{}{}:
			}
		}
	}()
}

// TestStalledSessionIsNotACleanReconnect: a session that ends before Synced without advancing the
// cursor must be counted as stalled, not treated as a clean reconnect.
//
// Before the fix, Run matched errStreamClosed alone, reset the backoff and the consecutive-failure
// count, and logged nothing -- so a stream could reconnect forever while replicating nothing
// durably. A CHTC hub ran 78 of these in one hour on one table with no error in the log.
func TestStalledSessionIsNotACleanReconnect(t *testing.T) {
	src := newRestartableSource(t)
	// Comfortably more rows than watchfeed's 64-event buffer. With only a handful, the whole
	// catch-up -- Synced included -- is buffered client-side the moment the watch opens, so
	// cutting the connection does not stop the session reaching Synced and the test passes or
	// fails on how fast the buffer filled.
	keys := make([]string, 500)
	for i := range keys {
		keys[i] = strconv.Itoa(i) + ".0"
	}
	src.put(keys...)

	ctx, cancel := context.WithCancel(context.Background())
	sink := &gateSink{ctx: ctx, got: make(chan replicate.Kind), release: make(chan struct{})}
	r, err := NewRunner(src.dial, Config{Source: "jobs", Src: "ap1"}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// Cut three sessions, each while the sink still holds the session's first change: it has not
	// reached Synced, so gateSink.Cursor() is still nil and nothing has been committed.
	for i := 1; i <= 3; i++ {
		select {
		case <-sink.got:
		case <-time.After(20 * time.Second):
			t.Fatalf("session %d delivered no change", i)
		}
		src.dropConns()
		// Let the session drain what was already buffered and end. Both directions are served:
		// Apply calls still held are released, and ones that arrive meanwhile are accepted, so a
		// change of the NEXT session cannot deadlock this loop.
		deadline := time.After(20 * time.Second)
		for r.Status().StalledSessions < i {
			select {
			case sink.release <- struct{}{}:
			case <-sink.got:
			case <-time.After(20 * time.Millisecond):
			case <-deadline:
				t.Fatalf("session %d never registered as stalled", i)
			}
		}
	}

	st := r.Status()
	if st.StalledSessions < 3 {
		t.Fatalf("StalledSessions = %d, want at least 3", st.StalledSessions)
	}
	if !st.LastProgress.IsZero() {
		t.Fatalf("LastProgress = %v, want zero: no session committed anything", st.LastProgress)
	}
	if !st.SyncedAt.IsZero() {
		t.Fatalf("SyncedAt = %v, want zero: no session reached Synced", st.SyncedAt)
	}
}

// TestSessionReachingSyncedClearsTheStall: a session that reaches Synced clears the stall count, so
// the gauge separates a stream that recovered from one that is still wedged.
func TestSessionReachingSyncedClearsTheStall(t *testing.T) {
	src := newRestartableSource(t)
	src.put("1.0")

	ctx, cancel := context.WithCancel(context.Background())
	sink := &gateSink{ctx: ctx, got: make(chan replicate.Kind), release: make(chan struct{})}
	pump(ctx, sink)
	r, err := NewRunner(src.dial, Config{Source: "jobs", Src: "ap1"}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.setStatus(func(s *Status) { s.StalledSessions = 7 }) // as if it had been wedged

	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, 10*time.Second, func() bool { return !r.Status().SyncedAt.IsZero() })
	src.dropConns() // end the session that reached Synced
	waitFor(t, 10*time.Second, func() bool {
		st := r.Status()
		return st.StalledSessions == 0 && !st.LastProgress.IsZero()
	})
}
