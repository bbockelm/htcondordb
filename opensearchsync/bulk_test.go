package opensearchsync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testCfg() Config {
	c := Config{Table: "history", Addresses: []string{"http://x"}}
	_ = c.Validate()
	return c
}

func batch(cursor string, ids ...string) Batch {
	b := Batch{Cursor: []byte(cursor)}
	for i, id := range ids {
		b.Docs = append(b.Docs, Doc{ID: id, Version: uint64(i + 1), Body: []byte(`{"x":1}`)})
	}
	return b
}

// firstDocID pulls the _id of the first action line in a bulk body (each batch in these tests
// uses a distinct first id, so it identifies the batch regardless of goroutine call order).
func firstDocID(ndjson []byte) string {
	line := ndjson
	if i := strings.IndexByte(string(ndjson), '\n'); i >= 0 {
		line = ndjson[:i]
	}
	var a bulkAction
	_ = json.Unmarshal(line, &a)
	return a.Index.ID
}

// gateClient blocks each call until the gate for its first doc id is released, so a test can
// force out-of-order completion and observe backpressure. Gates are pre-created per id.
type gateClient struct {
	gates     map[string]chan struct{}
	startedCh chan string
}

func newGateClient(ids ...string) *gateClient {
	g := &gateClient{gates: map[string]chan struct{}{}, startedCh: make(chan string, len(ids))}
	for _, id := range ids {
		g.gates[id] = make(chan struct{})
	}
	return g
}

func (g *gateClient) Bulk(ctx context.Context, index string, ndjson []byte) (BulkOutcome, error) {
	id := firstDocID(ndjson)
	g.startedCh <- id
	select {
	case <-g.gates[id]:
		return BulkOutcome{Indexed: 1}, nil
	case <-ctx.Done():
		return BulkOutcome{}, ctx.Err()
	}
}

func (g *gateClient) release(id string) { close(g.gates[id]) }

// scriptClient returns a scripted outcome per call (no blocking) — for retry/permanent/fatal.
type scriptClient struct {
	mu      sync.Mutex
	call    int
	outcome func(call int) (BulkOutcome, error)
}

func (s *scriptClient) Bulk(ctx context.Context, index string, ndjson []byte) (BulkOutcome, error) {
	s.mu.Lock()
	c := s.call
	s.call++
	s.mu.Unlock()
	return s.outcome(c)
}

// TestUploaderContiguousWatermark verifies the committed cursor advances only over the oldest
// fully-acked contiguous prefix, even when batches complete out of order.
func TestUploaderContiguousWatermark(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := newGateClient("a", "b", "c")
	cfg := testCfg()
	cfg.MaxConcurrentBulk = 3
	u := NewUploader(ctx, g, cfg, discardLog())

	_ = u.Submit(ctx, batch("C0", "a"))
	_ = u.Submit(ctx, batch("C1", "b"))
	_ = u.Submit(ctx, batch("C2", "c"))
	for i := 0; i < 3; i++ {
		<-g.startedCh
	}

	// Complete batch "b" (seq 1) first: committed must stay nil (seq 0 not done).
	g.release("b")
	waitFor(t, func() bool { return u.donesForTest() == 1 })
	if u.Committed() != nil {
		t.Fatalf("committed advanced past an un-acked prefix: %q", u.Committed())
	}
	// Complete "a" (seq 0): prefix [0,1] contiguous -> committed jumps to C1.
	g.release("a")
	waitFor(t, func() bool { return string(u.Committed()) == "C1" })
	// Complete "c" (seq 2): committed advances to C2.
	g.release("c")
	waitFor(t, func() bool { return string(u.Committed()) == "C2" })
	if err := u.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestUploaderBackpressure verifies Submit blocks once the concurrency cap is reached and
// unblocks when a request completes.
func TestUploaderBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := newGateClient("a", "b", "c")
	cfg := testCfg()
	cfg.MaxConcurrentBulk = 2
	u := NewUploader(ctx, g, cfg, discardLog())

	_ = u.Submit(ctx, batch("C0", "a"))
	_ = u.Submit(ctx, batch("C1", "b"))
	<-g.startedCh
	<-g.startedCh

	done := make(chan struct{})
	go func() { _ = u.Submit(ctx, batch("C2", "c")); close(done) }()
	select {
	case <-done:
		t.Fatal("Submit returned while at the concurrency cap; backpressure not applied")
	case <-time.After(100 * time.Millisecond):
	}
	g.release("a") // free one slot -> blocked Submit proceeds
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Submit stayed blocked after capacity freed")
	}
	g.release("b")
	<-g.startedCh // "c" now running
	g.release("c")
	_ = u.Drain(ctx)
}

// TestUploaderRetryThenConflict verifies transient (transport + 429/503) retries and that a
// 409 conflict counts as success (idempotent no-op).
func TestUploaderRetryThenConflict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &scriptClient{outcome: func(call int) (BulkOutcome, error) {
		switch call {
		case 0:
			return BulkOutcome{}, errors.New("connection reset")
		case 1:
			return BulkOutcome{Retryable: 1}, nil
		default:
			return BulkOutcome{Conflicts: 1}, nil
		}
	}}
	u := NewUploader(ctx, s, testCfg(), discardLog())
	u.initBackoff, u.maxBackoff = time.Millisecond, 2*time.Millisecond
	_ = u.Submit(ctx, batch("C0", "a"))
	waitFor(t, func() bool { return string(u.Committed()) == "C0" })
	if err := u.Fatal(); err != nil {
		t.Fatalf("unexpected fatal: %v", err)
	}
	if indexed, _ := u.Stats(); indexed != 1 {
		t.Errorf("indexed = %d, want 1 (409 counts as success)", indexed)
	}
}

// TestUploaderPermanentSkip verifies a permanent per-item error is counted/skipped and the
// batch still commits (a poison doc cannot wedge the sync).
func TestUploaderPermanentSkip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &scriptClient{outcome: func(int) (BulkOutcome, error) {
		return BulkOutcome{Indexed: 1, Permanent: []ItemError{{ID: "bad", Status: 400, Type: "mapper_parsing_exception", Reason: "boom"}}}, nil
	}}
	u := NewUploader(ctx, s, testCfg(), discardLog())
	_ = u.Submit(ctx, batch("C0", "ok", "bad"))
	waitFor(t, func() bool { return string(u.Committed()) == "C0" })
	if _, skipped := u.Stats(); skipped != 1 {
		t.Errorf("skipped = %d, want 1", skipped)
	}
	if err := u.Fatal(); err != nil {
		t.Errorf("permanent per-item error should not be fatal: %v", err)
	}
}

// TestUploaderFatalAfterRetries verifies a persistently-failing request becomes fatal after
// the retry budget and the committed cursor does not advance.
func TestUploaderFatalAfterRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &scriptClient{outcome: func(int) (BulkOutcome, error) { return BulkOutcome{}, errors.New("down") }}
	u := NewUploader(ctx, s, testCfg(), discardLog())
	u.initBackoff, u.maxBackoff = time.Millisecond, time.Millisecond
	_ = u.Submit(ctx, batch("C0", "a"))
	waitFor(t, func() bool { return u.Fatal() != nil })
	if u.Committed() != nil {
		t.Errorf("committed advanced despite a fatal batch: %q", u.Committed())
	}
}

// --- helpers ---

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func (u *Uploader) donesForTest() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return int(u.ackSeq) + len(u.doneCursors)
}
