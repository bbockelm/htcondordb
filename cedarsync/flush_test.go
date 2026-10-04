package cedarsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db/replicate"
)

// flushSink records the Runner's calls into a Flusher / SessionAware sink.
type flushSink struct {
	mu      sync.Mutex
	kinds   []replicate.Kind
	flushes int
	begins  int
	ends    int
	cur     []byte
}

func (f *flushSink) Apply(c replicate.Change) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kinds = append(f.kinds, c.Kind)
	return nil
}
func (f *flushSink) Commit([]byte) error { return nil }
func (f *flushSink) Cursor() []byte      { return f.cur }
func (f *flushSink) Flush() error        { f.mu.Lock(); f.flushes++; f.mu.Unlock(); return nil }
func (f *flushSink) BeginSession()       { f.mu.Lock(); f.begins++; f.mu.Unlock() }
func (f *flushSink) EndSession()         { f.mu.Lock(); f.ends++; f.mu.Unlock() }

func (f *flushSink) snapshot() (flushes, begins, ends int, kinds []replicate.Kind) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushes, f.begins, f.ends, append([]replicate.Kind(nil), f.kinds...)
}

// TestRunnerFlushesAndBracketsSessions: a Flusher sink is flushed on the interval even with no
// events arriving and once more when the session ends; a SessionAware sink sees BeginSession
// before the first event and EndSession after the last; Status tracks the connection.
func TestRunnerFlushesAndBracketsSessions(t *testing.T) {
	catA, archA := mustArchive(t, "history")
	appendOld(t, archA, `GlobalJobId = "ap40#1.0"; Owner = "alice"; ClusterId = 1`)
	sink := &flushSink{}
	r, err := NewRunner(dialFor(t, catA), Config{Source: "history", Src: "ap40", FlushInterval: 10 * time.Millisecond}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		fl, b, _, kinds := sink.snapshot()
		return b == 1 && fl >= 3 && len(kinds) >= 3 // reset, upsert, synced; then idle flushes
	})
	if !r.Status().Connected || r.Status().Sessions != 1 {
		t.Errorf("status = %+v, want connected with one session", r.Status())
	}
	_, _, _, kinds := sink.snapshot()
	if kinds[0] != replicate.KindReset || kinds[len(kinds)-1] != replicate.KindSynced {
		t.Errorf("events = %v", kinds)
	}
	before, _, _, _ := sink.snapshot()
	cancel()
	<-done
	after, _, ends, _ := sink.snapshot()
	if ends != 1 || after <= before {
		t.Errorf("session end: ends=%d flushes %d -> %d, want one EndSession after a final Flush", ends, before, after)
	}
	if r.Status().Connected {
		t.Error("status still connected after Run returned")
	}
}
