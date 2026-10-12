package cedarsync

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/watchfeed"
)

// restartableSource is a source catalog with one "jobs" table, served over dbrpc on in-memory
// pipes, that can be stopped and reopened: a reopened catalog has a new watch epoch, so a resuming
// Runner gets a Reset.
type restartableSource struct {
	t   *testing.T
	dir string

	mu    sync.Mutex
	cat   *db.Catalog
	srv   *dbrpc.Server
	conns []net.Conn
	wg    sync.WaitGroup
}

func newRestartableSource(t *testing.T) *restartableSource {
	s := &restartableSource{t: t, dir: t.TempDir()}
	s.open()
	t.Cleanup(s.stop)
	return s
}

func (s *restartableSource) open() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cat, err := db.OpenCatalog(s.dir)
	if err != nil {
		s.t.Fatal(err)
	}
	if _, err := cat.CreateTable("jobs"); err != nil {
		s.t.Fatal(err)
	}
	s.cat, s.srv = cat, dbrpc.NewServerCatalog(cat)
}

func (s *restartableSource) stop() {
	s.mu.Lock()
	cat, srv, conns := s.cat, s.srv, s.conns
	s.cat, s.srv, s.conns = nil, nil, nil
	s.mu.Unlock()
	if cat == nil {
		return
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	srv.Close()
	_ = cat.Close()
}

// dropConns drops every session without restarting the source: a resuming Runner reconnects into
// the same watch epoch and resumes from its cursor, with no Reset.
func (s *restartableSource) dropConns() {
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (s *restartableSource) dial(context.Context) (*dbrpc.Client, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv == nil {
		return nil, nil, errors.New("source down")
	}
	cp, sp := net.Pipe()
	s.conns = append(s.conns, cp, sp)
	srv := s.srv
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = srv.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true})
	}()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return c, func() { _ = c.Close() }, nil
}

func (s *restartableSource) put(keys ...string) {
	s.t.Helper()
	s.mu.Lock()
	d, _ := s.cat.Table("jobs")
	s.mu.Unlock()
	tx := d.Begin()
	for _, k := range keys {
		ad := classad.New()
		ad.InsertAttrString("Key", k)
		tx.NewClassAd(k, ad)
	}
	if err := tx.Commit(); err != nil {
		s.t.Fatal(err)
	}
}

// gateSink hands each change to the test and holds Apply until the test releases it, so the test
// can read the Runner's Status at every point of a session.
type gateSink struct {
	ctx     context.Context
	got     chan replicate.Kind
	release chan struct{}

	mu  sync.Mutex
	cur []byte
}

func (g *gateSink) Apply(c replicate.Change) error {
	select {
	case g.got <- c.Kind:
	case <-g.ctx.Done():
		return g.ctx.Err()
	}
	select {
	case <-g.release:
	case <-g.ctx.Done():
		return g.ctx.Err()
	}
	if c.Kind == replicate.KindSynced && len(c.Cursor) > 0 {
		g.mu.Lock()
		g.cur = append([]byte(nil), c.Cursor...)
		g.mu.Unlock()
	}
	return nil
}
func (g *gateSink) Commit([]byte) error { return nil }
func (g *gateSink) Cursor() []byte {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]byte(nil), g.cur...)
}

// TestStatusSyncedOverSessions: Status.Synced is false from the moment a session opens, through a
// partial replay, until the sink has applied Synced; it stays true on the live tail; a resumed
// session (no Reset) is false through its catch-up overlap until its Synced; it turns false when the
// session drops (and stays false while the source is down); and a reconnect whose replay is a Reset
// is false again until that replay reaches Synced.
func TestStatusSyncedOverSessions(t *testing.T) {
	src := newRestartableSource(t)
	src.put("1.0", "2.0", "3.0")

	ctx, cancel := context.WithCancel(context.Background())
	sink := &gateSink{ctx: ctx, got: make(chan replicate.Kind), release: make(chan struct{})}
	r, err := NewRunner(src.dial, Config{Source: "jobs", Src: "ap1"}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// next waits for the Runner to hand the sink a change of kind want, checks Status.Synced while
	// the sink holds it (before Apply returns), and releases it.
	next := func(want replicate.Kind, wantSynced bool, what string) {
		t.Helper()
		select {
		case k := <-sink.got:
			if k != want {
				t.Fatalf("%s: sink got %s, want %s", what, k, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: no %s delivered", what, want)
		}
		if st := r.Status(); st.Synced != wantSynced || !st.Connected {
			t.Errorf("%s: Status during %s = connected %v synced %v, want connected and synced %v",
				what, want, st.Connected, st.Synced, wantSynced)
		}
		sink.release <- struct{}{}
	}
	replay := func(session string, rows int) {
		t.Helper()
		next(replicate.KindReset, false, session+" start")
		for i := 0; i < rows; i++ {
			next(replicate.KindUpsert, false, session+" partial replay")
		}
		next(replicate.KindSynced, false, session+" Synced, not yet applied")
		waitFor(t, 5*time.Second, func() bool { return r.Status().Synced })
	}

	replay("first session", 3)
	first := r.Status().SyncedAt
	if first.IsZero() {
		t.Fatal("SyncedAt not set when the session reached Synced")
	}

	// The live tail keeps it.
	src.put("4.0")
	next(replicate.KindUpsert, true, "live tail")
	if !r.Status().Synced {
		t.Error("a live upsert cleared Synced")
	}

	// The connection drops and the Runner resumes from its cursor (taken at Synced, so the live
	// upsert after it is re-delivered): no Reset, and not Synced until the resume's Synced.
	src.dropConns()
	for kind := replicate.Kind(""); kind != replicate.KindSynced; {
		select {
		case kind = <-sink.got:
		case <-time.After(10 * time.Second):
			t.Fatal("resumed session: no Synced delivered")
		}
		if kind == replicate.KindReset {
			t.Fatal("a resume into the same watch epoch was answered with a Reset; the test does not cover a resume")
		}
		if st := r.Status(); st.Synced || !st.Connected {
			t.Errorf("resumed session, %s before its Synced applied: connected %v synced %v", kind, st.Connected, st.Synced)
		}
		sink.release <- struct{}{}
	}
	waitFor(t, 5*time.Second, func() bool { return r.Status().Synced })
	resumed := r.Status().SyncedAt
	if !resumed.After(first) {
		t.Errorf("SyncedAt %v not moved by the resume's Synced (first %v)", resumed, first)
	}

	// The source goes away: the session drops, and a Runner that cannot reconnect is not Synced.
	src.stop()
	waitFor(t, 5*time.Second, func() bool { st := r.Status(); return !st.Connected && !st.Synced })
	if got := r.Status().SyncedAt; !got.Equal(resumed) {
		t.Errorf("SyncedAt changed on disconnect: %v -> %v", resumed, got)
	}

	// It comes back with a new watch epoch: the resume is answered with a Reset and a replay.
	src.open()
	replay("reconnect", 4)
	if got := r.Status().SyncedAt; !got.After(resumed) {
		t.Errorf("SyncedAt %v not after the previous session's %v", got, resumed)
	}
}

// failSink fails every Synced.
type failSink struct{ flushSink }

func (f *failSink) Apply(c replicate.Change) error {
	if c.Kind == replicate.KindSynced {
		return errors.New("sweep failed (test)")
	}
	return f.flushSink.Apply(c)
}

// TestStatusSyncedResetAndResyncMidSession: within one session, a Reset or a Resync after Synced
// sends the Runner back into catch-up (Synced false) until the next Synced; and a Synced the sink
// fails to apply does not count.
func TestStatusSyncedResetAndResyncMidSession(t *testing.T) {
	nodial := func(context.Context) (*dbrpc.Client, func(), error) { return nil, nil, errors.New("unused") }
	r, err := NewRunner(nodial, Config{Source: "jobs", Src: "ap1"}, &flushSink{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ver uint64
	step := func(kind uint8, wantSynced bool, what string) {
		t.Helper()
		ev := watchfeed.Event{Kind: kind, Cursor: []byte("c")}
		if kind == 0 {
			ev.Key, ev.Ad = "1.0", classad.New()
		}
		if err := r.deliver(ev, &ver); err != nil {
			t.Fatal(err)
		}
		if got := r.Status().Synced; got != wantSynced {
			t.Errorf("after %s: Synced = %v, want %v", what, got, wantSynced)
		}
	}
	// db.WatchKind: 0 upsert, 2 reset, 3 synced, 4 resync.
	step(2, false, "Reset")
	step(0, false, "replayed upsert")
	step(3, true, "Synced")
	step(0, true, "live upsert")
	step(2, false, "a second Reset")
	step(0, false, "its partial replay")
	step(3, true, "its Synced")
	step(4, false, "Resync")
	step(3, true, "Synced after the Resync")

	r, err = NewRunner(nodial, Config{Source: "jobs", Src: "ap1"}, &failSink{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.deliver(watchfeed.Event{Kind: 2}, &ver)
	if err := r.deliver(watchfeed.Event{Kind: 3, Cursor: []byte("c")}, &ver); err == nil {
		t.Fatal("failing Synced did not fail")
	}
	if st := r.Status(); st.Synced || !st.SyncedAt.IsZero() {
		t.Errorf("a Synced the sink failed to apply counted: %+v", st)
	}
}
