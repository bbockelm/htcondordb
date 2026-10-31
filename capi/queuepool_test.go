package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/golang-htcondor/config"
)

// The pool is tested over real TCP sockets to a real dbrpc server, not over
// net.Pipe: a pipe is synchronous and in-process, so two "connections" over it are
// indistinguishable from one and every claim this file makes about spreading work
// across connections would pass vacuously.

// poolTestServer starts a dbrpc server over TCP and returns its address.
func poolTestServer(t *testing.T, ads map[string]string) string {
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(nc)) }()
		}
	}()
	return ln.Addr().String()
}

// poolDialer makes *conn values the way openConnAs does, minus the CEDAR handshake:
// the same transport watch, so the death detection under test is the shipping one.
type poolDialer struct {
	t     *testing.T
	addr  string
	dials atomic.Int64

	mu    sync.Mutex
	socks map[*conn]net.Conn // so a test can kill one connection's transport
}

func newPoolDialer(t *testing.T, addr string) *poolDialer {
	return &poolDialer{t: t, addr: addr, socks: map[*conn]net.Conn{}}
}

func (p *poolDialer) dial() (*conn, error) {
	p.dials.Add(1)
	nc, err := net.Dial("tcp", p.addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{addr: p.addr, ctx: ctx, cancel: cancel}
	c.attach(dbrpc.NewStreamConn(nc)) // the production wiring, so the watch is covered
	p.mu.Lock()
	p.socks[c] = nc
	p.mu.Unlock()
	p.t.Cleanup(func() { closeConn(c) })
	return c, nil
}

// kill drops a connection's socket from underneath it, the way a restarted daemon
// or a reset would.
func (p *poolDialer) kill(c *conn) {
	p.mu.Lock()
	nc := p.socks[c]
	p.mu.Unlock()
	if nc != nil {
		_ = nc.Close()
	}
}

// newPoolClient builds a queue client with one connection already up, the verdict
// Ready, and the given cap.
func newPoolClient(t *testing.T, d *poolDialer, capacity int) *queueClient {
	t.Helper()
	q := &queueClient{notifyFD: -1, dialFn: d.dial, backoff: reconnectFloor}
	if _, _, ok := q.ensureSession(); !ok {
		t.Fatalf("the first session did not come up")
	}
	q.mu.Lock()
	q.poolCap = capacity
	q.mu.Unlock()
	q.setState(hcdbQueueReady, "test")
	return q
}

// warmPool drives contended acquisitions until the pool reaches n connections.
//
// Growth is asynchronous on purpose -- the query that finds every connection busy
// shares one rather than waiting on a dial -- so a test that wants a warm pool has
// to ask for it the way production does: by being busy.
func warmPool(t *testing.T, q *queueClient, n int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for q.poolDepth() < n {
		if time.Now().After(deadline) {
			t.Fatalf("the pool reached %d connections, want %d", q.poolDepth(), n)
		}
		releases := make([]func(), 0, n)
		for i := 0; i < n; i++ {
			pc, release := q.acquire()
			if pc == nil {
				t.Fatalf("no session in the pool")
			}
			releases = append(releases, release)
		}
		for _, release := range releases {
			release()
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPoolGrowsToDistinctConnections(t *testing.T) {
	addr := poolTestServer(t, map[string]string{"1.0": "ClusterId = 1\nProcId = 0\nJobStatus = 1\nJobUniverse = 5"})
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 4)

	warmPool(t, q, 4)

	// Hold all four at once: that is the only way to see whether they are distinct
	// objects rather than the same one handed out four times.
	seen := map[*conn]bool{}
	var releases []func()
	for i := 0; i < 4; i++ {
		pc, release := q.acquire()
		if pc == nil {
			t.Fatalf("no session in the pool")
		}
		seen[pc.c] = true
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
	}
	if len(seen) != 4 {
		t.Fatalf("four concurrent acquisitions used %d distinct connections, want 4", len(seen))
	}
	if got := d.dials.Load(); got != 4 {
		t.Fatalf("%d dials, want 4 (one floor + three grown)", got)
	}
}

func TestPoolSharesAtCapWithoutBlocking(t *testing.T) {
	addr := poolTestServer(t, map[string]string{"1.0": "ClusterId = 1\nProcId = 0"})
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 1)

	start := time.Now()
	seen := map[*conn]bool{}
	var releases []func()
	for i := 0; i < 8; i++ {
		pc, release := q.acquire()
		if pc == nil {
			t.Fatalf("acquire returned no connection at the cap; it must share, not refuse")
		}
		seen[pc.c] = true
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
	}
	if len(seen) != 1 {
		t.Fatalf("a pool capped at one handed out %d connections", len(seen))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("eight acquisitions at the cap took %v; acquisition must not wait on anything", d)
	}
	if got := d.dials.Load(); got != 1 {
		t.Fatalf("%d dials, want 1: the cap must stop growth", got)
	}
}

func TestAcquireWithNoSessionReturnsNil(t *testing.T) {
	q := &queueClient{notifyFD: -1}
	if pc, _ := q.acquire(); pc != nil {
		t.Fatalf("acquire returned a connection from an empty pool")
	}
}

// contend runs n rounds of two overlapping acquisitions: the shape that asks the pool
// to grow.
func contend(q *queueClient, n int) {
	for i := 0; i < n; i++ {
		_, a := q.acquire()
		_, b := q.acquire()
		a()
		b()
	}
}

// dialsInFlight is the pool's own count of growth dials it has started and not yet
// heard back from. growLocked raises it before acquire returns, so unlike a counter
// bumped inside the dial goroutine it needs no waiting to be accurate.
func dialsInFlight(q *queueClient) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dialing
}

func TestGrowthOnlyWhileReady(t *testing.T) {
	addr := poolTestServer(t, map[string]string{"1.0": "ClusterId = 1"})
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 4)
	q.setState(hcdbQueueStale, "the mirror is behind")

	contend(q, 20)
	if got := dialsInFlight(q); got != 0 {
		t.Fatalf("%d growth dials started while the database was stale, want 0: the pool must not open sockets to a database we will not read", got)
	}
	if got := d.dials.Load(); got != 1 {
		t.Fatalf("%d dials while the database was stale, want 1: the pool must not open sockets to a database we will not read", got)
	}
}

func TestGrowthPacedAfterADialFailure(t *testing.T) {
	addr := poolTestServer(t, map[string]string{"1.0": "ClusterId = 1"})
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 8)

	// Every dial from here on fails, as it would against a daemon that just died --
	// but only when the test says so. Until then the dial hangs, the way a connect to
	// a host that has gone away does, which is the window in which nothing has yet
	// told the pool the daemon is gone.
	var dials atomic.Int64
	gate := make(chan struct{})
	var openGate sync.Once
	fail := func() { openGate.Do(func() { close(gate) }) }
	t.Cleanup(fail)
	q.dialFn = func() (*conn, error) {
		dials.Add(1)
		<-gate
		return nil, fmt.Errorf("connection refused")
	}

	// One contended acquisition starts one dial.
	contend(q, 1)
	if got := dialsInFlight(q); got != 1 {
		t.Fatalf("%d growth dials in flight after the first contention, want 1", got)
	}

	// Hundreds of contended queries while that dial is still out must not start
	// another: the pool does not yet know the daemon is down, and a dial per query in
	// that window is exactly the reconnect storm the backoff exists to prevent.
	contend(q, 500)
	if got := dialsInFlight(q); got != 1 {
		t.Fatalf("%d growth dials in flight while the first had not returned, want 1", got)
	}

	// The dial fails and arms the backoff. Wait for that, not for time to pass.
	fail()
	deadline := time.Now().Add(5 * time.Second)
	var armedUntil time.Time
	var backoff time.Duration
	for {
		q.mu.Lock()
		inFlight := q.dialing
		armedUntil, backoff = q.growAfter, q.growBackoff
		q.mu.Unlock()
		if inFlight == 0 && !armedUntil.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the failed dial never armed the growth backoff (in flight %d, after %v)", inFlight, armedUntil)
		}
		time.Sleep(time.Millisecond)
	}
	// One failure is one step of backoff; a burst of parallel failures would have
	// doubled it once each.
	if backoff != reconnectFloor {
		t.Fatalf("the growth backoff is %v after one failed dial, want %v", backoff, reconnectFloor)
	}

	// Hundreds of contended queries inside the backoff window must not add a single
	// dial: a pool that redialled per query would turn one outage into a storm.
	contend(q, 500)
	if time.Now().After(armedUntil) {
		t.Fatalf("the contended loop outlasted the %v backoff window, so it no longer tests the window", backoff)
	}
	if got := dialsInFlight(q); got != 0 {
		t.Fatalf("%d growth dials started inside the backoff window, want 0", got)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("%d dials across the failure and its backoff window, want 1", got)
	}
}

func TestADialThatLandsAfterATeardownIsDiscarded(t *testing.T) {
	addr := poolTestServer(t, map[string]string{"1.0": "ClusterId = 1"})
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 4)

	release := make(chan struct{})
	dialed := make(chan *conn, 1)
	q.dialFn = func() (*conn, error) {
		<-release
		c, err := d.dial()
		dialed <- c
		return c, err
	}

	_, r1 := q.acquire()
	_, r2 := q.acquire()
	r1()
	r2()
	// The supervisor gives up on the database while that dial is still out.
	q.drop()
	close(release)

	c := <-dialed
	deadline := time.Now().Add(5 * time.Second)
	for !c.dead.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if q.poolDepth() != 0 {
		t.Fatalf("a dial that finished after the teardown revived the pool (%d connections)", q.poolDepth())
	}
	if !c.dead.Load() {
		t.Fatalf("the orphaned connection was not closed")
	}
}

func TestReapIdleKeepsTheHeadAndAnythingInUse(t *testing.T) {
	addr := poolTestServer(t, map[string]string{"1.0": "ClusterId = 1"})
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 4)
	warmPool(t, q, 4)

	q.mu.Lock()
	head := q.pool[0].c
	busy := q.pool[2]
	q.mu.Unlock()
	busy.inUse = 1 // as if a query were streaming on it

	q.reapIdle(time.Now().Add(2 * poolIdleTimeout))

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pool) != 2 {
		t.Fatalf("the pool kept %d connections, want 2 (the head and the busy one)", len(q.pool))
	}
	if q.pool[0].c != head {
		t.Fatalf("reaping took the head; reconnection has one owner and it must keep its session")
	}
	if q.pool[1] != busy {
		t.Fatalf("reaping took a connection with a query on it")
	}
	if head.dead.Load() {
		t.Fatalf("the head's transport was closed")
	}
}

// poolQuery runs one full dump over pc and returns the ads it produced.
func poolQuery(t *testing.T, q *queueClient, pc *poolConn) (int, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(pc.c.ctx)
	defer cancel()
	r := &queueRequest{
		client:   q,
		chunks:   make(chan queueChunk, chunksInFlight),
		cancel:   cancel,
		connDead: func() bool { return pc.c.dead.Load() },
	}
	r.wg.Add(1)
	go r.stream(ctx, pc.c.dbc, dbrpc.DefaultTable, "true", nil, 0, &adRecordBuilder{emit: true}, true)
	go func() { r.wg.Wait(); close(r.chunks) }()
	ads := 0
	for ch := range r.chunks {
		ads += ch.ads
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return ads, r.err
}

func TestConcurrentQueriesStreamOnDistinctConnections(t *testing.T) {
	const (
		jobs = 4000
		k    = 4
	)
	ads := map[string]string{}
	for i := 0; i < jobs; i++ {
		ads[fmt.Sprintf("job_%d.0", i)] = benchAd(i, 0, 1+i%5, 5)
	}
	addr := poolTestServer(t, ads)
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, k)
	warmPool(t, q, k)

	var mu sync.Mutex
	used := map[*conn]int{}
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc, release := q.acquire()
			if pc == nil {
				t.Errorf("no session in the pool")
				return
			}
			defer release()
			mu.Lock()
			used[pc.c]++
			mu.Unlock()
			got, err := poolQuery(t, q, pc)
			if err != nil {
				t.Errorf("query failed: %v", err)
			}
			if got != jobs {
				t.Errorf("got %d ads, want %d", got, jobs)
			}
		}()
	}
	wg.Wait()

	if len(used) != k {
		t.Fatalf("%d concurrent queries ran on %d distinct connections, want %d: %v", k, len(used), k, used)
	}
}

// cutProxy forwards one TCP connection and can sever it on demand.
type cutProxy struct {
	addr  string
	socks chan net.Conn
}

func newCutProxy(t *testing.T, backend string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	p := &cutProxy{addr: ln.Addr().String(), socks: make(chan net.Conn, 8)}
	go func() {
		for {
			front, err := ln.Accept()
			if err != nil {
				return
			}
			back, err := net.Dial("tcp", backend)
			if err != nil {
				_ = front.Close()
				return
			}
			p.socks <- front
			p.socks <- back
			go func() { _, _ = io.Copy(back, front); _ = back.Close() }()
			go func() { _, _ = io.Copy(front, back); _ = front.Close() }()
		}
	}()
	return p
}

func (p *cutProxy) cut() {
	for {
		select {
		case c := <-p.socks:
			_ = c.Close()
		default:
			return
		}
	}
}

func TestAConnectionDyingMidQueryFailsIt(t *testing.T) {
	// Enough ads that the result spans many chunks, so there is a real middle to
	// die in. dbrpc ends a stream by closing its channel and fails one the same
	// way, so without the transport watch this query would come back short and
	// successful -- the exact silent truncation this asserts against.
	const jobs = 20000
	ads := map[string]string{}
	for i := 0; i < jobs; i++ {
		ads[fmt.Sprintf("job_%d.0", i)] = benchAd(i, 0, 1+i%5, 5)
	}
	backend := poolTestServer(t, ads)
	proxy := newCutProxy(t, backend)
	d := newPoolDialer(t, proxy.addr)
	q := newPoolClient(t, d, 2)

	pc, release := q.acquire()
	if pc == nil {
		t.Fatalf("no session in the pool")
	}
	defer release()

	ctx, cancel := context.WithCancel(pc.c.ctx)
	defer cancel()
	r := &queueRequest{
		client:   q,
		chunks:   make(chan queueChunk, chunksInFlight),
		cancel:   cancel,
		connDead: func() bool { return pc.c.dead.Load() },
	}
	r.wg.Add(1)
	go r.stream(ctx, pc.c.dbc, dbrpc.DefaultTable, "true", nil, 0, &adRecordBuilder{emit: true}, true)
	go func() { r.wg.Wait(); close(r.chunks) }()

	got, chunks := 0, 0
	for ch := range r.chunks {
		got += ch.ads
		chunks++
		if chunks == 1 {
			proxy.cut()
		}
	}
	r.mu.Lock()
	err := r.err
	r.mu.Unlock()

	if got >= jobs {
		t.Skipf("the whole result arrived before the cut (%d ads); nothing was interrupted", got)
	}
	if err == nil {
		t.Fatalf("a query that lost its connection after %d of %d ads reported success", got, jobs)
	}
	if !pc.c.dead.Load() {
		t.Fatalf("the connection was not marked dead")
	}
}

func TestReconnectWhileOtherConnectionsAreInUse(t *testing.T) {
	const jobs = 2000
	ads := map[string]string{}
	for i := 0; i < jobs; i++ {
		ads[fmt.Sprintf("job_%d.0", i)] = benchAd(i, 0, 1, 5)
	}
	addr := poolTestServer(t, ads)
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 3)
	warmPool(t, q, 3)

	// One connection is busy serving a query for the whole of this test.
	busy, release := q.acquire()
	defer release()

	q.mu.Lock()
	var victim *conn
	for _, pc := range q.pool {
		if pc != busy {
			victim = pc.c
			break
		}
	}
	q.mu.Unlock()
	d.kill(victim)
	deadline := time.Now().Add(5 * time.Second)
	for !victim.dead.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// The supervisor's pass: the daemon is fine, so the probe succeeds and only the
	// dead socket goes.
	healthy := classad.New()
	healthy.InsertAttrBool("JobQueueCaughtUp", true)
	q.probeFn = func(context.Context, *config.Config, string) (*classad.ClassAd, error) { return healthy, nil }
	q.cycle(context.Background())

	if q.state.Load() != hcdbQueueReady {
		t.Fatalf("state = %d after a healthy probe, want ready", q.state.Load())
	}
	if got := q.poolDepth(); got != 2 {
		t.Fatalf("the pool holds %d connections after one died, want 2", got)
	}
	// The connection that was in use all along still answers.
	if got, err := poolQuery(t, q, busy); err != nil || got != jobs {
		t.Fatalf("the surviving connection returned %d ads, err %v; want %d ads", got, err, jobs)
	}
	// And the pool grows back when it is contended again.
	warmPool(t, q, 3)
}

func TestNoGoroutineOrDescriptorLeakAcrossManyQueries(t *testing.T) {
	const jobs = 200
	ads := map[string]string{}
	for i := 0; i < jobs; i++ {
		ads[fmt.Sprintf("job_%d.0", i)] = benchAd(i, 0, 1, 5)
	}
	addr := poolTestServer(t, ads)
	d := newPoolDialer(t, addr)
	q := newPoolClient(t, d, 4)
	warmPool(t, q, 4)

	run := func(n int) {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				pc, release := q.acquire()
				if pc == nil {
					t.Errorf("no session in the pool")
					return
				}
				defer release()
				if got, err := poolQuery(t, q, pc); err != nil || got != jobs {
					t.Errorf("query returned %d ads, err %v", got, err)
				}
			}()
		}
		wg.Wait()
	}

	// A first batch settles whatever the runtime and the server start lazily; the
	// baseline is taken after it, so the comparison is about the steady state.
	run(40)
	settle()
	baseGo, baseFD := runtime.NumGoroutine(), openDescriptors(t)

	run(400)
	settle()
	if got := q.poolDepth(); got > 4 {
		t.Fatalf("the pool holds %d connections, over its cap of 4", got)
	}
	goroutines, fds := runtime.NumGoroutine(), openDescriptors(t)
	// Slack for the runtime's own bookkeeping, not for a per-query leak: 400 queries
	// leaking anything at all would be far past these.
	if goroutines > baseGo+10 {
		t.Errorf("goroutines went from %d to %d over 400 queries", baseGo, goroutines)
	}
	if fds > baseFD+8 {
		t.Errorf("open descriptors went from %d to %d over 400 queries", baseFD, fds)
	}
}

// settle waits for goroutines parked on their way out to finish.
func settle() {
	for i := 0; i < 50; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	runtime.GC()
}

// openDescriptors counts this process's open files. /dev/fd is the one spelling
// darwin and linux agree on.
func openDescriptors(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Skipf("cannot count descriptors: %v", err)
	}
	return len(ents)
}

func TestQueuePoolSizeFromConfig(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"", defaultQueuePoolSize},
		{"1", 1},
		{"12", 12},
		{"0", defaultQueuePoolSize},
		{"-3", defaultQueuePoolSize},
		{"lots", defaultQueuePoolSize},
		{"4000", maxQueuePoolSize},
	} {
		body := ""
		if tc.value != "" {
			body = queuePoolSizeParam + " = " + tc.value + "\n"
		}
		if got := queuePoolSize(testConfig(t, body)); got != tc.want {
			t.Errorf("%s = %q gave a pool size of %d, want %d", queuePoolSizeParam, tc.value, got, tc.want)
		}
	}
	if got := queuePoolSize(nil); got != defaultQueuePoolSize {
		t.Errorf("no configuration gave a pool size of %d, want %d", got, defaultQueuePoolSize)
	}
}
