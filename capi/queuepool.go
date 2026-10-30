// The queue client's connection pool.
//
// Every query used to stream over one shared dbrpc session. A session multiplexes,
// so that was correct -- but one session is one socket, one client-side read loop
// decoding every frame, and one server-side write lock, and a job-queue dump is
// megabytes of ads. Wherever that session is the narrow part, concurrent condor_q
// invocations queue behind each other: measured against a 100k-job mirror, eight
// concurrent full dumps took 5994 ms through one session and 3950 ms over eight.
//
// How much there is to win depends on the machine, and it is worth being plain
// about that. A single full dump already spreads the database's own scan across
// several cores, so against a daemon sharing the client's cores eight concurrent
// dumps saturate the box and extra sessions change little -- measured on a 12-core
// laptop with the daemon alongside the client, the difference is inside the noise.
// The win is where the session is the limit and not the CPU: a mirror on another
// host, or a submit node with cores to spare. Hence a low default and a knob.
//
// So the client keeps several sessions and spreads queries over them. Three rules
// shape the design, all of them from the daemon this serves:
//
//   - A query never waits for a connection. Acquisition is a mutex and a scan of a
//     handful of slots; when every connection is busy the query shares one, which is
//     exactly what every query did before this file existed. The pool is an
//     optimisation, never a dependency, so there is no exhaustion path to get wrong
//     and no way for it to add latency to the single-query case.
//   - Connections are dialled on demand, not up front. A schedd that serves one
//     condor_q at a time should hold one session, not eight; dialling costs an
//     authenticated CEDAR handshake and a file descriptor at both ends.
//   - Dialling stays paced. The supervisor keeps the first connection up with its
//     own clock-based backoff; growth beyond it happens only while the database is
//     Ready, only under real contention, one dial at a time, and stops for a backoff
//     window after a failed dial. One database outage must not become N reconnect
//     storms.
package main

import (
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/bbockelm/golang-htcondor/config"
)

// How large the pool may grow, and the knob that changes it.
//
// Four is the default because the gain is in the first few connections and the cost
// is linear: the measured 8-way spread recovered about a third of the single-session
// time on the deployment that motivated this, and the database stops scaling well
// before the pool would, since the same scan machinery is shared behind every
// session. Four covers the concurrency a real schedd sees (a handful of simultaneous
// condor_q, not dozens) while holding four descriptors and four authenticated
// sessions instead of one, and nothing is dialled until queries actually contend --
// so a schedd that never serves two at once still holds exactly one.
//
// The knob exists because the right number is a property of the deployment, not of
// this library: a busy submit host behind a well-provisioned mirror can afford more.
// It is read from the HTCondor configuration rather than passed in, so an operator
// can change it without rebuilding the daemon that dlopens this, and so that the
// exported C signature stays put.
const (
	queuePoolSizeParam   = "HTCONDORDB_QUEUE_POOL_SIZE"
	defaultQueuePoolSize = 4
	maxQueuePoolSize     = 64
)

// poolIdleTimeout is how long a grown connection may sit unused before it is closed.
// Long enough that a user running condor_q repeatedly keeps the pool warm, short
// enough that one burst does not pin descriptors and server-side goroutines for the
// rest of the daemon's life.
const poolIdleTimeout = 2 * time.Minute

// errConnLost is what a query gets when its connection died while it was streaming.
var errConnLost = errors.New("the database connection closed while the query was streaming; the result would have been incomplete")

// poolConn is one session plus the bookkeeping that decides who streams on it.
type poolConn struct {
	c     *conn
	inUse int // queries streaming on it right now
	// idleSince is when inUse last fell to zero (or when the connection was made,
	// for one that has never been used). Only meaningful while inUse == 0.
	idleSince time.Time
}

// queuePoolSize reads the cap from the configuration, clamped to something sane. An
// unparseable or absurd value takes the default rather than failing the connection:
// a typo in a config file must not stop a schedd answering condor_q.
func queuePoolSize(cfg *config.Config) int {
	n := defaultQueuePoolSize
	if cfg != nil {
		if v, ok := cfg.Get(queuePoolSizeParam); ok {
			if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && parsed > 0 {
				n = parsed
			} else {
				slog.Warn("ignoring an unusable queue pool size",
					"param", queuePoolSizeParam, "value", v, "using", n)
			}
		}
	}
	return min(max(n, 1), maxQueuePoolSize)
}

// dial opens one session. The indirection is a test seam: everything else in this
// file can be exercised against a plain socket, but a real dial needs a CEDAR
// handshake against a live daemon, which no unit test has.
func (q *queueClient) dial() (*conn, error) {
	if q.dialFn != nil {
		return q.dialFn()
	}
	return openConnAs(q.addr, q.subsys)
}

// acquire hands a query the connection it will stream on, plus the release to call
// when it is finished. It never blocks and it never dials on the caller's goroutine.
//
// It returns nil only when there is no session at all, which is the same "no session
// to the database" the single-session client reported. When every connection is busy
// the least-loaded one is shared: sharing is merely slower, and making a query wait
// for a dial -- or fail because one failed -- would be worse than the thing the pool
// is here to fix.
func (q *queueClient) acquire() (*poolConn, func()) {
	now := time.Now()
	q.mu.Lock()
	defer q.mu.Unlock()
	q.evictDeadLocked()
	if len(q.pool) == 0 {
		return nil, nil
	}
	best := q.pool[0]
	for _, pc := range q.pool[1:] {
		if pc.inUse < best.inUse {
			best = pc
		}
	}
	if best.inUse > 0 {
		// Real contention, which is the only evidence that another session would be
		// used. The connection it produces serves the NEXT query; this one does not
		// wait for it.
		q.growLocked(now)
	}
	best.inUse++
	return best, func() { q.release(best) }
}

// release gives a connection back.
func (q *queueClient) release(pc *poolConn) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if pc.inUse > 0 {
		pc.inUse--
	}
	if pc.inUse == 0 {
		pc.idleSince = time.Now()
	}
}

// capLocked is how many connections the pool may hold. It is 0 until something has
// connected, because the cap comes from that connection's configuration; before then
// the default stands. Caller holds q.mu.
func (q *queueClient) capLocked() int {
	if q.poolCap <= 0 {
		return defaultQueuePoolSize
	}
	return q.poolCap
}

// growLocked starts one more connection if the pool may have one. Caller holds q.mu.
func (q *queueClient) growLocked(now time.Time) {
	if q.state.Load() != hcdbQueueReady {
		// Not the moment to open sockets: the supervisor is either still establishing
		// the first session or has just judged the database unusable.
		return
	}
	if len(q.pool)+q.dialing >= q.capLocked() {
		return
	}
	if q.dialing > 0 {
		// One growth dial at a time. The backoff is armed only when a dial fails, so
		// while one is still out nothing yet says the daemon is gone: without this,
		// every contended query in that window -- and a dial to a dead host can hang
		// for a connect timeout -- would start another, up to the cap, and each of
		// those failures would double the backoff again. Waiting for the outcome costs
		// only ramp-up speed; no query ever waits on a dial.
		return
	}
	if now.Before(q.growAfter) {
		return // a growth dial failed recently; let the backoff run out
	}
	q.dialing++
	go q.grow(q.poolGen)
}

// grow dials one connection and, if the pool still wants it, adds it.
//
// gen is the pool generation at the moment the dial started. The supervisor bumps it
// whenever it tears the pool down, so a connection that arrives after that teardown
// is closed rather than quietly reviving a pool the supervisor has written off.
func (q *queueClient) grow(gen uint64) {
	c, err := q.dial()

	q.mu.Lock()
	q.dialing--
	switch {
	case err != nil:
		q.growBackoff = min(max(q.growBackoff*2, reconnectFloor), reconnectCeiling)
		q.growAfter = time.Now().Add(q.growBackoff)
		backoff := q.growBackoff
		q.mu.Unlock()
		slog.Debug("queue pool dial failed", "err", err, "retry_after", backoff)
		return
	case gen != q.poolGen, len(q.pool) >= q.capLocked():
		q.mu.Unlock()
		closeConn(c)
		return
	}
	q.growBackoff = 0
	q.pool = append(q.pool, &poolConn{c: c, idleSince: time.Now()})
	n := len(q.pool)
	q.mu.Unlock()
	slog.Debug("queue pool grew", "connections", n)
}

// evictDeadLocked drops connections whose transport has failed. Caller holds q.mu.
//
// The pool makes this necessary: the supervisor's health probe is its own short-lived
// CEDAR exchange, so it keeps reporting a healthy daemon while one pooled socket lies
// dead, and that socket would otherwise go on being handed to one query in N forever.
// Requests still on it have already been failed -- their context is the connection's,
// and the transport watch cancelled it -- so there is nothing to wait for.
func (q *queueClient) evictDeadLocked() {
	keep := q.pool[:0]
	var closing []*conn
	for _, pc := range q.pool {
		if pc.c.dead.Load() {
			closing = append(closing, pc.c)
			continue
		}
		keep = append(keep, pc)
	}
	if len(closing) == 0 {
		return
	}
	for i := len(keep); i < len(q.pool); i++ {
		q.pool[i] = nil
	}
	q.pool = keep
	// Closing a socket is a syscall on a connection nobody can use; doing it on
	// another goroutine keeps it off the path of the query that noticed.
	go func() {
		for _, c := range closing {
			closeConn(c)
		}
	}()
}

// reapIdle closes grown connections that have gone unused, as of now.
//
// It runs on the supervisor's goroutine, so it cannot race the supervisor's own
// dialling, and it leaves the head of the pool alone: that is the session the
// supervisor established and the one it re-establishes, and keeping reconnection to a
// single owner is what stops reaping and reconnecting from fighting. Everything above
// it is the pool's to shed, which also guarantees a floor of one connection -- the
// shape the client had before it pooled anything.
func (q *queueClient) reapIdle(now time.Time) {
	q.mu.Lock()
	keep := q.pool[:0]
	var closing []*conn
	for i, pc := range q.pool {
		if i > 0 && pc.inUse == 0 && now.Sub(pc.idleSince) >= poolIdleTimeout {
			closing = append(closing, pc.c)
			continue
		}
		keep = append(keep, pc)
	}
	for i := len(keep); i < len(q.pool); i++ {
		q.pool[i] = nil
	}
	q.pool = keep
	q.mu.Unlock()
	for _, c := range closing {
		closeConn(c)
	}
	if len(closing) > 0 {
		slog.Debug("queue pool reaped idle connections", "closed", len(closing), "remaining", len(keep))
	}
}

// poolDepth reports how many connections are up. For tests and diagnostics.
func (q *queueClient) poolDepth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pool)
}
