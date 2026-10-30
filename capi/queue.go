// The queue-query entry points: an asynchronous, wire-buffered read path for a
// C/C++ daemon that answers its own clients from this database.
//
// The condor_schedd is the motivating caller. It answers QUERY_JOB_ADS from its
// single-threaded DaemonCore event loop, so this API is shaped around three rules
// that loop imposes:
//
//   - Nothing blocks. hcdb_queue_open returns before a socket is dialled;
//     hcdb_queue_begin returns before a row is read; hcdb_queue_next returns
//     HCDB_QUEUE_PENDING rather than waiting. All the waiting happens on
//     goroutines the caller never sees.
//   - Completion is announced on a file descriptor the caller owns and already
//     has in its event loop, so it learns a chunk is ready without polling.
//   - The result is BYTES, already in HTCondor's on-the-wire ad framing (see
//     queuewire.go). The caller copies them to its client's socket; it never
//     parses an ad, builds one, or unparses one.
//
// Liveness is continuous and separate from any query: a supervisor goroutine
// keeps a session up and re-reads the daemon's sync-health ad on an interval,
// so the caller's "may I use the database for this query?" is a lock-free read of
// a cached verdict rather than a round trip.
//
// Queries are spread over a small pool of sessions rather than sharing one, since
// one session is one socket and one decode loop and a job-queue dump is megabytes
// of ads; see queuepool.go.
package main

/*
#include <stdlib.h>
// stdint.h for uintptr_t: glibc does not pull it in via stdlib.h the way macOS does, so
// without this the package builds on a Mac and fails on Linux with "could not determine
// what C.uintptr_t refers to".
#include <stdint.h>
*/
import "C"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/cgo"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/dbstatus"
)

// Liveness states reported by hcdb_queue_state. Only Ready means a query may be
// served from here; the others each say something different about why not, which
// an operator needs in a log line.
const (
	hcdbQueueDown       = 0 // no session: never connected, or the last one failed
	hcdbQueueConnecting = 1 // a session is being established right now
	hcdbQueueReady      = 2 // connected, and the mirror is current enough to read
	hcdbQueueStale      = 3 // connected, but the mirror is behind or mirrors something else
)

// Extra status codes for hcdb_queue_next, alongside hcdbOK and hcdbErr.
const (
	hcdbQueuePending = 1 // nothing ready yet; the caller should wait for a notification
	hcdbQueueDone    = 2 // the stream ended; no more chunks
)

// Flags for hcdb_queue_begin.
const (
	// hcdbQueueServerTime appends a ServerTime expression to every ad, matching
	// HTCondor's PUT_CLASSAD_SERVER_TIME.
	hcdbQueueServerTime = 1 << 0
	// hcdbQueueCounts also tallies the matching ads by (JobUniverse, JobStatus),
	// for the summary ad a job query ends with. The tally is taken from the rows
	// as they stream past, so it costs no second query and reflects exactly the
	// ads that were returned.
	hcdbQueueCounts = 1 << 1
	// hcdbQueueNoAds asks for the counts alone: a summary-only query returns no
	// ads, and reads only the two attributes it counts by.
	hcdbQueueNoAds = 1 << 2
)

// chunkTarget is how many bytes of ad records accumulate before a chunk is handed
// to the caller. Big enough that the per-chunk handoff and notification are noise
// next to the copying, small enough that the first ads reach the client long
// before the last row is read.
const chunkTarget = 1 << 20

// chunksInFlight bounds the producer: with the caller holding one chunk and this
// many queued, a slow client stalls the stream instead of buffering a whole job
// queue in memory.
const chunksInFlight = 2

// defaultProbeInterval is how often the supervisor re-reads the daemon's
// sync-health ad when the caller does not choose.
const defaultProbeInterval = 15 * time.Second

// reconnectFloor and reconnectCeiling bound the backoff between dial attempts. A
// database that is down must not be dialled every time the supervisor wakes, and
// must still be picked up promptly once it returns.
const (
	reconnectFloor   = 2 * time.Second
	reconnectCeiling = 60 * time.Second
)

// queueClient is one caller's long-lived view of the database: a session that
// reconnects itself, a cached liveness verdict, and the notification fd.
type queueClient struct {
	addr      string
	table     string
	subsys    string
	maxLagSec int64
	// wantSource, when set, is the job_queue.log path the caller expects the
	// mirror to be following. A database mirroring a DIFFERENT schedd answers
	// every query successfully and with the wrong jobs, which no freshness check
	// would catch -- so the caller names its own log and we refuse a mismatch.
	wantSource string

	notifyFD int

	// dialFn and probeFn are the two places this talks to the outside world, named
	// so a test can supply its own; both are nil in the shipping path. See dial()
	// and probe().
	dialFn  func() (*conn, error)
	probeFn func(context.Context, *config.Config, string) (*classad.ClassAd, error)

	mu sync.Mutex
	// pool is the set of live sessions, empty when none is up. The head is the
	// supervisor's; the rest are grown on demand and reaped when idle. See
	// queuepool.go.
	pool    []*poolConn
	poolCap int
	poolGen uint64 // bumped on teardown, so an in-flight dial knows it is obsolete
	dialing int    // growth dials in flight; at most one, see growLocked
	// Growth pacing, shared by every goroutine that may dial for the pool.
	growAfter   time.Time
	growBackoff time.Duration
	// cfg and connAddr are what the health probe needs: it is a separate CEDAR
	// command and cannot ride a pooled session, so it reuses the security policy and
	// the resolved address of whichever session established them first, rather than
	// re-reading CONDOR_CONFIG or depending on a particular pool member being alive.
	cfg      *config.Config
	connAddr string

	state  atomic.Int32
	detail atomic.Pointer[string]

	probeNow chan struct{}
	cancel   context.CancelFunc
	done     chan struct{}

	// Dial pacing for the pool's first connection, touched only by the supervisor.
	lastDial time.Time
	backoff  time.Duration
}

// setState records the verdict and the sentence explaining it.
func (q *queueClient) setState(s int32, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	q.detail.Store(&msg)
	q.state.Store(s)
}

// drop tears every session down so the next supervisor cycle redials.
//
// Queries still streaming go with them: each request's context descends from its
// connection's, so closing cancels them. That is deliberate and unchanged -- the
// reason to drop is that we no longer believe the database is answering, and a
// half-delivered job queue is worse than a reported failure.
func (q *queueClient) drop() {
	q.mu.Lock()
	pool := q.pool
	q.pool = nil
	// Any dial already in flight is for a pool that no longer exists.
	q.poolGen++
	q.mu.Unlock()
	for _, pc := range pool {
		closeConn(pc.c)
	}
}

// run is the supervisor: keep a session up, and keep the freshness verdict
// current. It is the only goroutine that dials.
func (q *queueClient) run(ctx context.Context, interval time.Duration) {
	defer close(q.done)
	for {
		q.cycle(ctx)
		wait := interval
		if q.state.Load() == hcdbQueueDown && q.backoff < interval {
			// Come back when the next dial is allowed rather than at the next
			// interval, so a database that returns is picked up promptly.
			wait = q.backoff
		}
		select {
		case <-ctx.Done():
			q.drop()
			return
		case <-time.After(wait):
		case <-q.probeNow:
		}
	}
}

// cycle connects if needed and refreshes the verdict.
//
// The backoff is measured against the clock, not against how often this is
// called, because hcdb_queue_probe lets the caller ask for a check at any
// moment: a daemon that nudges on every failed query must not turn into a dial
// per query against a database that is down.
func (q *queueClient) cycle(ctx context.Context) {
	cfg, addr, ok := q.ensureSession()
	if !ok {
		return // ensureSession has already set the verdict
	}

	ad, err := q.probe(ctx, cfg, addr)
	if err != nil {
		// A failed status read while we believe sessions are up is most often the
		// daemon itself having gone; drop them so a later cycle redials rather than
		// serving queries over sockets nobody has proven work.
		q.drop()
		q.setState(hcdbQueueDown, "reading sync health from %s: %v", addr, err)
		return
	}
	state, why := q.judge(ad)
	q.setState(state, "%s", why)
	q.reapIdle(time.Now())
}

// ensureSession makes sure the pool holds at least one connection, and reports the
// configuration and address the health probe should use.
//
// This is the only place the pool's floor is dialled, and it keeps the pacing the
// single-session client had: one dial attempt per backoff window, measured against
// the clock. Growth above the floor is a separate path with its own pacing, and it
// runs only while the verdict is Ready -- so a database that is down is dialled by
// this one goroutine at this one rate, pool or no pool.
func (q *queueClient) ensureSession() (*config.Config, string, bool) {
	q.mu.Lock()
	q.evictDeadLocked()
	have := len(q.pool)
	cfg, addr := q.cfg, q.connAddr
	q.mu.Unlock()
	if have > 0 {
		return cfg, addr, true
	}

	if !q.lastDial.IsZero() && time.Since(q.lastDial) < q.backoff {
		return nil, "", false // too soon to try again; the verdict stands
	}
	q.lastDial = time.Now()
	q.setState(hcdbQueueConnecting, "connecting to the database")
	nc, err := q.dial()
	if err != nil {
		q.backoff = min(q.backoff*2, reconnectCeiling)
		q.setState(hcdbQueueDown, "connecting to the database: %v", err)
		return nil, "", false
	}
	q.backoff = reconnectFloor

	q.mu.Lock()
	q.cfg, q.connAddr = nc.cfg, nc.addr
	// The cap comes from the configuration this session was built from, so it is
	// known only once something has connected. Nothing grows before that anyway.
	q.poolCap = queuePoolSize(nc.cfg)
	q.pool = append(q.pool, &poolConn{c: nc, idleSince: time.Now()})
	q.mu.Unlock()
	return nc.cfg, nc.addr, true
}

// probe reads the daemon's sync-health ad.
//
// It gets its own short-lived connection rather than borrowing one from the pool,
// and not by choice: the health ad answers a different CEDAR command than a dbrpc
// session speaks, so there is no pooled connection it could ride. That turns out to
// be the behaviour we want anyway -- the probe must be able to say "the daemon is
// there" while every pooled socket is busy with a full dump, or dead.
func (q *queueClient) probe(ctx context.Context, cfg *config.Config, addr string) (*classad.ClassAd, error) {
	if q.probeFn != nil {
		return q.probeFn(ctx, cfg, addr)
	}
	return dbstatus.Query(ctx, cfg, addr)
}

// judge decides whether the mirror behind this database may answer a live job
// query, from the sync-health ad the daemon publishes.
//
// The rule is the one golang-htcondor's dbmirror package applies when it routes a
// read away from the schedd, stated once more here because this library must not
// depend on that module: the database has to be mirroring a live job queue at all,
// it has to be ours, it has to have reached end of file on its last pass, and that
// pass has to be recent. Anything else is stale -- including a database that says
// nothing about a job queue, which is a history-only mirror and has no live jobs
// to serve.
func (q *queueClient) judge(ad *classad.ClassAd) (int32, string) {
	caughtUp, reported := ad.EvaluateAttrBool("JobQueueCaughtUp")
	if !reported {
		return hcdbQueueStale, "the database is not mirroring a job queue"
	}
	if q.wantSource != "" {
		src, _ := ad.EvaluateAttrString("JobQueueSource")
		if src != "" && src != q.wantSource {
			return hcdbQueueStale, fmt.Sprintf("the database mirrors %s, not %s", src, q.wantSource)
		}
	}
	if !caughtUp {
		lag, _ := ad.EvaluateAttrInt("JobQueueLagBytes")
		return hcdbQueueStale, fmt.Sprintf("the mirror is %d bytes behind the job queue log", lag)
	}
	if q.maxLagSec > 0 {
		secs, ok := ad.EvaluateAttrInt("JobQueueSecondsSinceSync")
		if !ok {
			return hcdbQueueStale, "the database does not report when it last read the job queue log"
		}
		if secs > q.maxLagSec {
			return hcdbQueueStale, fmt.Sprintf("the mirror last read the job queue log %ds ago (limit %ds)", secs, q.maxLagSec)
		}
	}
	return hcdbQueueReady, "the mirror is current with the job queue log"
}

// notify tells the caller's event loop that req has something to collect. The
// token is the request handle, so the caller dispatches straight to the right
// query without scanning its outstanding ones.
//
// A short or interrupted write is retried: losing a token would hang that query
// until its caller timed it out. A pipe whose reader has gone (EPIPE) is the
// caller shutting down, and the abandoned request is freed with everything else.
func (q *queueClient) notify(req uintptr) {
	if q.notifyFD < 0 {
		return
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(req))
	for off := 0; off < len(buf); {
		n, err := syscall.Write(q.notifyFD, buf[off:])
		if n > 0 {
			off += n
			continue
		}
		if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) {
			time.Sleep(time.Millisecond)
			continue
		}
		return
	}
}

// queueChunk is one handoff: ad records in wire framing, and how many ads they
// hold -- reported so a caller can meter progress or stop early without first
// walking the block.
type queueChunk struct {
	data []byte
	ads  int
}

// queueRequest is one in-flight query.
type queueRequest struct {
	client *queueClient
	self   uintptr // this request's own handle, for notifications

	chunks chan queueChunk
	cancel context.CancelFunc

	// sink, when set, is the client's socket and this request writes its own
	// response to it rather than queueing chunks for the caller (queuefd.go).
	sink    *fdSink
	written atomic.Int64 // ads put on the socket, for a sink request

	// detached marks a sink request the caller has already released while its
	// writer was still running: its handle is gone, so no completion is announced.
	detached atomic.Bool

	// connDead reports whether the connection this request streams on has lost its
	// transport. nil when there is nothing to ask -- a test driving stream directly
	// over a client it owns. See the check in stream for why it is needed.
	connDead func() bool

	mu     sync.Mutex
	err    error
	counts string // the (universe, status, count) triples, valid once finished
	wg     sync.WaitGroup
	closed atomic.Bool
}

// fail records the first failure; later ones are consequences of it.
func (r *queueRequest) fail(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.mu.Unlock()
}

// Opens a long-lived queue client against the htcondordb daemon at addr (empty =
// located from the ambient configuration), under HTCondor subsystem subsys.
//
// It returns before anything is dialled: the first session is established, and its
// health re-checked every probe_seconds, on a goroutine. Ask hcdb_queue_state
// whether the database may be used. Concurrent queries spread over further
// sessions, dialled as they are needed up to HTCONDORDB_QUEUE_POOL_SIZE.
//
// table names the mirrored job table (empty = the database's default table).
// job_queue_log, when non-empty, is the caller's own JOB_QUEUE_LOG path, and a
// database mirroring a different one is reported stale rather than answered from.
// max_lag_seconds bounds how long ago the mirror may last have read that log; 0
// disables the time bound (the end-of-file check still applies). notify_fd is a
// writable descriptor -- a pipe the caller's event loop selects on -- that
// receives one 8-byte little-endian request handle each time that request has a
// chunk ready or has finished; pass -1 for none.
//
// Release with hcdb_queue_close.
//
//export hcdb_queue_open
func hcdb_queue_open(addr, subsys, table, job_queue_log *C.char, max_lag_seconds C.longlong, probe_seconds C.int, notify_fd C.int) C.uintptr_t {
	return guardHandle("hcdb_queue_open", nil, func() C.uintptr_t {
		return hcdb_queue_openImpl(addr, subsys, table, job_queue_log, max_lag_seconds, probe_seconds, notify_fd)
	})
}

func hcdb_queue_openImpl(addr, subsys, table, job_queue_log *C.char, max_lag_seconds C.longlong, probe_seconds C.int, notify_fd C.int) C.uintptr_t {
	sub := C.GoString(subsys)
	if strings.TrimSpace(sub) == "" {
		sub = "TOOL"
	}
	interval := time.Duration(probe_seconds) * time.Second
	if interval <= 0 {
		interval = defaultProbeInterval
	}
	q := &queueClient{
		addr:       C.GoString(addr),
		table:      C.GoString(table),
		subsys:     sub,
		maxLagSec:  int64(max_lag_seconds),
		wantSource: C.GoString(job_queue_log),
		notifyFD:   int(notify_fd),
		probeNow:   make(chan struct{}, 1),
		done:       make(chan struct{}),
		backoff:    reconnectFloor,
	}
	q.setState(hcdbQueueDown, "not connected yet")
	ctx, cancel := context.WithCancel(context.Background())
	q.cancel = cancel
	go q.run(ctx, interval)
	return C.uintptr_t(cgo.NewHandle(q))
}

// handleQueue resolves a queue-client handle, returning nil rather than panicking
// on a zero, stale or wrong-typed one -- see handleConn.
func handleQueue(h C.uintptr_t) (q *queueClient) {
	defer func() {
		if recover() != nil {
			q = nil
		}
	}()
	q, _ = cgo.Handle(h).Value().(*queueClient)
	return q
}

// handleQueueRequest resolves an in-flight query handle, with the same contract.
func handleQueueRequest(h C.uintptr_t) (r *queueRequest) {
	defer func() {
		if recover() != nil {
			r = nil
		}
	}()
	r, _ = cgo.Handle(h).Value().(*queueRequest)
	return r
}

// Reports whether the database may answer a query right now: HCDB_QUEUE_DOWN,
// _CONNECTING, _READY or _STALE. When detail is non-NULL it also receives a
// one-sentence reason (a C string the caller frees with hcdb_free), which is what
// belongs in the daemon's log when the answer changes.
//
// This reads a cached verdict; it never talks to the network and never blocks.
//
//export hcdb_queue_state
func hcdb_queue_state(h C.uintptr_t, detail **C.char) C.int {
	return guardStatus("hcdb_queue_state", detail, func() C.int { return hcdb_queue_stateImpl(h, detail) })
}

func hcdb_queue_stateImpl(h C.uintptr_t, detail **C.char) C.int {
	q := handleQueue(h)
	if q == nil {
		if detail != nil {
			*detail = C.CString("invalid queue handle")
		}
		return hcdbQueueDown
	}
	if detail != nil {
		if d := q.detail.Load(); d != nil {
			*detail = C.CString(*d)
		} else {
			*detail = nil
		}
	}
	return C.int(q.state.Load())
}

// Asks the supervisor to re-check the database now instead of at its next
// interval. Returns immediately; the verdict changes whenever the check finishes.
//
//export hcdb_queue_probe
func hcdb_queue_probe(h C.uintptr_t) {
	guardVoid("hcdb_queue_probe", func() {
		q := handleQueue(h)
		if q == nil {
			return
		}
		select {
		case q.probeNow <- struct{}{}:
		default: // a check is already pending; one is enough
		}
	})
}

// Starts a query and returns its handle, or 0 with the reason in *err (a C string
// the caller frees with hcdb_free).
//
// It returns before any row is read. constraint is a ClassAd expression (a query
// ad's Requirements). projection is a comma-separated attribute list, or empty for
// every attribute; what comes back also carries the attributes the projected
// expressions themselves reference, so it evaluates self-contained the way
// HTCondor's own projected results do. limit caps the ads (<= 0 for no cap).
// flags is a bitwise OR of HCDB_QUEUE_SERVER_TIME, HCDB_QUEUE_COUNTS and
// HCDB_QUEUE_NO_ADS.
//
// Collect the result with hcdb_queue_next; release it with hcdb_queue_free, which
// is required even for a query that ran to completion.
//
//export hcdb_queue_begin
func hcdb_queue_begin(h C.uintptr_t, constraint, projection *C.char, limit C.longlong, flags C.int, err **C.char) C.uintptr_t {
	return guardHandle("hcdb_queue_begin", err, func() C.uintptr_t {
		return hcdb_queue_beginImpl(h, constraint, projection, limit, flags, nil, err)
	})
}

// hcdb_queue_beginImpl serves both entry points. sink is nil for the chunked
// one and the client's socket for hcdb_queue_begin_fd.
func hcdb_queue_beginImpl(h C.uintptr_t, constraint, projection *C.char, limit C.longlong, flags C.int, sink *fdSink, err **C.char) C.uintptr_t {
	q := handleQueue(h)
	if q == nil {
		if sink != nil {
			sink.close()
		}
		*err = C.CString("invalid queue handle")
		return 0
	}
	pc, release := q.acquire()
	if pc == nil {
		// The socket was handed to us; drop it rather than leak the descriptor.
		if sink != nil {
			sink.close()
		}
		*err = C.CString("no session to the database")
		return 0
	}
	c := pc.c

	ctx, cancel := context.WithCancel(c.ctx)
	r := &queueRequest{
		client:   q,
		chunks:   make(chan queueChunk, chunksInFlight),
		cancel:   cancel,
		sink:     sink,
		connDead: func() bool { return c.dead.Load() },
	}
	handle := cgo.NewHandle(r)
	r.self = uintptr(handle)

	attrs := splitList(C.GoString(projection))
	cons := C.GoString(constraint)
	wantAds := (flags & hcdbQueueNoAds) == 0

	// An unencrypted session takes the frames straight from the builder; an
	// encrypted one has to re-frame each ad anyway, so there it builds records.
	b := &adRecordBuilder{emit: wantAds, cedarFrame: sink != nil && !sink.encrypted()}
	if wantAds {
		if (flags & hcdbQueueServerTime) != 0 {
			b.serverTime = serverTimeExpr(time.Now().Unix())
		}
		if (flags & hcdbQueueCounts) != 0 {
			// Counting needs the two bucketing attributes even when the client's
			// projection leaves them out; suppress drops them again on the way out.
			attrs, b.suppress = withCountingAttrs(attrs)
		}
	} else {
		// A summary-only query reads nothing but what it counts by.
		attrs = []string{attrJobStatus, attrJobUniverse}
	}

	r.wg.Add(1)
	go r.stream(ctx, c.dbc, q.table, cons, attrs, int(limit), b, (flags&hcdbQueueCounts) != 0)
	go func() {
		r.wg.Wait()
		release()
		close(r.chunks)
		// Give the socket up before announcing the result, so that by the time the
		// caller is told the response is finished, nothing here can write to it.
		if r.sink != nil {
			r.fail(r.sink.finish())
		}
		r.closed.Store(true)
		if !r.detached.Load() {
			q.notify(r.self)
		}
	}()

	*err = nil
	return C.uintptr_t(handle)
}

// forceTextRows makes stream take the text fallback against a database that does
// offer wire rows, so one test can run the same query down both paths and compare
// the bytes they produce. Nothing in the library ever sets it.
var forceTextRows = false

// stream reads the matching rows, hands the ads over as wire-framed chunks, and
// tallies the per-(universe, status) counts behind the summary ad as it goes.
//
// The counts come from the rows rather than from a second, grouped query for two
// reasons. They are free here -- the row's text is already being walked -- and a
// GROUP BY on two columns at once is the shape that drops the database off its
// columnar aggregate path onto a full record scan. Counting here also reproduces
// the schedd's own semantics exactly, including under a result limit, where it
// counts the ads it returned and not every ad that matched.
func (r *queueRequest) stream(ctx context.Context, dbc *dbrpc.Client, table, constraint string,
	attrs []string, limit int, b *adRecordBuilder, wantCounts bool) {
	defer r.wg.Done()

	buf := make([]byte, 0, chunkTarget+(64<<10))
	ads := 0
	counts := map[[2]int]int64{}
	sendErr := error(nil)
	rowErr := error(nil)

	flush := func() bool {
		if len(buf) == 0 {
			return true
		}
		if r.sink != nil {
			// These bytes are already the client's frames, so they go out from this
			// goroutine. Nothing is queued for the caller's event loop, which is the
			// whole point of the hcdb_queue_begin_fd path.
			if err := r.sink.writeChunk(ctx, buf); err != nil {
				sendErr = err
				return false
			}
			r.written.Add(int64(ads))
			buf, ads = buf[:0], 0
			return true
		}
		select {
		case r.chunks <- queueChunk{data: buf, ads: ads}:
		case <-ctx.Done():
			sendErr = ctx.Err()
			return false
		}
		r.client.notify(r.self)
		buf = make([]byte, 0, chunkTarget+(64<<10))
		ads = 0
		return true
	}
	// tally is the shared tail of both row forms: the ad is already in buf, and
	// what is left is the bookkeeping and the handoff.
	tally := func() bool {
		ads++
		if wantCounts && b.haveStatus {
			counts[[2]int{b.universe, b.status}]++
		}
		if len(buf) >= chunkTarget {
			return flush()
		}
		return true
	}
	row := func(text string) bool {
		buf = b.appendRecord(buf, text)
		return tally()
	}
	wireRow := func(w []byte) bool {
		var ok bool
		buf, ok = b.appendWireRecord(buf, w)
		if !ok {
			rowErr = errUnreadableWireRow
			return false
		}
		return tally()
	}
	// reset discards what has been built and starts the read again, for a
	// fallback that declares itself before the first row. It reports false when
	// the response has already begun going out: a restart then would replay ads
	// the client has seen, and there is no taking them back.
	reset := func() bool {
		if r.sink != nil && r.written.Load() > 0 {
			rowErr = errRestartAfterSend
			return false
		}
		buf, ads = buf[:0], 0
		clear(counts)
		return true
	}

	// The wire-row stream first: the database ships the ad's structure rather than
	// its text, and ships many ads per frame rather than one, which is most of the
	// cost of relaying a job queue. It is not always available (an older database,
	// or a table that cannot produce self-contained rows), and it says so before
	// the first row, so the text streams below remain the fallback.
	wireAttrs, wireProj := wireRequest(attrs, b, true)
	b.wireProj = wireProj
	err := error(dbrpc.ErrRawWireRefsUnsupported)
	if !forceTextRows {
		err = dbc.QueryRawWireRefsStream(ctx, table, constraint, wireAttrs, limit, true, wireRow)
	}
	// A database with wire rows but no server-side closure: ask again for the whole
	// ad and close it here. Checked before the general wire refusal because the
	// reference-chasing error WRAPS it, and the two want different retries.
	if errors.Is(err, dbrpc.ErrRawWireRefsUnsupported) && !forceTextRows {
		wireAttrs, wireProj = wireRequest(attrs, b, false)
		b.wireProj = wireProj
		reset()
		err = dbc.QueryRawWireStream(ctx, table, constraint, wireAttrs, limit, true, wireRow)
	}
	if errors.Is(err, dbrpc.ErrRawWireUnsupported) && reset() {
		b.wireProj = nil

		// The reference-chasing projection is the one that matches HTCondor:
		// putClassAd expands a projection to the attributes the projected expressions
		// refer to, so a client asking for Requirements can still evaluate it. A
		// database too old for that opcode falls back to projecting here, over every
		// attribute of every ad -- correct for literal-valued attributes, and the
		// reason to upgrade.
		err = dbc.QueryRawProjectRefsStream(ctx, table, constraint, attrs, limit, row)
		if errors.Is(err, dbrpc.ErrProjectRefsUnsupported) && reset() {
			b.projection = lowerSet(attrs)
			err = dbc.QueryRawTableStream(ctx, table, constraint, limit, row)
		}
	}
	if err == nil && r.connDead != nil && r.connDead() {
		// A clean end and a lost connection look identical from inside dbrpc: when
		// the read loop dies it closes the stream's channel, which is exactly what
		// the end-of-stream terminator does, so the query returns short and happy.
		// The connection's own transport watch is the only witness, and the answer
		// to an ambiguous ending has to be an error -- a condor_q that reports a
		// failure gets retried, one that silently lists nine tenths of the queue
		// does not.
		err = errConnLost
	}
	if rowErr != nil {
		r.fail(rowErr)
		return
	}
	if err != nil {
		r.fail(err)
		return
	}
	if sendErr != nil {
		r.fail(sendErr)
		return
	}
	flush()

	var sb strings.Builder
	for key, n := range counts {
		fmt.Fprintf(&sb, "%d %d %d\n", key[0], key[1], n)
	}
	r.mu.Lock()
	r.counts = sb.String()
	r.mu.Unlock()
}

// Collects the next chunk of a query's result without waiting.
//
// Returns HCDB_OK with *buf pointing at the chunk (a C allocation the caller frees
// with hcdb_free), *len its byte length and *ads the number of ads it holds;
// HCDB_QUEUE_PENDING when nothing is ready yet (wait for the notification);
// HCDB_QUEUE_DONE when the result is complete; or HCDB_ERR with the reason in
// *errout.
//
// A chunk is a sequence of records, each a 4-byte little-endian payload length
// followed by that many bytes: an 8-byte big-endian expression count in network
// byte order, that many NUL-terminated "Attr = Value" strings, and two empty
// strings. That is byte for byte what HTCondor's putClassAd writes for one ad on
// an unencrypted channel, so the caller copies the payload to its socket and ends
// the message. On an encrypted channel CEDAR length-prefixes each string: the
// caller writes the count, then walks the NUL-terminated strings and puts them
// individually. Either way it parses nothing.
//
//export hcdb_queue_next
func hcdb_queue_next(rh C.uintptr_t, buf *unsafe.Pointer, length *C.size_t, ads *C.int, errout **C.char) C.int {
	return guardStatus("hcdb_queue_next", errout, func() C.int {
		return hcdb_queue_nextImpl(rh, buf, length, ads, errout)
	})
}

func hcdb_queue_nextImpl(rh C.uintptr_t, buf *unsafe.Pointer, length *C.size_t, ads *C.int, errout **C.char) C.int {
	r := handleQueueRequest(rh)
	if r == nil {
		if errout != nil {
			*errout = C.CString("invalid query handle")
		}
		return hcdbErr
	}
	*buf = nil
	*length = 0
	*ads = 0

	select {
	case ch, ok := <-r.chunks:
		if ok {
			*buf = C.CBytes(ch.data)
			*length = C.size_t(len(ch.data))
			*ads = C.int(ch.ads)
			return hcdbOK
		}
	default:
		if !r.closed.Load() {
			return hcdbQueuePending
		}
		// The producers finished between the receive and this check; the channel is
		// closed and drained, so fall through to the terminal answer.
	}

	r.mu.Lock()
	err := r.err
	r.mu.Unlock()
	if err != nil {
		if errout != nil {
			*errout = C.CString(err.Error())
		}
		return hcdbErr
	}
	return hcdbQueueDone
}

// Writes a finished query's job counts to *out -- a C string the caller frees with
// hcdb_free -- as one "<universe> <status> <count>" line per bucket, and returns
// hcdbOK. The caller folds them into its own counters; the triples are exactly
// what HTCondor's IncrementLiveJobCounter takes, with the count as the increment.
// An ad with no integer JobStatus is in no bucket, which is what that function
// does with one too.
//
// Only meaningful once hcdb_queue_next has returned HCDB_QUEUE_DONE, and only for
// a query begun with HCDB_QUEUE_COUNTS; otherwise *out is an empty string.
//
//export hcdb_queue_counts
func hcdb_queue_counts(rh C.uintptr_t, out **C.char) C.int {
	return guardStatus("hcdb_queue_counts", out, func() C.int {
		r := handleQueueRequest(rh)
		if r == nil {
			*out = C.CString("")
			return hcdbErr
		}
		r.mu.Lock()
		counts := r.counts
		r.mu.Unlock()
		*out = C.CString(counts)
		return hcdbOK
	})
}

// Cancels a query if it is still running, waits for its goroutines to stop, and
// frees the handle. Required for every query, finished or not.
//
//export hcdb_queue_free
func hcdb_queue_free(rh C.uintptr_t) {
	guardVoid("hcdb_queue_free", func() {
		r := handleQueueRequest(rh)
		if r == nil {
			return
		}
		if r.sink != nil && !r.closed.Load() {
			detachSink(r, rh)
			return
		}
		r.cancel()
		// Drain so a producer blocked handing over a chunk sees the cancellation and
		// returns, rather than parking forever.
		go func() {
			for range r.chunks {
			}
		}()
		r.wg.Wait()
		cgo.Handle(rh).Delete()
	})
}

// Stops the supervisor, closes every session and frees the handle. Outstanding
// queries must be freed first: their contexts descend from their session's, so
// closing cancels them, but their handles are still the caller's to release.
//
//export hcdb_queue_close
func hcdb_queue_close(h C.uintptr_t) {
	guardVoid("hcdb_queue_close", func() {
		q := handleQueue(h)
		if q == nil {
			return
		}
		q.cancel()
		<-q.done
		cgo.Handle(h).Delete()
	})
}
