// Package cedarsync replicates a source htcondordb's table/archive change stream into a local
// store over native CEDAR (dbrpc WatchTable), selectively (an optional constraint) and stamped with
// a source label. It is the CEDAR sibling of classad/changefeed's HTTP transport: both feed the
// same transport-neutral db/replicate.Sink, so an htcondordb->htcondordb feed and an external
// non-CEDAR sink share one idempotent apply core.
//
// Fan-in is "run one Runner per source into one catalog": each Runner stamps its own Src and keeps
// its own resume cursor. Delivery is at-least-once (resume cursor + idempotent Sink).
package cedarsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/htcondordb/watchfeed"
)

// Dial opens a fresh dbrpc client to a source (leader) htcondordb; cleanup releases it. A fresh
// dial per session lets a reconnect recover from a dropped connection.
type Dial func(ctx context.Context) (client *dbrpc.Client, cleanup func(), err error)

// Config configures one source->target replication.
type Config struct {
	// Source is the table/archive name to watch on the leader.
	Source string
	// Src labels the replicated rows (stamped as replicate.SrcAttr) for fan-in queries/dedup.
	Src string
	// Constraint, if set, is a client-side filter: only upserts whose ad matches are applied
	// (deletes/reset/synced always pass). Server-side filtering would need a dbrpc filtered-watch
	// op; this keeps the source unmodified.
	Constraint string
	// MaxConsecutiveFailures, if > 0, makes Run give up after that many failed sessions in a row
	// (progress resets the count). 0 retries forever.
	MaxConsecutiveFailures int
	// FlushInterval is how often a sink implementing Flusher is flushed while a session runs.
	// <= 0 means DefaultFlushInterval. Ignored for a sink that does not implement Flusher.
	FlushInterval time.Duration
}

// DefaultFlushInterval is the flush cadence for a Flusher sink, matching ha/leaderfollower's
// cursor flush: a restart replays at most about this much.
const DefaultFlushInterval = time.Second

// Flusher is implemented by a sink that buffers applied changes and makes them durable -- with the
// resume cursor, and only after them -- in batches. The Runner calls Flush every FlushInterval
// while a session runs (also when no events arrive, so a quiet stream still commits the cursor of
// its last change) and once when a session ends. A replicate.Sink that commits on Synced alone
// replays everything since its last connect after a restart; a Flusher replays at most one
// interval.
type Flusher interface {
	Flush() error
}

// SessionAware is implemented by a sink whose behavior depends on where a watch session stands.
// BeginSession is called once the watch is open, before its first event; EndSession after the
// session ends and the sink has been flushed. Every event between BeginSession and the session's
// Synced is catch-up -- either a Reset replay or the at-least-once overlap a resume re-delivers --
// and a sink that must stay idempotent across that window (an append-only archive) needs to know
// it is in it.
type SessionAware interface {
	BeginSession()
	EndSession()
}

// UndecodableSink is implemented by a sink that must account for an upsert whose ad arrived but
// could not be decoded. The Runner passes it to ApplyUndecodable -- in place of Apply, and past the
// client-side Constraint, which cannot evaluate it -- as the Change Apply would have received, with
// Kind KindUpsert and a nil Ad. A sink without it never sees such a change (the Runner logs and
// skips it), which is wrong for one reconciling a Reset replay: a key missing from the replay
// reads as deleted at the source.
type UndecodableSink interface {
	ApplyUndecodable(c replicate.Change, err error) error
}

// Status is a Runner's connection state, for health reporting.
type Status struct {
	Connected     bool      // a watch session is open
	Sessions      int       // sessions opened since the Runner started
	LastConnected time.Time // when the current (or last) session opened
	LastEvent     time.Time // when the last event was applied
	LastError     string    // the last session error, "" if none since the last clean session
	LastErrorTime time.Time
}

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 30 * time.Second
)

// errStreamClosed signals the watch stream ended (server close / connection drop); reconnect from
// the last checkpoint. Not a failure by itself.
var errStreamClosed = errors.New("cedarsync: watch stream closed")

// Runner replicates one Config into sink, reconnecting until ctx is cancelled.
type Runner struct {
	dial    Dial
	cfg     Config
	sink    replicate.Sink
	matcher *vm.Query
	log     *slog.Logger

	mu     sync.Mutex
	status Status
}

// Status returns the Runner's current connection state. Safe for concurrent use with Run.
func (r *Runner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

func (r *Runner) setStatus(f func(*Status)) {
	r.mu.Lock()
	f(&r.status)
	r.mu.Unlock()
}

// NewRunner builds a Runner. A nil log discards output.
func NewRunner(dial Dial, cfg Config, sink replicate.Sink, log *slog.Logger) (*Runner, error) {
	if dial == nil || sink == nil {
		return nil, errors.New("cedarsync: dial and sink are required")
	}
	if strings.TrimSpace(cfg.Source) == "" || strings.TrimSpace(cfg.Src) == "" {
		return nil, errors.New("cedarsync: Source and Src are required")
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	var matcher *vm.Query
	if c := strings.TrimSpace(cfg.Constraint); c != "" {
		m, err := vm.Parse(c)
		if err != nil {
			return nil, fmt.Errorf("cedarsync: bad constraint %q: %w", c, err)
		}
		matcher = m
	}
	return &Runner{dial: dial, cfg: cfg, sink: sink, matcher: matcher, log: log}, nil
}

// Run drives replication until ctx is cancelled, reconnecting with capped backoff and always
// resuming from the sink's committed cursor.
func (r *Runner) Run(ctx context.Context) error {
	backoff := initialBackoff
	fails := 0
	for ctx.Err() == nil {
		err := r.session(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, errStreamClosed):
			backoff, fails = initialBackoff, 0 // clean reconnect (made progress)
		default:
			fails++
			r.setStatus(func(s *Status) { s.LastError, s.LastErrorTime = err.Error(), time.Now() })
			r.log.Warn("cedarsync: session error; reconnecting", "src", r.cfg.Src, "table", r.cfg.Source, "err", err)
			if r.cfg.MaxConsecutiveFailures > 0 && fails >= r.cfg.MaxConsecutiveFailures {
				return fmt.Errorf("cedarsync: giving up after %d consecutive failures: %w", fails, err)
			}
		}
		if !sleep(ctx, backoff) {
			return nil
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return nil
}

// session runs one watch connection: it maps each event to a Change, applies the client-side
// constraint, and feeds the sink (which checkpoints its resume cursor on WatchSynced, or on Flush
// for a Flusher).
func (r *Runner) session(ctx context.Context) (err error) {
	c, cleanup, err := r.dial(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	events, stop, wireForm, err := watchfeed.Watch(ctx, c, r.cfg.Source, r.sink.Cursor())
	if err != nil {
		return err
	}
	defer stop()
	r.log.Debug("cedarsync: change feed open", "src", r.cfg.Src, "wire", wireForm)
	r.setStatus(func(s *Status) { s.Connected, s.LastConnected = true, time.Now(); s.Sessions++ })

	flusher, _ := r.sink.(Flusher)
	sa, _ := r.sink.(SessionAware)
	if sa != nil {
		sa.BeginSession()
	}
	defer func() {
		// Make whatever this session applied durable before reconnecting: the next session resumes
		// from the committed cursor, so an unflushed tail would simply be re-delivered, but there
		// is no reason to re-deliver it.
		if flusher != nil {
			if ferr := flusher.Flush(); ferr != nil && (err == nil || errors.Is(err, errStreamClosed)) {
				err = ferr
			}
		}
		if sa != nil {
			sa.EndSession()
		}
		r.setStatus(func(s *Status) { s.Connected = false })
	}()

	var flushC <-chan time.Time
	if flusher != nil {
		iv := r.cfg.FlushInterval
		if iv <= 0 {
			iv = DefaultFlushInterval
		}
		t := time.NewTicker(iv)
		defer t.Stop()
		flushC = t.C
	}

	var ver uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-flushC:
			if err := flusher.Flush(); err != nil {
				return err
			}
		case ev, ok := <-events:
			if !ok {
				return errStreamClosed
			}
			if err := r.deliver(ev, &ver); err != nil {
				return err
			}
		}
	}
}

// deliver hands one watch event to the sink.
func (r *Runner) deliver(ev watchfeed.Event, ver *uint64) error {
	if ev.Err != nil {
		return r.deliverUndecodable(ev, ver)
	}
	ch, ok := r.toChange(ev, ver)
	if !ok {
		return nil
	}
	if ch.Kind == replicate.KindUpsert && r.matcher != nil && ch.Ad != nil && !r.matcher.Matches(ch.Ad) {
		return nil // selective: drop a non-matching upsert
	}
	if err := r.sink.Apply(ch); err != nil {
		return err
	}
	r.setStatus(func(s *Status) {
		s.LastEvent = time.Now()
		if ch.Kind == replicate.KindSynced {
			s.LastError = "" // a session that reached Synced is healthy
		}
	})
	return nil
}

// deliverUndecodable hands an upsert whose ad could not be decoded to an UndecodableSink, and
// skips it for any other sink.
func (r *Runner) deliverUndecodable(ev watchfeed.Event, ver *uint64) error {
	us, ok := r.sink.(UndecodableSink)
	if !ok || db.WatchKind(ev.Kind) != db.WatchUpsert {
		r.log.Warn("cedarsync: skipping undecodable ad", "src", r.cfg.Src, "key", ev.Key, "err", ev.Err)
		return nil
	}
	r.log.Warn("cedarsync: undecodable ad; the sink keeps what it holds for the key", "src", r.cfg.Src, "key", ev.Key, "err", ev.Err)
	ch, _ := replicate.ChangeFromWatch(db.WatchEvent{Kind: db.WatchUpsert, Key: ev.Key, Cursor: ev.Cursor}, r.cfg.Src, *ver)
	*ver++
	if err := us.ApplyUndecodable(ch, ev.Err); err != nil {
		return err
	}
	r.setStatus(func(s *Status) { s.LastEvent = time.Now() })
	return nil
}

// toChange converts a decoded watch event to a replicate.Change. ver is advanced only for a
// sink-visible change; ok is false for a resync.
func (r *Runner) toChange(ev watchfeed.Event, ver *uint64) (replicate.Change, bool) {
	we := db.WatchEvent{Kind: db.WatchKind(ev.Kind), Key: ev.Key, Cursor: ev.Cursor, Ad: ev.Ad}
	ch, ok := replicate.ChangeFromWatch(we, r.cfg.Src, *ver)
	if ok {
		*ver++
	}
	return ch, ok
}

// sleep waits for d or ctx cancellation; returns false if ctx was cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
