// Package leaderfollower implements htcondordb's "leader-follower" HA mode.
//
// The leader is an ordinary read/write htcondordb daemon. Each follower opens a
// DAEMON-level session to the leader and consumes its commit stream -- the same
// change feed the store's Watch produces -- applying every upsert and delete to
// its own local database, so the follower converges to the leader's state. A
// follower persists the stream cursor, so after a restart it resumes exactly
// where it left off and misses no change. If the follower's cursor has fallen
// out of the leader's retention (it was down too long), the leader answers with
// a reset: the follower clears its keyspace and the leader replays the full
// current state.
//
// This mode gives no transactional/quorum safety -- replication is asynchronous
// and best-effort -- but it is cheap and lets a follower serve read-only queries
// from local state, offloading reads from the leader. Writes must go to the
// leader (a follower's server runs with ForceReadOnly).
package leaderfollower

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Dialer establishes a fresh DAEMON-level dbrpc client to the leader. The
// replicator owns the returned client and closes it when the connection ends.
type Dialer func(ctx context.Context) (*dbrpc.Client, error)

// ReplicatorConfig configures a follower's replicator.
type ReplicatorConfig struct {
	// Local is the follower's database; the replicator is its sole writer.
	Local *db.DB

	// Dial connects to the leader and returns a dbrpc client. Required.
	Dial Dialer

	// CursorFile, if set, persists the last-applied stream cursor so a restart
	// resumes without a full replay. Empty keeps the cursor in memory only (a
	// restart then triggers a full resync from the leader).
	CursorFile string

	// ReconnectMin/ReconnectMax bound the exponential backoff between
	// reconnection attempts. Defaults: 500ms / 30s.
	ReconnectMin time.Duration
	ReconnectMax time.Duration

	// FlushInterval throttles cursor persistence (default 1s). The cursor is
	// also flushed when Run returns.
	FlushInterval time.Duration

	// Logger receives replication diagnostics. Defaults to slog.Default().
	Logger *slog.Logger
}

// Replicator streams the leader's commits into the local database.
type Replicator struct {
	cfg    ReplicatorConfig
	log    *slog.Logger
	rmin   time.Duration
	rmax   time.Duration
	flushN time.Duration

	mu         sync.Mutex
	cursor     []byte // last applied stream cursor
	cursorDirt bool

	applied atomic.Uint64 // count of applied events (for status/tests)
	resets  atomic.Uint64 // count of full resyncs performed
}

// NewReplicator validates cfg and builds a Replicator, loading any persisted
// cursor.
func NewReplicator(cfg ReplicatorConfig) (*Replicator, error) {
	if cfg.Local == nil {
		return nil, errConfig("Local database is required")
	}
	if cfg.Dial == nil {
		return nil, errConfig("Dial is required")
	}
	r := &Replicator{
		cfg:    cfg,
		log:    cfg.Logger,
		rmin:   orDur(cfg.ReconnectMin, 500*time.Millisecond),
		rmax:   orDur(cfg.ReconnectMax, 30*time.Second),
		flushN: orDur(cfg.FlushInterval, time.Second),
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	if cfg.CursorFile != "" {
		if data, err := os.ReadFile(cfg.CursorFile); err == nil && len(data) > 0 {
			r.cursor = data
		}
	}
	return r, nil
}

// AppliedCount returns how many stream events have been applied (upserts +
// deletes + resets). Useful for tests and status.
func (r *Replicator) AppliedCount() uint64 { return r.applied.Load() }

// ResetCount returns how many full resyncs have occurred.
func (r *Replicator) ResetCount() uint64 { return r.resets.Load() }

// Run replicates until ctx is cancelled, reconnecting with exponential backoff.
// It returns ctx.Err() on cancellation.
func (r *Replicator) Run(ctx context.Context) error {
	defer r.flushCursor()
	stopFlush := r.startFlusher(ctx)
	defer stopFlush()

	backoff := r.rmin
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := r.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			r.log.Warn("replication session ended; will reconnect", "err", err.Error(), "backoff", backoff.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > r.rmax {
			backoff = r.rmax
		}
	}
}

// session runs one connection: dial, watch from the persisted cursor, apply
// events until the stream ends or errors.
func (r *Replicator) session(ctx context.Context) error {
	client, err := r.cfg.Dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	events, stop, err := client.Watch(r.snapshotCursor())
	if err != nil {
		return err
	}
	defer stop()

	r.log.Info("replication session established", "from_cursor_len", len(r.snapshotCursor()))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return errStreamEnded
			}
			if err := r.applyEvent(ev); err != nil {
				return err
			}
		}
	}
}

// applyEvent applies one watch event to the local database and records its
// cursor.
func (r *Replicator) applyEvent(ev dbrpc.WatchEvent) error {
	switch ev.Kind {
	case wkReset:
		if err := r.clearLocal(); err != nil {
			return err
		}
		r.resets.Add(1)
		r.log.Info("full resync: cleared local keyspace, replaying leader state")
	case wkUpsert:
		ad, err := classad.Parse(ev.AdText) // server streams the new-ClassAd format
		if err != nil {
			return err
		}
		tx := r.cfg.Local.Begin()
		tx.NewClassAd(ev.Key, ad)
		if err := tx.CommitNondurable(); err != nil {
			return err
		}
	case wkDelete:
		tx := r.cfg.Local.Begin()
		tx.DestroyClassAd(ev.Key)
		if err := tx.CommitNondurable(); err != nil {
			return err
		}
	}
	r.applied.Add(1)
	r.setCursor(ev.Cursor)
	return nil
}

// clearLocal removes every key from the local database (for a full resync).
func (r *Replicator) clearLocal() error {
	keys := r.cfg.Local.Keys()
	if len(keys) == 0 {
		return nil
	}
	tx := r.cfg.Local.Begin()
	for _, k := range keys {
		tx.DestroyClassAd(k)
	}
	return tx.CommitNondurable()
}

func (r *Replicator) snapshotCursor() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cursor == nil {
		return nil
	}
	return append([]byte(nil), r.cursor...)
}

func (r *Replicator) setCursor(cur []byte) {
	r.mu.Lock()
	r.cursor = append(r.cursor[:0:0], cur...)
	r.cursorDirt = true
	r.mu.Unlock()
}

// startFlusher periodically persists the cursor to disk; returns a stop func.
func (r *Replicator) startFlusher(ctx context.Context) func() {
	if r.cfg.CursorFile == "" {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(r.flushN)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				r.flushCursor()
			}
		}
	}()
	return func() { close(done) }
}

// flushCursor writes the current cursor to CursorFile if it changed.
func (r *Replicator) flushCursor() {
	if r.cfg.CursorFile == "" {
		return
	}
	r.mu.Lock()
	if !r.cursorDirt {
		r.mu.Unlock()
		return
	}
	cur := append([]byte(nil), r.cursor...)
	r.cursorDirt = false
	r.mu.Unlock()

	tmp := r.cfg.CursorFile + ".tmp"
	if err := os.WriteFile(tmp, cur, 0o600); err != nil {
		r.log.Warn("could not persist replication cursor", "err", err.Error())
		return
	}
	if err := os.Rename(tmp, r.cfg.CursorFile); err != nil {
		r.log.Warn("could not commit replication cursor", "err", err.Error())
	}
}

func orDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
