// Package leaderfollower implements htcondordb's "leader-follower" HA mode.
//
// The leader is an ordinary read/write htcondordb daemon. Each follower opens a
// DAEMON-level session to the leader and consumes its commit stream -- the same
// change feed the store's Watch produces -- for EVERY table, applying every upsert
// and delete to its own local catalog, so the follower converges to the leader's
// whole-catalog state. A follower persists a per-table stream cursor, so after a
// restart it resumes exactly where it left off and misses no change. If a table's
// cursor has fallen out of the leader's retention (it was down too long), the leader
// answers with a reset: the follower clears that table and the leader replays it.
//
// This mode gives no transactional/quorum safety -- replication is asynchronous and
// best-effort -- but it is cheap and lets a follower serve read-only queries from
// local state. Writes must go to the leader (a follower's server runs ForceReadOnly).
package leaderfollower

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Dialer establishes a fresh DAEMON-level dbrpc client to the leader. The replicator
// owns the returned client and closes it when the connection ends.
type Dialer func(ctx context.Context) (*dbrpc.Client, error)

// ReplicatorConfig configures a follower's replicator.
type ReplicatorConfig struct {
	// Catalog is the follower's local catalog; the replicator is its sole writer,
	// mirroring every table the leader serves.
	Catalog *db.Catalog

	// Dial connects to the leader and returns a dbrpc client. Required.
	Dial Dialer

	// CursorDir, if set, persists each table's last-applied cursor (one file per
	// table) so a restart resumes without a full replay. Empty keeps cursors in
	// memory only (a restart then triggers a full resync per table).
	CursorDir string

	// ReconnectMin/ReconnectMax bound the exponential backoff between reconnection
	// attempts. Defaults: 500ms / 30s.
	ReconnectMin time.Duration
	ReconnectMax time.Duration

	// DiscoverInterval is how often, within a session, the follower re-lists the
	// leader's tables to start mirroring newly-created ones (default 5s).
	DiscoverInterval time.Duration

	// FlushInterval throttles cursor persistence (default 1s).
	FlushInterval time.Duration

	// Logger receives replication diagnostics. Defaults to slog.Default().
	Logger *slog.Logger
}

// Replicator streams the leader's per-table commits into the local catalog.
type Replicator struct {
	cfg       ReplicatorConfig
	log       *slog.Logger
	rmin      time.Duration
	rmax      time.Duration
	flushN    time.Duration
	discoverN time.Duration

	mu      sync.Mutex
	cursors map[string][]byte // table -> last applied cursor
	dirty   map[string]bool   // table -> cursor changed since last flush

	applied atomic.Uint64 // count of applied events (for status/tests)
	resets  atomic.Uint64 // count of full resyncs performed
}

// NewReplicator validates cfg and builds a Replicator, loading any persisted cursors.
func NewReplicator(cfg ReplicatorConfig) (*Replicator, error) {
	if cfg.Catalog == nil {
		return nil, errConfig("Catalog is required")
	}
	if cfg.Dial == nil {
		return nil, errConfig("Dial is required")
	}
	r := &Replicator{
		cfg:       cfg,
		log:       cfg.Logger,
		rmin:      orDur(cfg.ReconnectMin, 500*time.Millisecond),
		rmax:      orDur(cfg.ReconnectMax, 30*time.Second),
		flushN:    orDur(cfg.FlushInterval, time.Second),
		discoverN: orDur(cfg.DiscoverInterval, 5*time.Second),
		cursors:   map[string][]byte{},
		dirty:     map[string]bool{},
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	r.loadCursors()
	return r, nil
}

// AppliedCount returns how many stream events have been applied (upserts + deletes +
// resets) across all tables. ResetCount returns how many full per-table resyncs occurred.
func (r *Replicator) AppliedCount() uint64 { return r.applied.Load() }
func (r *Replicator) ResetCount() uint64   { return r.resets.Load() }

// Run replicates until ctx is cancelled, reconnecting with exponential backoff.
func (r *Replicator) Run(ctx context.Context) error {
	defer r.flushCursors()
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
		if backoff *= 2; backoff > r.rmax {
			backoff = r.rmax
		}
	}
}

// session runs one connection: dial, discover the leader's tables, watch each (and any
// that appear later) concurrently, and apply their events until any watcher fails or ctx
// is done. A single watcher failure tears down the session so Run reconnects.
func (r *Replicator) session(parent context.Context) error {
	client, err := r.cfg.Dial(parent)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var wg sync.WaitGroup
	errOnce := make(chan error, 1)
	watching := map[string]bool{}
	var wmu sync.Mutex

	spawn := func(table string) {
		wmu.Lock()
		if watching[table] {
			wmu.Unlock()
			return
		}
		watching[table] = true
		wmu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if werr := r.watchTable(ctx, client, table); werr != nil && ctx.Err() == nil {
				select {
				case errOnce <- werr:
				default:
				}
				cancel()
			}
		}()
	}

	tables, err := client.Tables(ctx)
	if err != nil {
		return err
	}
	r.log.Info("replication session established", "tables", len(tables))
	for _, t := range tables {
		spawn(t)
	}

	ticker := time.NewTicker(r.discoverN)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return sessionErr(errOnce, parent)
		case <-ticker.C:
			more, terr := client.Tables(ctx)
			if terr != nil {
				cancel()
				wg.Wait()
				return terr
			}
			for _, t := range more {
				spawn(t)
			}
		}
	}
}

func sessionErr(errOnce chan error, parent context.Context) error {
	select {
	case e := <-errOnce:
		return e
	default:
		return parent.Err()
	}
}

// watchTable streams one table's commits from the leader and applies them to the local
// table of the same name (created if absent), from that table's persisted cursor.
func (r *Replicator) watchTable(ctx context.Context, client *dbrpc.Client, table string) error {
	local, err := r.cfg.Catalog.CreateTable(table)
	if err != nil {
		return err
	}
	events, stop, err := client.WatchTable(ctx, table, r.cursorFor(table))
	if err != nil {
		return err
	}
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return errStreamEnded
			}
			if err := r.applyEvent(local, table, ev); err != nil {
				return err
			}
		}
	}
}

// applyEvent applies one watch event to a local table and records its cursor.
func (r *Replicator) applyEvent(local *db.DB, table string, ev dbrpc.WatchEvent) error {
	switch ev.Kind {
	case wkReset:
		if err := clearTable(local); err != nil {
			return err
		}
		r.resets.Add(1)
		r.log.Info("full resync: cleared local table, replaying leader state", "table", table)
	case wkUpsert:
		ad, err := classad.Parse(ev.AdText) // server streams the new-ClassAd format
		if err != nil {
			return err
		}
		tx := local.Begin()
		tx.NewClassAd(ev.Key, ad)
		if err := tx.CommitNondurable(); err != nil {
			return err
		}
	case wkDelete:
		tx := local.Begin()
		tx.DestroyClassAd(ev.Key)
		if err := tx.CommitNondurable(); err != nil {
			return err
		}
	}
	r.applied.Add(1)
	r.setCursor(table, ev.Cursor)
	return nil
}

// clearTable removes every key from a table (for a full resync).
func clearTable(local *db.DB) error {
	keys := local.Keys()
	if len(keys) == 0 {
		return nil
	}
	tx := local.Begin()
	for _, k := range keys {
		tx.DestroyClassAd(k)
	}
	return tx.CommitNondurable()
}

// --- per-table cursor state ---

func (r *Replicator) cursorFor(table string) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c := r.cursors[table]; c != nil {
		return append([]byte(nil), c...)
	}
	return nil
}

func (r *Replicator) setCursor(table string, cur []byte) {
	r.mu.Lock()
	r.cursors[table] = append([]byte(nil), cur...)
	r.dirty[table] = true
	r.mu.Unlock()
}

func (r *Replicator) startFlusher(ctx context.Context) func() {
	if r.cfg.CursorDir == "" {
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
				r.flushCursors()
			}
		}
	}()
	return func() { close(done) }
}

// flushCursors persists each changed table cursor to CursorDir/<table>.cursor.
func (r *Replicator) flushCursors() {
	if r.cfg.CursorDir == "" {
		return
	}
	r.mu.Lock()
	pending := map[string][]byte{}
	for table, d := range r.dirty {
		if d {
			pending[table] = append([]byte(nil), r.cursors[table]...)
			r.dirty[table] = false
		}
	}
	r.mu.Unlock()
	if len(pending) == 0 {
		return
	}
	_ = os.MkdirAll(r.cfg.CursorDir, 0o750)
	for table, cur := range pending {
		path := filepath.Join(r.cfg.CursorDir, table+".cursor")
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, cur, 0o600); err != nil {
			r.log.Warn("could not persist replication cursor", "table", table, "err", err.Error())
			continue
		}
		if err := os.Rename(tmp, path); err != nil {
			r.log.Warn("could not commit replication cursor", "table", table, "err", err.Error())
		}
	}
}

// loadCursors reads any persisted per-table cursors at startup.
func (r *Replicator) loadCursors() {
	if r.cfg.CursorDir == "" {
		return
	}
	entries, err := os.ReadDir(r.cfg.CursorDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != ".cursor" {
			continue
		}
		if data, err := os.ReadFile(filepath.Join(r.cfg.CursorDir, name)); err == nil && len(data) > 0 {
			r.cursors[name[:len(name)-len(".cursor")]] = data
		}
	}
}

func orDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
