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
// constraint, and feeds the sink (which checkpoints its resume cursor on WatchSynced).
func (r *Runner) session(ctx context.Context) error {
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

	var ver uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				return errStreamClosed
			}
			ch, ok := r.toChange(ev, &ver)
			if !ok {
				continue
			}
			if ch.Kind == replicate.KindUpsert && r.matcher != nil && ch.Ad != nil && !r.matcher.Matches(ch.Ad) {
				continue // selective: drop a non-matching upsert
			}
			if err := r.sink.Apply(ch); err != nil {
				return err
			}
		}
	}
}

// toChange converts a watch event to a replicate.Change. The ad arrives decoded (see
// watchfeed), so this only has to notice one that could not be decoded.
// ver is advanced only for a sink-visible change. ok is false for an undecodable ad or a resync.
func (r *Runner) toChange(ev watchfeed.Event, ver *uint64) (replicate.Change, bool) {
	if ev.Err != nil {
		r.log.Warn("cedarsync: skipping undecodable ad", "src", r.cfg.Src, "key", ev.Key, "err", ev.Err)
		return replicate.Change{}, false
	}
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
