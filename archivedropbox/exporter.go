package archivedropbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// errResync signals the watch stream must be reconnected from the last checkpoint (an explicit
// WatchResync, or the stream closing). It is not a failure and not data loss -- retention still
// covers the cursor.
var errResync = errors.New("archivedropbox: watch resync")

const (
	sessionInitBackoff = 500 * time.Millisecond
	sessionMaxBackoff  = 30 * time.Second

	// beatCap bounds the status-refresh cadence so the daemon's liveness monitor always sees a
	// fresh beat even when RollInterval is long (rolls can be 10m apart).
	beatCap = 30 * time.Second
	// defaultBackpressurePoll is how often a blocked exporter re-checks whether the consumer has
	// drained the dropbox below the ceiling.
	defaultBackpressurePoll = 2 * time.Second
)

// Writer is the durable sink the Runner drops tarballs and loss reports into. It is an interface
// so tests can substitute a fake; the production implementation is dropboxWriter.
type Writer interface {
	WriteTarball(seq uint64, recs []record) (string, error)
	WriteLossReport(adText string, detectedUnix int64) (string, error)
	DirSize() (int64, error)
}

// Runner exports one archive's records into a dropbox for a single exporter definition. It is a
// dbrpc client end to end: it reads/writes its resume state through the exporter registry and
// watches the archive over the wire, batching records into fsync'd, atomically-renamed tarballs.
// Delivery is at-least-once (a crash re-exports the last batch as a duplicate tarball).
type Runner struct {
	name string
	cfg  Config
	c    *dbrpc.Client
	w    Writer
	log  *slog.Logger

	backpressurePoll time.Duration // overridable in tests

	// MaxConsecutiveFailures, if > 0, makes Run return after that many consecutive failed sessions
	// (a resync resets the count). 0 retries forever.
	MaxConsecutiveFailures int
}

// NewRunner builds a Runner. A nil log discards output.
func NewRunner(name string, cfg Config, c *dbrpc.Client, w Writer, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Runner{name: name, cfg: cfg, c: c, w: w, log: log, backpressurePoll: defaultBackpressurePoll}
}

// Run drives the exporter until ctx is cancelled, reconnecting (with backoff) across transient
// errors and resyncs, always resuming from the last checkpointed cursor.
func (r *Runner) Run(ctx context.Context) error {
	backoff := sessionInitBackoff
	fails := 0
	fail := func(err error) error {
		fails++
		if r.MaxConsecutiveFailures > 0 && fails >= r.MaxConsecutiveFailures {
			return fmt.Errorf("archivedropbox: giving up after %d consecutive failures: %w", fails, err)
		}
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
		backoff = nextBackoff(backoff)
		return nil
	}
	for {
		st, err := r.loadState(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.log.Warn("archivedropbox: loading resume state failed", "exporter", r.name, "err", err)
			if gaveUp := fail(err); gaveUp != nil {
				return gaveUp
			}
			continue
		}
		err = r.session(ctx, st)
		switch {
		case err == nil:
			return nil // ctx cancelled cleanly
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, errResync):
			r.log.Info("archivedropbox: resync; reconnecting from last checkpoint", "exporter", r.name)
			backoff = sessionInitBackoff
			fails = 0
		default:
			r.log.Warn("archivedropbox: session error; reconnecting", "exporter", r.name, "err", err)
			if gaveUp := fail(err); gaveUp != nil {
				return gaveUp
			}
		}
	}
}

func (r *Runner) loadState(ctx context.Context) (*State, error) {
	blob, _, err := r.c.GetExporterState(ctx, r.name)
	if err != nil {
		return nil, err
	}
	return decodeState(blob)
}

// session runs one watch connection. It accumulates records and rolls a tarball every RollJobs
// records or every RollInterval, whichever comes first; the resume cursor advances only after a
// tarball is durably on disk. A WatchReset that arrives when we already had a resume cursor means
// the cursor was pruned (data loss): it drops a loss report describing the gap, then exports from
// the oldest retained record.
func (r *Runner) session(ctx context.Context, st *State) error {
	events, stop, err := r.c.WatchTable(ctx, r.cfg.Table, st.WireCursor)
	if err != nil {
		return err
	}
	defer stop()

	// Cumulative counters persist across sessions via the durable status.
	indexed := st.Status.DocsIndexed
	skipped := st.Status.DocsSkipped

	var (
		pending     []record
		lastCursor  []byte // resume cursor of the newest event seen
		hadCursor   = len(st.WireCursor) > 0
		lossPending bool  // a reset was seen with a prior cursor; awaiting the first replayed record
		lossStart   int64 // last-exported record-time captured when the reset was seen
	)

	// writeStatus persists the live status (beat + progress). advanceTo, when non-empty and
	// changed, also moves the durable resume cursor -- callers pass it ONLY after everything up to
	// that cursor is durably on disk. It always writes so an idle-but-healthy exporter shows a
	// fresh beat and the daemon does not mistake it for stalled.
	writeStatus := func(advanceTo []byte) error {
		st.Status = Status{Beat: time.Now().Unix(), DocsIndexed: indexed, DocsSkipped: skipped, InFlight: len(pending)}
		if len(advanceTo) > 0 {
			st.WireCursor = advanceTo
		}
		blob, err := st.encode()
		if err != nil {
			return err
		}
		return r.c.PutExporterState(ctx, r.name, blob)
	}

	// flush writes the buffered records as one tarball (blocking on backpressure first), then
	// advances the cursor over them. It is the at-least-once boundary: the tarball is fsync'd and
	// renamed BEFORE the cursor moves.
	flush := func(cursor []byte) error {
		if len(pending) == 0 {
			return nil
		}
		if err := r.awaitDropboxSpace(ctx, writeStatus); err != nil {
			return err
		}
		path, err := r.w.WriteTarball(st.TarballSeq+1, pending)
		if err != nil {
			return err
		}
		st.TarballSeq++
		if last := pending[len(pending)-1].modUnix; last > 0 {
			st.LastRecordUnix = last
		}
		indexed += uint64(len(pending))
		r.log.Info("archivedropbox: wrote tarball", "exporter", r.name, "path", path, "records", len(pending), "seq", st.TarballSeq)
		pending = pending[:0]
		return writeStatus(cursor)
	}

	rollTicker := time.NewTicker(time.Duration(r.cfg.RollInterval))
	defer rollTicker.Stop()
	beatTicker := time.NewTicker(beatInterval(time.Duration(r.cfg.RollInterval)))
	defer beatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-rollTicker.C:
			// Time-based roll: bound how long a buffered record waits for durability.
			if err := flush(lastCursor); err != nil {
				return err
			}
		case <-beatTicker.C:
			if err := writeStatus(nil); err != nil {
				return err
			}
		case ev, ok := <-events:
			if !ok {
				// Stream closed: flush what we have, then reconnect from the last checkpoint.
				if err := flush(lastCursor); err != nil {
					return err
				}
				return errResync
			}
			if len(ev.Cursor) > 0 {
				lastCursor = ev.Cursor
			}
			switch db.WatchKind(ev.Kind) {
			case db.WatchUpsert:
				ad, err := classad.Parse(ev.AdText)
				if err != nil {
					skipped++
					r.log.Warn("archivedropbox: skipping undecodable ad", "exporter", r.name, "key", ev.Key, "err", err)
					continue
				}
				rt := recordTime(ad)
				if lossPending {
					// This is the oldest retained record; the gap ends here.
					if err := r.emitLoss(lossStart, rt); err != nil {
						return err
					}
					lossPending = false
				}
				pending = append(pending, record{name: entryName(len(pending), ev.Key, ad), adText: ad.MarshalOld(), modUnix: rt})
				if len(pending) >= r.cfg.RollJobs {
					if err := flush(ev.Cursor); err != nil {
						return err
					}
				}
			case db.WatchDelete:
				// An append-only archive should not delete; ignore but let the cursor advance.
			case db.WatchReset:
				// Rebuild from the oldest retained record. If we had a resume cursor, the records
				// between it and the oldest retained record were pruned before export -> data loss.
				pending = pending[:0]
				if hadCursor {
					lossPending = true
					lossStart = st.LastRecordUnix
				}
				hadCursor = false // only the first reset of a resumed watch signals loss
			case db.WatchSynced:
				// Caught up. Roll the catch-up tail and checkpoint the synced cursor directly (a
				// safe resume point: everything up to it is now durable). If the reset produced no
				// records at all (empty archive), still emit the pending loss report.
				if err := flush(ev.Cursor); err != nil {
					return err
				}
				if lossPending {
					if err := r.emitLoss(lossStart, time.Now().Unix()); err != nil {
						return err
					}
					lossPending = false
				}
				if err := writeStatus(ev.Cursor); err != nil {
					return err
				}
			case db.WatchResync:
				return errResync
			}
		}
	}
}

// emitLoss drops a loss-report ClassAd unless the window is empty (end <= start means no records
// were actually lost -- the oldest retained record is no older than our last export).
func (r *Runner) emitLoss(start, end int64) error {
	if start > 0 && end > 0 && end <= start {
		return nil // no real gap
	}
	now := time.Now().Unix()
	path, err := r.w.WriteLossReport(buildLossReport(r.name, r.cfg.Table, start, end, now), now)
	if err != nil {
		return err
	}
	r.log.Warn("archivedropbox: data loss detected; wrote loss report",
		"exporter", r.name, "path", path, "lossStart", start, "lossEnd", end)
	return nil
}

// awaitDropboxSpace blocks while the dropbox holds at least MaxDropboxBytes of undelivered data,
// so the exporter never floods a dropbox the consumer has stopped draining. It refreshes the
// status beat while waiting so a legitimately-backpressured exporter is not mistaken for wedged.
func (r *Runner) awaitDropboxSpace(ctx context.Context, refresh func([]byte) error) error {
	max := int64(r.cfg.MaxDropboxBytes)
	warned := false
	for {
		size, err := r.w.DirSize()
		if err != nil {
			return err
		}
		if size < max {
			return nil
		}
		if !warned {
			r.log.Warn("archivedropbox: dropbox full; pausing until the consumer drains it",
				"exporter", r.name, "bytes", size, "max", max)
			warned = true
		}
		if err := refresh(nil); err != nil {
			return err
		}
		if !sleepCtx(ctx, r.backpressurePoll) {
			return ctx.Err()
		}
	}
}

// beatInterval picks a status-refresh cadence: fast enough for the daemon's liveness monitor
// (<= beatCap) but never faster than makes sense for a short RollInterval.
func beatInterval(roll time.Duration) time.Duration {
	iv := roll
	if iv > beatCap {
		iv = beatCap
	}
	if iv < 100*time.Millisecond {
		iv = 100 * time.Millisecond
	}
	return iv
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > sessionMaxBackoff {
		return sessionMaxBackoff
	}
	return d
}

// sleepCtx waits for d or ctx cancellation; it returns false if ctx was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
