package kafkasync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Record header keys. The version header is the monotonic ExportSeq (8-byte big-endian): a
// consumer keeps the highest-versioned value per key, so duplicates and re-materialized
// snapshots converge. content-type is absent on a tombstone (delete).
const (
	HeaderVersion     = "htc-version"
	HeaderSourceTable = "htc-source-table"
	HeaderContentType = "content-type"
	ContentTypeAd     = "application/x-classad"
)

// errResync signals the watch stream must be reconnected from the last checkpoint (an
// explicit WatchResync, or the stream closing unexpectedly). It is not a failure.
var errResync = errors.New("kafkasync: watch resync")

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 30 * time.Second
)

// Runner mirrors one table's change stream into Kafka for a single exporter definition. It
// is a dbrpc client end to end: it reads/writes its resume state through the catalog's
// exporter registry and watches the table over the wire. Delivery is at-least-once (see the
// package doc); the reset/delete-sweep logic here compensates for the change stream having
// no before-image.
type Runner struct {
	name     string
	cfg      Config
	client   *dbrpc.Client
	producer Producer
	log      *slog.Logger

	// MaxConsecutiveFailures, if > 0, makes Run return the last error after that many
	// consecutive failed sessions (a resync resets the count). A supervising process (or
	// the cmd's outer re-dial loop) then reconnects with a fresh CEDAR connection. 0 means
	// retry forever, which suits an embedded caller that owns the connection's lifetime.
	MaxConsecutiveFailures int
}

// NewRunner builds a Runner. A nil log discards output.
func NewRunner(name string, cfg Config, client *dbrpc.Client, producer Producer, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Runner{name: name, cfg: cfg, client: client, producer: producer, log: log}
}

// Run drives the exporter until ctx is cancelled. It reconnects (with backoff) across
// transient errors and resyncs, always resuming from the last checkpointed cursor.
func (r *Runner) Run(ctx context.Context) error {
	backoff := initialBackoff
	fails := 0
	// fail applies backoff and enforces MaxConsecutiveFailures; it returns the error to
	// propagate (giving up) or nil to keep retrying.
	fail := func(err error) error {
		fails++
		if r.MaxConsecutiveFailures > 0 && fails >= r.MaxConsecutiveFailures {
			return fmt.Errorf("kafkasync: giving up after %d consecutive failures: %w", fails, err)
		}
		if !sleep(ctx, backoff) {
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
			r.log.Warn("kafkasync: loading resume state failed", "exporter", r.name, "err", err)
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
			r.log.Info("kafkasync: resync; reconnecting from last checkpoint", "exporter", r.name)
			backoff = initialBackoff
			fails = 0
		default:
			r.log.Warn("kafkasync: session error; reconnecting", "exporter", r.name, "err", err)
			if gaveUp := fail(err); gaveUp != nil {
				return gaveUp
			}
		}
	}
}

func (r *Runner) loadState(ctx context.Context) (*State, error) {
	blob, _, err := r.client.GetExporterState(ctx, r.name)
	if err != nil {
		return nil, err
	}
	return decodeState(blob)
}

// session runs one watch connection: it maps events to records, produces in batches, and
// checkpoints the cursor after the broker acknowledges. State is passed in freshly loaded so
// that any in-memory mutation from a failed prior session is discarded (only acknowledged
// state was checkpointed).
func (r *Runner) session(ctx context.Context, st *State) error {
	events, stop, err := r.client.WatchTable(ctx, r.cfg.Table, st.WireCursor)
	if err != nil {
		return err
	}
	defer stop()

	var (
		batch           []Record
		pendingVersions = map[string]uint64{} // upserts queued but not yet acknowledged
		pendingDeletes  = map[string]bool{}   // tombstones queued but not yet acknowledged
		inReset         bool
		resetSeen       map[string]bool // keys present in the in-progress snapshot
		lastCursor      []byte          // most recent real (non-empty) resume cursor
	)

	// produceBatch sends the queued records and, only on broker ack, folds their effects
	// into st.KeyVersions. It does NOT persist state (that is checkpoint's job).
	produceBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := r.producer.Produce(ctx, batch); err != nil {
			return err
		}
		for k, v := range pendingVersions {
			st.KeyVersions[k] = v
		}
		for k := range pendingDeletes {
			delete(st.KeyVersions, k)
		}
		batch = batch[:0]
		pendingVersions = map[string]uint64{}
		pendingDeletes = map[string]bool{}
		return nil
	}
	// checkpoint produces the batch, then persists the resume state (cursor + ExportSeq +
	// KeyVersions). The produce-then-persist order is the at-least-once boundary.
	checkpoint := func(cursor []byte) error {
		if err := produceBatch(); err != nil {
			return err
		}
		if len(cursor) > 0 {
			st.WireCursor = cursor
		}
		blob, err := st.encode()
		if err != nil {
			return err
		}
		return r.client.PutExporterState(ctx, r.name, blob)
	}

	ticker := time.NewTicker(time.Duration(r.cfg.FlushInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Keep a trickle of changes moving. Mid-snapshot we only produce (to bound
			// memory) since there is no resumable cursor yet.
			if inReset {
				if err := produceBatch(); err != nil {
					return err
				}
			} else if err := checkpoint(lastCursor); err != nil {
				return err
			}
		case ev, ok := <-events:
			if !ok {
				return errResync // stream closed; reconnect from the last checkpoint
			}
			switch db.WatchKind(ev.Kind) {
			case db.WatchUpsert:
				ver := st.ExportSeq
				st.ExportSeq++
				batch = append(batch, r.adRecord(ev.Key, ev.AdText, ver))
				pendingVersions[ev.Key] = ver
				delete(pendingDeletes, ev.Key)
				if inReset {
					resetSeen[ev.Key] = true
				}
				if len(ev.Cursor) > 0 {
					lastCursor = ev.Cursor
				}
				if err := r.maybeFlush(&batch, inReset, ev.Cursor, produceBatch, checkpoint); err != nil {
					return err
				}
			case db.WatchDelete:
				ver := st.ExportSeq
				st.ExportSeq++
				batch = append(batch, r.tombstone(ev.Key, ver))
				pendingDeletes[ev.Key] = true
				delete(pendingVersions, ev.Key)
				if len(ev.Cursor) > 0 {
					lastCursor = ev.Cursor
				}
				if err := r.maybeFlush(&batch, inReset, ev.Cursor, produceBatch, checkpoint); err != nil {
					return err
				}
			case db.WatchReset:
				// Discard downstream-derived state; an authoritative snapshot follows.
				inReset = true
				resetSeen = map[string]bool{}
				batch = batch[:0]
				pendingVersions = map[string]uint64{}
				pendingDeletes = map[string]bool{}
			case db.WatchSynced:
				if inReset {
					// Delete-sweep: any key we had that the snapshot did not reproduce was
					// deleted during the gap. The change stream cannot tell us (no
					// before-image), so we infer it and emit a tombstone.
					for k := range st.KeyVersions {
						if !resetSeen[k] {
							ver := st.ExportSeq
							st.ExportSeq++
							batch = append(batch, r.tombstone(k, ver))
							pendingDeletes[k] = true
						}
					}
				}
				if err := checkpoint(ev.Cursor); err != nil {
					return err
				}
				if len(ev.Cursor) > 0 {
					lastCursor = ev.Cursor
				}
				inReset = false
				resetSeen = nil
			case db.WatchResync:
				return errResync
			}
		}
	}
}

// maybeFlush produces (mid-snapshot) or checkpoints (live) once the batch reaches the
// configured size, bounding memory during a large replay while only advancing the cursor at
// resumable points.
func (r *Runner) maybeFlush(batch *[]Record, inReset bool, cursor []byte, produceBatch func() error, checkpoint func([]byte) error) error {
	if len(*batch) < r.cfg.BatchSize {
		return nil
	}
	if inReset {
		return produceBatch()
	}
	return checkpoint(cursor)
}

func (r *Runner) adRecord(key, adText string, ver uint64) Record {
	return Record{
		Key:     []byte(key),
		Value:   []byte(adText),
		Headers: r.headers(ver, ContentTypeAd),
	}
}

func (r *Runner) tombstone(key string, ver uint64) Record {
	return Record{Key: []byte(key), Value: nil, Headers: r.headers(ver, "")}
}

func (r *Runner) headers(ver uint64, contentType string) []Header {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, ver)
	hs := []Header{
		{Key: HeaderVersion, Value: v},
		{Key: HeaderSourceTable, Value: []byte(r.cfg.Table)},
	}
	if contentType != "" {
		hs = append(hs, Header{Key: HeaderContentType, Value: []byte(contentType)})
	}
	return hs
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

// sleep waits for d or ctx cancellation; it returns false if ctx was cancelled.
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
