package opensearchsync

import (
	"bytes"
	"context"
	"encoding/json"
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
// WatchResync, or the stream closing). It is not a failure.
var errResync = errors.New("opensearchsync: watch resync")

const (
	sessionInitBackoff = 500 * time.Millisecond
	sessionMaxBackoff  = 30 * time.Second
)

// Runner mirrors one archive/table's change stream into an OpenSearch index for a single
// exporter definition. It is a dbrpc client end to end: it reads/writes its resume state
// through the exporter registry and watches the table over the wire, transforms each ad with
// the adstash-compatible transform, and bulk-indexes with a bounded in-flight window. Delivery
// is at-least-once; external versioning makes the replayed tail idempotent.
type Runner struct {
	name   string
	cfg    Config
	client *dbrpc.Client
	bulk   BulkClient
	xform  *Transformer
	log    *slog.Logger

	// MaxConsecutiveFailures, if > 0, makes Run return after that many consecutive failed
	// sessions (a resync resets the count). 0 retries forever.
	MaxConsecutiveFailures int
}

// NewRunner builds a Runner. launchTime (unix seconds) seeds the transform's record_time
// fallback. A nil log discards output.
func NewRunner(name string, cfg Config, client *dbrpc.Client, bulk BulkClient, launchTime int64, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Runner{name: name, cfg: cfg, client: client, bulk: bulk, xform: NewTransformer(launchTime), log: log}
}

// Run drives the exporter until ctx is cancelled, reconnecting (with backoff) across transient
// errors and resyncs, always resuming from the last checkpointed cursor.
func (r *Runner) Run(ctx context.Context) error {
	backoff := sessionInitBackoff
	fails := 0
	fail := func(err error) error {
		fails++
		if r.MaxConsecutiveFailures > 0 && fails >= r.MaxConsecutiveFailures {
			return fmt.Errorf("opensearchsync: giving up after %d consecutive failures: %w", fails, err)
		}
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
		backoff = nextBulkBackoff(backoff, sessionMaxBackoff)
		return nil
	}
	for {
		st, err := r.loadState(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.log.Warn("opensearchsync: loading resume state failed", "exporter", r.name, "err", err)
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
			r.log.Info("opensearchsync: resync; reconnecting from last checkpoint", "exporter", r.name)
			backoff = sessionInitBackoff
			fails = 0
		default:
			r.log.Warn("opensearchsync: session error; reconnecting", "exporter", r.name, "err", err)
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

// session runs one watch connection: it transforms events into documents, submits them to the
// uploader in batches, and checkpoints the uploader's committed (fully-acked) cursor. The
// uploader owns the in-flight window and the at-least-once boundary (checkpoint only advances
// over acknowledged documents).
func (r *Runner) session(ctx context.Context, st *State) error {
	events, stop, err := r.client.WatchTable(ctx, r.cfg.Table, st.WireCursor)
	if err != nil {
		return err
	}
	defer stop()

	upl := NewUploader(ctx, r.bulk, r.cfg, r.log)

	var (
		pending    []Doc  // documents accumulated for the next batch
		lastCursor []byte // resume cursor of the newest event seen (batch boundary)
	)

	// submit hands the pending docs to the uploader as one batch (blocking on backpressure).
	submit := func(cursor []byte) error {
		if len(pending) == 0 {
			return nil
		}
		b := Batch{Docs: pending, Cursor: cursor}
		pending = nil
		return upl.Submit(ctx, b)
	}
	// writeState refreshes the live status (beat + uploader progress), advances the durable
	// resume cursor if it moved past the stored one (the caller must guarantee everything up to
	// cursor has been acknowledged), and persists. It ALWAYS writes -- even when the cursor did
	// not advance -- so the daemon sees a fresh beat on an idle-but-healthy exporter and does not
	// mistake it for a stalled one.
	writeState := func(cursor []byte) error {
		indexed, skipped := upl.Stats()
		st.Status = Status{
			Beat:        time.Now().Unix(),
			DocsIndexed: indexed,
			DocsSkipped: skipped,
			InFlight:    upl.InFlight(),
		}
		if len(cursor) > 0 && !bytes.Equal(cursor, st.WireCursor) {
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
		if err := upl.Fatal(); err != nil {
			return err // an unrecoverable bulk error -> reconnect from the last checkpoint
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Push a partial batch and advance the checkpoint over whatever has been acked
			// (the uploader's committed contiguous-prefix cursor).
			if err := submit(lastCursor); err != nil {
				return err
			}
			if err := writeState(upl.Committed()); err != nil {
				return err
			}
		case ev, ok := <-events:
			if !ok {
				// Stream closed: flush, drain, checkpoint the acked prefix, then reconnect.
				if err := submit(lastCursor); err != nil {
					return err
				}
				_ = upl.Drain(ctx)
				_ = writeState(upl.Committed())
				return errResync
			}
			if len(ev.Cursor) > 0 {
				lastCursor = ev.Cursor
			}
			switch db.WatchKind(ev.Kind) {
			case db.WatchUpsert:
				if err := r.appendDoc(&pending, st, ev.Key, ev.AdText); err != nil {
					r.log.Warn("opensearchsync: skipping undecodable ad", "key", ev.Key, "err", err)
				}
				if len(pending) >= r.cfg.BatchSize {
					if err := submit(ev.Cursor); err != nil {
						return err
					}
				}
			case db.WatchDelete:
				// The history archive is append-only; a delete is unexpected. Ignore it (the
				// document is immutable) but let the cursor advance.
			case db.WatchReset:
				// Drop the pending batch; an authoritative snapshot follows. Re-indexed docs
				// are idempotent (stable id + higher external version overwrites same content).
				pending = nil
			case db.WatchSynced:
				// Caught up: flush, wait for every in-flight doc to be acked, then checkpoint
				// the synced cursor directly. Draining guarantees every change up to this point
				// is durable, so the synced cursor is a safe resume point even when no batch
				// carried it (e.g. small batches submitted mid-replay with empty cursors).
				if err := submit(ev.Cursor); err != nil {
					return err
				}
				if err := upl.Drain(ctx); err != nil {
					return err
				}
				if err := writeState(ev.Cursor); err != nil {
					return err
				}
			case db.WatchResync:
				return errResync
			}
		}
	}
}

// appendDoc parses the ad text (the watch streams new-ClassAd bracketed form), transforms it,
// and appends the resulting document (stamped with the next ExportSeq as its external version)
// to the pending batch. Ads the transform skips (DAG roots, missing required attrs) are
// silently dropped, matching adstash.
func (r *Runner) appendDoc(pending *[]Doc, st *State, key, adText string) error {
	ad, err := classad.Parse(adText)
	if err != nil {
		return err
	}
	id, doc, keep := r.xform.Transform(ad)
	if !keep {
		return nil
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	ver := st.ExportSeq
	st.ExportSeq++
	*pending = append(*pending, Doc{ID: id, Version: ver, Body: body})
	return nil
}
