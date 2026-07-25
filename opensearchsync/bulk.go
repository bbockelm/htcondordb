package opensearchsync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Doc is one transformed document to index: Version is stamped as the external version so a
// stale/duplicate re-index is dropped by OpenSearch as a no-op version conflict.
type Doc struct {
	ID      string
	Version uint64
	Body    []byte // marshaled JSON document
}

// Batch is a unit of work submitted to the Uploader. Cursor is the watch resume point AFTER
// this batch's last change — nil mid-snapshot (no resumable point yet).
type Batch struct {
	Docs   []Doc
	Cursor []byte
}

// ItemError is a permanent per-document bulk failure (not a 409 conflict or a transient
// 429/503): a bad mapping or malformed doc. Matching adstash, these are logged, counted, and
// skipped so a single poison document can't wedge the sync.
type ItemError struct {
	ID     string
	Status int
	Type   string
	Reason string
}

// BulkOutcome is the classified result of one _bulk round-trip. Retryable counts items the
// server asked us to retry (429/503); Permanent holds items that failed unrecoverably.
type BulkOutcome struct {
	Indexed   int
	Conflicts int
	Retryable int
	Permanent []ItemError
}

// BulkClient performs one _bulk request and classifies its per-item results. A non-nil error
// is a whole-request (transport) failure that the Uploader retries.
type BulkClient interface {
	Bulk(ctx context.Context, index string, ndjson []byte) (BulkOutcome, error)
}

const (
	bulkMaxRetries  = 5
	bulkInitBackoff = 500 * time.Millisecond
	bulkMaxBackoff  = 15 * time.Second
	externalVersion = "external"
)

// Uploader submits batches to OpenSearch with a bounded in-flight window and tracks the oldest
// MVCC/export sequence still in flight, so the resume cursor advances only over the oldest
// fully-acked contiguous prefix. Submit blocks when too many documents are in flight
// (backpressure), which bounds how much is replayed on restart.
//
// It is SDK-agnostic (BulkClient), so the whole in-flight/watermark/backpressure machinery is
// unit-testable against a fake client.
type Uploader struct {
	client  BulkClient
	index   string
	log     *slog.Logger
	maxDocs int
	maxConc int

	// retry backoff bounds (defaults from the bulk* constants; overridable in tests).
	initBackoff time.Duration
	maxBackoff  time.Duration

	mu        sync.Mutex
	cond      *sync.Cond
	cancelled bool
	fatal     error

	inFlight  int    // documents currently in flight
	conc      int    // outstanding bulk requests
	submitSeq uint64 // next batch sequence to assign

	// contiguous-ack watermark: batches complete possibly out of order; the committed cursor
	// is that of the newest batch S such that all batches [0,S] are done.
	ackSeq      uint64            // lowest batch seq not yet acked (head of the prefix)
	doneCursors map[uint64][]byte // acked-but-ahead batches: seq -> cursor
	committed   []byte            // resume cursor of the fully-acked prefix

	indexed uint64
	skipped uint64
}

// NewUploader starts an Uploader bound to ctx (cancelling ctx unblocks any waiting Submit).
func NewUploader(ctx context.Context, client BulkClient, cfg Config, log *slog.Logger) *Uploader {
	u := &Uploader{
		client:      client,
		index:       cfg.Index,
		log:         log,
		maxDocs:     cfg.MaxInFlightDocs,
		maxConc:     cfg.MaxConcurrentBulk,
		initBackoff: bulkInitBackoff,
		maxBackoff:  bulkMaxBackoff,
		doneCursors: map[uint64][]byte{},
	}
	u.cond = sync.NewCond(&u.mu)
	go func() {
		<-ctx.Done()
		u.mu.Lock()
		u.cancelled = true
		u.cond.Broadcast()
		u.mu.Unlock()
	}()
	return u
}

// Submit hands a batch to the in-flight window, blocking while doing so would exceed the
// document or concurrency cap (backpressure). It returns immediately once the batch is
// accepted; the upload proceeds asynchronously. A nil/empty batch is a no-op.
func (u *Uploader) Submit(ctx context.Context, b Batch) error {
	n := len(b.Docs)
	if n == 0 {
		return nil
	}
	u.mu.Lock()
	// Block while at capacity — but always make progress when nothing is in flight (a lone
	// batch may equal the whole budget; Config.Validate guarantees maxDocs >= BatchSize).
	for u.fatal == nil && !u.cancelled && u.conc > 0 &&
		(u.inFlight+n > u.maxDocs || u.conc >= u.maxConc) {
		u.cond.Wait()
	}
	if u.fatal != nil {
		err := u.fatal
		u.mu.Unlock()
		return err
	}
	if u.cancelled {
		u.mu.Unlock()
		return ctx.Err()
	}
	seq := u.submitSeq
	u.submitSeq++
	u.inFlight += n
	u.conc++
	u.mu.Unlock()

	go u.run(ctx, seq, b)
	return nil
}

// run performs one batch's bulk upload with bounded retries, then records completion and the
// contiguous-ack watermark.
func (u *Uploader) run(ctx context.Context, seq uint64, b Batch) {
	body := buildBulkBody(u.index, b.Docs)
	err := u.upload(ctx, body, len(b.Docs))

	u.mu.Lock()
	defer u.mu.Unlock()
	u.inFlight -= len(b.Docs)
	u.conc--
	if err != nil {
		if u.fatal == nil {
			u.fatal = err
		}
	} else {
		u.doneCursors[seq] = b.Cursor
		// Advance the committed prefix over every now-contiguous acked batch.
		for {
			cur, ok := u.doneCursors[u.ackSeq]
			if !ok {
				break
			}
			if len(cur) > 0 {
				u.committed = cur
			}
			delete(u.doneCursors, u.ackSeq)
			u.ackSeq++
		}
	}
	u.cond.Broadcast()
}

// upload sends one bulk body, retrying transient (whole-request or 429/503) failures with
// backoff. Permanent per-item errors are logged and counted but do not fail the batch (adstash
// behavior). A retry re-sends the whole body; external versioning makes the re-send idempotent.
func (u *Uploader) upload(ctx context.Context, body []byte, n int) error {
	backoff := u.initBackoff
	for attempt := 0; ; attempt++ {
		out, err := u.client.Bulk(ctx, u.index, body)
		switch {
		case err != nil:
			if attempt >= bulkMaxRetries {
				return fmt.Errorf("opensearchsync: bulk request failed after %d retries: %w", attempt, err)
			}
			u.log.Warn("opensearchsync: bulk request error; retrying", "attempt", attempt+1, "err", err)
		case out.Retryable > 0:
			if attempt >= bulkMaxRetries {
				return fmt.Errorf("opensearchsync: %d/%d docs still retryable after %d attempts", out.Retryable, n, attempt)
			}
			u.log.Warn("opensearchsync: bulk items throttled; retrying batch", "retryable", out.Retryable, "attempt", attempt+1)
		default:
			// Done. Count indexed + conflicts (no-op idempotent) as success; log+skip permanent.
			u.mu.Lock()
			u.indexed += uint64(out.Indexed + out.Conflicts)
			u.skipped += uint64(len(out.Permanent))
			u.mu.Unlock()
			for _, e := range out.Permanent {
				u.log.Warn("opensearchsync: dropping document on permanent bulk error",
					"id", e.ID, "status", e.Status, "type", e.Type, "reason", e.Reason)
			}
			return nil
		}
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
		backoff = nextBulkBackoff(backoff, u.maxBackoff)
	}
}

// Committed returns the resume cursor of the oldest fully-acked contiguous prefix (nil if
// nothing has been acked yet with a resumable cursor).
func (u *Uploader) Committed() []byte {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.committed
}

// Fatal returns the first unrecoverable error, if any.
func (u *Uploader) Fatal() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.fatal
}

// InFlight reports the number of documents currently in flight (for tests/metrics).
func (u *Uploader) InFlight() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.inFlight
}

// Stats returns cumulative counts (indexed includes idempotent conflicts).
func (u *Uploader) Stats() (indexed, skipped uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.indexed, u.skipped
}

// Drain waits until all in-flight batches complete (or ctx is cancelled / a fatal error
// occurs), then returns the fatal error if any. Used before a checkpoint or at shutdown.
func (u *Uploader) Drain(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	for u.conc > 0 && u.fatal == nil && !u.cancelled {
		u.cond.Wait()
	}
	if u.fatal != nil {
		return u.fatal
	}
	if u.cancelled {
		return ctx.Err()
	}
	return nil
}

// buildBulkBody renders the _bulk NDJSON for a batch: an "index" action per doc carrying its
// _id and external version, followed by the document.
func buildBulkBody(index string, docs []Doc) []byte {
	var buf []byte
	for _, d := range docs {
		action := bulkAction{}
		action.Index.ID = d.ID
		action.Index.Version = d.Version
		action.Index.VersionType = externalVersion
		line, _ := json.Marshal(action)
		buf = append(buf, line...)
		buf = append(buf, '\n')
		buf = append(buf, d.Body...)
		buf = append(buf, '\n')
	}
	return buf
}

type bulkAction struct {
	Index struct {
		ID          string `json:"_id"`
		Version     uint64 `json:"version"`
		VersionType string `json:"version_type"`
	} `json:"index"`
}

// classifyStatus maps an HTTP per-item status to its handling: conflict (409, idempotent),
// retryable (429/503), or permanent.
func classifyStatus(status int) (conflict, retryable bool) {
	switch status {
	case 409:
		return true, false
	case 429, 502, 503, 504:
		return false, true
	default:
		return false, false
	}
}

func nextBulkBackoff(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}

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
