// Package scheddsync mirrors an HTCondor schedd's on-disk state into an htcondordb
// database: it replays the job_queue.log (the ClassAdLog of live jobs) into a mutable
// table and tails the history file(s) of completed jobs into an archive table. Both are
// followed live and survive the schedd rotating them.
//
// The job_queue.log parser, offset tracking, and rotation detection are reused from
// golang-htcondor's classadlog package; scheddsync applies the parsed entries DIRECTLY to
// the target DB (the single materialized copy -- it does not hold a second in-memory copy
// of the queue), buffering each on-disk transaction into one atomic DB transaction.
package scheddsync

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/golang-htcondor/classadlog"
)

// DefaultPollInterval is how often job_queue.log is polled when unset.
const DefaultPollInterval = 200 * time.Millisecond

// JobSync tails a schedd job_queue.log and applies its committed changes to a mutable DB
// table. It reuses classadlog's parser/prober for parsing + rotation detection, but keeps
// no in-memory copy of the queue -- the DB table is the materialized state.
type JobSync struct {
	target   *db.DB
	parser   *classadlog.Parser
	prober   *classadlog.Prober
	interval time.Duration
	log      *slog.Logger

	// tx is the DB transaction currently accumulating the on-disk transaction being
	// replayed. It persists ACROSS polls when a schedd transaction spans a poll boundary
	// (BeginTransaction seen, EndTransaction not yet). implicit is set when tx batches
	// ops that were written outside an explicit transaction (committed at end of a read
	// pass); an explicit transaction stays open until its EndTransaction.
	tx       *db.Txn
	implicit bool

	// store durably records the resume position (offset + which file we were reading) after
	// each committed batch, so a restart resumes instead of replaying the whole log -- and
	// detects a compaction/rotation that happened while we were down. nil disables it.
	store    PositionStore
	restored bool // whether restore() has run this process

	// curID identifies the file we last read from; a differing inode on the next poll means
	// the schedd rotated/compacted the log (it writes job_queue.log.tmp then renames over it,
	// so the new file's size may equal or exceed our offset -- which the size-based prober
	// misses). haveID gates the check until the first successful read.
	curID  fileIdentity
	haveID bool
}

// reconcileBatch bounds how many buffered writes a reconciling reload commits at once, so a
// large compaction never holds the whole delta in one transaction.
const reconcileBatch = 4096

// JobSyncConfig configures a JobSync.
type JobSyncConfig struct {
	Filename     string        // path to job_queue.log (required)
	PollInterval time.Duration // default 200ms
	Logger       *slog.Logger  // default slog.Default()
	// Store, if set, durably records the resume position so a restart resumes instead of
	// replaying the whole log, and recovers correctly if the log was compacted while down.
	Store PositionStore
}

// NewJobSync creates a syncer that mirrors cfg.Filename into target. The log need not
// exist yet; it is picked up when it appears.
func NewJobSync(target *db.DB, cfg JobSyncConfig) *JobSync {
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &JobSync{
		target:   target,
		parser:   classadlog.NewParser(cfg.Filename),
		prober:   classadlog.NewProber(),
		interval: interval,
		log:      logger,
		store:    cfg.Store,
	}
}

// Run polls and applies until ctx is cancelled, starting with an immediate poll. Transient
// errors (e.g. the log not existing yet) are logged and retried on the next tick.
func (s *JobSync) Run(ctx context.Context) error {
	if err := s.Poll(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn("job_queue.log initial poll failed", "err", err.Error())
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.abort()
			return ctx.Err()
		case <-ticker.C:
			if err := s.Poll(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Warn("job_queue.log poll failed", "err", err.Error())
			}
		}
	}
}

// Poll probes the log and applies any new committed changes. Exported for synchronous
// control in tests.
func (s *JobSync) Poll(ctx context.Context) error {
	if !s.restored {
		if err := s.restore(ctx); err != nil {
			return err
		}
		s.restored = true
	}
	// Detect rotation/compaction independently of the size-based prober: if the path now
	// names a different inode than the file we last read, the schedd replaced the log (a
	// new inode whose size may equal or exceed our offset, which the prober's size heuristic
	// would misread as a plain append and read our stale offset into the new file).
	if s.haveID {
		if cur, serr := statIdentity(s.parser.GetFilename()); serr == nil && !sameFileIdentity(cur, s.curID) {
			return s.reconcileReload(ctx)
		}
	}
	result, err := s.prober.Probe(s.parser.GetFilename(), s.parser.GetNextOffset())
	if err != nil {
		return err
	}
	switch result {
	case classadlog.ProbeNoChange:
		return nil
	case classadlog.ProbeCompressed:
		return s.reconcileReload(ctx)
	case classadlog.ProbeAddition:
		return s.readAndApply(ctx, false)
	default:
		// ProbeError / ProbeFatalError / unknown.
		return errors.New("scheddsync: probe error on " + s.parser.GetFilename())
	}
}

// reconcileReload rebuilds the table from the current log after a rotation/compaction WITHOUT
// truncating and WITHOUT re-publishing unchanged jobs. It replays the current log (a complete
// copy of the live jobs) one key at a time -- the schedd writes each job's ops contiguously --
// comparing each reconstructed job against the table and writing it only when new or changed;
// then it deletes the pre-reload keys the log no longer mentions (jobs that completed while it
// was rotated away). So a compaction produces exactly the real deltas: a delete per completed
// job, an upsert per changed job, and nothing at all for the (typically vast majority)
// unchanged jobs -- where Truncate+replay would blink every job out (Truncate emits no delete,
// so watchers keep phantoms) and re-add all of them. Peak extra memory is one job ad plus the
// key sets the sweep already needs; there is no second copy of the queue. The position is
// checkpointed only after the sweep commits, so a crash mid-reload re-runs the idempotent
// reconcile rather than resuming past an unfinished table.
func (s *JobSync) reconcileReload(ctx context.Context) (err error) {
	s.abort()
	s.parser.SetNextOffset(0)
	s.prober.Reset()
	before := s.target.Keys()
	seen := make(map[string]struct{}, len(before))

	if oerr := s.parser.Open(); oerr != nil {
		return oerr
	}
	// closeParser finalizes the parser's next offset (Close) so a subsequent checkpoint is
	// accurate, refreshes the prober baseline, and records the file identity for the next
	// poll's rotation check. Idempotent; deferred so it also runs on an early error return.
	closed := false
	closeParser := func() {
		if closed {
			return
		}
		closed = true
		_ = s.parser.Close()
		if uerr := s.prober.Update(s.parser.GetFilename()); uerr != nil && err == nil {
			err = uerr
		}
		if id, ierr := statIdentity(s.parser.GetFilename()); ierr == nil {
			s.curID, s.haveID = id, true
		}
	}
	defer closeParser()

	rec := &reconciler{target: s.target, seen: seen, log: s.log}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		entry, rerr := s.parser.ReadEntry()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			return rerr
		}
		if aerr := rec.apply(entry); aerr != nil {
			return aerr
		}
	}
	closeParser() // finalize the offset before we checkpoint below
	if err != nil {
		return err // prober update failed
	}
	if err = rec.finish(); err != nil { // flush the last key and commit the final batch
		return err
	}
	if err = s.sweepKeys(before, seen); err != nil {
		return err
	}
	s.checkpoint() // position recorded only after the reconciled table matches the log
	return nil
}

// sweepKeys deletes every key present before a reconcile reload that the current log no longer
// contains -- the jobs that completed (and were compacted away) while the log was rotated.
// Deletions are committed in bounded batches so a large sweep never holds one giant transaction.
func (s *JobSync) sweepKeys(before []string, seen map[string]struct{}) error {
	var batch *db.Txn
	n := 0
	commit := func() error {
		if batch == nil {
			return nil
		}
		err := batch.Commit()
		batch, n = nil, 0
		return err
	}
	for _, k := range before {
		if _, ok := seen[k]; ok {
			continue
		}
		if batch == nil {
			batch = s.target.Begin()
		}
		batch.DestroyClassAd(k)
		if n++; n >= reconcileBatch {
			if err := commit(); err != nil {
				return err
			}
		}
	}
	return commit()
}

// reconciler applies a reconcile reload's log stream one key at a time. Because the schedd
// writes each job's ops contiguously, it accumulates the current key's ad and, when the key
// changes (or at end of stream), compares it against the table and writes only a real delta --
// holding just one ad at a time. Writes are buffered into a transaction committed in bounded
// batches. The op handling mirrors JobSync.applyEntry; keep the two in sync.
type reconciler struct {
	target *db.DB
	seen   map[string]struct{}
	log    *slog.Logger

	batch *db.Txn
	n     int

	curKey  string
	curAd   *classad.ClassAd
	destroy bool // the current key was destroyed within the log window
}

func (r *reconciler) apply(e *classadlog.LogEntry) error {
	switch e.OpType {
	case classadlog.OpBeginTransaction, classadlog.OpEndTransaction, classadlog.OpLogHistoricalSequenceNumber:
		return nil // transaction grouping is irrelevant to a full-state reconcile
	}
	if e.Key != r.curKey {
		if err := r.flush(); err != nil {
			return err
		}
		r.curKey, r.curAd, r.destroy = e.Key, classad.New(), false
	}
	switch e.OpType {
	case classadlog.OpNewClassAd:
		r.curAd, r.destroy = classad.New(), false
		if e.MyType != "" && e.MyType != "(unknown)" {
			r.curAd.InsertAttrString("MyType", e.MyType)
		}
		if e.TargetType != "" && e.TargetType != "(unknown)" {
			r.curAd.InsertAttrString("TargetType", e.TargetType)
		}
	case classadlog.OpDestroyClassAd:
		r.destroy = true
	case classadlog.OpSetAttribute:
		expr, perr := classad.ParseExpr(e.Value)
		if perr != nil {
			r.log.Warn("job_queue.log: skipping unparseable attribute",
				"key", e.Key, "attr", e.Name, "err", perr.Error())
			return nil
		}
		r.curAd.InsertExpr(e.Name, expr)
		r.destroy = false
	case classadlog.OpDeleteAttribute:
		r.curAd.Delete(e.Name)
	}
	return nil
}

// flush finalizes the current key: it records the key as seen (so the sweep keeps it) and
// writes it only when the table lacks it or holds a different ad -- an unchanged job produces
// no write and therefore no watch event.
func (r *reconciler) flush() error {
	if r.curKey == "" {
		return nil
	}
	r.seen[r.curKey] = struct{}{}
	cur, ok := r.target.LookupClassAd(r.curKey)
	switch {
	case r.destroy:
		if ok {
			r.batchTx().DestroyClassAd(r.curKey)
			r.n++
		}
	case !ok || !cur.Equal(r.curAd):
		r.batchTx().NewClassAd(r.curKey, r.curAd)
		r.n++
	}
	r.curKey, r.curAd, r.destroy = "", nil, false
	if r.n >= reconcileBatch {
		return r.commit()
	}
	return nil
}

func (r *reconciler) batchTx() *db.Txn {
	if r.batch == nil {
		r.batch = r.target.Begin()
	}
	return r.batch
}

func (r *reconciler) commit() error {
	if r.batch == nil {
		return nil
	}
	err := r.batch.Commit()
	r.batch, r.n = nil, 0
	return err
}

// finish flushes the last accumulated key and commits any remaining buffered writes.
func (r *reconciler) finish() error {
	if err := r.flush(); err != nil {
		return err
	}
	return r.commit()
}

// restore positions the syncer from the persisted resume point (once, before the first
// read). It resumes in place only when the saved position still refers to the SAME file and
// that file has not shrunk below the saved offset; otherwise (no saved position, a different
// inode, a shorter file, an unreadable/absent log) the log was compacted or rotated while we
// were down -- or we simply don't know how far we got -- so it rebuilds from a clean table.
// Resuming replays the bytes from the saved offset to EOF, which include any DestroyClassAd
// for jobs that ended while we were down, keeping the table correct without a full rebuild.
// The rebuild reconciles rather than truncates, so a persistent table keeps its unchanged
// jobs (only the ones the current log dropped are deleted).
func (s *JobSync) restore(ctx context.Context) error {
	if s.store == nil {
		return nil // persistence disabled: legacy behavior (replay from the start each run)
	}
	blob, ok, err := s.store.Load()
	if err != nil {
		return err
	}
	if ok {
		if pos, derr := decodeJobPosition(blob); derr == nil {
			if cur, serr := statIdentity(s.parser.GetFilename()); serr == nil &&
				sameFileIdentity(cur, pos.File) && cur.Size >= pos.Offset {
				s.parser.SetNextOffset(pos.Offset)
				s.curID, s.haveID = cur, true
				return nil
			}
			s.log.Info("scheddsync: job_queue.log rotated/compacted while down; rebuilding")
		} else {
			s.log.Warn("scheddsync: unreadable saved position; rebuilding", "err", derr.Error())
		}
	}
	return s.reconcileReload(ctx)
}

// checkpoint durably records the resume position after a clean read pass. It saves only at a
// batch boundary with no explicit transaction open (s.tx == nil) -- committing the offset in
// the middle of an unfinished on-disk transaction would resume past its BeginTransaction --
// reconcileReload does not use s.tx; it calls checkpoint itself once its sweep commits.
func (s *JobSync) checkpoint() {
	if s.store == nil || s.tx != nil {
		return
	}
	id, err := statIdentity(s.parser.GetFilename())
	if err != nil {
		return
	}
	blob, err := jobPosition{File: id, Offset: s.parser.GetNextOffset()}.encode()
	if err != nil {
		return
	}
	if serr := s.store.Save(blob); serr != nil {
		s.log.Warn("scheddsync: saving job position failed", "err", serr.Error())
	}
}

// readAndApply reads entries from the current offset to EOF, applying them. reload marks a
// full replay (offset already rewound). It updates the prober so the next probe is relative
// to what was consumed.
func (s *JobSync) readAndApply(ctx context.Context, reload bool) (err error) {
	if oerr := s.parser.Open(); oerr != nil {
		return oerr
	}
	defer func() {
		// Close finalizes the parser's next offset to the current file position; only after
		// it is the offset accurate to checkpoint.
		_ = s.parser.Close()
		if uerr := s.prober.Update(s.parser.GetFilename()); uerr != nil && err == nil {
			err = uerr
		}
		// Record the inode we just read so the next poll can detect a rotation to a new file.
		if id, ierr := statIdentity(s.parser.GetFilename()); ierr == nil {
			s.curID, s.haveID = id, true
		}
		if err == nil {
			s.checkpoint()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		entry, rerr := s.parser.ReadEntry()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			return rerr
		}
		if aerr := s.applyEntry(entry); aerr != nil {
			return aerr
		}
	}
	// End of this read pass: commit an implicit (non-explicit-transaction) batch. An
	// explicit transaction with no EndTransaction yet stays open across polls.
	if s.tx != nil && s.implicit {
		err = s.commit()
	}
	// Durably record how far we got, but only at a clean boundary (no open explicit
	// transaction) so a resume never lands mid-transaction. Saved after the commit above, so
	// the position never runs ahead of applied data.
	if err == nil {
		s.checkpoint()
	}
	return err
}

// applyEntry applies one log entry to the target, managing the buffered DB transaction.
func (s *JobSync) applyEntry(e *classadlog.LogEntry) error {
	switch e.OpType {
	case classadlog.OpBeginTransaction:
		// A commit of any implicit batch precedes an explicit transaction.
		if s.tx != nil && s.implicit {
			if err := s.commit(); err != nil {
				return err
			}
		}
		if s.tx == nil {
			s.tx = s.target.Begin()
			s.implicit = false
		}
		return nil
	case classadlog.OpEndTransaction:
		if s.tx != nil && !s.implicit {
			return s.commit()
		}
		return nil
	case classadlog.OpLogHistoricalSequenceNumber:
		return nil
	}

	tx := s.ensureTx()
	switch e.OpType {
	case classadlog.OpNewClassAd:
		ad := classad.New()
		if e.MyType != "" && e.MyType != "(unknown)" {
			ad.InsertAttrString("MyType", e.MyType)
		}
		if e.TargetType != "" && e.TargetType != "(unknown)" {
			ad.InsertAttrString("TargetType", e.TargetType)
		}
		tx.NewClassAd(e.Key, ad)
	case classadlog.OpDestroyClassAd:
		tx.DestroyClassAd(e.Key)
	case classadlog.OpSetAttribute:
		if err := tx.SetAttribute(e.Key, e.Name, e.Value); err != nil {
			// A single malformed value must not abort the whole sync; skip it.
			s.log.Warn("job_queue.log: skipping unparseable attribute",
				"key", e.Key, "attr", e.Name, "err", err.Error())
		}
	case classadlog.OpDeleteAttribute:
		tx.DeleteAttribute(e.Key, e.Name)
	}
	return nil
}

// ensureTx returns the open transaction, starting an implicit one if none is open (for a
// data op written outside an explicit transaction).
func (s *JobSync) ensureTx() *db.Txn {
	if s.tx == nil {
		s.tx = s.target.Begin()
		s.implicit = true
	}
	return s.tx
}

func (s *JobSync) commit() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.Commit()
	s.tx = nil
	s.implicit = false
	return err
}

func (s *JobSync) abort() {
	if s.tx != nil {
		s.tx.Abort()
		s.tx = nil
		s.implicit = false
	}
}
