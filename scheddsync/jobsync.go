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

	// reconcile is set during a reconciling reload (after a rotation/compaction). While set,
	// applyEntry records every key the current log contains in seen; the reload then deletes
	// only the pre-reload keys the new log no longer mentions -- the jobs that completed while
	// the log was rotated away -- instead of truncating. Unlike Truncate+replay this emits a
	// real delete for each such job (Truncate drops them silently, leaving phantom copies in
	// downstream watchers) and never makes a surviving job blink out. The resume position is
	// checkpointed only after the post-reload sweep commits.
	reconcile bool
	seen      map[string]struct{}
}

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

// reconcileReload rebuilds the table from the current log after a rotation/compaction
// WITHOUT truncating. It rewinds to the start of the current log, replays it (upserting every
// job the log contains and recording each key in seen), then deletes only the pre-reload keys
// the new log no longer mentions -- the jobs that completed and were compacted away while the
// log was rotated. This emits a real delete for each such job (Truncate would drop them
// silently, leaving phantom copies in downstream watchers) and never makes a surviving job
// blink out. The resume position is checkpointed only after the sweep commits, so a crash
// mid-reload re-runs the (idempotent) reconcile rather than resuming past an un-swept table.
func (s *JobSync) reconcileReload(ctx context.Context) error {
	s.abort()
	s.parser.SetNextOffset(0)
	s.prober.Reset()
	before := s.target.Keys()
	s.reconcile = true
	s.seen = make(map[string]struct{}, len(before))
	err := s.readAndApply(ctx, true)
	if err == nil {
		err = s.sweep(before)
	}
	s.reconcile = false
	s.seen = nil
	if err == nil {
		s.checkpoint() // position recorded only after the swept table matches the log
	}
	return err
}

// sweep deletes every key present before a reconcile reload that the current log no longer
// contains -- the jobs that completed (and were compacted away) while the log was rotated.
func (s *JobSync) sweep(before []string) error {
	var stale []string
	for _, k := range before {
		if _, ok := s.seen[k]; !ok {
			stale = append(stale, k)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	tx := s.target.Begin()
	for _, k := range stale {
		tx.DestroyClassAd(k)
	}
	return tx.Commit()
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
// and never mid-reconcile (s.reconcile), where the table is not yet swept; reconcileReload
// checkpoints explicitly once the sweep commits.
func (s *JobSync) checkpoint() {
	if s.store == nil || s.tx != nil || s.reconcile {
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
	if s.reconcile {
		// Record every key the current log contains so the post-reload sweep can delete only
		// the pre-reload keys it no longer mentions.
		s.seen[e.Key] = struct{}{}
	}
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
