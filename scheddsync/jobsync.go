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
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/golang-htcondor/classadlog"
)

// KeyAttr is the ad attribute the schedd-sync stamps with each row's storage key, so the
// REPL can address the row for UPDATE/DELETE (its default key attribute is "Key").
const KeyAttr = "Key"

// DefaultPollInterval is how often job_queue.log is polled when unset.
const DefaultPollInterval = 200 * time.Millisecond

// JobSync tails a schedd job_queue.log and applies its committed changes to a mutable DB
// table. It reuses classadlog's parser/prober for parsing + rotation detection, but keeps
// no in-memory copy of the queue -- the DB table is the materialized state.
type JobSync struct {
	// target is the jobs table (real proc ads). users, jobsets, clusters, and header are the
	// sibling tables the other job_queue.log namespaces flatten into: user/owner records, jobset
	// ads, cluster ads, and the single schedd header ad ("0.0", holding queue counters like
	// NextClusterNum). A key's "cluster.proc" namespace (see tableFor) routes it to exactly one of
	// these -- or to none (cluster-private ads and OCU ads are still dropped). Cluster ads get
	// their own durable table so a proc materializing into a pre-existing cluster after a
	// resume-from-offset restart can still chain its cluster's attributes; the header gets one so a
	// job_queue.log reconstruction can restore the schedd's queue counters (id-reuse safety).
	target         *db.DB
	users          *db.DB
	jobsets        *db.DB
	clusters       *db.DB
	header         *db.DB
	clusterprivate *db.DB // cluster-private ads ("C.-2"): per-cluster private attributes
	// logmeta holds a single record: the log's LogHistoricalSequenceNumber (op 107, the
	// sequence number + creation timestamp that head every job_queue.log). It is NOT routed
	// from an ad key -- 107 carries no key -- so it is captured out of band from the log's
	// first line (captureLogMeta) and is never touched by the reconcile sweep.
	logmeta  *db.DB
	parser   *classadlog.Parser
	prober   *classadlog.Prober
	interval time.Duration
	// saveInterval throttles position checkpoints on the steady append path (0 = save every
	// batch); lastSaveAt is when the position was last durably saved.
	saveInterval time.Duration
	lastSaveAt   time.Time
	log          *slog.Logger

	// txs holds one open DB transaction per target table for the on-disk transaction being
	// replayed -- writes across the four tables are separate *db.Txn, opened lazily and committed
	// together. It persists ACROSS polls when a schedd transaction spans a poll boundary
	// (BeginTransaction seen, EndTransaction not yet). explicit marks that an explicit schedd
	// transaction is open; when it is false the open txns batch ops written outside a transaction
	// (committed at the end of a read pass), and an explicit transaction stays open until its
	// EndTransaction.
	txs      map[*db.DB]*db.Txn
	explicit bool

	// children maps a cluster ad key ("0C.-1") to the set of its proc ad keys ("C.P"). Some
	// HTCondor versions keep cluster-wide attributes (ClusterId, Owner, ...) only on the
	// cluster ad and chain the proc ads to it (condor_q's merged view); a flat mirror would
	// drop them. This index lets a new proc inherit its cluster's current attributes and a
	// cluster-ad edit fan out to the chained proc rows -- in either on-disk order. Reset on a
	// reconcile reload (the reconciler re-chains via a separate pass).
	children map[string]map[string]struct{}

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

	// status holds the latest published SyncStatus snapshot, read lock-free by Status().
	status atomic.Pointer[SyncStatus]

	// resyncReq, when set (via Resync), makes the next Poll rebuild the mirror from the current
	// log with reconcileReload. It heals a table corrupted by an older sync without truncating
	// (reconcile writes only real deltas), so live consumers see the corrected rows, not a blink.
	resyncReq atomic.Bool
}

// Resync requests that the next Poll rebuild the mirror from the current job_queue.log
// (reconcileReload). Non-destructive: it re-reads the source and writes only real deltas, so it
// heals rows a prior sync corrupted without wiping the table. Safe to call from another goroutine.
func (s *JobSync) Resync() { s.resyncReq.Store(true) }

// reconcileBatch bounds how many buffered writes a reconciling reload commits at once, so a
// large compaction never holds the whole delta in one transaction.
const reconcileBatch = 4096

// JobSyncConfig configures a JobSync.
type JobSyncConfig struct {
	Filename     string        // path to job_queue.log (required)
	PollInterval time.Duration // default 200ms
	Logger       *slog.Logger  // default slog.Default()
	// Users, Jobsets, Clusters, Header, and ClusterPrivate are the sibling tables the non-proc
	// job_queue.log namespaces flatten into (the jobs table is the NewJobSync target). When any is
	// nil a private in-memory table stands in, so routing and cluster-ad chaining still work for
	// callers that only inspect the jobs table (e.g. tests). Header holds the single schedd header
	// ad ("0.0"); ClusterPrivate holds the per-cluster private ads ("C.-2"). LogMeta holds a single
	// record for the log's sequence header (op 107); it is captured from the log's first line.
	Users          *db.DB
	Jobsets        *db.DB
	Clusters       *db.DB
	Header         *db.DB
	ClusterPrivate *db.DB
	LogMeta        *db.DB
	// Store, if set, durably records the resume position so a restart resumes instead of
	// replaying the whole log, and recovers correctly if the log was compacted while down.
	Store PositionStore
	// SaveInterval throttles how often the resume position is checkpointed on the steady append
	// path: at most one save per interval, so a busy log does not rewrite the position file on
	// every poll. 0 (default) saves after every batch. A crash loses at most one interval of
	// progress, which the syncer re-applies idempotently on restart; rotation/compaction and a
	// clean shutdown always checkpoint regardless.
	SaveInterval time.Duration
}

// NewJobSync creates a syncer that mirrors cfg.Filename into target (the jobs table) and routes
// the schedd's other namespaces into cfg.Users/Jobsets/Clusters. The log need not exist yet; it
// is picked up when it appears.
func NewJobSync(target *db.DB, cfg JobSyncConfig) *JobSync {
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	users, jobsets, clusters, header := cfg.Users, cfg.Jobsets, cfg.Clusters, cfg.Header
	clusterprivate, logmeta := cfg.ClusterPrivate, cfg.LogMeta
	if users == nil {
		users = mustMemTable()
	}
	if jobsets == nil {
		jobsets = mustMemTable()
	}
	if clusters == nil {
		clusters = mustMemTable()
	}
	if header == nil {
		header = mustMemTable()
	}
	if clusterprivate == nil {
		clusterprivate = mustMemTable()
	}
	if logmeta == nil {
		logmeta = mustMemTable()
	}
	return &JobSync{
		target:         target,
		users:          users,
		jobsets:        jobsets,
		clusters:       clusters,
		header:         header,
		clusterprivate: clusterprivate,
		logmeta:        logmeta,
		parser:         classadlog.NewParser(cfg.Filename),
		prober:         classadlog.NewProber(),
		interval:       interval,
		saveInterval:   cfg.SaveInterval,
		log:            logger,
		children:       map[string]map[string]struct{}{},
		txs:            map[*db.DB]*db.Txn{},
		store:          cfg.Store,
	}
}

// mustMemTable opens a private in-memory table for a sibling namespace a caller did not supply.
// An in-memory Open does not fail in practice; a failure here means the process cannot function.
func mustMemTable() *db.DB {
	d, err := db.Open("")
	if err != nil {
		panic("scheddsync: opening in-memory table: " + err.Error())
	}
	return d
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
			// Persist the latest position the append-path throttle may have skipped, so a clean
			// shutdown does not force a re-apply on the next start. No-op if a transaction is
			// still open (checkpoint refuses mid-transaction) -- abort then rolls it back.
			s.checkpoint()
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
	// An operator-requested resync rebuilds the mirror from the current log (heals corruption
	// without truncating). Consume the request before probing so it runs exactly once.
	if s.resyncReq.Swap(false) {
		s.log.Info("scheddsync: job resync requested; rebuilding jobs mirror from the current log")
		return s.reconcileReload(ctx)
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
	s.children = map[string]map[string]struct{}{}
	s.parser.SetNextOffset(0)
	s.prober.Reset()
	// Snapshot each table's keys before the reload so the post-reconcile sweep can delete the
	// keys the current log no longer mentions (per namespace).
	beforeJobs := s.target.Keys()
	beforeUsers := s.users.Keys()
	beforeJobsets := s.jobsets.Keys()
	beforeClusters := s.clusters.Keys()
	beforeHeader := s.header.Keys()
	beforeClusterPrivate := s.clusterprivate.Keys()
	seen := map[*db.DB]map[string]struct{}{}

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

	rec := &reconciler{
		jobs: s.target, users: s.users, jobsets: s.jobsets, clusters: s.clusters, header: s.header,
		clusterprivate: s.clusterprivate,
		seen:           seen, log: s.log, batches: map[*db.DB]*db.Txn{},
	}
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
	// Chain proc ads to their cluster ads in a second pass: the cluster ad may be written
	// after its procs in the log, so it is only guaranteed complete once every key is flushed.
	if err = s.chainReconciledProcs(seen[s.target]); err != nil {
		return err
	}
	// Sweep each namespace's table: delete the pre-reload keys the current log no longer contains.
	for _, sw := range []struct {
		table  *db.DB
		before []string
	}{
		{s.target, beforeJobs}, {s.users, beforeUsers},
		{s.jobsets, beforeJobsets}, {s.clusters, beforeClusters},
		{s.header, beforeHeader},
		{s.clusterprivate, beforeClusterPrivate},
	} {
		if err = s.sweepKeys(sw.table, sw.before, seen[sw.table]); err != nil {
			return err
		}
	}
	s.captureLogMeta() // the leading 107 sequence header (offset was reset to 0)
	s.checkpoint()     // position recorded only after the reconciled table matches the log
	s.publishStatus(true)
	return nil
}

// chainReconciledProcs, run after a reconcile has written every key, copies each proc ad's
// parent cluster ad attributes onto the proc row -- for HTCondor versions that keep
// cluster-wide attributes only on the cluster ad. It is a no-op for procs that already carry
// their own attributes. Writes are committed in bounded batches.
func (s *JobSync) chainReconciledProcs(seen map[string]struct{}) error {
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
	for key := range seen {
		parentKey, ok := clusterKeyOf(key)
		if !ok {
			continue
		}
		parent, ok := s.clusters.LookupClassAd(parentKey)
		if !ok {
			continue
		}
		proc, ok := s.target.LookupClassAd(key)
		if !ok || !chainAttrsInto(proc, parent) {
			continue
		}
		if batch == nil {
			batch = s.target.Begin()
		}
		batch.NewClassAd(key, proc)
		if n++; n >= reconcileBatch {
			if err := commit(); err != nil {
				return err
			}
		}
	}
	return commit()
}

// sweepKeys deletes every key present in table before a reconcile reload that the current log no
// longer contains -- e.g. the jobs that completed (and were compacted away) while the log was
// rotated. Deletions are committed in bounded batches so a large sweep never holds one giant
// transaction.
func (s *JobSync) sweepKeys(table *db.DB, before []string, seen map[string]struct{}) error {
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
			batch = table.Begin()
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

// reconciler applies a reconcile reload's log stream. It accumulates the current key's run of
// ops and, when the key changes (or at end of stream), writes it -- comparing against the table
// and writing only a real delta, so an unchanged job produces no write (and no watch event).
//
// A live (appended-to) job_queue.log is NOT contiguous per key: a job's submission block is
// early and its runtime updates (JobStatus 1->2, RemoteHost, lease renewals, ...) are appended
// later, far from the submission block. So a key can appear in several non-adjacent runs. The
// FIRST run establishes the ad (a full replace, clearing any stale pre-reconcile content); a
// LATER run for the same key MERGES its sets/deletes onto the already-written ad (read back
// through the batch transaction, which sees the earlier flush whether committed or still
// buffered) rather than replacing it -- otherwise the reconstructed ad would collapse to only
// the key's last run and every submission attribute would vanish. The op handling mirrors
// JobSync.applyEntry; keep the two in sync.
type reconciler struct {
	jobs, users, jobsets, clusters, header, clusterprivate *db.DB
	// seen is PER TABLE: the keys given a live ad in each table this reconcile. It must not be a
	// single global set -- a key legitimately routed to one table (e.g. an owner "0O.-1" in users)
	// would then protect a stale, misrouted row under the SAME key in another table (a pre-routing
	// "0O.-1" left in jobs) from the sweep, leaving undeletable cruft. Per-table, each table's
	// sweep drops the keys the log did not route to THAT table.
	seen map[*db.DB]map[string]struct{}
	log  *slog.Logger

	batches map[*db.DB]*db.Txn // one buffered transaction per table touched this batch
	n       int

	curKey   string
	curAd    *classad.ClassAd
	curDels  []string // attributes DeleteAttribute'd in the current run (for a merge-flush)
	curTable *db.DB   // the table curKey routes to; nil for a dropped namespace
	destroy  bool     // the current key was destroyed within the log window
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
		r.curKey, r.curAd, r.curDels, r.destroy = e.Key, classad.New(), nil, false
		r.curTable = routeTable(e.Key, r.jobs, r.users, r.jobsets, r.clusters, r.header, r.clusterprivate)
	}
	switch e.OpType {
	case classadlog.OpNewClassAd:
		r.curAd, r.curDels, r.destroy = classad.New(), nil, false
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
		r.curDels = append(r.curDels, e.Name)
	}
	return nil
}

// flush finalizes the current key's run of ops. A dropped namespace (the schedd header,
// cluster-private ads, OCU ads) is discarded without touching any table. The FIRST run of a key
// this reconcile establishes its ad (a full replace, so stale pre-reconcile content and
// attributes the log no longer sets are cleared); a LATER, non-contiguous run MERGES its sets and
// deletes onto the already-written ad so the submission attributes survive. Writes go through the
// per-table batch transaction, and an unchanged first run produces no write (hence no watch event).
func (r *reconciler) flush() error {
	key, ad, dels, table, destroy := r.curKey, r.curAd, r.curDels, r.curTable, r.destroy
	r.curKey, r.curAd, r.curDels, r.curTable, r.destroy = "", nil, nil, nil, false
	if key == "" {
		return nil
	}
	if table == nil {
		return nil
	}
	tableSeen := r.seenFor(table)

	if destroy {
		// Destroyed within the log window: remove it, and drop it from seen so the post-reconcile
		// sweep does not treat it as live. (A key created and destroyed within the window that was
		// never in the table is a no-op.)
		delete(tableSeen, key)
		tx := r.batchTx(table)
		if _, ok := tx.LookupClassAd(key); ok {
			tx.DestroyClassAd(key)
			r.n++
		}
		return r.maybeCommit()
	}

	_, reappeared := tableSeen[key]
	tableSeen[key] = struct{}{}
	tx := r.batchTx(table)

	if !reappeared {
		// First run: establish the ad. Stamp the storage key (as applyEntry does) so the row is
		// addressable by the REPL's key attribute; stamped before the Equal check so an
		// already-stamped stored ad reconciles as unchanged (no spurious write/watch event).
		ad.InsertAttrString(KeyAttr, key)
		if cur, ok := tx.LookupClassAd(key); !ok || !cur.Equal(ad) {
			tx.NewClassAd(key, ad)
			r.n++
		}
		return r.maybeCommit()
	}

	// A later, non-contiguous run: merge this run's sets/deletes onto the ad already written this
	// reconcile (read back through tx, which sees the earlier flush whether committed or buffered).
	base, ok := tx.LookupClassAd(key)
	if !ok {
		// Defensive: the earlier flush found the ad unchanged AND the table had no row -- treat
		// this run as the base.
		ad.InsertAttrString(KeyAttr, key)
		tx.NewClassAd(key, ad)
		r.n++
		return r.maybeCommit()
	}
	for _, name := range ad.GetAttributes() {
		if expr, ok := ad.Lookup(name); ok {
			base.InsertExpr(name, expr)
		}
	}
	for _, name := range dels {
		base.Delete(name)
	}
	tx.NewClassAd(key, base)
	r.n++
	return r.maybeCommit()
}

// seenFor returns (creating if needed) the per-table set of keys given a live ad this reconcile.
func (r *reconciler) seenFor(table *db.DB) map[string]struct{} {
	m := r.seen[table]
	if m == nil {
		m = map[string]struct{}{}
		r.seen[table] = m
	}
	return m
}

// maybeCommit commits the buffered batch once it reaches the size threshold.
func (r *reconciler) maybeCommit() error {
	if r.n >= reconcileBatch {
		return r.commit()
	}
	return nil
}

func (r *reconciler) batchTx(table *db.DB) *db.Txn {
	tx := r.batches[table]
	if tx == nil {
		tx = table.Begin()
		r.batches[table] = tx
	}
	return tx
}

// commit commits every buffered per-table transaction (the tables are independent) and resets the
// batch. It returns the first commit error, if any.
func (r *reconciler) commit() error {
	var firstErr error
	for _, tx := range r.batches {
		if err := tx.Commit(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.batches = map[*db.DB]*db.Txn{}
	r.n = 0
	return firstErr
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
				// One-time migration: a jobs table written before record-type routing holds
				// cluster/jobset/user/header ads too. Resuming does not rewrite them (they are
				// not in the tail we replay), so sweep them now -- jobs holds only proc ads, and
				// each proc already carries its cluster's chained attributes.
				if err := s.migrateJobsTable(); err != nil {
					return err
				}
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

// migrateJobsTable removes any key in the jobs table that does not belong there under the
// current namespace routing -- cluster ads ("0C.-1"), jobset ads, user/owner records, and the
// schedd header ad, all left over from a build that mirrored every job_queue.log record into
// one table. It is a no-op once the table is clean (the common case), so it costs one key scan
// on the resume path. Deletions are committed in bounded batches.
func (s *JobSync) migrateJobsTable() error {
	var stale []string
	for _, k := range s.target.Keys() {
		if !isJobKey(k) {
			stale = append(stale, k)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	s.log.Info("scheddsync: sweeping non-job rows from the jobs table (record-type routing migration)",
		"count", len(stale))
	var batch *db.Txn
	commit := func() error {
		if batch == nil {
			return nil
		}
		err := batch.Commit()
		batch = nil
		return err
	}
	for i, k := range stale {
		if batch == nil {
			batch = s.target.Begin()
		}
		batch.DestroyClassAd(k)
		if (i+1)%reconcileBatch == 0 {
			if err := commit(); err != nil {
				return err
			}
		}
	}
	return commit()
}

// checkpoint durably records the resume position after a clean read pass. It saves only at a
// batch boundary with no explicit transaction open (!s.explicit) -- committing the offset in
// the middle of an unfinished on-disk transaction would resume past its BeginTransaction --
// reconcileReload does not use s.txs; it calls checkpoint itself once its sweep commits.
func (s *JobSync) checkpoint() {
	if s.store == nil || s.explicit {
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
		return
	}
	s.lastSaveAt = time.Now()
}

// maybeCheckpoint records the resume position on the steady append path, throttled to at most
// one save per saveInterval so a busy log does not rewrite the position file on every poll. The
// first save of a run and any save once the interval has elapsed go through; the rest are
// skipped, so a crash re-applies at most one interval of the log (idempotent). checkpoint()
// itself stays unconditional and is used where a save must not be dropped: after a
// rotation/compaction reload and on a clean shutdown.
func (s *JobSync) maybeCheckpoint() {
	if s.saveInterval > 0 && !s.lastSaveAt.IsZero() && time.Since(s.lastSaveAt) < s.saveInterval {
		return
	}
	s.checkpoint()
}

// readAndApply reads entries from the current offset to EOF, applying them. reload marks a
// full replay (offset already rewound). It updates the prober so the next probe is relative
// to what was consumed.
func (s *JobSync) readAndApply(ctx context.Context, reload bool) (err error) {
	if s.parser.GetNextOffset() == 0 {
		// Reading from the start: the log's leading 107 sequence header is in this pass.
		s.captureLogMeta()
	}
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
			s.maybeCheckpoint()
		}
		s.publishStatus(err == nil)
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
	// End of this read pass: commit any implicit (non-explicit-transaction) batches. An
	// explicit transaction with no EndTransaction yet stays open across polls.
	if !s.explicit {
		err = s.commitAll()
	}
	// The resume position is checkpointed exactly once, by the deferred cleanup above -- after
	// parser.Close() finalizes the offset (checkpoint's own precondition) and after this
	// commit. Checkpointing here too wrote jobs.pos twice per poll (and the pre-Close copy
	// carried a stale offset), so it was dropped.
	return err
}

// applyEntry applies one log entry to the table its key routes to, managing the buffered
// per-table DB transactions.
func (s *JobSync) applyEntry(e *classadlog.LogEntry) error {
	switch e.OpType {
	case classadlog.OpBeginTransaction:
		// A commit of any implicit batch precedes an explicit transaction.
		if !s.explicit {
			if err := s.commitAll(); err != nil {
				return err
			}
		}
		s.explicit = true
		return nil
	case classadlog.OpEndTransaction:
		if s.explicit {
			return s.commitAll()
		}
		return nil
	case classadlog.OpLogHistoricalSequenceNumber:
		return nil
	}

	// Route the data op to the table its key namespace belongs to; drop keys we do not mirror
	// (the schedd header, cluster-private ads, OCU ads).
	table := s.tableFor(e.Key)
	if table == nil {
		return nil
	}
	tx := s.ensureTx(table)
	switch e.OpType {
	case classadlog.OpNewClassAd:
		ad := classad.New()
		// Stamp the storage key so the row is addressable by the REPL's default key attribute
		// (UPDATE/DELETE recover a row's key from this). Synced ads otherwise carry no "Key",
		// so `DELETE FROM jobs WHERE ...` could not target them.
		ad.InsertAttrString(KeyAttr, e.Key)
		if e.MyType != "" && e.MyType != "(unknown)" {
			ad.InsertAttrString("MyType", e.MyType)
		}
		if e.TargetType != "" && e.TargetType != "(unknown)" {
			ad.InsertAttrString("TargetType", e.TargetType)
		}
		tx.NewClassAd(e.Key, ad)
		// Chain a proc ad to its parent cluster ad: inherit the cluster's attributes already
		// set (the proc's own, applied by later entries, win) and register it so a subsequent
		// cluster-ad edit fans out to it. Together these cover both on-disk orders -- cluster
		// attributes set before or after the proc's NewClassAd. The cluster ad lives in the
		// clusters table, so chaining reads it from there (durable across a restart).
		if parent, ok := clusterKeyOf(e.Key); ok {
			s.chainFromParent(tx, e.Key, parent)
			s.addChild(parent, e.Key)
		}
	case classadlog.OpDestroyClassAd:
		tx.DestroyClassAd(e.Key)
		if parent, ok := clusterKeyOf(e.Key); ok {
			if kids := s.children[parent]; kids != nil {
				delete(kids, e.Key)
			}
		} else {
			delete(s.children, e.Key) // a destroyed cluster ad drops its child set
		}
	case classadlog.OpSetAttribute:
		if err := tx.SetAttribute(e.Key, e.Name, e.Value); err != nil {
			// A single malformed value must not abort the whole sync; skip it.
			s.log.Warn("job_queue.log: skipping unparseable attribute",
				"key", e.Key, "attr", e.Name, "err", err.Error())
		} else if kids := s.children[e.Key]; len(kids) > 0 {
			// Propagate a cluster-ad attribute to its chained proc rows in the jobs table (only
			// cluster ads ever have children).
			jtx := s.ensureTx(s.target)
			for child := range kids {
				if perr := jtx.SetAttribute(child, e.Name, e.Value); perr != nil {
					s.log.Warn("job_queue.log: skipping unparseable cluster attribute",
						"key", child, "attr", e.Name, "err", perr.Error())
				}
			}
		}
	case classadlog.OpDeleteAttribute:
		tx.DeleteAttribute(e.Key, e.Name)
		if kids := s.children[e.Key]; len(kids) > 0 {
			jtx := s.ensureTx(s.target)
			for child := range kids {
				jtx.DeleteAttribute(child, e.Name)
			}
		}
	}
	return nil
}

// tableFor returns the mirror table a job_queue.log key belongs to, or nil for keys we do not
// mirror (OCU ads "C.-99"). The namespace is encoded in the "cluster.proc" key; see the is*Key
// classifiers.
func (s *JobSync) tableFor(key string) *db.DB {
	return routeTable(key, s.target, s.users, s.jobsets, s.clusters, s.header, s.clusterprivate)
}

// routeTable classifies a job_queue.log key and returns the table it flattens into, or nil for a
// dropped namespace. Shared by the incremental (applyEntry) and reconcile (reconciler) paths.
func routeTable(key string, jobs, users, jobsets, clusters, header, clusterprivate *db.DB) *db.DB {
	switch {
	case isJobKey(key):
		return jobs
	case isClusterKey(key):
		return clusters
	case isJobsetKey(key):
		return jobsets
	case isUserKey(key):
		return users
	case isHeaderKey(key):
		return header
	case isClusterPrivateKey(key):
		return clusterprivate
	default:
		return nil
	}
}

// addChild registers proc key child under its parent cluster ad key.
func (s *JobSync) addChild(parent, child string) {
	kids := s.children[parent]
	if kids == nil {
		kids = map[string]struct{}{}
		s.children[parent] = kids
	}
	kids[child] = struct{}{}
}

// parseJobKey splits a job_queue.log key "cluster.proc" into its two integer components. HTCondor
// may pad the cluster with a leading zero for namespace sorting (e.g. "01.-1"), which Atoi
// tolerates. ok is false for a key without a '.' or with a non-integer part.
func parseJobKey(key string) (cluster, proc int, ok bool) {
	dot := strings.IndexByte(key, '.')
	if dot < 0 {
		return 0, 0, false
	}
	c, err := strconv.Atoi(key[:dot])
	if err != nil {
		return 0, 0, false
	}
	p, err := strconv.Atoi(key[dot+1:])
	if err != nil {
		return 0, 0, false
	}
	return c, p, true
}

// isJobKey reports whether key names a real proc ad (cluster>0, proc>=0) -- the only keys that
// become rows in the jobs table. It is exactly clusterKeyOf's success condition.
func isJobKey(key string) bool {
	_, ok := clusterKeyOf(key)
	return ok
}

// isClusterKey reports whether key names a cluster ad (cluster>0, proc==-1): not a job row, but
// the holder of attributes shared by (and chained into) the cluster's procs.
func isClusterKey(key string) bool {
	c, p, ok := parseJobKey(key)
	return ok && c > 0 && p == -1
}

// isJobsetKey reports whether key names a jobset ad (cluster>0, proc==-100).
func isJobsetKey(key string) bool {
	c, p, ok := parseJobKey(key)
	return ok && c > 0 && p == -100
}

// isClusterPrivateKey reports whether key names a cluster-private ad (cluster>0, proc==-2, the
// schedd's CLUSTERPRIVATE_qkey2): the per-cluster private attributes, created alongside every
// cluster ad. Not a job row and not chained into procs; mirrored so a backup is complete.
func isClusterPrivateKey(key string) bool {
	c, p, ok := parseJobKey(key)
	return ok && c > 0 && p == -2
}

// LogMetaKey is the single logmeta record's storage key. It is deliberately not a valid
// "cluster.proc" ClassAd key so it can never collide with a mirrored ad.
const LogMetaKey = "sequence"

// LogSeqAttr and LogCreationTimeAttr hold the job_queue.log's LogHistoricalSequenceNumber
// (op 107) fields in the logmeta record, so a reconstruction re-emits the same sequence header.
const (
	LogSeqAttr          = "SequenceNumber"
	LogCreationTimeAttr = "CreationTimestamp"
)

// captureLogMeta reads the log's first record and, if it is a LogHistoricalSequenceNumber
// (op 107, "107 <seq> CreationTimestamp <birthdate>"), records the sequence number and creation
// timestamp in the logmeta table so a reconstruction reproduces the same header. The 107 carries
// no ClassAd key, so it is not part of the routed ad stream (nor the reconcile sweep); it is
// rewritten only when the schedd truncates the log -- which triggers a full reload here -- so
// capturing it on every read-from-start keeps it current. A log without a 107 leaves logmeta
// untouched (the writer then emits a fresh sequence).
func (s *JobSync) captureLogMeta() {
	seq, ts, ok := readLogSequence(s.parser.GetFilename())
	if !ok {
		return
	}
	ad := classad.New()
	ad.InsertAttrString(KeyAttr, LogMetaKey)
	ad.InsertAttr(LogSeqAttr, seq)
	ad.InsertAttr(LogCreationTimeAttr, ts)
	if cur, ok := s.logmeta.LookupClassAd(LogMetaKey); ok && cur.Equal(ad) {
		return // unchanged; avoid a needless write/watch event
	}
	tx := s.logmeta.Begin()
	tx.NewClassAd(LogMetaKey, ad)
	if err := tx.Commit(); err != nil {
		s.log.Warn("scheddsync: recording log sequence number failed", "err", err.Error())
	}
}

// readLogSequence opens filename and returns the sequence number and creation timestamp from its
// leading LogHistoricalSequenceNumber record ("107 <seq> CreationTimestamp <birthdate>"). ok is
// false if the file cannot be read or its first real record is not a valid 107.
func readLogSequence(filename string) (seq, ts int64, ok bool) {
	f, err := os.Open(filename)
	if err != nil {
		return 0, 0, false
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue // skip blanks and comments, as the parser does
		}
		fields := strings.Fields(line)
		// "107 <seq> CreationTimestamp <birthdate>": fields[2] is the label, which we ignore.
		if len(fields) >= 4 && fields[0] == "107" {
			s, err1 := strconv.ParseInt(fields[1], 10, 64)
			t, err2 := strconv.ParseInt(fields[3], 10, 64)
			if err1 == nil && err2 == nil {
				return s, t, true
			}
		}
		return 0, 0, false // first real record is not a usable 107
	}
	return 0, 0, false
}

// isUserKey reports whether key names a user/owner/project record (cluster==0, proc>0).
func isUserKey(key string) bool {
	c, p, ok := parseJobKey(key)
	return ok && c == 0 && p > 0
}

// isHeaderKey reports whether key names the single schedd header ad ("0.0", cluster==0,
// proc==0) -- the queue-metadata ad (NextClusterNum, ...). Stored so a job_queue.log
// reconstruction can restore the schedd's counters; not a job row.
func isHeaderKey(key string) bool {
	c, p, ok := parseJobKey(key)
	return ok && c == 0 && p == 0
}

// clusterKeyOf returns the parent cluster ad key for a proc ad key of the form "C.P"
// (ProcId >= 0), following HTCondor's job_queue.log convention where cluster C's ad is
// keyed "0C.-1". It returns ("", false) for anything that is not a real proc ad: cluster
// ads ("0C.-1"), jobset ads ("C.-100"), and the schedd header/user namespace (cluster 0).
func clusterKeyOf(key string) (string, bool) {
	cluster, proc, ok := parseJobKey(key)
	if !ok || cluster <= 0 || proc < 0 {
		return "", false
	}
	// The on-disk cluster ad key keeps the raw cluster substring prefixed with "0" (parsing the
	// int would drop any leading zero), so build it from the substring, not the parsed int.
	dot := strings.IndexByte(key, '.')
	return "0" + key[:dot] + ".-1", true
}

// chainFromParent copies the parent cluster ad's attributes onto the proc ad, so the materialized
// proc row carries its cluster's attributes. The parent lives in the clusters table; reading it
// through that table's open transaction sees both cluster ads written earlier in this pass and
// ones committed by a prior pass (so a proc that materializes into a pre-existing cluster after a
// resume still chains). A no-op when the parent has nothing the proc lacks.
func (s *JobSync) chainFromParent(jobsTx *db.Txn, procKey, parentKey string) {
	parent, ok := s.ensureTx(s.clusters).LookupClassAd(parentKey)
	if !ok {
		return
	}
	proc, ok := jobsTx.LookupClassAd(procKey)
	if !ok {
		return
	}
	if chainAttrsInto(proc, parent) {
		jobsTx.NewClassAd(procKey, proc)
	}
}

// chainAttrsInto copies every attribute of parent that dst does not already define into dst,
// reporting whether anything was added. dst's own attributes win (HTCondor chaining, where a
// proc ad overrides its cluster ad).
func chainAttrsInto(dst, parent *classad.ClassAd) bool {
	changed := false
	for _, name := range parent.GetAttributes() {
		if _, has := dst.Lookup(name); has {
			continue
		}
		if e, ok := parent.Lookup(name); ok {
			dst.InsertExpr(name, e)
			changed = true
		}
	}
	return changed
}

// ensureTx returns the open transaction for table, starting one if none is open for it. When no
// explicit schedd transaction is in progress the txn batches ops written outside a transaction
// (committed at the end of the read pass); inside an explicit transaction it commits at
// EndTransaction.
func (s *JobSync) ensureTx(table *db.DB) *db.Txn {
	tx := s.txs[table]
	if tx == nil {
		tx = table.Begin()
		s.txs[table] = tx
	}
	return tx
}

// commitAll commits every open per-table transaction (the tables are independent, so order does
// not matter) and clears the set. It returns the first commit error, if any.
func (s *JobSync) commitAll() error {
	var firstErr error
	for _, tx := range s.txs {
		if err := tx.Commit(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.txs = map[*db.DB]*db.Txn{}
	s.explicit = false
	return firstErr
}

func (s *JobSync) abort() {
	for _, tx := range s.txs {
		tx.Abort()
	}
	s.txs = map[*db.DB]*db.Txn{}
	s.explicit = false
}
