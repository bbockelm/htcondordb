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
	"strconv"
	"strings"
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

	// children maps a cluster ad key ("0C.-1") to the set of its proc ad keys ("C.P").
	// HTCondor stores cluster-wide attributes (ClusterId, Owner, Cmd, ...) only on the
	// cluster ad and chains proc ads to it; a flat mirror would lose them. This index lets
	// a cluster-ad attribute change fan out to the chained proc rows so each materialized
	// proc carries its cluster's attributes -- condor_q's chained view. Rebuilt from
	// scratch on every full replay.
	children map[string]map[string]struct{}
}

// JobSyncConfig configures a JobSync.
type JobSyncConfig struct {
	Filename     string        // path to job_queue.log (required)
	PollInterval time.Duration // default 200ms
	Logger       *slog.Logger  // default slog.Default()
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
		children: map[string]map[string]struct{}{},
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
	result, err := s.prober.Probe(s.parser.GetFilename(), s.parser.GetNextOffset())
	if err != nil {
		return err
	}
	switch result {
	case classadlog.ProbeNoChange:
		return nil
	case classadlog.ProbeCompressed:
		return s.fullReload(ctx)
	case classadlog.ProbeAddition:
		return s.readAndApply(ctx, false)
	default:
		// ProbeError / ProbeFatalError / unknown.
		return errors.New("scheddsync: probe error on " + s.parser.GetFilename())
	}
}

// fullReload handles a rotated/rewritten log: abort any open transaction, truncate the
// table, rewind to the start, and replay the whole current log.
func (s *JobSync) fullReload(ctx context.Context) error {
	s.abort()
	s.target.Truncate()
	s.children = map[string]map[string]struct{}{}
	s.parser.SetNextOffset(0)
	s.prober.Reset()
	return s.readAndApply(ctx, true)
}

// readAndApply reads entries from the current offset to EOF, applying them. reload marks a
// full replay (offset already rewound). It updates the prober so the next probe is relative
// to what was consumed.
func (s *JobSync) readAndApply(ctx context.Context, reload bool) (err error) {
	if oerr := s.parser.Open(); oerr != nil {
		return oerr
	}
	defer func() {
		_ = s.parser.Close()
		if uerr := s.prober.Update(s.parser.GetFilename()); uerr != nil && err == nil {
			err = uerr
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
		// Chain a proc ad to its parent cluster ad: seed it with the cluster's current
		// attributes (the proc's own, set by later entries, override) and register it so
		// subsequent cluster-ad edits fan out to it.
		if parent, ok := clusterKeyOf(e.Key); ok {
			s.chainFromParent(tx, e.Key, parent)
			kids := s.children[parent]
			if kids == nil {
				kids = map[string]struct{}{}
				s.children[parent] = kids
			}
			kids[e.Key] = struct{}{}
		}
	case classadlog.OpDestroyClassAd:
		tx.DestroyClassAd(e.Key)
		if parent, ok := clusterKeyOf(e.Key); ok {
			if kids := s.children[parent]; kids != nil {
				delete(kids, e.Key)
			}
		} else if isClusterKey(e.Key) {
			delete(s.children, e.Key)
		}
	case classadlog.OpSetAttribute:
		if err := tx.SetAttribute(e.Key, e.Name, e.Value); err != nil {
			// A single malformed value must not abort the whole sync; skip it.
			s.log.Warn("job_queue.log: skipping unparseable attribute",
				"key", e.Key, "attr", e.Name, "err", err.Error())
		} else if isClusterKey(e.Key) {
			// Propagate the cluster-ad attribute to every chained proc ad so the
			// materialized proc rows stay in sync with a cluster-wide edit.
			for child := range s.children[e.Key] {
				if perr := tx.SetAttribute(child, e.Name, e.Value); perr != nil {
					s.log.Warn("job_queue.log: skipping unparseable cluster attribute",
						"key", child, "attr", e.Name, "err", perr.Error())
				}
			}
		}
	case classadlog.OpDeleteAttribute:
		tx.DeleteAttribute(e.Key, e.Name)
		if isClusterKey(e.Key) {
			for child := range s.children[e.Key] {
				tx.DeleteAttribute(child, e.Name)
			}
		}
	}
	return nil
}

// clusterKeyOf returns the parent cluster ad key for a proc ad key of the form "C.P"
// (ProcId >= 0), following HTCondor's job_queue.log convention where cluster C's ad is
// keyed "0C.-1". It returns ("", false) for anything that is not a real proc ad: cluster
// ads ("0C.-1"), jobset ads ("C.-2"), and the schedd header namespace (cluster 0).
func clusterKeyOf(key string) (string, bool) {
	dot := strings.IndexByte(key, '.')
	if dot <= 0 {
		return "", false
	}
	clusterStr, procStr := key[:dot], key[dot+1:]
	if proc, err := strconv.Atoi(procStr); err != nil || proc < 0 {
		return "", false
	}
	if cluster, err := strconv.Atoi(clusterStr); err != nil || cluster <= 0 {
		return "", false
	}
	return "0" + clusterStr + ".-1", true
}

// isClusterKey reports whether key is a cluster ad key ("0C.-1").
func isClusterKey(key string) bool {
	return strings.HasPrefix(key, "0") && strings.HasSuffix(key, ".-1")
}

// chainFromParent copies the parent cluster ad's attributes into the proc ad, skipping any
// the proc already defines (its own attributes win), so the materialized proc row carries
// its cluster's attributes -- HTCondor keeps them only on the chained cluster ad.
func (s *JobSync) chainFromParent(tx *db.Txn, procKey, parentKey string) {
	parent, ok := tx.LookupClassAd(parentKey)
	if !ok {
		return
	}
	proc, ok := tx.LookupClassAd(procKey)
	if !ok {
		return
	}
	changed := false
	for _, name := range parent.GetAttributes() {
		if _, has := proc.Lookup(name); has {
			continue
		}
		if e, ok := parent.Lookup(name); ok {
			proc.InsertExpr(name, e)
			changed = true
		}
	}
	if changed {
		tx.NewClassAd(procKey, proc)
	}
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
