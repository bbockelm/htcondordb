package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/scheddsync"
	"github.com/bbockelm/htcondordb/server"
)

// scheddSyncManager owns the schedd-sync tailers so their configuration
// (HTCONDORDB_SYNC_SCHEDD, JOB_QUEUE_LOG/HTCONDORDB_JOB_QUEUE_LOG,
// HISTORY/HTCONDORDB_HISTORY) can be applied on condor_reconfig without a daemon
// restart -- mirroring how the authorization policy already reloads. The tailers
// read their paths once when they start, so a path change means stopping the old
// tailers and starting new ones.
type scheddSyncManager struct {
	parent context.Context
	svc    *server.Service
	logger *slog.Logger

	mu        sync.Mutex
	cancel    context.CancelFunc  // cancels the running tailers; nil when stopped
	done      chan struct{}       // closed once the running tailers have exited
	current   scheddSyncSettings  // settings the running tailers were started with
	sources   []dbad.StatusSource // live sources for the collector ad
	resyncers map[string]resyncer // "jobs"/"history" -> the running tailer, for on-demand resync
}

// resyncer is a running sync a caller can ask to re-read its source from scratch.
type resyncer interface{ Resync() }

// Resync asks the named tailer ("jobs" or "history") to re-read its source non-destructively on
// its next poll. It errors if schedd-sync is disabled or the target is unknown.
func (m *scheddSyncManager) Resync(target string) error {
	m.mu.Lock()
	r, ok := m.resyncers[target]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no such schedd-sync target %q (want jobs, history, or epoch, and it must be enabled)", target)
	}
	r.Resync()
	return nil
}

// ResyncTargets returns the currently-resyncable schedd-sync targets (for validation/help).
func (m *scheddSyncManager) ResyncTargets() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.resyncers))
	for name := range m.resyncers {
		out = append(out, name)
	}
	return out
}

// defaultArchiveCategoricalAttrs are the history archive's categorical (string equality)
// indexes when HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS is unset. Owner is the one grouping
// dimension every pool has. Sites that group by AccountingGroup, ProjectName, or a custom
// attribute should name them too: an unindexed GROUP BY is a full scan of every segment the
// zone maps cannot prune, and the cost of adding an index later grows with the archive
// (the backfill decompresses every record once).
const defaultArchiveCategoricalAttrs = "Owner"

// scheddSyncSettings is the resolved, comparable configuration of the tailers. Every field
// must stay comparable -- apply() reconciles by struct equality -- so attribute lists are
// carried as their canonical config strings and split at the point of use.
type scheddSyncSettings struct {
	enabled   bool
	jobLog    string
	histFile  string
	epochFile string
	posDir    string

	// History-archive tuning. archiveSegSize applies only when the archive is first
	// created (archiveconfig.json is authoritative on reopen); the index attributes are
	// reconciled onto an existing archive at startup. See launch.
	archiveSegSize  int
	archiveCatAttrs string
	archiveValAttrs string
}

func resolveScheddSyncSettings(cfg *config.Config) scheddSyncSettings {
	if !configBool(cfg, "HTCONDORDB_SYNC_SCHEDD") {
		return scheddSyncSettings{}
	}
	// Unset leaves SegmentSize zero, i.e. the library default (8 MiB). Deliberately not
	// overridden: a small sealed segment is what keeps the tail of the archive queryable,
	// since the active segment carries no sidecar index and is scanned linearly until it
	// seals -- and recent history is the hot query path. The cure for segment count at
	// scale is merging cold segments in the background, not sealing large ones up front,
	// which would trade the hot path for the cold one.
	//
	// Raising it is still useful for a deployment that knows its ingest rate. The
	// structural ceiling is 4 GiB (segment offsets are uint32 throughout the record and
	// sidecar formats); going near it is untested.
	segSize := configInt(cfg, "HTCONDORDB_ARCHIVE_SEGMENT_SIZE")
	if segSize < 0 {
		segSize = 0
	}
	catAttrs := getStr(cfg, "HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS")
	if strings.TrimSpace(catAttrs) == "" {
		catAttrs = defaultArchiveCategoricalAttrs
	}
	return scheddSyncSettings{
		enabled:   true,
		jobLog:    firstNonEmpty(getStr(cfg, "HTCONDORDB_JOB_QUEUE_LOG"), getStr(cfg, "JOB_QUEUE_LOG")),
		histFile:  firstNonEmpty(getStr(cfg, "HTCONDORDB_HISTORY"), getStr(cfg, "HISTORY")),
		epochFile: firstNonEmpty(getStr(cfg, "HTCONDORDB_JOB_EPOCH_HISTORY"), getStr(cfg, "JOB_EPOCH_HISTORY")),
		// The position store lives under the database dir, resolved the same way the DB is
		// (HTCONDORDB_DIR or $(SPOOL)/htcondordb) -- not HTCONDORDB_DIR alone, which left
		// SPOOL-configured deployments with no persisted resume position.
		posDir: resolveDBDir(cfg),

		archiveSegSize:  segSize,
		archiveCatAttrs: canonicalAttrList(catAttrs),
		archiveValAttrs: canonicalAttrList(firstNonEmpty(getStr(cfg, "HTCONDORDB_ARCHIVE_VALUE_ATTRS"), "ClusterId")),
	}
}

// canonicalAttrList normalizes a comma- and/or space-separated attribute list to a stable
// comma-joined form, so two spellings of the same list compare equal in scheddSyncSettings
// and a reconfig that only reformats the list does not restart the tailers.
func canonicalAttrList(s string) string {
	return strings.Join(splitAttrList(s), ",")
}

// reconcileArchiveIndexes brings an existing history archive's index set up to the
// configured one, in the background. It only ever ADDS: an attribute that was indexed but
// is no longer configured is left alone, so a index added by hand (via the repl) is not
// silently discarded by a config that predates it.
//
// The rebuild is not interruptible once started -- AddIndex reindexes synchronously -- so
// ctx is only consulted before beginning. That is acceptable because the work is
// idempotent: an interrupted rebuild leaves segments stale, StaleIndexSegments reports
// them, and the next startup (or a Reindex) retries just those.
//
// REQUIRES a classad release in which ArchiveTable.AddIndex folds its result back into
// archiveconfig.json. Without that, the reopen path restores the creation-time index set,
// this reconciliation finds the attribute missing again on every restart, and the backfill
// re-runs each time -- growing from minutes to hours as the archive matures.
func (m *scheddSyncManager) reconcileArchiveIndexes(ctx context.Context, hist *db.ArchiveTable, s scheddSyncSettings) {
	haveCat, haveVal := hist.IndexedAttrs()
	missing := func(want []string, have []string) []string {
		var out []string
		for _, w := range want {
			if !slices.ContainsFunc(have, func(h string) bool { return strings.EqualFold(h, w) }) {
				out = append(out, w)
			}
		}
		return out
	}
	addCat := missing(splitAttrList(s.archiveCatAttrs), haveCat)
	addVal := missing(splitAttrList(s.archiveValAttrs), haveVal)
	if len(addCat) == 0 && len(addVal) == 0 {
		return
	}
	if ctx.Err() != nil {
		return
	}
	go func() {
		stale, sealed := hist.StaleIndexSegments()
		m.logger.Info("schedd-sync: backfilling history archive indexes",
			"categorical", addCat, "value", addVal, "segments", sealed, "stale_before", stale)
		if !hist.AddIndex(addCat, addVal) {
			return
		}
		stale, sealed = hist.StaleIndexSegments()
		m.logger.Info("schedd-sync: history archive index backfill finished",
			"categorical", addCat, "value", addVal, "segments", sealed, "stale_after", stale)
	}()
}

// splitAttrList splits a comma- and/or space-separated attribute list, dropping empties.
func splitAttrList(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// Sources returns the tailers currently running, for the collector ad's live
// health snapshot. Safe for concurrent use with apply.
func (m *scheddSyncManager) Sources() []dbad.StatusSource {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sources
}

// apply reconciles the running tailers with cfg: a no-op when the resolved
// settings are unchanged, otherwise it stops the current tailers and (if still
// enabled) starts fresh ones. Called once at startup and again on each reconfig.
func (m *scheddSyncManager) apply(cfg *config.Config) error {
	next := resolveScheddSyncSettings(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()
	if next == m.current {
		return nil // nothing changed
	}
	if next.enabled {
		// Never read a schedd's job_queue.log/history as root (symlink risk).
		if err := scheddSyncGuardEUID(os.Geteuid()); err != nil {
			return err
		}
		if next.jobLog == "" && next.histFile == "" && next.epochFile == "" {
			return fmt.Errorf("HTCONDORDB_SYNC_SCHEDD is set but none of JOB_QUEUE_LOG, HISTORY, or JOB_EPOCH_HISTORY is configured")
		}
	}

	// Stop the currently-running tailers (if any) and wait for them to exit before
	// starting new ones, so two tailers never write the same table concurrently.
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel = nil
		m.done = nil
	}
	m.sources = nil
	m.resyncers = nil
	m.current = scheddSyncSettings{}

	if !next.enabled {
		return nil
	}

	ctx, cancel := context.WithCancel(m.parent)
	sources, resyncers, done, err := m.launch(ctx, next)
	if err != nil {
		cancel()
		return err
	}
	m.cancel = cancel
	m.done = done
	m.sources = sources
	m.resyncers = resyncers
	m.current = next
	return nil
}

// launch starts the tailers for settings s under ctx and returns their live
// status sources plus a channel closed when all of them have exited.
func (m *scheddSyncManager) launch(ctx context.Context, s scheddSyncSettings) ([]dbad.StatusSource, map[string]resyncer, chan struct{}, error) {
	syncStore := func(name string) scheddsync.PositionStore {
		if s.posDir == "" {
			return nil
		}
		return &scheddsync.FileStore{Path: filepath.Join(s.posDir, "scheddsync", name)}
	}
	var sources []dbad.StatusSource
	resyncers := map[string]resyncer{}
	var wg sync.WaitGroup

	if s.jobLog != "" {
		// job_queue.log flattens into four tables by key namespace: proc ads -> jobs, cluster ads
		// -> clusters (their own durable table so late procs still chain), jobset ads -> jobsets,
		// user/owner records -> users. CreateTable is idempotent (returns the existing table).
		jobs, err := m.svc.Catalog().CreateTable("jobs")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("schedd-sync: creating jobs table: %w", err)
		}
		users, err := m.svc.Catalog().CreateTable("users")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("schedd-sync: creating users table: %w", err)
		}
		jobsets, err := m.svc.Catalog().CreateTable("jobsets")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("schedd-sync: creating jobsets table: %w", err)
		}
		clusters, err := m.svc.Catalog().CreateTable("clusters")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("schedd-sync: creating clusters table: %w", err)
		}
		js := scheddsync.NewJobSync(jobs, scheddsync.JobSyncConfig{
			Filename: s.jobLog, Logger: m.logger, Store: syncStore("jobs.pos"),
			Users: users, Jobsets: jobsets, Clusters: clusters,
		})
		wg.Add(1)
		go func() { defer wg.Done(); _ = js.Run(ctx) }()
		sources = append(sources, js)
		resyncers["jobs"] = js
		m.logger.Info("schedd-sync: mirroring job_queue.log", "file", s.jobLog,
			"tables", "jobs,users,jobsets,clusters")
	}
	if s.histFile != "" {
		hist, err := m.svc.Catalog().CreateArchiveTable("history", db.ArchiveConfig{
			SegmentSize:      s.archiveSegSize,
			CategoricalAttrs: splitAttrList(s.archiveCatAttrs),
			ValueAttrs:       splitAttrList(s.archiveValAttrs),
			// Zone-map both the job's completion time and htcondordb's ingest time
			// (EnteredHistoryTime, stamped per record), so range queries on either prune whole
			// segments instead of scanning -- e.g. "jobs that entered history in the last 24h".
			ZoneAttrs: []string{"CompletionDate", scheddsync.EnteredHistoryAttr},
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("schedd-sync: creating history archive: %w", err)
		}
		// The config above only takes effect when the archive is created: archiveconfig.json
		// is authoritative on reopen, so an existing archive keeps the index set it was made
		// with. Reconcile the configured attributes onto it explicitly, in the background --
		// the backfill decompresses every record once, which is minutes on a young archive
		// and hours on a mature one, and must not hold up daemon startup. AddIndex is correct
		// immediately (segments full-scan for the new attribute until their sidecars are
		// rebuilt), so serving during the backfill is safe.
		m.reconcileArchiveIndexes(ctx, hist, s)
		hs := scheddsync.NewHistorySync(hist, scheddsync.HistorySyncConfig{
			Filename: s.histFile,
			Logger:   m.logger,
			Store:    syncStore("history.pos"),
			OnResync: func(ev scheddsync.ResyncEvent) {
				m.logger.Error("schedd-sync: history durability gap; completed jobs lost to rotation",
					"reason", ev.Reason, "oldest_available_completion", ev.OldestAvailableCompletion)
			},
		})
		wg.Add(1)
		go func() { defer wg.Done(); _ = hs.Run(ctx) }()
		sources = append(sources, hs)
		resyncers["history"] = hs
		m.logger.Info("schedd-sync: tailing history file", "file", s.histFile, "archive", "history")
	}
	if s.epochFile != "" {
		ep, err := m.svc.Catalog().CreateArchiveTable("epoch_history", db.ArchiveConfig{
			SegmentSize:      s.archiveSegSize,
			CategoricalAttrs: splitAttrList(s.archiveCatAttrs),
			ValueAttrs:       splitAttrList(s.archiveValAttrs),
			// Zone-map the epoch write time and htcondordb's ingest time so range
			// queries on either prune whole segments. Epoch records carry no
			// CompletionDate (a run instance is not a completed job).
			ZoneAttrs: []string{scheddsync.EpochWriteDateAttr, scheddsync.EnteredHistoryAttr},
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("schedd-sync: creating epoch archive: %w", err)
		}
		m.reconcileArchiveIndexes(ctx, ep, s)
		es := scheddsync.NewJobEpochSync(ep, scheddsync.HistorySyncConfig{
			Filename: s.epochFile,
			Logger:   m.logger,
			Store:    syncStore("epoch.pos"),
			OnResync: func(ev scheddsync.ResyncEvent) {
				m.logger.Error("schedd-sync: epoch durability gap; run-instance records lost to rotation",
					"reason", ev.Reason)
			},
		})
		wg.Add(1)
		go func() { defer wg.Done(); _ = es.Run(ctx) }()
		sources = append(sources, es)
		resyncers["epoch"] = es
		m.logger.Info("schedd-sync: tailing epoch history file", "file", s.epochFile, "archive", "epoch_history")
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	return sources, resyncers, done, nil
}
