package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

	mu      sync.Mutex
	cancel  context.CancelFunc  // cancels the running tailers; nil when stopped
	done    chan struct{}       // closed once the running tailers have exited
	current scheddSyncSettings  // settings the running tailers were started with
	sources []dbad.StatusSource // live sources for the collector ad
}

// scheddSyncSettings is the resolved, comparable configuration of the tailers.
type scheddSyncSettings struct {
	enabled  bool
	jobLog   string
	histFile string
	posDir   string
}

func resolveScheddSyncSettings(cfg *config.Config) scheddSyncSettings {
	if !configBool(cfg, "HTCONDORDB_SYNC_SCHEDD") {
		return scheddSyncSettings{}
	}
	return scheddSyncSettings{
		enabled:  true,
		jobLog:   firstNonEmpty(getStr(cfg, "HTCONDORDB_JOB_QUEUE_LOG"), getStr(cfg, "JOB_QUEUE_LOG")),
		histFile: firstNonEmpty(getStr(cfg, "HTCONDORDB_HISTORY"), getStr(cfg, "HISTORY")),
		posDir:   getStr(cfg, "HTCONDORDB_DIR"),
	}
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
		if next.jobLog == "" && next.histFile == "" {
			return fmt.Errorf("HTCONDORDB_SYNC_SCHEDD is set but neither JOB_QUEUE_LOG nor HISTORY is configured")
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
	m.current = scheddSyncSettings{}

	if !next.enabled {
		return nil
	}

	ctx, cancel := context.WithCancel(m.parent)
	sources, done, err := m.launch(ctx, next)
	if err != nil {
		cancel()
		return err
	}
	m.cancel = cancel
	m.done = done
	m.sources = sources
	m.current = next
	return nil
}

// launch starts the tailers for settings s under ctx and returns their live
// status sources plus a channel closed when all of them have exited.
func (m *scheddSyncManager) launch(ctx context.Context, s scheddSyncSettings) ([]dbad.StatusSource, chan struct{}, error) {
	syncStore := func(name string) scheddsync.PositionStore {
		if s.posDir == "" {
			return nil
		}
		return &scheddsync.FileStore{Path: filepath.Join(s.posDir, "scheddsync", name)}
	}
	var sources []dbad.StatusSource
	var wg sync.WaitGroup

	if s.jobLog != "" {
		// job_queue.log flattens into four tables by key namespace: proc ads -> jobs, cluster ads
		// -> clusters (their own durable table so late procs still chain), jobset ads -> jobsets,
		// user/owner records -> users. CreateTable is idempotent (returns the existing table).
		jobs, err := m.svc.Catalog().CreateTable("jobs")
		if err != nil {
			return nil, nil, fmt.Errorf("schedd-sync: creating jobs table: %w", err)
		}
		users, err := m.svc.Catalog().CreateTable("users")
		if err != nil {
			return nil, nil, fmt.Errorf("schedd-sync: creating users table: %w", err)
		}
		jobsets, err := m.svc.Catalog().CreateTable("jobsets")
		if err != nil {
			return nil, nil, fmt.Errorf("schedd-sync: creating jobsets table: %w", err)
		}
		clusters, err := m.svc.Catalog().CreateTable("clusters")
		if err != nil {
			return nil, nil, fmt.Errorf("schedd-sync: creating clusters table: %w", err)
		}
		js := scheddsync.NewJobSync(jobs, scheddsync.JobSyncConfig{
			Filename: s.jobLog, Logger: m.logger, Store: syncStore("jobs.pos"),
			Users: users, Jobsets: jobsets, Clusters: clusters,
		})
		wg.Add(1)
		go func() { defer wg.Done(); _ = js.Run(ctx) }()
		sources = append(sources, js)
		m.logger.Info("schedd-sync: mirroring job_queue.log", "file", s.jobLog,
			"tables", "jobs,users,jobsets,clusters")
	}
	if s.histFile != "" {
		hist, err := m.svc.Catalog().CreateArchiveTable("history", db.ArchiveConfig{
			ValueAttrs: []string{"ClusterId"},
			ZoneAttrs:  []string{"CompletionDate"},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("schedd-sync: creating history archive: %w", err)
		}
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
		m.logger.Info("schedd-sync: tailing history file", "file", s.histFile, "archive", "history")
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	return sources, done, nil
}
