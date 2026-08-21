package main

// mirrorout_manager.go runs the follower end of schedd mirroring: it regenerates a
// job_queue.log from the mirrored tables (scheddsync.MirrorOut) on a cadence, so a follower
// htcondordb that replicates a leader's schedd-sync tables re-emits a live, restorable backup
// log. It mirrors the scheddSyncManager lifecycle: (re)started on condor_reconfig so a
// HTCONDORDB_MIRROR_JOB_QUEUE_LOG change takes effect without a daemon restart.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/scheddsync"
	"github.com/bbockelm/htcondordb/server"
)

// mirrorOutSettings is the resolved, comparable configuration of the mirror-out.
type mirrorOutSettings struct {
	enabled  bool
	outPath  string
	interval time.Duration
}

// resolveMirrorOutSettings reads the mirror-out knobs. The feature is off unless
// HTCONDORDB_MIRROR_JOB_QUEUE_LOG names a destination path.
func resolveMirrorOutSettings(cfg *config.Config) mirrorOutSettings {
	out := getStr(cfg, "HTCONDORDB_MIRROR_JOB_QUEUE_LOG")
	interval := scheddsync.DefaultMirrorInterval
	if s := configInt(cfg, "HTCONDORDB_MIRROR_JOB_QUEUE_INTERVAL_SECONDS"); s > 0 {
		interval = time.Duration(s) * time.Second
	}
	return mirrorOutSettings{
		enabled:  out != "",
		outPath:  out,
		interval: interval,
	}
}

// mirrorOutManager owns the running MirrorOut, restarting it on config changes.
type mirrorOutManager struct {
	parent context.Context
	svc    *server.Service
	logger *slog.Logger

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	current mirrorOutSettings
	source  dbad.StatusSource // the running mirror, for the collector ad; nil when stopped
}

// Sources returns the live mirror-out status source (empty when stopped), for the collector ad.
func (m *mirrorOutManager) Sources() []dbad.StatusSource {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.source == nil {
		return nil
	}
	return []dbad.StatusSource{m.source}
}

// apply reconciles the running mirror with cfg: a no-op when the resolved settings are
// unchanged, otherwise it stops the current mirror and (if still enabled) starts a fresh one.
func (m *mirrorOutManager) apply(cfg *config.Config) error {
	next := resolveMirrorOutSettings(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()
	if next == m.current {
		return nil
	}

	// Stop the running mirror (if any) and wait for it to exit before starting a new one.
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel = nil
		m.done = nil
	}
	m.source = nil
	m.current = mirrorOutSettings{}

	if !next.enabled {
		return nil
	}

	// Open the seven namespace tables the mirror reads. On a follower these already exist
	// (replicated); CreateTable is idempotent and returns the existing table.
	jobs, err := m.svc.Catalog().CreateTable("jobs")
	if err != nil {
		return err
	}
	users, err := m.svc.Catalog().CreateTable("users")
	if err != nil {
		return err
	}
	jobsets, err := m.svc.Catalog().CreateTable("jobsets")
	if err != nil {
		return err
	}
	clusters, err := m.svc.Catalog().CreateTable("clusters")
	if err != nil {
		return err
	}
	header, err := m.svc.Catalog().CreateTable("header")
	if err != nil {
		return err
	}
	clusterprivate, err := m.svc.Catalog().CreateTable("clusterprivate")
	if err != nil {
		return err
	}
	logmeta, err := m.svc.Catalog().CreateTable("logmeta")
	if err != nil {
		return err
	}

	mirror := scheddsync.NewMirrorOut(scheddsync.MirrorOutConfig{
		OutPath: next.outPath, Interval: next.interval, Logger: m.logger,
		Jobs: jobs, Users: users, Jobsets: jobsets, Clusters: clusters,
		Header: header, ClusterPrivate: clusterprivate, LogMeta: logmeta,
	})

	ctx, cancel := context.WithCancel(m.parent)
	done := make(chan struct{})
	go func() { defer close(done); _ = mirror.Run(ctx) }()

	m.cancel = cancel
	m.done = done
	m.source = mirror
	m.current = next
	m.logger.Info("mirror-out: regenerating job_queue.log", "out", next.outPath,
		"interval", next.interval.String())
	return nil
}
