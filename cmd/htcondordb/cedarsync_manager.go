package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/cedarsync"
	"github.com/bbockelm/htcondordb/command"
)

// cedarSyncManager owns the native-CEDAR fan-in replicators (cedarsync.Runner): each mirrors a
// source htcondordb's table/archive into a local target, selectively and stamped with a source
// label. Configuration is reapplied on condor_reconfig without a restart, mirroring the schedd-sync
// and exporter managers.
//
// Config: HTCONDORDB_REPLICATE_SOURCES lists source names; for each NAME, the per-source knobs are
// HTCONDORDB_REPLICATE_<NAME>_{ADDRESS,TABLE,TARGET,CONSTRAINT,SRC,TYPE}. ADDRESS (the leader's
// command address) is required; TABLE defaults to "history"; TARGET defaults to TABLE; SRC defaults
// to NAME; TYPE is archive (default) or table.
type cedarSyncManager struct {
	parent context.Context
	cat    *db.Catalog
	logger *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc // cancels the running runners; nil when stopped
	done   chan struct{}      // closed once the running runners have exited
	sig    string             // signature of the running config (change detection)
}

// replicateSource is one resolved source->target replication.
type replicateSource struct {
	Name, Address, Source, Target, Constraint, Src, Type string
}

type cedarSyncSettings struct {
	enabled bool
	posDir  string
	sources []replicateSource
	sig     string
}

func resolveCedarSyncSettings(cfg *config.Config) cedarSyncSettings {
	names := strings.Fields(getStr(cfg, "HTCONDORDB_REPLICATE_SOURCES"))
	if len(names) == 0 {
		return cedarSyncSettings{}
	}
	var srcs []replicateSource
	var sb strings.Builder
	for _, n := range names {
		p := "HTCONDORDB_REPLICATE_" + strings.ToUpper(n) + "_"
		s := replicateSource{
			Name:       n,
			Address:    getStr(cfg, p+"ADDRESS"),
			Source:     firstNonEmpty(getStr(cfg, p+"TABLE"), "history"),
			Target:     getStr(cfg, p+"TARGET"),
			Constraint: getStr(cfg, p+"CONSTRAINT"),
			Src:        firstNonEmpty(getStr(cfg, p+"SRC"), n),
			Type:       firstNonEmpty(strings.ToLower(getStr(cfg, p+"TYPE")), "archive"),
		}
		if s.Target == "" {
			s.Target = s.Source
		}
		srcs = append(srcs, s)
		fmt.Fprintf(&sb, "%s|%s|%s|%s|%s|%s|%s\n", s.Name, s.Address, s.Source, s.Target, s.Constraint, s.Src, s.Type)
	}
	return cedarSyncSettings{enabled: true, posDir: resolveDBDir(cfg), sources: srcs, sig: sb.String()}
}

// apply reconciles the running replicators with cfg: a no-op when unchanged, otherwise it stops the
// running runners and (if still enabled) starts fresh ones.
func (m *cedarSyncManager) apply(cfg *config.Config) error {
	next := resolveCedarSyncSettings(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()
	if next.sig == m.sig {
		return nil
	}
	if next.enabled {
		for _, s := range next.sources {
			if s.Address == "" {
				return fmt.Errorf("HTCONDORDB_REPLICATE_%s_ADDRESS is required", strings.ToUpper(s.Name))
			}
			if s.Type != "archive" && s.Type != "table" {
				return fmt.Errorf("HTCONDORDB_REPLICATE_%s_TYPE=%q (want archive or table)", strings.ToUpper(s.Name), s.Type)
			}
		}
	}

	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel, m.done = nil, nil
	}
	m.sig = ""
	if !next.enabled {
		m.logger.Info("cedar-sync: disabled")
		return nil
	}

	ctx, cancel := context.WithCancel(m.parent)
	done := make(chan struct{})
	go m.run(ctx, cfg, next, done)
	m.cancel, m.done, m.sig = cancel, done, next.sig
	m.logger.Info("cedar-sync: enabled", "sources", len(next.sources))
	return nil
}

// Stop cancels the running replicators and WAITS for them to exit -- like
// scheddSyncManager.Stop, it must return before the catalog is closed, since a
// cedarsync replicator writes local target tables in process and Catalog.Close
// munmaps their segments. Idempotent and safe to call when already stopped.
func (m *cedarSyncManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel, m.done = nil, nil
	}
	m.sig = ""
}

// run starts one cedarsync.Runner per source and waits for them all to exit (on ctx cancel).
func (m *cedarSyncManager) run(ctx context.Context, cfg *config.Config, s cedarSyncSettings, done chan struct{}) {
	defer close(done)
	var wg sync.WaitGroup
	for _, src := range s.sources {
		sink, err := m.sinkFor(src, s.posDir)
		if err != nil {
			m.logger.Error("cedar-sync: cannot build target; skipping source", "name", src.Name, "err", err.Error())
			continue
		}
		runner, err := cedarsync.NewRunner(m.dialFor(cfg, src.Address), cedarsync.Config{
			Source: src.Source, Src: src.Src, Constraint: src.Constraint,
		}, sink, m.logger)
		if err != nil {
			m.logger.Error("cedar-sync: bad source config; skipping", "name", src.Name, "err", err.Error())
			continue
		}
		m.logger.Info("cedar-sync: replicating", "name", src.Name, "from", src.Address,
			"source", src.Source, "target", src.Target, "constraint", src.Constraint)
		wg.Add(1)
		go func() { defer wg.Done(); _ = runner.Run(ctx) }()
	}
	wg.Wait()
}

// dialFor builds a cedarsync.Dial that opens a fresh authenticated dbrpc session to the leader at
// addr (a normal DBSession, so any htcondordb is a source). A fresh dial per session lets a
// reconnect recover from a dropped connection.
func (m *cedarSyncManager) dialFor(cfg *config.Config, addr string) cedarsync.Dial {
	return func(dctx context.Context) (*dbrpc.Client, func(), error) {
		sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
		if err != nil {
			return nil, nil, fmt.Errorf("cedar-sync: security config: %w", err)
		}
		sec.Command = command.DBSession
		cl, err := cedarclient.ConnectAndAuthenticate(dctx, addr, sec)
		if err != nil {
			return nil, nil, fmt.Errorf("cedar-sync: connecting to %s: %w", addr, err)
		}
		c := dbrpc.NewClient(dbrpc.NewCedarConn(m.parent, cl.GetStream()))
		return c, func() { _ = c.Close(); _ = cl.Close() }, nil
	}
}

// sinkFor creates (or reuses) the local target table/archive and a durable per-source cursor store,
// and returns the replicate.Sink the runner feeds.
func (m *cedarSyncManager) sinkFor(src replicateSource, posDir string) (replicate.Sink, error) {
	store, err := m.cursorStore(src.Name, posDir)
	if err != nil {
		return nil, err
	}
	if src.Type == "table" {
		d, err := m.cat.CreateTable(src.Target)
		if err != nil {
			return nil, err
		}
		return replicate.NewTableSink(d, src.Src, store)
	}
	a, err := m.cat.CreateArchiveTable(src.Target, db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId"},
		ZoneAttrs:  []string{"CompletionDate", "EnteredHistoryTime"},
	})
	if err != nil {
		return nil, err
	}
	return replicate.NewArchiveSink(a, src.Src, store)
}

func (m *cedarSyncManager) cursorStore(name, posDir string) (replicate.CursorStore, error) {
	if posDir == "" {
		return &replicate.MemCursorStore{}, nil // no persistence dir: ephemeral cursor
	}
	dir := filepath.Join(posDir, "cedarsync")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return replicate.FileCursorStore{Path: filepath.Join(dir, name+".cursor")}, nil
}
