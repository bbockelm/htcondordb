package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/cedarsync"
	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/federate"
)

// federationManager runs this daemon as a federation hub (see the federate package and
// docs/federation.md): it fans in the spokes of an AP set -- schedds matching
// HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT, plus static HTCONDORDB_FEDERATE_SPOKES -- into this
// catalog. Reapplied on condor_reconfig: a changed configuration restarts the hub, which resumes
// every source from its persisted cursor and federation_sources row, so a restart costs a resume,
// not a replay.
type federationManager struct {
	parent  context.Context
	cat     *db.Catalog
	logger  *slog.Logger
	metrics *federate.Metrics // one set for the daemon's lifetime, across hub restarts

	mu     sync.Mutex
	hub    *federate.Hub
	cancel context.CancelFunc
	done   chan struct{}
	sig    string
}

func newFederationManager(parent context.Context, cat *db.Catalog, logger *slog.Logger) *federationManager {
	return &federationManager{parent: parent, cat: cat, logger: logger, metrics: federate.NewMetrics()}
}

// federationSettings is the resolved hub configuration.
type federationSettings struct {
	enabled          bool
	constraint       string
	pool             string
	static           []federate.Spoke
	tables           []string
	retireAfter      time.Duration
	fresh            time.Duration
	discoverInterval time.Duration
	stateInterval    time.Duration
	posDir           string
	archive          federate.ArchiveOptions
	sig              string
}

// resolveFederationSettings reads the hub configuration. bad names knobs whose values could not be
// parsed (the default is used for each); err is a configuration that cannot run at all.
func resolveFederationSettings(cfg *config.Config) (s federationSettings, bad []string, err error) {
	s.constraint = strings.TrimSpace(getStr(cfg, "HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT"))
	for _, name := range strings.Fields(strings.ReplaceAll(getStr(cfg, "HTCONDORDB_FEDERATE_SPOKES"), ",", " ")) {
		p := "HTCONDORDB_FEDERATE_SPOKE_" + strings.ToUpper(name) + "_"
		addr := strings.TrimSpace(getStr(cfg, p+"ADDRESS"))
		if addr == "" {
			return s, nil, fmt.Errorf("%sADDRESS is required for static spoke %q", p, name)
		}
		s.static = append(s.static, federate.Spoke{
			Schedd:  firstNonEmpty(strings.TrimSpace(getStr(cfg, p+"SCHEDD")), name),
			Address: addr,
			Static:  true,
		})
	}
	if s.constraint == "" && len(s.static) == 0 {
		return federationSettings{}, nil, nil
	}
	s.enabled = true
	s.pool = strings.TrimSpace(getStr(cfg, "COLLECTOR_HOST"))
	if s.constraint != "" && s.pool == "" {
		return s, nil, errors.New("HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT needs a collector to query (COLLECTOR_HOST is unset)")
	}
	s.tables = splitAttrList(getStr(cfg, "HTCONDORDB_FEDERATE_TABLES"))
	if len(s.tables) == 0 {
		s.tables = federate.DefaultTables
	}
	dur := func(key string, def time.Duration) time.Duration {
		secs, ok := configSeconds(cfg, key)
		if !ok || secs < 0 {
			bad = append(bad, key+"="+getStr(cfg, key))
			return def
		}
		if secs == 0 {
			return def
		}
		return time.Duration(secs) * time.Second
	}
	s.retireAfter = dur("HTCONDORDB_FEDERATE_RETIRE_AFTER", federate.DefaultRetireAfter)
	s.fresh = dur("HTCONDORDB_FEDERATE_FRESH_SECONDS", federate.DefaultFreshThreshold)
	s.discoverInterval = dur("HTCONDORDB_FEDERATE_DISCOVER_INTERVAL", federate.DefaultDiscoverInterval)
	s.stateInterval = dur("HTCONDORDB_FEDERATE_STATE_INTERVAL", federate.DefaultStateInterval)
	s.posDir = resolveDBDir(cfg)

	// The hub's archives take the same tuning knobs as schedd sync's.
	defMax := configBytes(cfg, "HTCONDORDB_ARCHIVE_MAX_BYTES")
	segSize := configInt(cfg, "HTCONDORDB_ARCHIVE_SEGMENT_SIZE")
	if segSize < 0 {
		segSize = 0
	}
	s.archive = federate.ArchiveOptions{
		ExtraCategorical: splitAttrList(getStr(cfg, "HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS")),
		ExtraValue:       splitAttrList(getStr(cfg, "HTCONDORDB_ARCHIVE_VALUE_ATTRS")),
		SegmentSize:      segSize,
		MaxBytes: map[string]int64{
			federate.TableHistory:      configBytesOr(cfg, "HTCONDORDB_HISTORY_MAX_BYTES", defMax),
			federate.TableEpochHistory: configBytesOr(cfg, "HTCONDORDB_EPOCH_HISTORY_MAX_BYTES", defMax),
		},
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s|%s|%v|%v|%v|%v|%v|%s|%v|", s.constraint, s.pool, s.tables, s.retireAfter, s.fresh,
		s.discoverInterval, s.stateInterval, s.posDir, s.archive)
	for _, sp := range s.static {
		fmt.Fprintf(&sb, "%s=%s;", sp.Schedd, sp.Address)
	}
	s.sig = sb.String()
	return s, bad, nil
}

// checkFederationExclusive refuses a hub and schedd sync in one daemon: both write the jobs,
// history and syncstatus tables, the hub keyed by (schedd, job) and schedd sync by job, and either
// would corrupt the other's rows.
func checkFederationExclusive(cfg *config.Config, s federationSettings) error {
	if s.enabled && configBool(cfg, "HTCONDORDB_SYNC_SCHEDD") {
		return errors.New("HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT / HTCONDORDB_FEDERATE_SPOKES and HTCONDORDB_SYNC_SCHEDD " +
			"cannot both be set: a federation hub and schedd sync both write the jobs, history and syncstatus tables. " +
			"Run the hub as a separate htcondordb (its own HTCONDORDB_DIR and local name)")
	}
	return nil
}

// apply reconciles the running hub with cfg.
func (m *federationManager) apply(cfg *config.Config) error {
	next, bad, err := resolveFederationSettings(cfg)
	if err != nil {
		return err
	}
	for _, k := range bad {
		m.logger.Error("federate: unparseable configuration value, using the default", "knob", k)
	}
	if err := checkFederationExclusive(cfg, next); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if next.sig == m.sig && (m.hub != nil) == next.enabled {
		return nil
	}
	m.stopLocked()
	if !next.enabled {
		return nil
	}
	if next.posDir == "" {
		return errors.New("a federation hub needs a persistent database (set HTCONDORDB_DIR or SPOOL): its archives and cursors live on disk")
	}

	disc := &federate.Discovery{ScheddConstraint: next.constraint, Static: next.static}
	if next.constraint != "" {
		disc.Collector = federate.PoolCollector{Address: next.pool}
	}
	hub, err := federate.New(federate.Config{
		Catalog:          m.cat,
		Tables:           next.tables,
		Archive:          next.archive,
		Discovery:        disc,
		Dial:             func(addr string) cedarsync.Dial { return dbSessionDial(m.parent, cfg, addr, "federate") },
		CursorDir:        filepath.Join(next.posDir, "federate"),
		DiscoverInterval: next.discoverInterval,
		StateInterval:    next.stateInterval,
		FreshThreshold:   next.fresh,
		RetireAfter:      next.retireAfter,
		Metrics:          m.metrics,
		Logger:           m.logger,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(m.parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := hub.Run(ctx); err != nil {
			m.logger.Error("federate: hub stopped", "err", err.Error())
		}
	}()
	m.hub, m.cancel, m.done, m.sig = hub, cancel, done, next.sig
	m.logger.Info("federate: running as a federation hub", "constraint", next.constraint,
		"static_spokes", len(next.static), "tables", strings.Join(next.tables, ","),
		"retire_after", next.retireAfter.String(), "fresh_seconds", int(next.fresh.Seconds()))
	return nil
}

// stopLocked stops the running hub and waits for it: the hub writes this catalog in process, so it
// must be gone before the catalog closes.
func (m *federationManager) stopLocked() {
	if m.cancel != nil {
		m.cancel()
		<-m.done
	}
	m.hub, m.cancel, m.done, m.sig = nil, nil, nil, ""
}

// Stop stops the hub and waits for it. Idempotent.
func (m *federationManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked()
}

// Summary is the hub's collector-ad summary, nil when this daemon is not a hub.
func (m *federationManager) Summary() *dbad.Federation {
	m.mu.Lock()
	hub := m.hub
	m.mu.Unlock()
	if hub == nil {
		return nil
	}
	return hub.Summary()
}

// Retire retires a source now (the `.retire` admin command).
func (m *federationManager) Retire(ctx context.Context, schedd string) error {
	m.mu.Lock()
	hub := m.hub
	m.mu.Unlock()
	if hub == nil {
		return errors.New("this daemon is not a federation hub")
	}
	return hub.Retire(ctx, schedd)
}

// Metrics is the hub's Prometheus collector.
func (m *federationManager) Metrics() *federate.Metrics { return m.metrics }
