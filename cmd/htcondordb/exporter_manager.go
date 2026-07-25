package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/cedar/security"
	htconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/droppriv"

	"github.com/bbockelm/htcondordb/command"
)

// exporterLister is the slice of the catalog the manager needs: the set of registered
// exporters. Satisfied by *db.Catalog (svc.Catalog()); narrowed for testability.
type exporterLister interface {
	Exporters() []db.ExporterDef
}

// exporterManager launches and supervises the standalone change-data exporter processes
// (kafkasync / opensearchsync) as children of the daemon, so an operator only has to register
// an exporter (CreateExporter) -- the daemon runs it. It mirrors scheddSyncManager's reconcile
// shape, but supervises subprocesses instead of in-process goroutines and reconciles against the
// catalog's exporter set (which changes at runtime via dbrpc, not config), polling for
// add/drop.
//
// Each launch:
//   - mints a fresh, standalone CEDAR session (condor@parent, distinct from the family session)
//     and passes it to the child via CONDOR_PRIVATE_INHERIT, so the child reaches the daemon's
//     DAEMON-gated dbrpc surface by resuming that session -- no full auth handshake;
//   - runs the child as an unprivileged user (default "nobody") when the daemon has the
//     privilege to setuid it (droppriv.StartAsUser);
//   - forwards the child's stdout/stderr into the daemon log, tagged with the exporter name;
//   - invalidates the minted session the moment the child exits, so a leaked token from a dead
//     child is immediately useless;
//   - restarts with exponential backoff capped at 30s.
type exporterManager struct {
	parent     context.Context
	catalog    exporterLister
	logger     *slog.Logger
	daemonAddr string // the address children dial back on (advertisedAddr)

	mu      sync.Mutex
	cancel  context.CancelFunc // cancels the reconcile loop + all supervisors; nil when stopped
	done    chan struct{}      // closed once the loop and all supervisors have exited
	current exporterSettings
}

// exporterSettings is the resolved, comparable configuration of the manager. The per-Kind
// launcher binaries have a built-in default (kafka->kafkasync, opensearch->opensearchsync),
// overridable by config; the daemon never imports the exporter modules (that would pull their
// Kafka/OpenSearch client deps into the core build).
type exporterSettings struct {
	enabled       bool
	kafkaBin      string
	opensearchBin string
	runAsUser     string
	pollInterval  time.Duration
}

const defaultExporterPoll = 30 * time.Second

func resolveExporterSettings(cfg *htconfig.Config) exporterSettings {
	// Managed by default; an operator running the syncs externally sets this false.
	if v, ok := cfg.Get("HTCONDORDB_MANAGE_EXPORTERS"); ok && !truthy(v) {
		return exporterSettings{}
	}
	runAs := getStr(cfg, "HTCONDORDB_EXPORTER_USER")
	if runAs == "" {
		runAs = "nobody"
	}
	poll := defaultExporterPoll
	if s := configInt(cfg, "HTCONDORDB_EXPORTER_POLL_SECONDS"); s > 0 {
		poll = time.Duration(s) * time.Second
	}
	return exporterSettings{
		enabled:       true,
		kafkaBin:      getStr(cfg, "KAFKASYNC"),
		opensearchBin: getStr(cfg, "OPENSEARCHSYNC"),
		runAsUser:     runAs,
		pollInterval:  poll,
	}
}

// truthy mirrors configBool's acceptance set for a value we read directly (default-on knob).
func truthy(v string) bool {
	switch v {
	case "true", "1", "yes", "t", "TRUE", "True", "YES", "Yes", "T":
		return true
	}
	return false
}

// apply reconciles the manager with cfg: a no-op when settings are unchanged, otherwise it stops
// the running loop+supervisors and (if enabled) starts a fresh reconcile loop.
func (m *exporterManager) apply(cfg *htconfig.Config) error {
	next := resolveExporterSettings(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()
	if next == m.current {
		return nil
	}
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel = nil
		m.done = nil
	}
	m.current = exporterSettings{}
	if !next.enabled {
		m.logger.Info("exporter manager: disabled")
		return nil
	}

	ctx, cancel := context.WithCancel(m.parent)
	done := make(chan struct{})
	go m.reconcileLoop(ctx, next, done)
	m.cancel = cancel
	m.done = done
	m.current = next
	m.logger.Info("exporter manager: enabled", "runAs", next.runAsUser, "poll", next.pollInterval)
	return nil
}

// reconcileLoop supervises one goroutine per known-launchable exporter, adding/removing
// supervisors as the catalog's exporter set changes. It exits (closing done) when ctx is
// cancelled and every supervisor has stopped.
func (m *exporterManager) reconcileLoop(ctx context.Context, s exporterSettings, done chan struct{}) {
	defer close(done)
	var wg sync.WaitGroup
	supervisors := map[string]context.CancelFunc{} // exporter name -> its supervisor's cancel

	reconcile := func() {
		want := map[string]db.ExporterDef{}
		for _, def := range m.catalog.Exporters() {
			if s.binaryFor(def.Kind) == "" {
				continue // no launcher for this kind (unknown/foreign): leave it to an external runner
			}
			want[def.Name] = def
		}
		// Stop supervisors for exporters that were dropped.
		for name, cancel := range supervisors {
			if _, ok := want[name]; !ok {
				m.logger.Info("exporter manager: exporter removed; stopping", "name", name)
				cancel()
				delete(supervisors, name)
			}
		}
		// Start supervisors for newly-registered exporters.
		for name, def := range want {
			if _, ok := supervisors[name]; ok {
				continue
			}
			sctx, scancel := context.WithCancel(ctx)
			supervisors[name] = scancel
			bin := s.binaryFor(def.Kind)
			wg.Add(1)
			go func(name, bin string) {
				defer wg.Done()
				m.supervise(sctx, s, name, bin)
			}(name, bin)
			m.logger.Info("exporter manager: supervising exporter", "name", name, "kind", def.Kind, "bin", bin)
		}
	}

	reconcile()
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

// supervise runs the launch/mint/invalidate/backoff loop for one exporter until ctx is
// cancelled.
func (m *exporterManager) supervise(ctx context.Context, s exporterSettings, name, bin string) {
	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := m.runOnce(ctx, s, name, bin)
		if ctx.Err() != nil {
			return
		}
		// A run that stayed up a while resets the backoff.
		if time.Since(start) >= maxBackoff {
			backoff = time.Second
		}
		if err != nil {
			m.logger.Warn("exporter manager: exporter exited; restarting", "name", name, "err", err, "backoff", backoff)
		} else {
			m.logger.Info("exporter manager: exporter exited; restarting", "name", name, "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// runOnce mints a fresh session, launches the child, forwards its logs, waits for it to exit,
// and invalidates the session. A fresh session per launch means a crashed child's credential is
// reaped immediately.
func (m *exporterManager) runOnce(ctx context.Context, s exporterSettings, name, bin string) error {
	sid, token, err := security.MintInheritableSession(m.daemonAddr, command.DBSession)
	if err != nil {
		return fmt.Errorf("minting session: %w", err)
	}
	defer security.GetSessionCache().Invalidate(sid)

	cmd := exec.CommandContext(ctx, bin, "run", "-name", name, "-addr", m.daemonAddr)
	cmd.Env = append(os.Environ(), security.EnvCondorPrivateInherit+"="+token)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	// Launch the child as the unprivileged run-as user (default "nobody") via droppriv, which
	// elevates only the forking thread across the fork and restores it -- the daemon never runs
	// as root. Falls back to the daemon's own user when unprivileged.
	if err := droppriv.StartAsUser(s.runAsUser, cmd); err != nil {
		return fmt.Errorf("starting %s: %w", bin, err)
	}

	var lwg sync.WaitGroup
	lwg.Add(2)
	go func() { defer lwg.Done(); m.forwardLog(name, "stdout", stdout) }()
	go func() { defer lwg.Done(); m.forwardLog(name, "stderr", stderr) }()
	lwg.Wait() // pipes close at process exit

	return cmd.Wait()
}

// forwardLog copies a child's output stream into the daemon log line-by-line, tagged with the
// exporter name and stream.
func (m *exporterManager) forwardLog(name, stream string, r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		m.logger.Info("exporter", "name", name, "stream", stream, "line", sc.Text())
	}
}

// binaryFor resolves the launcher binary for an exporter Kind: the config override if set, else
// the built-in default basename resolved on PATH or next to the daemon binary. Returns "" for a
// Kind with no known launcher.
func (s exporterSettings) binaryFor(kind string) string {
	switch kind {
	case "kafka":
		return resolveBinary(s.kafkaBin, "kafkasync")
	case "opensearch":
		return resolveBinary(s.opensearchBin, "opensearchsync")
	default:
		return ""
	}
}

func resolveBinary(override, defaultName string) string {
	if override != "" {
		return override
	}
	if p, err := exec.LookPath(defaultName); err == nil {
		return p
	}
	if self, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(self), defaultName)
		if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
			return cand
		}
	}
	return "" // not found: skip (an external runner can still handle this exporter)
}
