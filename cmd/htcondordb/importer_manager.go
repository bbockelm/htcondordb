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
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/droppriv"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/historyimport"
)

// importerManager launches and supervises the out-of-process history-import
// runners -- one per HTCONDORDB_HISTORY_IMPORT job. It mirrors exporterManager's
// mint/spawn/droppriv/backoff shape, but its job set comes from config (not a DB
// catalog), so it is fixed per config epoch and re-read on reconfigure. Unlike the
// network-facing exporters it runs as a real service account (default "condor"):
// the importer makes OUTBOUND authenticated queries to the pool's schedds and so
// needs that user's pool credentials, not the credential-less "nobody".
type importerManager struct {
	parent     context.Context
	logger     *slog.Logger
	daemonAddr string // the address children dial back on

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	current importerSettings
	runtime map[string]*importerRuntime // per-job live status, for liveness + the ad
}

// importerRuntime is the daemon's view of one supervised import job: what the
// daemon knows (running, restarts, timing) plus the runner's last-reported status.
type importerRuntime struct {
	Running   bool
	Restarts  int
	LastStart time.Time
	LastExit  time.Time
	LastErr   string
	Status    historyimport.Status
}

type importerSettings struct {
	enabled         bool
	user            string
	binOverride     string
	logDir          string
	jobs            []string
	livenessTimeout time.Duration
	gracefulTimeout time.Duration
}

func (s importerSettings) equal(o importerSettings) bool {
	return s.enabled == o.enabled && s.user == o.user && s.binOverride == o.binOverride &&
		s.logDir == o.logDir && s.livenessTimeout == o.livenessTimeout &&
		s.gracefulTimeout == o.gracefulTimeout && slices.Equal(s.jobs, o.jobs)
}

func newImporterManager(ctx context.Context, logger *slog.Logger, daemonAddr string) *importerManager {
	return &importerManager{parent: ctx, logger: logger, daemonAddr: daemonAddr}
}

func resolveImporterSettings(cfg *config.Config) importerSettings {
	// Managed by default; an operator running the importer externally sets this false.
	if v, ok := cfg.Get("HTCONDORDB_MANAGE_HISTORY_IMPORT"); ok && !truthy(v) {
		return importerSettings{}
	}
	jobs, err := historyimport.JobsFromConfig(cfg.Get)
	if err != nil {
		// A malformed import config disables the manager rather than crashing the
		// daemon; the error is logged by apply.
		return importerSettings{}
	}
	if len(jobs) == 0 {
		return importerSettings{}
	}
	names := make([]string, len(jobs))
	for i, j := range jobs {
		names[i] = j.Name
	}
	slices.Sort(names)

	user := getStr(cfg, "HTCONDORDB_HISTORY_IMPORT_USER")
	if user == "" {
		user = "condor"
	}
	grace := defaultGracefulShutdown
	if s := configInt(cfg, "HTCONDORDB_HISTORY_IMPORT_SHUTDOWN_SECONDS"); s > 0 {
		grace = time.Duration(s) * time.Second
	}
	live := defaultLivenessTimeout
	if s := configInt(cfg, "HTCONDORDB_HISTORY_IMPORT_LIVENESS_SECONDS"); s > 0 {
		live = time.Duration(s) * time.Second
	}
	logDir := getStr(cfg, "LOG")
	if logDir == "" {
		logDir = os.TempDir()
	}
	return importerSettings{
		enabled:         true,
		user:            user,
		binOverride:     getStr(cfg, "HISTORY_IMPORT"),
		logDir:          logDir,
		jobs:            names,
		livenessTimeout: live,
		gracefulTimeout: grace,
	}
}

// apply reconciles the manager with cfg: a no-op when settings are unchanged,
// otherwise it stops the running supervisors and (if enabled) starts fresh ones.
func (m *importerManager) apply(cfg *config.Config) error {
	// Surface a malformed import config once, even though it disables the manager.
	if _, err := historyimport.JobsFromConfig(cfg.Get); err != nil {
		m.logger.Warn("importer manager: invalid HTCONDORDB_HISTORY_IMPORT config; disabled", "err", err)
	}
	next := resolveImporterSettings(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()
	if next.equal(m.current) {
		return nil
	}
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel = nil
		m.done = nil
	}
	m.current = importerSettings{}
	if !next.enabled {
		m.logger.Info("importer manager: disabled")
		return nil
	}

	bin := resolveBinary(next.binOverride, "history-import")
	if bin == "" {
		m.logger.Warn("importer manager: history-import binary not found; leaving import jobs to an external runner",
			"jobs", next.jobs)
		return nil
	}

	// Drop runtime status for jobs no longer configured, so the ad stops
	// advertising them.
	keep := make(map[string]bool, len(next.jobs))
	for _, n := range next.jobs {
		keep[n] = true
	}
	for n := range m.runtime {
		if !keep[n] {
			delete(m.runtime, n)
		}
	}

	ctx, cancel := context.WithCancel(m.parent)
	done := make(chan struct{})
	go m.run(ctx, next, bin, done)
	m.cancel = cancel
	m.done = done
	m.current = next
	m.logger.Info("importer manager: enabled", "user", next.user, "bin", bin, "jobs", next.jobs)
	return nil
}

// run supervises one goroutine per configured job until ctx is cancelled.
func (m *importerManager) run(ctx context.Context, s importerSettings, bin string, done chan struct{}) {
	defer close(done)
	var wg sync.WaitGroup
	for _, name := range s.jobs {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			m.supervise(ctx, s, name, bin)
		}(name)
	}
	wg.Wait()
}

// supervise runs the launch/backoff loop for one import job until ctx is cancelled.
func (m *importerManager) supervise(ctx context.Context, s importerSettings, name, bin string) {
	const maxBackoff = 30 * time.Second
	backoff := time.Second
	m.logger.Info("importer manager: supervising import job", "name", name, "bin", bin)
	for ctx.Err() == nil {
		start := time.Now()
		err := m.runOnce(ctx, s, name, bin)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) >= maxBackoff {
			backoff = time.Second // a run that stayed up a while resets the backoff
		}
		if err != nil {
			m.logger.Warn("importer manager: import job exited; restarting", "name", name, "err", err, "backoff", backoff)
		} else {
			m.logger.Info("importer manager: import job exited; restarting", "name", name, "backoff", backoff)
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

// runOnce mints a fresh inheritable session, launches the child as the importer
// user, forwards its logs, and waits for it to exit. A fresh session per launch
// means a crashed child's token cannot be reused.
func (m *importerManager) runOnce(ctx context.Context, s importerSettings, name, bin string) error {
	sid, token, err := security.MintInheritableSession(m.daemonAddr, command.DBSession)
	if err != nil {
		return fmt.Errorf("minting session: %w", err)
	}
	defer security.GetSessionCache().Invalidate(sid)

	childCtx, cancelChild := context.WithCancel(ctx)
	defer cancelChild()

	// The manager owns the cursor + status file paths (under $(LOG)) so it knows
	// where to read the child's liveness beat.
	cursorFile := filepath.Join(s.logDir, "history-import-"+name+".cursors.json")
	statusFile := filepath.Join(s.logDir, "history-import-"+name+".status.json")

	cmd := exec.CommandContext(childCtx, bin, "run", "-name", name, "-addr", m.daemonAddr,
		"-cursor-file", cursorFile, "-status-file", statusFile)
	cmd.Env = append(os.Environ(), security.EnvCondorPrivateInherit+"="+token)
	// Graceful stop: on childCtx cancel send SIGTERM so the child drains, then
	// SIGKILL after gracefulTimeout. history-import installs a SIGTERM handler.
	grace := s.gracefulTimeout
	if grace <= 0 {
		grace = defaultGracefulShutdown
	}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	cmd.WaitDelay = grace

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	// Run as the importer's service account (default "condor"), which holds the
	// pool credentials the child needs to query remote schedds. droppriv elevates
	// only the forking thread and restores it; falls back to the daemon's own user
	// when unprivileged.
	if err := droppriv.StartAsUser(s.user, cmd); err != nil {
		return fmt.Errorf("starting %s: %w", bin, err)
	}
	m.markStart(name)
	go m.monitorLiveness(childCtx, cancelChild, name, statusFile, time.Now(), s.livenessTimeout)

	var lwg sync.WaitGroup
	lwg.Add(2)
	go func() { defer lwg.Done(); forwardChildLog(m.logger, name, "stdout", stdout) }()
	go func() { defer lwg.Done(); forwardChildLog(m.logger, name, "stderr", stderr) }()
	lwg.Wait() // pipes close at process exit

	err = cmd.Wait()
	m.markExit(name, err)
	return err
}

// monitorLiveness restarts a child whose status beat has gone stale -- a
// deadlocked-but-alive runner the supervisor would otherwise never see exit. It
// also refreshes the daemon's view of the runner's progress for the ad/metrics.
func (m *importerManager) monitorLiveness(ctx context.Context, cancel context.CancelFunc, name, statusFile string, start time.Time, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	t := time.NewTicker(livenessCheckInterval(timeout))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			st, ok := historyimport.ReadStatusFile(statusFile)
			if ok {
				m.setStatus(name, st)
			}
			if now.Sub(start) < timeout {
				continue // grace: the runner gets `timeout` to start and write its first beat
			}
			if !ok || now.Sub(time.Unix(st.Beat, 0)) > timeout {
				m.logger.Warn("importer manager: runner unresponsive (no status beat); restarting",
					"name", name, "up", now.Sub(start).Round(time.Second), "beat_seen", ok)
				cancel()
				return
			}
		}
	}
}

// --- per-job runtime status (for the collector ad + Prometheus) ---

// rt returns the runtime record for name, creating it. Caller holds m.mu.
func (m *importerManager) rt(name string) *importerRuntime {
	if m.runtime == nil {
		m.runtime = map[string]*importerRuntime{}
	}
	e := m.runtime[name]
	if e == nil {
		e = &importerRuntime{}
		m.runtime[name] = e
	}
	return e
}

func (m *importerManager) markStart(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.rt(name)
	e.Running = true
	e.LastStart = time.Now()
}

func (m *importerManager) markExit(name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.rt(name)
	e.Running = false
	e.LastExit = time.Now()
	e.Restarts++
	if err != nil {
		e.LastErr = err.Error()
	}
}

func (m *importerManager) setStatus(name string, st historyimport.Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rt(name).Status = st
}

// Statuses returns each supervised job's health for the collector ad and metrics.
func (m *importerManager) Statuses() []dbad.ImporterStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]dbad.ImporterStatus, 0, len(m.runtime))
	for name, e := range m.runtime {
		var beat, cyc time.Time
		if e.Status.Beat > 0 {
			beat = time.Unix(e.Status.Beat, 0)
		}
		if e.Status.LastCycleUnix > 0 {
			cyc = time.Unix(e.Status.LastCycleUnix, 0)
		}
		// A cycle-level failure (the runner still alive) is more informative than a
		// past process-exit error, so prefer it.
		lastErr := e.Status.LastError
		if lastErr == "" {
			lastErr = e.LastErr
		}
		out = append(out, dbad.ImporterStatus{
			Name:          name,
			Running:       e.Running,
			Restarts:      e.Restarts,
			LastBeat:      beat,
			LastCycle:     cyc,
			Schedds:       e.Status.Schedds,
			Failures:      e.Status.Failures,
			ImportedTotal: e.Status.ImportedTotal,
			LastErr:       lastErr,
		})
	}
	return out
}

// forwardChildLog relays a child's output stream into the daemon log, one line per
// record, tagged with the job name.
func forwardChildLog(logger *slog.Logger, name, stream string, r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		logger.Info("history-import child", "job", name, "stream", stream, "line", sc.Text())
	}
}
