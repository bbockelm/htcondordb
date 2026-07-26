package main

import (
	"bufio"
	"context"
	"encoding/json"
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
	"github.com/bbockelm/htcondordb/dbad"
)

// exporterCatalog is the slice of the catalog the manager needs: the registered exporters and
// their durable state (which carries the child's live status). Satisfied by *db.Catalog
// (svc.Catalog()); narrowed for testability.
type exporterCatalog interface {
	Exporters() []db.ExporterDef
	LoadExporterState(name string) ([]byte, bool, error)
}

// childStatus is the live health/progress an exporter child writes into its durable state (the
// "status" object). The daemon parses just this shape -- it never imports the exporter modules
// (dep isolation), so the JSON contract is duplicated here, like the DB-session command constant.
type childStatus struct {
	Beat        int64  `json:"beat"` // child's unix-seconds wall clock at its last refresh
	DocsIndexed uint64 `json:"docsIndexed"`
	DocsSkipped uint64 `json:"docsSkipped"`
	InFlight    int    `json:"inFlight"`
}

// loadChildStatus reads the exporter's state and extracts its status object; ok is false when no
// state (or no status) has been written yet.
func loadChildStatus(cat exporterCatalog, name string) (childStatus, bool) {
	blob, present, err := cat.LoadExporterState(name)
	if err != nil || !present || len(blob) == 0 {
		return childStatus{}, false
	}
	var wrap struct {
		Status childStatus `json:"status"`
	}
	if json.Unmarshal(blob, &wrap) != nil || wrap.Status.Beat == 0 {
		return childStatus{}, false
	}
	return wrap.Status, true
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
	catalog    exporterCatalog
	logger     *slog.Logger
	daemonAddr string // the address children dial back on (advertisedAddr)

	mu      sync.Mutex
	cancel  context.CancelFunc // cancels the reconcile loop + all supervisors; nil when stopped
	done    chan struct{}      // closed once the loop and all supervisors have exited
	current exporterSettings
	runtime map[string]*exporterRuntime // per-exporter live status, for the ad + metrics
}

// exporterRuntime is the daemon's view of one supervised exporter: what the daemon itself knows
// (running, restarts, timing) plus the child's last-reported status.
type exporterRuntime struct {
	Kind      string
	Running   bool
	Restarts  int
	LastStart time.Time
	LastExit  time.Time
	LastErr   string
	Status    childStatus // last status read from the child's durable state
}

// exporterSettings is the resolved, comparable configuration of the manager. The per-Kind
// launcher binaries have a built-in default (kafka->kafkasync, opensearch->opensearchsync),
// overridable by config; the daemon never imports the exporter modules (that would pull their
// Kafka/OpenSearch client deps into the core build).
type exporterSettings struct {
	enabled       bool
	kafkaBin      string
	opensearchBin string
	dropboxBin    string
	runAsUser     string
	// dropboxUser overrides runAsUser for the archive-dropbox kind: its export directory must not
	// be world-/nobody-writable, so the dropbox exporter runs as a real service account (default
	// "condor") rather than the unprivileged "nobody" the network-facing exporters use.
	dropboxUser  string
	pollInterval time.Duration
	// livenessTimeout: a child whose status beat has not advanced within this window (and that has
	// been running at least this long, to cover startup) is treated as wedged and restarted -- the
	// safety net for a deadlocked-but-alive child the supervisor would otherwise never see exit.
	livenessTimeout time.Duration
}

const (
	defaultExporterPoll    = 30 * time.Second
	defaultLivenessTimeout = 90 * time.Second
)

// livenessCheckInterval picks how often to poll a child's beat: often enough relative to the
// timeout to react promptly, capped so a long timeout doesn't poll wastefully.
func livenessCheckInterval(timeout time.Duration) time.Duration {
	iv := timeout / 3
	if iv > 15*time.Second {
		iv = 15 * time.Second
	}
	if iv < 100*time.Millisecond {
		iv = 100 * time.Millisecond
	}
	return iv
}

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
	live := defaultLivenessTimeout
	if s := configInt(cfg, "HTCONDORDB_EXPORTER_LIVENESS_SECONDS"); s > 0 {
		live = time.Duration(s) * time.Second
	}
	dropboxUser := getStr(cfg, "HTCONDORDB_EXPORTER_DROPBOX_USER")
	if dropboxUser == "" {
		dropboxUser = "condor"
	}
	return exporterSettings{
		enabled:         true,
		kafkaBin:        getStr(cfg, "KAFKASYNC"),
		opensearchBin:   getStr(cfg, "OPENSEARCHSYNC"),
		dropboxBin:      getStr(cfg, "ARCHIVEDROPBOX"),
		runAsUser:       runAs,
		dropboxUser:     dropboxUser,
		pollInterval:    poll,
		livenessTimeout: live,
	}
}

// userFor resolves the unprivileged user a given exporter kind runs as. The archive-dropbox kind
// writes a local export directory (not a network endpoint), so it runs as a real service account;
// every other kind runs as the shared unprivileged user.
func (s exporterSettings) userFor(kind string) string {
	if kind == "dropbox" {
		return s.dropboxUser
	}
	return s.runAsUser
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
				m.forgetRuntime(name)
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
			go func(name, kind, bin string) {
				defer wg.Done()
				m.supervise(sctx, s, name, kind, bin)
			}(name, def.Kind, bin)
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
func (m *exporterManager) supervise(ctx context.Context, s exporterSettings, name, kind, bin string) {
	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := m.runOnce(ctx, s, name, kind, bin)
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

// runOnce mints a fresh session, launches the child, forwards its logs, watches its liveness,
// waits for it to exit, and invalidates the session. A fresh session per launch means a crashed
// child's credential is reaped immediately.
func (m *exporterManager) runOnce(ctx context.Context, s exporterSettings, name, kind, bin string) error {
	sid, token, err := security.MintInheritableSession(m.daemonAddr, command.DBSession)
	if err != nil {
		return fmt.Errorf("minting session: %w", err)
	}
	defer security.GetSessionCache().Invalidate(sid)

	// A child-scoped context so the liveness monitor can kill a wedged child (which exec.Cmd
	// with this context terminates), triggering the supervisor's restart.
	childCtx, cancelChild := context.WithCancel(ctx)
	defer cancelChild()

	cmd := exec.CommandContext(childCtx, bin, "run", "-name", name, "-addr", m.daemonAddr)
	cmd.Env = append(os.Environ(), security.EnvCondorPrivateInherit+"="+token)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	// Launch the child as its kind's run-as user (default "nobody"; "condor" for the archive
	// dropbox, whose export directory must not be nobody-writable) via droppriv, which elevates
	// only the forking thread across the fork and restores it -- the daemon never runs as root.
	// Falls back to the daemon's own user when unprivileged.
	if err := droppriv.StartAsUser(s.userFor(kind), cmd); err != nil {
		return fmt.Errorf("starting %s: %w", bin, err)
	}
	m.markStart(name, kind)
	go m.monitorLiveness(childCtx, cancelChild, name, time.Now(), s.livenessTimeout)

	var lwg sync.WaitGroup
	lwg.Add(2)
	go func() { defer lwg.Done(); m.forwardLog(name, "stdout", stdout) }()
	go func() { defer lwg.Done(); m.forwardLog(name, "stderr", stderr) }()
	lwg.Wait() // pipes close at process exit

	err = cmd.Wait()
	m.markExit(name, err)
	return err
}

// monitorLiveness kills the child (via cancel) if it stops advancing its status beat while it
// should be running -- catching a deadlocked-but-alive child the supervisor would never see exit.
// It also refreshes the daemon's view of the child's progress for the ad/metrics.
func (m *exporterManager) monitorLiveness(ctx context.Context, cancel context.CancelFunc, name string, start time.Time, timeout time.Duration) {
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
			st, ok := loadChildStatus(m.catalog, name)
			if ok {
				m.setStatus(name, st)
			}
			// Grace: a child gets at least `timeout` to start up and write its first beat.
			if now.Sub(start) < timeout {
				continue
			}
			stale := !ok || now.Sub(time.Unix(st.Beat, 0)) > timeout
			if stale {
				m.logger.Warn("exporter manager: child unresponsive (no status beat); restarting",
					"name", name, "up", now.Sub(start).Round(time.Second), "beat_seen", ok)
				cancel() // terminate the child; runOnce's cmd.Wait returns and the supervisor restarts
				return
			}
		}
	}
}

// --- per-exporter runtime status (for the collector ad + Prometheus) ---

// rt returns the runtime record for name, creating it. Caller holds m.mu.
func (m *exporterManager) rt(name string) *exporterRuntime {
	if m.runtime == nil {
		m.runtime = map[string]*exporterRuntime{}
	}
	e := m.runtime[name]
	if e == nil {
		e = &exporterRuntime{}
		m.runtime[name] = e
	}
	return e
}

func (m *exporterManager) markStart(name, kind string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.rt(name)
	e.Kind = kind
	e.Running = true
	e.LastStart = time.Now()
}

func (m *exporterManager) markExit(name string, err error) {
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

func (m *exporterManager) setStatus(name string, st childStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rt(name).Status = st
}

func (m *exporterManager) forgetRuntime(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.runtime, name)
}

// Statuses returns a snapshot of every supervised exporter's status, in the shape the collector
// ad + Prometheus consume. Safe for concurrent use.
func (m *exporterManager) Statuses() []dbad.ExporterStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]dbad.ExporterStatus, 0, len(m.runtime))
	for name, e := range m.runtime {
		var beat time.Time
		if e.Status.Beat > 0 {
			beat = time.Unix(e.Status.Beat, 0)
		}
		out = append(out, dbad.ExporterStatus{
			Name:        name,
			Kind:        e.Kind,
			Running:     e.Running,
			Restarts:    e.Restarts,
			LastBeat:    beat,
			DocsIndexed: e.Status.DocsIndexed,
			DocsSkipped: e.Status.DocsSkipped,
			InFlight:    e.Status.InFlight,
			LastErr:     e.LastErr,
		})
	}
	return out
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
	case "dropbox":
		return resolveBinary(s.dropboxBin, "archivedropbox")
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
