// Command htcondordb runs the HTCondor ClassAd database as a Go daemon.
//
// It serves the embedded ClassAd-log database (a transactional key/ad store with
// constraint queries, matchmaking, ordered indexes, and change watches) over a
// single CEDAR command, enforcing HTCondor READ / WRITE / DAEMON authorization:
//
//   - READ  clients get a read-only view with private attributes stripped;
//   - WRITE clients get full read/write;
//   - DAEMON clients additionally reach the HA/replication surface.
//
// It runs under condor_master like any DaemonCore daemon (shared-port endpoint,
// DC_SET_READY / DC_CHILDALIVE, privilege drop on start, SIGHUP reconfigure),
// mirroring cmd/golang-negotiator and cmd/golang-collector.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/authz"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "htcondordb:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":0", "fallback TCP listen address when not inheriting a shared-port endpoint")
	// condor_master appends these standard DaemonCore flags for a daemon not in
	// its built-in list; accept them so flag.Parse does not reject our launch.
	// -local-name additionally scopes config lookups (HTCONDORDB.<key> beats <key>).
	localName := flag.String("local-name", "", "HTCondor subsystem local-name; passed by condor_master")
	_ = flag.String("sock", "", "HTCondor shared-port endpoint name; accepted for compatibility (fd inherited via CONDOR_INHERIT)")
	flag.Parse()

	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "HTCONDORDB", LocalName: *localName})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Bootstrap logging and condor_master integration (drops privileges to the
	// condor user when started as root).
	d, err := daemon.New(daemon.Options{Subsys: "HTCONDORDB", Config: cfg})
	if err != nil {
		return err
	}
	log := d.Logger()
	slog.SetDefault(d.Slog()) // route cedar's server/security slog into our log

	// Server-side security policy for our command socket (SEC_* knobs). The
	// negotiated command is DBSession; DAEMON is the strongest level we serve.
	sec, err := htcondor.GetServerSecurityConfig(d.Config(), command.DBSession, "DAEMON")
	if err != nil {
		return fmt.Errorf("building security config: %w", err)
	}
	srv := cedarserver.New(sec)

	// Per-command ALLOW_/DENY_ authorization from the configuration.
	policy, err := authz.NewPolicy(d.Config(), "HTCONDORDB")
	if err != nil {
		return fmt.Errorf("building authorization policy: %w", err)
	}
	srv.Authorizer = policy.Authorize

	// Resolve the HA configuration (standalone / leader-follower / consistent).
	ha, err := detectHA(cfg)
	if err != nil {
		return err
	}

	// The database service. A follower (or a non-leader raft node) serves
	// read-only: writes go to the leader.
	svc, err := server.New(server.Config{
		Dir:           databaseDir(d, cfg),
		Authorize:     policy.Authorize,
		ForceReadOnly: ha.forceReadOnly,
		Logger:        d.Slog(),
	})
	if err != nil {
		return err
	}
	defer func() { _ = svc.Close() }()
	svc.RegisterOn(srv)

	// DC_NOP / DC_RECONFIG / DC_OFF so condor_ping, condor_reconfig -daemon and
	// condor_off -daemon work against this daemon's command port.
	d.RegisterDefaultCommands(srv)

	// Command-socket listener: the inherited shared-port endpoint under
	// condor_master, else a plain TCP bind.
	ln, err := d.Listener(func() (net.Listener, error) {
		return net.Listen("tcp", *listen)
	})
	if err != nil {
		log.Error(logging.DestinationGeneral, "listener setup failed", "err", err.Error())
		return err
	}
	defer func() { _ = ln.Close() }()

	// Publish the command address so clients (the REPL, followers) can find us.
	if path := writeAddressFile(d, cfg, ln); path != "" {
		defer func() { _ = os.Remove(path) }()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start any background HA machinery (a follower's replicator, or the raft
	// coordinator and its command handlers in consistent mode).
	defer ha.close()
	if err := ha.start(ctx, d, cfg, svc, srv, advertisedAddr(d, ln)); err != nil {
		return err
	}

	log.Info(logging.DestinationGeneral, "htcondordb starting",
		"listen", ln.Addr().String(), "address", advertisedAddr(d, ln),
		"db_dir", databaseDir(d, cfg), "under_master", d.UnderMaster(),
		"ha_mode", ha.mode, "role", ha.role, "read_only", ha.forceReadOnly)

	return d.Serve(ctx, ln, srv.Serve)
}

// databaseDir resolves the on-disk database directory: HTCONDORDB_DIR if set,
// else $(SPOOL)/htcondordb. Empty (in-memory) only when neither is configured.
func databaseDir(d *daemon.Daemon, cfg *config.Config) string {
	if v, ok := cfg.Get("HTCONDORDB_DIR"); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if spool, ok := cfg.Get("SPOOL"); ok && strings.TrimSpace(spool) != "" {
		return filepath.Join(strings.TrimSpace(spool), "htcondordb")
	}
	d.Logger().Warn(logging.DestinationGeneral, "no HTCONDORDB_DIR or SPOOL configured; database is in-memory only")
	return ""
}

// advertisedAddr is the daemon's externally reachable command address: the
// shared-port sinful under condor_master, else the plain listen address.
func advertisedAddr(d *daemon.Daemon, ln net.Listener) string {
	if sinful, ok := d.AdvertisedSinful(); ok {
		return sinful
	}
	return ln.Addr().String()
}

// writeAddressFile publishes the command address to HTCONDORDB_ADDRESS_FILE
// (default $(LOG)/.htcondordb_address). Returns the path written, or "".
func writeAddressFile(d *daemon.Daemon, cfg *config.Config, ln net.Listener) string {
	path, ok := cfg.Get("HTCONDORDB_ADDRESS_FILE")
	if !ok || strings.TrimSpace(path) == "" {
		logDir, ok := cfg.Get("LOG")
		if !ok || logDir == "" {
			return ""
		}
		path = filepath.Join(logDir, ".htcondordb_address")
	}
	if err := os.WriteFile(path, []byte("<"+advertisedAddr(d, ln)+">\n"), 0o644); err != nil {
		d.Logger().Warn(logging.DestinationGeneral, "could not write address file", "path", path, "err", err.Error())
		return ""
	}
	return path
}
