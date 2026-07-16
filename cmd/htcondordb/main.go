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
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/cedar/security"
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
	// This daemon gates every operation on the peer's authenticated identity
	// (READ vs WRITE vs DAEMON), so authorization is meaningless without an
	// identity. HTCondor's default SEC_*_AUTHENTICATION is OPTIONAL, and
	// OPTIONAL+OPTIONAL negotiates to *no* authentication -- leaving every peer
	// anonymous and therefore read-only. Prefer authentication instead: it runs
	// whenever the peer offers a mutually-supported method (so a local client
	// maps to its user via FS) but still admits a peer with no method (which
	// stays anonymous/read-only). An admin who really wants OPTIONAL/NEVER can
	// set SEC_DEFAULT_AUTHENTICATION explicitly and this leaves it untouched.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}
	srv := cedarserver.New(sec)

	// Per-command ALLOW_/DENY_ authorization from the configuration. The policy
	// is held behind an atomic pointer and rebuilt on reconfigure (SIGHUP /
	// condor_reconfig), so an ALLOW_WRITE change takes effect on the next
	// connection without a daemon restart. The authorize closure reads the
	// current policy race-free.
	var policyPtr atomic.Pointer[authz.Policy]
	policy, err := authz.NewPolicy(d.Config(), "HTCONDORDB")
	if err != nil {
		return fmt.Errorf("building authorization policy: %w", err)
	}
	policyPtr.Store(policy)
	authorize := func(perm, peerAddr, user string) bool {
		return policyPtr.Load().Authorize(perm, peerAddr, user)
	}
	srv.Authorizer = authorize
	d.OnReconfig(func(newCfg *config.Config) {
		p, perr := authz.NewPolicy(newCfg, "HTCONDORDB")
		if perr != nil {
			log.Error(logging.DestinationGeneral, "reconfigure: keeping old authorization policy", "err", perr.Error())
			return
		}
		policyPtr.Store(p)
		log.Info(logging.DestinationGeneral, "reloaded authorization policy on reconfigure")
	})

	// Resolve the HA configuration (standalone / leader-follower / consistent).
	ha, err := detectHA(cfg)
	if err != nil {
		return err
	}

	// Encryption at rest (opt-in via HTCONDORDB_ENCRYPT_AT_REST): wrap each table's
	// master key under the pool signing keys. Node-local -- a follower uses its own keys.
	poolKeys, encAttrs, err := encryptionConfig(cfg)
	if err != nil {
		return err
	}
	if len(poolKeys) > 0 {
		log.Info(logging.DestinationGeneral, "encryption at rest enabled",
			"pool_keys", len(poolKeys), "extra_encrypted_attrs", len(encAttrs))
	}

	// The database service. A follower (or a non-leader raft node) serves
	// read-only: writes go to the leader.
	svc, err := server.New(server.Config{
		Dir:            databaseDir(d, cfg),
		Authorize:      authorize,
		ForceReadOnly:  ha.forceReadOnly,
		Logger:         d.Slog(),
		PoolKeys:       poolKeys,
		EncryptedAttrs: encAttrs,
	})
	if err != nil {
		return err
	}
	defer func() { _ = svc.Close() }()

	// Restore-on-startup (disaster recovery): if HTCONDORDB_RESTORE_FILE names an existing
	// snapshot, load it before serving, then the file is moved aside so a restart serves
	// live data. An encrypted snapshot is opened with this daemon's pool keys.
	if restoreFile := getStr(cfg, "HTCONDORDB_RESTORE_FILE"); restoreFile != "" {
		if restored, rerr := svc.RestoreOnStartup(restoreFile); rerr != nil {
			return fmt.Errorf("restore-on-startup from %s: %w", restoreFile, rerr)
		} else if restored {
			log.Info(logging.DestinationGeneral, "restored database from snapshot", "file", restoreFile)
		}
	}

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

	// Periodic encrypted backups: every HTCONDORDB_SNAPSHOT_INTERVAL seconds, write a
	// timestamped snapshot to HTCONDORDB_SNAPSHOT_DIR, keeping the most recent
	// HTCONDORDB_SNAPSHOT_KEEP (default 7). Disabled when either is unset/zero. A
	// follower snapshots its own (independently encrypted) copy.
	if snapDir := getStr(cfg, "HTCONDORDB_SNAPSHOT_DIR"); snapDir != "" {
		if secs := configInt(cfg, "HTCONDORDB_SNAPSHOT_INTERVAL"); secs > 0 {
			keep := configInt(cfg, "HTCONDORDB_SNAPSHOT_KEEP")
			if keep <= 0 {
				keep = 7
			}
			go svc.RunPeriodicSnapshots(ctx, snapDir, time.Duration(secs)*time.Second, keep)
		}
	}

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

// encryptionConfig resolves encryption at rest from configuration. It is opt-in via
// HTCONDORDB_ENCRYPT_AT_REST; when enabled it loads the pool signing keys (the same
// SEC_PASSWORD_DIRECTORY keys used for token signing) as the KEKs that wrap each
// table's master key, and reads any extra attributes to encrypt beyond the always-on
// private attributes. Disabled ⇒ (nil, nil, nil). Enabled with no signing keys is an
// error: encryption was asked for but cannot be keyed.
func encryptionConfig(cfg *config.Config) (poolKeys []db.KEK, attrs []string, err error) {
	if !configBool(cfg, "HTCONDORDB_ENCRYPT_AT_REST") {
		return nil, nil, nil
	}
	keyMap, err := htcondor.LoadSigningKeys(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("encryption at rest: loading pool signing keys: %w", err)
	}
	if len(keyMap) == 0 {
		return nil, nil, fmt.Errorf("encryption at rest: HTCONDORDB_ENCRYPT_AT_REST is set but no signing keys found (configure SEC_PASSWORD_DIRECTORY)")
	}
	for id, material := range keyMap {
		poolKeys = append(poolKeys, db.KEK{ID: id, Material: material})
	}
	return poolKeys, splitAttrs(getStr(cfg, "HTCONDORDB_ENCRYPT_ATTRS")), nil
}

// splitAttrs splits a comma/whitespace-separated attribute list from configuration.
func splitAttrs(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	if len(fields) == 0 {
		return nil
	}
	return fields
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
