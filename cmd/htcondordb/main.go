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
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/authz"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/locate"
	"github.com/bbockelm/htcondordb/metrics"
	"github.com/bbockelm/htcondordb/server"
)

// version is stamped at build time via `-ldflags "-X main.version=..."` (see the
// Makefile); it is "dev" for a plain `go build`.
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "htcondordb:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":0", "fallback TCP listen address when not inheriting a shared-port endpoint")
	showVersion := flag.Bool("version", false, "print version and exit")
	// condor_master appends these standard DaemonCore flags for a daemon not in
	// its built-in list; accept them so flag.Parse does not reject our launch.
	// -local-name additionally scopes config lookups (HTCONDORDB.<key> beats <key>).
	localName := flag.String("local-name", "", "HTCondor subsystem local-name; passed by condor_master")
	_ = flag.String("sock", "", "HTCondor shared-port endpoint name; accepted for compatibility (fd inherited via CONDOR_INHERIT)")
	flag.Parse()

	if *showVersion {
		fmt.Println("htcondordb", version)
		return nil
	}

	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "HTCONDORDB", LocalName: *localName})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Bootstrap logging and condor_master integration (drops privileges to the
	// condor user when started as root).
	d, err := daemon.New(daemon.Options{Subsys: "HTCONDORDB", LocalName: *localName, Config: cfg})
	if err != nil {
		return err
	}
	log := d.Logger()
	slog.SetDefault(d.Slog()) // route cedar's server/security slog into our log

	// Time each startup phase: everything from here to the listener used to be silent, so a slow
	// start showed only as a gap between two timestamps.
	boot := newStartupTimer(log)

	// Explain a slow reopen: log when a table's/archive's sealed segments were NOT all
	// index-adopted at open (classad #208), so the rebuild that dominates a slow startup names
	// its cause (no sidecar file / rejected sidecar / stale attribute section) instead of showing
	// only as elapsed time. Silent on a clean fast open (every segment adopted). Set once, before
	// the catalog opens below.
	collections.OpenIndexDiagHook = func(dg collections.OpenIndexDiag) {
		if dg.AttrIndexAdopted < dg.SealedSegments {
			log.Info(logging.DestinationGeneral, "columnar index adoption at open",
				"dir", dg.Dir, "sealed", dg.SealedSegments, "sidecarFiles", dg.SidecarFiles,
				"keyAdopted", dg.KeyIndexAdopted, "attrAdopted", dg.AttrIndexAdopted,
				"reasons", fmt.Sprint(dg.Reasons))
		}
		// Name the phase that dominated a slow open (classad #214). When every sidecar adopts
		// cleanly the adoption line above stays silent, so a slow reopen would otherwise show only
		// as one elapsed number; this breaks it into phases. Thresholded so a fast open is quiet.
		if t := dg.Timing; openTimingTotal(t) >= time.Second {
			ms := func(d time.Duration) string { return d.Round(time.Millisecond).String() }
			log.Info(logging.DestinationGeneral, "table open phase timing",
				"dir", dg.Dir, "sealed", dg.SealedSegments,
				"mapSegments", ms(t.MapSegments), "dirRestore", ms(t.DirRestore), "dirRebuilt", t.DirRebuilt,
				"publishDict", ms(t.PublishDict), "publishColumns", ms(t.PublishColumns),
				"loadSidecars", ms(t.LoadSidecars), "zoneRecompute", ms(t.ZoneRecompute),
				"reindex", ms(t.Reindex), "rebuildOrdered", ms(t.RebuildOrdered),
				"adoptSchema", ms(t.AdoptSchema), "loadDicts", ms(t.LoadDicts), "loadDemand", ms(t.LoadDemand))
		}
	}

	// Server-side security policy for our command socket (SEC_* knobs). The
	// negotiated command is DBSession; DAEMON is the strongest level we serve.
	sec, err := htcondor.GetServerSecurityConfig(d.Config(), command.DBSession, "DAEMON")
	if err != nil {
		return fmt.Errorf("building security config: %w", err)
	}
	boot.mark("security-config")
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
	boot.mark("authz-policy")
	policyPtr.Store(policy)
	authorize := func(perm, peerAddr, user string) bool {
		// A CEDAR family/parent session is a pre-shared key this daemon minted and handed only
		// to its own supervised children over a private channel (CONDOR_PRIVATE_INHERIT);
		// possessing it proves a trusted, daemon-managed identity. Grant it DAEMON so a
		// supervised exporter child can reach the DAEMON-gated dbrpc surface without the
		// operator having to list condor@parent in ALLOW_DAEMON. An attacker cannot present
		// this identity without the session key, which never leaves the child's environment.
		if perm == "DAEMON" && (user == "condor@parent" || user == "condor@family") {
			return true
		}
		return policyPtr.Load().Authorize(perm, peerAddr, user)
	}
	srv.Authorizer = authorize

	// Fully-qualify a bare authenticated identity with the local domain, mirroring
	// the C++ FS authenticator (condor_auth_fs.cpp setRemoteDomain(getLocalDomain())):
	// FS auth yields a bare username, but ALLOW_<PERM> entries of the form "user@host"
	// are matched against the *fully-qualified* user ("user@domain"), so without this
	// a local FS-authenticated peer could never match a user rule and would fall back
	// to read-only. An anonymous peer (empty identity) and an already-qualified
	// identity (one containing '@', e.g. from token/SSL auth) are left untouched.
	if dom := localUIDDomain(cfg); dom != "" {
		srv.FQUMapper = func(authUser, _ string) string {
			if authUser == "" || strings.ContainsRune(authUser, '@') {
				return "" // keep as-is: anonymous, or already fully-qualified
			}
			return authUser + "@" + dom
		}
	}

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
	boot.mark("server-setup")
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
	boot.mark("ha-detect")
	logQueries := configBool(cfg, "HTCONDORDB_LOG_QUERIES")
	memoryTables := splitAttrs(getStr(cfg, "HTCONDORDB_MEMORY_TABLES"))
	// Take an exclusive lock on the database directory before opening it, so a second daemon
	// pointed at the same dir (e.g. a restart that overlaps the old process, or a misconfigured
	// second instance) refuses to start rather than double-opening the mmap'd segments and
	// corrupting them. No-op for an in-memory database (empty dir).
	dbDir := databaseDir(d, cfg)
	unlockDB, err := lockDatabaseDir(dbDir)
	if err != nil {
		return err
	}
	defer unlockDB()
	svc, err := server.New(server.Config{
		OnPhase:         boot.record,
		OnTableOpen:     boot.recordTableOpen,
		OnSealMigration: boot.recordSealMigration,
		Dir:             dbDir,
		Authorize:       authorize,
		ForceReadOnly:   ha.forceReadOnly,
		Logger:          d.Slog(),
		LogQueries:      logQueries,
		MemoryTables:    memoryTables,
		PoolKeys:        poolKeys,
		EncryptedAttrs:  encAttrs,
	})
	if err != nil {
		return err
	}
	defer func() { _ = svc.Close() }()
	if logQueries {
		log.Info(logging.DestinationGeneral, "per-query logging enabled (HTCONDORDB_LOG_QUERIES)")
	}
	if len(memoryTables) > 0 {
		log.Info(logging.DestinationGeneral, "in-memory (non-persistent) tables configured",
			"tables", strings.Join(memoryTables, ","))
	}

	// Restore-on-startup (disaster recovery): if HTCONDORDB_RESTORE_FILE names an existing
	// snapshot, load it before serving, then the file is moved aside so a restart serves
	// live data. An encrypted snapshot is opened with this daemon's pool keys.
	boot.mark("open-database")

	if restoreFile := getStr(cfg, "HTCONDORDB_RESTORE_FILE"); restoreFile != "" {
		if restored, rerr := svc.RestoreOnStartup(restoreFile); rerr != nil {
			return fmt.Errorf("restore-on-startup from %s: %w", restoreFile, rerr)
		} else if restored {
			log.Info(logging.DestinationGeneral, "restored database from snapshot", "file", restoreFile)
		}
	}

	boot.mark("restore-snapshot")

	svc.RegisterOn(srv)

	// DC_NOP / DC_RECONFIG / DC_OFF so condor_ping, condor_reconfig -daemon and
	// condor_off -daemon work against this daemon's command port.
	boot.mark("register-commands")
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
	boot.mark("listener")
	boot.done()
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

	// Enforce archive (history) table retention periodically (default hourly; 0 disables).
	// A no-op until an archive table exists.
	rotSecs := configInt(cfg, "HTCONDORDB_ARCHIVE_ROTATE_INTERVAL")
	if _, set := cfg.Get("HTCONDORDB_ARCHIVE_ROTATE_INTERVAL"); !set {
		rotSecs = 3600
	}
	// An archive's compression dictionary is retrained on the same maintenance boundary (default
	// daily; 0 disables). It is adopted lazily -- new writes use it, existing segments keep reading
	// on the dictionary they were written under -- so this costs a sample pass, not a reseal.
	retrainSecs := int(server.DefaultArchiveRetrainInterval / time.Second)
	if _, set := cfg.Get("HTCONDORDB_ARCHIVE_RETRAIN_INTERVAL"); set {
		retrainSecs = configInt(cfg, "HTCONDORDB_ARCHIVE_RETRAIN_INTERVAL")
	}
	if rotSecs > 0 {
		go svc.RunPeriodicArchiveMaintenanceEvery(ctx,
			time.Duration(rotSecs)*time.Second, time.Duration(retrainSecs)*time.Second)
	}

	// Schedd-sync mode: mirror a schedd's job_queue.log into a "jobs" table and its
	// history file into a "history" archive table (a read model of the schedd's state).
	// The manager (re)starts the tailers so a JOB_QUEUE_LOG / HISTORY /
	// HTCONDORDB_SYNC_SCHEDD change takes effect on condor_reconfig without a restart.
	syncMgr := &scheddSyncManager{parent: ctx, svc: svc, logger: d.Slog()}
	// Stop the tailers (and WAIT for them) before svc.Close runs: a tailer
	// mid-commit writes the collections' mmap'd segments, which Close munmaps --
	// racing them faults (SIGSEGV in segment.append). Deferred after svc.Close is
	// deferred, so LIFO runs this first. Cancelling ctx alone does not join them.
	defer syncMgr.Stop()
	if serr := syncMgr.apply(cfg); serr != nil {
		return serr
	}
	d.OnReconfig(func(newCfg *config.Config) {
		if serr := syncMgr.apply(newCfg); serr != nil {
			log.Error(logging.DestinationGeneral, "reconfigure: schedd-sync not reapplied", "err", serr.Error())
			return
		}
	})

	// Mirror-out (follower end): regenerate a job_queue.log from the mirrored tables on a cadence,
	// so a follower that replicates a leader's schedd-sync tables re-emits a live, restorable backup
	// log. Reapplied on reconfigure. Off unless HTCONDORDB_MIRROR_JOB_QUEUE_LOG names a destination.
	mirrorMgr := &mirrorOutManager{parent: ctx, svc: svc, logger: d.Slog()}
	if merr := mirrorMgr.apply(cfg); merr != nil {
		return merr
	}
	d.OnReconfig(func(newCfg *config.Config) {
		if merr := mirrorMgr.apply(newCfg); merr != nil {
			log.Error(logging.DestinationGeneral, "reconfigure: mirror-out not reapplied", "err", merr.Error())
			return
		}
	})

	// Exporter manager: launch + supervise the standalone change-data exporters (kafkasync /
	// opensearchsync) as children, so registering an exporter (CreateExporter) is enough for the
	// daemon to run it -- each with a fresh, standalone inherited CEDAR session, as an
	// unprivileged user, restarted with backoff. Enabled by default (HTCONDORDB_MANAGE_EXPORTERS).
	expMgr := &exporterManager{parent: ctx, catalog: svc.Catalog(), logger: d.Slog(), daemonAddr: advertisedAddr(d, ln)}
	if eerr := expMgr.apply(cfg); eerr != nil {
		return eerr
	}
	d.OnReconfig(func(newCfg *config.Config) {
		if eerr := expMgr.apply(newCfg); eerr != nil {
			log.Error(logging.DestinationGeneral, "reconfigure: exporter manager not reapplied", "err", eerr.Error())
			return
		}
	})

	// Native-CEDAR fan-in replicators (cedarsync): mirror configured source htcondordbs'
	// tables/archives into local targets, selectively and Src-stamped, over a normal DBSession.
	// Reapplied on reconfigure. Off unless HTCONDORDB_REPLICATE_SOURCES is set.
	cedarMgr := &cedarSyncManager{parent: ctx, cat: svc.Catalog(), logger: d.Slog()}
	// Like syncMgr, an in-process writer: stop-and-wait before svc.Close munmaps.
	defer cedarMgr.Stop()
	if cerr := cedarMgr.apply(cfg); cerr != nil {
		return cerr
	}
	d.OnReconfig(func(newCfg *config.Config) {
		if cerr := cedarMgr.apply(newCfg); cerr != nil {
			log.Error(logging.DestinationGeneral, "reconfigure: cedar-sync not reapplied", "err", cerr.Error())
		}
	})

	// History importers (historyimport): pull completed-job history from every schedd of a pool
	// (remote condor_history) into an archive table, one supervised subprocess per configured job,
	// running as a service account with pool credentials. Reapplied on reconfigure. Off unless
	// HTCONDORDB_HISTORY_IMPORT names jobs.
	impMgr := newImporterManager(ctx, d.Slog(), advertisedAddr(d, ln))
	if ierr := impMgr.apply(cfg); ierr != nil {
		return ierr
	}
	d.OnReconfig(func(newCfg *config.Config) {
		if ierr := impMgr.apply(newCfg); ierr != nil {
			log.Error(logging.DestinationGeneral, "reconfigure: importer manager not reapplied", "err", ierr.Error())
		}
	})

	// Serve the transport-neutral HTTP/SSE change feed for external, non-CEDAR sinks (opt-in via
	// HTCONDORDB_CHANGEFEED_ADDRESS, token-gated). No-op when unset.
	startChangeFeed(ctx, d, cfg, svc)

	// Administrative sync control (DBSyncControl): let an operator resync a schedd-sync tailer
	// (jobs/history) or a managed exporter without a restart -- e.g. `.resync jobs` to heal a
	// mirror from the current log, or `.resync <exporter>` to re-export from the start. Registered
	// in every mode (unlike the HA-only DBControl).
	registerSyncControl(srv, syncMgr, expMgr)

	// Advertise a discovery/monitoring ClassAd to the collector: agents (and the htcondor-api
	// MCP) discover this database and its command address here, and the ad doubles as a metrics
	// sink carrying per-table storage gauges and per-source sync health -- scrapable via the
	// collector even when the daemon's own /metrics endpoint is off.
	// The collector ad and /metrics report every sync source: the schedd-sync tailers (in)
	// and the mirror-out (out).
	syncSources := func() []dbad.StatusSource {
		return append(syncMgr.Sources(), mirrorMgr.Sources()...)
	}
	startCollectorAdvertise(ctx, d, cfg, svc, advertisedAddr(d, ln), syncSources, expMgr.Statuses, impMgr.Statuses)

	// Start any background HA machinery (a follower's replicator, or the raft
	// coordinator and its command handlers in consistent mode).
	defer ha.close()
	if err := ha.start(ctx, d, cfg, svc, srv, advertisedAddr(d, ln)); err != nil {
		return err
	}

	// Prometheus /metrics endpoint (opt-in via HTCONDORDB_METRICS_ADDRESS, e.g.
	// ":9095"): a plain HTTP listener exposing per-table storage and operational
	// timing counters, so the database's health is scrapable directly (not only via a
	// collector in front of it). It carries no secrets -- storage sizes and timings --
	// but bind it to a trusted interface.
	if addr := getStr(cfg, "HTCONDORDB_METRICS_ADDRESS"); addr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(svc.Catalog(), syncSources, expMgr.Statuses, impMgr.Statuses))
		// pprof (opt-in via HTCONDORDB_ENABLE_PPROF): profiling endpoints on the same
		// trusted listener, so a memory/CPU anomaly in a live daemon is one
		// `go tool pprof http://.../debug/pprof/heap` away instead of a blind restart.
		// Off by default: profiles expose internals (symbol names, allocation sites)
		// beyond the storage-size counters /metrics carries.
		if configBool(cfg, "HTCONDORDB_ENABLE_PPROF") {
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			log.Info(logging.DestinationGeneral, "pprof profiling endpoints enabled (HTCONDORDB_ENABLE_PPROF)")
		}
		metricsSrv := &http.Server{Addr: addr, Handler: mux}
		go func() {
			<-ctx.Done()
			_ = metricsSrv.Close()
		}()
		go func() {
			log.Info(logging.DestinationGeneral, "serving Prometheus metrics", "address", addr)
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error(logging.DestinationGeneral, "metrics endpoint failed", "err", err.Error())
			}
		}()
	}

	selfVer, classadVer := buildIdentity()
	log.Info(logging.DestinationGeneral, "htcondordb starting",
		"version", selfVer, "classad", classadVer,
		"listen", ln.Addr().String(), "address", advertisedAddr(d, ln),
		"db_dir", databaseDir(d, cfg), "under_master", d.UnderMaster(),
		"ha_mode", ha.mode, "role", ha.role, "read_only", ha.forceReadOnly)

	return d.Serve(ctx, ln, srv.Serve)
}

// openTimingTotal sums an open's sequential phases into a rough wall-clock, for the "is this open
// slow enough to log its breakdown" threshold. ZoneRecompute is excluded because it is a sub-timer
// of LoadSidecars (counting it would double-count).
func openTimingTotal(t collections.OpenTiming) time.Duration {
	return t.MapSegments + t.DirRestore + t.PublishDict + t.PublishColumns + t.LoadSidecars +
		t.Reindex + t.RebuildOrdered + t.AdoptSchema + t.LoadDicts + t.LoadDemand
}

// databaseDir resolves the on-disk database directory: HTCONDORDB_DIR if set,
// else $(SPOOL)/htcondordb. Empty (in-memory) only when neither is configured.
// localUIDDomain returns the domain used to fully-qualify a bare authenticated
// identity, mirroring the C++ Condor_Auth_Base local domain (param("UID_DOMAIN")).
// Empty when UID_DOMAIN is unset, in which case the identity is left bare.
func localUIDDomain(cfg *config.Config) string {
	if v, ok := cfg.Get("UID_DOMAIN"); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// resolveDBDir resolves the on-disk database directory from config alone: HTCONDORDB_DIR if
// set, else $(SPOOL)/htcondordb, else "" (in-memory). It is the single source of truth for
// the DB dir so everything under it -- the catalog, archives, and the schedd-sync position
// store -- lands in the same place regardless of which knob is set.
func resolveDBDir(cfg *config.Config) string {
	if v, ok := cfg.Get("HTCONDORDB_DIR"); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if spool, ok := cfg.Get("SPOOL"); ok && strings.TrimSpace(spool) != "" {
		return filepath.Join(strings.TrimSpace(spool), "htcondordb")
	}
	return ""
}

func databaseDir(d *daemon.Daemon, cfg *config.Config) string {
	dir := resolveDBDir(cfg)
	if dir == "" {
		d.Logger().Warn(logging.DestinationGeneral, "no HTCONDORDB_DIR or SPOOL configured; database is in-memory only")
	}
	return dir
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

// startCollectorAdvertise launches the periodic HTCondorDB ad advertisement to the collector
// (COLLECTOR_HOST). It is a no-op when no collector is configured or when advertisement is
// explicitly disabled (HTCONDORDB_ADVERTISE=false). The ad carries the daemon's command
// address for discovery plus per-table storage gauges and per-source sync health for
// monitoring.
func startCollectorAdvertise(ctx context.Context, d *daemon.Daemon, cfg *config.Config, svc *server.Service, addr string, sourcesFunc func() []dbad.StatusSource, exportersFunc func() []dbad.ExporterStatus, importersFunc func() []dbad.ImporterStatus) {
	if v := getStr(cfg, "HTCONDORDB_ADVERTISE"); strings.TrimSpace(v) != "" && !configBool(cfg, "HTCONDORDB_ADVERTISE") {
		return // explicitly disabled
	}
	// The generic daemon.Advertise owns the whole cycle -- the base ad (PublishAd: identity,
	// version, MonitorSelf*, <SUBSYS>_ATTRS), MyType, the sequence number, DAEMON_SHUTDOWN, the
	// COLLECTOR_HOST list, the HTCONDORDB_UPDATE_INTERVAL cadence, and INVALIDATE-on-shutdown.
	// dbad supplies only the HTCondorDB-specific attributes (table gauges, sync + exporter health,
	// capabilities, reachable address). It is a no-op when COLLECTOR_HOST is empty.
	// Push an early advertise when a sync source catches up or falls behind, so its JobQueue*
	// CaughtUp/Lag attributes reach the collector within seconds instead of at the next
	// HTCONDORDB_UPDATE_INTERVAL. Debounced (see runCaughtUpTrigger) so it cannot flap.
	trigger := make(chan struct{}, 1)
	go runCaughtUpTrigger(ctx, sourcesFunc, trigger)
	go d.Advertise(ctx, daemon.AdvertiseConfig{
		MyType:  dbad.AdType,
		Augment: dbad.Augment(svc.Catalog(), sourcesFunc, exportersFunc, importersFunc, addr),
		Trigger: trigger,
		Logger:  d.Slog(),
	})
}

// scheddSyncGuardEUID enforces that schedd-sync never runs as root: reading the schedd's
// job_queue.log/history privileged is a symlink-following risk. Separated from os.Geteuid
// so it is unit-testable at any privilege level.
func scheddSyncGuardEUID(euid int) error {
	if euid == 0 {
		return fmt.Errorf("schedd-sync refuses to run as root: it would read the schedd's job_queue.log/history privileged; " +
			"ensure the daemon drops to the condor user (do not set DROP_PRIVILEGES=false with HTCONDORDB_SYNC_SCHEDD)")
	}
	return nil
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
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
//
// The path comes from the same resolver clients read, so redirecting the address file --
// including from the environment -- moves both halves together rather than leaving a client
// reading a path nothing writes.
func writeAddressFile(d *daemon.Daemon, cfg *config.Config, ln net.Listener) string {
	path := locate.AddressFilePath(cfg)
	if path == "" {
		return ""
	}
	if err := os.WriteFile(path, []byte("<"+advertisedAddr(d, ln)+">\n"), 0o644); err != nil {
		d.Logger().Warn(logging.DestinationGeneral, "could not write address file", "path", path, "err", err.Error())
		return ""
	}
	return path
}
