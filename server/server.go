// Package server is the htcondordb service: it wraps the embedded ClassAd-log
// database (package db) and its RPC server (package dbrpc) behind an HTCondor
// CEDAR command, enforcing HTCondor's READ / WRITE / DAEMON authorization on
// every connection.
//
// One authenticated CEDAR connection carries an entire dbrpc multiplex. The
// access level is decided once, at connection time, from the authenticated
// identity:
//
//   - READ  -> read-only, and every ad returned has its private (secret)
//     attributes stripped (claim ids, capabilities, transfer keys).
//   - WRITE -> full read/write, private attributes visible.
//   - DAEMON -> WRITE plus the HA/replication surface (separate commands).
//
// The level is recomputed per connection, so a reconfigure that changes the
// ALLOW_/DENY_ tables takes effect on the next connection without a restart.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/htcondordb/command"
)

// Authorizer reports whether an authenticated peer is allowed at an HTCondor
// authorization level. It has the same shape as cedarserver.Server.Authorizer
// (and authz.Policy.Authorize): perm is a DCpermission name ("READ", "WRITE",
// "DAEMON"), peerAddr is the peer's "host:port", user is the mapped FQU.
type Authorizer func(perm, peerAddr, user string) bool

// Level is the effective access a connection was authorized at.
type Level int

const (
	// LevelRead is read-only access with private attributes stripped.
	LevelRead Level = iota
	// LevelWrite is full read/write access.
	LevelWrite
	// LevelDaemon is WRITE plus the daemon/HA control surface.
	LevelDaemon
)

func (l Level) String() string {
	switch l {
	case LevelDaemon:
		return "DAEMON"
	case LevelWrite:
		return "WRITE"
	default:
		return "READ"
	}
}

// Config configures a Service.
type Config struct {
	// Dir is the database directory (the ClassAd log lives here). Empty means
	// an ephemeral in-memory database.
	Dir string

	// Authorize escalates an authenticated connection's level: after the CEDAR
	// server has gated the command at READ, the handler probes WRITE then DAEMON
	// on the peer's identity. Pass authz.Policy.Authorize. Required.
	Authorize Authorizer

	// ForceReadOnly makes every client connection read-only regardless of its
	// authorization level. A leader-follower replica sets this: writes must go
	// to the leader, and the replica applies them from the commit stream, not
	// from clients. Private-attribute visibility still follows the level.
	ForceReadOnly bool

	// LogQueries turns on a per-query log: every streamed query is logged (op,
	// table, constraint, LIMIT, rows returned, duration). It is opt-in and off by
	// default. Useful for spotting an expensive query pattern -- e.g. a client
	// that fetches every attribute of every ad (a full scan with a large row
	// count) instead of pushing a projection/limit down.
	LogQueries bool

	// DefaultTable is the table ensured to exist at startup and targeted by
	// clients that do not name a table. Defaults to "ads".
	DefaultTable string

	// MemoryTables are tables ensured to exist at startup as RAM-only even when
	// the catalog is persistent (Dir set): their data is never written to disk and
	// is gone after a restart (the table reappears empty). Intended for high-churn,
	// reconstructible data -- e.g. frequently-replaced ads that are re-advertised
	// every cycle -- where persistence is pure write amplification. Names already
	// present (e.g. the default table) keep their existing on-disk backing.
	MemoryTables []string

	// PoolKeys enables encryption at rest: every table's master key is wrapped under
	// these HTCondor pool/signing keys (any one opens the DB; a rotated-in key is added
	// on the next start). Built from htcondor.LoadSigningKeys. Empty disables encryption.
	// Each replica holds its OWN keys -- encryption is node-local and never replicated
	// (the commit stream ships decrypted, privilege-stripped ads), so a follower's keys
	// need not match the leader's.
	PoolKeys []db.KEK
	// EncryptedAttrs is the default set of attributes encrypted at rest, in addition to
	// HTCondor private attributes (which are always encrypted when PoolKeys is set).
	// Adjustable at runtime by a DAEMON via the encrypt.set meta-command.
	EncryptedAttrs []string

	// Logger receives per-connection diagnostics. Defaults to slog.Default().
	Logger *slog.Logger

	// DisableMaintenance turns off the background self-tuning loop (index auto-tune,
	// hot-set refresh, dictionary retrain). Off by default: maintenance runs.
	DisableMaintenance bool
	// MaintenanceInterval is the cadence of the maintenance loop. Default 15 minutes.
	MaintenanceInterval time.Duration
	// Maintenance overrides the maintenance pass options. The zero value uses sensible
	// defaults (hot-set refresh + dictionary retrain + a 10%-of-data index budget).
	Maintenance *db.MaintainOptions
}

// Service is the htcondordb database service. It serves a catalog of named
// tables; the default table always exists.
type Service struct {
	cat          *db.Catalog
	rpc          *dbrpc.Server
	defaultTable string
	authorize    Authorizer
	forceReadOn  bool
	logQueries   bool
	log          *slog.Logger
}

// New opens the table catalog and builds the service. The caller owns the
// returned Service and must Close it.
func New(cfg Config) (*Service, error) {
	if cfg.Authorize == nil {
		return nil, fmt.Errorf("server: an Authorize function is required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	defaultTable := cfg.DefaultTable
	if defaultTable == "" {
		defaultTable = dbrpc.DefaultTable
	}

	cat, err := db.OpenCatalogConfig(db.CatalogConfig{
		Dir:            cfg.Dir,
		PoolKeys:       cfg.PoolKeys,
		EncryptedAttrs: cfg.EncryptedAttrs,
	})
	if err != nil {
		return nil, fmt.Errorf("server: opening catalog: %w", err)
	}
	if _, err := cat.EnsureTable(defaultTable); err != nil {
		_ = cat.Close()
		return nil, fmt.Errorf("server: ensuring default table: %w", err)
	}
	for _, name := range cfg.MemoryTables {
		if _, err := cat.CreateTableOpts(name, db.TableOptions{InMemory: true}); err != nil {
			_ = cat.Close()
			return nil, fmt.Errorf("server: ensuring in-memory table %q: %w", name, err)
		}
	}

	svc := &Service{
		cat:          cat,
		rpc:          dbrpc.NewServerCatalog(cat),
		defaultTable: defaultTable,
		authorize:    cfg.Authorize,
		forceReadOn:  cfg.ForceReadOnly,
		logQueries:   cfg.LogQueries,
		log:          log,
	}
	// Background self-tuning (index auto-tune + hot-set refresh + dictionary retrain),
	// unless disabled. Stopped by Close (via rpc.Close).
	if !cfg.DisableMaintenance {
		opts := db.MaintainOptions{HotTopN: 32, Retrain: true, MinIndexDemand: 10, IndexBudgetHighFrac: 0.10}
		if cfg.Maintenance != nil {
			opts = *cfg.Maintenance
		}
		svc.rpc.StartMaintenance(cfg.MaintenanceInterval, opts)
	}
	return svc, nil
}

// Catalog returns the table catalog.
func (s *Service) Catalog() *db.Catalog { return s.cat }

// DB returns the default table's database (for HA layers that stream commits or
// drive snapshots). Multi-table HA is not yet wired; HA operates on the default
// table. The caller must not close it; Service.Close owns its lifetime.
func (s *Service) DB() *db.DB {
	d, _ := s.cat.Table(s.defaultTable)
	return d
}

// RPC returns the underlying dbrpc server (for HA layers that serve replica
// connections). Its lifetime is owned by Service.
func (s *Service) RPC() *dbrpc.Server { return s.rpc }

// RegisterOn wires the DB session command onto a CEDAR command server. It is
// registered at READ: the server's Authorizer admits any authorized reader, and
// the handler escalates to WRITE/DAEMON per identity.
func (s *Service) RegisterOn(srv *cedarserver.Server) {
	srv.Handle(command.DBSession, s.handleSession, "READ")
}

// handleSession serves one authenticated CEDAR connection as a dbrpc multiplex,
// scoped to the connection's effective authorization level.
func (s *Service) handleSession(ctx context.Context, c *cedarserver.Conn) error {
	level := s.effectiveLevel(c)
	opts := serveOptionsFor(level)
	if s.forceReadOn {
		opts.ReadOnly = true
	}
	if s.logQueries {
		user := peerUser(c)
		remote := c.RemoteAddr
		opts.QueryLog = func(q dbrpc.QueryLog) {
			s.log.Info("htcondordb query",
				"op", q.Op, "table", q.Table, "constraint", q.Constraint,
				"limit", q.Limit, "rows", q.Rows, "duration", q.Duration,
				"user", user, "remote", remote)
		}
	}

	// Logged at Info so an operator can see, per connection, the identity that
	// authenticated and the access level it was granted -- the answer to "why am
	// I read-only?" (the mapped user was not authorized for WRITE, or the daemon
	// is a read-only replica).
	s.log.Info("htcondordb session opened",
		"remote", c.RemoteAddr, "user", peerUser(c), "level", level.String(),
		"read_only", opts.ReadOnly, "private_visible", opts.IncludePrivate)

	conn := dbrpc.NewCedarConn(ctx, c.Stream)
	err := s.rpc.ServeConnOpts(conn, opts)
	s.log.Debug("htcondordb session closed", "remote", c.RemoteAddr, "err", errString(err))
	return err
}

// serveOptionsFor maps an access level to the dbrpc serving options.
func serveOptionsFor(level Level) dbrpc.ServeOptions {
	switch level {
	case LevelDaemon:
		// Only a DAEMON-authorized peer sees private (secret) attributes -- matching the
		// HTCondor daemons, where secret material is a DAEMON-level capability, not WRITE.
		// DAEMON also carries the privileged admin surface (e.g. the encryption toggle).
		return dbrpc.ServeOptions{ReadOnly: false, IncludePrivate: true, Privileged: true}
	case LevelWrite:
		// WRITE may read and write ads but does NOT see private attributes: a submitter
		// can add/update jobs without gaining visibility into other principals' secrets.
		return dbrpc.ServeOptions{ReadOnly: false, IncludePrivate: false}
	default: // LevelRead
		return dbrpc.ServeOptions{ReadOnly: true, IncludePrivate: false}
	}
}

// effectiveLevel returns the highest level the authenticated peer holds. The
// command was already gated at READ by the server's Authorizer, so a peer that
// reaches here holds at least READ; we escalate by probing WRITE then DAEMON.
func (s *Service) effectiveLevel(c *cedarserver.Conn) Level {
	user := peerUser(c)
	addr := c.RemoteAddr
	switch {
	case s.authorize("DAEMON", addr, user):
		return LevelDaemon
	case s.authorize("WRITE", addr, user):
		return LevelWrite
	default:
		return LevelRead
	}
}

// Close stops background work and closes the database.
func (s *Service) Close() error {
	s.rpc.Close()
	return s.cat.Close()
}

// peerUser is the authenticated fully-qualified user, or "" for an
// unauthenticated/raw peer.
func peerUser(c *cedarserver.Conn) string {
	if c.Negotiation != nil {
		return c.Negotiation.User
	}
	return ""
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
