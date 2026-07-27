// Command archivedropbox manages and runs htcondordb archive-dropbox exporters. It is a dbrpc
// client: exporter definitions and their resume state live in the database catalog, and this
// process watches an archive and drops compressed tarballs of job records into a directory for an
// out-of-band consumer. It is the only component that writes the dropbox -- the core daemon never
// does.
//
// Usage:
//
//	archivedropbox create -name N -table T -dir /path/to/dropbox [options]
//	archivedropbox list
//	archivedropbox drop -name N
//	archivedropbox run  -name N
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/archivedropbox"
)

// dbSessionCommand is the CEDAR command for htcondordb's multiplexed dbrpc session. It must match
// github.com/bbockelm/htcondordb/command.DBSession (74000); it is duplicated here so this
// standalone module need not depend on the htcondordb daemon module.
const dbSessionCommand = 74000

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "archivedropbox:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return usage()
	}
	switch os.Args[1] {
	case "create":
		return cmdCreate(os.Args[2:])
	case "drop":
		return cmdDrop(os.Args[2:])
	case "list":
		return cmdList(os.Args[2:])
	case "run":
		return cmdRun(os.Args[2:])
	case "-h", "--help", "help":
		return usage()
	default:
		return fmt.Errorf("unknown subcommand %q (want create|drop|list|run)", os.Args[1])
	}
}

func usage() error {
	fmt.Fprint(os.Stderr, `archivedropbox -- manage and run htcondordb archive-dropbox exporters

  archivedropbox create -name N -table T -dir /path/to/dropbox [options]
  archivedropbox list
  archivedropbox drop -name N
  archivedropbox run  -name N

Common: -addr HOST:PORT (else HTCONDORDB_ADDRESS_FILE / LOG/.htcondordb_address / HTCONDORDB_HOST).
`)
	return nil
}

// --- shared connection plumbing (mirrors opensearchsync / kafkasync) ---

func loadConfig() (*config.Config, error) {
	return config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
}

func getConfig(cfg *config.Config, key string) string {
	v, _ := cfg.Get(key)
	return strings.TrimSpace(v)
}

func locateDaemon(cfg *config.Config, addrFlag string) (string, error) {
	if addrFlag != "" {
		return addrFlag, nil
	}
	addrFile := getConfig(cfg, "HTCONDORDB_ADDRESS_FILE")
	if addrFile == "" {
		if logDir := getConfig(cfg, "LOG"); logDir != "" {
			addrFile = filepath.Join(logDir, ".htcondordb_address")
		}
	}
	if addrFile != "" {
		if data, err := os.ReadFile(addrFile); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if line = strings.TrimSpace(line); line != "" {
					return line, nil
				}
			}
		}
	}
	if host := getConfig(cfg, "HTCONDORDB_HOST"); host != "" {
		return host, nil
	}
	return "", fmt.Errorf("cannot locate htcondordb: pass -addr, or set HTCONDORDB_ADDRESS_FILE / HTCONDORDB_HOST")
}

func connect(ctx context.Context, cfg *config.Config, addr string) (*dbrpc.Client, func(), error) {
	sec, err := htcondor.GetSecurityConfig(cfg, dbSessionCommand, "CLIENT")
	if err != nil {
		return nil, nil, fmt.Errorf("building client security config: %w", err)
	}
	sec.Command = dbSessionCommand
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}
	// If the htcondordb daemon's exporter manager launched us, it handed us a dedicated, standalone
	// CEDAR session via CONDOR_PRIVATE_INHERIT. Resume that session by id instead of a full
	// handshake -- the daemon trusts the minted identity (condor@parent) as DAEMON.
	if sid := security.GetParentSessionID(); sid != "" {
		sec.SessionID = sid
		sec.SessionCache = security.GetSessionCache()
	}
	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, addr, sec)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to %s: %w", addr, err)
	}
	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	return dbc, func() { _ = dbc.Close(); _ = cl.Close() }, nil
}

func dial(ctx context.Context, addrFlag string) (*dbrpc.Client, func(), error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("loading config: %w", err)
	}
	addr, err := locateDaemon(cfg, addrFlag)
	if err != nil {
		return nil, nil, err
	}
	return connect(ctx, cfg, addr)
}

// --- subcommands ---

func cmdCreate(args []string) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	addr := fs.String("addr", "", "htcondordb address (host:port)")
	name := fs.String("name", "", "exporter name (required)")
	table := fs.String("table", "", "source archive to export, e.g. history (required)")
	dir := fs.String("dir", "", "dropbox directory to drop tarballs into (required)")
	rollJobs := fs.Int("roll-jobs", 0, "roll a tarball every N records (0=default 2000)")
	rollInterval := fs.Duration("roll-interval", 0, "roll a partial tarball at least this often (0=default 10m)")
	maxBytes := fs.String("max-bytes", "", "backpressure ceiling, e.g. 2GiB (0=default 2GiB)")
	level := fs.Int("compression-level", 0, "gzip level -1..9 (0=default 6)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *table == "" || *dir == "" {
		return errors.New("create: -name, -table, and -dir are all required")
	}
	cfg := archivedropbox.Config{
		Table:            *table,
		Directory:        *dir,
		RollJobs:         *rollJobs,
		RollInterval:     archivedropbox.Duration(*rollInterval),
		CompressionLevel: *level,
	}
	if *maxBytes != "" {
		bs, err := archivedropbox.ParseByteSize(*maxBytes)
		if err != nil {
			return err
		}
		cfg.MaxDropboxBytes = bs
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	raw, err := cfg.Marshal()
	if err != nil {
		return err
	}

	ctx := context.Background()
	c, cleanup, err := dial(ctx, *addr)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := c.CreateExporter(ctx, db.ExporterDef{Name: *name, Kind: archivedropbox.Kind, Config: raw}); err != nil {
		return err
	}
	fmt.Printf("created dropbox exporter %q (archive %q -> %q)\n", *name, *table, cfg.Directory)
	return nil
}

func cmdDrop(args []string) error {
	fs := flag.NewFlagSet("drop", flag.ContinueOnError)
	addr := fs.String("addr", "", "htcondordb address (host:port)")
	name := fs.String("name", "", "exporter name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("drop: -name required")
	}
	ctx := context.Background()
	c, cleanup, err := dial(ctx, *addr)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := c.DropExporter(ctx, *name); err != nil {
		return err
	}
	fmt.Printf("dropped exporter %q\n", *name)
	return nil
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	addr := fs.String("addr", "", "htcondordb address (host:port)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	c, cleanup, err := dial(ctx, *addr)
	if err != nil {
		return err
	}
	defer cleanup()
	infos, err := c.ListExporters(ctx)
	if err != nil {
		return err
	}
	if len(infos) == 0 {
		fmt.Println("no exporters")
		return nil
	}
	for _, in := range infos {
		fmt.Printf("%-24s %s\n", in.Name, in.Kind)
	}
	return nil
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	addr := fs.String("addr", "", "htcondordb address (host:port)")
	name := fs.String("name", "", "exporter name (required)")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("run: -name required")
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	backoff := time.Second
	for ctx.Err() == nil {
		if err := runOnce(ctx, *addr, *name, log); err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Warn("archivedropbox: exporter stopped; reconnecting", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		break
	}
	return nil
}

// runOnce dials, resolves the exporter definition, builds the dropbox writer, and runs the
// exporter until it stops. A returned error means the outer loop reconnects; nil is clean shutdown.
func runOnce(ctx context.Context, addrFlag, name string, log *slog.Logger) error {
	c, cleanup, err := dial(ctx, addrFlag)
	if err != nil {
		return err
	}
	defer cleanup()

	def, ok, err := c.GetExporter(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no exporter named %q", name)
	}
	cfg, err := archivedropbox.ParseConfig(def.Config)
	if err != nil {
		return fmt.Errorf("exporter %q: %w", name, err)
	}
	w, err := archivedropbox.NewWriter(cfg)
	if err != nil {
		return err
	}
	log.Info("archivedropbox: starting exporter", "name", name, "table", cfg.Table, "dir", cfg.Directory)
	runner := archivedropbox.NewRunner(name, cfg, c, w, log)
	runner.MaxConsecutiveFailures = 20 // give up so this loop re-dials with a fresh connection
	return runner.Run(ctx)
}
