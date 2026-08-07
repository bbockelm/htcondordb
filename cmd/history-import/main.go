// Command history-import pulls completed-job history from every schedd in a pool
// (selected by a requirements expression) into an htcondordb archive table, via
// remote condor_history. It is the out-of-process runner for one configured import
// job; the htcondordb daemon's importer manager launches and supervises it, though
// it also runs standalone.
//
//	history-import run -name <job> [-addr HOST:PORT] [-cursor-file PATH] [-debug]
//
// The named job's settings come from CONDOR_CONFIG
// (HTCONDORDB_HISTORY_IMPORT_<NAME>_*). Pool discovery and history queries
// authenticate from that same ambient config; the DB write connection resumes the
// CEDAR session the manager hands down via CONDOR_PRIVATE_INHERIT (or authenticates
// normally when run by hand).
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

	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/historyimport"
)

// dbSessionCommand is the CEDAR command for htcondordb's multiplexed dbrpc
// session (command.DBSession). Duplicated here so this command need not import the
// htcondordb command package, matching kafkasync.
const dbSessionCommand = 74000

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "history-import:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return usage()
	}
	switch os.Args[1] {
	case "run":
		return cmdRun(os.Args[2:])
	case "-h", "--help", "help":
		return usage()
	default:
		return fmt.Errorf("unknown subcommand %q (want run)", os.Args[1])
	}
}

func usage() error {
	fmt.Fprint(os.Stderr, `history-import — pull schedd history from a pool into htcondordb

  history-import run -name <job> [-addr HOST:PORT] [-cursor-file PATH] [-debug]

The job's settings come from CONDOR_CONFIG (HTCONDORDB_HISTORY_IMPORT_<NAME>_*).
-addr defaults to HTCONDORDB_ADDRESS_FILE / $(LOG)/.htcondordb_address / HTCONDORDB_HOST.
-cursor-file defaults to $(LOG)/history-import-<name>.cursors.json.
`)
	return nil
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	addr := fs.String("addr", "", "htcondordb address (host:port)")
	name := fs.String("name", "", "import job name (required)")
	cursorFile := fs.String("cursor-file", "", "path to the durable cursor file")
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

	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Resolve this job from the ambient config.
	jobs, err := historyimport.JobsFromConfig(cfg.Get)
	if err != nil {
		return err
	}
	var job historyimport.Job
	found := false
	for _, j := range jobs {
		if strings.EqualFold(j.Name, *name) {
			job, found = j, true
			break
		}
	}
	if !found {
		return fmt.Errorf("no import job named %q in HTCONDORDB_HISTORY_IMPORT", *name)
	}

	cursorPath := *cursorFile
	if cursorPath == "" {
		cursorPath = defaultCursorFile(cfg, job.Name)
	}
	cursors, err := historyimport.NewFileCursors(cursorPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The DB write connection is re-dialed by the outer loop so a transient DB
	// outage backs off and reconnects rather than killing the importer.
	backoff := time.Second
	for ctx.Err() == nil {
		err := runJob(ctx, cfg, *addr, job, cursors, log)
		if err == nil || ctx.Err() != nil {
			break // clean shutdown
		}
		log.Warn("history-import: stopped; reconnecting", "job", job.Name, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return nil
}

// runJob dials the DB, builds the importer, and runs the job's loop until the DB
// connection drops (returns the error to reconnect) or ctx is cancelled (nil).
func runJob(ctx context.Context, cfg *config.Config, addrFlag string, job historyimport.Job, cursors historyimport.Cursors, log *slog.Logger) error {
	client, cleanup, err := dial(ctx, cfg, addrFlag)
	if err != nil {
		return err
	}
	defer cleanup()

	im := &historyimport.Importer{
		Disc: historyimport.CollectorDiscovery{},
		Src:  historyimport.ScheddHistorySource{},
		W:    &historyimport.DBWriter{Client: client},
		Cur:  cursors,
		Log:  log,
	}
	log.Info("history-import: starting", "job", job.Name, "pool", job.Pool, "table", job.Table, "interval", job.Interval)
	return im.RunLoop(ctx, job)
}

// dial connects to the htcondordb daemon, resuming the manager-supplied inherited
// session when present, else authenticating normally.
func dial(ctx context.Context, cfg *config.Config, addrFlag string) (*dbrpc.Client, func(), error) {
	addr, err := resolveAddr(cfg, addrFlag)
	if err != nil {
		return nil, nil, err
	}
	sec, err := htcondor.GetSecurityConfig(cfg, dbSessionCommand, "CLIENT")
	if err != nil {
		return nil, nil, fmt.Errorf("building client security config: %w", err)
	}
	sec.Command = dbSessionCommand
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}
	if sid := security.GetParentSessionID(); sid != "" {
		sec.SessionID = sid
		sec.SessionCache = security.GetSessionCache()
	}
	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, addr, sec)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to htcondordb at %s: %w", addr, err)
	}
	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	return dbc, func() { _ = dbc.Close(); _ = cl.Close() }, nil
}

// resolveAddr finds the daemon address: the flag, then HTCONDORDB_ADDRESS_FILE,
// then $(LOG)/.htcondordb_address, then HTCONDORDB_HOST.
func resolveAddr(cfg *config.Config, addrFlag string) (string, error) {
	if addrFlag != "" {
		return addrFlag, nil
	}
	addrFile, _ := cfg.Get("HTCONDORDB_ADDRESS_FILE")
	if addrFile == "" {
		if logDir, _ := cfg.Get("LOG"); logDir != "" {
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
	if host, _ := cfg.Get("HTCONDORDB_HOST"); host != "" {
		return host, nil
	}
	return "", fmt.Errorf("cannot locate htcondordb: pass -addr, or set HTCONDORDB_ADDRESS_FILE / HTCONDORDB_HOST")
}

// defaultCursorFile places the cursor file under $(LOG) (writable by the importer's
// service user), one file per job.
func defaultCursorFile(cfg *config.Config, name string) string {
	dir, _ := cfg.Get("LOG")
	if dir == "" {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "history-import-"+name+".cursors.json")
}
