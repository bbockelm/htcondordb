// Command htcondordb-cli is an interactive SQL-like shell for an htcondordb
// daemon. It connects over CEDAR (authenticated and encrypted like any HTCondor
// client), then accepts SELECT / INSERT / UPDATE / DELETE against the ClassAd
// store. See package repl for the language.
//
// Usage:
//
//	htcondordb-cli                       # interactive, auto-locate the daemon
//	htcondordb-cli -addr '<host:port>'   # interactive against a specific daemon
//	htcondordb-cli -e "SELECT COUNT(*) FROM ads"   # one-shot
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"

	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/ha/consistent"
	"github.com/bbockelm/htcondordb/repl"
)

const usageText = `htcondordb-cli - interactive SQL-like shell and loader for an htcondordb daemon.

Usage:
  htcondordb-cli [flags]                 start an interactive shell
  htcondordb-cli [flags] -e "<sql>"      run one statement and exit
  htcondordb-cli [flags] load [-key A]   load a ClassAd stream from stdin

Flags:
  -addr <host:port>   daemon address (default: HTCONDORDB_ADDRESS_FILE / HTCONDORDB_HOST)
  -e <sql>            execute one statement, print the result, and exit
  -format <mode>      output format for -e: table (default) | json | classad | classad-new
  -key-attr <name>    attribute holding each row's primary key (default: Key)
  -consistent         route writes through the raft cluster (consistent HA mode)
  -debug              log at DEBUG (default is WARNING; quiets library chatter)
  -h, -help           show this help

load subcommand:
  condor_status -long        | htcondordb-cli load -table machines
  condor_status -any -long   | htcondordb-cli load -auto        # route by MyType
  condor_q -global -long     | htcondordb-cli load -table jobs -key GlobalJobId
  -table <name>       target table (default: ads); created if absent
  -auto               route each ad to a table named for its MyType
  -key <attr>         source attribute used as the primary key (default: Name)

  -version            print version and exit

Interactive meta-commands: .tables .use .help .format .output .quit
  (type .help in the shell)

Language: multiple tables (no joins). SELECT / INSERT / UPDATE / DELETE with
DISTINCT, aggregates (COUNT/SUM/AVG/MIN/MAX), GROUP BY, ORDER BY, LIMIT;
CREATE/DROP TABLE and CREATE/DROP INDEX. WHERE is a ClassAd expression
(==, =?=, undefined, regexp(), ...). Writing requires WRITE at the daemon.
`

// version is stamped at build time via `-ldflags "-X main.version=..."` (see the
// Makefile); it is "dev" for a plain `go build`.
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "htcondordb-cli:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := parseFlags()
	if fs.version {
		fmt.Println("htcondordb-cli", version)
		return nil
	}
	if fs.help {
		fmt.Print(usageText)
		return nil
	}

	// Default to WARNING so the CLI is quiet; -debug turns on DEBUG. This governs
	// the slog default the CEDAR client and other libraries log through.
	level := slog.LevelWarn
	if fs.debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	// Run as subsystem TOOL (like C++ command-line tools) so operator config scoped with
	// a TOOL. prefix -- e.g. TOOL.SEC_CLIENT_AUTHENTICATION_METHODS -- is honored. A bare
	// config.New() leaves the subsystem empty, which disables all <SUBSYS>.PARAM resolution.
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	addr := fs.addr
	if addr == "" {
		addr, err = locateDaemon(cfg)
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subcommand: `load` ingests a ClassAd stream from stdin.
	if len(fs.args) > 0 && fs.args[0] == "load" {
		return runLoad(ctx, cfg, addr, fs)
	}

	dbc, closeConn, err := connectDB(ctx, cfg, addr)
	if err != nil {
		return err
	}
	defer closeConn()

	execCfg := repl.ExecConfig{KeyAttr: fs.keyAttr}
	if fs.consistent {
		// Consistent mode: route writes through the raft cluster's DBControl
		// endpoint, following leader redirects. Reads still use the dbrpc session.
		execCfg.ApplyBatch = consistentWriter(ctx, cfg, addr)
	}
	exec := repl.NewExecutor(dbc, execCfg)

	// One-shot mode: -e, or statements passed as arguments.
	if oneShot := oneShotStatements(fs); oneShot != "" {
		res, err := exec.ExecString(oneShot)
		if err != nil {
			if h := repl.HintFor(err); h != "" {
				fmt.Fprintf(os.Stderr, "hint: %s\n", h)
			}
			return err
		}
		format := repl.FormatTable
		if fs.format != "" {
			format, err = repl.ParseFormat(fs.format)
			if err != nil {
				return err
			}
		}
		repl.FormatResult(os.Stdout, res, format)
		return nil
	}

	// Line input: a readline instance (arrow-key history + editing) on an
	// interactive terminal, else a plain scanner for piped input.
	readLine := repl.ScanLines(os.Stdin)
	if isInteractive() {
		rl, rlErr := readline.NewEx(&readline.Config{
			Prompt:          "htcondordb> ",
			HistoryFile:     historyFile(),
			InterruptPrompt: "^C",
			EOFPrompt:       "exit",
			HistoryLimit:    1000,
		})
		if rlErr == nil {
			defer func() { _ = rl.Close() }()
			readLine = func() (string, error) {
				line, err := rl.Readline()
				if err == readline.ErrInterrupt {
					// Ctrl-C abandons the current line and keeps the shell open.
					return "", nil
				}
				return line, err // io.EOF on Ctrl-D exits the loop
			}
		}
		fmt.Printf("Connected to htcondordb at %s. Type .help for help, .quit to exit.\n", addr)
	} else {
		// Piped input: Ctrl-C / SIGTERM cancels the loop cleanly (a readline
		// terminal handles Ctrl-C itself via ErrInterrupt).
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() { <-sigCh; cancel() }()
	}

	err = repl.Run(ctx, exec, readLine, os.Stdout)
	if err == context.Canceled {
		return nil
	}
	return err
}

// historyFile is where the interactive shell persists command history
// ($HOME/.htcondordb_history), or "" if the home directory is unavailable.
func historyFile() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".htcondordb_history")
}

type flags struct {
	addr       string
	keyAttr    string
	stmt       string
	consistent bool
	loadKey    string // `load`: source attribute used as the primary key
	format     string // one-shot output format (-format)
	loadTable  string // `load`: target table (default "ads")
	loadAuto   bool   // `load`: route each ad to a table named for its MyType
	help       bool
	debug      bool
	version    bool
	args       []string
}

func parseFlags() *flags {
	f := &flags{}
	// Minimal hand-rolled flag handling so a bare statement can follow.
	args := os.Args[1:]
	var rest []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-addr", "--addr":
			i++
			if i < len(args) {
				f.addr = args[i]
			}
		case "-e", "--execute":
			i++
			if i < len(args) {
				f.stmt = args[i]
			}
		case "-key-attr", "--key-attr":
			i++
			if i < len(args) {
				f.keyAttr = args[i]
			}
		case "-consistent", "--consistent":
			f.consistent = true
		case "-debug", "--debug":
			f.debug = true
		case "-key", "--key":
			i++
			if i < len(args) {
				f.loadKey = args[i]
			}
		case "-format", "--format":
			i++
			if i < len(args) {
				f.format = args[i]
			}
		case "-table", "--table":
			i++
			if i < len(args) {
				f.loadTable = args[i]
			}
		case "-auto", "--auto":
			f.loadAuto = true
		case "-version", "--version":
			f.version = true
		case "-h", "-help", "--help":
			f.help = true
		default:
			rest = append(rest, args[i])
		}
	}
	f.args = rest
	return f
}

func oneShotStatements(f *flags) string {
	if strings.TrimSpace(f.stmt) != "" {
		return f.stmt
	}
	if len(f.args) > 0 {
		return strings.Join(f.args, " ")
	}
	return ""
}

// locateDaemon resolves the daemon's command address: HTCONDORDB_ADDRESS_FILE
// (default $(LOG)/.htcondordb_address), else the HTCONDORDB_HOST knob.
func locateDaemon(cfg *config.Config) (string, error) {
	addrFile := strings.TrimSpace(getConfig(cfg, "HTCONDORDB_ADDRESS_FILE"))
	if addrFile == "" {
		if logDir := strings.TrimSpace(getConfig(cfg, "LOG")); logDir != "" {
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
	if host := strings.TrimSpace(getConfig(cfg, "HTCONDORDB_HOST")); host != "" {
		return host, nil
	}
	return "", fmt.Errorf("cannot locate the htcondordb daemon: pass -addr, or set HTCONDORDB_ADDRESS_FILE / HTCONDORDB_HOST")
}

func getConfig(cfg *config.Config, key string) string {
	v, _ := cfg.Get(key)
	return v
}

// connectDB opens an authenticated DBSession and returns a dbrpc client plus a
// cleanup that closes it and the underlying CEDAR connection.
func connectDB(ctx context.Context, cfg *config.Config, addr string) (*dbrpc.Client, func(), error) {
	sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
	if err != nil {
		return nil, nil, fmt.Errorf("building client security config: %w", err)
	}
	sec.Command = command.DBSession
	// Prefer authentication so the client maps to its user (e.g. via FS) and can
	// be authorized for WRITE; OPTIONAL on both ends would negotiate to no auth,
	// leaving the connection anonymous and read-only. PREFERRED still connects
	// (read-only) when no method is mutually available.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}

	connCtx, connCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connCancel()
	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, addr, sec)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to %s: %w", addr, err)
	}
	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	cleanup := func() { _ = dbc.Close(); _ = cl.Close() }
	return dbc, cleanup, nil
}

// runLoad ingests a ClassAd stream (native `condor_status -long` / `condor_q
// -long` output, ads separated by blank lines) from stdin into the store. Each
// ad is keyed by the -key attribute (default "Name"); that value is also stamped
// into the row's "Key" attribute so the REPL can address it.
// maxLoadReconnects bounds how many times a load batch reconnects and replays after
// a dropped connection before giving up.
const maxLoadReconnects = 8

func runLoad(ctx context.Context, cfg *config.Config, addr string, fs *flags) error {
	dbc, closeConn, err := connectDB(ctx, cfg, addr)
	if err != nil {
		return err
	}
	// closeConn is reassigned when a batch reconnects below, so defer through the
	// variable rather than capturing the original.
	defer func() { closeConn() }()

	defaultTable := fs.loadTable
	if defaultTable == "" {
		defaultTable = dbrpc.DefaultTable
	}
	// tableFor decides each ad's table: by MyType with -auto, else the fixed table.
	tableFor := func(ad *classad.ClassAd) string { return defaultTable }
	if fs.loadAuto {
		tableFor = func(ad *classad.ClassAd) string { return tableForType(ad, defaultTable) }
	}

	// Each batch commits idempotently: a per-run id plus a batch counter form a
	// stable idempotency key, so a batch replayed after a mid-load connection drop
	// applies exactly once (the server deduplicates it). This is the general-purpose
	// CommitIdempotent path -- a bulk load's inserts are not idempotent-by-key, so
	// exactly-once matters here, unlike the collector's ad store (which opts out).
	var runID [8]byte
	_, _ = rand.Read(runID[:])
	runTag := hex.EncodeToString(runID[:])
	batchSeq := 0

	// Per-table batch commit: ensure the table exists (once), then commit the ads,
	// reconnecting and replaying the batch (same idempotency key) on a dropped
	// connection.
	created := map[string]bool{}
	commit := func(table string, ops []repl.WriteOp) error {
		batchSeq++
		idemKey := fmt.Sprintf("cli-load-%s-%d", runTag, batchSeq)
		backoff := 100 * time.Millisecond
		for attempt := 0; ; attempt++ {
			err := func() error {
				if !created[table] {
					if e := dbc.CreateTable(ctx, table); e != nil {
						return e
					}
					created[table] = true
				}
				tx, e := dbc.BeginTable(ctx, table)
				if e != nil {
					return e
				}
				for _, op := range ops {
					if e := tx.NewClassAd(ctx, op.Key, op.Value); e != nil {
						_ = tx.Abort(ctx)
						return e
					}
				}
				return tx.CommitIdempotent(ctx, idemKey)
			}()
			if err == nil {
				return nil
			}
			// Retry only a dropped connection: replaying the batch under the same
			// idempotency key is exactly-once. Reconnect and try again, bounded.
			if !errors.Is(err, dbrpc.ErrConnClosed) || attempt >= maxLoadReconnects {
				return err
			}
			closeConn()
			if ndbc, nclose, derr := connectDB(ctx, cfg, addr); derr == nil {
				dbc, closeConn = ndbc, nclose
			}
			time.Sleep(backoff)
			if backoff *= 2; backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
	}

	srcKey := fs.loadKey
	if srcKey == "" {
		srcKey = "Name"
	}
	loaded, skipped, perTable, err := loadAds(os.Stdin, srcKey, tableFor, 200, commit)
	fmt.Printf("loaded %d ads (%d skipped for a missing %s key)\n", loaded, skipped, srcKey)
	if fs.loadAuto || fs.loadTable != "" {
		for t, n := range perTable {
			fmt.Printf("  %s: %d\n", t, n)
		}
	}
	return err
}

// tableForType maps an ad's MyType to a table name (lowercased and pluralized:
// Machine -> machines, Job -> jobs), falling back to fallback for a missing or
// unusable type.
func tableForType(ad *classad.ClassAd, fallback string) string {
	v := ad.EvaluateAttr("MyType")
	if !v.IsString() {
		return fallback
	}
	mt, _ := v.StringValue()
	if mt == "" {
		return fallback
	}
	name := strings.ToLower(mt)
	if !strings.HasSuffix(name, "s") {
		name += "s"
	}
	if !db.ValidTableName(name) {
		return fallback
	}
	return name
}

// loadAds reads blank-line-separated old-ClassAd blocks, keys each by srcKey,
// stamps a matching Key attribute, routes it to tableFor(ad), and commits per
// table in batches of batchSize. perTable counts loaded ads per table.
func loadAds(in io.Reader, srcKey string, tableFor func(*classad.ClassAd) string, batchSize int,
	commit func(table string, ops []repl.WriteOp) error) (loaded, skipped int, perTable map[string]int, err error) {

	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	perTable = map[string]int{}
	batches := map[string][]repl.WriteOp{}
	var block strings.Builder

	flush := func(table string) error {
		ops := batches[table]
		if len(ops) == 0 {
			return nil
		}
		if err := commit(table, ops); err != nil {
			return err
		}
		loaded += len(ops)
		perTable[table] += len(ops)
		batches[table] = ops[:0]
		return nil
	}
	flushAll := func() error {
		for table := range batches {
			if err := flush(table); err != nil {
				return err
			}
		}
		return nil
	}

	emit := func(text string) error {
		if strings.TrimSpace(text) == "" {
			return nil
		}
		ad, perr := classad.ParseOld(text)
		if perr != nil {
			skipped++
			return nil // skip an unparseable block rather than abort the whole load
		}
		key := keyString(ad.EvaluateAttr(srcKey))
		if key == "" {
			skipped++
			return nil
		}
		adText := text
		if _, ok := ad.Lookup("Key"); !ok {
			adText = strings.TrimRight(text, "\n") + "\nKey = " + quoteClassAd(key) + "\n"
		}
		table := tableFor(ad)
		batches[table] = append(batches[table], repl.WriteOp{Kind: repl.WNewClassAd, Key: key, Value: adText})
		if len(batches[table]) >= batchSize {
			return flush(table)
		}
		return nil
	}

	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" { // blank line terminates an ad
			if e := emit(block.String()); e != nil {
				return loaded, skipped, perTable, e
			}
			block.Reset()
			continue
		}
		block.WriteString(line)
		block.WriteByte('\n')
	}
	if e := sc.Err(); e != nil {
		return loaded, skipped, perTable, e
	}
	if e := emit(block.String()); e != nil { // trailing ad with no final blank line
		return loaded, skipped, perTable, e
	}
	return loaded, skipped, perTable, flushAll()
}

// keyString renders a key attribute value as a db key string.
func keyString(v classad.Value) string {
	if v.IsString() {
		s, _ := v.StringValue()
		return s
	}
	if v.IsUndefined() || v.IsError() {
		return ""
	}
	return v.String()
}

// quoteClassAd renders s as a ClassAd double-quoted string literal.
func quoteClassAd(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// consistentWriter builds a repl.ApplyBatch that submits write batches to the
// consistent-mode cluster via the DBControl command, following leader redirects.
func consistentWriter(ctx context.Context, cfg *config.Config, addr string) func([]repl.WriteOp) error {
	exchange := func(ectx context.Context, target string, req *classad.ClassAd) (*classad.ClassAd, error) {
		sec, err := htcondor.GetSecurityConfig(cfg, command.DBControl, "CLIENT")
		if err != nil {
			return nil, err
		}
		sec.Command = command.DBControl
		cl, err := cedarclient.ConnectAndAuthenticate(ectx, target, sec)
		if err != nil {
			return nil, err
		}
		defer func() { _ = cl.Close() }()
		s := cl.GetStream()
		out := message.NewMessageForStream(s)
		if err := out.PutClassAd(ectx, req); err != nil {
			return nil, err
		}
		if err := out.FinishMessage(ectx); err != nil { // flush the frame (EOM); PutClassAd only buffers
			return nil, err
		}
		return message.NewMessageFromStream(s).GetClassAd(ectx)
	}

	cc := consistent.NewControlClient(addr, exchange)
	return func(ops []repl.WriteOp) error {
		b := consistent.NewBatch()
		for _, op := range ops {
			switch op.Kind {
			case repl.WNewClassAd:
				b.NewClassAd(op.Key, op.Value)
			case repl.WSetAttribute:
				b.SetAttribute(op.Key, op.Name, op.Value)
			case repl.WDestroyClassAd:
				b.DestroyClassAd(op.Key)
			}
		}
		return cc.Apply(ctx, b)
	}
}

func isInteractive() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
