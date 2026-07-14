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
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/ha/consistent"
	"github.com/bbockelm/htcondordb/repl"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "htcondordb-cli:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := parseFlags()

	cfg, err := config.New()
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
			return err
		}
		repl.FormatResult(os.Stdout, res)
		return nil
	}

	// Interactive: Ctrl-C cancels the loop cleanly.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	fmt.Printf("Connected to htcondordb at %s. Type .help for help, .quit to exit.\n", addr)
	prompt := ""
	if isInteractive() {
		prompt = "htcondordb> "
	}
	err = repl.Run(ctx, exec, os.Stdin, os.Stdout, prompt)
	if err == context.Canceled {
		return nil
	}
	return err
}

type flags struct {
	addr       string
	keyAttr    string
	stmt       string
	consistent bool
	loadKey    string // `load`: source attribute used as the primary key
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
		case "-key", "--key":
			i++
			if i < len(args) {
				f.loadKey = args[i]
			}
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
func runLoad(ctx context.Context, cfg *config.Config, addr string, fs *flags) error {
	dbc, closeConn, err := connectDB(ctx, cfg, addr)
	if err != nil {
		return err
	}
	defer closeConn()

	// Write sink: consistent mode routes batches through raft; otherwise commit
	// locally over dbrpc.
	var apply func([]repl.WriteOp) error
	if fs.consistent {
		apply = consistentWriter(ctx, cfg, addr)
	}
	commitOps := func(ops []repl.WriteOp) error {
		if apply != nil {
			return apply(ops)
		}
		tx, err := dbc.Begin()
		if err != nil {
			return err
		}
		for _, op := range ops {
			if err := tx.NewClassAd(op.Key, op.Value); err != nil {
				_ = tx.Abort()
				return err
			}
		}
		return tx.Commit()
	}

	srcKey := fs.loadKey
	if srcKey == "" {
		srcKey = "Name"
	}
	loaded, skipped, err := loadAds(os.Stdin, srcKey, 200, commitOps)
	fmt.Printf("loaded %d ads (%d skipped for a missing %s key)\n", loaded, skipped, srcKey)
	return err
}

// loadAds reads blank-line-separated old-ClassAd blocks, keys each by srcKey,
// stamps a matching Key attribute, and commits them in batches of batchSize.
func loadAds(in io.Reader, srcKey string, batchSize int, commit func([]repl.WriteOp) error) (loaded, skipped int, err error) {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var block strings.Builder
	var batch []repl.WriteOp

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := commit(batch); err != nil {
			return err
		}
		loaded += len(batch)
		batch = batch[:0]
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
		batch = append(batch, repl.WriteOp{Kind: repl.WNewClassAd, Key: key, Value: adText})
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	}

	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" { // blank line terminates an ad
			if e := emit(block.String()); e != nil {
				return loaded, skipped, e
			}
			block.Reset()
			continue
		}
		block.WriteString(line)
		block.WriteByte('\n')
	}
	if e := sc.Err(); e != nil {
		return loaded, skipped, e
	}
	if e := emit(block.String()); e != nil { // trailing ad with no final blank line
		return loaded, skipped, e
	}
	return loaded, skipped, flush()
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
		if err := message.NewMessageForStream(s).PutClassAd(ectx, req); err != nil {
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
