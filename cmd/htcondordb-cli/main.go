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
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	cedarclient "github.com/bbockelm/cedar/client"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/htcondordb/command"
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

	// Client security from the HTCondor configuration for the DB session command.
	sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
	if err != nil {
		return fmt.Errorf("building client security config: %w", err)
	}
	sec.Command = command.DBSession

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connCtx, connCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connCancel()
	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, addr, sec)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", addr, err)
	}
	defer func() { _ = cl.Close() }()

	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	defer func() { _ = dbc.Close() }()

	exec := repl.NewExecutor(dbc, repl.ExecConfig{KeyAttr: fs.keyAttr})

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
	addr    string
	keyAttr string
	stmt    string
	args    []string
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

func isInteractive() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
