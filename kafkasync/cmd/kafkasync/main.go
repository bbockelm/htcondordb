// Command kafkasync manages and runs Kafka change-data exporters for an htcondordb
// instance. It is a dbrpc client: exporter definitions and their resume state live in the
// database catalog, and this process watches a table and mirrors its changes to a Kafka
// topic. It is the only component that links a Kafka client -- the core daemon never does.
//
// Usage:
//
//	kafkasync create -name <n> -table <t> -brokers <b1,b2> -topic <topic> [options]
//	kafkasync list
//	kafkasync drop -name <n>
//	kafkasync run  -name <n>
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
	"github.com/bbockelm/htcondordb/kafkasync"
)

// dbSessionCommand is the CEDAR command for htcondordb's multiplexed dbrpc session. It must
// match github.com/bbockelm/htcondordb/command.DBSession (74000); it is duplicated here so
// this standalone module need not depend on the htcondordb daemon module.
const dbSessionCommand = 74000

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kafkasync:", err)
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
	fmt.Fprint(os.Stderr, `kafkasync -- manage and run htcondordb Kafka exporters

  kafkasync create -name N -table T -brokers b1,b2 -topic TOPIC [options]
  kafkasync list
  kafkasync drop -name N
  kafkasync run  -name N

Common: -addr HOST:PORT (else HTCONDORDB_ADDRESS_FILE / LOG/.htcondordb_address / HTCONDORDB_HOST).
`)
	return nil
}

// --- shared connection plumbing (mirrors htcondordb-cli's connectDB) ---

func loadConfig() (*config.Config, error) {
	// Subsystem TOOL so TOOL.SEC_CLIENT_* operator config is honored, like the C++ tools.
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
	// Prefer authentication so the client maps to a DAEMON identity (the exporter ops are
	// DAEMON-gated); OPTIONAL on both ends would negotiate to anonymous/read-only.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
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

// dial loads config, resolves the address, and connects.
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
	table := fs.String("table", "", "source table to mirror (required)")
	brokers := fs.String("brokers", "", "comma-separated Kafka bootstrap brokers (required)")
	topic := fs.String("topic", "", "destination topic (required)")
	partitions := fs.Int("partitions", 1, "partitions when creating the topic")
	replication := fs.Int("replication", 1, "replication factor when creating the topic")
	noManageTopic := fs.Bool("no-manage-topic", false, "do not create/configure the topic (assume it exists)")
	noCompact := fs.Bool("no-compact", false, "do not set cleanup.policy=compact on a managed topic")
	batch := fs.Int("batch", 0, "records per flush/checkpoint during live tailing (0=default)")
	saslUser := fs.String("sasl-user", "", "SASL username (enables SASL)")
	saslMech := fs.String("sasl-mechanism", "", "SASL mechanism: PLAIN, SCRAM-SHA-256 (default), or SCRAM-SHA-512")
	// Passwords are referenced, never stored: the exporter reads them at runtime.
	saslPassFile := fs.String("sasl-password-file", "", "path the exporter reads the SASL password from")
	saslPassEnv := fs.String("sasl-password-env", "", "env var the exporter reads the SASL password from")
	tls := fs.Bool("tls", false, "dial the broker with TLS")
	tlsCA := fs.String("tls-ca", "", "CA bundle to verify the broker (empty = system roots); implies -tls")
	tlsCert := fs.String("tls-cert", "", "client certificate for mutual TLS; implies -tls")
	tlsKey := fs.String("tls-key", "", "client private key for mutual TLS; implies -tls")
	tlsServerName := fs.String("tls-server-name", "", "override the name verified against the broker cert")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *table == "" || *brokers == "" || *topic == "" {
		return errors.New("create: -name, -table, -brokers, and -topic are all required")
	}
	cfgK := kafkasync.Config{
		Table:             *table,
		Brokers:           splitCSV(*brokers),
		Topic:             *topic,
		BatchSize:         *batch,
		Partitions:        *partitions,
		ReplicationFactor: *replication,
		ManageTopic:       boolPtr(!*noManageTopic),
		Compact:           boolPtr(!*noCompact),
	}
	if *tls || *tlsCA != "" || *tlsCert != "" || *tlsKey != "" || *tlsServerName != "" {
		cfgK.TLS = &kafkasync.TLSConfig{CAFile: *tlsCA, CertFile: *tlsCert, KeyFile: *tlsKey, ServerName: *tlsServerName}
	}
	if *saslUser != "" {
		cfgK.SASL = &kafkasync.SASLConfig{Mechanism: *saslMech, Username: *saslUser, PasswordFile: *saslPassFile, PasswordEnv: *saslPassEnv}
	}
	if _, err := cfgK.Validate(); err != nil {
		return err
	}
	raw, err := cfgK.Marshal()
	if err != nil {
		return err
	}

	ctx := context.Background()
	c, cleanup, err := dial(ctx, *addr)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := c.CreateExporter(ctx, db.ExporterDef{Name: *name, Kind: kafkasync.Kind, Config: raw}); err != nil {
		return err
	}
	fmt.Printf("created kafka exporter %q (table %q -> topic %q)\n", *name, *table, *topic)
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
			log.Warn("kafkasync: exporter stopped; reconnecting", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		break // clean stop (ctx cancelled)
	}
	return nil
}

// runOnce dials, builds the producer, and runs the exporter until it stops. A returned
// error means the outer loop should reconnect; nil means a clean shutdown.
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
	cfg, err := kafkasync.ParseConfig(def.Config)
	if err != nil {
		return fmt.Errorf("exporter %q: %w", name, err)
	}
	prod, err := kafkasync.NewProducer(ctx, cfg)
	if err != nil {
		return err
	}
	defer prod.Close()

	log.Info("kafkasync: starting exporter", "name", name, "table", cfg.Table, "topic", cfg.Topic)
	runner := kafkasync.NewRunner(name, cfg, c, prod, log)
	runner.MaxConsecutiveFailures = 20 // give up so this loop re-dials with a fresh connection
	return runner.Run(ctx)
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func boolPtr(b bool) *bool { return &b }
