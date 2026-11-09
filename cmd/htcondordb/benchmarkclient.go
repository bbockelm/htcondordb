package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/bench"
)

// Benchmark client mode: the load generator, pointed at a server that is already running.
//
// IT IS A SEPARATE PROCESS FROM THE SERVER ON PURPOSE. A generator sharing the daemon's cores
// measures itself as much as the daemon, and the error is not small -- an in-process sweep of
// this workload reported four concurrent readers as slower than one and slower than eight, which
// is contention between the clients and the server they were timing rather than a scaling curve.
// -benchmark-self-test accepts that cost to stay a single command; this mode is what produces a
// number about a deployment.
//
// THE SERVER IT POINTS AT NEED NOT BE A BENCHMARK SERVER. A live daemon's jobs, history or
// epoch_history answers the same ops, which is the case worth measuring: generated data says what
// the hardware can do, a production table says what it does do.

// benchmarkClientConfig is the client's settings, separated from the flags so the mode is
// testable without a command line.
type benchmarkClientConfig struct {
	// Addr is the server's dbrpc address. Empty means the mode is off.
	Addr string
	// FlightAddr is the server's Flight SQL address. Empty leaves the Arrow arm out rather
	// than guessing a port: Arrow is a different server endpoint, not a setting of this one.
	FlightAddr string
	Table      string
	Constraint string
	Projection []string
	// SQLWhere is the Arrow arm's filter. The tool does not translate the ClassAd constraint
	// into SQL -- the languages differ -- so an Arrow arm with a non-trivial constraint and no
	// SQLWhere declines with a note instead of measuring a different question.
	SQLWhere   string
	Limits     []int
	Clients    int
	Iterations int
	// Transports is empty for the default set.
	Transports []bench.Transport
}

type benchmarkClientFlags struct {
	cfg        benchmarkClientConfig
	projection *string
	limits     *string
	transports *string
}

// registerBenchmarkClientFlags declares the client's flags. Called before flag.Parse.
func registerBenchmarkClientFlags() *benchmarkClientFlags {
	f := &benchmarkClientFlags{}
	flag.StringVar(&f.cfg.Addr, "benchmark-client", "",
		"measure the server at this dbrpc address (host:port) and exit; the load runs in this process")
	flag.StringVar(&f.cfg.FlightAddr, "benchmark-client-flight", "",
		"server's Flight SQL address; adds the Arrow arm, which is left out when unset")
	flag.StringVar(&f.cfg.Table, "benchmark-table", "history",
		"table to read: a benchmark server's jobs/history/epoch_history, or a live server's")
	flag.StringVar(&f.cfg.Constraint, "benchmark-constraint", "true", "ClassAd constraint for the read")
	f.projection = flag.String("benchmark-projection", "ClusterId,Owner,JobStatus,MemoryUsage",
		"comma-separated attributes to project; the whole-ad arm ignores it")
	flag.StringVar(&f.cfg.SQLWhere, "benchmark-sql-where", "",
		"SQL filter for the Arrow arm, which the caller asserts matches -benchmark-constraint")
	f.limits = flag.String("benchmark-limits", "0",
		"comma-separated result sizes to sweep; 0 means the whole result")
	f.transports = flag.String("benchmark-transports", "",
		"comma-separated transports to measure; empty means all of them")
	return f
}

// resolve turns the string-valued flags into the config's typed fields.
func (f *benchmarkClientFlags) resolve(clients, iterations int) (benchmarkClientConfig, error) {
	cfg := f.cfg
	cfg.Clients, cfg.Iterations = clients, iterations
	cfg.Projection = splitList(*f.projection)
	for _, s := range splitList(*f.limits) {
		n, err := strconv.Atoi(s)
		if err != nil {
			return cfg, fmt.Errorf("-benchmark-limits: %q is not a number", s)
		}
		cfg.Limits = append(cfg.Limits, n)
	}
	for _, s := range splitList(*f.transports) {
		cfg.Transports = append(cfg.Transports, bench.Transport(s))
	}
	// A Flight address is a request for the Arrow arm. The default set leaves Arrow out because
	// most servers have no Flight endpoint, so without this the flag would be accepted and
	// silently measure nothing -- naming the transports explicitly is the only other way to get
	// it, which is not what the flag says it does.
	if len(cfg.Transports) == 0 && cfg.FlightAddr != "" {
		cfg.Transports = append(bench.DefaultTransports(), bench.Arrow)
	}
	return cfg, nil
}

// splitList parses a comma-separated flag, dropping empties so a trailing comma is not an entry.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// dialBenchmarkServer opens one measured connection, carrying a Flight client too when an Arrow
// address was given.
//
// Both sockets are counted into one total, which is right because a cell's bytes are a DELTA
// taken around that cell: during a dbrpc transport only the dbrpc socket moves, during the Arrow
// one only the Flight socket does.
func dialBenchmarkServer(cfg benchmarkClientConfig) bench.Dialer {
	return func(ctx context.Context) (*bench.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("dialing dbrpc at %s: %w", cfg.Addr, err)
		}
		counted, bytes := bench.NewCountingConn(c)
		cl := dbrpc.NewClient(dbrpc.NewStreamConn(counted))
		conn := &bench.Conn{Client: cl, Close: func() { _ = cl.Close() }, Bytes: bytes}
		if cfg.FlightAddr == "" {
			return conn, nil
		}
		fc, ferr := bench.DialFlight(cfg.FlightAddr)
		if ferr != nil {
			conn.Close()
			return nil, ferr
		}
		return &bench.Conn{
			Client: cl,
			Flight: fc.Flight,
			Close:  func() { fc.Close(); _ = cl.Close() },
			Bytes:  func() int64 { return bytes() + fc.Bytes() },
		}, nil
	}
}

// runBenchmarkClient sweeps the server and prints the report.
func runBenchmarkClient(ctx context.Context, cfg benchmarkClientConfig, out io.Writer) error {
	if cfg.Clients < 1 {
		cfg.Clients = 1
	}
	if cfg.Iterations < 1 {
		cfg.Iterations = 1
	}
	spec := bench.Spec{
		Table:       cfg.Table,
		Constraint:  cfg.Constraint,
		Projection:  cfg.Projection,
		SQLWhere:    cfg.SQLWhere,
		Limits:      cfg.Limits,
		Concurrency: cfg.Clients,
		Iterations:  cfg.Iterations,
		Transports:  cfg.Transports,
	}
	_, _ = fmt.Fprintf(out, "htcondordb benchmark client\nserver: %s", cfg.Addr)
	if cfg.FlightAddr != "" {
		_, _ = fmt.Fprintf(out, "  flight: %s", cfg.FlightAddr)
	}
	_, _ = fmt.Fprintf(out, "\ntable %s  clients=%d  iterations=%d\n\n", cfg.Table, cfg.Clients, cfg.Iterations)
	rep, err := bench.Run(ctx, dialBenchmarkServer(cfg), spec)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprint(out, rep.String())
	return nil
}
