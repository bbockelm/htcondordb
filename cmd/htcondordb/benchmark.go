package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bbockelm/htcondordb/bench"
)

// Benchmark mode: a throwaway server on scratch, for comparing deployments.
//
// IT INTERCEPTS BEFORE THE DAEMON'S CONFIG IS LOADED, and that ordering is the safety property.
// Benchmark mode serves generated data with anonymous reads and an open Flight endpoint. If it
// ran after config.New it would be holding a SPOOL, a pool password and an advertise address
// that it must not use, and the only thing keeping those apart would be each branch remembering
// not to touch them. Returning before any of it is read means there is nothing to remember.
//
// IT IS A FLAG, NOT A CONFIG KEY. A config key can arrive from an inherited HTCondor
// configuration -- a condor_config already on the box, an environment variable, a local-name
// scope -- and turning on anonymous reads because a file somewhere said so is exactly the
// accident the scratch guard exists to prevent. A flag has to be typed.

// benchmarkConfig is the mode's settings, separated from the flags so the mode is testable
// without a command line.
type benchmarkConfig struct {
	Dir      string
	Rows     int
	Listen   string
	Flight   string
	SelfTest bool
	Seed     int64
	// Clients is how many readers run at once in self-test mode. One measures latency; more
	// measures the store under contention, which is the question a server is usually being
	// asked.
	Clients int
	// Iterations is how many times each reader repeats its read. The default is above one
	// deliberately: measured on a 20000-row table a single-iteration cell swung 2.6x between
	// runs, which is wider than any scaling effect it would be used to show.
	Iterations int
}

type benchmarkFlags struct {
	on  *bool
	cfg benchmarkConfig
}

// registerBenchmarkFlags declares the mode's flags. Called before flag.Parse.
func registerBenchmarkFlags() *benchmarkFlags {
	f := &benchmarkFlags{}
	f.on = flag.Bool("benchmark", false,
		"run as a throwaway benchmark server on generated data (anonymous reads; requires -benchmark-dir)")
	flag.StringVar(&f.cfg.Dir, "benchmark-dir", "",
		"scratch directory for benchmark mode; must be empty or previously used for this")
	flag.IntVar(&f.cfg.Rows, "benchmark-rows", 100000, "rows to generate per table in benchmark mode")
	flag.StringVar(&f.cfg.Listen, "benchmark-listen", "127.0.0.1:0", "benchmark dbrpc listen address")
	flag.StringVar(&f.cfg.Flight, "benchmark-flight", "127.0.0.1:0", "benchmark Flight SQL listen address")
	flag.BoolVar(&f.cfg.SelfTest, "benchmark-self-test", false,
		"in benchmark mode, measure the store in process and exit instead of serving")
	flag.Int64Var(&f.cfg.Seed, "benchmark-seed", 1, "dataset seed; the same seed gives the same data")
	flag.IntVar(&f.cfg.Clients, "benchmark-clients", 1,
		"concurrent readers for -benchmark-self-test")
	flag.IntVar(&f.cfg.Iterations, "benchmark-iterations", 5,
		"reads per reader; one iteration does not resolve on a small table")
	return f
}

// runBenchmark builds the scratch dataset and either self-tests it or serves it until the
// context is cancelled.
func runBenchmark(ctx context.Context, cfg benchmarkConfig, out io.Writer) error {
	if cfg.Iterations < 1 {
		cfg.Iterations = 1
	}
	if cfg.Clients < 1 {
		cfg.Clients = 1
	}
	if cfg.Dir == "" {
		return fmt.Errorf("benchmark mode needs -benchmark-dir; it will not use the configured SPOOL or database directory")
	}
	_, _ = fmt.Fprintf(out, "htcondordb benchmark mode\nscratch: %s\n", cfg.Dir)
	// Guarded here, before anything is announced. Build guards too -- it must, because it is
	// callable on its own -- but checking first means a refusal is not preceded by a line
	// saying the data is being generated. The second check sees the marker this one wrote.
	if err := bench.GuardScratchDir(cfg.Dir); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "generating %d rows per table (seed %d)...\n", cfg.Rows, cfg.Seed)
	start := time.Now()
	srv, reports, err := bench.Build(bench.ServeConfig{
		Dir: cfg.Dir, Rows: cfg.Rows, Seed: cfg.Seed,
	}, out)
	if err != nil {
		return err
	}
	defer srv.Close()
	for _, r := range reports {
		_, _ = fmt.Fprintf(out, "  %-14s %7d rows in %-8v columnar=%v indexes=%v\n",
			r.Shape, r.Rows, r.Elapsed.Round(time.Millisecond), r.Prepare.SchemaScanOn, r.Prepare.IndexesAdded)
		for _, n := range r.Prepare.Notes {
			_, _ = fmt.Fprintf(out, "      note: %s\n", n)
		}
	}
	_, _ = fmt.Fprintf(out, "built in %v\n", time.Since(start).Round(time.Millisecond))

	if cfg.SelfTest {
		for _, shape := range []bench.Shape{bench.ShapeJobs, bench.ShapeHistory, bench.ShapeEpochHistory} {
			t, ok := srv.Tables[shape]
			if !ok {
				continue
			}
			rep := bench.SelfTestArchive(t, bench.SelfTestSpec{
				Table: string(shape), Constraint: "true",
				Projection:  []string{"ClusterId", "Owner", "JobStatus", "MemoryUsage"},
				Iterations:  cfg.Iterations,
				Concurrency: cfg.Clients,
			})
			_, _ = fmt.Fprintf(out, "\n%s", rep)
		}
		return nil
	}

	addr, err := srv.ServeAnonymous(ctx, cfg.Listen)
	if err != nil {
		return err
	}
	faddr, err := srv.ServeFlight(ctx, cfg.Flight)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "\nSERVING GENERATED DATA WITH ANONYMOUS READS -- do not expose these ports\n")
	if addr != "" {
		_, _ = fmt.Fprintf(out, "  dbrpc:      %s\n", addr)
	}
	if faddr != "" {
		_, _ = fmt.Fprintf(out, "  flight sql: %s\n", faddr)
	}
	_, _ = fmt.Fprintf(out, "tables: jobs, history, epoch_history\nCtrl-C to stop.\n")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	select {
	case <-ctx.Done():
	case <-stop:
		_, _ = fmt.Fprintln(out, "stopping")
	}
	return nil
}
