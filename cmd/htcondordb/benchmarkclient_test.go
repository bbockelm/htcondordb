package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/bench"
)

// flagsFor builds the client's flags without touching the package-level flag set, which can only
// be registered once per process.
func flagsFor(cfg benchmarkClientConfig, projection, limits, transports string) *benchmarkClientFlags {
	return &benchmarkClientFlags{
		cfg:        cfg,
		projection: &projection,
		limits:     &limits,
		transports: &transports,
	}
}

func hasTransport(ts []bench.Transport, want bench.Transport) bool {
	for _, t := range ts {
		if t == want {
			return true
		}
	}
	return false
}

// A Flight address is the only way the client is told the server has an Arrow endpoint, so it has
// to be what adds the Arrow arm.
//
// The default transport set leaves Arrow out on purpose -- most servers have no Flight endpoint,
// and an arm that fails everywhere reads as a slow transport rather than an absent one. That made
// the flag accepted and inert: it was documented as adding the arm and measured nothing, which is
// worse than rejecting it, because the sweep still prints a full-looking table.
func TestBenchmarkClientAddsArrowWhenGivenAFlightAddress(t *testing.T) {
	f := flagsFor(benchmarkClientConfig{
		Addr: "127.0.0.1:1", FlightAddr: "127.0.0.1:2",
	}, "ClusterId", "0", "")
	cfg, err := f.resolve(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !hasTransport(cfg.Transports, bench.Arrow) {
		t.Errorf("a Flight address did not add the Arrow arm, so the flag measures nothing: %v",
			cfg.Transports)
	}
	// The rest of the sweep still runs: the Arrow arm is an addition, not a replacement.
	for _, want := range bench.DefaultTransports() {
		if !hasTransport(cfg.Transports, want) {
			t.Errorf("a Flight address dropped %s from the sweep: %v", want, cfg.Transports)
		}
	}
}

// Without a Flight address there is nothing to dial, so the arm must stay out rather than be
// measured as a failure.
func TestBenchmarkClientLeavesArrowOutWithoutAFlightAddress(t *testing.T) {
	f := flagsFor(benchmarkClientConfig{Addr: "127.0.0.1:1"}, "ClusterId", "0", "")
	cfg, err := f.resolve(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if hasTransport(cfg.Transports, bench.Arrow) {
		t.Errorf("Arrow was measured with no Flight endpoint to measure: %v", cfg.Transports)
	}
}

// An explicit transport list is honoured as given: it is how a caller measures one arm.
func TestBenchmarkClientHonoursAnExplicitTransportList(t *testing.T) {
	f := flagsFor(benchmarkClientConfig{
		Addr: "127.0.0.1:1", FlightAddr: "127.0.0.1:2",
	}, "ClusterId", "0", string(bench.Wire))
	cfg, err := f.resolve(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Transports) != 1 || cfg.Transports[0] != bench.Wire {
		t.Errorf("an explicit list was not honoured: %v", cfg.Transports)
	}
}

func TestBenchmarkClientRejectsANonNumericLimit(t *testing.T) {
	f := flagsFor(benchmarkClientConfig{Addr: "127.0.0.1:1"}, "ClusterId", "100,all", "")
	if _, err := f.resolve(1, 1); err == nil {
		t.Error("a non-numeric limit was accepted, so the sweep would silently skip it")
	}
}

// addrAfter pulls an announced address out of the server's output: the text after a label, to the
// end of that line.
func addrAfter(out, label string) string {
	i := strings.Index(out, label)
	if i < 0 {
		return ""
	}
	rest := out[i+len(label):]
	if j := strings.IndexByte(rest, '\n'); j >= 0 {
		rest = rest[:j]
	}
	return strings.TrimSpace(rest)
}

// End to end: the client measures a server over a real socket and reports rows for every arm.
//
// Measured against a SERVING benchmark server rather than an in-process store, because that is
// the whole difference between this mode and -benchmark-self-test -- the load has to cross a
// socket, and a client that cannot dial one is the failure this covers.
func TestBenchmarkClientMeasuresAServingServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// The scratch directory is taken HERE, not inside the goroutine: t.TempDir registers its
	// own removal, and that removal has to be registered before the one below so it runs after
	// it. The server writes into the directory until it stops, so removing it first fails with
	// "directory not empty" -- which is a flaky test, not a flaky server.
	dir := filepath.Join(t.TempDir(), "scratch")
	var out syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- runBenchmark(ctx, benchmarkConfig{
			Dir: dir, Rows: 400,
			Listen: "127.0.0.1:0", Flight: "127.0.0.1:0", Seed: 7,
		}, &out)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the benchmark server did not stop when the context was cancelled")
		}
	})
	deadline := time.After(120 * time.Second)
	for !strings.Contains(out.String(), "flight sql:") {
		select {
		case err := <-done:
			t.Fatalf("server returned before serving: %v\n%s", err, out.String())
		case <-deadline:
			t.Fatalf("server did not bind in time:\n%s", out.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
	dbAddr := addrAfter(out.String(), "dbrpc:")
	flAddr := addrAfter(out.String(), "flight sql:")
	if dbAddr == "" || flAddr == "" {
		t.Fatalf("could not read the server's addresses from:\n%s", out.String())
	}

	var creport syncBuffer
	if err := runBenchmarkClient(ctx, benchmarkClientConfig{
		Addr: dbAddr, FlightAddr: flAddr,
		Table: "history", Constraint: "true",
		Projection: []string{"ClusterId", "Owner", "JobStatus"},
		Limits:     []int{0},
		Clients:    2, Iterations: 1,
		Transports: []bench.Transport{bench.Wire, bench.Values, bench.Arrow},
	}, &creport); err != nil {
		t.Fatalf("client failed against %s: %v\n%s", dbAddr, err, creport.String())
	}
	rep := creport.String()
	// Every arm asked for has to appear AND have moved rows. A transport that errored still
	// prints its row, so naming it is not enough on its own.
	for _, tr := range []bench.Transport{bench.Wire, bench.Values, bench.Arrow} {
		if !strings.Contains(rep, string(tr)) {
			t.Errorf("%s is missing from the report:\n%s", tr, rep)
		}
	}
	if strings.Contains(rep, "records=0") {
		t.Errorf("the client measured an empty table:\n%s", rep)
	}
	// 2 clients x 400 rows on the one limit, for each of three arms: the exact total is the
	// server's business, but a report with a zero row count measured nothing.
	if strings.Contains(rep, "  0  ") {
		t.Errorf("an arm delivered no rows:\n%s", rep)
	}
}
