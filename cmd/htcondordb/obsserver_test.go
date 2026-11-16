package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/logging"
)

// freePort reserves a port and releases it, so the listener under test can bind it.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return p
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	var lastErr error
	// The listener comes up on its own goroutine; retry briefly rather than racing it.
	for i := 0; i < 50; i++ {
		resp, err := c.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	t.Fatalf("GET %s never succeeded: %v", url, lastErr)
	return 0, ""
}

// TestObservabilityAnswersBeforeTheCatalogIsOpen is the whole point of binding early: during a
// long catalog open an operator reaches for pprof, and a connection refused reads as a dead
// daemon rather than a busy one. pprof and /startupz must answer before SetMetrics is called, and
// /metrics must answer 503 with the progress rather than refusing the connection.
func TestObservabilityAnswersBeforeTheCatalogIsOpen(t *testing.T) {
	port := freePort(t)
	cfg := mkSyncCfg(t, fmt.Sprintf("HTCONDORDB_METRICS_ADDRESS = 127.0.0.1:%d\nHTCONDORDB_ENABLE_PPROF = true\n", port))
	log, err := logging.New(&logging.Config{OutputPath: t.TempDir() + "/log", SkipGlobalInstall: true})
	if err != nil {
		t.Fatal(err)
	}
	obs := startObservability(cfg, log, func() string { return "opening the catalog: open=\"archive history(5278 segs)\"" })
	if obs == nil {
		t.Fatal("no observability server with an address configured")
	}
	defer obs.Close()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	if code, body := get(t, base+"/startupz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "archive history(5278 segs)") {
		t.Errorf("/startupz before the catalog = %d %q; want 503 naming what is still open", code, body)
	}
	if code, body := get(t, base+"/metrics"); code != http.StatusServiceUnavailable || !strings.Contains(body, "still opening") {
		t.Errorf("/metrics before the catalog = %d %q; want 503 saying the catalog is still opening", code, body)
	}
	// pprof is the one that has to work here: it takes no storage lock, so it answers while the
	// open holds them.
	if code, _ := get(t, base+"/debug/pprof/"); code != http.StatusOK {
		t.Errorf("/debug/pprof/ before the catalog = %d, want 200", code)
	}

	obs.SetMetrics(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "htcondordb_ads{table=\"jobs\"} 1")
	}))
	if code, body := get(t, base+"/metrics"); code != http.StatusOK || !strings.Contains(body, "htcondordb_ads") {
		t.Errorf("/metrics after the catalog = %d %q; want the real handler", code, body)
	}
	if code, body := get(t, base+"/startupz"); code != http.StatusOK || !strings.Contains(body, "serving") {
		t.Errorf("/startupz after the catalog = %d %q; want 200", code, body)
	}
}

// TestObservabilityOffWithoutAnAddress: no address, no listener -- and the nil server's methods
// are still safe to call, because main defers Close and calls SetMetrics unconditionally.
func TestObservabilityOffWithoutAnAddress(t *testing.T) {
	cfg := mkSyncCfg(t, "HTCONDORDB_ENABLE_PPROF = true\n")
	log, err := logging.New(&logging.Config{OutputPath: t.TempDir() + "/log", SkipGlobalInstall: true})
	if err != nil {
		t.Fatal(err)
	}
	obs := startObservability(cfg, log, nil)
	if obs != nil {
		t.Fatalf("got a server with no address configured")
	}
	obs.SetMetrics(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	obs.Close()
}
