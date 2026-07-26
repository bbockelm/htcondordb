package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fakeCatalog struct {
	mu    sync.Mutex
	defs  []db.ExporterDef
	state map[string][]byte
}

func (c *fakeCatalog) Exporters() []db.ExporterDef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]db.ExporterDef(nil), c.defs...)
}
func (c *fakeCatalog) set(d []db.ExporterDef) {
	c.mu.Lock()
	c.defs = d
	c.mu.Unlock()
}
func (c *fakeCatalog) LoadExporterState(name string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.state[name]
	return b, ok, nil
}

func waitForFile(t *testing.T, path string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("file %s did not appear within %s", path, d)
}

func TestExporterBinaryResolution(t *testing.T) {
	s := exporterSettings{kafkaBin: "/opt/kafkasync", opensearchBin: "/opt/opensearchsync"}
	if got := s.binaryFor("kafka"); got != "/opt/kafkasync" {
		t.Errorf("kafka -> %q, want override", got)
	}
	if got := s.binaryFor("opensearch"); got != "/opt/opensearchsync" {
		t.Errorf("opensearch -> %q, want override", got)
	}
	if got := (exporterSettings{}).binaryFor("mystery"); got != "" {
		t.Errorf("unknown kind -> %q, want \"\"", got)
	}
}

// TestExporterManagerSupervises verifies the reconcile loop launches a registered exporter's
// binary, restarts it (a fast-exiting fake), stops it when the exporter is dropped, and shuts
// down cleanly on cancel.
func TestExporterManagerSupervises(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "launched")
	countFile := filepath.Join(dir, "count")
	script := filepath.Join(dir, "fakeexp")
	// The fake exporter records that it launched (and appends to a count file so we can see
	// restarts), then exits immediately -- exercising the supervisor's restart path.
	body := "#!/bin/sh\ntouch \"" + marker + "\"\necho x >> \"" + countFile + "\"\nexit 0\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	cat := &fakeCatalog{defs: []db.ExporterDef{{Name: "jobs", Kind: "opensearch"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &exporterManager{parent: ctx, catalog: cat, logger: discardLogger(), daemonAddr: "<127.0.0.1:1>"}
	s := exporterSettings{enabled: true, opensearchBin: script, runAsUser: "", pollInterval: 50 * time.Millisecond}

	done := make(chan struct{})
	go m.reconcileLoop(ctx, s, done)

	// It launched the registered exporter.
	waitForFile(t, marker, 3*time.Second)

	// It restarts a fast-exiting child (backoff starts at 1s): a second launch appears.
	waitForCount(t, countFile, 2, 5*time.Second)

	// Drop the exporter; its supervisor should stop and the loop stays healthy.
	cat.set(nil)
	time.Sleep(200 * time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile loop did not shut down after cancel")
	}
}

func waitForCount(t *testing.T, path string, want int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			n := 0
			for _, c := range b {
				if c == '\n' {
					n++
				}
			}
			if n >= want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("count in %s did not reach %d within %s", path, want, d)
}

// TestExporterLivenessRestartsWedgedChild verifies the liveness monitor restarts a child that
// runs but never advances its status beat (a deadlocked-but-alive process the supervisor would
// otherwise never see exit).
func TestExporterLivenessRestartsWedgedChild(t *testing.T) {
	dir := t.TempDir()
	countFile := filepath.Join(dir, "count")
	script := filepath.Join(dir, "wedged")
	// A child that records its launch then hangs forever without ever writing a status beat.
	// `exec` replaces the shell so the hang is the direct child (no grandchild left holding the
	// stdout pipe open after a kill) -- the real exporter is likewise a single process.
	body := "#!/bin/sh\necho x >> \"" + countFile + "\"\nexec sleep 3600\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	// No state written for this exporter -> the child never has a beat -> it looks wedged.
	cat := &fakeCatalog{defs: []db.ExporterDef{{Name: "jobs", Kind: "opensearch"}}, state: map[string][]byte{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &exporterManager{parent: ctx, catalog: cat, logger: discardLogger(), daemonAddr: "<127.0.0.1:1>"}
	s := exporterSettings{enabled: true, opensearchBin: script, runAsUser: "", pollInterval: 50 * time.Millisecond, livenessTimeout: 300 * time.Millisecond}

	done := make(chan struct{})
	go m.reconcileLoop(ctx, s, done)

	// The wedged child is killed after ~livenessTimeout and relaunched, so a second launch appears.
	waitForCount(t, countFile, 2, 8*time.Second)

	// The daemon tracked at least one restart for it.
	waitFor2(t, func() bool {
		for _, st := range m.Statuses() {
			if st.Name == "jobs" && st.Restarts >= 1 {
				return true
			}
		}
		return false
	}, 3*time.Second)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile loop did not shut down")
	}
}

func waitFor2(t *testing.T, cond func() bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
