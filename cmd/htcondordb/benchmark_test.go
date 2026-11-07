package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/bench"
)

// syncBuffer is a bytes.Buffer safe to read while another goroutine writes it.
//
// The serving test polls the output while runBenchmark is writing it from another goroutine, and
// a bytes.Buffer is not safe for that -- which the race detector caught in CI rather than here,
// because these packages were not being run under -race locally.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Benchmark mode will not fall back to a configured directory. Requiring the flag is what keeps
// the mode from ever opening a real SPOOL, so an empty -benchmark-dir is an error and not a
// default.
func TestBenchmarkModeRequiresItsOwnDirectory(t *testing.T) {
	t.Setenv("HTCONDORDB_DIR", "/var/lib/condor/htcondordb")
	t.Setenv("SPOOL", "/var/lib/condor/spool")
	var out bytes.Buffer
	err := runBenchmark(context.Background(), benchmarkConfig{Rows: 10}, &out)
	if err == nil {
		t.Fatal("benchmark mode ran without -benchmark-dir")
	}
	if !strings.Contains(err.Error(), "benchmark-dir") {
		t.Errorf("the error does not name the missing flag: %v", err)
	}
	if strings.Contains(out.String(), "/var/lib/condor") {
		t.Errorf("benchmark mode referred to a configured directory:\n%s", out.String())
	}
}

// And it refuses a directory holding someone else's data, through the same guard the library
// uses -- the command path must not have its own, weaker, check.
func TestBenchmarkModeRefusesForeignDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "job_queue.log"), []byte("real"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := runBenchmark(context.Background(), benchmarkConfig{Dir: dir, Rows: 10}, &out)
	if !errors.Is(err, bench.ErrNotScratch) {
		t.Fatalf("error = %v, want ErrNotScratch", err)
	}
}

// Self-test mode builds, measures in process and returns, without binding anything.
func TestBenchmarkSelfTestRunsAndReturns(t *testing.T) {
	var out bytes.Buffer
	err := runBenchmark(context.Background(), benchmarkConfig{
		Dir: filepath.Join(t.TempDir(), "scratch"), Rows: 2000, SelfTest: true, Seed: 3,
	}, &out)
	if err != nil {
		t.Fatal(err)
	}
	s := out.String()
	for _, want := range []string{"SELF-TEST", "jobs", "history", "epoch_history", "produce"} {
		if !strings.Contains(s, want) {
			t.Errorf("self-test output is missing %q:\n%s", want, s)
		}
	}
	// It must say what it is not, every time, because this number is the one most likely to be
	// quoted as the server's throughput.
	if !strings.Contains(s, "not how fast a consumer receives them") {
		t.Errorf("the self-test output does not distinguish production from delivery:\n%s", s)
	}
}

// Serving mode binds both endpoints, says plainly what it has opened up, and stops with the
// context.
func TestBenchmarkServeBindsAndAnnounces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- runBenchmark(ctx, benchmarkConfig{
			Dir: filepath.Join(t.TempDir(), "scratch"), Rows: 1000,
			Listen: "127.0.0.1:0", Flight: "127.0.0.1:0", Seed: 5,
		}, &out)
	}()
	deadline := time.After(90 * time.Second)
	for !strings.Contains(out.String(), "flight sql:") {
		select {
		case err := <-done:
			t.Fatalf("returned before serving: %v\n%s", err, out.String())
		case <-deadline:
			t.Fatalf("did not bind in time:\n%s", out.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
	s := out.String()
	if !strings.Contains(s, "ANONYMOUS READS") {
		t.Errorf("serving mode does not announce that reads are anonymous:\n%s", s)
	}
	if !strings.Contains(s, "dbrpc:") {
		t.Errorf("serving mode did not announce the dbrpc address:\n%s", s)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not stop when the context was cancelled")
	}
}
