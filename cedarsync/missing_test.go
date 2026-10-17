package cedarsync

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestMissingSourceTable: a source without the watched table (an optional one, such as job_metrics
// on a spoke that does not sample) is reported as missing -- SourceMissing, a LastError saying so,
// no session -- and asked again at MissingRetry, not at the error backoff's cadence. Once the table
// exists the Runner streams it and the flag clears.
func TestMissingSourceTable(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	_, central := mustArchive(t, "central")
	sink, err := replicate.NewArchiveSink(central, "ap1", &replicate.MemCursorStore{})
	if err != nil {
		t.Fatal(err)
	}
	inner := dialFor(t, cat)
	var dials atomic.Int32
	dial := func(ctx context.Context) (*dbrpc.Client, func(), error) {
		dials.Add(1)
		return inner(ctx)
	}
	r, err := NewRunner(dial, Config{Source: "job_metrics", Src: "ap1", MissingRetry: 2 * time.Second}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, 5*time.Second, func() bool { return r.Status().SourceMissing })
	st := r.Status()
	if st.Connected || st.Sessions != 0 || st.Synced {
		t.Errorf("missing table: Connected=%v Sessions=%d Synced=%v, want no session", st.Connected, st.Sessions, st.Synced)
	}
	if !strings.Contains(st.LastError, "no such table") || !strings.Contains(st.LastError, "job_metrics") {
		t.Errorf("LastError = %q, want it to name the missing table", st.LastError)
	}
	// The error backoff would dial at 0, 0.5s and 1.5s; the missing-table retry at 0 and 2s.
	time.Sleep(time.Until(start.Add(1600 * time.Millisecond)))
	if n := dials.Load(); n != 1 {
		t.Errorf("%d dials in 1.6s for a missing table, want 1: the Runner is retrying at the error backoff", n)
	}

	// The table appears (the source turned it on): the next attempt streams it.
	a, err := cat.CreateArchiveTable("job_metrics", db.ArchiveConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.AppendOld(`Key = "1.0"; SampleTime = 1`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return central.Count() == 1 && r.Status().Synced })
	if st := r.Status(); st.SourceMissing || st.LastError != "" {
		t.Errorf("after the table appeared: SourceMissing=%v LastError=%q", st.SourceMissing, st.LastError)
	}
}

// TestMissingNotStickyAcrossOtherErrors: once a table was reported missing, a later attempt
// that fails for another reason (the spoke is down) must not keep reporting "missing" -- the hub
// renders SourceMissing as "<Table>Missing, the spoke has no such table" and hides LastError behind
// it, so an unreachable spoke reads as "job metrics not enabled".
func TestMissingNotStickyAcrossOtherErrors(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	_, central := mustArchive(t, "central")
	sink, err := replicate.NewArchiveSink(central, "ap1", &replicate.MemCursorStore{})
	if err != nil {
		t.Fatal(err)
	}
	inner := dialFor(t, cat)
	var down atomic.Bool
	dial := func(ctx context.Context) (*dbrpc.Client, func(), error) {
		if down.Load() {
			return nil, nil, errors.New("connection refused")
		}
		return inner(ctx)
	}
	r, err := NewRunner(dial, Config{Source: "job_metrics", Src: "ap1", MissingRetry: 200 * time.Millisecond}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, 5*time.Second, func() bool { return r.Status().SourceMissing })
	down.Store(true)
	waitFor(t, 5*time.Second, func() bool { return r.Status().LastError == "connection refused" })
	if st := r.Status(); st.SourceMissing {
		t.Errorf("spoke unreachable: SourceMissing still true (LastError %q)", st.LastError)
	}
}

// TestMissingSourceTableWithoutRetry: without MissingRetry a missing table is an ordinary session
// error, exactly as before the option existed -- the server's own message in LastError, a warning
// and the error backoff on every attempt, no SourceMissing. Replication of a mistyped table must
// stay loud.
func TestMissingSourceTableWithoutRetry(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	_, central := mustArchive(t, "central")
	sink, err := replicate.NewArchiveSink(central, "ap1", &replicate.MemCursorStore{})
	if err != nil {
		t.Fatal(err)
	}
	inner := dialFor(t, cat)
	var dials atomic.Int32
	dial := func(ctx context.Context) (*dbrpc.Client, func(), error) {
		dials.Add(1)
		return inner(ctx)
	}
	r, err := NewRunner(dial, Config{Source: "job_metrics", Src: "ap1"}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, 5*time.Second, func() bool { return r.Status().LastError != "" })
	time.Sleep(time.Until(start.Add(1600 * time.Millisecond)))
	st := r.Status()
	if st.SourceMissing {
		t.Error("SourceMissing set without MissingRetry")
	}
	if st.LastError != "dbrpc: no such table: job_metrics" {
		t.Errorf("LastError = %q, want the server's error unchanged", st.LastError)
	}
	// The error backoff dials at 0, 0.5s and 1.5s.
	if n := dials.Load(); n < 3 {
		t.Errorf("%d dials in 1.6s, want the error backoff's 3", n)
	}
}
