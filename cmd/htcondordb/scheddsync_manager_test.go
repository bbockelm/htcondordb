package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/server"
)

// mkSyncCfg builds a config from body alone, with SkipDefaults so HTCondor's baked-in
// param defaults do NOT leak in. Without this, unresolved knobs like JOB_QUEUE_LOG,
// HISTORY, and JOB_EPOCH_HISTORY silently default to $(SPOOL)/... paths and conjure
// phantom sync sources -- so a test asserting "N sources" or "no sources -> error" would
// pass or fail on defaults it never set, not on the behavior it means to exercise. The
// manager's production path still loads the full config (defaults included); these are unit
// tests of apply(), which must see exactly the config each case declares.
func mkSyncCfg(t *testing.T, body string) *config.Config {
	t.Helper()
	cfg, err := config.NewFromReaderWithOptions(strings.NewReader(body), config.ConfigOptions{
		Subsystem:    "HTCONDORDB",
		SkipDefaults: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// sourceFile waits briefly for a freshly-started tailer to report the file it is
// following (Status().Source is populated on its first poll).
func sourceFile(t *testing.T, m *scheddSyncManager) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srcs := m.Sources(); len(srcs) == 1 {
			if s := srcs[0].Status().Source; s != "" {
				return s
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if srcs := m.Sources(); len(srcs) == 1 {
		return srcs[0].Status().Source
	}
	return ""
}

// TestScheddSyncManagerReconcile drives the reconcile state machine: enabling,
// no-op re-apply, a JOB_QUEUE_LOG change (tailers restarted on the new path),
// and disabling -- the behavior condor_reconfig now gets.
func TestScheddSyncManagerReconcile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	dir := t.TempDir()
	svc, err := server.New(server.Config{Dir: dir, Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()

	logDir := t.TempDir()
	jobA := filepath.Join(logDir, "job_queue_a.log")
	jobB := filepath.Join(logDir, "job_queue_b.log")
	for _, f := range []string{jobA, jobB} {
		// A valid minimal job_queue.log record. (The syncer now persists its resume position
		// under the DB dir even for SPOOL-only configs, so startup reconciles the log rather
		// than replaying blindly -- the content must parse.)
		if err := os.WriteFile(f, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &scheddSyncManager{parent: ctx, svc: svc, logger: slog.Default()}

	// Disabled: no tailers.
	if err := m.apply(mkSyncCfg(t, "")); err != nil {
		t.Fatal(err)
	}
	if got := len(m.Sources()); got != 0 {
		t.Fatalf("disabled: %d sources, want 0", got)
	}

	// Enable, tailing jobA.
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobA+"\n")); err != nil {
		t.Fatal(err)
	}
	srcs := m.Sources()
	if len(srcs) != 1 {
		t.Fatalf("enabled: %d sources, want 1", len(srcs))
	}
	if src := sourceFile(t, m); src != jobA {
		t.Errorf("source = %q, want %q", src, jobA)
	}
	first := srcs[0]

	// Re-apply the same config: a no-op, the same tailer keeps running.
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobA+"\n")); err != nil {
		t.Fatal(err)
	}
	if again := m.Sources(); len(again) != 1 || again[0] != first {
		t.Error("re-applying identical config restarted the tailer (should be a no-op)")
	}

	// Change the path: the old tailer stops and a new one starts on jobB.
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobB+"\n")); err != nil {
		t.Fatal(err)
	}
	srcs = m.Sources()
	if len(srcs) != 1 {
		t.Fatalf("after path change: %d sources, want 1", len(srcs))
	}
	if srcs[0] == first {
		t.Error("path change did not restart the tailer")
	}
	if src := sourceFile(t, m); src != jobB {
		t.Errorf("after path change source = %q, want %q", src, jobB)
	}

	// Disable: tailers stop.
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = false\n")); err != nil {
		t.Fatal(err)
	}
	if got := len(m.Sources()); got != 0 {
		t.Fatalf("disabled again: %d sources, want 0", got)
	}
}

// TestScheddSyncManagerStopWaitsForTailer is the regression for the shutdown crash:
// a tailer mid-commit writes the collections' mmap'd segments, and Catalog.Close
// munmaps them, so a SIGSEGV (segment.append) results if shutdown does not WAIT for
// the tailer to exit before closing. Cancelling the daemon context alone does not
// join the goroutine; Stop must. This asserts the tailer's done channel is closed
// by the time Stop returns.
func TestScheddSyncManagerStopWaitsForTailer(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	dir := t.TempDir()
	svc, err := server.New(server.Config{Dir: dir, Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()

	logDir := t.TempDir()
	jobLog := filepath.Join(logDir, "job_queue.log")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	m := &scheddSyncManager{parent: context.Background(), svc: svc, logger: slog.Default()}
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobLog+"\n")); err != nil {
		t.Fatal(err)
	}
	if len(m.Sources()) != 1 {
		t.Fatalf("enabled: %d sources, want 1", len(m.Sources()))
	}

	// Capture the tailers' done channel before Stop nils it. Stop must not return
	// until this is closed (every tailer goroutine has exited) -- otherwise a
	// concurrent write could still be in flight when the catalog is closed.
	done := m.done
	if done == nil {
		t.Fatal("no done channel after enabling schedd-sync")
	}

	m.Stop()

	select {
	case <-done:
		// closed: the tailer goroutine exited before Stop returned. Good.
	default:
		t.Fatal("Stop returned before the tailer goroutine exited; a mid-commit write could race Catalog.Close (SIGSEGV)")
	}
	if m.cancel != nil || m.done != nil {
		t.Error("Stop did not reset the manager's running state")
	}
	if got := len(m.Sources()); got != 0 {
		t.Errorf("after Stop: %d sources, want 0", got)
	}
	m.Stop() // idempotent: safe to call again
}

// TestScheddSyncManagerStopWaitsForIndexBackfill verifies Stop (and thus daemon shutdown) waits
// for an in-flight archive index backfill before returning -- so the catalog is not closed
// (segments munmapped) while AddIndex is still writing them. The backfill runs detached but is
// registered on the manager's WaitGroup; a regression that de-registers it would let Stop return
// early and this test would fail.
func TestScheddSyncManagerStopWaitsForIndexBackfill(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	// Block the backfill goroutine until we release it, and signal when it has started.
	started := make(chan struct{})
	release := make(chan struct{})
	archiveBackfillHook = func() { close(started); <-release }
	t.Cleanup(func() { archiveBackfillHook = nil })

	dir := t.TempDir()
	svc, err := server.New(server.Config{Dir: dir, Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()

	// Pre-create the history archive with NO categorical index and one record, so the manager's
	// reconcile finds the configured Owner index missing and schedules a backfill.
	hist, err := svc.Catalog().CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := hist.AppendOld("ClusterId = 1\nOwner = \"alice\""); err != nil {
		t.Fatal(err)
	}
	histFile := filepath.Join(t.TempDir(), "history")
	if err := os.WriteFile(histFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	m := &scheddSyncManager{parent: context.Background(), svc: svc, logger: slog.Default()}
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+
		"\nJOB_QUEUE_LOG =\nHTCONDORDB_HISTORY = "+histFile+"\n")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("index backfill did not start")
	}

	// Stop must block while the backfill is in flight.
	stopped := make(chan struct{})
	go func() { m.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while the index backfill was still in flight (catalog could be closed mid-AddIndex)")
	case <-time.After(200 * time.Millisecond):
	}

	// Release the backfill; Stop must now return.
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the backfill completed")
	}
}

// TestScheddSyncManagerEnabledNoPaths verifies the misconfiguration guard.
func TestScheddSyncManagerEnabledNoPaths(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	dir := t.TempDir()
	svc, err := server.New(server.Config{Dir: dir, Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()

	m := &scheddSyncManager{parent: context.Background(), svc: svc, logger: slog.Default()}
	// Enabled but JOB_QUEUE_LOG/HISTORY forced empty (override the params to blank).
	err = m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"\nJOB_QUEUE_LOG =\nHISTORY =\n"))
	if err == nil {
		t.Fatal("enabled with no sources should error")
	}
}
