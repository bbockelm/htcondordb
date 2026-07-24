package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/server"
)

func mkSyncCfg(t *testing.T, body string) *config.Config {
	t.Helper()
	cfg, err := config.NewFromReaderWithOptions(strings.NewReader(body), config.ConfigOptions{Subsystem: "HTCONDORDB"})
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
		if err := os.WriteFile(f, []byte("103\n"), 0o644); err != nil {
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
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobA+"\n")); err != nil {
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
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobA+"\n")); err != nil {
		t.Fatal(err)
	}
	if again := m.Sources(); len(again) != 1 || again[0] != first {
		t.Error("re-applying identical config restarted the tailer (should be a no-op)")
	}

	// Change the path: the old tailer stops and a new one starts on jobB.
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobB+"\n")); err != nil {
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
	err = m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nJOB_QUEUE_LOG =\nHISTORY =\n"))
	if err == nil {
		t.Fatal("enabled with no sources should error")
	}
}
