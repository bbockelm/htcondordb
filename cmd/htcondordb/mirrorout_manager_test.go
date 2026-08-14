package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/server"
)

func TestResolveMirrorOutSettings(t *testing.T) {
	// Off unless a destination is named.
	if s := resolveMirrorOutSettings(mkSyncCfg(t, "")); s.enabled {
		t.Errorf("no destination should be disabled, got %+v", s)
	}
	// Enabled with the default interval.
	s := resolveMirrorOutSettings(mkSyncCfg(t, "HTCONDORDB_MIRROR_JOB_QUEUE_LOG = /tmp/jq.log\n"))
	if !s.enabled || s.outPath != "/tmp/jq.log" {
		t.Errorf("enabled = %+v, want enabled outPath=/tmp/jq.log", s)
	}
	if s.interval != 30*time.Second {
		t.Errorf("default interval = %v, want 30s", s.interval)
	}
	// Custom interval.
	s = resolveMirrorOutSettings(mkSyncCfg(t, "HTCONDORDB_MIRROR_JOB_QUEUE_LOG = /tmp/jq.log\nHTCONDORDB_MIRROR_JOB_QUEUE_INTERVAL_SECONDS = 5\n"))
	if s.interval != 5*time.Second {
		t.Errorf("interval = %v, want 5s", s.interval)
	}
}

// TestMirrorOutManagerLifecycle drives enable -> no-op re-apply -> disable, and checks the
// mirror actually writes its output while enabled.
func TestMirrorOutManagerLifecycle(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("mirror-out manager test runs unprivileged")
	}
	dir := t.TempDir()
	svc, err := server.New(server.Config{Dir: dir, Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Close() }()

	// Seed a job into the jobs table so the mirror has something to write.
	jobs, err := svc.Catalog().CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	ad := classad.New()
	ad.InsertAttrString("Key", "1.0")
	ad.InsertAttrString("MyType", "Job")
	ad.InsertAttr("ClusterId", 1)
	ad.InsertAttr("ProcId", 0)
	tx := jobs.Begin()
	tx.NewClassAd("1.0", ad)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out", "job_queue.log")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &mirrorOutManager{parent: ctx, svc: svc, logger: slog.Default()}

	// Disabled: no source.
	if err := m.apply(mkSyncCfg(t, "")); err != nil {
		t.Fatal(err)
	}
	if got := len(m.Sources()); got != 0 {
		t.Fatalf("disabled: %d sources, want 0", got)
	}

	// Enable: one source, and the output file appears with the seeded job.
	cfgBody := "HTCONDORDB_MIRROR_JOB_QUEUE_LOG = " + out + "\nHTCONDORDB_MIRROR_JOB_QUEUE_INTERVAL_SECONDS = 1\n"
	if err := m.apply(mkSyncCfg(t, cfgBody)); err != nil {
		t.Fatal(err)
	}
	srcs := m.Sources()
	if len(srcs) != 1 {
		t.Fatalf("enabled: %d sources, want 1", len(srcs))
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(out); err == nil && len(b) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, err := os.ReadFile(out)
	if err != nil || len(b) == 0 {
		t.Fatalf("mirror output not written: err=%v len=%d", err, len(b))
	}

	// Re-apply the same config: a no-op (same running mirror).
	if err := m.apply(mkSyncCfg(t, cfgBody)); err != nil {
		t.Fatal(err)
	}
	if got := len(m.Sources()); got != 1 {
		t.Fatalf("re-apply: %d sources, want 1", got)
	}

	// Disable: source stops.
	if err := m.apply(mkSyncCfg(t, "")); err != nil {
		t.Fatal(err)
	}
	if got := len(m.Sources()); got != 0 {
		t.Fatalf("disabled again: %d sources, want 0", got)
	}
}
