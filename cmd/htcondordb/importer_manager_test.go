package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/historyimport"
)

func quietManager() *importerManager {
	return &importerManager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// TestImporterLivenessRestartsStaleBeat: a runner whose status beat has gone stale
// is cancelled (which drives the supervisor's restart), and its last-read status
// is recorded for the ad.
func TestImporterLivenessRestartsStaleBeat(t *testing.T) {
	statusFile := filepath.Join(t.TempDir(), "s.json")
	if err := historyimport.WriteStatusFile(statusFile, historyimport.Status{Beat: time.Now().Add(-time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	m := quietManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	restarted := make(chan struct{})
	timeout := 300 * time.Millisecond
	// start in the past so the grace window has already elapsed.
	go m.monitorLiveness(ctx, func() { close(restarted) }, "j", statusFile, time.Now().Add(-timeout), timeout)

	select {
	case <-restarted:
	case <-time.After(3 * time.Second):
		t.Fatal("stale beat did not trigger a restart")
	}
	if ss := m.Statuses(); len(ss) != 1 || ss[0].Name != "j" {
		t.Errorf("expected the stale runner's status recorded, got %+v", ss)
	}
}

// TestImporterLivenessKeepsBeatingChild: a runner that keeps beating is never
// restarted.
func TestImporterLivenessKeepsBeatingChild(t *testing.T) {
	statusFile := filepath.Join(t.TempDir(), "s.json")
	m := quietManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A goroutine that keeps the beat fresh, like a live runner. The beat is whole
	// seconds (the on-wire contract), so the liveness timeout must sit comfortably
	// above one second of truncation error -- as it does in production (90s).
	beatCtx, stopBeat := context.WithCancel(context.Background())
	beatDone := make(chan struct{})
	go func() {
		defer close(beatDone)
		tk := time.NewTicker(250 * time.Millisecond)
		defer tk.Stop()
		for {
			_ = historyimport.WriteStatusFile(statusFile, historyimport.Status{Beat: time.Now().Unix()})
			select {
			case <-beatCtx.Done():
				return
			case <-tk.C:
			}
		}
	}()

	restarted := make(chan struct{})
	go m.monitorLiveness(ctx, func() { close(restarted) }, "j", statusFile, time.Now(), 2*time.Second)

	var failed bool
	select {
	case <-restarted:
		failed = true
	case <-time.After(3 * time.Second):
		// good: survived well past the liveness timeout while beating
	}
	// Stop the beat goroutine and wait for it before t.TempDir cleanup runs, so no
	// write lands in the temp dir mid-removal.
	stopBeat()
	<-beatDone
	if failed {
		t.Fatal("a continuously-beating runner should not be restarted")
	}
}

func importerCfg(t *testing.T, body string) *config.Config {
	t.Helper()
	cfg, err := config.NewFromReaderWithOptions(strings.NewReader(body), config.ConfigOptions{Subsystem: "HTCONDORDB"})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

func TestResolveImporterSettings(t *testing.T) {
	t.Run("configured jobs enable the manager", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `
HTCONDORDB_HISTORY_IMPORT = ospool cm_east
HTCONDORDB_HISTORY_IMPORT_OSPOOL_POOL = cm.osg-htc.org:9618
HTCONDORDB_HISTORY_IMPORT_CM_EAST_POOL = cm-east.example:9618
HTCONDORDB_HISTORY_IMPORT_USER = importer_svc
HTCONDORDB_HISTORY_IMPORT_SHUTDOWN_SECONDS = 45
`))
		if !s.enabled {
			t.Fatal("should be enabled with jobs configured")
		}
		if s.user != "importer_svc" {
			t.Errorf("user = %q, want importer_svc", s.user)
		}
		if s.gracefulTimeout != 45*time.Second {
			t.Errorf("grace = %v, want 45s", s.gracefulTimeout)
		}
		if !reflect.DeepEqual(s.jobs, []string{"cm_east", "ospool"}) { // sorted
			t.Errorf("jobs = %v, want [cm_east ospool]", s.jobs)
		}
	})

	t.Run("default user is condor", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `
HTCONDORDB_HISTORY_IMPORT = j1
HTCONDORDB_HISTORY_IMPORT_J1_POOL = p:9618
`))
		if s.user != "condor" {
			t.Errorf("default user = %q, want condor", s.user)
		}
	})

	t.Run("no jobs disables", func(t *testing.T) {
		if s := resolveImporterSettings(importerCfg(t, ``)); s.enabled {
			t.Error("no jobs should leave the manager disabled")
		}
	})

	t.Run("MANAGE=false disables", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `
HTCONDORDB_MANAGE_HISTORY_IMPORT = false
HTCONDORDB_HISTORY_IMPORT = j1
HTCONDORDB_HISTORY_IMPORT_J1_POOL = p:9618
`))
		if s.enabled {
			t.Error("HTCONDORDB_MANAGE_HISTORY_IMPORT=false should disable the manager")
		}
	})

	t.Run("a job missing POOL disables (invalid config)", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `HTCONDORDB_HISTORY_IMPORT = j1`))
		if s.enabled {
			t.Error("an invalid import config should disable rather than enable")
		}
	})
}

func TestImporterSettingsEqual(t *testing.T) {
	a := importerSettings{enabled: true, user: "condor", jobs: []string{"a", "b"}, gracefulTimeout: time.Second}
	b := importerSettings{enabled: true, user: "condor", jobs: []string{"a", "b"}, gracefulTimeout: time.Second}
	if !a.equal(b) {
		t.Error("identical settings should be equal")
	}
	b.jobs = []string{"a", "c"}
	if a.equal(b) {
		t.Error("different job sets should not be equal (drives reconfigure restart)")
	}
}
