package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/logging"
)

func TestCountCatalogEntries(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"tables/jobs", "tables/ads", "tables/federation_sources", "archives/history"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A stray file is not a table; only directories count.
	if err := os.WriteFile(filepath.Join(dir, "tables", "NOTES"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if tables, archives := countCatalogEntries(dir); tables != 3 || archives != 1 {
		t.Fatalf("countCatalogEntries = %d tables, %d archives; want 3, 1", tables, archives)
	}

	// A catalog that does not exist yet is a new database, not an error.
	if tables, archives := countCatalogEntries(filepath.Join(dir, "nope")); tables != 0 || archives != 0 {
		t.Errorf("missing catalog = %d, %d; want 0, 0", tables, archives)
	}
}

// The heartbeat exists to tell a slow open apart from a stuck one, so both shapes have to be
// distinguishable in the log.
func TestStartProgressDistinguishesStalledFromProgressing(t *testing.T) {
	prev := progressEvery
	progressEvery = 10 * time.Millisecond
	defer func() { progressEvery = prev }()

	logPath := filepath.Join(t.TempDir(), "log")
	// The zero value of Verbosity is VerbosityError, which would drop every Info line the
	// heartbeat emits and leave this test asserting against an empty file.
	logger, err := logging.New(&logging.Config{
		OutputPath:        logPath,
		SkipGlobalInstall: true,
		DefaultLevel:      logging.VerbosityInfo,
		DestinationLevels: map[logging.Destination]logging.Verbosity{
			logging.DestinationGeneral: logging.VerbosityInfo,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tables", "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}

	timer := newStartupTimer(logger)
	timer.startProgress(dir)

	// Nothing completes: the beats must say so rather than going quiet.
	waitForLog(t, logPath, "nothing finished since the last report")

	// Something completes: the next beat must report it, so an operator can see it moving.
	timer.recordTableOpen("table", "jobs", time.Millisecond)
	waitForLog(t, logPath, "opening the catalog")

	timer.stopProgress()

	// After the stop, no further beat may appear -- the daemon is about to announce itself.
	before, _ := os.ReadFile(logPath)
	time.Sleep(50 * time.Millisecond)
	after, _ := os.ReadFile(logPath)
	if len(after) != len(before) {
		t.Errorf("heartbeat kept logging after stopProgress:\n%s", string(after[len(before):]))
	}
}

// stopProgress on a timer that never started must not panic: the failure paths defer it.
func TestStopProgressWithoutStart(t *testing.T) {
	newStartupTimer(nil).stopProgress()
	var nilTimer *startupTimer
	nilTimer.stopProgress()
	nilTimer.startProgress("/nonexistent")
}

func waitForLog(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && strings.Contains(string(b), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	b, _ := os.ReadFile(path)
	t.Fatalf("log never contained %q; got:\n%s", want, string(b))
}
