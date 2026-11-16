package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/logging"
)

func TestScanCatalogEntries(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"tables/jobs", "tables/ads", "tables/federation_sources", "archives/history"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Segment files, including one in a shard subdirectory: the walk has to reach it, because an
	// archive's segments live under its shards and that is the whole size signal.
	if err := os.MkdirAll(filepath.Join(dir, "archives", "history", "d0"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"archives/history/seg-1.dat", "archives/history/d0/seg-2.dat", "tables/jobs/seg-1.dat"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A stray file is not a table, and a non-.dat file is not a segment.
	for _, f := range []string{"tables/NOTES", "archives/history/archiveconfig.json"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got := scanCatalogEntries(dir)
	segs := map[string]int{}
	for _, e := range got {
		segs[e.label()] = e.segs
	}
	if len(got) != 4 {
		t.Fatalf("scanCatalogEntries returned %d entries (%v), want 4", len(got), segs)
	}
	if n := segs["archive history"]; n != 2 {
		t.Errorf("archive history = %d segments, want 2 (one of them under a shard directory)", n)
	}
	if n := segs["table jobs"]; n != 1 {
		t.Errorf("table jobs = %d segments, want 1", n)
	}
	if n, ok := segs["table ads"]; !ok || n != 0 {
		t.Errorf("table ads = %d segments (present=%v), want 0 and present", n, ok)
	}

	// A catalog that does not exist yet is a new database, not an error.
	if got := scanCatalogEntries(filepath.Join(dir, "nope")); len(got) != 0 {
		t.Errorf("missing catalog returned %d entries, want 0", len(got))
	}
}

// TestEstimateOpenScalesWithSegments: the estimate is the segment count over the rate, so an
// operator reading it can check it against the counts and the rate printed beside it.
func TestEstimateOpenScalesWithSegments(t *testing.T) {
	if got := estimateOpen(0, seedSegmentsPerSecond); got != 0 {
		t.Errorf("estimateOpen(0) = %v, want 0", got)
	}
	if got, want := estimateOpen(600, 10), time.Minute; got != want {
		t.Errorf("estimateOpen(600 segs, 10/s) = %v, want %v", got, want)
	}
	if estimateOpen(7007, 10) <= estimateOpen(5278, 10) {
		t.Error("a larger archive must not estimate shorter")
	}
	// The whole point of measuring the rate: the same work on a machine twice as fast must
	// estimate half as long, rather than reporting the seeded figure whatever the machine does.
	if got, want := estimateOpen(600, 20), 30*time.Second; got != want {
		t.Errorf("estimateOpen(600 segs, 20/s) = %v, want %v", got, want)
	}
	// A rate nothing has established yet cannot produce a number; it must not produce a
	// divide-by-zero one either.
	if got := estimateOpen(600, 0); got != 0 {
		t.Errorf("estimateOpen with no rate = %v, want 0", got)
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

// The overwhelmingly common case is a start that finishes in well under one beat. It must stay
// silent: a line on every normal startup is noise, and noise is what stops people reading the ones
// that matter.
func TestStartProgressSilentOnAFastStart(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "log")
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

	timer := newStartupTimer(logger)
	timer.startProgress(t.TempDir())
	timer.recordTableOpen("table", "jobs", time.Millisecond)
	timer.stopProgress()

	b, _ := os.ReadFile(logPath)
	if strings.Contains(string(b), "opening the catalog") {
		t.Errorf("a start shorter than one beat still logged progress:\n%s", string(b))
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
