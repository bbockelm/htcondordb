package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/bbockelm/golang-htcondor/logging"
)

// timerWithEntries builds a timer whose catalog scan has already run over dir, so the progress
// tests can drive the open callbacks without opening anything.
func timerWithEntries(t *testing.T, entries ...catalogEntry) (*startupTimer, string) {
	t.Helper()
	dir := t.TempDir()
	tm := newStartupTimer(nil)
	tm.dir = dir
	tm.entries = entries
	tm.doneNames = map[string]bool{}
	return tm, dir
}

func sidecarReport(dir string, done, total int, elapsed time.Duration) collections.OpenProgress {
	return collections.OpenProgress{Dir: dir, Phase: collections.OpenLoadSidecars,
		Done: done, Total: total, Elapsed: elapsed}
}

// TestEntryLabelMapsShardDirToCatalogEntry: the storage layer reports the shard directory it is
// working in and knows nothing of table names, so the whole in-flight signal rests on mapping one
// to the other. A path outside the catalog must map to nothing rather than to a wrong entry.
func TestEntryLabelMapsShardDirToCatalogEntry(t *testing.T) {
	tm, dir := timerWithEntries(t)
	for _, tc := range []struct{ in, want string }{
		{filepath.Join(dir, "archives", "history", "0"), "archive history"},
		{filepath.Join(dir, "tables", "jobs", "7"), "table jobs"},
		{filepath.Join(dir, "tables", "jobs"), "table jobs"},
		{filepath.Join(dir, "views", "v1", "0"), ""},
		{filepath.Join(dir, "tables"), ""},
		{"/somewhere/else/tables/jobs/0", ""},
	} {
		if got := tm.entryLabel(tc.in); got != tc.want {
			t.Errorf("entryLabel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// With no catalog root there is nothing to resolve against and nothing may be invented.
	bare := newStartupTimer(nil)
	if got := bare.entryLabel(filepath.Join(dir, "tables", "jobs", "0")); got != "" {
		t.Errorf("entryLabel with no catalog dir = %q, want empty", got)
	}
}

// TestProgressCountsSegmentsRemaining is the review's first item: a beat that says "still opening"
// has to say how many segments are left inside the entry it is opening, not how many that entry
// has in total.
func TestProgressCountsSegmentsRemaining(t *testing.T) {
	tm, dir := timerWithEntries(t, catalogEntry{kind: "archive", name: "history", segs: 5278})
	shard := filepath.Join(dir, "archives", "history", "0")

	tm.tblMu.Lock()
	open, remaining := tm.outstanding()
	tm.tblMu.Unlock()
	if remaining != 5278 || open[0].detail != "" {
		t.Fatalf("before any report: remaining=%d detail=%q, want 5278 and no detail", remaining, open[0].detail)
	}

	tm.onOpenProgress(sidecarReport(shard, 3120, 5278, 300*time.Second))

	tm.tblMu.Lock()
	open, remaining = tm.outstanding()
	tm.tblMu.Unlock()
	if remaining != 5278-3120 {
		t.Errorf("segments_remaining = %d, want %d", remaining, 5278-3120)
	}
	if want := "load_sidecars 3120/5278"; open[0].detail != want {
		t.Errorf("detail = %q, want %q", open[0].detail, want)
	}
	if got := openLabels(open); !strings.Contains(got, "archive history(load_sidecars 3120/5278)") {
		t.Errorf("openLabels = %q, want it to name the phase and counts", got)
	}
	if got := tm.statusLine(); !strings.Contains(got, "segments_remaining=2158") {
		t.Errorf("statusLine = %q, want segments_remaining=2158", got)
	}
}

// TestMapSegmentsPhaseCountsDownAndFeedsItsOwnRate: every phase is one pass over the entry's
// segments, so the mapping pass counts down like the others and the passes still ahead of it are
// counted too. An entry 900 of 1000 segments into the first of two passes has 100 + 1000 left.
//
// This replaces a test that pinned the opposite -- the mapping pass counting down nothing, and
// feeding no rate -- on the premise that mapping is a cheap prologue next to the sidecar load.
// Measured on a production hub whose sidecars adopted cleanly, one archive's open was
// mapSegments=1m1.664s against loadSidecars=34.396s. With the rate and the remaining count both
// sourced only from the sidecar pass, every number in the heartbeat froze for minutes at a time
// whenever the entries in flight were mapping.
func TestMapSegmentsPhaseCountsDownAndFeedsItsOwnRate(t *testing.T) {
	tm, dir := timerWithEntries(t, catalogEntry{kind: "archive", name: "history", segs: 1000})
	shard := filepath.Join(dir, "archives", "history", "0")
	tm.onOpenProgress(collections.OpenProgress{Dir: shard, Phase: collections.OpenMapSegments,
		Done: 900, Total: 1000, Elapsed: 10 * time.Second})

	tm.tblMu.Lock()
	open, remaining := tm.outstanding()
	mapRate, mapMeasured := tm.rateFor(string(collections.OpenMapSegments))
	_, sidecarMeasured := tm.rateFor(string(collections.OpenLoadSidecars))
	tm.tblMu.Unlock()

	// 100 left in this pass, plus the whole sidecar pass still to come. rebuild_dir is not
	// counted: it has not been reported, and a clean close skips it entirely.
	if want := 100 + 1000; remaining != want {
		t.Errorf("remaining during map_segments = %d, want %d (this pass's remainder plus the "+
			"sidecar pass still ahead)", remaining, want)
	}
	if want := "map_segments 900/1000"; open[0].detail != want {
		t.Errorf("detail = %q, want %q", open[0].detail, want)
	}
	// 900 segments in 10s is 90/s, and it belongs to the mapping pass alone.
	if !mapMeasured || math.Abs(mapRate-90) > 0.01 {
		t.Errorf("map_segments rate = %v measured=%v, want 90 and true", mapRate, mapMeasured)
	}
	if sidecarMeasured {
		t.Error("a mapping sample reached the sidecar pass's average; the two costs must not blend")
	}
}

// TestRebuildDirPassIsCountedOnlyOnceObserved: the directory replay is skipped when a clean close
// left a snapshot, so counting it before it reports would overstate every healthy open -- the
// common case, and the one an operator reads the beat during.
func TestRebuildDirPassIsCountedOnlyOnceObserved(t *testing.T) {
	tm, dir := timerWithEntries(t, catalogEntry{kind: "archive", name: "history", segs: 1000})
	shard := filepath.Join(dir, "archives", "history", "0")

	tm.onOpenProgress(collections.OpenProgress{Dir: shard, Phase: collections.OpenMapSegments,
		Done: 1000, Total: 1000, Elapsed: 10 * time.Second})
	tm.tblMu.Lock()
	_, beforeReplay := tm.outstanding()
	tm.tblMu.Unlock()
	if want := 0 + 1000; beforeReplay != want {
		t.Errorf("remaining after mapping, before any replay report = %d, want %d (the sidecar "+
			"pass only; the replay may never run)", beforeReplay, want)
	}

	tm.onOpenProgress(collections.OpenProgress{Dir: shard, Phase: collections.OpenRebuildDir,
		Done: 200, Total: 1000, Elapsed: 5 * time.Second})
	tm.tblMu.Lock()
	_, duringReplay := tm.outstanding()
	replayRate, replayMeasured := tm.rateFor(string(collections.OpenRebuildDir))
	tm.tblMu.Unlock()
	if want := 800 + 1000; duringReplay != want {
		t.Errorf("remaining during the replay = %d, want %d (this pass's remainder plus the "+
			"sidecar pass)", duringReplay, want)
	}
	if !replayMeasured || math.Abs(replayRate-40) > 0.01 {
		t.Errorf("rebuild_dir rate = %v measured=%v, want 40 and true", replayRate, replayMeasured)
	}
}

// TestRateIsLearnedFromObservedProgress is the review's fourth item: the estimate must come off
// an EMA of this machine's own throughput, not off a constant measured somewhere else.
func TestRateIsLearnedFromObservedProgress(t *testing.T) {
	tm, dir := timerWithEntries(t, catalogEntry{kind: "archive", name: "history", segs: 6000})
	shard := filepath.Join(dir, "archives", "history", "0")

	tm.tblMu.Lock()
	rate, measured := tm.rateFor(string(collections.OpenLoadSidecars))
	tm.tblMu.Unlock()
	if measured || rate != seedSegmentsPerSecond {
		t.Fatalf("before any observation: rate=%v measured=%v, want the seeded prior %v and measured=false",
			rate, measured, seedSegmentsPerSecond)
	}
	if got := rateSource(false); got != "seeded" {
		t.Errorf("rateSource(false) = %q, want %q -- a seeded figure must not read as a measurement", got, "seeded")
	}

	// First report of a phase carries Done over Elapsed: 2000 segments in 20s is 100/s, five
	// times the seed. It must REPLACE the prior, not be averaged with it.
	tm.onOpenProgress(sidecarReport(shard, 2000, 6000, 20*time.Second))
	tm.tblMu.Lock()
	rate, measured = tm.rateFor(string(collections.OpenLoadSidecars))
	tm.tblMu.Unlock()
	if !measured {
		t.Fatal("rate still reads as seeded after an observation")
	}
	if math.Abs(rate-100) > 0.01 {
		t.Errorf("rate after the first sample = %v, want 100 (the sample, not a blend with the seed)", rate)
	}

	// A second report differentiates against the first: 500 more segments in 10s is 50/s, and the
	// EMA must move toward it rather than jump to it.
	tm.onOpenProgress(sidecarReport(shard, 2500, 6000, 30*time.Second))
	tm.tblMu.Lock()
	rate, _ = tm.rateFor(string(collections.OpenLoadSidecars))
	tm.tblMu.Unlock()
	if rate >= 100 || rate <= 50 {
		t.Errorf("rate after a slower sample = %v, want it between the new sample (50) and the old average (100)", rate)
	}
	want := rateEMAAlpha*50 + (1-rateEMAAlpha)*100
	if math.Abs(rate-want) > 0.5 {
		t.Errorf("rate = %v, want the EMA %v", rate, want)
	}
}

// TestRateIgnoresSamplesCarryingNoInformation: two reports with the same count do not mean the
// open is running at zero segments a second, and treating them as a measurement would send the
// estimate to infinity on exactly the shape -- a stall -- where the other fields must still read.
func TestRateIgnoresSamplesCarryingNoInformation(t *testing.T) {
	tm, dir := timerWithEntries(t, catalogEntry{kind: "archive", name: "history", segs: 6000})
	shard := filepath.Join(dir, "archives", "history", "0")
	tm.onOpenProgress(sidecarReport(shard, 2000, 6000, 20*time.Second))
	tm.onOpenProgress(sidecarReport(shard, 2000, 6000, 30*time.Second)) // nothing landed
	tm.onOpenProgress(sidecarReport(shard, 2000, 6000, 40*time.Second))

	tm.tblMu.Lock()
	rate, _ := tm.rateFor(string(collections.OpenLoadSidecars))
	est := estimateOpen(4000, rate)
	tm.tblMu.Unlock()
	if math.Abs(rate-100) > 0.01 {
		t.Errorf("rate = %v, want it unchanged at 100: an empty window is not a measurement of zero", rate)
	}
	if est <= 0 || est > time.Hour {
		t.Errorf("estimate with a stalled phase = %v, want a finite sane number", est)
	}
}

// TestCompletedEntryFeedsTheRate covers the catalog shape the in-flight reports cannot see: many
// moderate tables, each finishing inside one reporting interval. Without this the EMA would never
// leave the seeded prior however long the start ran.
func TestCompletedEntryFeedsTheRate(t *testing.T) {
	tm, _ := timerWithEntries(t,
		catalogEntry{kind: "table", name: "jobs", segs: 200},
		catalogEntry{kind: "table", name: "tiny", segs: 2})

	// A two-segment table's open is fixed costs, not throughput; it must not become a rate.
	tm.recordTableOpen("table", "tiny", 2*time.Second)
	tm.tblMu.Lock()
	_, measured := tm.rateFor(string(collections.OpenLoadSidecars))
	tm.tblMu.Unlock()
	if measured {
		t.Errorf("a %d-segment table fed the EMA; entries below %d segments must not",
			2, minRateSampleSegments)
	}

	// 200 segments in 4s is 50/s and is worth having.
	tm.recordTableOpen("table", "jobs", 4*time.Second)
	tm.tblMu.Lock()
	rate, measured := tm.rateFor(string(collections.OpenLoadSidecars))
	tm.tblMu.Unlock()
	if !measured || math.Abs(rate-50) > 0.01 {
		t.Errorf("after a completed 200-segment table: rate=%v measured=%v, want 50 and true", rate, measured)
	}
}

// TestOutstandingOrdersByWorkRemaining: the estimate the beat prints is the head of this list, so
// an entry that is nearly done must not outrank a smaller one that has not started.
func TestOutstandingOrdersByWorkRemaining(t *testing.T) {
	tm, dir := timerWithEntries(t,
		catalogEntry{kind: "archive", name: "history", segs: 5000},
		catalogEntry{kind: "archive", name: "epoch_history", segs: 900})
	tm.onOpenProgress(sidecarReport(filepath.Join(dir, "archives", "history", "0"), 4900, 5000, 400*time.Second))

	tm.tblMu.Lock()
	open, _ := tm.outstanding()
	tm.tblMu.Unlock()
	if open[0].name != "epoch_history" {
		t.Errorf("head of the outstanding list = %q, want epoch_history: 900 segments untouched is more "+
			"work left than 100 of 5000", open[0].name)
	}
}

// TestProgressAfterStopIsDropped: the storage layer's hook is a process global that stays
// installed, so a maintenance pass reopening a collection hours later still calls it. Those
// reports must not reanimate a finished start.
func TestProgressAfterStopIsDropped(t *testing.T) {
	tm, dir := timerWithEntries(t, catalogEntry{kind: "archive", name: "history", segs: 5000})
	tm.stopProgress()
	tm.onOpenProgress(sidecarReport(filepath.Join(dir, "archives", "history", "0"), 1000, 5000, 10*time.Second))

	tm.tblMu.Lock()
	open, remaining := tm.outstanding()
	_, measured := tm.rateFor(string(collections.OpenLoadSidecars))
	tm.tblMu.Unlock()
	if remaining != 5000 || open[0].detail != "" {
		t.Errorf("a post-stop report changed the start's numbers: remaining=%d detail=%q",
			remaining, open[0].detail)
	}
	if measured {
		t.Error("a post-stop report fed the rate EMA")
	}
}

// TestProgressCallbacksAreRaceFree: the hook is called from every goroutine opening a table while
// the heartbeat ticker and /startupz read the same state. Meaningful only under -race.
func TestProgressCallbacksAreRaceFree(t *testing.T) {
	tm, dir := timerWithEntries(t,
		catalogEntry{kind: "archive", name: "history", segs: 5000},
		catalogEntry{kind: "archive", name: "epoch_history", segs: 7000},
		catalogEntry{kind: "table", name: "jobs", segs: 100})

	var wg sync.WaitGroup
	for _, name := range []string{"history", "epoch_history"} {
		for shard := 0; shard < 4; shard++ {
			wg.Add(1)
			go func(name string, shard int) {
				defer wg.Done()
				d := filepath.Join(dir, "archives", name, fmt.Sprint(shard))
				for i := 1; i <= 50; i++ {
					tm.onOpenProgress(sidecarReport(d, i*10, 500, time.Duration(i)*time.Second))
				}
			}(name, shard)
		}
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = tm.statusLine()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			tm.recordTableOpen("table", "jobs", 3*time.Second)
		}
	}()
	wg.Wait()

	if !strings.Contains(tm.statusLine(), "rate_source=measured") {
		t.Errorf("statusLine never reported a measured rate: %s", tm.statusLine())
	}
}

// TestHeartbeatReportsTheMeasuredRate: the fields an operator reads have to carry both the rate
// and where it came from, or an estimate built on a constant is indistinguishable from one built
// on the machine it is running on.
func TestHeartbeatReportsTheMeasuredRate(t *testing.T) {
	logPath, logger := progressLogger(t)
	prev := progressEvery
	progressEvery = 10 * time.Millisecond
	defer func() { progressEvery = prev }()

	dir := t.TempDir()
	shard := filepath.Join(dir, "archives", "history", "0")
	if err := os.MkdirAll(shard, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := os.WriteFile(filepath.Join(shard, fmt.Sprintf("seg-%d.d0.dat", i)), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	tm := newStartupTimer(logger)
	tm.startProgress(dir)
	waitForLog(t, logPath, "rate_source=seeded")
	tm.onOpenProgress(sidecarReport(shard, 12, 20, 2*time.Second))
	waitForLog(t, logPath, "rate_source=measured")
	waitForLog(t, logPath, "segments_remaining=8")
	tm.stopProgress()
}

// progressLogger is an Info-level logger writing to a file the test can read back. The zero
// Verbosity is VerbosityError, which would drop every line the heartbeat emits and leave a test
// asserting against an empty file.
func progressLogger(t *testing.T) (string, *logging.Logger) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log")
	logger, err := logging.New(&logging.Config{
		OutputPath:        path,
		SkipGlobalInstall: true,
		DefaultLevel:      logging.VerbosityInfo,
		DestinationLevels: map[logging.Destination]logging.Verbosity{
			logging.DestinationGeneral: logging.VerbosityInfo,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return path, logger
}
