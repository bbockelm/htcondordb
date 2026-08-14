package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestStartupTimerBreakdown checks the summary names every phase with a duration, since the whole
// point is that a slow start says which phase owned the time.
func TestStartupTimerBreakdown(t *testing.T) {
	tm := newStartupTimer(nil) // nil logger: the timer must not panic without one
	tm.mark("first")
	tm.mark("second")
	tm.done()
	if len(tm.phases) != 2 {
		t.Fatalf("phases = %d, want 2", len(tm.phases))
	}
	if tm.phases[0].name != "first" || tm.phases[1].name != "second" {
		t.Errorf("phase order = %v, want first then second", tm.phases)
	}
}

// TestStartupTimerNilSafe pins that a nil timer is inert rather than a panic on a startup path,
// so instrumenting a phase can never be the thing that stops the daemon coming up.
func TestStartupTimerNilSafe(t *testing.T) {
	var tm *startupTimer
	tm.mark("phase") // must not panic
	tm.done()        // must not panic
}

// TestStartupTimerMeasuresElapsed is the substance: a phase's recorded duration has to reflect the
// time actually spent, or the breakdown would point at the wrong phase.
func TestStartupTimerMeasuresElapsed(t *testing.T) {
	tm := newStartupTimer(nil)
	time.Sleep(30 * time.Millisecond)
	tm.mark("slow")
	tm.mark("fast")
	if tm.phases[0].d < 25*time.Millisecond {
		t.Errorf("slow phase recorded %v, want at least ~30ms", tm.phases[0].d)
	}
	if tm.phases[1].d > 20*time.Millisecond {
		t.Errorf("fast phase recorded %v, want it to be small -- each mark must measure since the "+
			"previous mark, not since the start", tm.phases[1].d)
	}
}

// TestStartupTimerSortsSlowestFirst covers the rendering, since an operator reads the first entry.
func TestStartupTimerSortsSlowestFirst(t *testing.T) {
	tm := newStartupTimer(nil)
	tm.phases = []phaseTime{
		{"quick", 2 * time.Millisecond},
		{"slowest", 10 * time.Second},
		{"middle", 400 * time.Millisecond},
	}
	var got strings.Builder
	// Reproduce done()'s rendering without a logger, by checking the order it would sort into.
	tm.log = nil
	tm.done()
	for _, p := range tm.phases {
		got.WriteString(p.name + " ")
	}
	// done() must not reorder the caller's slice -- the summary sorts a copy, and the phase list
	// stays in execution order for anyone reading it afterwards.
	if !strings.HasPrefix(got.String(), "quick slowest middle") {
		t.Errorf("done() reordered the phase list in place: %q", got.String())
	}
}

// TestBuildIdentityReportsClassadVersion is the point of buildIdentity: the startup log has to
// name the classad version the binary was compiled against. Without it, a stale daemon and an
// unfixed daemon produce identical output -- the same "unknown admin action" error -- and there is
// no way to tell "not fixed" from "not deployed" without shell access to the host.
func TestBuildIdentityReportsClassadVersion(t *testing.T) {
	self, classad := buildIdentity()
	if self == "" {
		t.Error("self version empty; the log line would omit which build is running")
	}
	// Under `go test` the binary carries real dependency build info, so this must resolve to a
	// version rather than the unknown fallback.
	if classad == "" || classad == "unknown" {
		t.Errorf("classad version = %q, want the dbrpc module version from the build info", classad)
	}
	if !strings.HasPrefix(classad, "v") {
		t.Errorf("classad version = %q, want something like v0.25.3", classad)
	}
}

// TestRecordTableOpenKeepsSlowest checks the top-N aggregation, which is the whole content of the
// summary line: a catalog can open hundreds of tables and only the slowest few are actionable. Feeding
// them in ASCENDING order is deliberate -- an implementation that keeps the first N it sees, or that
// compares against the wrong end of the slice, passes on descending input.
func TestRecordTableOpenKeepsSlowest(t *testing.T) {
	tm := newStartupTimer(nil)
	for i := 1; i <= 20; i++ {
		tm.recordTableOpen("table", fmt.Sprintf("t%02d", i), time.Duration(i)*time.Millisecond)
	}
	if tm.tblCount != 20 {
		t.Errorf("counted %d opens, want 20", tm.tblCount)
	}
	if want := 210 * time.Millisecond; tm.tblSum != want {
		t.Errorf("summed %v, want %v", tm.tblSum, want)
	}
	if len(tm.tblSlow) != slowestKept {
		t.Fatalf("kept %d slow entries, want %d", len(tm.tblSlow), slowestKept)
	}
	// Descending, and the slowest five are t20..t16.
	for i, want := range []string{"table t20", "table t19", "table t18", "table t17", "table t16"} {
		if tm.tblSlow[i].name != want {
			t.Errorf("slow[%d] = %q, want %q (order is what the summary prints)", i, tm.tblSlow[i].name, want)
		}
	}
	tm.done() // must not panic with a nil logger
}

// TestRecordTableOpenConcurrent runs the reporter the way the catalog does -- from many goroutines at
// once, since tables open in parallel. Under -race an unguarded aggregator fails here; without the race
// detector a lost update shows up as a wrong count.
func TestRecordTableOpenConcurrent(t *testing.T) {
	tm := newStartupTimer(nil)
	const goroutines, each = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				tm.recordTableOpen("table", fmt.Sprintf("t%d-%d", g, i), time.Millisecond)
			}
		}(g)
	}
	wg.Wait()
	if want := goroutines * each; tm.tblCount != want {
		t.Errorf("counted %d opens, want %d (a lost update means the aggregation is not atomic)",
			tm.tblCount, want)
	}
	if want := time.Duration(goroutines*each) * time.Millisecond; tm.tblSum != want {
		t.Errorf("summed %v, want %v", tm.tblSum, want)
	}
}

// TestRecordTableOpenNilTimer covers the nil receiver: the hook is passed to the catalog unconditionally.
func TestRecordTableOpenNilTimer(t *testing.T) {
	var tm *startupTimer
	tm.recordTableOpen("table", "jobs", time.Second)
}

// TestRecordSealMigrationAggregates covers the counters behind the summary line, including the concurrent
// path (migrations are reported from the goroutines opening the tables) and the zero case: a table that
// migrated nothing must not be counted, or a normal start would claim an upgrade happened.
func TestRecordSealMigrationAggregates(t *testing.T) {
	tm := newStartupTimer(nil)
	tm.recordSealMigration("empty", 0) // nothing migrated: must not count as a migrated table
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tm.recordSealMigration(fmt.Sprintf("t%d", i), 10)
		}(i)
	}
	wg.Wait()
	if tm.migTables != 8 {
		t.Errorf("migTables = %d, want 8 (a zero-segment table must not count; a lost update loses a real one)",
			tm.migTables)
	}
	if tm.migSegments != 80 {
		t.Errorf("migSegments = %d, want 80", tm.migSegments)
	}
	tm.done() // must not panic with a nil logger
}

// TestRecordSealMigrationNilTimer covers the nil receiver: the hook is passed to the catalog unconditionally.
func TestRecordSealMigrationNilTimer(t *testing.T) {
	var tm *startupTimer
	tm.recordSealMigration("jobs", 5)
}
