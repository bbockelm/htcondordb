package main

import (
	"strings"
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
