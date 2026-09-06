package main

import (
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/scheddsync"
)

func jobStatus(caughtUp bool) []scheddsync.SyncStatus {
	return []scheddsync.SyncStatus{{Kind: "job_queue.log", Source: "/var/lib/condor/job_queue/job_queue.log", CaughtUp: caughtUp}}
}

// TestCaughtUpTriggerDebounce checks the trigger fires on a CaughtUp transition, not on the first
// observation or a steady state, and is debounced to at most once per minGap (a flip within the gap
// is remembered and fires once the gap elapses).
func TestCaughtUpTriggerDebounce(t *testing.T) {
	const gap = 15 * time.Second
	c := newCaughtUpTrigger(gap)
	base := time.Unix(1_000_000, 0)

	// First observation of a source is not a transition.
	if c.observe(jobStatus(false), base) {
		t.Fatal("first observation should not fire")
	}
	// Unchanged state does not fire.
	if c.observe(jobStatus(false), base.Add(2*time.Second)) {
		t.Fatal("no change should not fire")
	}
	// false->true transition fires (nothing fired before).
	if !c.observe(jobStatus(true), base.Add(4*time.Second)) {
		t.Fatal("a CaughtUp transition should fire")
	}
	// true->false transition within minGap is debounced.
	if c.observe(jobStatus(false), base.Add(6*time.Second)) {
		t.Fatal("a transition within minGap should be debounced, not fire")
	}
	// Still within minGap, no new transition: no fire, but the pending flip is remembered.
	if c.observe(jobStatus(false), base.Add(10*time.Second)) {
		t.Fatal("should not fire before minGap elapses")
	}
	// minGap elapsed since the last fire (+4s -> +20s = 16s >= 15s): the pending flip fires.
	if !c.observe(jobStatus(false), base.Add(20*time.Second)) {
		t.Fatal("a flip seen during the gap should fire once the gap elapses")
	}
	// Steady state afterward: no transition, no fire.
	if c.observe(jobStatus(false), base.Add(40*time.Second)) {
		t.Fatal("steady state should not fire")
	}
}

// TestCaughtUpTriggerIgnoresUnreportedSource confirms a not-yet-reporting source (empty Kind) is
// ignored and cannot cause a fire.
func TestCaughtUpTriggerIgnoresUnreportedSource(t *testing.T) {
	c := newCaughtUpTrigger(time.Second)
	base := time.Unix(1_000_000, 0)
	empty := []scheddsync.SyncStatus{{}} // Kind ""
	if c.observe(empty, base) || c.observe(empty, base.Add(time.Hour)) {
		t.Fatal("an unreported (empty-Kind) source must never fire")
	}
}

// TestCaughtUpTriggerFiresOnFirstSeenCaughtUp: a source first observed ALREADY caught up must fire.
// A new source is assumed behind (last-known false), so its first true is a false->true edge. This
// is the case that silently never fired before: the syncer can reach caught-up before the trigger's
// first sample, so the flip is only ever visible as an already-true first observation.
func TestCaughtUpTriggerFiresOnFirstSeenCaughtUp(t *testing.T) {
	c := newCaughtUpTrigger(15 * time.Second)
	base := time.Unix(1_000_000, 0)
	if !c.observe(jobStatus(true), base) {
		t.Fatal("a source first observed caught up should fire")
	}
	// Steady caught-up afterward is not a new edge.
	if c.observe(jobStatus(true), base.Add(time.Second)) {
		t.Fatal("steady caught-up should not re-fire")
	}
	// A source first observed BEHIND is not an edge (false == assumed-false), so it must not fire.
	c2 := newCaughtUpTrigger(15 * time.Second)
	if c2.observe(jobStatus(false), base) {
		t.Fatal("a source first observed behind should not fire")
	}
}
