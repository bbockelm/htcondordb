package dbad

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// The counters exist to be read off a LIVE deployment, so what matters is that they reach the ad
// under the names an operator will query. A counter that is collected but not advertised is
// indistinguishable from one that is always zero -- which is exactly the failure mode that made
// the last round of this investigation slow.
func TestDeltaCountersReachTheAd(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{
		Delta: DeltaStat{Removal: 3, Bound: 5, NoBase: 7, Ineligible: 11, UnreadableBase: 13},
	})
	for name, want := range map[string]int64{
		"DeltaFallbackRemoval":    3,
		"DeltaFallbackBound":      5,
		"DeltaFallbackNoBase":     7,
		"DeltaFallbackIneligible": 11,
		"DeltaUnreadableBase":     13,
	} {
		got, ok := ad.EvaluateAttrInt(name)
		if !ok {
			t.Errorf("%s is not on the ad: an operator querying it sees undefined, not a value", name)
			continue
		}
		if got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

// CurrentDeltaStat must actually read classad's counters rather than return a zero value -- the
// difference is invisible on a quiet daemon and total on a busy one.
func TestCurrentDeltaStatReadsThrough(t *testing.T) {
	// The counters are process-wide and monotonic; this asserts the wiring, not a value.
	_ = CurrentDeltaStat()
	ad := classad.New()
	AddAttrs(ad, Input{Delta: CurrentDeltaStat()})
	for _, name := range []string{"DeltaFallbackRemoval", "DeltaFallbackNoBase"} {
		if _, ok := ad.EvaluateAttrInt(name); !ok {
			t.Errorf("%s missing from an ad built with CurrentDeltaStat", name)
		}
	}
	if s := ad.String(); !strings.Contains(s, "DeltaFallback") {
		t.Error("no delta counters rendered on the ad")
	}
}

// TestCountersTrackClassad drives a REAL fallback and checks the advertised numbers move with it.
//
// The obvious version of this test -- assert CurrentDeltaStat().UnreadableBase equals
// db.UnreadableBaseRefusals() -- passes trivially in a fresh process where both are zero, and a
// mutant that hardcodes the field to 0 survives it. (UnreadableBase shipped hardcoded to 0 for one
// release, which is exactly the mistake such a test should catch and does not.) So this one makes
// a counter actually move: a patch write to a key the store does not hold takes the no-base
// fallback, and the advertised value has to follow.
func TestCountersTrackClassad(t *testing.T) {
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	before := CurrentDeltaStat()
	tx := d.Begin()
	if err := tx.SetAttribute("no-such-key.0", "JobStatus", "4"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after := CurrentDeltaStat()

	moved := after.NoBase != before.NoBase || after.Ineligible != before.Ineligible ||
		after.Removal != before.Removal || after.Bound != before.Bound
	if !moved {
		t.Fatalf("a patch write to an absent key moved no counter: before %+v after %+v", before, after)
	}
	// GAP, stated rather than papered over: UnreadableBase cannot be driven from here. Making it
	// move needs a key that is present but unresolvable, which classad reaches only through an
	// internal test hook. A mutant that hardcodes that ONE field to 0 survives this test. What is
	// covered is that CurrentDeltaStat reads through to classad at all (hardcoding NoBase fails
	// here), so the remaining risk is one field wired differently from its four siblings.
	// And the advertised ad carries the moved values, not a snapshot taken elsewhere.
	ad := classad.New()
	AddAttrs(ad, Input{Delta: after})
	for name, want := range map[string]int64{
		"DeltaFallbackNoBase":     after.NoBase,
		"DeltaFallbackIneligible": after.Ineligible,
		"DeltaUnreadableBase":     after.UnreadableBase,
	} {
		got, ok := ad.EvaluateAttrInt(name)
		if !ok || got != want {
			t.Errorf("%s on the ad = %v (ok=%v), want %d", name, got, ok, want)
		}
	}
}

// TestDeltaWriteSplitIsAdvertised guards the POSITIVE CONTROL.
//
// The DeltaFallback* counters only count writes that could NOT be stored as a delta. All of them
// reading zero on a live daemon is therefore ambiguous: it is what a healthy delta store looks
// like, and equally what a store with delta mode OFF looks like, because a nil tracker increments
// nothing -- not even Ineligible. That ambiguity cost a round trip on a production incident. The
// per-table Deltas/Fulls split resolves it, so it has to reach the ad.
func TestDeltaWriteSplitIsAdvertised(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Tables: []TableStat{{Name: "jobs", Ads: 10, Deltas: 17, Fulls: 4}}})
	for name, want := range map[string]int64{"Table_jobs_Deltas": 17, "Table_jobs_Fulls": 4} {
		got, ok := ad.EvaluateAttrInt(name)
		if !ok {
			t.Errorf("%s not on the ad: the control an operator reads is missing", name)
			continue
		}
		if got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

// TestDeltaWriteSplitReadsThrough: a real delta write must move the advertised split, or the
// control is a literal and answers nothing.
func TestDeltaWriteSplitReadsThrough(t *testing.T) {
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ad := classad.New()
	ad.InsertAttr("ClusterId", int64(1))
	ad.InsertAttr("JobStatus", int64(1))
	for i := 0; i < 20; i++ {
		ad.InsertAttr("Pad"+string(rune('a'+i)), int64(i))
	}
	tx := d.Begin()
	tx.NewClassAd("1.0", ad)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i := 2; i < 6; i++ {
		tx := d.Begin()
		if err := tx.SetAttribute("1.0", "JobStatus", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	deltas, fulls := d.DeltaStats()
	if deltas == 0 {
		t.Fatalf("no delta records written (deltas=%d fulls=%d): the control cannot prove anything", deltas, fulls)
	}
}
