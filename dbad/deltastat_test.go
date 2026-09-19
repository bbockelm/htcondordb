package dbad

import (
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
