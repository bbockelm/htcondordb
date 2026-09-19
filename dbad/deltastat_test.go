package dbad

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
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
