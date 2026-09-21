package dbad

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// Every reason must reach the ad under a name an operator can query, including the ones sitting
// at zero: the point of the breakdown is to say which of five unrelated conditions a refusal was,
// and a reason that is absent until it fires reads as undefined at exactly the moment someone is
// checking whether it is the one happening.
func TestUnreadableReasonsReachTheAd(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Delta: DeltaStat{
		UnreadableBase:    9,
		UnreadableReasons: map[string]int64{"reassemble": 6, "delta-no-base": 2, "delta-flag-mismatch": 1},
	}})

	for name, want := range map[string]int64{
		"DeltaUnreadableBase":            9,
		"DeltaUnreadableReassemble":      6,
		"DeltaUnreadableNoBase":          2,
		"DeltaUnreadableFlagMismatch":    1,
		"DeltaUnreadableNotVisible":      0,
		"DeltaUnreadableSegmentGone":     0,
		"DeltaUnreadableDecode":          0,
		"DeltaUnreadableNoVersions":      0,
		"DeltaUnreadableChainReassemble": 0,
		"DeltaUnreadableChainDecompress": 0,
		"DeltaUnreadableChainDecode":     0,
	} {
		got, ok := ad.EvaluateAttrInt(name)
		if !ok {
			t.Errorf("%s is not on the ad: an operator querying it sees undefined", name)
			continue
		}
		if got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

// The suffixes this package publishes must match the reason names classad actually emits. They
// are two separate lists in two repositories, so a rename on either side silently produces a set
// of permanently-zero attributes -- which looks exactly like a healthy store.
func TestReasonNamesMatchClassad(t *testing.T) {
	known := map[string]bool{}
	for _, r := range unreadableReasons {
		known[r.reason] = true
	}
	// Against the FULL name list, not UnreadableBaseReasons() -- that map holds only reasons
	// that have fired, so in a fresh process it is empty and a loop over it checks nothing.
	names := db.UnreadableBaseReasonNames()
	if len(names) == 0 {
		t.Fatal("classad reports no reason names at all")
	}
	for _, reason := range names {
		if !known[reason] {
			t.Errorf("classad reports reason %q, which this package does not publish", reason)
		}
	}
	if len(known) != len(names) {
		t.Errorf("publishing %d reasons, classad has %d: %v vs %v", len(known), len(names), known, names)
	}
}
