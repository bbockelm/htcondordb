package dbad

import (
	"strings"
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
		LastDecodeStage:   "patch",
		LastDecodeError:   "wire: bad tag 0x7f",
	}})

	for name, want := range map[string]int64{
		"DeltaUnreadableBase":             9,
		"DeltaUnreadableReassemble":       6,
		"DeltaUnreadableNoBase":           2,
		"DeltaUnreadableFlagMismatch":     1,
		"DeltaUnreadableNotVisible":       0,
		"DeltaUnreadableSegmentGone":      0,
		"DeltaUnreadableDecode":           0,
		"DeltaUnreadableNoVersions":       0,
		"DeltaUnreadableChainReassemble":  0,
		"DeltaUnreadableChainDecompress":  0,
		"DeltaUnreadableChainBaseDecode":  0,
		"DeltaUnreadableChainPatchDecode": 0,
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

// The sampled decoder message must reach the ad, and must be bounded there: it is a diagnostic
// read off a live deployment, not a payload for every collector in the pool to store.
func TestDecodeErrorSampleReachesTheAd(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Delta: DeltaStat{LastDecodeStage: "patch", LastDecodeError: "wire: bad tag 0x7f"}})
	if got, ok := ad.EvaluateAttrString("DeltaLastDecodeStage"); !ok || got != "patch" {
		t.Errorf("DeltaLastDecodeStage = %q (present %v), want patch", got, ok)
	}
	if got, ok := ad.EvaluateAttrString("DeltaLastDecodeError"); !ok || got != "wire: bad tag 0x7f" {
		t.Errorf("DeltaLastDecodeError = %q (present %v)", got, ok)
	}

	long := strings.Repeat("x", maxDecodeErrorLen*3)
	ad2 := classad.New()
	AddAttrs(ad2, Input{Delta: DeltaStat{LastDecodeError: long}})
	got, ok := ad2.EvaluateAttrString("DeltaLastDecodeError")
	if !ok {
		t.Fatal("DeltaLastDecodeError missing")
	}
	if len(got) > maxDecodeErrorLen+3 {
		t.Errorf("sampled error is %d chars on the ad; it must be bounded", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("a clipped message must be marked, or a reader cannot tell it from a short one")
	}
}

// The no-base diagnostics have to reach the ad too: the reason names WHICH of three faults it
// was, and these say how much of the chain the walk found, which is what separates "the base is
// one link past a dead segment" from "there is nothing here". A counter collected but not
// advertised reads exactly like one that is always zero.
func TestNoBaseDiagnosticsReachTheAd(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Delta: DeltaStat{
		UnreadableBase:      9,
		UnreadableReasons:   map[string]int64{"delta-chain-broken": 5, "delta-index-pending": 4},
		SealedProbesSkipped: 77,
		NoBaseVersions:      3,
		NoBaseChainBroken:   true,
		NoBaseSealedSkipped: 2,
	}})

	for name, want := range map[string]int64{
		"DeltaUnreadableChainBroken":   5,
		"DeltaUnreadableIndexPending":  4,
		"DeltaSealedProbesSkipped":     77,
		"DeltaLastNoBaseVersions":      3,
		"DeltaLastNoBaseSealedSkipped": 2,
	} {
		got, ok := ad.EvaluateAttrInt(name)
		if !ok {
			t.Errorf("%s is not on the ad", name)
			continue
		}
		if got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
	if got, ok := ad.EvaluateAttrBool("DeltaLastNoBaseChainBroken"); !ok || !got {
		t.Errorf("DeltaLastNoBaseChainBroken = %v (present %v), want true", got, ok)
	}
}

// The stranded-fragment count has to be queryable, not just logged at open. It is the one
// number that catches the damage while the keys are still identifiable: afterwards it surfaces
// as DeltaUnreadableNoBase on a key nobody can tie back to a cause.
func TestStrandedSealedDeltasReachesTheAd(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Delta: DeltaStat{StrandedSealedDeltas: 12}})
	got, ok := ad.EvaluateAttrInt("DeltaStrandedSealedDeltas")
	if !ok {
		t.Fatal("DeltaStrandedSealedDeltas is not on the ad: an operator querying it sees undefined")
	}
	if got != 12 {
		t.Errorf("DeltaStrandedSealedDeltas = %d, want 12", got)
	}
}
