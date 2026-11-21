package syncstatus

import "testing"

// TestBuildAdCarriesTheSpokeBuild: the heartbeat names the build it came from, so a hub can tell
// an un-upgraded spoke from a slow one. Several replication fixes live in the spoke -- a catch-up
// cursor is minted by whichever daemon serves the watch -- and until this, the two were
// indistinguishable at the hub without reading cursor files in a shell on its pod.
func TestBuildAdCarriesTheSpokeBuild(t *testing.T) {
	ad := BuildAd(Row{ScheddName: "ap1", Version: "v0.28.0", ClassAdVersion: "v0.43.0"})
	if got, ok := ad.EvaluateAttrString(AttrSpokeVersion); !ok || got != "v0.28.0" {
		t.Errorf("%s = %q (ok=%v), want %q", AttrSpokeVersion, got, ok, "v0.28.0")
	}
	if got, ok := ad.EvaluateAttrString(AttrSpokeClassAdVersion); !ok || got != "v0.43.0" {
		t.Errorf("%s = %q (ok=%v), want %q", AttrSpokeClassAdVersion, got, ok, "v0.43.0")
	}
}

// TestBuildAdOmitsAnUnknownBuild: a spoke that does not know its own version must leave the
// attribute out, not write "". Absent is what an old spoke's heartbeat looks like at the hub, and
// an empty string would be a third state meaning the same thing.
func TestBuildAdOmitsAnUnknownBuild(t *testing.T) {
	ad := BuildAd(Row{ScheddName: "ap1"})
	for _, attr := range []string{AttrSpokeVersion, AttrSpokeClassAdVersion} {
		if _, ok := ad.EvaluateAttrString(attr); ok {
			t.Errorf("%s is present on a heartbeat with no build info, want absent", attr)
		}
	}
}
