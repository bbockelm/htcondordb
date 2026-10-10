package dbad

import (
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

func TestAddAttrsMirrored(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Now: time.Unix(1, 0), Mirrored: &Mirrored{Name: "ap1.example.org", Address: "<10.0.0.1:9618>"}})
	if v, _ := ad.EvaluateAttrString("MirroredScheddName"); v != "ap1.example.org" {
		t.Errorf("MirroredScheddName = %q", v)
	}
	if v, _ := ad.EvaluateAttrString("MirroredScheddAddress"); v != "<10.0.0.1:9618>" {
		t.Errorf("MirroredScheddAddress = %q", v)
	}

	// No address file: the name is still advertised, the address is omitted rather than empty.
	ad = classad.New()
	AddAttrs(ad, Input{Now: time.Unix(1, 0), Mirrored: &Mirrored{Name: "ap1.example.org"}})
	if _, ok := ad.Lookup("MirroredScheddAddress"); ok {
		t.Error("MirroredScheddAddress present with no address")
	}

	ad = classad.New()
	AddAttrs(ad, Input{Now: time.Unix(1, 0)})
	if _, ok := ad.Lookup("MirroredScheddName"); ok {
		t.Error("MirroredScheddName present without schedd-sync")
	}
	if _, ok := ad.Lookup("SourcesTotal"); ok {
		t.Error("federation summary present on a non-hub")
	}
}

func TestAddAttrsFederation(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Now: time.Unix(1, 0), Federation: &Federation{
		Constraint: `Name == "ap1"`, Total: 4, Fresh: 2, Stale: 1, Absent: 1, MaxStaleness: 75, MaxStalenessKnown: true,
	}})
	i := func(k string) int64 { v, _ := ad.EvaluateAttrInt(k); return v }
	if v, _ := ad.EvaluateAttrString("FederationConstraint"); v != `Name == "ap1"` {
		t.Errorf("FederationConstraint = %q", v)
	}
	if i("SourcesTotal") != 4 || i("SourcesFresh") != 2 || i("SourcesStale") != 1 || i("SourcesAbsent") != 1 || i("MaxSourceStaleness") != 75 {
		t.Errorf("summary wrong: %s", ad)
	}
	ad = classad.New()
	AddAttrs(ad, Input{Now: time.Unix(1, 0), Federation: &Federation{Total: 1, Stale: 1}})
	if _, ok := ad.Lookup("MaxSourceStaleness"); ok {
		t.Error("MaxSourceStaleness advertised with no measured staleness")
	}
}
