package server

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// writePatches seeds a key and then patches one attribute repeatedly -- the shape delta records
// exist for, and the only shape that produces them.
func writePatches(t *testing.T, d *db.DB) {
	t.Helper()
	ad := classad.New()
	ad.InsertAttr("ClusterId", int64(1))
	ad.InsertAttr("JobStatus", int64(1))
	for i := 0; i < 40; i++ {
		ad.InsertAttr(fmt.Sprintf("Pad%02d", i), int64(i))
	}
	tx := d.Begin()
	tx.NewClassAd("1.0", ad)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// SetAttribute is the schedd-shaped write -- one attribute of a wide ad -- which is the path
	// that can store a delta.
	for i := 2; i < 8; i++ {
		tx := d.Begin()
		if err := tx.SetAttribute("1.0", "JobStatus", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDeltaRecordsOnByDefault: taking classad v0.30.0 turns delta records on for every table,
// because Config.DeltaMax left at zero means the library default (16), not "off". That is a
// deliberate change and this is what says so out loud -- if a later bump flips the default back,
// this test is the thing that notices.
func TestDeltaRecordsOnByDefault(t *testing.T) {
	svc, err := New(Config{Dir: t.TempDir(), Authorize: allowAll})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	writePatches(t, svc.DB())
	deltas, fulls := svc.DB().DeltaStats()
	if deltas == 0 {
		t.Errorf("no delta records written with the default config (deltas=%d fulls=%d)", deltas, fulls)
	}
	// And the ad still reads back whole through the chain.
	ad, ok := svc.DB().LookupClassAd("1.0")
	if !ok {
		t.Fatal("1.0 missing")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 7 {
		t.Errorf("JobStatus = %v (ok=%v), want 7 through the delta chain", v, ok)
	}
	if n := len(ad.AST().Attributes); n != 42 { // 40 pads + ClusterId + JobStatus
		t.Errorf("read back %d attributes: a fragment, not the merged ad", n)
	}
}

// TestDeltaMaxOffDisables proves the escape hatch reaches the store. A config field that is
// accepted and quietly ignored is worse than not having one: an operator turning deltas off after
// trouble would believe they had, and the store would keep writing them.
func TestDeltaMaxOffDisables(t *testing.T) {
	svc, err := New(Config{Dir: t.TempDir(), Authorize: allowAll, DeltaMax: db.DeltaMaxOff})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	writePatches(t, svc.DB())
	deltas, fulls := svc.DB().DeltaStats()
	if deltas != 0 {
		t.Errorf("DeltaMaxOff still wrote %d delta records (fulls=%d)", deltas, fulls)
	}
	// DeltaStats reports 0,0 when delta mode is off -- the tracker does not exist -- so fulls
	// cannot serve as the "the writes really happened" check here. Read the data back instead,
	// which is the better check anyway: it proves the store is correct with the hatch pulled.
	_ = fulls
	ad, ok := svc.DB().LookupClassAd("1.0")
	if !ok {
		t.Fatal("1.0 missing: the writes did not happen, so the delta count proves nothing")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 7 {
		t.Errorf("JobStatus = %v (ok=%v), want 7 -- the updates did not land", v, ok)
	}
}
