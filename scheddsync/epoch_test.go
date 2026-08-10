package scheddsync

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

func TestEpochKeyConstraint(t *testing.T) {
	ad := classad.New()
	ad.InsertAttr("ClusterId", 5)
	ad.InsertAttr("ProcId", 0)
	ad.InsertAttr(RunInstanceAttr, 2)
	ad.InsertAttrString(EpochAdTypeAttr, "EPOCH")

	got, ok := epochKeyConstraint(ad)
	want := `ClusterId == 5 && ProcId == 0 && RunInstanceID == 2 && EpochAdType == "EPOCH"`
	if !ok || got != want {
		t.Errorf("epochKeyConstraint = %q (ok=%v), want %q", got, ok, want)
	}

	// A run instance's SPAWN and EPOCH ads are distinct records (different constraint).
	spawn := classad.New()
	spawn.InsertAttr("ClusterId", 5)
	spawn.InsertAttr("ProcId", 0)
	spawn.InsertAttr(RunInstanceAttr, 2)
	spawn.InsertAttrString(EpochAdTypeAttr, "SPAWN")
	if sc, _ := epochKeyConstraint(spawn); sc == want {
		t.Error("SPAWN and EPOCH ads of the same run instance must have distinct constraints")
	}

	// Missing RunInstanceID: no key.
	noRun := classad.New()
	noRun.InsertAttr("ClusterId", 5)
	noRun.InsertAttr("ProcId", 0)
	if _, ok := epochKeyConstraint(noRun); ok {
		t.Error("a record without RunInstanceID has no epoch key")
	}
}

func TestEpochEventTime(t *testing.T) {
	ad := classad.New()
	ad.InsertAttr(EpochWriteDateAttr, 1_700_000_000)
	if v, ok := epochEventTime(ad); !ok || v != 1_700_000_000 {
		t.Errorf("epochEventTime = %d (ok=%v), want 1700000000", v, ok)
	}
	if _, ok := epochEventTime(classad.New()); ok {
		t.Error("a record without EpochWriteDate should return ok=false (fall back to ingest clock)")
	}
}

func TestClassadStringLit(t *testing.T) {
	cases := map[string]string{
		"EPOCH": `"EPOCH"`,
		`a"b`:   `"a\"b"`,
		`c\d`:   `"c\\d"`,
		"":      `""`,
	}
	for in, want := range cases {
		if got := classadStringLit(in); got != want {
			t.Errorf("classadStringLit(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestJobEpochSyncUsesEpochPolicy confirms NewJobEpochSync sets the epoch kind and
// hooks (so the shared tailer dedups/keys per run instance, not per job).
func TestJobEpochSyncUsesEpochPolicy(t *testing.T) {
	s := NewJobEpochSync(nil, HistorySyncConfig{Filename: "/tmp/x"})
	if s.kind != "job_epoch" {
		t.Errorf("kind = %q, want job_epoch", s.kind)
	}
	ad := classad.New()
	ad.InsertAttr("ClusterId", 1)
	ad.InsertAttr("ProcId", 0)
	ad.InsertAttr(RunInstanceAttr, 0)
	ad.InsertAttrString(EpochAdTypeAttr, "EPOCH")
	if c, ok := s.keyConstraint(ad); !ok || c == "" {
		t.Error("epoch sync should key on the run-instance constraint")
	}
	if _, ok := s.eventTime(ad); ok {
		t.Error("no EpochWriteDate -> eventTime not ok")
	}
	ad.InsertAttr(EpochWriteDateAttr, 123)
	if v, ok := s.eventTime(ad); !ok || v != 123 {
		t.Errorf("eventTime = %d (ok=%v), want 123", v, ok)
	}
}
