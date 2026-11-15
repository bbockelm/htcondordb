package historyimport

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// EnteredHistoryTime must come from the RECORD, not from the import clock: an import is
// not an event in the job's life, and a hub that measures retention against its own
// receive time evicts by arrival order rather than by age. The import clock is the last
// resort, for a record that carries no time of its own.
func TestEnteredHistoryTimeDerivedFromRecord(t *testing.T) {
	const importClock = 1_700_000_000 // newImporter's fixed Now

	cases := []struct {
		name    string
		entered int64 // EnteredCurrentStatus; 0 = absent
		comp    int64 // CompletionDate; -1 = absent, 0 = present-but-zero (removed job)
		want    int64
	}{
		{"terminal transition wins", 1_600_000_000, 1_650_000_000, 1_600_000_000},
		{"falls back to completion", 0, 1_650_000_000, 1_650_000_000},
		// A removed job carries CompletionDate = 0. Taken literally that is the oldest
		// record in the archive and the first thing age-ordered retention evicts.
		{"zero completion is not a time", 0, 0, importClock},
		{"neither: import clock", 0, -1, importClock},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ad := histAd(i+1, 0, "schedA")
			if tc.entered > 0 {
				ad.InsertAttr("EnteredCurrentStatus", tc.entered)
			}
			if tc.comp >= 0 {
				ad.InsertAttr("CompletionDate", tc.comp)
			}

			w := runHistoryOne(t, ad)
			got, ok := w.appended["history"][0].EvaluateAttrInt(EnteredHistoryAttr)
			if !ok {
				t.Fatalf("%s not stamped", EnteredHistoryAttr)
			}
			if got != tc.want {
				t.Errorf("%s = %d, want %d", EnteredHistoryAttr, got, tc.want)
			}
		})
	}
}

// A value already on a record pulled from a schedd is the USER's: HTCondor never writes
// this attribute and the schedd does not protect it, so `+EnteredHistoryTime = 0` at submit
// would otherwise let a job place itself at the oldest end of the archive -- first to be
// evicted, and misplaced in every time-windowed query. It must be overwritten, as
// scheddsync overwrites it.
//
// Overwriting does not make the attribute drift between stores, because the replacement is
// derived from the record: two stores importing the same record stamp the same time.
func TestEnteredHistoryTimeOverwritesUserSuppliedValue(t *testing.T) {
	const userSupplied = 0
	const terminal = 1_600_000_000

	ad := histAd(1, 0, "schedA")
	ad.InsertAttr("EnteredCurrentStatus", int64(terminal))
	ad.InsertAttr(EnteredHistoryAttr, int64(userSupplied))

	w := runHistoryOne(t, ad)
	got, ok := w.appended["history"][0].EvaluateAttrInt(EnteredHistoryAttr)
	if !ok {
		t.Fatalf("%s lost", EnteredHistoryAttr)
	}
	if got != terminal {
		t.Errorf("%s = %d, want the derived %d (user value not overwritten)", EnteredHistoryAttr, got, terminal)
	}
}

// The epoch path has its own notion of the record's own time: EpochWriteDate, the epoch
// file's ordering key. It must not fall through to the import clock when that is present.
func TestEpochEnteredHistoryTimeFromWriteDate(t *testing.T) {
	const wd = 1_610_000_000

	w, _ := runEpoch(t, []*classad.ClassAd{epochAd(1, 0, 1, wd)}, fakeCursors{})
	recs := w.appended["epoch_history"]
	if len(recs) != 1 {
		t.Fatalf("appended %d epoch records, want 1", len(recs))
	}
	got, ok := recs[0].EvaluateAttrInt(EnteredHistoryAttr)
	if !ok {
		t.Fatalf("%s not stamped on epoch record", EnteredHistoryAttr)
	}
	if got != wd {
		t.Errorf("%s = %d, want EpochWriteDate %d", EnteredHistoryAttr, got, wd)
	}
}

// runHistoryOne imports a single completed-job record and returns the writer it landed in.
func runHistoryOne(t *testing.T, ad *classad.ClassAd) *fakeWriter {
	t.Helper()
	w := newFakeWriter()
	im := newImporter(
		fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}},
		fakeSource{hist: map[string][]*classad.ClassAd{"schedA": {ad}}},
		w, fakeCursors{},
	)
	if _, err := im.RunJob(context.Background(), Job{Name: "j1", Pool: "p", Table: "history"}); err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if n := len(w.appended["history"]); n != 1 {
		t.Fatalf("appended %d records, want 1", n)
	}
	return w
}
