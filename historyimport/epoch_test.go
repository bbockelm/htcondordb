package historyimport

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// fakeEpochSource yields a preset list of records newest-first. The list already
// reflects what `since` would return (write date >= the cursor second), so the
// tests exercise the importer's boundary dedup, not the schedd's since filter.
type fakeEpochSource struct{ recs []*classad.ClassAd }

func (f fakeEpochSource) History(_ context.Context, _ ScheddRef, _, _ string, limit int, yield func(*classad.ClassAd) error) error {
	for i, ad := range f.recs {
		if limit > 0 && i >= limit {
			break
		}
		if err := yield(ad); err != nil {
			return err
		}
	}
	return nil
}

func epochAd(cluster, proc, run int, wd int64) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttr("ClusterId", int64(cluster))
	ad.InsertAttr("ProcId", int64(proc))
	ad.InsertAttr(runInstanceAttr, int64(run))
	ad.InsertAttr(epochWriteDateAttr, wd)
	ad.InsertAttrString(epochAdTypeAttr, "EPOCH")
	return ad
}

func runEpoch(t *testing.T, recs []*classad.ClassAd, cur fakeCursors) (*fakeWriter, Stats) {
	t.Helper()
	w := newFakeWriter()
	im := newImporter(
		fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}},
		fakeEpochSource{recs: recs}, w, cur,
	)
	st, err := im.RunJob(context.Background(), Job{Name: "j", Pool: "p", Table: "epoch_history", Source: SourceEpoch})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	return w, st
}

func TestEpochKeyDistinguishesRunInstances(t *testing.T) {
	k0 := epochKey(epochAd(5, 0, 0, 100), "schedA")
	k1 := epochKey(epochAd(5, 0, 1, 101), "schedA")
	if k0 == k1 {
		t.Fatalf("run instances 0 and 1 of the same job must have distinct keys (%q)", k0)
	}
	if k0 != "schedA#5.0#0#EPOCH" {
		t.Errorf("epochKey = %q, want schedA#5.0#0#EPOCH", k0)
	}
}

func TestImportEpochBootstrap(t *testing.T) {
	// Three run instances of cluster 5.0 (newest write date first).
	recs := []*classad.ClassAd{
		epochAd(5, 0, 2, 102),
		epochAd(5, 0, 1, 101),
		epochAd(5, 0, 0, 100),
	}
	cur := fakeCursors{}
	w, st := runEpoch(t, recs, cur)

	if st.Imported != 3 {
		t.Fatalf("imported %d, want 3 (all run instances)", st.Imported)
	}
	if len(w.appended["epoch_history"]) != 3 {
		t.Errorf("appended %d records, want 3", len(w.appended["epoch_history"]))
	}
	c := decodeEpochCursor(func() string { v, _ := cur.Get("j", "schedA"); return v }())
	if c.T != 102 || len(c.Keys) != 1 || c.Keys[0] != "schedA#5.0#2#EPOCH" {
		t.Errorf("cursor = %+v, want T=102 keys=[schedA#5.0#2]", c)
	}
}

func TestImportEpochIncrementalSkipsBoundaryDup(t *testing.T) {
	cur := fakeCursors{}
	cur.Set("j", "schedA", encodeEpochCursor(epochCursor{T: 100, Keys: []string{"schedA#5.0#0#EPOCH"}}))
	// The since-filtered stream (write date >= 100), newest first: a new job at
	// 101, a new job at the boundary second 100, and the already-imported boundary
	// record at 100.
	recs := []*classad.ClassAd{
		epochAd(5, 1, 0, 101), // new
		epochAd(6, 0, 0, 100), // new, same second as the boundary
		epochAd(5, 0, 0, 100), // already imported (in cursor keys) -> skip
	}
	w, st := runEpoch(t, recs, cur)

	if st.Imported != 2 {
		t.Fatalf("imported %d, want 2 (the boundary dup is skipped)", st.Imported)
	}
	got := map[string]bool{}
	for _, r := range w.appended["epoch_history"] {
		got[epochKey(r, "schedA")] = true
	}
	if !got["schedA#5.1#0#EPOCH"] || !got["schedA#6.0#0#EPOCH"] || got["schedA#5.0#0#EPOCH"] {
		t.Errorf("appended set wrong: %v", got)
	}
	c := decodeEpochCursor(func() string { v, _ := cur.Get("j", "schedA"); return v }())
	if c.T != 101 || len(c.Keys) != 1 || c.Keys[0] != "schedA#5.1#0#EPOCH" {
		t.Errorf("cursor = %+v, want T=101 keys=[schedA#5.1#0]", c)
	}
}

func TestImportEpochSteadyStateNoDuplicates(t *testing.T) {
	cur := fakeCursors{}
	cur.Set("j", "schedA", encodeEpochCursor(epochCursor{T: 100, Keys: []string{"schedA#5.0#0#EPOCH"}}))
	// Steady state: the boundary re-scan returns only the record we already have.
	recs := []*classad.ClassAd{epochAd(5, 0, 0, 100)}
	w, st := runEpoch(t, recs, cur)

	if st.Imported != 0 {
		t.Fatalf("imported %d, want 0 (nothing new)", st.Imported)
	}
	if len(w.appended["epoch_history"]) != 0 {
		t.Errorf("appended %d, want 0", len(w.appended["epoch_history"]))
	}
	c := decodeEpochCursor(func() string { v, _ := cur.Get("j", "schedA"); return v }())
	if c.T != 100 {
		t.Errorf("cursor T = %d, want 100 (unchanged)", c.T)
	}
}

func TestImportEpochEmptyStreamKeepsCursor(t *testing.T) {
	cur := fakeCursors{}
	orig := encodeEpochCursor(epochCursor{T: 100, Keys: []string{"schedA#5.0#0#EPOCH"}})
	cur.Set("j", "schedA", orig)
	// Nothing returned (e.g. the boundary rotated away and no newer records yet).
	_, st := runEpoch(t, nil, cur)

	if st.Imported != 0 {
		t.Fatalf("imported %d, want 0", st.Imported)
	}
	if v, _ := cur.Get("j", "schedA"); v != orig {
		t.Errorf("cursor changed to %q on an empty scan, want unchanged", v)
	}
}
