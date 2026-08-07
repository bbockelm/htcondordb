package historyimport

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// --- fakes ---

type fakeDiscovery struct {
	schedds []ScheddRef
	err     error
}

func (f fakeDiscovery) Schedds(context.Context, string, string) ([]ScheddRef, error) {
	return f.schedds, f.err
}

// fakeSource models each schedd's retained history as an oldest-first slice and
// streams it newest-first, honoring `since` (stop when reaching that cluster.proc)
// and `limit`, matching condor_history's backward scan.
type fakeSource struct {
	hist map[string][]*classad.ClassAd // scheddName -> oldest-first
	err  map[string]error              // scheddName -> stream error
}

func (f fakeSource) History(_ context.Context, sd ScheddRef, _, since string, limit int, yield func(*classad.ClassAd) error) error {
	if e := f.err[sd.Name]; e != nil {
		return e
	}
	ads := f.hist[sd.Name]
	n := 0
	for i := len(ads) - 1; i >= 0; i-- { // newest first
		if since != "" && jobKey(ads[i]) == since {
			break // reached the cursor job; stop the backward scan
		}
		if err := yield(ads[i]); err != nil {
			return err
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return nil
}

type fakeWriter struct {
	appended map[string][]*classad.ClassAd // table -> records
	seen     map[string]map[string]bool    // table -> set of GlobalJobId
}

func newFakeWriter() *fakeWriter {
	return &fakeWriter{appended: map[string][]*classad.ClassAd{}, seen: map[string]map[string]bool{}}
}

func (w *fakeWriter) Append(_ context.Context, table string, ad *classad.ClassAd) error {
	w.appended[table] = append(w.appended[table], ad)
	if w.seen[table] == nil {
		w.seen[table] = map[string]bool{}
	}
	if g, ok := ad.EvaluateAttrString("GlobalJobId"); ok {
		w.seen[table][g] = true
	}
	return nil
}

func (w *fakeWriter) Has(_ context.Context, table, gid string) (bool, error) {
	return w.seen[table] != nil && w.seen[table][gid], nil
}

// preseed marks a GlobalJobId as already present without recording an append.
func (w *fakeWriter) preseed(table, gid string) {
	if w.seen[table] == nil {
		w.seen[table] = map[string]bool{}
	}
	w.seen[table][gid] = true
}

type fakeCursors map[string]string

func (c fakeCursors) key(job, schedd string) string { return job + "\x00" + schedd }
func (c fakeCursors) Get(job, schedd string) (string, bool) {
	v, ok := c[c.key(job, schedd)]
	return v, ok
}
func (c fakeCursors) Set(job, schedd, cur string) error {
	c[c.key(job, schedd)] = cur
	return nil
}

func histAd(cluster, proc int, schedd string) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttr("ClusterId", int64(cluster))
	ad.InsertAttr("ProcId", int64(proc))
	ad.InsertAttrString("GlobalJobId", fmt.Sprintf("%s#%d.%d#100", schedd, cluster, proc))
	ad.InsertAttr("JobStatus", int64(4))
	return ad
}

func newImporter(disc Discovery, src Source, w Writer, cur Cursors) *Importer {
	return &Importer{Disc: disc, Src: src, W: w, Cur: cur, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }}
}

// --- tests ---

func TestBootstrapImportsAllAndStamps(t *testing.T) {
	disc := fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}}
	src := fakeSource{hist: map[string][]*classad.ClassAd{
		"schedA": {histAd(1, 0, "schedA"), histAd(2, 0, "schedA"), histAd(3, 0, "schedA")},
	}}
	w := newFakeWriter()
	cur := fakeCursors{}
	im := newImporter(disc, src, w, cur)

	st, err := im.RunJob(context.Background(), Job{Name: "j1", Pool: "p", Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Imported != 3 || st.Schedds != 1 {
		t.Fatalf("stats = %+v, want 3 imported / 1 schedd", st)
	}
	recs := w.appended["history"]
	if len(recs) != 3 {
		t.Fatalf("appended %d, want 3", len(recs))
	}
	for _, r := range recs {
		if sn, _ := r.EvaluateAttrString(ScheddNameAttr); sn != "schedA" {
			t.Errorf("ScheddName not stamped: %q", sn)
		}
		if et, ok := r.EvaluateAttrInt(EnteredHistoryAttr); !ok || et != 1_700_000_000 {
			t.Errorf("EnteredHistoryTime not stamped: %d ok=%v", et, ok)
		}
	}
	// Cursor advanced to the newest (3.0).
	if v, _ := cur.Get("j1", "schedA"); v != "3.0" {
		t.Errorf("cursor = %q, want 3.0", v)
	}
}

func TestIncrementalUsesCursorAndAdvances(t *testing.T) {
	disc := fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}}
	src := fakeSource{hist: map[string][]*classad.ClassAd{
		"schedA": {histAd(1, 0, "schedA"), histAd(2, 0, "schedA"), histAd(3, 0, "schedA")},
	}}
	w := newFakeWriter()
	cur := fakeCursors{}
	cur.Set("j1", "schedA", "2.0") // already imported up to 2.0
	im := newImporter(disc, src, w, cur)

	st, err := im.RunJob(context.Background(), Job{Name: "j1", Pool: "p", Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	// Only 3.0 is newer than the 2.0 cursor.
	if st.Imported != 1 {
		t.Fatalf("imported %d, want 1 (only 3.0 is new)", st.Imported)
	}
	if k := jobKey(w.appended["history"][0]); k != "3.0" {
		t.Errorf("imported %q, want 3.0", k)
	}
	if v, _ := cur.Get("j1", "schedA"); v != "3.0" {
		t.Errorf("cursor = %q, want 3.0", v)
	}
}

func TestRecoveryDedupStopsAtArchivedPrefix(t *testing.T) {
	// No cursor (lost), but the archive already holds 1.0 and 2.0; 3.0 is new.
	disc := fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}}
	src := fakeSource{hist: map[string][]*classad.ClassAd{
		"schedA": {histAd(1, 0, "schedA"), histAd(2, 0, "schedA"), histAd(3, 0, "schedA")},
	}}
	w := newFakeWriter()
	w.preseed("history", "schedA#1.0#100")
	w.preseed("history", "schedA#2.0#100")
	cur := fakeCursors{} // no cursor -> recovery dedup
	im := newImporter(disc, src, w, cur)

	st, err := im.RunJob(context.Background(), Job{Name: "j1", Pool: "p", Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	// Only 3.0 appended; 2.0/1.0 recognized as present, backward scan stops.
	if st.Imported != 1 {
		t.Fatalf("imported %d, want 1 (dedup should skip the archived prefix)", st.Imported)
	}
	if len(w.appended["history"]) != 1 || jobKey(w.appended["history"][0]) != "3.0" {
		t.Errorf("appended wrong set: %+v", w.appended["history"])
	}
	if v, _ := cur.Get("j1", "schedA"); v != "3.0" {
		t.Errorf("cursor = %q, want 3.0", v)
	}
}

func TestMultiScheddAggregationIntoOneTable(t *testing.T) {
	disc := fakeDiscovery{schedds: []ScheddRef{
		{Name: "schedA", Address: "<a>"}, {Name: "schedB", Address: "<b>"},
	}}
	src := fakeSource{hist: map[string][]*classad.ClassAd{
		"schedA": {histAd(5, 0, "schedA")},                         // cluster 5 on A
		"schedB": {histAd(5, 0, "schedB"), histAd(5, 1, "schedB")}, // cluster 5 on B too
	}}
	w := newFakeWriter()
	cur := fakeCursors{}
	im := newImporter(disc, src, w, cur)

	st, err := im.RunJob(context.Background(), Job{Name: "j1", Pool: "p", Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Imported != 3 || st.Schedds != 2 {
		t.Fatalf("stats = %+v, want 3 imported / 2 schedds", st)
	}
	// Both schedds' cluster-5 jobs land in one table, distinguished by ScheddName
	// (proof (ClusterId,ProcId) collisions across schedds don't merge).
	byS := map[string]int{}
	for _, r := range w.appended["history"] {
		sn, _ := r.EvaluateAttrString(ScheddNameAttr)
		byS[sn]++
	}
	if byS["schedA"] != 1 || byS["schedB"] != 2 {
		t.Errorf("per-schedd counts = %v, want A:1 B:2", byS)
	}
	// Independent per-schedd cursors.
	if a, _ := cur.Get("j1", "schedA"); a != "5.0" {
		t.Errorf("schedA cursor = %q, want 5.0", a)
	}
	if b, _ := cur.Get("j1", "schedB"); b != "5.1" {
		t.Errorf("schedB cursor = %q, want 5.1", b)
	}
}

func TestOneScheddFailureDoesNotBlockOthers(t *testing.T) {
	disc := fakeDiscovery{schedds: []ScheddRef{
		{Name: "bad", Address: "<x>"}, {Name: "good", Address: "<y>"},
	}}
	src := fakeSource{
		hist: map[string][]*classad.ClassAd{"good": {histAd(1, 0, "good")}},
		err:  map[string]error{"bad": errors.New("unreachable")},
	}
	w := newFakeWriter()
	im := newImporter(disc, src, w, fakeCursors{})

	st, err := im.RunJob(context.Background(), Job{Name: "j1", Pool: "p", Table: "history"})
	if err != nil {
		t.Fatalf("RunJob should not fail wholesale: %v", err)
	}
	if st.Failures != 1 || st.Schedds != 1 || st.Imported != 1 {
		t.Fatalf("stats = %+v, want 1 failure / 1 schedd / 1 imported", st)
	}
}

func TestNoNewRecordsLeavesCursor(t *testing.T) {
	disc := fakeDiscovery{schedds: []ScheddRef{{Name: "schedA", Address: "<a>"}}}
	src := fakeSource{hist: map[string][]*classad.ClassAd{
		"schedA": {histAd(1, 0, "schedA")},
	}}
	w := newFakeWriter()
	cur := fakeCursors{}
	cur.Set("j1", "schedA", "1.0")
	im := newImporter(disc, src, w, cur)

	st, err := im.RunJob(context.Background(), Job{Name: "j1", Pool: "p", Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Imported != 0 {
		t.Fatalf("imported %d, want 0", st.Imported)
	}
	if v, _ := cur.Get("j1", "schedA"); v != "1.0" {
		t.Errorf("cursor changed to %q, want unchanged 1.0", v)
	}
}
