package scheddsync

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// A reconcile merges each log run onto the row already stored, so a key never loses attributes the
// schedd logged in a different run (TestReconcileNoTransientUndefinedNonContiguousJobStatus). That
// protection depends on being able to READ the stored row. If the lookup misses on a key the table
// actually holds -- the storage-side key-resolution fault seen in production -- merging from an
// empty ad writes one run's attributes AS the whole record, which is the identity-less row the
// reconcile was probably run to repair.
//
// The reconciler can tell the two reasons for a miss apart, which the storage layer cannot: it
// snapshots each table's keys before replaying, so a miss for a key in that snapshot is provably a
// resolve failure and not a create.

func newTestReconciler(t *testing.T, target *db.DB, before []string) *reconciler {
	t.Helper()
	b := map[string]struct{}{}
	for _, k := range before {
		b[k] = struct{}{}
	}
	return &reconciler{
		jobs:      target,
		seen:      map[*db.DB]map[string]struct{}{},
		batches:   map[*db.DB]*db.Txn{},
		before:    map[*db.DB]map[string]struct{}{target: b},
		destroyed: map[*db.DB]map[string]struct{}{target: {}},
	}
}

// applyRun feeds one key's run of attribute sets through the reconciler.
func applyRun(t *testing.T, r *reconciler, target *db.DB, key string, attrs map[string]int64) {
	t.Helper()
	r.curKey, r.curAd, r.curDels, r.curTable, r.destroy = key, classad.New(), nil, target, false
	for n, v := range attrs {
		r.curAd.InsertAttr(n, v)
	}
	if err := r.flush(); err != nil {
		t.Fatal(err)
	}
	if err := r.commit(); err != nil {
		t.Fatal(err)
	}
}

// TestReconcileRefusesToOverwriteOnLookupMiss: the table "held" the key (it is in the before set)
// but the row is not readable, so the run must NOT be written as a whole record.
func TestReconcileRefusesToOverwriteOnLookupMiss(t *testing.T) {
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// The key is in the before-snapshot but absent from the table: the shape of a resolve miss.
	r := newTestReconciler(t, d, []string{"1.0"})
	applyRun(t, r, d, "1.0", map[string]int64{"CompletionDate": 1789838852, "RemoteWallClockTime": 2890})

	if r.lookupMiss != 1 {
		t.Errorf("lookupMiss = %d, want 1", r.lookupMiss)
	}
	if ad, ok := d.LookupClassAd("1.0"); ok {
		t.Errorf("a fragment was written anyway: %d attributes", len(ad.AST().Attributes))
	}
}

// TestReconcileStillCreatesGenuinelyNewKeys: the other side. A key NOT in the before-snapshot is a
// create, and refusing it would make a reconcile unable to add jobs submitted since the snapshot.
func TestReconcileStillCreatesGenuinelyNewKeys(t *testing.T) {
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	r := newTestReconciler(t, d, nil) // empty before-set: every key is new
	applyRun(t, r, d, "2.0", map[string]int64{"ClusterId": 2, "JobStatus": 1})

	if r.lookupMiss != 0 {
		t.Errorf("lookupMiss = %d on a genuine create, want 0", r.lookupMiss)
	}
	ad, ok := d.LookupClassAd("2.0")
	if !ok {
		t.Fatal("a genuinely new key was not created")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 1 {
		t.Errorf("JobStatus = %v (ok=%v), want 1", v, ok)
	}
}

// TestReconcileRecreatesAfterDestroy: a key destroyed during this reconcile and then re-created by
// a later log run must be written, even though it is in the before-snapshot. Without tracking
// destroys, the guard would swallow the re-creation.
func TestReconcileRecreatesAfterDestroy(t *testing.T) {
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	seed := classad.New()
	seed.InsertAttr("ClusterId", int64(3))
	seed.InsertAttr("JobStatus", int64(2))
	tx := d.Begin()
	tx.NewClassAd("3.0", seed)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	r := newTestReconciler(t, d, []string{"3.0"})
	r.seen[d] = map[string]struct{}{}
	// Destroy it the way a log run would.
	r.curKey, r.curAd, r.curTable, r.destroy = "3.0", classad.New(), d, true
	if err := r.flush(); err != nil {
		t.Fatal(err)
	}
	// Then a later run re-creates it.
	applyRun(t, r, d, "3.0", map[string]int64{"ClusterId": 3, "JobStatus": 1})

	if r.lookupMiss != 0 {
		t.Errorf("lookupMiss = %d: a re-creation after destroy was mistaken for a resolve miss", r.lookupMiss)
	}
	ad, ok := d.LookupClassAd("3.0")
	if !ok {
		t.Fatal("the re-created key is missing: the guard swallowed it")
	}
	if v, ok := ad.EvaluateAttrInt("JobStatus"); !ok || v != 1 {
		t.Errorf("JobStatus = %v (ok=%v), want 1", v, ok)
	}
}
