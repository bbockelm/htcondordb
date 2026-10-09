package federate

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// TestFailedCommitThenFlushKeepsCursor: a batch whose commit fails is dropped, so the cursor of its
// last change must be dropped with it. The Runner flushes once more when the failed session ends;
// that flush must not commit the dropped batch's cursor, or the next session resumes past rows the
// hub never wrote.
func TestFailedCommitThenFlushKeepsCursor(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	store := &replicate.MemCursorStore{}
	s, err := newTableSink(jobs, TableJobs, "ap1", store, NewMetrics(), time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	fail := true
	s.commit = func(tx *db.Txn) error {
		if fail {
			tx.Abort()
			return errors.New("disk full")
		}
		return tx.Commit()
	}
	s.BeginSession()
	apply(t, s, liveChange(upsert("3.0", jobAd(t, 3, 0, "")), "c3"))
	if err := s.Flush(); err == nil {
		t.Fatal("periodic flush succeeded with a failed commit")
	}
	fail = false
	_ = s.Flush() // the Runner's flush as the failed session ends
	s.EndSession()
	if cur, _ := store.Load(); len(cur) != 0 {
		t.Fatalf("cursor %q committed for a batch that was never written", cur)
	}

	// The next session starts clean: the dropped batch's state does not leak into it.
	s.BeginSession()
	apply(t, s, upsert("3.0", jobAd(t, 3, 0, "")), synced("s1"))
	if n := countWhere(t, jobs, `Key == "3.0"`); n != 1 {
		t.Fatalf("redelivered row 3.0: %d rows, want 1", n)
	}
	if cur, _ := store.Load(); string(cur) != "s1" {
		t.Fatalf("cursor after the redelivery = %q, want s1", cur)
	}
}

// TestArchiveFailedSyncThenFlushKeepsCursor is the archive sink's version: after a failed flush the
// session's later flushes commit no cursor; the next session commits again.
func TestArchiveFailedSyncThenFlushKeepsCursor(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	store := &replicate.MemCursorStore{}
	s := newHistSink(t, hist, "ap1", store, NewMetrics())
	fail := true
	s.syncData = func() error {
		if fail {
			return errors.New("EIO")
		}
		return nil
	}
	s.BeginSession()
	apply(t, s, reset(), synced("s0"))
	if err := s.Flush(); err != nil { // nothing appended: no sync, nothing to fail
		t.Fatal(err)
	}
	apply(t, s, liveChange(upsert("r", histRecord(t, "ap1", 1)), "c1"))
	if err := s.Flush(); err == nil {
		t.Fatal("flush succeeded with a failed sync")
	}
	fail = false
	if err := s.Apply(liveChange(upsert("r", histRecord(t, "ap1", 2)), "c2")); err == nil {
		t.Error("a sink whose flush failed accepted another change in the same session")
	}
	_ = s.Flush()
	s.EndSession()
	if cur, _ := store.Load(); string(cur) != "s0" {
		t.Fatalf("cursor after a failed sync = %q, want s0", cur)
	}
	s.BeginSession()
	apply(t, s, upsert("r", histRecord(t, "ap1", 1)), upsert("r", histRecord(t, "ap1", 2)), synced("s1"))
	if cur, _ := store.Load(); string(cur) != "s1" {
		t.Fatalf("cursor after the next session = %q, want s1", cur)
	}
	if n := countArchive(t, hist, `true`); n != 2 {
		t.Fatalf("hub history = %d records, want 2", n)
	}
}

// TestFailedCommitRedeliveredOverRealWatch: through the real Runner and Watch, a live upsert whose
// batch commit fails once arrives on the next session -- the resume cursor did not skip it.
func TestFailedCommitRedeliveredOverRealWatch(t *testing.T) {
	spoke := newTestSpoke(t)
	spoke.putJobs(1)
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	s, err := newTableSink(jobs, TableJobs, "ap1", &replicate.MemCursorStore{}, NewMetrics(), time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	var armed, fired atomic.Bool
	s.commit = func(tx *db.Txn) error {
		if armed.Load() && !fired.Load() && tx.Has(HubKey("ap1", jobKey(2))) {
			fired.Store(true)
			tx.Abort()
			return errors.New("transient commit failure")
		}
		return tx.Commit()
	}
	r, err := cedarsync.NewRunner(spoke.dial, cedarsync.Config{Source: TableJobs, Src: "ap1", FlushInterval: 20 * time.Millisecond}, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "initial sync", func() bool { return sameKeys(jobKeysOf(t, cat, "ap1"), 1) })
	armed.Store(true)
	spoke.putJobs(2)
	waitFor(t, "the commit failure", fired.Load)
	waitFor(t, "a second session", func() bool { return r.Status().Sessions >= 2 && r.Status().Connected })
	spoke.putJobs(3)
	waitFor(t, "jobs 1-3 on the hub", func() bool { return sameKeys(jobKeysOf(t, cat, "ap1"), 1, 2, 3) })
}

// TestPersistentCommitFailureStalls: a write the hub refuses every time must stall the stream where
// it is -- visibly, in the Runner's LastError, and backing off rather than spinning -- and never be
// skipped: once the refusal clears, the row arrives.
func TestPersistentCommitFailureStalls(t *testing.T) {
	spoke := newTestSpoke(t)
	spoke.putJobs(1)
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	store := &replicate.MemCursorStore{}
	s, err := newTableSink(jobs, TableJobs, "ap1", store, NewMetrics(), time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	var refuse atomic.Bool
	var refusals atomic.Int32
	s.commit = func(tx *db.Txn) error {
		if refuse.Load() && tx.Has(HubKey("ap1", jobKey(2))) {
			refusals.Add(1)
			tx.Abort()
			return errors.New("table is read-only")
		}
		return tx.Commit()
	}
	r, err := cedarsync.NewRunner(spoke.dial, cedarsync.Config{Source: TableJobs, Src: "ap1", FlushInterval: 20 * time.Millisecond}, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "initial sync", func() bool { return sameKeys(jobKeysOf(t, cat, "ap1"), 1) })
	refuse.Store(true)
	spoke.putJobs(2)
	waitFor(t, "the first refusal", func() bool { return refusals.Load() >= 1 })
	start := r.Status().Sessions
	time.Sleep(2 * time.Second)
	st := r.Status()
	if st.LastError == "" {
		t.Error("a stalled stream reports no LastError")
	}
	// Backoff 0.5s, 1s, 2s, ...: at most 3 more sessions in 2s. A spin would be hundreds.
	if n := st.Sessions - start; n > 3 {
		t.Errorf("%d sessions in 2s while every commit fails: the Runner is not backing off", n)
	}
	if jobKeysOf(t, cat, "ap1")[jobKey(2)] {
		t.Fatal("refused row present")
	}
	refuse.Store(false)
	spoke.putJobs(3)
	waitFor(t, "jobs 1-3 on the hub after the refusal cleared", func() bool { return sameKeys(jobKeysOf(t, cat, "ap1"), 1, 2, 3) })
}
