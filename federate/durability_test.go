package federate

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
)

// orderLog records the sequence of durability steps across the data store and the cursor store.
type orderLog struct {
	mu    sync.Mutex
	steps []string
}

func (o *orderLog) add(s string) { o.mu.Lock(); o.steps = append(o.steps, s); o.mu.Unlock() }
func (o *orderLog) take() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := o.steps
	o.steps = nil
	return out
}

// recordingStore is a CursorStore that logs each Save.
type recordingStore struct {
	replicate.MemCursorStore
	log *orderLog
}

func (r *recordingStore) Save(c []byte) error {
	r.log.add("cursor:" + string(c))
	return r.MemCursorStore.Save(c)
}

func liveChange(c replicate.Change, cur string) replicate.Change {
	c.Cursor = []byte(cur)
	return c
}

func sameSteps(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestTableFlushOrder: a flush makes the batch durable before it commits the cursor that covers
// it, and a batch that fails to commit leaves the cursor where it was. Otherwise an OS crash could
// lose writes a committed cursor already covers -- a silent, permanent gap in the hub.
func TestTableFlushOrder(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	jobs := mustTable(t, cat, TableJobs)
	log := &orderLog{}
	store := &recordingStore{log: log}
	s, err := newTableSink(jobs, TableJobs, "ap1", store, NewMetrics(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var failCommit bool
	s.commit = func(tx *db.Txn) error {
		if failCommit {
			log.add("commit-failed")
			tx.Abort()
			return errors.New("disk full")
		}
		log.add("commit")
		return tx.Commit()
	}

	apply(t, s, reset(), synced("s0"))
	log.take()
	apply(t, s, liveChange(upsert("1.0", jobAd(t, 1, 0, "")), "c1"), liveChange(upsert("2.0", jobAd(t, 2, 0, "")), "c2"))
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); !sameSteps(got, "commit", "cursor:c2") {
		t.Fatalf("flush steps = %v, want one durable commit, then the cursor", got)
	}

	failCommit = true
	apply(t, s, liveChange(upsert("3.0", jobAd(t, 3, 0, "")), "c3"))
	if err := s.Flush(); err == nil {
		t.Fatal("flush succeeded with a failed commit")
	}
	if got := log.take(); !sameSteps(got, "commit-failed") {
		t.Fatalf("steps after a failed commit = %v, want no cursor save", got)
	}
	if cur, _ := store.Load(); string(cur) != "c2" {
		t.Fatalf("cursor after a failed commit = %q, want c2", cur)
	}
}

// TestArchiveFlushOrder: the archive's durability step runs once per flush (not per record) and
// before the cursor is committed; a failed sync leaves the cursor where it was.
func TestArchiveFlushOrder(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)
	log := &orderLog{}
	store := &recordingStore{log: log}
	s := newHistSink(t, hist, "ap1", store, NewMetrics())
	var failSync bool
	s.syncData = func() error {
		if failSync {
			log.add("sync-failed")
			return errors.New("EIO")
		}
		log.add("sync")
		return nil
	}

	apply(t, s, reset(), synced("s0"))
	if got := log.take(); !sameSteps(got, "cursor:", "cursor:s0") {
		t.Fatalf("Reset then Synced with nothing appended: steps %v, want the Reset's cleared cursor, then the cursor", got)
	}
	for i := 1; i <= 3; i++ {
		apply(t, s, liveChange(upsert("r", histRecord(t, "ap1", i)), "c"+itoa(i)))
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); !sameSteps(got, "sync", "cursor:c3") {
		t.Fatalf("flush steps = %v, want one sync for three appends, then the cursor", got)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); len(got) != 0 {
		t.Fatalf("idle flush steps = %v, want none", got)
	}

	failSync = true
	apply(t, s, liveChange(upsert("r", histRecord(t, "ap1", 4)), "c4"))
	if err := s.Flush(); err == nil {
		t.Fatal("flush succeeded with a failed sync")
	}
	if got := log.take(); !sameSteps(got, "sync-failed") {
		t.Fatalf("steps after a failed sync = %v, want no cursor save", got)
	}
	if cur, _ := store.Load(); string(cur) != "c3" {
		t.Fatalf("cursor after a failed sync = %q, want c3", cur)
	}
}

func TestFileCursorStore(t *testing.T) {
	st := fileCursorStore{path: t.TempDir() + "/x.cursor"}
	if b, err := st.Load(); err != nil || b != nil {
		t.Fatalf("empty store Load = %q, %v", b, err)
	}
	for _, c := range []string{"one", "two"} {
		if err := st.Save([]byte(c)); err != nil {
			t.Fatal(err)
		}
		if b, _ := st.Load(); string(b) != c {
			t.Fatalf("Load = %q, want %q", b, c)
		}
	}
}

// TestResetClearsCursor: a Reset durably clears the committed cursor before the replay is applied,
// so a replay cut off by a hub crash or a dropped stream restarts as a full replay -- never as a
// resume from the pre-Reset cursor, which could resume incrementally (an HA peer with the old
// epoch, say) and leave the unfinished replay's phantoms unswept. Both sinks.
func TestResetClearsCursor(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	path := t.TempDir() + "/jobs.cursor"
	jobs := mustTable(t, cat, TableJobs)
	s, err := newTableSink(jobs, TableJobs, "ap1", fileCursorStore{path: path}, NewMetrics(), time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.BeginSession()
	apply(t, s, reset(), upsert("1.0", jobAd(t, 1, 0, "")), synced("c1"))
	s.EndSession()
	s.BeginSession()
	apply(t, s, reset(), upsert("1.0", jobAd(t, 1, 0, ""))) // the hub crashes mid-replay
	reopened, err := newTableSink(jobs, TableJobs, "ap1", fileCursorStore{path: path}, NewMetrics(), time.Now, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cur := reopened.Cursor(); len(cur) != 0 {
		t.Errorf("table sink: cursor after a crash mid-replay = %q, want none", cur)
	}

	hist := hubArchive(t, cat, TableHistory)
	hpath := t.TempDir() + "/history.cursor"
	a := newHistSink(t, hist, "ap1", fileCursorStore{path: hpath}, NewMetrics())
	a.BeginSession()
	apply(t, a, reset(), upsert("r1", histRecord(t, "ap1", 1)), synced("h1"))
	a.EndSession()
	a.BeginSession()
	apply(t, a, reset(), upsert("r1", histRecord(t, "ap1", 1)))
	ra := newHistSink(t, hist, "ap1", fileCursorStore{path: hpath}, NewMetrics())
	if cur := ra.Cursor(); len(cur) != 0 {
		t.Errorf("archive sink: cursor after a crash mid-replay = %q, want none", cur)
	}
}
