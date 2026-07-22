package scheddsync

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// TestJobSyncRuntimeRotationDetection: a rotation that happens while the syncer is RUNNING
// (not across a restart) must be caught by the inode check even when the new file is LARGER
// than our current offset -- the case the size-based prober misreads as a plain append and
// would read our stale offset into the wrong file. Reusing one syncer across polls exercises
// the live-detection path (haveID set from the first read), not the startup restore path.
func TestJobSyncRuntimeRotationDetection(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n"+
		"105\n101 2.0 Job Machine\n103 2.0 Owner \"bob\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	s := NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target.Len() != 2 {
		t.Fatalf("after first sync Len = %d, want 2", target.Len())
	}

	// While running, the schedd compacts to a NEW inode: 2.0 completed and is gone, 3.0 and
	// 4.0 are new. The new file is LARGER than the old one (3 jobs vs 2), so the prober's
	// size heuristic sees growth, not a shrink -- only the inode check catches the rotation.
	renameOver(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n"+
		"105\n101 3.0 Job Machine\n103 3.0 Owner \"carol\"\n106\n"+
		"105\n101 4.0 Job Machine\n103 4.0 Owner \"dave\"\n106\n")

	// Same syncer, next poll: the inode differs, so it reconciles rather than mis-reading.
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ghost := target.LookupClassAd("2.0"); ghost {
		t.Error("2.0 is a ghost: dropped by the rotation but still present")
	}
	for _, k := range []string{"1.0", "3.0", "4.0"} {
		if _, ok := target.LookupClassAd(k); !ok {
			t.Errorf("%s missing after runtime rotation reconcile", k)
		}
	}
	if target.Len() != 3 {
		t.Fatalf("after reconcile Len = %d, want 3", target.Len())
	}
}

// TestJobSyncReconcileEmitsProperDeletes: a reconciling reload must emit a real DELETE for a
// job that completed while the log was rotated away (so downstream watchers drop it), and must
// NOT delete a surviving job (no blink-out). This is the watcher-correctness win over
// Truncate+replay, which drops everything silently (no delete) and re-adds survivors.
func TestJobSyncReconcileEmitsProperDeletes(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n"+
		"105\n101 2.0 Job Machine\n103 2.0 Owner \"bob\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	s := NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Watch from the current head, so we observe only the events the reload produces.
	cursor, err := target.WatchCursor()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seq, err := target.Watch(ctx, cursor)
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan db.WatchEvent, 64)
	go func() {
		for ev := range seq {
			events <- ev
		}
	}()

	// Compact while running: 2.0 completed and is gone; only 1.0 remains (fresh inode).
	renameOver(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var deletes, upserts []string
	deadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case ev := <-events:
			switch ev.Kind {
			case db.WatchDelete:
				deletes = append(deletes, ev.Key)
			case db.WatchUpsert:
				upserts = append(upserts, ev.Key)
			}
			// The sweep's delete of 2.0 is the last commit of the reload; once seen we have
			// everything.
			for _, k := range deletes {
				if k == "2.0" {
					break collect
				}
			}
		case <-deadline:
			break collect
		}
	}
	cancel()

	sawDelete := func(key string) bool {
		for _, k := range deletes {
			if k == key {
				return true
			}
		}
		return false
	}
	if !sawDelete("2.0") {
		t.Errorf("no WatchDelete for 2.0: a completed-while-rotated job left no delete event (phantom in watchers); deletes=%v upserts=%v", deletes, upserts)
	}
	if sawDelete("1.0") {
		t.Errorf("1.0 was deleted during reconcile (blink-out); a surviving job must not be dropped; deletes=%v", deletes)
	}
}

// TestJobSyncReconcileSkipsUnchanged: a reconciling reload must publish ONLY the real deltas --
// no event at all for a job whose value is unchanged (the common case in a compaction), an
// upsert for a changed job and a new job, and a delete for a removed job. This is the
// watcher-quiet win over both Truncate+replay and a plain re-upsert-everything reload.
func TestJobSyncReconcileSkipsUnchanged(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n"+
		"105\n101 2.0 Job Machine\n103 2.0 Owner \"bob\"\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	s := NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	cursor, err := target.WatchCursor()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seq, err := target.Watch(ctx, cursor)
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan db.WatchEvent, 64)
	go func() {
		for ev := range seq {
			events <- ev
		}
	}()

	// Compaction: 1.0 unchanged, 3.0 is new, 2.0 completed (removed). The rewrite carries the
	// full current state (1.0 + 3.0) under a fresh inode.
	renameOver(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 Owner \"alice\"\n106\n"+
		"105\n101 3.0 Job Machine\n103 3.0 Owner \"carol\"\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var deletes, upserts []string
	deadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case ev := <-events:
			switch ev.Kind {
			case db.WatchDelete:
				deletes = append(deletes, ev.Key)
			case db.WatchUpsert:
				upserts = append(upserts, ev.Key)
			}
			for _, k := range deletes {
				if k == "2.0" { // the sweep's delete is the final commit of the reload
					break collect
				}
			}
		case <-deadline:
			break collect
		}
	}
	cancel()

	has := func(s []string, key string) bool {
		for _, k := range s {
			if k == key {
				return true
			}
		}
		return false
	}
	if has(upserts, "1.0") {
		t.Errorf("1.0 was re-published though unchanged; reconcile must skip it. upserts=%v", upserts)
	}
	if !has(upserts, "3.0") {
		t.Errorf("3.0 (new) should have been upserted; upserts=%v", upserts)
	}
	if !has(deletes, "2.0") {
		t.Errorf("2.0 (removed) should have been deleted; deletes=%v", deletes)
	}
}

// TestJobSyncReconcilePublishesChangedJob: a job present before AND after a compaction but with
// a changed attribute value must be re-published (ClassAd.Equal detects the difference), while
// a genuinely unchanged sibling is not. This is the discriminating case the skip test does not
// cover.
func TestJobSyncReconcilePublishesChangedJob(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}
	writeFile(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 JobStatus 1\n106\n"+
		"105\n101 2.0 Job Machine\n103 2.0 JobStatus 1\n106\n"+
		"105\n101 4.0 Job Machine\n103 4.0 JobStatus 1\n106\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	s := NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	cursor, err := target.WatchCursor()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seq, err := target.Watch(ctx, cursor)
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan db.WatchEvent, 64)
	go func() {
		for ev := range seq {
			events <- ev
		}
	}()

	// Compaction: 1.0 unchanged, 2.0's JobStatus changed 1 -> 4, 3.0 is new, and 4.0 is dropped.
	// The upserts land in one commit (intra-commit key order is non-deterministic), so we key
	// the terminal condition on 4.0's DELETE, which the sweep commits strictly afterward.
	renameOver(t, logPath, "105\n101 1.0 Job Machine\n103 1.0 JobStatus 1\n106\n"+
		"105\n101 2.0 Job Machine\n103 2.0 JobStatus 4\n106\n"+
		"105\n101 3.0 Job Machine\n103 3.0 JobStatus 1\n106\n")
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	upserts := map[string]bool{}
	sawSweep := false
	deadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case ev := <-events:
			switch ev.Kind {
			case db.WatchUpsert:
				upserts[ev.Key] = true
			case db.WatchDelete:
				if ev.Key == "4.0" { // sweep delete: strictly after every upsert in commit order
					sawSweep = true
					break collect
				}
			}
		case <-deadline:
			break collect
		}
	}
	cancel()

	if !sawSweep {
		t.Fatalf("did not observe the sweep delete of 4.0 within the deadline; upserts=%v", upserts)
	}
	if !upserts["2.0"] {
		t.Errorf("2.0 changed value (JobStatus 1->4) but was not re-published; upserts=%v", upserts)
	}
	if upserts["1.0"] {
		t.Errorf("1.0 was unchanged but was re-published; upserts=%v", upserts)
	}
	if ad, ok := target.LookupClassAd("2.0"); !ok {
		t.Error("2.0 missing from table after reconcile")
	} else if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 4 {
		t.Errorf("2.0 JobStatus in table = %d, want 4", v)
	}
}

// TestJobSyncReconcileBatchBoundary: with the commit batch lowered so the reload crosses
// several boundaries, every delta must still be applied -- nothing lost at a batch edge or in
// the final partial batch.
func TestJobSyncReconcileBatchBoundary(t *testing.T) {
	orig := reconcileBatch
	reconcileBatch = 2
	defer func() { reconcileBatch = orig }()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	store := &FileStore{Path: filepath.Join(dir, "jobs.pos")}

	var before strings.Builder
	for i := 1; i <= 5; i++ {
		fmt.Fprintf(&before, "105\n101 %d.0 Job Machine\n103 %d.0 JobStatus 1\n106\n", i, i)
	}
	writeFile(t, logPath, before.String())

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath, Store: store})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target.Len() != 5 {
		t.Fatalf("after first sync Len = %d, want 5", target.Len())
	}

	// Compaction: all 5 change JobStatus 1 -> 4, one (3.0) is dropped, one (6.0) is new.
	var after strings.Builder
	for _, i := range []int{1, 2, 4, 5, 6} {
		fmt.Fprintf(&after, "105\n101 %d.0 Job Machine\n103 %d.0 JobStatus 4\n106\n", i, i)
	}
	renameOver(t, logPath, after.String())
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, ghost := target.LookupClassAd("3.0"); ghost {
		t.Error("3.0 (dropped) is a ghost after a multi-batch reconcile")
	}
	if target.Len() != 5 {
		t.Fatalf("after reconcile Len = %d, want 5 (1,2,4,5,6)", target.Len())
	}
	for _, i := range []int{1, 2, 4, 5, 6} {
		ad, ok := target.LookupClassAd(fmt.Sprintf("%d.0", i))
		if !ok {
			t.Errorf("%d.0 missing after multi-batch reconcile", i)
			continue
		}
		if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 4 {
			t.Errorf("%d.0 JobStatus = %d, want 4 (change lost across a batch boundary?)", i, v)
		}
	}
}
