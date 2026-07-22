package scheddsync

import (
	"context"
	"path/filepath"
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
