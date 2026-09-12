package scheddsync

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// TestIdleSourceStaysFresh is about the difference between a mirror with nothing to do and a
// mirror that has stopped working.
//
// Consumers gate their reads on how long ago the mirror last synced: a tailer that has wedged
// stops reporting, its LastSync ages, and the gate closes. That check only works if a *healthy*
// tailer keeps its LastSync moving. Refreshing it only when records arrive breaks that, because a
// quiet access point writes nothing to its queue for minutes at a time -- the tailer is sitting at
// EOF, exactly current, and the reported lag grows with the wall clock until every consumer routes
// away from a mirror that has nothing wrong with it. On a pool that is idle more often than not,
// the mirror is then never used at all.
//
// So the assertion is that a poll which reads no data still advances LastSync. The clock is pinned
// and moved by hand, because the bug is precisely that the reported time stops tracking it.
func TestIdleSourceStaysFresh(t *testing.T) {
	realNow := nowFn
	t.Cleanup(func() { nowFn = realNow })
	clock := time.Unix(1_700_000_000, 0)
	nowFn = func() time.Time { return clock }

	ctx := context.Background()

	t.Run("job queue", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "job_queue.log")
		writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+submitJob(1, 0))

		d, err := db.Open("")
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()

		js := NewJobSync(d, JobSyncConfig{Filename: logPath})
		if err := js.Poll(ctx); err != nil {
			t.Fatalf("first poll: %v", err)
		}
		first := js.Status()
		if first.LastSync.IsZero() {
			t.Fatal("a poll that consumed the log left LastSync unset")
		}

		// Nothing is written to the log; only time passes.
		clock = clock.Add(5 * time.Minute)
		if err := js.Poll(ctx); err != nil {
			t.Fatalf("idle poll: %v", err)
		}
		idle := js.Status()

		if !idle.CaughtUp || idle.LagBytes != 0 {
			t.Fatalf("idle tailer should be at EOF: CaughtUp=%v LagBytes=%d", idle.CaughtUp, idle.LagBytes)
		}
		if !idle.LastSync.After(first.LastSync) {
			t.Errorf("LastSync did not advance over an idle poll: %v then %v -- an idle queue reads as a stalled mirror",
				first.LastSync, idle.LastSync)
		}
	})

	t.Run("history", func(t *testing.T) {
		dir := t.TempDir()
		histPath := filepath.Join(dir, "history")
		writeFile(t, histPath, histRecord(1, 0, 4))

		arch, cleanup := newArchive(t)
		defer cleanup()

		hs := NewHistorySync(arch, HistorySyncConfig{Filename: histPath})
		if err := hs.Poll(ctx); err != nil {
			t.Fatalf("first poll: %v", err)
		}
		first := hs.Status()
		if first.LastSync.IsZero() {
			t.Fatal("a poll that consumed the history file left LastSync unset")
		}

		clock = clock.Add(5 * time.Minute)
		if err := hs.Poll(ctx); err != nil {
			t.Fatalf("idle poll: %v", err)
		}
		idle := hs.Status()

		if !idle.LastSync.After(first.LastSync) {
			t.Errorf("LastSync did not advance over an idle poll: %v then %v", first.LastSync, idle.LastSync)
		}
	})

	// The other half of the contract: a source whose file does not exist has verified nothing, so
	// it must still report no sync at all. Consumers use that to tell "this mirror does not carry
	// history" apart from "this mirror's history is current".
	t.Run("absent file never reports a sync", func(t *testing.T) {
		arch, cleanup := newArchive(t)
		defer cleanup()

		hs := NewHistorySync(arch, HistorySyncConfig{Filename: filepath.Join(t.TempDir(), "nope")})
		if err := hs.Poll(ctx); err != nil {
			t.Fatalf("poll over an absent file: %v", err)
		}
		if st := hs.Status(); !st.LastSync.IsZero() {
			t.Errorf("LastSync = %v for a file that does not exist, want zero", st.LastSync)
		}
	})
}
