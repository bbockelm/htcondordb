package cedarsync

import (
	"context"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db/replicate"
)

// TestCedarResume: a Runner restarted with the same (persisted) cursor store resumes from its
// committed cursor and imports only the records appended while it was down -- no re-import of the
// already-synced ones.
func TestCedarResume(t *testing.T) {
	catA, archA := mustArchive(t, "history")
	appendOld(t, archA, `GlobalJobId = "ap40#1.0"; Owner = "alice"; ClusterId = 1`)
	appendOld(t, archA, `GlobalJobId = "ap40#2.0"; Owner = "bob";   ClusterId = 2`)

	_, dst := mustArchive(t, "central")
	store := &replicate.MemCursorStore{} // durable cursor, shared across the two Runner lifetimes
	dial := dialFor(t, catA)

	// runUntil starts a Runner (fresh sink over the shared store) and stops it once cond holds.
	runUntil := func(cond func() bool) {
		sink, err := replicate.NewArchiveSink(dst, "ap40", store)
		if err != nil {
			t.Fatal(err)
		}
		r, err := NewRunner(dial, Config{Source: "history", Src: "ap40"}, sink, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = r.Run(ctx) }()
		waitFor(t, 5*time.Second, cond)
	}

	// First run: sync the two existing records and checkpoint a cursor.
	runUntil(func() bool {
		b, _ := store.Load()
		return dst.Count() == 2 && len(b) > 0
	})

	// While "down", a new record is appended to the source.
	appendOld(t, archA, `GlobalJobId = "ap40#3.0"; Owner = "carol"; ClusterId = 3`)

	// Second run resumes from the committed cursor: only the new record imports (total 3, no dupes).
	runUntil(func() bool { return dst.Count() == 3 })
	if n := count(t, dst, `Owner == "carol"`); n != 1 {
		t.Errorf("carol rows = %d, want 1", n)
	}
	time.Sleep(100 * time.Millisecond) // settle: catch any erroneous re-import
	if dst.Count() != 3 {
		t.Fatalf("dst has %d after resume, want 3 (no re-import of the first two)", dst.Count())
	}
}
