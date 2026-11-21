package federate

import (
	"testing"

	"github.com/PelicanPlatform/classad/db/replicate"
)

// replayInto drives one Reset replay of n history records of schedd into s, without ending the
// session, and returns the changes so a caller can finish it.
func replayInto(t *testing.T, s *archiveSink, schedd string, n int) {
	t.Helper()
	s.BeginSession()
	changes := []replicate.Change{reset()}
	for i := 1; i <= n; i++ {
		changes = append(changes, upsert("r", histRecord(t, schedd, i)))
	}
	apply(t, s, changes...)
}

// TestIdentityBudgetIsSharedAcrossSinks: the ceiling on identity sets belongs to the hub, not to
// each sink.
//
// It used to be per sink -- 64 MiB each -- which is why no sink could ever load one: an AP's
// job_metrics archive holds more identities than that buys, so every load was abandoned and the
// replay fell back to one exact query per record, measured at 0.09 records/s. Pooling the same
// memory means a lone replay may take all of it. The price is that two replays at once share it,
// and the one that cannot fit falls back to probing, which is what this covers.
func TestIdentityBudgetIsSharedAcrossSinks(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)

	// Room for one AP's set and not two, counted one identity at a time.
	const n = probeBudget + 20
	budget := newIdentityBudget(n + n/2)
	// Built through the constructor with the shared budget, as the hub builds them: a sink that
	// made its own would pass every assertion below but share nothing in production.
	mk := func(schedd string) *archiveSink {
		s, err := newArchiveSink(hist, TableHistory, schedd, &replicate.MemCursorStore{}, NewMetrics(), discard, nil, budget)
		if err != nil {
			t.Fatal(err)
		}
		s.identityChunk = 1
		return s
	}
	a, b := mk("ap1.example.org"), mk("ap2.example.org")
	if a.budget != budget || b.budget != budget {
		t.Fatal("newArchiveSink did not take the budget it was given")
	}

	// Fill the hub with each AP's records, so a later replay has identities to load.
	for _, x := range []struct {
		s      *archiveSink
		schedd string
	}{{a, "ap1.example.org"}, {b, "ap2.example.org"}} {
		replayInto(t, x.s, x.schedd, n)
		apply(t, x.s, synced("c1"))
		x.s.EndSession()
	}

	// ap1 replays again and holds its set: its catch-up is still open.
	replayInto(t, a, "ap1.example.org", n)
	if !a.setLoaded || a.setGaveUp {
		t.Fatalf("precondition: ap1 setLoaded=%v setGaveUp=%v, want the set loaded", a.setLoaded, a.setGaveUp)
	}
	if a.identityHeld < n {
		t.Fatalf("ap1 holds %d identities of the budget, want at least %d", a.identityHeld, n)
	}

	// ap2 replays while ap1 holds the budget: there is not room, so it probes instead.
	loadsBefore := b.setLoads
	replayInto(t, b, "ap2.example.org", n)
	if !b.setGaveUp {
		t.Errorf("ap2 setGaveUp=false; it should not have fitted beside ap1's set")
	}
	if b.setLoads != loadsBefore {
		t.Errorf("ap2 loaded a set (%d -> %d) although the budget was taken", loadsBefore, b.setLoads)
	}
	if b.identityHeld != 0 {
		t.Errorf("ap2 still holds %d identities after giving up, want 0", b.identityHeld)
	}

	// ap1's catch-up ends and returns its reservation; ap2 can then load.
	apply(t, a, synced("c2"))
	a.EndSession()
	if a.identityHeld != 0 {
		t.Fatalf("ap1 still holds %d identities after EndSession, want 0", a.identityHeld)
	}
	b.EndSession()
	replayInto(t, b, "ap2.example.org", n)
	if !b.setLoaded || b.setGaveUp {
		t.Errorf("ap2 setLoaded=%v setGaveUp=%v after the budget was returned, want the set loaded", b.setLoaded, b.setGaveUp)
	}
	apply(t, b, synced("c2"))
	b.EndSession()

	// The dedup itself is unaffected: two replays each, one copy of each record.
	if got := countArchive(t, hist, `true`); got != 2*n {
		t.Fatalf("hub holds %d records after two replays of %d by each of two APs, want %d", got, n, 2*n)
	}
}

// TestIdentityBudgetReleasedAtEndOfCatchup: a sink that loads a set and finishes gives the whole
// reservation back, so a hub does not leak its budget one replay at a time.
func TestIdentityBudgetReleasedAtEndOfCatchup(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hist := hubArchive(t, cat, TableHistory)

	const n = probeBudget + 20
	budget := newIdentityBudget(n + n/2)
	s, err := newArchiveSink(hist, TableHistory, "ap1.example.org", &replicate.MemCursorStore{}, NewMetrics(), discard, nil, budget)
	if err != nil {
		t.Fatal(err)
	}
	s.identityChunk = 1

	for i := 0; i < 4; i++ {
		replayInto(t, s, "ap1.example.org", n)
		apply(t, s, synced("c"))
		s.EndSession()
		if budget.used != 0 {
			t.Fatalf("after replay %d the budget still holds %d identities, want 0", i+1, budget.used)
		}
	}
}
