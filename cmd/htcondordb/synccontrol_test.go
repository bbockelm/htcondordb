package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestExporterResyncClearsStateAndRestarts: Resync gracefully stops the child, clears its cursor,
// and relaunches, so the exporter re-exports from the start.
func TestExporterResyncClearsStateAndRestarts(t *testing.T) {
	dir := t.TempDir()
	countFile := filepath.Join(dir, "count")
	script := filepath.Join(dir, "exp")
	// Records each launch, then sleeps -- exec so SIGTERM (graceful stop) reaches the direct child.
	body := "#!/bin/sh\necho x >> \"" + countFile + "\"\nexec sleep 3600\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	cat := &fakeCatalog{
		defs:  []db.ExporterDef{{Name: "jobs", Kind: "opensearch"}},
		state: map[string][]byte{"jobs": []byte("cursor=abc")},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &exporterManager{parent: ctx, catalog: cat, logger: discardLogger(), daemonAddr: "<127.0.0.1:1>"}
	s := exporterSettings{enabled: true, opensearchBin: script, pollInterval: 50 * time.Millisecond, gracefulTimeout: 500 * time.Millisecond}

	done := make(chan struct{})
	go m.reconcileLoop(ctx, s, done)

	// Launched once and marked running (so its child-cancel is published).
	waitForCount(t, countFile, 1, 3*time.Second)
	waitFor2(t, func() bool {
		for _, st := range m.Statuses() {
			if st.Name == "jobs" && st.Running {
				return true
			}
		}
		return false
	}, 3*time.Second)

	if err := m.Resync("jobs"); err != nil {
		t.Fatalf("Resync: %v", err)
	}

	// The child is stopped and relaunched (a second launch appears)...
	waitForCount(t, countFile, 2, 5*time.Second)
	// ...and the cursor was cleared so the relaunch re-exports from the start.
	waitFor2(t, func() bool {
		b, ok, _ := cat.LoadExporterState("jobs")
		return !ok || len(b) == 0
	}, 3*time.Second)

	// Resync of an unmanaged exporter errors.
	if err := m.Resync("nope"); err == nil {
		t.Error("Resync of an unknown exporter should error")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile loop did not shut down")
	}
}

// fakeResyncer records whether Resync was called, to prove routing reached a schedd-sync tailer.
type fakeResyncer struct{ called bool }

func (f *fakeResyncer) Resync() { f.called = true }

// TestSyncControllerRoutesEpochToSchedd guards the fix for `.resync epoch` misrouting: epoch must
// reach the schedd-sync manager (which registers an "epoch" resyncer), not fall through to the
// exporter branch. With an "epoch" resyncer registered, the request succeeds and the tailer is hit.
func TestSyncControllerRoutesEpochToSchedd(t *testing.T) {
	fake := &fakeResyncer{}
	sched := &scheddSyncManager{resyncers: map[string]resyncer{"epoch": fake}}
	// Empty exporter manager: had epoch wrongly routed here, it would error instead of succeeding.
	sc := &syncController{sched: sched, exp: &exporterManager{}}

	resp := sc.handle(mkReq("resync", "epoch"))
	if ok, _ := resp.EvaluateAttrBool("Ok"); !ok {
		e, _ := resp.EvaluateAttrString("Error")
		t.Fatalf("resync epoch should succeed via the schedd manager; got error %q", e)
	}
	if !fake.called {
		t.Error("resync epoch did not reach the schedd-sync tailer")
	}
}

func mkReq(action, target string) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("Action", action)
	ad.InsertAttrString("Target", target)
	return ad
}

// TestSyncControllerDispatch checks the DBSyncControl request routing and error surfacing without
// running real tailers/exporters (empty managers => targets resolve but have nothing to resync).
func TestSyncControllerDispatch(t *testing.T) {
	sc := &syncController{sched: &scheddSyncManager{}, exp: &exporterManager{}}

	okOf := func(ad *classad.ClassAd) bool { v, _ := ad.EvaluateAttrBool("Ok"); return v }

	// Empty target -> error.
	if okOf(sc.handle(mkReq("resync", ""))) {
		t.Error("empty target should fail")
	}
	// Unknown action -> error.
	if okOf(sc.handle(mkReq("frobnicate", "jobs"))) {
		t.Error("unknown action should fail")
	}
	// jobs/history route to the schedd-sync manager (which has no running tailers here -> error,
	// but the routing is exercised).
	if okOf(sc.handle(mkReq("resync", "jobs"))) {
		t.Error("resync jobs with no tailers running should fail")
	}
	// An unknown name routes to the exporter manager (no such managed exporter -> error).
	if okOf(sc.handle(mkReq("resync", "some-exporter"))) {
		t.Error("resync of an unknown exporter should fail")
	}
	// A missing Action defaults to resync (still fails here for lack of a live target, but Ok is
	// a bool and the Error is populated).
	resp := sc.handle(mkReq("", "jobs"))
	if okOf(resp) {
		t.Error("default action on a dead target should fail")
	}
	if e, _ := resp.EvaluateAttrString("Error"); e == "" {
		t.Error("failed resync should carry an Error string")
	}
}
