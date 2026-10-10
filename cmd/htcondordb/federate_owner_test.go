package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/server"
)

// hubCfg is a hub with one unreachable static spoke.
func hubCfg(dir, extra string) string {
	return "HTCONDORDB_DIR = " + dir + "\nHTCONDORDB_FEDERATE_SPOKES = ap1\nHTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <127.0.0.1:1>\n" + extra
}

func waitTable(t *testing.T, svc *server.Service, name string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := svc.Catalog().Table(name); ok {
			return
		}
		if _, ok := svc.Catalog().ArchiveTable(name); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("table %s never created", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestHubOwnsItsTables: every table a hub writes in process -- the federated tables and
// federation_sources -- is owned while the hub runs, so a remote client can neither write nor drop
// one under it; operator actions on them route through the daemon (truncate refused toward
// `.retire`, rotate/retention applied); and they are released when the hub stops.
func TestHubOwnsItsTables(t *testing.T) {
	svc := newOwnerTestService(t)
	fm := newFederationManager(t.Context(), svc.Catalog(), svc.Owners(), slog.Default())
	defer fm.Stop()
	hubTables := []string{"jobs", "history", "epoch_history", "syncstatus", "federation_sources"}
	if err := fm.apply(mkSyncCfg(t, hubCfg(t.TempDir(), "HTCONDORDB_FEDERATE_TABLES = jobs history epoch_history syncstatus\n"))); err != nil {
		t.Fatal(err)
	}
	for _, tb := range hubTables {
		if o := svc.Owners().Owners(tb); !slices.Equal(o, []string{ownerFederation}) {
			t.Errorf("owners(%s) = %v, want [%s]", tb, o, ownerFederation)
		}
	}
	waitTable(t, svc, "federation_sources")
	waitTable(t, svc, "jobs")
	waitTable(t, svc, "history")

	c := writeSession(t, svc)
	ctx := context.Background()
	for _, tb := range []string{"federation_sources", "jobs", "syncstatus"} {
		if err := c.DropTable(ctx, tb); err == nil {
			t.Errorf("a WRITE client dropped hub table %s", tb)
		}
	}
	tx, err := c.BeginTable(ctx, "federation_sources")
	if err != nil {
		t.Fatal(err)
	}
	perr := tx.NewClassAd(ctx, "ap9", "ScheddName = \"ap9\"\nSpokeAddress = \"<10.6.6.6:9620>\"\n")
	if perr == nil {
		perr = tx.Commit(ctx)
	}
	if perr == nil {
		t.Error("a WRITE client planted a federation_sources row")
	}

	sc := &syncController{fed: fm, owners: svc.Owners(), cat: svc.Catalog()}
	resp := sc.handle(ctx, mkReq("truncate", "jobs"))
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
		t.Fatal("truncate of the hub's jobs succeeded")
	}
	e, _ := resp.EvaluateAttrString("Error")
	if !strings.Contains(e, "federation hub") || !strings.Contains(e, ".retire <schedd>") {
		t.Errorf("truncate jobs error = %q, want the owner and `.retire`", e)
	}
	for _, bad := range []string{". .", "..", " ;", ";."} {
		if strings.Contains(e, bad) {
			t.Errorf("truncate jobs error has an empty sentence (%q): %q", bad, e)
		}
	}
	note, _ := sc.handle(ctx, mkReq("owner", "federation_sources")).EvaluateAttrString("Note")
	if !strings.Contains(note, "federation hub") || strings.Contains(note, "..") {
		t.Errorf("owner federation_sources = %q", note)
	}

	out := daemonSession(t, svc, sc, ".rotate history\n.retention history 0 1GB\n")
	if !strings.Contains(out, "rotated: dropped 0 segment(s)") || !strings.Contains(out, "retention set on history") {
		t.Errorf("rotate/retention on the hub's history = %q", out)
	}

	// A reconfigure dropping a table releases it; the rest stay owned throughout.
	if err := fm.apply(mkSyncCfg(t, hubCfg(t.TempDir(), "HTCONDORDB_FEDERATE_TABLES = jobs syncstatus\n"))); err != nil {
		t.Fatal(err)
	}
	if got := ownedSet(svc.Owners(), hubTables...); !slices.Equal(got, []string{"jobs", "syncstatus", "federation_sources"}) {
		t.Errorf("after dropping the archives: owned %v", got)
	}
	fm.Stop()
	if got := ownedSet(svc.Owners(), hubTables...); len(got) != 0 {
		t.Errorf("hub stopped: still owned %v", got)
	}
}

// TestHubScheddSyncSwitchInOneReconfig: a reconfigure that turns a hub into schedd sync (or back)
// never runs both writers on one table, in whichever order the managers are applied: the taker is
// refused while the giver holds the tables, and the retry at the end of the reconfigure starts it.
func TestHubScheddSyncSwitchInOneReconfig(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	svc := newOwnerTestService(t)
	dir := t.TempDir()
	jobLog := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fm := newFederationManager(t.Context(), svc.Catalog(), svc.Owners(), slog.Default())
	defer fm.Stop()
	sm := &scheddSyncManager{parent: t.Context(), svc: svc, logger: slog.Default()}
	defer sm.Stop()
	hub := mkSyncCfg(t, hubCfg(dir+"/db", ""))
	spoke := mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"/db\nJOB_EPOCH_HISTORY =\nHISTORY =\nHTCONDORDB_JOB_QUEUE_LOG = "+jobLog+"\n")
	hubRunning := func() bool { fm.mu.Lock(); defer fm.mu.Unlock(); return fm.hub != nil }
	syncRunning := func() bool { sm.mu.Lock(); defer sm.mu.Unlock(); return sm.cancel != nil }

	if err := fm.apply(hub); err != nil {
		t.Fatal(err)
	}
	// Schedd sync applied first (main.go's order) while the hub still runs: refused, nothing started.
	if err := sm.apply(spoke); !errors.Is(err, server.ErrTableClaimed) {
		t.Fatalf("schedd sync onto the hub's tables: err = %v, want ErrTableClaimed", err)
	}
	if syncRunning() || !hubRunning() {
		t.Fatalf("after the refusal: schedd sync running=%v, hub running=%v", syncRunning(), hubRunning())
	}

	// The whole reconfigure, as main.go runs it.
	reconfigure := func(cfg any) {
		t.Helper()
		var r reconfigRetry
		c := spoke
		if cfg == "hub" {
			c = hub
		}
		if err := r.run("schedd-sync", c, sm.apply); err != nil {
			t.Fatal(err)
		}
		if err := r.run("federation hub", c, fm.apply); err != nil {
			t.Fatal(err)
		}
		if failed := r.flush(c); len(failed) != 0 {
			t.Fatalf("retry failed: %v", failed)
		}
	}
	reconfigure("spoke")
	if !syncRunning() || hubRunning() {
		t.Fatalf("hub -> schedd sync: schedd sync running=%v, hub running=%v", syncRunning(), hubRunning())
	}
	if o := svc.Owners().Owners("jobs"); !slices.Equal(o, []string{ownerScheddSync}) {
		t.Errorf("owners(jobs) = %v after the switch", o)
	}
	if svc.Owners().Owned("federation_sources") {
		t.Error("federation_sources still owned after the hub stopped")
	}

	// And back: the hub applied while schedd sync holds jobs is refused the same way.
	if err := fm.apply(hub); !errors.Is(err, server.ErrTableClaimed) || !syncRunning() || hubRunning() {
		t.Fatalf("hub onto schedd sync's tables: err = %v, schedd sync running=%v, hub running=%v", err, syncRunning(), hubRunning())
	}
	reconfigure("hub")
	if syncRunning() || !hubRunning() {
		t.Fatalf("schedd sync -> hub: schedd sync running=%v, hub running=%v", syncRunning(), hubRunning())
	}
	if o := svc.Owners().Owners("jobs"); !slices.Equal(o, []string{ownerFederation}) {
		t.Errorf("owners(jobs) = %v after switching back", o)
	}
}

// TestReplicationTargetCollidesWithHub: a replication target naming a hub table is refused, in
// either order, and the writer already holding it keeps running.
func TestReplicationTargetCollidesWithHub(t *testing.T) {
	svc := newOwnerTestService(t)
	fm := newFederationManager(t.Context(), svc.Catalog(), svc.Owners(), slog.Default())
	defer fm.Stop()
	cm := &cedarSyncManager{parent: t.Context(), cat: svc.Catalog(), logger: discardLogger(), owners: svc.Owners()}
	defer cm.Stop()
	repl := mkSyncCfg(t, "HTCONDORDB_REPLICATE_SOURCES = r1\nHTCONDORDB_REPLICATE_R1_ADDRESS = <127.0.0.1:1>\nHTCONDORDB_REPLICATE_R1_TARGET = history\n")
	hub := mkSyncCfg(t, hubCfg(t.TempDir(), ""))

	if err := fm.apply(hub); err != nil {
		t.Fatal(err)
	}
	if err := cm.apply(repl); !errors.Is(err, server.ErrTableClaimed) {
		t.Fatalf("replication into the hub's history: err = %v, want ErrTableClaimed", err)
	}
	if o := svc.Owners().Owners("history"); !slices.Equal(o, []string{ownerFederation}) {
		t.Errorf("owners(history) = %v", o)
	}
	fm.Stop()

	if err := cm.apply(repl); err != nil {
		t.Fatal(err)
	}
	if err := fm.apply(hub); !errors.Is(err, server.ErrTableClaimed) {
		t.Fatalf("hub onto a replication target: err = %v, want ErrTableClaimed", err)
	}
	fm.mu.Lock()
	running := fm.hub != nil
	fm.mu.Unlock()
	if running || svc.Owners().Owned("federation_sources") {
		t.Errorf("refused hub: running=%v, federation_sources owned=%v", running, svc.Owners().Owned("federation_sources"))
	}
	if o := svc.Owners().Owners("history"); !slices.Equal(o, []string{ownerReplication}) {
		t.Errorf("owners(history) = %v", o)
	}
}

// TestFederationRetireEndToEnd: `.retire` from the REPL reaches the hub through DBSyncControl and
// prints the daemon's note; an unknown name is an error.
func TestFederationRetireEndToEnd(t *testing.T) {
	svc := newOwnerTestService(t)
	fm := newFederationManager(t.Context(), svc.Catalog(), svc.Owners(), slog.Default())
	defer fm.Stop()
	if err := fm.apply(mkSyncCfg(t, hubCfg(t.TempDir(), ""))); err != nil {
		t.Fatal(err)
	}
	sc := &syncController{fed: fm, owners: svc.Owners(), cat: svc.Catalog()}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if tb, ok := svc.Catalog().Table("federation_sources"); ok {
			if _, ok := tb.LookupClassAd("ap1"); ok {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("ap1 never known")
		}
		time.Sleep(20 * time.Millisecond)
	}
	out := daemonSession(t, svc, sc, ".retire ap1\n.retire nosuch\n")
	if !strings.Contains(out, `retired "ap1"`) || !strings.Contains(out, "archive rows age out") ||
		!strings.Contains(out, "no federated source") {
		t.Errorf(".retire output = %q, want the daemon's note and the unknown-name error", out)
	}
}

// TestFederationHubExitedEarly: a hub whose Run fails (it cannot open its tables) is forgotten, so
// `.retire` says no hub is running instead of waiting for one, and the next apply of the same
// configuration starts it again.
func TestFederationHubExitedEarly(t *testing.T) {
	svc := newOwnerTestService(t)
	// An archive named jobs: the hub cannot create its mutable jobs table.
	if _, err := svc.Catalog().CreateArchiveTable("jobs", db.ArchiveConfig{}); err != nil {
		t.Fatal(err)
	}
	fm := newFederationManager(t.Context(), svc.Catalog(), svc.Owners(), slog.Default())
	defer fm.Stop()
	cfg := mkSyncCfg(t, hubCfg(t.TempDir(), ""))
	if err := fm.apply(cfg); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		fm.mu.Lock()
		gone := fm.hub == nil
		fm.mu.Unlock()
		if gone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a hub whose Run failed is still registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	start := time.Now()
	resp := (&syncController{fed: fm}).handle(t.Context(), mkReq("retire", "ap1"))
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok || time.Since(start) > 2*time.Second {
		t.Fatalf("retire with no running hub: ok=%v after %v", ok, time.Since(start))
	}
	if err := fm.apply(cfg); err != nil {
		t.Fatal(err)
	}
	fm.mu.Lock()
	restarted := fm.hub != nil
	fm.mu.Unlock()
	if !restarted {
		t.Error("the same configuration did not restart the hub")
	}
}
