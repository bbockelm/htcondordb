package server

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// newGateService opens a persistent service with an owned mutable table ("jobs"), an owned
// archive ("history"), and an unowned table ("scratch") in the same catalog.
func newGateService(t *testing.T) *Service {
	t.Helper()
	svc, err := New(Config{Dir: t.TempDir(), Authorize: func(_, _, _ string) bool { return true }, DisableMaintenance: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	for _, name := range []string{"jobs", "scratch"} {
		if _, err := svc.Catalog().CreateTable(name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Catalog().CreateArchiveTable("history", db.ArchiveConfig{}); err != nil {
		t.Fatal(err)
	}
	svc.Owners().Set("schedd-sync", []string{"jobs", "history"})
	return svc
}

// dialAs serves one in-memory connection with exactly the options handleSession builds for a peer
// at level on CEDAR session sessionID, and returns a client on it.
func dialAs(t *testing.T, svc *Service, level Level, sessionID string) *dbrpc.Client {
	t.Helper()
	cp, sp := net.Pipe()
	go func() { _ = svc.RPC().ServeConnOpts(dbrpc.NewStreamConn(sp), svc.ServeOptions(level, sessionID)) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// putAd writes one ad to table in its own transaction.
func putAd(ctx context.Context, c *dbrpc.Client, table, key string) error {
	tx, err := c.BeginTable(ctx, table)
	if err != nil {
		return err
	}
	if err := tx.NewClassAd(ctx, key, `Owner = "alice"`); err != nil {
		_ = tx.Abort(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// TestOwnedTableReadOnlyToClients is the contract of the ownership gate: a WRITE and a DAEMON
// connection are both refused every data write to an owned table -- with dbrpc.ErrTableReadOnly,
// the non-retryable refusal -- while the same connections write an unowned table in the same
// catalog, and still read and watch the owned one.
func TestOwnedTableReadOnlyToClients(t *testing.T) {
	svc := newGateService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, level := range []Level{LevelWrite, LevelDaemon} {
		t.Run(level.String(), func(t *testing.T) {
			c := dialAs(t, svc, level, "")

			if err := putAd(ctx, c, "jobs", "1.0"); !errors.Is(err, dbrpc.ErrTableReadOnly) {
				t.Fatalf("write to owned jobs: err = %v, want ErrTableReadOnly", err)
			}
			// Case is folded: on a case-insensitive filesystem "JOBS" is the same directory.
			if err := c.CreateTable(ctx, "JOBS"); !errors.Is(err, dbrpc.ErrTableReadOnly) {
				t.Errorf("CREATE TABLE JOBS: err = %v, want ErrTableReadOnly", err)
			}
			if err := c.DropTable(ctx, "jobs"); !errors.Is(err, dbrpc.ErrTableReadOnly) {
				t.Errorf("DROP TABLE jobs: err = %v, want ErrTableReadOnly", err)
			}
			if err := c.ArchiveAppend(ctx, "history", "ClusterId = 1\nProcId = 0"); !errors.Is(err, dbrpc.ErrTableReadOnly) {
				t.Errorf("append to owned history: err = %v, want ErrTableReadOnly", err)
			}
			if err := putAd(ctx, c, "scratch", level.String()); err != nil {
				t.Errorf("write to unowned scratch: %v", err)
			}
			if _, err := c.QueryTable(ctx, "jobs", "true", 0); err != nil {
				t.Errorf("read of owned jobs: %v", err)
			}
		})
	}
	if d, _ := svc.Catalog().Table("jobs"); d.Len() != 0 {
		t.Errorf("owned jobs holds %d ads after refused writes, want 0", d.Len())
	}

	// The admin actions that remove data are refused even to DAEMON; the row-preserving ones
	// are not (an operator may still tune an owned table).
	d := dialAs(t, svc, LevelDaemon, "")
	if _, err := d.AdminTable(ctx, "history", "truncate"); !errors.Is(err, dbrpc.ErrTableReadOnly) {
		t.Errorf("admin truncate history: err = %v, want ErrTableReadOnly", err)
	}
	if _, err := d.AdminTable(ctx, "history", "index.reindex"); err != nil {
		t.Errorf("admin index.reindex history: %v", err)
	}

	// Watch on the owned table delivers the owner's (in-process) writes.
	w := dialAs(t, svc, LevelWrite, "")
	events, stop, err := w.WatchTable(ctx, "jobs", nil)
	if err != nil {
		t.Fatalf("watch owned jobs: %v", err)
	}
	defer stop()
	jobs, _ := svc.Catalog().Table("jobs")
	tx := jobs.Begin()
	if !tx.NewClassAdOld("2.0", `Owner = "bob"`) {
		t.Fatal("unparseable ad")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("in-process owner write: %v", err)
	}
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("watch closed before the owner's write arrived")
			}
			if ev.Key == "2.0" {
				return
			}
		case <-ctx.Done():
			t.Fatal("owner's write never reached the watch on the owned table")
		}
	}
}

// TestOwnedTableReleaseAndGrant: releasing ownership makes the table writable again on an
// already-open connection, and a session granted to the owner (an out-of-process writer the
// daemon launched) writes it while an ungranted one does not.
func TestOwnedTableReleaseAndGrant(t *testing.T) {
	svc := newGateService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc.Owners().GrantSession("sid-owner", "schedd-sync")
	svc.Owners().GrantSession("sid-other", "replication")
	granted := dialAs(t, svc, LevelDaemon, "sid-owner")
	other := dialAs(t, svc, LevelDaemon, "sid-other")
	if err := putAd(ctx, granted, "jobs", "g.0"); err != nil {
		t.Errorf("granted session writing its owner's table: %v", err)
	}
	if err := putAd(ctx, other, "jobs", "o.0"); !errors.Is(err, dbrpc.ErrTableReadOnly) {
		t.Errorf("session granted to a different owner: err = %v, want ErrTableReadOnly", err)
	}
	svc.Owners().RevokeSession("sid-owner")
	if err := putAd(ctx, granted, "jobs", "g.1"); !errors.Is(err, dbrpc.ErrTableReadOnly) {
		t.Errorf("revoked session: err = %v, want ErrTableReadOnly", err)
	}

	c := dialAs(t, svc, LevelWrite, "")
	svc.Owners().Set("schedd-sync", nil)
	if err := putAd(ctx, c, "jobs", "1.0"); err != nil {
		t.Errorf("write after ownership released: %v", err)
	}
}

// TestOwnedTableRefusalMessage pins the wording a client surfaces: it names the table, so the
// REPL can ask the daemon who owns it.
func TestOwnedTableRefusalMessage(t *testing.T) {
	svc := newGateService(t)
	c := dialAs(t, svc, LevelDaemon, "")
	err := putAd(context.Background(), c, "jobs", "1.0")
	if err == nil || !strings.Contains(err.Error(), `read-only table "jobs"`) {
		t.Fatalf("refusal = %v, want it to name the table", err)
	}
}
