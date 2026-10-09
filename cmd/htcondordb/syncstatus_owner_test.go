package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/server"
	"github.com/bbockelm/htcondordb/syncstatus"
)

// writeSession is a WRITE-level remote connection to svc.
func writeSession(t *testing.T, svc *server.Service) *dbrpc.Client {
	t.Helper()
	cp, sp := net.Pipe()
	go func() { _ = svc.RPC().ServeConnOpts(dbrpc.NewStreamConn(sp), svc.ServeOptions(server.LevelWrite, "")) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	t.Cleanup(func() { c.Close() })
	return c
}

// TestScheddSyncOwnsSyncstatus: the heartbeat table is written in process by schedd sync, so it is
// owned like the tables the tailers write -- a remote client can neither forge the heartbeat a hub
// computes freshness from nor drop the table under the writer -- and an operator truncate of it is
// refused with the reason.
func TestScheddSyncOwnsSyncstatus(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	svc := newOwnerTestService(t)
	dir := t.TempDir()
	jobLog := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := &scheddSyncManager{parent: t.Context(), svc: svc, logger: slog.Default()}
	defer m.Stop()
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"/db\nJOB_EPOCH_HISTORY =\nHISTORY =\n"+
		"HTCONDORDB_JOB_QUEUE_LOG = "+jobLog+"\nHTCONDORDB_SYNCSTATUS_INTERVAL = 3600\n")); err != nil {
		t.Fatal(err)
	}
	if !svc.Owners().Owned(syncstatus.Table) {
		t.Fatal("syncstatus is written by schedd sync but not owned")
	}

	c := writeSession(t, svc)
	ctx := context.Background()
	tx, err := c.BeginTable(ctx, syncstatus.Table)
	if err != nil {
		t.Fatal(err)
	}
	ferr := tx.NewClassAd(ctx, syncstatus.Key, "HeartbeatSeq = 9223372036854775807\nSpokeLagSeconds = 0\n")
	if ferr == nil {
		ferr = tx.Commit(ctx)
	}
	if ferr == nil {
		t.Error("a WRITE client replaced the heartbeat row")
	}
	if err := c.DropTable(ctx, syncstatus.Table); err == nil {
		t.Error("a WRITE client dropped syncstatus under its writer")
	}
	if _, ok := svc.Catalog().Table(syncstatus.Table); !ok {
		t.Fatal("syncstatus is gone")
	}

	sc := &syncController{sched: m, owners: svc.Owners(), cat: svc.Catalog()}
	resp := sc.handle(ctx, mkReq("truncate", syncstatus.Table))
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
		t.Fatal("truncate syncstatus succeeded")
	}
	if e, _ := resp.EvaluateAttrString("Error"); !strings.Contains(e, "heartbeat") {
		t.Errorf("truncate syncstatus error = %q, want it to say what the table is", e)
	}

	m.Stop()
	if svc.Owners().Owned(syncstatus.Table) {
		t.Error("syncstatus still owned after schedd sync stopped")
	}
}
