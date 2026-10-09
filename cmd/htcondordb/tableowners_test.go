package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/repl"
	"github.com/bbockelm/htcondordb/scheddsync"
	"github.com/bbockelm/htcondordb/server"
)

func newOwnerTestService(t *testing.T) *server.Service {
	t.Helper()
	svc, err := server.New(server.Config{Dir: t.TempDir(), Authorize: func(_, _, _ string) bool { return true }, DisableMaintenance: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func ownedSet(o *server.TableOwners, tables ...string) []string {
	var out []string
	for _, t := range tables {
		if o.Owned(t) {
			out = append(out, t)
		}
	}
	return out
}

// TestScheddSyncOwnershipFollowsReconfig: the tables schedd sync writes are owned exactly while
// their tailer is configured -- enabling claims them, dropping a source releases its table, and
// disabling (or stopping) releases everything.
func TestScheddSyncOwnershipFollowsReconfig(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	svc := newOwnerTestService(t)
	dir := t.TempDir()
	jobLog := filepath.Join(dir, "job_queue.log")
	hist := filepath.Join(dir, "history")
	if err := os.WriteFile(jobLog, []byte("101 1.0 Job Machine\n103 1.0 ProcId 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hist, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &scheddSyncManager{parent: ctx, svc: svc, logger: slog.Default()}
	defer m.Stop()

	all := append(slices.Clone(jobQueueTables), "history", "epoch_history", scheddsync.DefaultJobMetricsTable, "syncstatus")
	base := "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = " + dir + "/db\nJOB_EPOCH_HISTORY =\n"

	if err := m.apply(mkSyncCfg(t, base+"HTCONDORDB_JOB_QUEUE_LOG = "+jobLog+"\nHTCONDORDB_HISTORY = "+hist+"\nHTCONDORDB_JOB_METRICS = true\n")); err != nil {
		t.Fatal(err)
	}
	want := append(slices.Clone(jobQueueTables), "history", scheddsync.DefaultJobMetricsTable, "syncstatus")
	if got := ownedSet(svc.Owners(), all...); !slices.Equal(got, want) {
		t.Errorf("enabled: owned %v, want %v", got, want)
	}
	if o := svc.Owners().Owners("history"); !slices.Equal(o, []string{ownerScheddSync}) {
		t.Errorf("history owners = %v, want [%s]", o, ownerScheddSync)
	}

	// Drop the history source (and metrics): their tables are released, the job tables stay.
	if err := m.apply(mkSyncCfg(t, base+"HTCONDORDB_JOB_QUEUE_LOG = "+jobLog+"\nHISTORY =\n")); err != nil {
		t.Fatal(err)
	}
	if got, want := ownedSet(svc.Owners(), all...), append(slices.Clone(jobQueueTables), "syncstatus"); !slices.Equal(got, want) {
		t.Errorf("history dropped: owned %v, want %v", got, want)
	}

	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = false\n")); err != nil {
		t.Fatal(err)
	}
	if got := ownedSet(svc.Owners(), all...); len(got) != 0 {
		t.Errorf("disabled: still owned %v", got)
	}

	// Re-enable, then Stop: Stop releases too.
	if err := m.apply(mkSyncCfg(t, base+"HTCONDORDB_HISTORY = "+hist+"\nJOB_QUEUE_LOG =\n")); err != nil {
		t.Fatal(err)
	}
	if !svc.Owners().Owned("history") {
		t.Error("re-enabled: history not owned")
	}
	m.Stop()
	if svc.Owners().Owned("history") {
		t.Error("stopped: history still owned")
	}
}

// TestCedarSyncOwnershipFollowsReconfig: replication targets are owned while their source is
// configured and released when it is removed.
func TestCedarSyncOwnershipFollowsReconfig(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	owners := server.NewTableOwners()
	m := &cedarSyncManager{parent: context.Background(), cat: cat, logger: discardLogger(), owners: owners}
	defer m.Stop()

	// The address is never reachable; the runners just back off. Ownership does not wait for them.
	two := "HTCONDORDB_REPLICATE_SOURCES = ap1 ap2\n" +
		"HTCONDORDB_REPLICATE_AP1_ADDRESS = <127.0.0.1:1>\nHTCONDORDB_REPLICATE_AP1_TARGET = pool_history\n" +
		"HTCONDORDB_REPLICATE_AP2_ADDRESS = <127.0.0.1:1>\nHTCONDORDB_REPLICATE_AP2_TARGET = pool_history\n"
	if err := m.apply(mkSyncCfg(t, two)); err != nil {
		t.Fatal(err)
	}
	if o := owners.Owners("pool_history"); !slices.Equal(o, []string{ownerReplication}) {
		t.Errorf("owners(pool_history) = %v, want [%s]", o, ownerReplication)
	}
	if got := m.SourcesFor("pool_history"); !slices.Equal(got, []string{"ap1", "ap2"}) {
		t.Errorf("SourcesFor = %v, want [ap1 ap2]", got)
	}
	if err := m.apply(mkSyncCfg(t, "")); err != nil {
		t.Fatal(err)
	}
	if owners.Owned("pool_history") {
		t.Error("replication disabled: pool_history still owned")
	}
}

// TestImporterOwnershipAndTerminalExit: a managed import job's target is owned while the job is
// configured, and a runner exiting with exitTableReadOnly is not relaunched.
func TestImporterOwnershipAndTerminalExit(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "history-import")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	owners := server.NewTableOwners()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newImporterManager(ctx, discardLogger(), "<127.0.0.1:9618>", owners)
	me, err := user.Current()
	if err != nil {
		t.Skip(err)
	}

	cfg := "HTCONDORDB_HISTORY_IMPORT = osg\nHTCONDORDB_HISTORY_IMPORT_OSG_POOL = cm.example.org\n" +
		"HTCONDORDB_HISTORY_IMPORT_OSG_TABLE = osg_history\nHISTORY_IMPORT = " + bin + "\nLOG = " + dir + "\n" +
		"HTCONDORDB_HISTORY_IMPORT_USER = " + me.Username + "\n"
	if err := m.apply(mkSyncCfg(t, cfg)); err != nil {
		t.Fatal(err)
	}
	if o := owners.Owners("osg_history"); !slices.Equal(o, []string{ownerHistoryImport}) {
		t.Errorf("owners(osg_history) = %v, want [%s]", o, ownerHistoryImport)
	}
	if got := m.JobsFor("osg_history"); !slices.Equal(got, []string{"osg"}) {
		t.Errorf("JobsFor = %v, want [osg]", got)
	}

	// The runner exits 3 at once; the supervisor's first backoff is 1s, so a relaunch would
	// show as a second exit well within this window.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ss := m.Statuses(); len(ss) == 1 && ss[0].Restarts > 1 {
			t.Fatalf("runner relaunched after exitTableReadOnly (%d exits)", ss[0].Restarts)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ss := m.Statuses(); len(ss) != 1 || ss[0].Restarts != 1 {
		t.Fatalf("statuses = %+v, want exactly one exit", ss)
	}

	if err := m.apply(mkSyncCfg(t, "")); err != nil {
		t.Fatal(err)
	}
	if owners.Owned("osg_history") {
		t.Error("importer disabled: osg_history still owned")
	}
}

// localSyncControl is a repl.SyncControl that hands the request to sc in process, the way the
// DBSyncControl handler does for the CLI.
func localSyncControl(sc *syncController) func(action, target string, args []string) (string, error) {
	return func(action, target string, args []string) (string, error) {
		req := mkReq(action, target)
		if len(args) > 0 {
			req.InsertAttrString("Args", strings.Join(args, " "))
		}
		resp := sc.handle(context.Background(), req)
		if ok, _ := resp.EvaluateAttrBool("Ok"); !ok {
			msg, _ := resp.EvaluateAttrString("Error")
			return "", fmt.Errorf("%s", msg)
		}
		note, _ := resp.EvaluateAttrString("Note")
		return note, nil
	}
}

// daemonSession runs REPL lines on a DAEMON-level connection to svc, with the session's sync
// control wired to sc (nil: none, as for a client that cannot reach DBSyncControl).
func daemonSession(t *testing.T, svc *server.Service, sc *syncController, lines string) string {
	t.Helper()
	cp, sp := net.Pipe()
	go func() { _ = svc.RPC().ServeConnOpts(dbrpc.NewStreamConn(sp), svc.ServeOptions(server.LevelDaemon, "")) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	defer c.Close()
	cfg := repl.ExecConfig{}
	if sc != nil {
		cfg.SyncControl = localSyncControl(sc)
	}
	var out bytes.Buffer
	if err := repl.Run(context.Background(), repl.NewExecutor(c, cfg), repl.ScanLines(strings.NewReader(lines)), &out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func historyRecord(cluster int) string {
	return fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nProcId = 0\nJobStatus = 4\nCompletionDate = %d\n*** Offset = 0 ClusterId = %d ProcId = 0\n",
		cluster, cluster, 1700000000+cluster, cluster)
}

func waitCount(t *testing.T, a *db.ArchiveTable, want int, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for a.Count() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: archive Count = %d, want %d", what, a.Count(), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTruncateOwnedHistoryThroughScheddSync is the operator's from-scratch history re-sync end to
// end: `.truncate history` on a DAEMON session is refused by the table gate, re-sent through sync
// control to schedd sync, which wipes the archive and re-reads the history file -- so a record
// that is not in the file is gone and the file's records are back. Without sync control the same
// command is refused, saying why.
func TestTruncateOwnedHistoryThroughScheddSync(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	svc := newOwnerTestService(t)
	dir := t.TempDir()
	hist := filepath.Join(dir, "history")
	if err := os.WriteFile(hist, []byte(historyRecord(1)+historyRecord(2)+historyRecord(3)), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &scheddSyncManager{parent: ctx, svc: svc, logger: slog.Default()}
	defer m.Stop()
	if err := m.apply(mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nHTCONDORDB_DIR = "+dir+"/db\nJOB_QUEUE_LOG =\nJOB_EPOCH_HISTORY =\n"+
		"HTCONDORDB_HISTORY = "+hist+"\nHTCONDORDB_SCHEDDSYNC_POLL_MS = 20\n")); err != nil {
		t.Fatal(err)
	}
	arch, ok := svc.Catalog().ArchiveTable("history")
	if !ok {
		t.Fatal("no history archive")
	}
	waitCount(t, arch, 3, "initial sync")

	// A record the history file does not hold: only a real truncate removes it.
	stray := classad.New()
	stray.InsertAttr("ClusterId", 99)
	stray.InsertAttr("ProcId", 0)
	if err := arch.Append(stray); err != nil {
		t.Fatal(err)
	}
	waitCount(t, arch, 4, "stray appended")

	// No sync control: the gate's refusal reaches the operator with the reason.
	out := daemonSession(t, svc, nil, ".truncate history\n")
	if !strings.Contains(out, `read-only table "history"`) || !strings.Contains(out, "maintained by a writer inside the daemon") {
		t.Errorf("raw truncate output = %q, want the read-only refusal and its hint", out)
	}
	if arch.Count() != 4 {
		t.Fatalf("refused truncate changed the archive: Count = %d", arch.Count())
	}

	sc := &syncController{sched: m, owners: svc.Owners(), cat: svc.Catalog()}
	out = daemonSession(t, svc, sc, ".truncate history\n")
	if !strings.Contains(out, "history truncated; schedd sync is re-reading") {
		t.Fatalf(".truncate history output = %q, want it routed through schedd sync", out)
	}
	waitCount(t, arch, 3, "after truncate + re-read")
	seq, err := arch.Query("ClusterId == 99")
	if err != nil {
		t.Fatal(err)
	}
	for range seq {
		t.Fatal("the stray record survived the truncate")
	}

	// .rotate and .retention on the owned archive also go through the daemon.
	out = daemonSession(t, svc, sc, ".rotate history\n.retention history 0 1GB\n")
	if !strings.Contains(out, "rotated: dropped 0 segment(s)") || !strings.Contains(out, "retention set on history") {
		t.Errorf("rotate/retention output = %q", out)
	}
	if r := arch.Retention(); r.MaxBytes != 1_000_000_000 {
		t.Errorf("retention MaxBytes = %d, want 1e9", r.MaxBytes)
	}
}

// TestSyncControlOwnedTableRefusals: truncate of a table no owner can rebuild is refused with
// the reason and the remedy; "owner" explains a table for a refused write.
func TestSyncControlOwnedTableRefusals(t *testing.T) {
	owners := server.NewTableOwners()
	owners.Set(ownerScheddSync, []string{"jobs"})
	owners.Set(ownerReplication, []string{"pool_history"})
	sc := &syncController{sched: &scheddSyncManager{}, owners: owners}
	errOf := func(action, target string) string {
		resp := sc.handle(context.Background(), mkReq(action, target))
		if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
			t.Fatalf("%s %s succeeded, want refused", action, target)
		}
		e, _ := resp.EvaluateAttrString("Error")
		return e
	}
	if e := errOf("truncate", "jobs"); !strings.Contains(e, "mirrored from job_queue.log") || !strings.Contains(e, ".resync jobs") {
		t.Errorf("truncate jobs: %q", e)
	}
	if e := errOf("truncate", "pool_history"); !strings.Contains(e, "replication") || !strings.Contains(e, "HTCONDORDB_REPLICATE_SOURCES") {
		t.Errorf("truncate pool_history: %q", e)
	}
	if e := errOf("truncate", "scratch"); !strings.Contains(e, "not owned") {
		t.Errorf("truncate of an unowned table: %q", e)
	}
	resp := sc.handle(context.Background(), mkReq("owner", "JOBS"))
	if note, _ := resp.EvaluateAttrString("Note"); !strings.Contains(note, "schedd sync") || !strings.Contains(note, ".resync") {
		t.Errorf("owner JOBS = %q", note)
	}
}
