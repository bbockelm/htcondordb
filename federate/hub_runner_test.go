package federate

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// testSpoke is an in-process spoke: a persistent catalog served over dbrpc on in-memory pipes, so
// the hub's runners exercise the real Watch protocol (snapshot, Synced, live tail, resume, and a
// Reset when the spoke "restarts" with a new watch epoch).
type testSpoke struct {
	t   *testing.T
	dir string

	mu    sync.Mutex
	cat   *db.Catalog
	srv   *dbrpc.Server
	conns []net.Conn
	wg    sync.WaitGroup
}

func newTestSpoke(t *testing.T) *testSpoke {
	s := &testSpoke{t: t, dir: t.TempDir()}
	s.open()
	t.Cleanup(s.stop)
	return s
}

func (s *testSpoke) open() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cat := openCatalog(s.t, s.dir)
	for _, n := range []string{TableJobs, TableSyncStatus} {
		mustTable(s.t, cat, n)
	}
	if _, err := cat.CreateArchiveTable(TableHistory, db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}}); err != nil {
		s.t.Fatal(err)
	}
	s.cat, s.srv = cat, dbrpc.NewServerCatalog(cat)
}

// stop drops every session and closes the catalog -- a spoke process exiting.
func (s *testSpoke) stop() {
	s.mu.Lock()
	cat, srv, conns := s.cat, s.srv, s.conns
	s.cat, s.srv, s.conns = nil, nil, nil
	s.mu.Unlock()
	if cat == nil {
		return
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	srv.Close()
	_ = cat.Close()
}

// restart is a spoke process restart: its watch epoch is new, so every resuming hub gets a Reset.
func (s *testSpoke) restart() { s.stop(); s.open() }

func (s *testSpoke) dial(context.Context) (*dbrpc.Client, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv == nil {
		return nil, nil, errors.New("spoke down")
	}
	cp, sp := net.Pipe()
	s.conns = append(s.conns, cp, sp)
	srv := s.srv
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = srv.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true})
	}()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return c, func() { _ = c.Close() }, nil
}

func (s *testSpoke) table(name string) *db.DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, _ := s.cat.Table(name)
	return d
}

func (s *testSpoke) putJobs(keys ...int) {
	s.t.Helper()
	tx := s.table(TableJobs).Begin()
	for _, k := range keys {
		tx.NewClassAd(jobKey(k), jobAd(s.t, k, 0, ""))
	}
	if err := tx.Commit(); err != nil {
		s.t.Fatal(err)
	}
}

func (s *testSpoke) delJobs(keys ...int) {
	s.t.Helper()
	tx := s.table(TableJobs).Begin()
	for _, k := range keys {
		tx.DestroyClassAd(jobKey(k))
	}
	if err := tx.Commit(); err != nil {
		s.t.Fatal(err)
	}
}

func (s *testSpoke) appendHistory(schedd string, clusters ...int) {
	s.t.Helper()
	s.mu.Lock()
	a, _ := s.cat.ArchiveTable(TableHistory)
	s.mu.Unlock()
	for _, c := range clusters {
		if err := a.Append(histRecord(s.t, schedd, c)); err != nil {
			s.t.Fatal(err)
		}
	}
}

func jobKeysOf(t *testing.T, cat *db.Catalog, schedd string) map[string]bool {
	t.Helper()
	d, ok := cat.Table(TableJobs)
	if !ok {
		return nil
	}
	seq, err := d.Query(scheddConstraint(schedd))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for ad := range seq {
		k, _ := ad.EvaluateAttrString("Key")
		out[k] = true
	}
	return out
}

func sameKeys(got map[string]bool, want ...int) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		if !got[jobKey(w)] {
			return false
		}
	}
	return true
}

// TestHubOverRealWatch runs the hub against two in-process spokes over the real Watch protocol:
// the same job id on both, a live delete on one, a delete on a spoke while the hub is down
// followed by that spoke restarting (Reset), and a spoke restarting under a running hub.
func TestHubOverRealWatch(t *testing.T) {
	a, b := newTestSpoke(t), newTestSpoke(t)
	a.putJobs(1, 2)
	b.putJobs(1)
	a.appendHistory("ap1", 1, 2)
	b.appendHistory("ap2", 1)

	hubDir := t.TempDir()
	disc := &Discovery{Static: []Spoke{{Schedd: "ap1", Address: "A"}, {Schedd: "ap2", Address: "B"}}}
	dial := func(addr string) cedarsync.Dial {
		if addr == "A" {
			return a.dial
		}
		return b.dial
	}
	metrics := NewMetrics()
	cat := openCatalog(t, hubDir+"/db")
	t.Cleanup(func() { _ = cat.Close() })
	cfg := Config{Catalog: cat, Discovery: disc, Dial: dial, CursorDir: hubDir + "/cursors",
		FlushInterval: 20 * time.Millisecond, Metrics: metrics}
	hub := startHub(t, cfg)
	hist := func(schedd string) int {
		ar, ok := cat.ArchiveTable(TableHistory)
		if !ok {
			return -1
		}
		return countArchive(t, ar, scheddConstraint(schedd))
	}

	waitFor(t, "initial replication", func() bool {
		return sameKeys(jobKeysOf(t, cat, "ap1"), 1, 2) && sameKeys(jobKeysOf(t, cat, "ap2"), 1) &&
			hist("ap1") == 2 && hist("ap2") == 1
	})

	// Live delete of ap1's 1.0 leaves ap2's 1.0.
	a.delJobs(1)
	waitFor(t, "live delete", func() bool { return sameKeys(jobKeysOf(t, cat, "ap1"), 2) })
	if !sameKeys(jobKeysOf(t, cat, "ap2"), 1) {
		t.Fatalf("ap2's 1.0 went with ap1's delete: %v", jobKeysOf(t, cat, "ap2"))
	}
	hub.stop(t)

	// While the hub is down, ap1's 2.0 leaves the queue and 3.0 arrives; then ap1's spoke restarts,
	// so the hub's resume is answered with a Reset and a full replay.
	a.delJobs(2)
	a.putJobs(3)
	a.appendHistory("ap1", 3)
	a.restart()
	resetsBefore := val(metrics.Resets.WithLabelValues(TableJobs))
	hub = startHub(t, cfg)
	waitFor(t, "reconcile after spoke restart", func() bool {
		return sameKeys(jobKeysOf(t, cat, "ap1"), 3) && hist("ap1") == 3
	})
	if val(metrics.Resets.WithLabelValues(TableJobs)) <= resetsBefore {
		t.Error("the restarted spoke's replay was not a Reset; the test did not exercise reconcile")
	}
	if !sameKeys(jobKeysOf(t, cat, "ap2"), 1) || hist("ap2") != 1 {
		t.Fatalf("ap2 changed by ap1's reconcile: jobs %v, history %d", jobKeysOf(t, cat, "ap2"), hist("ap2"))
	}

	// ap2's spoke restarts cleanly under the running hub. Its mutable tables get a new watch epoch,
	// so they replay: a Reset of an unchanged source writes nothing and duplicates nothing. Its
	// history archive keeps its epoch across a clean restart, so the hub resumes it without a Reset.
	jobs, _ := cat.Table(TableJobs)
	wc := countWrites(t, jobs)
	before := cursors(t, hub.hub, "ap2")
	if len(before) != 3 {
		t.Fatalf("ap2 cursors before restart: %v", before)
	}
	histResets := val(metrics.Resets.WithLabelValues(TableHistory))
	b.restart()
	waitFor(t, "ap2's mutable tables replayed to Synced", func() bool {
		after := cursors(t, hub.hub, "ap2")
		return after[TableJobs] != before[TableJobs] && after[TableSyncStatus] != before[TableSyncStatus]
	})
	if after := cursors(t, hub.hub, "ap2"); after[TableHistory] != before[TableHistory] {
		t.Errorf("ap2's history cursor changed across a clean spoke restart: the archive was replayed")
	}
	if n := val(metrics.Resets.WithLabelValues(TableHistory)); n != histResets {
		t.Errorf("history Resets went %v -> %v across a clean spoke restart, want no Reset", histResets, n)
	}
	ups, dels := wc.settle(t)
	if ups != 0 || dels != 0 {
		t.Errorf("replay of unchanged ap2 wrote %d upserts and %d deletes, want 0 and 0", ups, dels)
	}
	if n := hist("ap2"); n != 1 {
		t.Errorf("ap2 history after its restart = %d, want 1", n)
	}
	if n := hist("ap1"); n != 3 {
		t.Errorf("ap1 history = %d, want 3", n)
	}
	hub.stop(t)
}

// cursors reads schedd's committed cursor per table. A cursor changes when a session reaches
// Synced after a spoke restart (new watch epoch), which is how a test sees a replay complete from
// outside the hub.
func cursors(t *testing.T, h *Hub, schedd string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, table := range h.tables {
		b, err := os.ReadFile(filepath.Join(h.cursorDir(schedd), table+".cursor"))
		if err == nil {
			out[table] = string(b)
		}
	}
	return out
}
