package federate

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// replayGate stalls replays mid-way: while armed, each new connection passes the first allow bytes
// from the spoke to the hub and then blocks the hub's reads until open. A large table's replay
// stops part-way, with what arrived committed by the hub's flush ticker; small streams (an empty
// syncstatus or history) finish under the allowance.
type replayGate struct {
	mu      sync.Mutex
	armed   bool
	allow   int
	release chan struct{}
}

func (g *replayGate) arm(allow int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed, g.allow, g.release = true, allow, make(chan struct{})
}

func (g *replayGate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.armed {
		close(g.release)
		g.armed = false
	}
}

func (g *replayGate) wrap(c net.Conn) net.Conn {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed {
		return c
	}
	return &gatedConn{Conn: c, left: g.allow, release: g.release, closed: make(chan struct{})}
}

type gatedConn struct {
	net.Conn
	left      int // read by one goroutine only (the dbrpc client's reader)
	release   <-chan struct{}
	closeOnce sync.Once
	closed    chan struct{}
}

func (c *gatedConn) Read(p []byte) (int, error) {
	if c.left <= 0 {
		select {
		case <-c.release:
		case <-c.closed:
			return 0, net.ErrClosed
		}
		return c.Conn.Read(p)
	}
	if len(p) > c.left {
		p = p[:c.left]
	}
	n, err := c.Conn.Read(p)
	c.left -= n
	return n, err
}

func (c *gatedConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (s *testSpoke) setGate(g *replayGate) { s.mu.Lock(); s.gate = g; s.mu.Unlock() }

// putPaddedJobs writes jobs 1..n, each padded so the table's replay is far larger than a gate's
// allowance.
func (s *testSpoke) putPaddedJobs(n int) {
	s.t.Helper()
	pad := `Pad = "` + strings.Repeat("x", 200) + `"`
	tx := s.table(TableJobs).Begin()
	for k := 1; k <= n; k++ {
		tx.NewClassAd(jobKey(k), jobAd(s.t, k, 0, pad))
	}
	if err := tx.Commit(); err != nil {
		s.t.Fatal(err)
	}
}

// rowsOf counts schedd's rows in a hub table, -1 on error (usable off the test goroutine).
func rowsOf(cat *db.Catalog, table, schedd string) int {
	d, ok := cat.Table(table)
	if !ok {
		return 0
	}
	seq, err := d.Query(scheddConstraint(schedd))
	if err != nil {
		return -1
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

const (
	paddedJobs = 2000
	gateAllow  = 64 << 10
)

// TestSourceRowSyncedIsStable: <Table>Synced is false while that table's replay is part-way (with
// the stream connected and every other table caught up), and the row is rewritten the moment the
// replay reaches Synced -- with the hub clock standing still, so the coarse refresh of volatile
// fields cannot be what wrote it. A spoke that goes away publishes false.
func TestSourceRowSyncedIsStable(t *testing.T) {
	a := newTestSpoke(t)
	a.putPaddedJobs(paddedJobs)
	gate := &replayGate{}
	gate.arm(gateAllow)
	a.setGate(gate)
	t.Cleanup(gate.open)
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: func(string) cedarsync.Dial { return a.dial },
		Now: clock.now, FlushInterval: 20 * time.Millisecond})
	defer hub.stop(t)

	waitFor(t, "a partial jobs replay", func() bool { return rowsOf(cat, TableJobs, "ap1") > 0 })
	hub.statePasses(t, 2)
	row, _ := sourceRow(cat, "ap1")
	if row == nil {
		t.Fatal("no federation_sources row")
	}
	if n := rowsOf(cat, TableJobs, "ap1"); n >= paddedJobs {
		t.Fatalf("the replay was not stalled part-way (%d rows); the test proves nothing", n)
	}
	for attr, want := range map[string]bool{"JobsConnected": true, "JobsSynced": false, "SyncStatusSynced": true, "HistorySynced": true} {
		if got, ok := row.EvaluateAttrBool(attr); !ok || got != want {
			t.Errorf("mid-replay %s = %v (present %v), want %v", attr, got, ok, want)
		}
	}
	if _, ok := row.EvaluateAttrInt("JobsSyncedAt"); ok {
		t.Error("JobsSyncedAt published before the jobs stream ever reached Synced")
	}
	if st := sourceState(cat, "ap1"); st == StateFresh {
		t.Error("a source with a jobs replay part-way is fresh")
	}

	gate.open()
	waitFor(t, "JobsSynced published", func() bool { v, _ := sourceRowAttrBool(cat, "ap1", "JobsSynced"); return v })
	if n := rowsOf(cat, TableJobs, "ap1"); n != paddedJobs {
		t.Errorf("JobsSynced true with %d of %d rows", n, paddedJobs)
	}
	row, _ = sourceRow(cat, "ap1")
	if v, ok := row.EvaluateAttrInt("JobsSyncedAt"); !ok || v <= 0 {
		t.Errorf("JobsSyncedAt = %d (present %v) once Synced", v, ok)
	}

	a.stop()
	waitFor(t, "a disconnected stream publishes not Synced", func() bool {
		c, ok1 := sourceRowAttrBool(cat, "ap1", "JobsConnected")
		s, ok2 := sourceRowAttrBool(cat, "ap1", "JobsSynced")
		return ok1 && ok2 && !c && !s
	})
}

// TestFromScratchReplayNotSyncedUntilComplete: a source retired and rediscovered is replayed from
// scratch into a hub table that holds none of its rows. Throughout -- retirement, rediscovery and
// a replay stalled part-way -- whenever its row says JobsSynced, every one of its jobs is in the
// hub; while the replay is part-way the row says JobsSynced = false and the source is not fresh.
func TestFromScratchReplayNotSyncedUntilComplete(t *testing.T) {
	a := newTestSpoke(t)
	a.putPaddedJobs(paddedJobs)
	gate := &replayGate{}
	a.setGate(gate)
	t.Cleanup(gate.open)
	dir := t.TempDir()
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, dir+"/db")
	t.Cleanup(func() { _ = cat.Close() })
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: func(string) cedarsync.Dial { return a.dial },
		FlushInterval: 20 * time.Millisecond, CursorDir: dir + "/cursors"})
	defer hub.stop(t)
	synced := func() bool { v, _ := sourceRowAttrBool(cat, "ap1", "JobsSynced"); return v }
	waitFor(t, "initial replication", func() bool { return synced() && rowsOf(cat, TableJobs, "ap1") == paddedJobs })

	// A consumer's check, run continuously: read the row, then the table. Rows only arrive during
	// the replay, so a row that says Synced must be followed by the whole table.
	var (
		mu         sync.Mutex
		violations []string
		sawPartial bool
	)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if row, ok := sourceRow(cat, "ap1"); ok {
				s, _ := row.EvaluateAttrBool("JobsSynced")
				n := rowsOf(cat, TableJobs, "ap1")
				mu.Lock()
				if s && n != paddedJobs {
					violations = append(violations, fmt.Sprintf("JobsSynced with %d of %d rows", n, paddedJobs))
				}
				if !s && n > 0 && n < paddedJobs {
					sawPartial = true
				}
				mu.Unlock()
			}
			time.Sleep(time.Millisecond)
		}
	}()

	gate.arm(gateAllow)
	if err := hub.hub.Retire(context.Background(), "ap1"); err != nil {
		t.Fatal(err)
	}
	hub.hub.Rediscover()
	waitFor(t, "a partial replay of the rediscovered source", func() bool {
		_, ok := sourceRow(cat, "ap1")
		return ok && rowsOf(cat, TableJobs, "ap1") > 0
	})
	hub.statePasses(t, 2)
	if n := rowsOf(cat, TableJobs, "ap1"); n >= paddedJobs {
		t.Fatalf("the replay was not stalled part-way (%d rows); the test proves nothing", n)
	}
	if synced() {
		t.Error("JobsSynced true while the from-scratch replay is part-way")
	}
	if st := sourceState(cat, "ap1"); st == StateFresh {
		t.Error("source fresh while its from-scratch jobs replay is part-way")
	}

	gate.open()
	waitFor(t, "the replay completes and is published", synced)
	close(stop)
	<-done
	if n := rowsOf(cat, TableJobs, "ap1"); n != paddedJobs {
		t.Errorf("after the replay: %d of %d rows", n, paddedJobs)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(violations) > 0 {
		t.Errorf("%d reads saw JobsSynced over a partial table, first: %s", len(violations), violations[0])
	}
	if !sawPartial {
		t.Error("the consumer check never saw the partial replay; it proves nothing")
	}
}

// TestNotFreshUntilJobsSynced: a source whose heartbeat is fresh is still stale while its jobs
// stream has not reached Synced, saying so in Reason, and turns fresh when it does.
func TestNotFreshUntilJobsSynced(t *testing.T) {
	a := newTestSpoke(t)
	a.putPaddedJobs(paddedJobs)
	gate := &replayGate{}
	gate.arm(gateAllow)
	a.setGate(gate)
	t.Cleanup(gate.open)
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	ss := mustTable(t, cat, TableSyncStatus)
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: func(string) cedarsync.Dial { return a.dial },
		Now: clock.now, FlushInterval: 20 * time.Millisecond, FreshThreshold: 60 * time.Second})
	defer hub.stop(t)

	waitFor(t, "syncstatus caught up, jobs part-way", func() bool {
		s, _ := sourceRowAttrBool(cat, "ap1", "SyncStatusSynced")
		return s && rowsOf(cat, TableJobs, "ap1") > 0
	})
	// A live heartbeat, as the syncstatus runner applies it: staleness 0 + 2 + 5.
	sink, err := newTableSink(ss, TableSyncStatus, "ap1", &replicate.MemCursorStore{}, NewMetrics(), clock.now, nil)
	if err != nil {
		t.Fatal(err)
	}
	apply(t, sink, reset(), synced("c"))
	apply(t, sink, upsert("status", parseAd(t, `HeartbeatSeq = 1; HeartbeatTime = 1; SpokeLagSeconds = 2; HeartbeatIntervalSeconds = 5`)))
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the heartbeat measured", func() bool {
		sum := hub.hub.Summary()
		return sum != nil && sum.MaxStalenessKnown && sum.MaxStaleness == 7
	})
	hub.statePasses(t, 2)
	row, _ := sourceRow(cat, "ap1")
	st, _ := row.EvaluateAttrString("State")
	reason, _ := row.EvaluateAttrString("Reason")
	if st != StateStale || !strings.Contains(reason, "jobs stream is catching up") {
		t.Errorf("heartbeat fresh, jobs part-way: State %q Reason %q; want stale, jobs catching up", st, reason)
	}

	gate.open()
	waitFor(t, "fresh once jobs is Synced", func() bool { return sourceState(cat, "ap1") == StateFresh })
	if v, _ := sourceRowAttrBool(cat, "ap1", "JobsSynced"); !v {
		t.Error("fresh without JobsSynced")
	}
}

// slowValidator holds restored-address validation until released.
type slowValidator struct {
	*fakeDiscovery
	release chan struct{}
}

func (v *slowValidator) ValidateRestored(ctx context.Context, _, _ string) (string, bool) {
	select {
	case <-v.release:
	case <-ctx.Done():
	}
	return "", true
}

// TestRestoredRowNotSynced: a restarted hub does not leave the previous run's <Table>Synced
// standing while it revalidates restored spokes (which dials them, and can take a while): what it
// vouched for was the previous run's streams.
func TestRestoredRowNotSynced(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	tx := mustTable(t, cat, TableSources).Begin()
	tx.NewClassAd("ap1", parseAd(t, `ScheddName = "ap1"; State = "fresh"; SpokeAddress = "spoke-of-ap1"; JobsConnected = true; JobsSynced = true; LastSeen = `+itoa(int(time.Now().Unix()))))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	v := &slowValidator{fakeDiscovery: &fakeDiscovery{}, release: make(chan struct{})}
	v.set(members("ap1"))
	hub := startHub(t, Config{Catalog: cat, Discovery: v, Dial: unreachable})
	defer hub.stop(t)
	defer close(v.release)
	waitFor(t, "the restored row rewritten before revalidation finishes", func() bool {
		s, ok := sourceRowAttrBool(cat, "ap1", "JobsSynced")
		return ok && !s
	})
	if st := sourceState(cat, "ap1"); st == StateFresh {
		t.Error("restored row still fresh")
	}
}
