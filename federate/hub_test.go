package federate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/cedarsync"
	"github.com/bbockelm/htcondordb/scheddsync"
	"github.com/bbockelm/htcondordb/syncstatus"
)

// fakeDiscovery returns whatever snapshot the test last set.
type fakeDiscovery struct {
	mu    sync.Mutex
	snap  Snapshot
	calls int
}

func (f *fakeDiscovery) set(s Snapshot) { f.mu.Lock(); f.snap = s; f.mu.Unlock() }

func (f *fakeDiscovery) Discover(context.Context) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.snap, nil
}

func (f *fakeDiscovery) Constraint() string { return `Name == "ap1"` }

// ValidateRestored accepts every persisted pairing: these tests restore what they replicated.
func (f *fakeDiscovery) ValidateRestored(context.Context, string, string) (string, bool) {
	return "", true
}

func (f *fakeDiscovery) callCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

func members(schedds ...string) Snapshot {
	s := Snapshot{Matched: map[string]bool{}, Present: map[string]bool{}, Spokes: map[string]Spoke{},
		MatchKnown: true, PresentKnown: true, SpokesKnown: true}
	for _, n := range schedds {
		s.Matched[n], s.Present[n] = true, true
		s.Spokes[n] = Spoke{Schedd: n, Address: "spoke-of-" + n}
	}
	return s
}

// testClock is an injectable, advanceable clock.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// unreachable is a Dial for spokes that never answer.
func unreachable(string) cedarsync.Dial {
	return func(context.Context) (*dbrpc.Client, func(), error) {
		return nil, nil, errors.New("spoke unreachable (test)")
	}
}

type runningHub struct {
	hub    *Hub
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

func startHub(t *testing.T, cfg Config) *runningHub {
	t.Helper()
	if cfg.StateInterval == 0 {
		cfg.StateInterval = 5 * time.Millisecond
	}
	if cfg.DiscoverInterval == 0 {
		cfg.DiscoverInterval = time.Hour // tests trigger discovery with Rediscover
	}
	if cfg.Logger == nil {
		cfg.Logger = discard
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rh := &runningHub{hub: h, cancel: cancel, done: make(chan error, 1)}
	go func() { rh.done <- h.Run(ctx) }()
	// Registered after the catalog's cleanup, so it runs first: a test that fails mid-way must not
	// close the catalog under running replication streams.
	t.Cleanup(func() { _ = rh.halt() })
	return rh
}

func (rh *runningHub) halt() error {
	rh.once.Do(func() { rh.cancel(); rh.err = <-rh.done })
	return rh.err
}

func (rh *runningHub) stop(t *testing.T) {
	t.Helper()
	if err := rh.halt(); err != nil {
		t.Fatalf("hub Run: %v", err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func sourceRow(cat *db.Catalog, schedd string) (*classad.ClassAd, bool) {
	d, ok := cat.Table(TableSources)
	if !ok {
		return nil, false
	}
	return d.LookupClassAd(schedd)
}

func sourceState(cat *db.Catalog, schedd string) string {
	row, ok := sourceRow(cat, schedd)
	if !ok {
		return ""
	}
	s, _ := row.EvaluateAttrString("State")
	return s
}

// seedHubRows writes rows for schedd into the hub's jobs, syncstatus and history tables, as if
// they had been replicated earlier.
func seedHubRows(t *testing.T, cat *db.Catalog, schedd string, jobs int) {
	t.Helper()
	j := mustTable(t, cat, TableJobs)
	tx := j.Begin()
	for i := 1; i <= jobs; i++ {
		ad := jobAd(t, i, 0, "")
		ad.InsertAttrString(ScheddNameAttr, schedd)
		tx.NewClassAd(HubKey(schedd, jobKey(i)), ad)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ss := mustTable(t, cat, TableSyncStatus)
	tx = ss.Begin()
	tx.NewClassAd(HubKey(schedd, "status"), parseAd(t, `ScheddName = "`+schedd+`"; HeartbeatSeq = 1`))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	h, err := cat.CreateArchiveTable(TableHistory, db.ArchiveConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ad := histRecord(t, schedd, 1)
	ad.InsertAttrString(ScheddNameAttr, schedd)
	if err := h.Append(ad); err != nil {
		t.Fatal(err)
	}
}

func scheddRows(t *testing.T, cat *db.Catalog, table, schedd string) int {
	t.Helper()
	d, ok := cat.Table(table)
	if !ok {
		return 0
	}
	return countWhere(t, d, scheddConstraint(schedd))
}

// TestAbsentIsNotRetiredAndLastSeenPersists: a source that vanishes from the collector is absent
// with its rows kept; LastSeen survives a hub restart, so a hub restarting into an empty collector
// does not retire anything early; and the source is retired -- mutable rows deleted, archive rows
// kept -- once it has gone unseen for RetireAfter while the hub was running.
func TestAbsentIsNotRetiredAndLastSeenPersists(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	t0 := clock.now()
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cfg := func(cat *db.Catalog) Config {
		return Config{Catalog: cat, Discovery: disc, Dial: unreachable, Now: clock.now,
			RetireAfter: 7 * 24 * time.Hour, CursorDir: dir + "/cursors"}
	}

	cat := openCatalog(t, dir+"/db")
	seedHubRows(t, cat, "ap1", 3)
	hub := startHub(t, cfg(cat))
	waitFor(t, "ap1 seen", func() bool {
		row, ok := sourceRow(cat, "ap1")
		if !ok {
			return false
		}
		v, _ := row.EvaluateAttrInt("LastSeen")
		return v == t0.Unix()
	})

	// The collector forgets ap1 (a collector restart, say).
	disc.set(members())
	clock.advance(time.Hour)
	hub.hub.Rediscover()
	waitFor(t, "ap1 absent", func() bool { return sourceState(cat, "ap1") == StateAbsent })
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 3 {
		t.Fatalf("absent source lost rows: %d jobs, want 3", n)
	}
	hub.stop(t)
	if err := cat.Close(); err != nil {
		t.Fatal(err)
	}

	// The hub restarts six days later into a collector that still has nothing.
	clock.advance(6 * 24 * time.Hour)
	cat = openCatalog(t, dir+"/db")
	t.Cleanup(func() { _ = cat.Close() })
	hub = startHub(t, cfg(cat))
	waitFor(t, "first discovery after restart", func() bool { return disc.callCount() >= 3 })
	waitFor(t, "ap1 absent after restart", func() bool { return sourceState(cat, "ap1") == StateAbsent })
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 3 {
		t.Fatalf("hub restart into an empty collector deleted rows: %d jobs, want 3 (LastSeen not persisted?)", n)
	}
	row, _ := sourceRow(cat, "ap1")
	if v, _ := row.EvaluateAttrInt("LastSeen"); v != t0.Unix() {
		t.Fatalf("LastSeen after restart = %d, want %d", v, t0.Unix())
	}

	// Past seven days since last seen, but the hub was down for six of them: not yet.
	clock.advance(24*time.Hour + time.Minute)
	time.Sleep(50 * time.Millisecond)
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 3 {
		t.Fatalf("hub downtime counted as unseen time: %d jobs, want 3", n)
	}

	// Seven days unseen while the hub runs: retired.
	clock.advance(6 * 24 * time.Hour)
	waitFor(t, "ap1 retired", func() bool { _, ok := sourceRow(cat, "ap1"); return !ok })
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 0 {
		t.Errorf("retired source kept %d jobs rows", n)
	}
	if n := scheddRows(t, cat, TableSyncStatus, "ap1"); n != 0 {
		t.Errorf("retired source kept %d syncstatus rows", n)
	}
	a, _ := cat.ArchiveTable(TableHistory)
	if n := countArchive(t, a, scheddConstraint("ap1")); n != 1 {
		t.Errorf("retirement touched the archive: %d history rows, want 1 (archives age out)", n)
	}
	if v := val(hub.hub.Metrics().Retired); v != 1 {
		t.Errorf("retired_total = %v, want 1", v)
	}
	hub.stop(t)
}

// TestRetiringWhenConstraintStopsMatching: a schedd still advertising but no longer matching the
// constraint is leaving the set: retiring, its runners stopped, its rows deleted only after the
// delay -- and un-retired if it matches again first.
func TestRetiringWhenConstraintStopsMatching(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1", "ap2"))
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	seedHubRows(t, cat, "ap1", 2)
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable, Now: clock.now, RetireAfter: time.Hour})
	defer hub.stop(t)
	waitFor(t, "ap1 known", func() bool { return sourceState(cat, "ap1") != "" })

	snap := members("ap2")
	snap.Present["ap1"] = true // still advertising, no longer matching
	disc.set(snap)
	hub.hub.Rediscover()
	waitFor(t, "ap1 retiring", func() bool { return sourceState(cat, "ap1") == StateRetiring })
	clock.advance(30 * time.Minute)
	disc.set(members("ap1", "ap2")) // the admin reverts the constraint
	hub.hub.Rediscover()
	waitFor(t, "ap1 back", func() bool { return sourceState(cat, "ap1") == StateStale })
	clock.advance(time.Hour)
	time.Sleep(50 * time.Millisecond)
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 2 {
		t.Fatalf("un-retired source lost rows: %d", n)
	}

	disc.set(snap)
	hub.hub.Rediscover()
	waitFor(t, "ap1 retiring again", func() bool { return sourceState(cat, "ap1") == StateRetiring })
	clock.advance(time.Hour + time.Second)
	waitFor(t, "ap1 deleted", func() bool { _, ok := sourceRow(cat, "ap1"); return !ok })
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 0 {
		t.Errorf("retired source kept %d jobs rows", n)
	}
	if sourceState(cat, "ap2") == "" {
		t.Error("ap2 disappeared")
	}
}

// TestAdminRetire: the .retire command's backend deletes at once and refuses an unknown name.
func TestAdminRetire(t *testing.T) {
	disc := &fakeDiscovery{}
	disc.set(members())
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	seedHubRows(t, cat, "ap1", 2)
	seedHubRows(t, cat, "ap2", 2)
	src := mustTable(t, cat, TableSources)
	for _, n := range []string{"ap1", "ap2"} {
		tx := src.Begin()
		// Seen recently, so the absent source is not due for automatic retirement.
		tx.NewClassAd(n, parseAd(t, `ScheddName = "`+n+`"; State = "absent"; LastSeen = `+itoa(int(time.Now().Unix()))))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable})
	defer hub.stop(t)
	ctx := context.Background()
	if err := hub.hub.Retire(ctx, "nope"); err == nil {
		t.Error("retiring an unknown source succeeded")
	}
	if err := hub.hub.Retire(ctx, "ap1"); err != nil {
		t.Fatal(err)
	}
	if n := scheddRows(t, cat, TableJobs, "ap1"); n != 0 {
		t.Errorf("ap1 jobs after retire = %d", n)
	}
	if n := scheddRows(t, cat, TableJobs, "ap2"); n != 2 {
		t.Errorf("retiring ap1 touched ap2: %d jobs", n)
	}
	if _, ok := sourceRow(cat, "ap1"); ok {
		t.Error("ap1 federation_sources row survived retire")
	}
}

func TestStalenessFormula(t *testing.T) {
	now := time.Unix(1_800_000_100, 0)
	row := parseAd(t, `HubReceivedTime = 1800000090; SpokeLagSeconds = 3; HeartbeatIntervalSeconds = 5`)
	if s, ok := staleness(row, now); !ok || s != 10+3+5 {
		t.Errorf("staleness = %d, %v; want 18", s, ok)
	}
	if _, ok := staleness(parseAd(t, `HubReceivedTime = 1800000090`), now); ok {
		t.Error("staleness known without the spoke's lag")
	}
	if _, ok := staleness(nil, now); ok {
		t.Error("staleness known with no heartbeat")
	}
	// A hub clock behind the receipt stamp (it was stamped by this hub, so only a clock step) does
	// not produce negative staleness.
	if s, _ := staleness(row, time.Unix(1_800_000_000, 0)); s != 8 {
		t.Errorf("staleness with clock behind = %d, want 8", s)
	}
}

// TestStalenessThroughHeartbeats: an idle AP that keeps heartbeating stays fresh however long it
// is quiet; when heartbeats stop, staleness grows with the hub's clock and the source turns stale.
// No heartbeat at all is stale, never fresh.
func TestStalenessThroughHeartbeats(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	ss := mustTable(t, cat, TableSyncStatus)
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable, Now: clock.now, FreshThreshold: 60 * time.Second})
	defer hub.stop(t)

	waitFor(t, "ap1 stale before any heartbeat", func() bool { return sourceState(cat, "ap1") == StateStale })
	if row, _ := sourceRow(cat, "ap1"); row != nil {
		if _, ok := row.EvaluateAttrInt("StalenessSeconds"); ok {
			t.Error("StalenessSeconds published with no heartbeat")
		}
	}

	// What the syncstatus runner would do on each heartbeat.
	sink, err := newTableSink(ss, TableSyncStatus, "ap1", &replicate.MemCursorStore{}, NewMetrics(), clock.now, nil)
	if err != nil {
		t.Fatal(err)
	}
	apply(t, sink, reset(), synced("c"))
	beat := func(seq int) {
		apply(t, sink, upsert("status", parseAd(t, `HeartbeatSeq = `+itoa(seq)+`; HeartbeatTime = `+itoa(seq)+`; SpokeLagSeconds = 2; HeartbeatIntervalSeconds = 5`)))
		if err := sink.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	staleSecs := func() int64 {
		row, ok := sourceRow(cat, "ap1")
		if !ok {
			return -1
		}
		v, _ := row.EvaluateAttrInt("StalenessSeconds")
		return v
	}
	// Ten minutes of an idle queue, heartbeating every 5s.
	for seq := 1; seq <= 120; seq++ {
		beat(seq)
		clock.advance(5 * time.Second)
	}
	waitFor(t, "fresh while heartbeating", func() bool { return sourceState(cat, "ap1") == StateFresh && staleSecs() == 5+2+5 })

	// Heartbeats stop.
	clock.advance(2 * time.Minute)
	waitFor(t, "stale after heartbeats stop", func() bool { return sourceState(cat, "ap1") == StateStale })
	first := staleSecs()
	clock.advance(time.Minute)
	waitFor(t, "staleness grows", func() bool { return staleSecs() == first+60 })
	if s := hub.hub.Summary(); s == nil || s.Stale != 1 || !s.MaxStalenessKnown || s.MaxStaleness != first+60 {
		t.Errorf("summary = %+v", s)
	}
}

// TestCatchingUpSpokeIsStale: a spoke replaying a large backlog heartbeats on time and its tailer
// applies records on every pass, so a now-minus-LastSync lag would read one poll interval and the
// hub would call it fresh. The heartbeat the spoke actually writes (syncstatus.BuildAd) lags from
// the last time it was caught up, and the hub must classify the AP stale while it is behind.
func TestCatchingUpSpokeIsStale(t *testing.T) {
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	ss := mustTable(t, cat, TableSyncStatus)
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: unreachable, Now: clock.now, FreshThreshold: 60 * time.Second})
	defer hub.stop(t)

	sink, err := newTableSink(ss, TableSyncStatus, "ap1", &replicate.MemCursorStore{}, NewMetrics(), clock.now, nil)
	if err != nil {
		t.Fatal(err)
	}
	apply(t, sink, reset(), synced("c"))
	caughtUpAt := clock.now()
	for seq := 1; seq <= 60; seq++ { // five minutes of heartbeats while 10 GB behind
		now := clock.now()
		row := syncstatus.BuildAd(syncstatus.Row{
			ScheddName: "ap1", Seq: int64(seq), Now: now, Interval: 5 * time.Second,
			Sources:    []scheddsync.SyncStatus{{Kind: "job_queue.log", LagBytes: 10 << 30, LastSync: now}},
			CaughtUpAt: map[int]time.Time{0: caughtUpAt},
		})
		apply(t, sink, upsert("status", row))
		if err := sink.Flush(); err != nil {
			t.Fatal(err)
		}
		clock.advance(5 * time.Second)
	}
	waitFor(t, "a catching-up spoke classified stale", func() bool {
		row, ok := sourceRow(cat, "ap1")
		if !ok {
			return false
		}
		st, _ := row.EvaluateAttrString("State")
		secs, _ := row.EvaluateAttrInt("StalenessSeconds")
		// 295s behind at the last heartbeat, 5s since it arrived, plus the 5s interval.
		return st == StateStale && secs == 295+5+5
	})
}
