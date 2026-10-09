package syncstatus

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/dbad"
	"github.com/bbockelm/htcondordb/scheddsync"
)

type fixedSource scheddsync.SyncStatus

func (f fixedSource) Status() scheddsync.SyncStatus { return scheddsync.SyncStatus(f) }

func memTable(t *testing.T) *db.DB {
	t.Helper()
	cat, err := db.OpenCatalog("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	tbl, err := cat.CreateTable(Table)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

// TestBuildAdLag: a caught-up source's lag is the collector ad's SecondsSinceSync quantity measured
// at the row's own time; a source behind lags from the last time it was caught up, however
// recently it last applied records; SpokeLagSeconds is the worst of them -- absent, not zero,
// while any source's lag is unknown.
func TestBuildAdLag(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)
	ad := BuildAd(Row{
		ScheddName: "ap1.example.org", Seq: 7, Now: now, Interval: 5 * time.Second,
		Sources: []scheddsync.SyncStatus{
			{Kind: "job_queue.log", CaughtUp: true, LastSync: now.Add(-2 * time.Second)},
			// Behind, applying records (LastSync 1s ago), last caught up 9s ago.
			{Kind: "history", LagBytes: 300, LastSync: now.Add(-1 * time.Second), Resyncs: 1},
		},
		CaughtUpAt: map[int]time.Time{1: now.Add(-9 * time.Second)},
	})
	i := func(k string) int64 { v, _ := ad.EvaluateAttrInt(k); return v }
	b := func(k string) bool { v, _ := ad.EvaluateAttrBool(k); return v }
	if i("JobQueueLagSeconds") != 2 || i("HistoryLagSeconds") != 9 || i(AttrSpokeLagSeconds) != 9 {
		t.Errorf("lags: jq=%d hist=%d spoke=%d", i("JobQueueLagSeconds"), i("HistoryLagSeconds"), i(AttrSpokeLagSeconds))
	}
	if !b("JobQueueCaughtUp") || b("HistoryCaughtUp") || !b("HistoryGapDetected") || i("HistoryLagBytes") != 300 {
		t.Errorf("health attrs wrong: %s", ad)
	}
	if i(AttrHeartbeatSeq) != 7 || i(AttrHeartbeatTime) != now.Unix() || i(AttrHeartbeatInterval) != 5 {
		t.Errorf("heartbeat attrs wrong: %s", ad)
	}

	unknown := BuildAd(Row{Now: now, Interval: time.Second, Sources: []scheddsync.SyncStatus{
		{Kind: "job_queue.log", CaughtUp: true, LastSync: now},
		{Kind: "history"}, // never synced
	}})
	if _, ok := unknown.EvaluateAttrInt(AttrSpokeLagSeconds); ok {
		t.Errorf("SpokeLagSeconds present with an unsynced source: %s", unknown)
	}
	if _, ok := unknown.EvaluateAttrInt("HistoryLagSeconds"); ok {
		t.Errorf("HistoryLagSeconds present before the first pass: %s", unknown)
	}

	// A source whose file does not exist has nothing to lag on: it does not hold the spoke's lag
	// unknown forever (an epoch history the schedd never wrote would otherwise keep the AP stale).
	missing := BuildAd(Row{Now: now, Interval: time.Second, Missing: map[int]bool{1: true}, Sources: []scheddsync.SyncStatus{
		{Kind: "job_queue.log", CaughtUp: true, LastSync: now.Add(-3 * time.Second)},
		{Kind: "job_epoch"},
	}})
	if v, ok := missing.EvaluateAttrInt(AttrSpokeLagSeconds); !ok || v != 3 {
		t.Errorf("SpokeLagSeconds with a missing epoch file = %d (present %v), want 3", v, ok)
	}
	if v, ok := missing.EvaluateAttrBool("EpochFilePresent"); !ok || v {
		t.Errorf("EpochFilePresent = %v (present %v), want false", v, ok)
	}
}

// fakeTicker hands the writer a channel the test fires by hand, so cadence is asserted by count
// rather than by sleeping.
type fakeTicker struct {
	mu       sync.Mutex
	ch       chan time.Time
	interval time.Duration
}

func (f *fakeTicker) new(d time.Duration) (<-chan time.Time, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.interval = d
	return f.ch, func() {}
}

// TestWriterCadence: one row at start, one per tick, the sequence strictly increasing, the clock
// the injected one -- and a restarted writer continues the sequence instead of re-issuing numbers a
// hub has seen.
func TestWriterCadence(t *testing.T) {
	tbl := memTable(t)
	clock := time.Unix(1_700_000_000, 0)
	var mu sync.Mutex
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }

	ft := &fakeTicker{ch: make(chan time.Time)}
	src := fixedSource{Kind: "job_queue.log", CaughtUp: true, LastSync: time.Now()}
	w := &Writer{
		Table: tbl, Interval: 3 * time.Second, Now: now, NewTicker: ft.new,
		Sources:  func() []dbad.StatusSource { return []dbad.StatusSource{src} },
		Mirrored: func() (string, string) { return "ap1.example.org", "<10.0.0.1:9618>" },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()

	waitSeq := func(want int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if row, ok := tbl.LookupClassAd(Key); ok {
				if n, _ := row.EvaluateAttrInt(AttrHeartbeatSeq); n == want {
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		row, _ := tbl.LookupClassAd(Key)
		t.Fatalf("heartbeat seq never reached %d; row: %v", want, row)
	}
	waitSeq(1)
	for i := int64(2); i <= 4; i++ {
		advance(3 * time.Second)
		ft.ch <- time.Time{}
		waitSeq(i)
	}
	cancel()
	<-done
	if ft.interval != 3*time.Second {
		t.Errorf("ticker interval = %v, want 3s", ft.interval)
	}
	row, _ := tbl.LookupClassAd(Key)
	if v, _ := row.EvaluateAttrInt(AttrHeartbeatTime); v != clock.Unix() {
		t.Errorf("HeartbeatTime = %d, want the injected clock %d", v, clock.Unix())
	}
	if v, _ := row.EvaluateAttrString(AttrMirroredScheddAddress); v != "<10.0.0.1:9618>" {
		t.Errorf("MirroredScheddAddress = %q", v)
	}

	// A new writer (a daemon restart) continues from the stored sequence.
	w2 := &Writer{Table: tbl, Now: now, Sources: w.Sources}
	if err := w2.WriteOnce(); err != nil {
		t.Fatal(err)
	}
	row, _ = tbl.LookupClassAd(Key)
	if n, _ := row.EvaluateAttrInt(AttrHeartbeatSeq); n != 5 {
		t.Errorf("restarted writer seq = %d, want 5", n)
	}
}

// liveSource is a StatusSource whose status the test changes between heartbeats.
type liveSource struct {
	mu sync.Mutex
	st scheddsync.SyncStatus
}

func (l *liveSource) Status() scheddsync.SyncStatus { l.mu.Lock(); defer l.mu.Unlock(); return l.st }
func (l *liveSource) set(f func(*scheddsync.SyncStatus)) {
	l.mu.Lock()
	f(&l.st)
	l.mu.Unlock()
}

// TestCatchingUpLagGrows: a tailer working through a large backlog applies records on every pass,
// so its LastSync is always fresh -- yet the mirror is gigabytes behind. Its heartbeat lag must grow
// from the last time it was caught up (here, as judged by the collector ad's CaughtUp against the
// live file), not read as one poll interval. A process that has never seen it caught up reports no
// lag at all.
func TestCatchingUpLagGrows(t *testing.T) {
	tbl := memTable(t)
	path := filepath.Join(t.TempDir(), "job_queue.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil { // 1 MiB, sparse
		t.Fatal(err)
	}
	clock := time.Now() // LiveStatuses judges sync freshness on the real clock
	src := &liveSource{st: scheddsync.SyncStatus{Kind: "job_queue.log", Source: path, Offset: 1 << 20, LastSync: clock}}
	w := &Writer{Table: tbl, Now: func() time.Time { return clock },
		Sources: func() []dbad.StatusSource { return []dbad.StatusSource{src} }}
	lag := func() (int64, bool) {
		t.Helper()
		if err := w.WriteOnce(); err != nil {
			t.Fatal(err)
		}
		row, _ := tbl.LookupClassAd(Key)
		return row.EvaluateAttrInt(AttrSpokeLagSeconds)
	}
	if v, ok := lag(); !ok || v != 0 {
		t.Fatalf("caught-up lag = %d (known %v), want 0", v, ok)
	}

	// The schedd writes 64 MiB the tailer has not read; the tailer keeps applying records, so
	// LastSync is refreshed on every pass.
	if err := f.Truncate(65 << 20); err != nil {
		t.Fatal(err)
	}
	caughtAt := clock
	for step := 1; step <= 3; step++ {
		clock = clock.Add(30 * time.Second)
		src.set(func(st *scheddsync.SyncStatus) { st.Offset += 4 << 20; st.LastSync = clock })
		v, ok := lag()
		if want := int64(clock.Sub(caughtAt).Seconds()); !ok || v != want {
			t.Fatalf("step %d: lag = %d (known %v), want %d -- a backlog read as fresh", step, v, ok, want)
		}
	}
	if row, _ := tbl.LookupClassAd(Key); row != nil {
		if up, _ := row.EvaluateAttrBool("JobQueueCaughtUp"); up {
			t.Error("JobQueueCaughtUp true with a 60 MiB tail")
		}
	}

	// Caught up again: back to the SecondsSinceSync quantity.
	src.set(func(st *scheddsync.SyncStatus) { st.Offset = 65 << 20; st.LastSync = clock })
	if v, ok := lag(); !ok || v != 0 {
		t.Fatalf("lag after catching up = %d (known %v), want 0", v, ok)
	}

	// A fresh process (a spoke restart) that finds the source behind has no lag to report.
	w2 := &Writer{Table: tbl, Now: func() time.Time { return clock },
		Sources: func() []dbad.StatusSource { return []dbad.StatusSource{src} }}
	if err := f.Truncate(129 << 20); err != nil {
		t.Fatal(err)
	}
	if err := w2.WriteOnce(); err != nil {
		t.Fatal(err)
	}
	row, _ := tbl.LookupClassAd(Key)
	if v, ok := row.EvaluateAttrInt(AttrSpokeLagSeconds); ok {
		t.Fatalf("restarted writer reported lag %d for a source it never saw caught up", v)
	}
	_ = f.Close()
}

// TestMissingJobQueueLogLagUnknown: a missing history or epoch file has nothing to be behind on,
// but a missing job_queue.log is a schedd that is gone or a mistyped path -- the mirror cannot say
// how current it is, so the heartbeat states no lag (stale at a hub), never zero (fresh).
func TestMissingJobQueueLogLagUnknown(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ad := BuildAd(Row{Now: now, Interval: 5 * time.Second, Seq: 1,
		Sources: []scheddsync.SyncStatus{{Kind: "job_queue.log", Source: "/wrong/path/job_queue.log"}},
		Missing: map[int]bool{0: true}})
	if lag, ok := ad.EvaluateAttrInt(AttrSpokeLagSeconds); ok {
		t.Errorf("missing job_queue.log: SpokeLagSeconds = %d, want absent", lag)
	}
	if v, ok := ad.EvaluateAttrBool("JobQueue" + SuffixFilePresent); !ok || v {
		t.Errorf("JobQueueFilePresent = %v, %v; want false", v, ok)
	}

	// A missing history file next to a caught-up job queue still leaves the lag known.
	ad = BuildAd(Row{Now: now, Interval: 5 * time.Second, Seq: 1,
		Sources: []scheddsync.SyncStatus{
			{Kind: "job_queue.log", CaughtUp: true, LastSync: now.Add(-2 * time.Second)},
			{Kind: "history", Source: "/no/history"},
		},
		Missing: map[int]bool{1: true}})
	if lag, ok := ad.EvaluateAttrInt(AttrSpokeLagSeconds); !ok || lag != 2 {
		t.Errorf("missing history: SpokeLagSeconds = %d, %v; want 2", lag, ok)
	}
}
