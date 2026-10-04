package syncstatus

import (
	"context"
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

// TestBuildAdLag: the per-source lag is the collector ad's SecondsSinceSync quantity measured at
// the row's own time, and SpokeLagSeconds is the worst of them -- absent, not zero, while any
// source has never completed a pass.
func TestBuildAdLag(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)
	ad := BuildAd(Row{
		ScheddName: "ap1.example.org", Seq: 7, Now: now, Interval: 5 * time.Second,
		Sources: []scheddsync.SyncStatus{
			{Kind: "job_queue.log", CaughtUp: true, LastSync: now.Add(-2 * time.Second)},
			{Kind: "history", LagBytes: 300, LastSync: now.Add(-9 * time.Second), Resyncs: 1},
		},
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
		{Kind: "job_queue.log", LastSync: now},
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
		{Kind: "job_queue.log", LastSync: now.Add(-3 * time.Second)},
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
