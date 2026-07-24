package dbad

import (
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/scheddsync"
)

func TestBuildAd(t *testing.T) {
	now := time.Unix(1_700_000_500, 0)
	in := Input{
		Identity: Identity{
			Name:      "htcondordb@ap40",
			Machine:   "ap40.chtc.wisc.edu",
			MyAddress: "<10.0.0.1:9619>",
			Version:   "$CondorVersion: 25.4.0 $",
			StartTime: time.Unix(1_700_000_000, 0),
		},
		Tables: []TableStat{
			{Name: "jobs", Ads: 1200, LiveBytes: 4096, DeadBytes: 512, Segments: 3},
			{Name: "history", Archive: true, Ads: 98000},
		},
		Capabilities: Capabilities{
			TimeTravelEnabled:            true,
			TimeTravelMaxDistanceSeconds: 86400,
			Encrypted:                    true,
			WatchSupported:               true,
		},
		Sources: []scheddsync.SyncStatus{
			{Kind: "job_queue.log", Source: "/spool/job_queue.log", Offset: 900, FileSize: 900, LagBytes: 0, CaughtUp: true, LastSync: time.Unix(1_700_000_480, 0)},
			{Kind: "history", Source: "/spool/history", Offset: 500, FileSize: 700, LagBytes: 200, CaughtUp: false, LastSync: time.Unix(1_700_000_400, 0), Resyncs: 2, LastResync: time.Unix(1_700_000_300, 0)},
		},
		Now:       now,
		UpdateSeq: 7,
	}
	ad := BuildAd(in)

	str := func(k string) string { v, _ := ad.EvaluateAttrString(k); return v }
	i := func(k string) int64 { v, _ := ad.EvaluateAttrInt(k); return v }
	b := func(k string) bool { v, _ := ad.EvaluateAttrBool(k); return v }

	if str("MyType") != AdType {
		t.Errorf("MyType = %q, want %q", str("MyType"), AdType)
	}
	if str("Name") != "htcondordb@ap40" || str("MyAddress") != "<10.0.0.1:9619>" {
		t.Errorf("identity wrong: Name=%q Addr=%q", str("Name"), str("MyAddress"))
	}
	if i("UpdateSequenceNumber") != 7 || i("DaemonStartTime") != 1_700_000_000 || i("MyCurrentTime") != 1_700_000_500 {
		t.Errorf("timing/seq wrong")
	}

	// Capabilities.
	if !b("TimeTravelEnabled") || i("TimeTravelMaxDistanceSeconds") != 86400 || !b("Encrypted") || !b("WatchSupported") {
		t.Errorf("capabilities wrong")
	}

	// Per-table gauges + totals.
	if i("Table_jobs_Ads") != 1200 || i("Table_jobs_LiveBytes") != 4096 || i("Table_jobs_Segments") != 3 {
		t.Errorf("jobs table gauges wrong")
	}
	if !b("Table_history_Archive") || i("Table_history_Ads") != 98000 {
		t.Errorf("history archive gauges wrong")
	}
	if i("NumTables") != 2 || i("TotalAds") != 1200+98000 || i("TotalLiveBytes") != 4096 {
		t.Errorf("totals wrong: NumTables=%d TotalAds=%d", i("NumTables"), i("TotalAds"))
	}

	// Sync sources.
	if !b("Syncing") {
		t.Error("Syncing should be true")
	}
	if !b("JobQueueCaughtUp") || i("JobQueueLagBytes") != 0 {
		t.Errorf("job_queue sync wrong")
	}
	if i("HistoryLagBytes") != 200 || b("HistoryCaughtUp") {
		t.Errorf("history lag wrong: lag=%d caughtUp=%v", i("HistoryLagBytes"), b("HistoryCaughtUp"))
	}
	if i("HistoryResyncs") != 2 || !b("HistoryGapDetected") || i("HistoryLastResyncTime") != 1_700_000_300 {
		t.Errorf("history resync/gap wrong: resyncs=%d gap=%v", i("HistoryResyncs"), b("HistoryGapDetected"))
	}
	// SecondsSinceSync = Now - LastSync.
	if got := i("HistorySecondsSinceSync"); got != 100 {
		t.Errorf("HistorySecondsSinceSync = %d, want 100", got)
	}
}

func TestBuildAdNoSources(t *testing.T) {
	ad := BuildAd(Input{Identity: Identity{Name: "db"}, Now: time.Unix(1, 0)})
	if v, _ := ad.EvaluateAttrBool("Syncing"); v {
		t.Error("Syncing should be false with no sources")
	}
	if v, _ := ad.EvaluateAttrInt("NumTables"); v != 0 {
		t.Errorf("NumTables = %d, want 0", v)
	}
}
