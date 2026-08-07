package dbad

import (
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/scheddsync"
)

func TestAddAttrs(t *testing.T) {
	now := time.Unix(1_700_000_500, 0)
	in := Input{
		MyAddress: "10.0.0.1:9619",
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
		Now: now,
	}
	// AddAttrs augments a (here empty) base ad -- MyType/UpdateSequenceNumber/identity are the
	// daemon's job (daemon.Advertise/PublishAd), tested in golang-htcondor.
	ad := classad.New()
	AddAttrs(ad, in)

	str := func(k string) string { v, _ := ad.EvaluateAttrString(k); return v }
	i := func(k string) int64 { v, _ := ad.EvaluateAttrInt(k); return v }
	b := func(k string) bool { v, _ := ad.EvaluateAttrBool(k); return v }

	if str("MyAddress") != "<10.0.0.1:9619>" {
		t.Errorf("MyAddress = %q, want <10.0.0.1:9619>", str("MyAddress"))
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

func TestAddAttrsNoSources(t *testing.T) {
	ad := classad.New()
	AddAttrs(ad, Input{Now: time.Unix(1, 0)})
	if v, _ := ad.EvaluateAttrBool("Syncing"); v {
		t.Error("Syncing should be false with no sources")
	}
	if v, _ := ad.EvaluateAttrInt("NumTables"); v != 0 {
		t.Errorf("NumTables = %d, want 0", v)
	}
	if v, _ := ad.EvaluateAttrInt("NumExporters"); v != 0 {
		t.Errorf("NumExporters = %d, want 0", v)
	}
}

// TestAddAttrsImporters: history-import job health is surfaced per-job, with
// SecondsSinceBeat computed against Now and LastError only present when set.
func TestAddAttrsImporters(t *testing.T) {
	now := time.Unix(1_700_000_500, 0)
	in := Input{
		Now: now,
		Importers: []ImporterStatus{
			{Name: "ospool", Running: true, Restarts: 1,
				LastBeat: time.Unix(1_700_000_470, 0), LastCycle: time.Unix(1_700_000_400, 0),
				Schedds: 8, Failures: 1, ImportedTotal: 9123},
			{Name: "cm-east", Running: false, LastErr: "collector unreachable"},
		},
	}
	ad := classad.New()
	AddAttrs(ad, in)

	i := func(k string) int64 { v, _ := ad.EvaluateAttrInt(k); return v }
	b := func(k string) bool { v, _ := ad.EvaluateAttrBool(k); return v }
	str := func(k string) string { v, _ := ad.EvaluateAttrString(k); return v }

	if i("NumImportJobs") != 2 {
		t.Errorf("NumImportJobs = %d, want 2", i("NumImportJobs"))
	}
	if !b("ImportJob_ospool_Running") || i("ImportJob_ospool_Restarts") != 1 ||
		i("ImportJob_ospool_Schedds") != 8 || i("ImportJob_ospool_Failures") != 1 ||
		i("ImportJob_ospool_ImportedTotal") != 9123 {
		t.Errorf("ospool gauges wrong")
	}
	if i("ImportJob_ospool_LastBeatTime") != 1_700_000_470 || i("ImportJob_ospool_SecondsSinceBeat") != 30 {
		t.Errorf("ospool beat wrong: t=%d since=%d", i("ImportJob_ospool_LastBeatTime"), i("ImportJob_ospool_SecondsSinceBeat"))
	}
	if i("ImportJob_ospool_LastCycleTime") != 1_700_000_400 {
		t.Errorf("ospool LastCycleTime = %d, want 1700000400", i("ImportJob_ospool_LastCycleTime"))
	}
	if _, ok := ad.EvaluateAttrString("ImportJob_ospool_LastError"); ok {
		t.Error("healthy import job should have no LastError attr")
	}
	// sanitize maps '-' to '_', so cm-east's attrs use ImportJob_cm_east_*.
	if b("ImportJob_cm_east_Running") {
		t.Error("cm-east should not be running")
	}
	if str("ImportJob_cm_east_LastError") != "collector unreachable" {
		t.Errorf("cm-east error not surfaced: %q", str("ImportJob_cm_east_LastError"))
	}
}

// TestAddAttrsExporters: exporter health is surfaced per-exporter, with SecondsSinceBeat (the
// "is it wedged / falling behind" signal) computed against Now, and LastError only present when set.
func TestAddAttrsExporters(t *testing.T) {
	now := time.Unix(1_700_000_500, 0)
	in := Input{
		Now: now,
		Exporters: []ExporterStatus{
			{Name: "jobs-os", Kind: "opensearch", Running: true, Restarts: 2,
				LastBeat: time.Unix(1_700_000_450, 0), DocsIndexed: 4200, DocsSkipped: 3, InFlight: 12},
			{Name: "jobs-kafka", Kind: "kafka", Running: false, Restarts: 5,
				DocsIndexed: 10, LastErr: "broker unreachable"},
		},
	}
	ad := classad.New()
	AddAttrs(ad, in)

	str := func(k string) string { v, _ := ad.EvaluateAttrString(k); return v }
	i := func(k string) int64 { v, _ := ad.EvaluateAttrInt(k); return v }
	b := func(k string) bool { v, _ := ad.EvaluateAttrBool(k); return v }

	if i("NumExporters") != 2 {
		t.Errorf("NumExporters = %d, want 2", i("NumExporters"))
	}
	// The healthy OpenSearch exporter.
	if str("Exporter_jobs_os_Kind") != "opensearch" || !b("Exporter_jobs_os_Running") {
		t.Errorf("jobs-os kind/running wrong")
	}
	if i("Exporter_jobs_os_Restarts") != 2 || i("Exporter_jobs_os_DocsIndexed") != 4200 ||
		i("Exporter_jobs_os_DocsSkipped") != 3 || i("Exporter_jobs_os_InFlight") != 12 {
		t.Errorf("jobs-os gauges wrong")
	}
	if i("Exporter_jobs_os_LastBeatTime") != 1_700_000_450 || i("Exporter_jobs_os_SecondsSinceBeat") != 50 {
		t.Errorf("jobs-os beat wrong: t=%d since=%d", i("Exporter_jobs_os_LastBeatTime"), i("Exporter_jobs_os_SecondsSinceBeat"))
	}
	if _, ok := ad.EvaluateAttrString("Exporter_jobs_os_LastError"); ok {
		t.Error("healthy exporter should have no LastError attr")
	}
	// The failed Kafka exporter: not running, no beat, error surfaced.
	if b("Exporter_jobs_kafka_Running") || str("Exporter_jobs_kafka_LastError") != "broker unreachable" {
		t.Errorf("jobs-kafka running/error wrong")
	}
	if _, ok := ad.EvaluateAttrInt("Exporter_jobs_kafka_SecondsSinceBeat"); ok {
		t.Error("exporter with no beat should have no SecondsSinceBeat attr")
	}
}
