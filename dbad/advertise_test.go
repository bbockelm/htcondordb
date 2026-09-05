package dbad

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/scheddsync"
)

type fakeSource struct{ st scheddsync.SyncStatus }

func (f fakeSource) Status() scheddsync.SyncStatus { return f.st }

func TestCatalogTablesAndCapabilities(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	jobs, err := cat.CreateTable("jobs")
	if err != nil {
		t.Fatal(err)
	}
	tx := jobs.Begin()
	ad := classad.New()
	ad.InsertAttr("ClusterId", 1)
	tx.NewClassAd("1.0", ad)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	hist, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	h := classad.New()
	h.InsertAttr("ClusterId", 2)
	if err := hist.Append(h); err != nil {
		t.Fatal(err)
	}

	tables := CatalogTables(cat)
	var sawJobs, sawHist bool
	for _, ts := range tables {
		switch ts.Name {
		case "jobs":
			sawJobs = true
			if ts.Archive || ts.Ads != 1 {
				t.Errorf("jobs table stat wrong: %+v", ts)
			}
		case "history":
			sawHist = true
			if !ts.Archive || ts.Ads != 1 {
				t.Errorf("history table stat wrong: %+v", ts)
			}
		}
	}
	if !sawJobs || !sawHist {
		t.Fatalf("missing tables in %+v", tables)
	}

	caps := CatalogCapabilities(cat)
	if !caps.WatchSupported {
		t.Error("WatchSupported should be true")
	}
	if caps.TimeTravelEnabled {
		t.Error("TimeTravelEnabled should be false (not configured)")
	}
}

// TestAugment: the Augment callback reads the live catalog + sources and writes the dbad
// attributes onto a base ad. (MyType/UpdateSequenceNumber/identity come from daemon.Advertise.)
func TestAugment(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	if _, err := cat.CreateTable("jobs"); err != nil {
		t.Fatal(err)
	}

	sources := func() []StatusSource {
		return []StatusSource{
			fakeSource{st: scheddsync.SyncStatus{Kind: "job_queue.log", Offset: 10, FileSize: 10, CaughtUp: true, LastSync: time.Unix(1000, 0)}},
			fakeSource{st: scheddsync.SyncStatus{Kind: "history", Offset: 5, FileSize: 20, LagBytes: 15, Resyncs: 1, LastResync: time.Unix(900, 0)}},
		}
	}
	ad := classad.New()
	Augment(cat, sources, nil, nil, "")(ad)

	if v, _ := ad.EvaluateAttrInt("Table_jobs_Ads"); v != 0 {
		t.Errorf("Table_jobs_Ads = %d, want 0 (empty table)", v)
	}
	if v, _ := ad.EvaluateAttrBool("JobQueueCaughtUp"); !v {
		t.Error("JobQueueCaughtUp should be true")
	}
	if v, _ := ad.EvaluateAttrInt("HistoryLagBytes"); v != 15 {
		t.Errorf("HistoryLagBytes = %d, want 15", v)
	}
	if v, _ := ad.EvaluateAttrBool("HistoryGapDetected"); !v {
		t.Error("HistoryGapDetected should be true (Resyncs=1)")
	}
}

// TestAugmentLiveLag: LagBytes is recomputed against the CURRENT file size at ad-build time, so
// a syncer whose offset is frozen while the file keeps growing shows a real, growing lag.
func TestAugmentLiveLag(t *testing.T) {
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	if err := os.WriteFile(histPath, make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	// Offset frozen at 200 while the file is 1000 bytes -> live lag 800.
	sources := func() []StatusSource {
		return []StatusSource{
			fakeSource{st: scheddsync.SyncStatus{Kind: "history", Source: histPath, Offset: 200, FileSize: 200, CaughtUp: true}},
		}
	}
	ad := classad.New()
	Augment(cat, sources, nil, nil, "")(ad)

	if v, _ := ad.EvaluateAttrInt("HistoryFileSize"); v != 1000 {
		t.Errorf("HistoryFileSize = %d, want 1000 (live stat)", v)
	}
	if v, _ := ad.EvaluateAttrInt("HistoryLagBytes"); v != 800 {
		t.Errorf("HistoryLagBytes = %d, want 800 (live lag)", v)
	}
	if v, _ := ad.EvaluateAttrBool("HistoryCaughtUp"); v {
		t.Error("HistoryCaughtUp should be false with live lag")
	}
}

// TestLiveStatusesCaughtUpFreshness: a small residual lag on a busy source still counts as caught
// up while the syncer keeps making progress (fresh LastSync), but a stalled syncer (stale LastSync)
// with the same lag reads as behind. LagBytes reports the true tail in both cases.
func TestLiveStatusesCaughtUpFreshness(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job_queue.log")
	// File is 10776 bytes; a syncer at offset 10000 trails by a 776-byte tail (the busy-queue case).
	if err := os.WriteFile(path, make([]byte, 10776), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()

	sources := func() []StatusSource {
		return []StatusSource{
			// Fresh: synced a moment ago -> the 776-byte tail is expected churn, caught up.
			fakeSource{st: scheddsync.SyncStatus{Kind: "job_queue.log", Source: path, Offset: 10000, LastSync: now.Add(-time.Second)}},
			// Stale: last progress well past the freshness window -> genuinely behind.
			fakeSource{st: scheddsync.SyncStatus{Kind: "history", Source: path, Offset: 10000, LastSync: now.Add(-caughtUpSyncFreshness - time.Minute)}},
		}
	}
	got := LiveStatuses(sources)
	if len(got) != 2 {
		t.Fatalf("got %d statuses, want 2", len(got))
	}
	for _, st := range got {
		if st.LagBytes != 776 {
			t.Errorf("%s LagBytes = %d, want 776", st.Kind, st.LagBytes)
		}
	}
	if !got[0].CaughtUp {
		t.Error("fresh syncer with a 776-byte tail should be CaughtUp")
	}
	if got[1].CaughtUp {
		t.Error("stale syncer with the same tail should NOT be CaughtUp")
	}
}
