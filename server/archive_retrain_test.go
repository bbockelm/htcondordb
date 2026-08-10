package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// Archive dictionary retraining on the maintenance boundary.
//
// An archive is append-only, so its dictionary is the one nobody ever revisits: the mutable side
// retrains on a 15-minute maintenance tick, while a history table kept whatever dictionary it
// happened to acquire early in life. That is also the case where a stale dictionary hurts most,
// because the data drifts and the sample that trained it is the oldest data in the table.

// retrainArchiveFixture builds a service with one archive table holding n ads. A small segment size
// so segments actually seal: an archive whose data is all in the active segment has nothing for the
// sampler to draw from and nothing to reindex.
func retrainArchiveFixture(t *testing.T, n int) *Service {
	t.Helper()
	s, err := New(Config{Dir: t.TempDir(), Authorize: allowAll})
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.cat.CreateArchiveTable("history", db.ArchiveConfig{SegmentSize: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		ad, err := classad.ParseOld(fmt.Sprintf(
			"ClusterId = %d\nProcId = %d\nOwner = \"user%d\"\nCmd = \"/home/user%d/run.sh\"\n"+
				"JobStatus = 4\nRemoteWallClockTime = %d.5", i/10, i%10, i%32, i%32, i%3600))
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Append(ad); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// archiveCount counts every record the archive returns, which also proves the records are still
// decodable -- a retrain that left a segment unreadable would show up here rather than as a
// compression statistic.
func archiveCount(t *testing.T, a *db.ArchiveTable) int {
	t.Helper()
	seq, err := a.Query("true")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

// TestArchiveRetrainRunsOnMaintenance covers the happy path: with the interval elapsed, a
// maintenance pass retrains and the archive ends up with a trained dictionary.
func TestArchiveRetrainRunsOnMaintenance(t *testing.T) {
	s := retrainArchiveFixture(t, 4000)
	defer s.Close()

	a, _ := s.cat.ArchiveTable("history")
	before := a.CodecStats(200)

	// Interval elapsed: seed the clock in the past.
	s.archiveRetrainEvery = time.Hour
	s.archiveRetrainSeed = time.Now().Add(-2 * time.Hour)

	s.maintainArchivesOnce(float64(time.Now().Unix()))

	after := a.CodecStats(200)
	if after.DictBytes == 0 {
		t.Fatalf("no dictionary after maintenance (codec %q, was %q)", after.Codec, before.Codec)
	}
	if after.LastRetrain.IsZero() {
		t.Error("LastRetrain not set; RetrainDict did not run")
	}
	// The archive must still be readable, and hold every record: a retrain that lost data would be
	// far worse than a stale dictionary.
	if count := archiveCount(t, a); count != 4000 {
		t.Errorf("archive holds %d records after retrain, want 4000", count)
	}
	t.Logf("codec %q -> %q, dict %d B", before.Codec, after.Codec, after.DictBytes)
}

// TestArchiveRetrainRespectsInterval pins the gate. Retraining on every pass would register a
// dictionary per pass, each then referenced by the segments written under it.
func TestArchiveRetrainRespectsInterval(t *testing.T) {
	s := retrainArchiveFixture(t, 2000)
	defer s.Close()

	// Interval NOT elapsed: seeded now.
	s.archiveRetrainEvery = time.Hour
	s.archiveRetrainSeed = time.Now()
	s.maintainArchivesOnce(float64(time.Now().Unix()))
	a, _ := s.cat.ArchiveTable("history")
	if got := a.CodecStats(100); !got.LastRetrain.IsZero() {
		t.Error("retrained despite the interval not having elapsed")
	}

	// Elapsed once: retrains, and the clock is claimed so an immediately following pass does not.
	s.archiveRetrainSeed = time.Now().Add(-2 * time.Hour)
	s.maintainArchivesOnce(float64(time.Now().Unix()))
	first := a.CodecStats(100).LastRetrain
	if first.IsZero() {
		t.Fatal("did not retrain once the interval elapsed")
	}
	s.maintainArchivesOnce(float64(time.Now().Unix()))
	if second := a.CodecStats(100).LastRetrain; !second.Equal(first) {
		t.Errorf("retrained twice in a row: %v then %v", first, second)
	}
}

// TestArchiveRetrainDisabled covers the off switch, and that rotation and reindexing still run --
// the retrain is additive to the pass, not a replacement for it.
func TestArchiveRetrainDisabled(t *testing.T) {
	s := retrainArchiveFixture(t, 1000)
	defer s.Close()

	s.archiveRetrainEvery = 0
	s.archiveRetrainSeed = time.Now().Add(-100 * time.Hour)
	s.maintainArchivesOnce(float64(time.Now().Unix()))

	a, _ := s.cat.ArchiveTable("history")
	if got := a.CodecStats(100); !got.LastRetrain.IsZero() {
		t.Error("retrained with the interval disabled")
	}
	if count := archiveCount(t, a); count != 1000 {
		t.Errorf("archive holds %d records, want 1000", count)
	}
}
