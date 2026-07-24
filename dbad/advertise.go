package dbad

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/cedar/commands"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/htcondordb/scheddsync"
)

// DefaultInterval is the collector update cadence when unset (matches HTCondor's typical
// UPDATE_INTERVAL feel; the collector's own timeout is much longer).
const DefaultInterval = 5 * time.Minute

// StatusSource is anything exposing a live SyncStatus -- a *scheddsync.JobSync or
// *scheddsync.HistorySync.
type StatusSource interface {
	Status() scheddsync.SyncStatus
}

// CatalogTables extracts per-table storage stats (mutable tables + archive tables) from a live
// catalog for the ad. Cheap: it reads maintained counters, not a scan.
func CatalogTables(cat *db.Catalog) []TableStat {
	var out []TableStat
	for _, name := range cat.Tables() {
		t, ok := cat.Table(name)
		if !ok {
			continue
		}
		st := t.Stats()
		out = append(out, TableStat{
			Name:      name,
			Ads:       int64(st.Ads),
			LiveBytes: int64(st.LiveBytes()),
			DeadBytes: int64(st.DeadBytes),
			Segments:  int64(st.Segments),
		})
	}
	for _, name := range cat.ArchiveTables() {
		a, ok := cat.ArchiveTable(name)
		if !ok {
			continue
		}
		out = append(out, TableStat{Name: name, Archive: true, Ads: int64(a.Count())})
	}
	return out
}

// CatalogCapabilities inspects a catalog's mutable tables to report the discoverable feature
// set: time-travel (enabled anywhere, with the widest configured window) and encryption. Watch
// is always supported by an htcondordb catalog.
func CatalogCapabilities(cat *db.Catalog) Capabilities {
	caps := Capabilities{WatchSupported: true}
	for _, name := range cat.Tables() {
		t, ok := cat.Table(name)
		if !ok {
			continue
		}
		if maxDist, _, enabled := t.TimeTravel(); enabled {
			caps.TimeTravelEnabled = true
			if secs := int64(maxDist.Seconds()); secs > caps.TimeTravelMaxDistanceSeconds {
				caps.TimeTravelMaxDistanceSeconds = secs
			}
		}
		if t.EncryptionEnabled() {
			caps.Encrypted = true
		}
	}
	return caps
}

// Advertiser periodically builds the HTCondorDB ad from live state and sends it to a collector.
type Advertiser struct {
	Collector *htcondor.Collector
	Catalog   *db.Catalog
	// PublishBase seeds each ad with the daemon's common attributes (normally
	// (*daemon.Daemon).PublishAd) before dbad augments it. Its Name is used for INVALIDATE.
	PublishBase  func(*classad.ClassAd)
	MyAddress    string // authoritative reachable command address (covers the non-shared-port fallback)
	Name         string // daemon Name, for the INVALIDATE query on shutdown
	Capabilities Capabilities
	Sources      []StatusSource
	Interval     time.Duration
	Logger       *slog.Logger

	seq int64
}

// Run advertises immediately, then every Interval, until ctx is cancelled. On the way out it
// sends a final INVALIDATE so the collector expires the ad promptly instead of waiting for its
// classad timeout.
func (a *Advertiser) Run(ctx context.Context) {
	interval := a.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	if a.Logger == nil {
		a.Logger = slog.Default()
	}
	a.advertiseOnce(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.invalidate()
			return
		case <-ticker.C:
			a.advertiseOnce(ctx)
		}
	}
}

func (a *Advertiser) advertiseOnce(ctx context.Context) {
	a.seq++
	ad := a.build(time.Now())
	if err := a.Collector.Advertise(ctx, ad, nil); err != nil {
		a.Logger.Warn("htcondordb: collector advertise failed", "err", err.Error())
	}
}

// build assembles the current ad; separated from advertiseOnce so a test can inspect it.
func (a *Advertiser) build(now time.Time) *classad.ClassAd {
	srcs := make([]scheddsync.SyncStatus, 0, len(a.Sources))
	for _, s := range a.Sources {
		st := s.Status()
		// Recompute the lag against the LIVE file size (the snapshot's is measured right after a
		// poll drains to EOF, so it is ~0): a stalled syncer whose offset is frozen while the
		// schedd keeps appending then shows a growing LagBytes, not a misleading zero.
		if st.Source != "" {
			if fi, err := os.Stat(st.Source); err == nil {
				st.FileSize = fi.Size()
				st.LagBytes = 0
				if st.FileSize > st.Offset {
					st.LagBytes = st.FileSize - st.Offset
				}
				st.CaughtUp = st.LagBytes == 0
			}
		}
		srcs = append(srcs, st)
	}
	return BuildAd(Input{
		PublishBase:  a.PublishBase,
		MyAddress:    a.MyAddress,
		Tables:       CatalogTables(a.Catalog),
		Capabilities: a.Capabilities,
		Sources:      srcs,
		Now:          now,
		UpdateSeq:    a.seq,
	})
}

// invalidate asks the collector to expire our ad (best effort, on a fresh short context since
// ctx is already cancelled at shutdown).
func (a *Advertiser) invalidate() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ad := classad.New()
	ad.InsertAttrString("MyType", "Query")
	ad.InsertAttrString("TargetType", AdType)
	ad.InsertAttrString("Name", a.Name)
	if err := a.Collector.Advertise(ctx, ad, &htcondor.AdvertiseOptions{Command: commands.INVALIDATE_ADS_GENERIC}); err != nil {
		a.Logger.Warn("htcondordb: collector invalidate failed", "err", err.Error())
	}
}
