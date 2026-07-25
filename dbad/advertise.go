package dbad

import (
	"os"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/scheddsync"
)

// StatusSource is anything exposing a live SyncStatus -- a *scheddsync.JobSync or
// *scheddsync.HistorySync.
type StatusSource interface {
	Status() scheddsync.SyncStatus
}

// Augment returns the daemon.AdvertiseConfig.Augment callback: on each advertisement it reads
// the live catalog (per-table storage gauges, discoverable capabilities) and sync sources
// (per-source health, with the lag recomputed against the current file size) and writes the
// HTCondorDB-specific attributes onto the daemon-produced base ad. The generic daemon.Advertise
// owns the loop, the base ad (PublishAd), MyType, the sequence number, DAEMON_SHUTDOWN, the
// collector list, and INVALIDATE-on-shutdown -- dbad only supplies the subsystem attributes.
//
// myAddress is the daemon's authoritative reachable command address (covering the non-shared-port
// fallback that PublishAd cannot know). sources is queried each cycle so a set that changes at
// runtime (schedd-sync tailers restarted on reconfigure) is always current.
func Augment(cat *db.Catalog, sources func() []StatusSource, myAddress string) func(*classad.ClassAd) {
	return func(ad *classad.ClassAd) {
		AddAttrs(ad, Input{
			MyAddress:    myAddress,
			Tables:       CatalogTables(cat),
			Capabilities: CatalogCapabilities(cat),
			Sources:      liveStatuses(sources),
			Now:          time.Now(),
		})
	}
}

// liveStatuses snapshots each source's status, recomputing the lag against the LIVE file size:
// a syncer's own snapshot measures lag right after a poll drains to EOF, so it reads ~0; a
// stalled syncer whose offset is frozen while the schedd keeps appending must instead show a
// growing LagBytes, not a misleading zero.
func liveStatuses(sources func() []StatusSource) []scheddsync.SyncStatus {
	if sources == nil {
		return nil
	}
	srcs := sources()
	out := make([]scheddsync.SyncStatus, 0, len(srcs))
	for _, s := range srcs {
		st := s.Status()
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
		out = append(out, st)
	}
	return out
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
