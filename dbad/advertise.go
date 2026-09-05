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
func Augment(cat *db.Catalog, sources func() []StatusSource, exporters func() []ExporterStatus, importers func() []ImporterStatus, myAddress string) func(*classad.ClassAd) {
	return func(ad *classad.ClassAd) {
		var exp []ExporterStatus
		if exporters != nil {
			exp = exporters()
		}
		var imp []ImporterStatus
		if importers != nil {
			imp = importers()
		}
		AddAttrs(ad, Input{
			MyAddress:    myAddress,
			Tables:       CatalogTables(cat),
			Capabilities: CatalogCapabilities(cat),
			Sources:      LiveStatuses(sources),
			Exporters:    exp,
			Importers:    imp,
			Now:          time.Now(),
		})
	}
}

// A source counts as caught up when its unconsumed tail is small AND it synced recently -- both
// conditions, so neither a stalled-but-near-EOF syncer nor an actively-catching-up one that is
// still far behind is mislabeled:
//
//   - caughtUpLagTolerance bounds the residual tail. A busy source is appended between the
//     tailer's polls, so its offset trails the live file by the last poll's worth of writes even
//     while it keeps pace; that churn (bytes to low KB, occasionally more on a burst) must still
//     read as caught up. A real backlog (many MB to GB) must not. 1 MiB sits well above per-poll
//     churn and far below any genuine backlog. Requiring a SMALL lag -- not merely a fresh sync --
//     is what makes CaughtUp false during the initial catch-up (LastSync is fresh on every poll
//     while catching up, so freshness alone would read caught-up while still GB behind) and gives
//     the early-advertise trigger a real false->true edge when the mirror reaches the tail.
//   - caughtUpSyncFreshness bounds staleness. A syncer that has stalled stops making progress, so
//     its LastSync goes stale and CaughtUp flips false once this window elapses even if it froze
//     near EOF -- while LagBytes keeps reporting the true, growing backlog throughout. Sized well
//     above the sub-second default poll interval so a healthy syncer never flaps.
const (
	caughtUpLagTolerance  = 1 << 20 // 1 MiB
	caughtUpSyncFreshness = 10 * time.Second
)

// LiveStatuses snapshots each source's status, recomputing the lag against the LIVE file size:
// a syncer's own snapshot measures lag right after a poll drains to EOF, so it reads ~0; a
// stalled syncer whose offset is frozen while the schedd keeps appending must instead show a
// growing LagBytes, not a misleading zero. CaughtUp is true when the residual lag is small AND
// the sync is recent (see caughtUpLagTolerance / caughtUpSyncFreshness): a busy-but-keeping-pace
// queue reads caught up over its churn tail, while a real backlog (large lag) or a stall (stale
// sync) reads behind. Shared by the collector ad and the Prometheus exporter so both report the
// same live-lag numbers.
func LiveStatuses(sources func() []StatusSource) []scheddsync.SyncStatus {
	if sources == nil {
		return nil
	}
	now := time.Now()
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
				st.CaughtUp = st.LagBytes == 0 ||
					(st.LagBytes <= caughtUpLagTolerance &&
						!st.LastSync.IsZero() && now.Sub(st.LastSync) <= caughtUpSyncFreshness)
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
