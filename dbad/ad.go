// Package dbad builds and advertises an htcondordb daemon's discovery/monitoring ClassAd to
// an HTCondor collector. The ad serves two purposes at once, in the HTCondor idiom:
//
//   - discovery: an agent or the htcondor-api MCP finds the database (its dbrpc address, the
//     tables it holds, whether time-travel/watch are available) by querying the collector,
//     instead of a hard-coded endpoint;
//   - monitoring: the ad carries per-table storage gauges and per-source sync health
//     (lag, caught-up, resync/gap events), so the collector doubles as a metrics sink that a
//     Prometheus exporter can scrape even when the daemon's own /metrics endpoint is off.
package dbad

import (
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/scheddsync"
)

// AdType is the ad's MyType. It is not a standard HTCondor daemon type, so the collector routes
// it via UPDATE_AD_GENERIC.
const AdType = "HTCondorDB"

// TableStat is one table's storage footprint.
type TableStat struct {
	Name      string
	Archive   bool // an append-only archive table (e.g. history) vs a mutable table
	Ads       int64
	LiveBytes int64
	DeadBytes int64
	Segments  int64
}

// Capabilities describes optional DB features an agent may want to discover.
type Capabilities struct {
	TimeTravelEnabled            bool
	TimeTravelMaxDistanceSeconds int64
	Encrypted                    bool
	WatchSupported               bool
}

// Input is everything BuildAd needs; it holds no live handles, so BuildAd is pure and testable.
type Input struct {
	// PublishBase, if set, seeds the ad with the daemon's common attributes -- identity, address,
	// version, timing, self-monitoring, and config_fill_ad's <SUBSYS>_ATTRS -- before
	// augmentation. It is normally (*daemon.Daemon).PublishAd; dbad adds only the
	// HTCondorDB-specific attributes on top, rather than re-deriving identity itself.
	PublishBase func(*classad.ClassAd)
	// MyAddress is the daemon's authoritative reachable command address. PublishBase can only
	// know the shared-port sinful; the caller (which has the listener) supplies the address that
	// also covers the non-shared-port fallback, so dbad sets it after PublishBase.
	MyAddress    string
	Tables       []TableStat
	Capabilities Capabilities
	Sources      []scheddsync.SyncStatus
	Now          time.Time
	UpdateSeq    int64
}

// BuildAd assembles the HTCondorDB ClassAd by augmenting the daemon-produced base ad. All
// numeric attributes are chosen so a ClassAd->Prometheus exporter reads them as gauges/counters
// directly.
func BuildAd(in Input) *classad.ClassAd {
	ad := classad.New()
	if in.PublishBase != nil {
		in.PublishBase(ad) // daemon common attrs: Name, Machine, MyAddress, version, MonitorSelf*, <SUBSYS>_ATTRS, ...
	}
	ad.InsertAttrString("MyType", AdType) // subsystem-specific; the daemon base ad does not set MyType
	if in.MyAddress != "" {
		ad.InsertAttrString("MyAddress", ensureAngle(in.MyAddress))
	}
	ad.InsertAttr("UpdateSequenceNumber", in.UpdateSeq)

	// Capabilities.
	ad.InsertAttrBool("TimeTravelEnabled", in.Capabilities.TimeTravelEnabled)
	if in.Capabilities.TimeTravelEnabled {
		ad.InsertAttr("TimeTravelMaxDistanceSeconds", in.Capabilities.TimeTravelMaxDistanceSeconds)
	}
	ad.InsertAttrBool("Encrypted", in.Capabilities.Encrypted)
	ad.InsertAttrBool("WatchSupported", in.Capabilities.WatchSupported)

	// Per-table storage gauges + totals.
	var totalAds, totalLive, totalDead int64
	for _, t := range in.Tables {
		totalAds += t.Ads
		totalLive += t.LiveBytes
		totalDead += t.DeadBytes
		p := "Table_" + sanitize(t.Name) + "_"
		ad.InsertAttr(p+"Ads", t.Ads)
		ad.InsertAttr(p+"LiveBytes", t.LiveBytes)
		ad.InsertAttr(p+"DeadBytes", t.DeadBytes)
		ad.InsertAttr(p+"Segments", t.Segments)
		ad.InsertAttrBool(p+"Archive", t.Archive)
	}
	ad.InsertAttr("NumTables", int64(len(in.Tables)))
	ad.InsertAttr("TotalAds", totalAds)
	ad.InsertAttr("TotalLiveBytes", totalLive)
	ad.InsertAttr("TotalDeadBytes", totalDead)

	// Per-source sync health.
	ad.InsertAttrBool("Syncing", len(in.Sources) > 0)
	for _, s := range in.Sources {
		p := syncPrefix(s.Kind)
		if p == "" {
			continue
		}
		if s.Source != "" {
			ad.InsertAttrString(p+"Source", s.Source)
		}
		ad.InsertAttr(p+"Offset", s.Offset)
		ad.InsertAttr(p+"FileSize", s.FileSize)
		ad.InsertAttr(p+"LagBytes", s.LagBytes)
		ad.InsertAttrBool(p+"CaughtUp", s.CaughtUp)
		if !s.LastSync.IsZero() {
			ad.InsertAttr(p+"LastSyncTime", s.LastSync.Unix())
			secs := int64(in.Now.Sub(s.LastSync).Seconds())
			if secs < 0 {
				secs = 0
			}
			ad.InsertAttr(p+"SecondsSinceSync", secs)
		}
		if s.Kind == "history" {
			ad.InsertAttr(p+"Resyncs", s.Resyncs)
			ad.InsertAttrBool(p+"GapDetected", s.Resyncs > 0)
			if !s.LastResync.IsZero() {
				ad.InsertAttr(p+"LastResyncTime", s.LastResync.Unix())
			}
		}
	}
	return ad
}

// syncPrefix maps a source kind to a stable attribute prefix; "" skips an unknown kind.
func syncPrefix(kind string) string {
	switch kind {
	case "job_queue.log":
		return "JobQueue"
	case "history":
		return "History"
	default:
		return ""
	}
}

// ensureAngle wraps a bare command address in <> if it is not already a sinful string.
func ensureAngle(addr string) string {
	if strings.HasPrefix(addr, "<") {
		return addr
	}
	return "<" + addr + ">"
}

// sanitize turns a table name into a valid ClassAd attribute-name fragment (identifier chars
// only), so per-table gauge attributes are always well-formed.
func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}
