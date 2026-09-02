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

// Input is the HTCondorDB-specific state AddAttrs writes onto an ad. It holds no live handles,
// so AddAttrs is pure and testable; the Augment closure fills it from the live catalog/sources.
type Input struct {
	// MyAddress is the daemon's authoritative reachable command address. The generic base ad
	// (daemon.PublishAd) only knows the shared-port sinful; the caller (which has the listener)
	// supplies the address that also covers the non-shared-port fallback.
	MyAddress    string
	Tables       []TableStat
	Capabilities Capabilities
	Sources      []scheddsync.SyncStatus
	Exporters    []ExporterStatus
	Importers    []ImporterStatus
	Now          time.Time
}

// ExporterStatus is one change-data exporter's health as the daemon's exporter manager sees it:
// what the daemon knows (running, restarts) plus the child's last-reported progress. It lets an
// operator spot a stuck or falling-behind exporter from the collector ad.
type ExporterStatus struct {
	Name        string
	Kind        string
	Running     bool
	Restarts    int
	LastBeat    time.Time // the child's last status beat (zero if none seen yet)
	DocsIndexed uint64
	DocsSkipped uint64
	InFlight    int
	LastErr     string
}

// ImporterStatus is one history-import job's health as the daemon's importer
// manager sees it: what the daemon knows (running, restarts) plus the runner's
// last-reported progress. SecondsSinceBeat is the "is it stuck" signal;
// ImportedTotal and the last cycle's Schedds/Failures show whether it is making
// progress across the pool.
type ImporterStatus struct {
	Name          string
	Running       bool
	Restarts      int
	LastBeat      time.Time // the runner's last status beat (zero if none seen yet)
	LastCycle     time.Time // the last completed import cycle (zero if none yet)
	Schedds       int       // schedds imported in the last cycle
	Failures      int       // schedds that errored in the last cycle
	ImportedTotal uint64    // cumulative records imported since the runner started
	LastErr       string
}

// AddAttrs augments a daemon-produced base ad with the HTCondorDB-specific attributes: the
// reachable address, discoverable capabilities, per-table storage gauges, and per-source sync
// health. It does NOT set MyType or UpdateSequenceNumber -- daemon.Advertise owns those. All
// numeric attributes are chosen so a ClassAd->Prometheus exporter reads them as gauges/counters
// directly.
func AddAttrs(ad *classad.ClassAd, in Input) {
	if in.MyAddress != "" {
		ad.InsertAttrString("MyAddress", ensureAngle(in.MyAddress))
	}

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
		// Partial-ad ("orphan") diagnostics (observe-only counters).
		ad.InsertAttr(p+"SetAttrAbsentKey", s.SetAttrAbsentKey)
		ad.InsertAttr(p+"Reconciles", s.Reconciles)
		if !s.LastSync.IsZero() {
			ad.InsertAttr(p+"LastSyncTime", s.LastSync.Unix())
			secs := int64(in.Now.Sub(s.LastSync).Seconds())
			if secs < 0 {
				secs = 0
			}
			ad.InsertAttr(p+"SecondsSinceSync", secs)
		}
		if s.Kind == "history" || s.Kind == "job_epoch" {
			ad.InsertAttr(p+"Resyncs", s.Resyncs)
			ad.InsertAttrBool(p+"GapDetected", s.Resyncs > 0)
			if !s.LastResync.IsZero() {
				ad.InsertAttr(p+"LastResyncTime", s.LastResync.Unix())
			}
		}
	}

	// Per-exporter health (the daemon-managed change-data syncs). SecondsSinceBeat is the key
	// "is it stuck / falling behind" signal.
	ad.InsertAttr("NumExporters", int64(len(in.Exporters)))
	for _, e := range in.Exporters {
		p := "Exporter_" + sanitize(e.Name) + "_"
		ad.InsertAttrString(p+"Kind", e.Kind)
		ad.InsertAttrBool(p+"Running", e.Running)
		ad.InsertAttr(p+"Restarts", int64(e.Restarts))
		ad.InsertAttr(p+"DocsIndexed", int64(e.DocsIndexed))
		ad.InsertAttr(p+"DocsSkipped", int64(e.DocsSkipped))
		ad.InsertAttr(p+"InFlight", int64(e.InFlight))
		if !e.LastBeat.IsZero() {
			ad.InsertAttr(p+"LastBeatTime", e.LastBeat.Unix())
			secs := int64(in.Now.Sub(e.LastBeat).Seconds())
			if secs < 0 {
				secs = 0
			}
			ad.InsertAttr(p+"SecondsSinceBeat", secs)
		}
		if e.LastErr != "" {
			ad.InsertAttrString(p+"LastError", e.LastErr)
		}
	}

	// Per-import-job health (the daemon-managed remote-history importers).
	ad.InsertAttr("NumImportJobs", int64(len(in.Importers)))
	for _, im := range in.Importers {
		p := "ImportJob_" + sanitize(im.Name) + "_"
		ad.InsertAttrBool(p+"Running", im.Running)
		ad.InsertAttr(p+"Restarts", int64(im.Restarts))
		ad.InsertAttr(p+"Schedds", int64(im.Schedds))
		ad.InsertAttr(p+"Failures", int64(im.Failures))
		ad.InsertAttr(p+"ImportedTotal", int64(im.ImportedTotal))
		if !im.LastBeat.IsZero() {
			ad.InsertAttr(p+"LastBeatTime", im.LastBeat.Unix())
			secs := int64(in.Now.Sub(im.LastBeat).Seconds())
			if secs < 0 {
				secs = 0
			}
			ad.InsertAttr(p+"SecondsSinceBeat", secs)
		}
		if !im.LastCycle.IsZero() {
			ad.InsertAttr(p+"LastCycleTime", im.LastCycle.Unix())
		}
		if im.LastErr != "" {
			ad.InsertAttrString(p+"LastError", im.LastErr)
		}
	}
}

// syncPrefix maps a source kind to a stable attribute prefix; "" skips an unknown kind.
func syncPrefix(kind string) string {
	switch kind {
	case "job_queue.log":
		return "JobQueue"
	case "history":
		return "History"
	case "job_epoch":
		return "Epoch"
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
