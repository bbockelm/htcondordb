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

	// Deltas and Fulls are this table's delta-record write split, from db.DeltaStats. They are
	// the POSITIVE CONTROL for the DeltaFallback* counters: those only count writes that could
	// not be stored as a delta, so all of them reading zero is ambiguous -- it looks identical
	// whether every patch write is succeeding as a delta or delta mode is off and none are being
	// attempted. Deltas > 0 distinguishes the two. Both zero means delta mode is off for this
	// table (or nothing has been written).
	Deltas int64
	Fulls  int64
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
	Delta        DeltaStat
	Exporters    []ExporterStatus
	Importers    []ImporterStatus
	Now          time.Time
}

// DeltaStat is the process-wide delta-record write accounting. Observe-only: these say why patch
// writes fell back to whole records, and how many were refused outright.
type DeltaStat struct {
	Removal        int64 // an attribute removal, which a delta cannot express
	Bound          int64 // the chain reached DeltaMax
	NoBase         int64 // no whole record to chain to yet (a create)
	Ineligible     int64 // delta records not in use for this write
	UnreadableBase int64 // REFUSED: key present, current record unreadable
	// UnreadableReasons breaks UnreadableBase down by WHY, keyed by classad's reason names.
	// The total says a refusal happened; only the reason says what to repair.
	UnreadableReasons map[string]int64
	// LastDecodeStage and LastDecodeError sample the most recent decode failure behind a
	// refused chain merge. The counts say how often the bytes would not decode; only the
	// decoder's own message says what it objected to, and a deployment refusing ~90 writes a
	// minute cannot tell a truncated record from an unrecognised format without it.
	LastDecodeStage string
	LastDecodeError string
	// SealedProbesSkipped counts sealed-segment key probes skipped because the segment's key
	// index is not built yet. It is the rate behind delta-index-pending: reads racing the
	// reindex pass.
	SealedProbesSkipped int64
	// The shape of the most recent chain that had no whole record. The reason says which fault;
	// these say how much of the chain the walk did find, which is what separates "the base is
	// one link past a dead segment" from "there is nothing here".
	NoBaseVersions      int
	NoBaseChainBroken   bool
	NoBaseSealedSkipped int
	// StrandedSealedDeltas counts live delta fragments found in SEALED segments at open. Zero on
	// a healthy store: the seal-collapse invariant says a live fragment only ever sits in an
	// active segment, and every collapse pass relies on it. Above zero means those keys are
	// invisible to the collapse and will lose their base to the next compaction -- which is what
	// DeltaUnreadableNoBase reports afterwards, once it is too late to tell which key it was.
	StrandedSealedDeltas int64
	// CompactLiveDeltas counts live delta records a compaction MET -- which the pre-pass collapse
	// is supposed to leave none of. Non-zero means the collapse was overtaken by writers and the
	// shard was sealed with a fragment still in it, which is how a key ends up unreadable. It is
	// the direct confirmation that the race classad#272 fixes was firing here.
	//
	// CompactDeferred counts shards that compaction declined to touch because their active
	// segment still held a live delta. That is the fix working: deferring costs a pass, sealing
	// costs the row. A high rate means writers routinely overtake the pre-pass collapse, and the
	// collapse should move inside the per-shard critical section rather than ahead of it.
	CompactLiveDeltas    int64
	CompactDroppedDeltas int64
	CompactDeferred      int64
}

// maxDecodeErrorLen bounds the sampled decoder message on the ad. It is a diagnostic, not a
// payload, and an unbounded string from a decoder reading damaged bytes does not belong in an
// ad every collector in the pool stores.
const maxDecodeErrorLen = 200

// truncate bounds a diagnostic string, marking it when it had to cut, so a reader can tell a
// short message from a clipped one.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// unreadableReasons are the reason names classad reports, paired with the ad attribute suffix
// each is published under. The list is fixed rather than driven off the map so that every reason
// has an attribute at all times: a reason that shows up only once it is non-zero is, to an
// operator querying it, indistinguishable from one that does not exist.
var unreadableReasons = []struct{ reason, suffix string }{
	{"not-visible", "NotVisible"},
	{"segment-gone", "SegmentGone"},
	{"reassemble", "Reassemble"},
	{"decode", "Decode"},
	// The delta-chain reasons. These replaced a single "delta-chain" count, which on this
	// deployment absorbed 100% of the refusals and so said only that the fault was somewhere
	// in the chain walk. NoBase is the one that means a live delta's whole record is gone;
	// FlagMismatch means a rewrite dropped a record's wire flags. Different bugs.
	{"delta-no-versions", "NoVersions"},
	{"delta-no-base", "NoBase"},
	{"delta-reassemble", "ChainReassemble"},
	{"delta-decompress", "ChainDecompress"},
	{"delta-flag-mismatch", "FlagMismatch"},
	{"delta-base-decode", "ChainBaseDecode"},
	{"delta-patch-decode", "ChainPatchDecode"},
	// The three ways a chain ends up with no whole record. IndexPending is the one that is
	// TRANSIENT -- a sealed segment's key index is built by a reindex pass, not at seal time --
	// so a non-zero count there means writes are being dropped that a retry would have landed.
	{"delta-chain-broken", "ChainBroken"},
	{"delta-index-pending", "IndexPending"},
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
		ad.InsertAttr(p+"Deltas", t.Deltas)
		ad.InsertAttr(p+"Fulls", t.Fulls)
	}
	ad.InsertAttr("NumTables", int64(len(in.Tables)))
	// Delta-record write outcomes, process-wide (not per table: the counters are package-level in
	// classad). Why a patch write had to store a WHOLE record rather than a delta, and -- the one
	// that matters for partial-ad reports -- how many were refused because the key was present but
	// its current record could not be read. A climbing DeltaUnreadableBase means the store is
	// failing to resolve keys it holds; before the refusal existed, each of those became an
	// identity-less row holding only the attributes one transaction touched.
	ad.InsertAttr("DeltaFallbackRemoval", in.Delta.Removal)
	ad.InsertAttr("DeltaFallbackBound", in.Delta.Bound)
	ad.InsertAttr("DeltaFallbackNoBase", in.Delta.NoBase)
	ad.InsertAttr("DeltaFallbackIneligible", in.Delta.Ineligible)
	ad.InsertAttr("DeltaUnreadableBase", in.Delta.UnreadableBase)
	// ... and which failure each refusal was. DeltaUnreadableBase climbing says the store cannot
	// resolve keys it holds; these say whether that is an MVCC/snapshot miss, a reaped segment, a
	// lost columnar payload, an unresolvable delta chain or a decode failure -- five unrelated
	// causes whose repairs have nothing in common.
	for _, r := range unreadableReasons {
		ad.InsertAttr("DeltaUnreadable"+r.suffix, in.Delta.UnreadableReasons[r.reason])
	}
	// ... and the decoder's own last words, which is the one thing a count cannot give.
	ad.InsertAttrString("DeltaLastDecodeStage", in.Delta.LastDecodeStage)
	ad.InsertAttrString("DeltaLastDecodeError", truncate(in.Delta.LastDecodeError, maxDecodeErrorLen))
	ad.InsertAttr("DeltaSealedProbesSkipped", in.Delta.SealedProbesSkipped)
	ad.InsertAttr("DeltaLastNoBaseVersions", int64(in.Delta.NoBaseVersions))
	ad.InsertAttrBool("DeltaLastNoBaseChainBroken", in.Delta.NoBaseChainBroken)
	ad.InsertAttr("DeltaLastNoBaseSealedSkipped", int64(in.Delta.NoBaseSealedSkipped))
	ad.InsertAttr("DeltaStrandedSealedDeltas", in.Delta.StrandedSealedDeltas)
	ad.InsertAttr("DeltaCompactLiveDeltas", in.Delta.CompactLiveDeltas)
	ad.InsertAttr("DeltaCompactDroppedDeltas", in.Delta.CompactDroppedDeltas)
	ad.InsertAttr("DeltaCompactDeferred", in.Delta.CompactDeferred)
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
		ad.InsertAttr(p+"ReconcileLookupMiss", s.ReconcileLookupMiss)
		ad.InsertAttr(p+"Unapplied", s.Unapplied)
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
