package federate

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// Hub table names.
const (
	TableJobs         = "jobs"
	TableHistory      = "history"
	TableEpochHistory = "epoch_history"
	TableSyncStatus   = "syncstatus"
	TableSources      = "federation_sources"
	// TableJobMetrics is the spokes' job resource-usage sample archive (scheddsync's
	// DefaultJobMetricsTable), on a spoke only when HTCONDORDB_JOB_METRICS is on.
	TableJobMetrics = "job_metrics"
)

// DefaultTables is what a hub federates when HTCONDORDB_FEDERATE_TABLES is unset.
var DefaultTables = []string{TableJobs, TableHistory, TableSyncStatus}

// Attributes the hub writes onto replicated rows.
const (
	// ScheddNameAttr names the access point a row belongs to. The hub OVERWRITES it on every
	// replicated row from the source's validated identity, so a row can never claim another AP.
	ScheddNameAttr = "ScheddName"
	// HubReceivedTimeAttr is stamped on each syncstatus row: the hub's clock when the heartbeat
	// arrived. Staleness is computed from it and the hub's clock only.
	HubReceivedTimeAttr = "HubReceivedTime"
)

// HubKey is the storage key of source key sourceKey from schedd in a hub's mutable tables.
//
// The encoding is internal to this package and may change between releases (which would require
// rebuilding a hub's mutable tables from its spokes). Nothing outside this function may construct
// or parse one: readers select rows by their ScheddName / ClusterId / ProcId attributes.
//
// A schedd's identity is case-insensitive, as schedd names are in HTCondor and as ClassAd ==
// compares ScheddName: the schedd is folded to lower case, so every spelling of one schedd maps
// to one key space (the row's ScheddName keeps the spelling its source uses). Otherwise the key
// is injective -- the folded schedd's length leads -- so two different schedds never collide.
func HubKey(schedd, sourceKey string) string {
	schedd = strings.ToLower(schedd)
	return strconv.Itoa(len(schedd)) + ":" + schedd + ":" + sourceKey
}

// recordIdentity is an archive record's dedup identity: the values of its table's identity parts
// (identitySpecs).
//
//   - history: GlobalJobId. The schedd composes it from its name, the job id and the queue date,
//     so it is unique per job.
//   - epoch_history: GlobalJobId + RunInstanceID + EpochAdType -- a job has a record per run
//     attempt, and a run instance can emit both a SPAWN and an EPOCH ad. A missing EpochAdType
//     reads as "", as scheddsync's own epoch dedup treats it.
//   - job_metrics: Key + RunInstanceID + SampleTime + SampleTrigger + LogSeq + LogOffset. The
//     spoke's sampler names a sample by the job_queue.log position of the commit it came from
//     (LogSeq, LogOffset) and the job (Key, the spoke's always-present "cluster.proc" -- scoped to
//     the AP by ScheddName here); a commit samples each job at most once. The run, SampleTime and
//     trigger add nothing to that, but they are what tells two samples apart on a record from a
//     spoke too old to write LogOffset. Without the position the rest is not unique (ingest-clock
//     samples of one job read in one pass share a SampleTime), and it is no help on its own in a
//     resume's overlap: a sample re-delivered and a new one with the same identity would both be
//     matched against the one the hub holds.
//
// An identity is not unique: one run instance writes a CHECKPOINT epoch record per checkpoint and a
// COMMON record per common-files group, and a spoke whose log was replaced under an unchanged
// sequence number can repeat a job_metrics position. The archive sink therefore dedups by count
// (see archiveSink), never by presence alone.
type recordIdentity struct {
	spec []idPart
	vals []idVal
}

// idPart is one attribute of an identity. An optional string reads as "" when absent; an optional
// integer is matched as absent.
type idPart struct {
	attr     string
	integer  bool
	required bool
}

type idVal struct {
	set bool
	s   string
	i   int64
}

// identitySpecs are the identity parts of each federated archive.
var identitySpecs = map[string][]idPart{
	TableHistory: {{attr: "GlobalJobId", required: true}},
	TableEpochHistory: {
		{attr: "GlobalJobId", required: true},
		{attr: "RunInstanceID", integer: true, required: true},
		{attr: "EpochAdType"},
	},
	TableJobMetrics: {
		{attr: "Key", required: true},
		{attr: "RunInstanceID", integer: true, required: true},
		{attr: "SampleTime", integer: true, required: true},
		{attr: "SampleTrigger"},
		{attr: "LogSeq", integer: true},
		{attr: "LogOffset", integer: true},
	},
}

// identityAttrs are the attributes table's identity is built from, in identitySpecs order.
func identityAttrs(table string) []string {
	spec := identitySpecs[table]
	out := make([]string, len(spec))
	for i, p := range spec {
		out[i] = p.attr
	}
	return out
}

// archiveIdentity extracts a record's identity for table, or ok=false when the record lacks an
// attribute it needs (such a record is appended without a check, and counted).
func archiveIdentity(table string, ad *classad.ClassAd) (recordIdentity, bool) {
	spec := identitySpecs[table]
	id := recordIdentity{spec: spec, vals: make([]idVal, len(spec))}
	for i, p := range spec {
		v := &id.vals[i]
		if p.integer {
			v.i, v.set = ad.EvaluateAttrInt(p.attr)
		} else {
			v.s, v.set = ad.EvaluateAttrString(p.attr)
			v.set = v.set && v.s != ""
		}
		if !v.set && p.required {
			return recordIdentity{}, false
		}
	}
	return id, len(spec) > 0
}

// identityFromValues is archiveIdentity over a projection of identityAttrs(table).
func identityFromValues(table string, vals []classad.Value) (recordIdentity, bool) {
	spec := identitySpecs[table]
	if len(vals) < len(spec) || len(spec) == 0 {
		return recordIdentity{}, false
	}
	id := recordIdentity{spec: spec, vals: make([]idVal, len(spec))}
	for i, p := range spec {
		v := &id.vals[i]
		var err error
		if p.integer {
			v.i, err = vals[i].IntValue()
		} else {
			v.s, err = vals[i].StringValue()
			if err == nil && v.s == "" {
				err = errEmpty
			}
		}
		v.set = err == nil
		if !v.set && p.required {
			return recordIdentity{}, false
		}
	}
	return id, true
}

var errEmpty = errors.New("empty")

// constraint is the exact probe for id within schedd's rows. Scoping to ScheddName means a record
// forged by one spoke can never suppress another's.
func (id recordIdentity) constraint(schedd string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s == %s", ScheddNameAttr, stringLit(schedd))
	for i, p := range id.spec {
		v := id.vals[i]
		switch {
		case p.integer && v.set:
			fmt.Fprintf(&b, " && %s == %d", p.attr, v.i)
		case p.integer:
			fmt.Fprintf(&b, " && %s is undefined", p.attr)
		case v.set:
			fmt.Fprintf(&b, " && %s == %s", p.attr, stringLit(v.s))
		default:
			fmt.Fprintf(&b, ` && (%s is undefined || %s == "")`, p.attr, p.attr)
		}
	}
	return b.String()
}

// digest is a 128-bit hash of the identity, for the in-memory set a large catch-up uses. At a
// million records the chance of any collision is around 1e-27.
func (id recordIdentity) digest() [16]byte {
	h := sha256.New()
	var b [8]byte
	for i, p := range id.spec {
		v := id.vals[i]
		switch {
		case p.integer && v.set:
			binary.LittleEndian.PutUint64(b[:], uint64(v.i))
			h.Write([]byte{1})
			h.Write(b[:])
		case p.integer:
			h.Write([]byte{0})
		default:
			// ClassAd == on strings ignores case. Length-prefixed, so parts cannot run together.
			s := strings.ToLower(v.s)
			binary.LittleEndian.PutUint64(b[:], uint64(len(s)))
			h.Write(b[:])
			_, _ = io.WriteString(h, s)
		}
	}
	var out [16]byte
	copy(out[:], h.Sum(nil))
	return out
}

// scheddConstraint selects one AP's rows. ClassAd == ignores case, matching HubKey's folding.
func scheddConstraint(schedd string) string {
	return ScheddNameAttr + " == " + stringLit(schedd)
}

// stringLit quotes s as a ClassAd string literal.
func stringLit(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// isArchiveTable reports whether a federated table is append-only on the spoke.
func isArchiveTable(table string) bool {
	return table == TableHistory || table == TableEpochHistory || table == TableJobMetrics
}
