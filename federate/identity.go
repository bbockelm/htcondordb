package federate

import (
	"crypto/sha256"
	"encoding/binary"
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
// or parse one: readers select rows by their ScheddName / ClusterId / ProcId attributes. It is
// injective -- the schedd's length leads -- so no (schedd, key) pair can collide with another
// however either is spelled.
func HubKey(schedd, sourceKey string) string {
	return strconv.Itoa(len(schedd)) + ":" + schedd + ":" + sourceKey
}

// recordIdentity is an archive record's dedup identity.
//
//   - history: GlobalJobId. The schedd composes it from its name, the job id and the queue date,
//     so it is unique per job.
//   - epoch_history: GlobalJobId + RunInstanceID + EpochAdType -- a job has a record per run
//     attempt, and a run instance can emit both a SPAWN and an EPOCH ad. A missing EpochAdType
//     reads as "", as scheddsync's own epoch dedup treats it.
//
// An identity is not unique: one run instance writes a CHECKPOINT record per checkpoint and a
// COMMON record per common-files group. The archive sink therefore dedups by count (see
// archiveSink), never by presence alone.
type recordIdentity struct {
	gid string
	run int64
	typ string
}

// identityAttrs are the attributes recordIdentity is built from.
var identityAttrs = []string{"GlobalJobId", "RunInstanceID", "EpochAdType"}

// archiveIdentity extracts a record's identity for table, or ok=false when the record lacks the
// attributes it needs (such a record is appended without a check, and counted).
func archiveIdentity(table string, ad *classad.ClassAd) (recordIdentity, bool) {
	gid, ok := ad.EvaluateAttrString("GlobalJobId")
	if !ok || gid == "" {
		return recordIdentity{}, false
	}
	if table != TableEpochHistory {
		return recordIdentity{gid: gid}, true
	}
	run, ok := ad.EvaluateAttrInt("RunInstanceID")
	if !ok {
		return recordIdentity{}, false
	}
	typ, _ := ad.EvaluateAttrString("EpochAdType")
	return recordIdentity{gid: gid, run: run, typ: typ}, true
}

// identityFromValues is archiveIdentity over a projection of identityAttrs.
func identityFromValues(table string, vals []classad.Value) (recordIdentity, bool) {
	if len(vals) < len(identityAttrs) {
		return recordIdentity{}, false
	}
	gid, err := vals[0].StringValue()
	if err != nil || gid == "" {
		return recordIdentity{}, false
	}
	if table != TableEpochHistory {
		return recordIdentity{gid: gid}, true
	}
	run, err := vals[1].IntValue()
	if err != nil {
		return recordIdentity{}, false
	}
	typ, _ := vals[2].StringValue()
	return recordIdentity{gid: gid, run: run, typ: typ}, true
}

// constraint is the exact probe for id within schedd's rows. Scoping to ScheddName means a record
// forged by one spoke can never suppress another's.
func (id recordIdentity) constraint(table, schedd string) string {
	c := fmt.Sprintf("%s == %s && GlobalJobId == %s", ScheddNameAttr, stringLit(schedd), stringLit(id.gid))
	if table == TableEpochHistory {
		typ := "EpochAdType == " + stringLit(id.typ)
		if id.typ == "" {
			typ = `(EpochAdType is undefined || EpochAdType == "")`
		}
		c += fmt.Sprintf(" && RunInstanceID == %d && %s", id.run, typ)
	}
	return c
}

// digest is a 128-bit hash of the identity, for the in-memory set a large catch-up uses. At a
// million records the chance of any collision is around 1e-27.
func (id recordIdentity) digest() [16]byte {
	h := sha256.New()
	_, _ = io.WriteString(h, strings.ToLower(id.gid)) // ClassAd == on strings ignores case
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(id.run))
	h.Write([]byte{0})
	h.Write(b[:])
	_, _ = io.WriteString(h, strings.ToLower(id.typ))
	var out [16]byte
	copy(out[:], h.Sum(nil))
	return out
}

// scheddConstraint selects one AP's rows.
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
	return table == TableHistory || table == TableEpochHistory
}
