package scheddsync

import (
	"fmt"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// Epoch (per-run-instance) history is the schedd's JOB_EPOCH_HISTORY file: like
// the completed-job history file it is append-only, banner-terminated, and
// rotated, so JobEpochSync reuses HistorySync's whole tailer (offset resume,
// rotation chain, recovery dedup). It differs only in the record identity -- a
// job has many epoch records (one per run attempt, plus a SPAWN and an EPOCH ad),
// so the dedup key is (ClusterId, ProcId, RunInstanceID, EpochAdType), not just
// (ClusterId, ProcId) -- and in the event-time attribute (EpochWriteDate).
const (
	// EpochWriteDateAttr (ATTR_JOB_EPOCH_WRITE_DATE) is the Unix time the schedd
	// wrote the epoch record; monotonic in the file, so it is the record's event time.
	EpochWriteDateAttr = "EpochWriteDate"
	// RunInstanceAttr (ATTR_RUN_INSTANCE_ID) numbers a job's run attempts from 0.
	RunInstanceAttr = "RunInstanceID"
	// EpochAdTypeAttr (ATTR_EPOCH_AD_TYPE) is the record's kind ("SPAWN"/"EPOCH"/...).
	EpochAdTypeAttr = "EpochAdType"
)

// NewJobEpochSync creates a syncer that appends per-run-instance (epoch) records
// from cfg.Filename to archive, keyed so a job's multiple run instances (and a run
// instance's SPAWN vs EPOCH ads) are distinct. The file need not exist yet.
func NewJobEpochSync(archive *db.ArchiveTable, cfg HistorySyncConfig) *HistorySync {
	s := NewHistorySync(archive, cfg)
	s.kind = "job_epoch"
	s.keyConstraint = epochKeyConstraint
	s.eventTime = epochEventTime
	// Re-publish now that the kind is final, so the initial status carries "job_epoch" (NewHistorySync
	// published it as "history"). Synchronous, before the advertise loop, so the intermediate label is
	// never observed.
	s.publishStatus(false)
	return s
}

// epochKeyConstraint identifies one epoch record: the job, its run instance, and
// the ad type (a run instance can emit both a SPAWN and an EPOCH ad).
func epochKeyConstraint(ad *classad.ClassAd) (string, bool) {
	cid, ok1 := ad.EvaluateAttrInt("ClusterId")
	pid, ok2 := ad.EvaluateAttrInt("ProcId")
	run, ok3 := ad.EvaluateAttrInt(RunInstanceAttr)
	if !ok1 || !ok2 || !ok3 {
		return "", false
	}
	typ, _ := ad.EvaluateAttrString(EpochAdTypeAttr)
	return fmt.Sprintf("ClusterId == %d && ProcId == %d && %s == %d && %s == %s",
		cid, pid, RunInstanceAttr, run, EpochAdTypeAttr, classadStringLit(typ)), true
}

// epochEventTime is the write time of the epoch record.
func epochEventTime(ad *classad.ClassAd) (int64, bool) {
	if v, ok := ad.EvaluateAttrInt(EpochWriteDateAttr); ok && v > 0 {
		return v, true
	}
	return 0, false
}

// classadStringLit quotes s as a ClassAd string literal (escaping quotes and
// backslashes), so an ad-type value is safe to splice into the dedup constraint.
func classadStringLit(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			out = append(out, '\\', '"')
		case '\\':
			out = append(out, '\\', '\\')
		default:
			out = append(out, s[i])
		}
	}
	out = append(out, '"')
	return string(out)
}
