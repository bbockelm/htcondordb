package historyimport

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PelicanPlatform/classad/classad"
)

// Epoch (per-run-instance) records differ from completed-job history in two ways
// that the importer must handle: a job has MANY epoch records (one per run
// attempt, distinguished by RunInstanceID), and the records are ordered by
// EpochWriteDate, not by a one-per-job key. So the cursor is the newest
// EpochWriteDate imported plus the set of record keys at that exact second (the
// boundary), and `since` re-scans that boundary second (`EpochWriteDate <
// cursor`) each cycle -- EpochWriteDate is monotonic non-decreasing in the epoch
// file, so nothing older than the cursor is ever written afterwards. Skipping the
// boundary keys already imported makes the import exactly-once across cycles and
// restarts.
const (
	epochWriteDateAttr = "EpochWriteDate" // ATTR_JOB_EPOCH_WRITE_DATE
	runInstanceAttr    = "RunInstanceID"  // ATTR_RUN_INSTANCE_ID
	epochAdTypeAttr    = "EpochAdType"    // ATTR_EPOCH_AD_TYPE: "EPOCH", "SPAWN", ...
)

// epochCursor is the durable resume state for an epoch import of one schedd.
type epochCursor struct {
	T    int64    `json:"t"`    // newest EpochWriteDate imported
	Keys []string `json:"keys"` // record keys at EpochWriteDate == T (already imported)
}

func decodeEpochCursor(s string) epochCursor {
	var c epochCursor
	if s != "" {
		_ = json.Unmarshal([]byte(s), &c)
	}
	return c
}

func encodeEpochCursor(c epochCursor) string {
	b, _ := json.Marshal(c)
	return string(b)
}

// epochKey uniquely identifies one epoch record across schedds: the schedd, the
// job, the run instance, and the ad type (a run instance can have both a SPAWN
// and an EPOCH ad). It is the exactly-once dedup key.
func epochKey(ad *classad.ClassAd, scheddName string) string {
	jk := jobKey(ad) // "cluster.proc"
	run, _ := ad.EvaluateAttrInt(runInstanceAttr)
	typ, _ := ad.EvaluateAttrString(epochAdTypeAttr)
	return fmt.Sprintf("%s#%s#%d#%s", scheddName, jk, run, typ)
}

// importEpochSchedd pulls one schedd's new epoch records and appends them,
// exactly once. It re-scans the boundary second (EpochWriteDate == the stored
// cursor T), skipping the record keys already imported there, and appends
// everything newer; the new cursor is the newest EpochWriteDate seen plus its
// boundary key set.
func (im *Importer) importEpochSchedd(ctx context.Context, j Job, sd ScheddRef) (int, error) {
	prev := decodeEpochCursor(func() string { v, _ := im.Cur.Get(j.Name, sd.Name); return v }())
	prevKeys := make(map[string]bool, len(prev.Keys))
	for _, k := range prev.Keys {
		prevKeys[k] = true
	}

	since := ""
	if prev.T > 0 {
		// Strictly-less so the boundary second is re-scanned and deduped, never missed.
		since = fmt.Sprintf("%s < %d", epochWriteDateAttr, prev.T)
	}

	maxT := prev.T
	boundary := map[string]bool{}
	sawRecord := false
	imported := 0

	err := im.Src.History(ctx, sd, j.Constraint, since, j.MaxPerCycle, func(ad *classad.ClassAd) error {
		wd, ok := ad.EvaluateAttrInt(epochWriteDateAttr)
		if !ok {
			return nil // not an epoch record (no write date): skip
		}
		key := epochKey(ad, sd.Name)
		sawRecord = true

		// Track the boundary: all keys at the newest EpochWriteDate seen.
		switch {
		case wd > maxT:
			maxT = wd
			boundary = map[string]bool{key: true}
		case wd == maxT:
			boundary[key] = true
		}

		// Exactly-once: a record at the previous boundary second that we already
		// imported is skipped; everything else is new.
		if wd == prev.T && prevKeys[key] {
			return nil
		}
		stamp(ad, sd.Name, im.now())
		if aerr := im.W.Append(ctx, j.Table, ad); aerr != nil {
			return fmt.Errorf("append: %w", aerr)
		}
		imported++
		return nil
	})
	if err != nil {
		return imported, err
	}

	// Persist only when the stream returned records at/after the cursor; an empty
	// scan (e.g. rotated-away boundary with no newer records) leaves the cursor be
	// so a later cycle still advances once new records arrive.
	if sawRecord {
		keys := make([]string, 0, len(boundary))
		for k := range boundary {
			keys = append(keys, k)
		}
		if serr := im.Cur.Set(j.Name, sd.Name, encodeEpochCursor(epochCursor{T: maxT, Keys: keys})); serr != nil {
			return imported, fmt.Errorf("persist cursor: %w", serr)
		}
	}
	return imported, nil
}
