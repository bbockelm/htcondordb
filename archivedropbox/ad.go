package archivedropbox

import (
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// recordTimeAttrs are tried in order for a record's timestamp: a completed job's CompletionDate is
// the natural "when did this happen", falling back to status/queue times. Best-effort -- a record
// with none yields 0 (the tarball entry then gets a zero mod time and the loss window uses the
// neighboring records' times).
var recordTimeAttrs = []string{"CompletionDate", "EnteredCurrentStatus", "JobFinishedHookDone", "QDate"}

// recordTime extracts a best-effort unix timestamp for an ad. A CompletionDate of 0 (a job that
// never completed) is treated as absent so the fallbacks apply.
func recordTime(ad *classad.ClassAd) int64 {
	for _, a := range recordTimeAttrs {
		if v, ok := ad.EvaluateAttrInt(a); ok && v > 0 {
			return v
		}
	}
	return 0
}

// entryName builds a stable, unique, path-safe tar entry name for a record: a zero-padded index
// (guaranteeing uniqueness and preserving order within the tarball) followed by the sanitized
// GlobalJobId (or the watch key when absent).
func entryName(index int, key string, ad *classad.ClassAd) string {
	id := key
	if gjid, ok := ad.EvaluateAttrString("GlobalJobId"); ok && gjid != "" {
		id = gjid
	}
	return fmt.Sprintf("%06d-%s.classad", index, sanitizeName(id))
}

// sanitizeName makes an identifier safe as a single tar path component: path separators and any
// control/space characters become '_', and an empty result becomes "job".
func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '/' || r == '\\' || r == 0:
			b.WriteByte('_')
		case r < 0x20 || r == 0x7f:
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	out := strings.TrimSpace(b.String())
	if out == "" {
		return "job"
	}
	return out
}

// buildLossReport renders the data-loss ClassAd dropped into the dropbox. start is the record-time
// of the last record successfully exported before the gap; end is the record-time of the oldest
// record still retained (the first record of the re-sync), i.e. the first record AFTER the gap.
// The exact number of lost records is unknowable (they were pruned before we saw them), so it is
// deliberately not reported.
func buildLossReport(exporter, table string, start, end, detected int64) string {
	ad := classad.New()
	ad.InsertAttrString("MyType", "ArchiveDropboxDataLoss")
	ad.InsertAttrString("Exporter", exporter)
	ad.InsertAttrString("Table", table)
	ad.InsertAttr("DetectedTime", detected)
	ad.InsertAttr("EstimatedLossStartTime", start)
	ad.InsertAttr("EstimatedLossEndTime", end)
	if end > start && start > 0 {
		ad.InsertAttr("EstimatedLossSeconds", end-start)
	}
	ad.InsertAttrString("Reason",
		"the watch resume cursor fell out of the archive's retention window; records between the "+
			"last exported record and the oldest still-retained record were pruned before export. "+
			"Export resumed from the oldest retained record.")
	return ad.MarshalOld()
}
