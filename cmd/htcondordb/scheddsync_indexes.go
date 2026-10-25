package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/scheddsync"
)

// tableIndexSet is a schedd-sync table's built-in index set: the indexes the table needs for the
// reads it is known to serve, applied when the table is created and reconciled onto it at every
// start, independent of demand-driven auto-indexing (which still adds to it).
//
// The point of having one is that demand is the wrong mechanism for an index everybody needs.
// The auto-tuner adds an archive index only after a sustained read pattern, and an index added
// to an existing archive is carried back only as far as the backfill horizon -- so on a mature
// history table the indexes every list page and dashboard depend on covered only the segments
// sealed after the tuner got around to them, and the rest were scanned in full.
//
// Each entry costs sidecar bytes per record and ingest time, so the sets are small and every
// attribute is justified where it is listed. Measured on 200k synthetic history records (~540 B
// each as stored): a small-integer index (ProcId, JobStatus) adds 4-6 B per record and a timestamp
// index (QDate, CompletionDate) about 32 B, against ~12 B for the ClusterId + Owner pair the table
// always had -- 86 B per record for the whole history set, on disk and page cache rather than
// heap. Appending and indexing the 200k took ~15% longer with it (median 5.9s against 5.1s).
type tableIndexSet struct {
	categorical []string
	value       []string
}

var (
	// historyIndexes serve the history reads the web API, condor_history and the dashboards issue.
	//
	//   - Owner (categorical): the scope every non-admin read is ANDed with.
	//   - ClusterId (value): the per-job page's point lookup and the archive pager's keyset cursor.
	//   - ProcId (value): the second half of that point lookup. ClusterId alone narrows to a
	//     cluster, which for a large submission is many thousands of records; a small integer,
	//     so cheap.
	//   - JobStatus (value): history holds two states, completed (4) and removed (3). `== 4` is
	//     most of the table and the planner rightly scans for it; `== 3`, the removed jobs, is the
	//     selective side, and the index is the cheapest in the set (two values).
	//   - CompletionDate (value): the recent-completions list and the goodput panel filter on it.
	//     It is zone-mapped, but a removed job's CompletionDate is 0, so every segment's zone
	//     minimum is 0 and an upper bound never prunes; a window in the past reads every segment
	//     newer than it without this index.
	//   - QDate (value): submit time has no relation to the order jobs reach history (a job can
	//     sit queued for weeks), so a zone map prunes nothing on it.
	//
	// EnteredHistoryTime is deliberately NOT here although history is filtered on it constantly:
	// htcondordb stamps it at ingest, so it increases with append order and its zone map already
	// resolves any range to exactly the segments holding it. A value index would add ~32 B per
	// record to narrow within the one or two boundary segments.
	historyIndexes = tableIndexSet{
		categorical: []string{"Owner"},
		value:       []string{"ClusterId", "ProcId", "JobStatus", "CompletionDate", "QDate"},
	}

	// epochHistoryIndexes: an epoch record is one run instance of a job, read per job (all the
	// runs of ClusterId.ProcId) or per owner. Its time filters are on EpochWriteDate and
	// EnteredHistoryTime, both written in append order and zone-mapped (see launch).
	epochHistoryIndexes = tableIndexSet{
		categorical: []string{"Owner"},
		value:       []string{"ClusterId", "ProcId"},
	}

	// jobMetricsIndexes: see scheddsync.JobMetricsValueAttrs.
	jobMetricsIndexes = tableIndexSet{
		categorical: scheddsync.JobMetricsCategoricalAttrs,
		value:       scheddsync.JobMetricsValueAttrs,
	}

	// jobsIndexes serve the live queue: the job list, the status tiles (GROUP BY JobStatus,
	// re-read every 30s), the "changed in the last hour" lists and the held-jobs views.
	//
	//   - Owner (categorical): the scope of every non-admin read.
	//   - ClusterId (value): a cluster's jobs, and member(ClusterId, {...}) batch lookups.
	//   - JobStatus (value): every status tile drill-down and the held/completed filters.
	//   - EnteredCurrentStatus (value): the recent-activity and issues reads all bound it.
	//
	// The other recent-activity bounds (QDate, JobCurrentStartDate, CompletionDate) and the rarer
	// equality filters (HoldReasonCode, always ANDed with JobStatus == 5; DAGManJobId) are left to
	// the auto-tuner. On a mutable table that is enough: its threshold is low and, unlike an
	// archive, an index it adds covers every record.
	jobsIndexes = tableIndexSet{
		categorical: []string{"Owner"},
		value:       []string{"ClusterId", "JobStatus", "EnteredCurrentStatus"},
	}
)

// with returns the effective index set: the built-in one plus the configured attributes. The
// configured lists are ADDITIONS, not replacements, for two reasons. The reconciliation that
// applies them to an existing table only ever adds, so a replacement could take effect on a new
// table and never on the one that exists -- a knob whose meaning depends on the table's age. And
// a site that set a knob to add one attribute would otherwise silently lose every built-in index
// it did not think to repeat.
//
// An attribute may carry one kind of index, not both. When the configuration names an attribute
// with the other kind than the built-in set does, the configuration wins.
func (d tableIndexSet) with(cfgCat, cfgVal []string) (categorical, value []string) {
	has := func(list []string, a string) bool {
		return slices.ContainsFunc(list, func(b string) bool { return strings.EqualFold(a, b) })
	}
	add := func(dst []string, src ...string) []string {
		for _, a := range src {
			if !has(dst, a) {
				dst = append(dst, a)
			}
		}
		return dst
	}
	for _, a := range d.categorical {
		if !has(cfgVal, a) || has(cfgCat, a) {
			categorical = add(categorical, a)
		}
	}
	categorical = add(categorical, cfgCat...)
	for _, a := range d.value {
		if !has(cfgCat, a) || has(cfgVal, a) {
			value = add(value, a)
		}
	}
	value = add(value, cfgVal...)
	return categorical, value
}

// The effective index set of each schedd-sync table under settings s: its built-in set plus the
// attributes the configuration adds. One place, so the table's creation, the reconciliation onto
// an existing table, and the tests all see the same lists.
func (s scheddSyncSettings) historyIndexSet() (categorical, value []string) {
	return historyIndexes.with(splitAttrList(s.archiveCatAttrs), splitAttrList(s.archiveValAttrs))
}

func (s scheddSyncSettings) epochHistoryIndexSet() (categorical, value []string) {
	return epochHistoryIndexes.with(splitAttrList(s.archiveCatAttrs), splitAttrList(s.archiveValAttrs))
}

func (s scheddSyncSettings) jobMetricsIndexSet() (categorical, value []string) {
	return jobMetricsIndexes.with(splitAttrList(s.metricsCatAttrs), nil)
}

// jobsIndexSet has no configured additions: there is no knob for the live queue's indexes, and
// the auto-tuner covers that table completely (see jobsIndexes).
func (s scheddSyncSettings) jobsIndexSet() (categorical, value []string) {
	return jobsIndexes.with(nil, nil)
}

// indexAdditions is what AddIndex must be given to bring a table holding (haveCat, haveVal) up to
// (wantCat, wantVal).
//
// An attribute already indexed under either kind is left as it is -- an index created by hand is
// never redefined -- with one exception: one the auto-tuner created (named in auto) is re-added
// under its CURRENT kind, which records it as configured. That matters because provenance decides
// what the auto-tuner's memory-budget trimmer may drop, and an index this daemon depends on must
// not be dropped as an experiment that stopped paying. The re-add rebuilds nothing.
func indexAdditions(haveCat, haveVal, auto, wantCat, wantVal []string) (addCat, addVal []string) {
	has := func(list []string, a string) bool {
		return slices.ContainsFunc(list, func(b string) bool { return strings.EqualFold(a, b) })
	}
	consider := func(a string, wantCategorical bool) {
		if has(addCat, a) || has(addVal, a) {
			return
		}
		switch {
		case has(haveCat, a):
			if has(auto, a) {
				addCat = append(addCat, a)
			}
		case has(haveVal, a):
			if has(auto, a) {
				addVal = append(addVal, a)
			}
		case wantCategorical:
			addCat = append(addCat, a)
		default:
			addVal = append(addVal, a)
		}
	}
	for _, a := range wantCat {
		consider(a, true)
	}
	for _, a := range wantVal {
		consider(a, false)
	}
	return addCat, addVal
}

// archiveIndexCoverage reports, per attribute, how many of the archive's segments carry no index
// on it and are therefore scanned in full by a query probing it. Attributes the archive does not
// index are omitted.
//
// It asks the planner rather than the index configuration because the configuration cannot tell:
// an index added to an existing archive is listed as soon as it is added, but it is carried back
// only as far as the archive's backfill horizon, and every segment older than that keeps the
// sidecar it was sealed with. The plan for an impossible equality on the attribute counts, segment
// by segment, which indexes cover it -- the same numbers `.explain` prints.
func archiveIndexCoverage(arch *db.ArchiveTable, attrs []string) map[string]int {
	cat, val := arch.IndexedAttrs()
	out := map[string]int{}
	for _, a := range attrs {
		var probe string
		switch {
		case slices.ContainsFunc(cat, func(b string) bool { return strings.EqualFold(a, b) }):
			probe = fmt.Sprintf("%s == %q", a, "htcondordb coverage probe")
		case slices.ContainsFunc(val, func(b string) bool { return strings.EqualFold(a, b) }):
			probe = a + " == -1"
		default:
			continue
		}
		ex, err := arch.Explain(probe)
		if err != nil || ex.Plan != "indexed" {
			continue
		}
		out[a] = ex.UnindexedSegments
	}
	return out
}

// reconcileTableIndexes is reconcileArchiveIndexes for a mutable table (the live job queue). A
// mutable table's indexes have no backfill horizon: AddIndex publishes the new set and Reindex
// builds it over every live ad. The Reindex runs in the background on wg -- on a large queue it
// reads every ad once -- and queries are correct throughout (an attribute not yet built is
// scanned).
func (m *scheddSyncManager) reconcileTableIndexes(ctx context.Context, d *db.DB, name string, wantCat, wantVal []string, wg *sync.WaitGroup) {
	haveCat, haveVal := d.IndexedAttrs()
	var auto []string
	for _, ix := range d.IndexSizes().PerIndex {
		if ix.Auto {
			auto = append(auto, ix.Attr)
		}
	}
	addCat, addVal := indexAdditions(haveCat, haveVal, auto, wantCat, wantVal)
	if len(addCat) == 0 && len(addVal) == 0 {
		return
	}
	if !d.AddIndex(addCat, addVal) || ctx.Err() != nil {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.logger.Info("schedd-sync: building table indexes", "table", name,
			"categorical", addCat, "value", addVal)
		d.Reindex()
	}()
}
