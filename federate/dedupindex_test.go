package federate

import (
	"slices"
	"strings"
	"testing"
)

// TestEveryDedupIdentityLeaderIsIndexed: the attribute each archive's dedup identity leads with
// must carry an index on the hub, or a Reset replay the sink cannot hold in memory scans the whole
// archive once per record.
//
// job_metrics is why this exists. Its identity leads with Key, which the hub did not index, so its
// probes were a scan while history's and epoch_history's -- both leading with the indexed
// GlobalJobId -- were not. It was the one federated table that never caught up.
func TestEveryDedupIdentityLeaderIsIndexed(t *testing.T) {
	for _, table := range []string{TableHistory, TableEpochHistory, TableJobMetrics} {
		spec := identitySpecs[table]
		if len(spec) == 0 {
			t.Fatalf("%s has no identity spec", table)
		}
		leader := spec[0].attr
		cats := archiveCategoricals(table)
		if !slices.ContainsFunc(cats, func(c string) bool { return strings.EqualFold(c, leader) }) {
			t.Errorf("%s dedup identity leads with %q, which is not in the categorical index set %v",
				table, leader, cats)
		}
	}
}

// TestDedupLeaderIsNotValueIndexed: these are string equalities, and a value index on a
// high-cardinality string is measurably WORSE than no index -- 19.7 ms a probe against 9.5 ms on
// the benchmark. Keep them categorical.
func TestDedupLeaderIsNotValueIndexed(t *testing.T) {
	for _, table := range []string{TableHistory, TableEpochHistory, TableJobMetrics} {
		leader := identitySpecs[table][0].attr
		if slices.ContainsFunc(archiveValue, func(v string) bool { return strings.EqualFold(v, leader) }) {
			t.Errorf("%s dedup identity leads with %q, which is value-indexed; it must be categorical", table, leader)
		}
	}
}

// TestJobMetricsArchiveIndexesKey: the index set reaches the archive the hub actually creates, not
// only the list it is built from.
func TestJobMetricsArchiveIndexesKey(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	t.Cleanup(func() { _ = cat.Close() })
	a := hubArchive(t, cat, TableJobMetrics)
	cats, vals := a.IndexedAttrs()
	if !slices.ContainsFunc(cats, func(c string) bool { return strings.EqualFold(c, "Key") }) {
		t.Errorf("the hub's job_metrics archive has categorical indexes %v (value %v), which do not include Key", cats, vals)
	}
}
