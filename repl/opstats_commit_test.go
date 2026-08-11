package repl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
)

// The commit row is the only Count in this table that is a number of COMMITS. Every other row
// counts an internal event, so 97k commits with no batching and 12k commits fanning out over 8
// shards produce identical-looking numbers -- which is the question operators actually ask of
// these counters, and could not be answered before.
func TestShowOpStatsCommitRow(t *testing.T) {
	var b bytes.Buffer
	showOpStats(&b, db.OpStats{OpStats: collections.OpStats{
		ShardWriteHold: db.OpStat{Count: 97883, Nanos: 25110000000},
		Sync:           db.OpStat{Count: 97847, Nanos: 673710000000},
		CommitSync:     db.OpStat{Count: 12235, Nanos: 700000000000},
	}})
	got := b.String()
	if !strings.Contains(got, "commit (durability)") {
		t.Errorf("no commit row:\n%s", got)
	}
	// 97847/12235 = 8.0 -- the shard fan-out, which is the whole point of showing both.
	if !strings.Contains(got, "8.0 msync(s) per commit") {
		t.Errorf("no durability ratio, or wrong:\n%s", got)
	}
	if !strings.Contains(got, "97847 syncs / 12235 commits") {
		t.Errorf("ratio should show its inputs:\n%s", got)
	}
}

// A Put-only workload records no commitSync (a bare Put goes through the shard's group-commit
// queue, not Txn.Commit). Showing "n=0" would read as "no commits happened", which is false and
// worse than omitting the row.
func TestShowOpStatsHidesEmptyCommitRow(t *testing.T) {
	var b bytes.Buffer
	showOpStats(&b, db.OpStats{OpStats: collections.OpStats{
		ShardWriteHold: db.OpStat{Count: 500, Nanos: 1000},
		Sync:           db.OpStat{Count: 500, Nanos: 2000},
	}})
	got := b.String()
	if strings.Contains(got, "commit (durability)") {
		t.Errorf("commit row shown for a non-transactional workload:\n%s", got)
	}
	if strings.Contains(got, "per commit") {
		t.Errorf("ratio shown with no commit count:\n%s", got)
	}
	if !strings.Contains(got, "sync (msync)") {
		t.Errorf("the other rows must still print:\n%s", got)
	}
}
