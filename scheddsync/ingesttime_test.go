package scheddsync

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestEnteredHistoryTime verifies EnteredHistoryTime is derived from the record's own
// history-entry time -- EnteredCurrentStatus, else CompletionDate, else the ingest clock --
// not the time htcondordb read the record, and that it is queryable as a range.
func TestEnteredHistoryTime(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")

	// Three records exercising the derivation chain:
	//   1: CompletionDate only            -> EnteredHistoryTime = CompletionDate
	//   2: EnteredCurrentStatus wins over CompletionDate
	//   3: neither                        -> EnteredHistoryTime = ingest clock (fallback)
	rec := func(cluster int, extra string) string {
		return fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nProcId = 0\nJobStatus = 4\n%s*** Offset = 0 ClusterId = %d ProcId = 0\n",
			cluster, cluster, extra, cluster)
	}
	writeFile(t, histPath,
		rec(1, "CompletionDate = 1700000001\n")+
			rec(2, "CompletionDate = 1700000002\nEnteredCurrentStatus = 1700000500\n")+
			rec(3, ""))

	ingest := time.Unix(1_800_000_000, 0)
	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath})
	s.now = func() time.Time { return ingest }
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 3 {
		t.Fatalf("Count = %d, want 3", arch.Count())
	}

	want := map[int64]int64{
		1: 1700000001,    // CompletionDate
		2: 1700000500,    // EnteredCurrentStatus wins
		3: ingest.Unix(), // fallback
	}
	seq, err := arch.Query("true")
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for ad := range seq {
		cid, _ := ad.EvaluateAttrInt("ClusterId")
		eht, ok := ad.EvaluateAttrInt(EnteredHistoryAttr)
		if !ok {
			t.Fatalf("cluster %d missing %s", cid, EnteredHistoryAttr)
		}
		if eht != want[cid] {
			t.Errorf("cluster %d: %s = %d, want %d", cid, EnteredHistoryAttr, eht, want[cid])
		}
		seen++
	}
	if seen != 3 {
		t.Fatalf("queried %d records, want 3", seen)
	}

	// Range query on the derived time (the "last 24h" pattern) -- the two dated records fall
	// before the cutoff; the fallback record (ingest ~1.8e9) is after it.
	rseq, err := arch.Query(EnteredHistoryAttr + " > " + itoa(ingest.Unix()-86400))
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for range rseq {
		matched++
	}
	if matched != 1 {
		t.Errorf("range query matched %d, want 1 (only the ingest-fallback record is recent)", matched)
	}
}

func itoa(v int64) string {
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
		if v == 0 {
			break
		}
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
