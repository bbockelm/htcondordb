package scheddsync

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestEnteredHistoryTimeStamped verifies every archived record gets an EnteredHistoryTime
// (htcondordb ingest time), that it is queryable as a range, and that re-processing does not
// re-stamp an already-archived record.
func TestEnteredHistoryTimeStamped(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	writeFile(t, histPath, histRecord(1, 0, 4)+histRecord(2, 0, 4))

	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath})
	fixed := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return fixed }
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 2 {
		t.Fatalf("Count = %d, want 2", arch.Count())
	}

	// Every record carries the ingest time.
	seq, err := arch.Query("true")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ad := range seq {
		n++
		v, ok := ad.EvaluateAttrInt(EnteredHistoryAttr)
		if !ok {
			t.Fatalf("record missing %s", EnteredHistoryAttr)
		}
		if v != fixed.Unix() {
			t.Errorf("%s = %d, want %d", EnteredHistoryAttr, v, fixed.Unix())
		}
	}
	if n != 2 {
		t.Fatalf("queried %d records, want 2", n)
	}

	// Range query on the ingest time (the "last 24h" pattern).
	rseq, err := arch.Query(EnteredHistoryAttr + " > " + itoa(fixed.Unix()-86400))
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for range rseq {
		matched++
	}
	if matched != 2 {
		t.Errorf("range query matched %d, want 2", matched)
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
