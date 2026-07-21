package repl

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// TestHistLine: only non-empty buckets are rendered, with <= bound labels and a final
// > overflow label.
func TestHistLine(t *testing.T) {
	// bounds: 100µs,1ms,10ms,100ms,1s,5s,10s,30s -> 9 buckets (8 + overflow).
	buckets := []int64{0, 0, 0, 0, 5, 0, 0, 2, 1}
	got := histLine(buckets)
	want := "<=1s:5 <=30s:2 >30s:1"
	if got != want {
		t.Errorf("histLine = %q, want %q", got, want)
	}
	if histLine(nil) != "" {
		t.Errorf("histLine(nil) should be empty")
	}
}

// TestShowOpStatsMaxAndHistogram: the max is always printed; the histogram breakdown
// appears only for an op whose tail is non-trivial (max > 100ms).
func TestShowOpStatsMaxAndHistogram(t *testing.T) {
	var o db.OpStats
	// A fast op: sub-ms, no histogram line expected.
	o.ShardWriteHold = db.OpStat{Count: 1000, Nanos: int64(42 * time.Millisecond), MaxNanos: int64(500 * time.Microsecond)}
	// A tail-prone op: a 20s max should trigger the histogram line.
	o.Sync = db.OpStat{
		Count:    62308,
		Nanos:    int64(153 * time.Second),
		MaxNanos: int64(20 * time.Second),
		Buckets:  []int64{0, 60000, 2000, 200, 80, 0, 20, 8, 0},
	}

	var b bytes.Buffer
	showOpStats(&b, o)
	out := b.String()

	if !strings.Contains(out, "max=") {
		t.Errorf("expected max= on every row; got:\n%s", out)
	}
	if !strings.Contains(out, "max=20s") {
		t.Errorf("expected sync max=20s; got:\n%s", out)
	}
	// The sync tail histogram should be shown.
	if !strings.Contains(out, "<=1ms:60000") || !strings.Contains(out, "<=30s:8") {
		t.Errorf("expected sync histogram breakdown; got:\n%s", out)
	}
	// The fast op (max 500µs) must NOT get a histogram line.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "shard write hold") && strings.Contains(line, ":") && strings.Contains(line, "<=") {
			t.Errorf("fast op should not render a histogram line: %q", line)
		}
	}
}
