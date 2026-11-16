package federate

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// TestApproxAdBytesTracksTheHeap: the catch-up batch is bounded in bytes, so the estimate it is
// bounded by has to be of the right order -- a budget fed a number ten times too small is a
// per-record probe loop again, and one ten times too large is ten times the memory. Measured
// against ads of three shapes, for the heap they really hold down.
func TestApproxAdBytesTracksTheHeap(t *testing.T) {
	ad := func(ints, strs int, long bool) string {
		var b strings.Builder
		b.WriteString("[")
		for i := 0; i < ints; i++ {
			fmt.Fprintf(&b, "Attribute%03d = %d; ", i, 1_700_000_000+i)
		}
		for i := 0; i < strs; i++ {
			fmt.Fprintf(&b, "StrAttr%02d = %q; ", i, strings.Repeat("v", 40))
		}
		if long {
			fmt.Fprintf(&b, "Environment = %q; Args = %q; ", strings.Repeat("e", 300), strings.Repeat("a", 200))
		}
		b.WriteString(`GlobalJobId = "ap1#1.0#1700000000"]`)
		return b.String()
	}
	for _, c := range []struct {
		name string
		text string
	}{
		{"history-shaped", ad(90, 15, true)},
		{"sample-shaped", ad(6, 2, false)},
		{"wide", ad(200, 0, false)},
	} {
		t.Run(c.name, func(t *testing.T) {
			const n = 2000
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			ads := make([]*classad.ClassAd, 0, n)
			for i := 0; i < n; i++ {
				parsed, err := classad.Parse(c.text)
				if err != nil {
					t.Fatal(err)
				}
				ads = append(ads, parsed)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			heap := float64(after.HeapAlloc-before.HeapAlloc) / n
			est := float64(approxAdBytes(ads[0]))
			runtime.KeepAlive(ads)
			t.Logf("%d attributes, %d bytes of text: estimate %.0f B, heap %.0f B per ad (%.2fx)",
				ads[0].Size(), len(c.text), est, heap, est/heap)
			if est < heap/2 || est > heap*2 {
				t.Errorf("estimate %.0f B is %.2fx the %.0f B an ad really holds; the batch budget would be off by that much", est, est/heap, heap)
			}
		})
	}
}
