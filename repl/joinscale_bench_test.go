package repl

import (
	"fmt"
	"testing"
)

// Scratch: where does a store-side join's time go once the fixture is big enough to matter?
// Every other join measurement here runs at 400-2048 left rows, which fits in cache whole.
func BenchmarkJoinStoreScale(b *testing.B) {
	for _, left := range []int{2048, 20000, 100000} {
		fix, ops, want := newExpansionJoinFixture(b, left, 2, true)
		ops.reset()
		res, err := fix.srvExec.ExecString(expansionJoinSQL)
		if err != nil {
			b.Fatalf("left=%d: %v", left, err)
		}
		if len(res.Rows) != want {
			b.Fatalf("left=%d: %d rows, want %d", left, len(res.Rows), want)
		}
		if got := ops.waitOpsB(b, 1); got[0] != "JoinWire" {
			b.Fatalf("left=%d: first op %q, not JoinWire", left, got[0])
		}
		for _, arm := range []struct {
			name string
			f    func() error
		}{
			{"in-store", func() error { _, e := fix.srvExec.ExecString(expansionJoinSQL); return e }},
			{"client-side", func() error { _, e := fix.exec.ExecString(expansionJoinSQL); return e }},
		} {
			b.Run(fmt.Sprintf("left=%d/rows=%d/%s", left, want, arm.name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if e := arm.f(); e != nil {
						b.Fatal(e)
					}
				}
			})
		}
		fix.cleanup()
	}
}
