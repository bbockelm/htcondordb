package repl

import (
	"fmt"
	"testing"
)

// WHAT A CONSTRAINT LONGER THAN THE OLD CAP COSTS.
//
// The key-batch sweeps stopped at 511 keys because the planner refused to build a longer
// constraint than that, so every reading of the batched arm's cost comes from below 512 and the
// shape of the curve past it was never measured -- only extrapolated. Pricing the decision
// instead of capping it means plans above 512 keys are now reachable, and a model that has
// never been checked there is a model being trusted, not used.
//
// So this sweeps the key count through and past the old cap against a right table large enough
// that the trade is live, and prices both arms the same way the sweeps below the cap priced
// them: in one binary, in process, with the constraint the only difference between the arms.
//
// It stops at 1,024 because that is where the constraint stops being evaluable rather than
// where it stops paying: the terms nest, and past about 2,000 of them the evaluator gives up
// and the predicate matches nothing. TestKeyBatchStaysInsideTheEvaluatorsDepthLimit pins that
// boundary; what it means here is that the arms above 1,024 cannot be compared, because one of
// them would not be computing the answer.
func BenchmarkJoinKeyBatchPastTheCap(b *testing.B) {
	// 131,072 epoch rows: large enough that the unbatched scan is the expensive arm at every
	// key count here, so what moves between cells is the constraint and not the scan.
	const clusters = 65536
	fix := newJoinBenchFixture(b, clusters)
	defer fix.cleanup()

	for _, k := range []int{256, 512, 1024} {
		sql := fmt.Sprintf(
			`SELECT h.ClusterId, e.ExitCode FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId WHERE h.ClusterId <= %d`, k)
		st, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		arms := []bool{true, false}
		if benchReverseJoinArms {
			arms = []bool{false, true}
		}
		for _, batching := range arms {
			name := "batched"
			if !batching {
				name = "unbatched"
			}
			// Agreement first, at every key count: a constraint this long must still be a
			// pure narrowing. If a 8,192-term probe ever dropped a row, the cap would have
			// been load-bearing for correctness and not only for cost.
			got, rerr := runBenchJoinBatch(st, fix.fetchWhole, batching)
			if rerr != nil {
				b.Fatal(rerr)
			}
			if got != 2*k {
				b.Fatalf("keys=%d %s: joined %d rows, expected %d", k, name, got, 2*k)
			}
			b.Run(fmt.Sprintf("keys=%d/%s", k, name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := runBenchJoinBatch(st, fix.fetchWhole, batching); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
