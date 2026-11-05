package repl

import (
	"fmt"
	"testing"
)

// Scratch: row-oriented against columnar, for the store-side join.
//
// The archive's columnar accelerator is opt-in (BuildAndEnableSchemaScan), and the other join
// fixtures here never call it -- so every join measurement so far read ROW-ORIENTED records,
// where each record is its own zstd frame and DecompressPrefix pays frame setup per record. A
// columnar block compresses its regions ONCE per block (colblock.go: strComp, coldComp,
// coldNumComp), so the per-record frame cost should largely disappear.
//
// That matters beyond decompression: if the read gets much cheaper, the pairing's SHARE of the
// join rises, and the question of whether the pair loop is worth optimizing is a different
// question than it is on the row path.
func BenchmarkJoinColumnar(b *testing.B) {
	for _, left := range []int{20000, 100000} {
		// Two rounds with the arm order flipped: the first arm warms the allocator for the
		// second, which is worth more than the effect being measured if left unchecked.
		for round := 0; round < 2; round++ {
			cols := []bool{false, true}
			if round%2 == 1 {
				cols = []bool{true, false}
			}
			for _, columnar := range cols {
				fix, ops, want := newExpansionJoinFixture(b, left, 2, true)
				label := "rows"
				if columnar {
					label = "columnar"
					for _, name := range []string{"history", "epoch"} {
						at, ok := fix.cat.ArchiveTable(name)
						if !ok {
							b.Fatalf("no archive table %q", name)
						}
						if !at.BuildAndEnableSchemaScan(4096, 16) {
							b.Fatalf("%s: columnar accelerator did not build -- this arm would "+
								"silently measure the row path again", name)
						}
					}
				}
				ops.reset()
				res, err := fix.srvExec.ExecString(expansionJoinSQL)
				if err != nil {
					b.Fatalf("%s: %v", label, err)
				}
				if len(res.Rows) != want {
					b.Fatalf("%s: %d rows, want %d", label, len(res.Rows), want)
				}
				if got := ops.waitOpsB(b, 1); got[0] != "JoinWire" {
					b.Fatalf("%s: first op %q, not JoinWire", label, got[0])
				}
				b.Run(fmt.Sprintf("left=%d/rows=%d/r%d/%s", left, want, round, label), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, e := fix.srvExec.ExecString(expansionJoinSQL); e != nil {
							b.Fatal(e)
						}
					}
				})
				fix.cleanup()
			}
		}
	}
}
