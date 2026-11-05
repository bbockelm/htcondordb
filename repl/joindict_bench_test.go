package repl

import (
	"fmt"
	"testing"
)

// Scratch: does a trained ZSTD dictionary remove the per-record Huffman table cost?
//
// Each record is its own zstd frame, so DecompressPrefix pays frame setup per record, and the
// CPU profile puts 61% of the literals decode in huff0.ReadTable rather than in the decode
// loop. A dictionary carries entropy tables the decoder loads once. The base codec is
// NewZSTDCodec(nil) -- dictionary-less -- and nothing in these fixtures calls RetrainDict,
// while db.Maintain does. So this compares the two.
func BenchmarkJoinDictEffect(b *testing.B) {
	const left = 20000
	// Two rounds with the order flipped: the first arm warms the allocator and page cache for
	// the second, and a 1.3x claim is exactly the size that effect can manufacture.
	order := [][]bool{{false, true}, {true, false}}
	for round, seq := range order {
		for _, trained := range seq {
			fix, ops, want := newExpansionJoinFixture(b, left, 2, true)
			label := fmt.Sprintf("r%d/nodict", round)
			if trained {
				label = fmt.Sprintf("r%d/dict", round)
				for _, name := range []string{"history", "epoch"} {
					at, ok := fix.cat.ArchiveTable(name)
					if !ok {
						b.Fatalf("no archive table %q", name)
					}
					n, err := at.RetrainDict(4096)
					if err != nil {
						b.Fatalf("RetrainDict(%s): %v", name, err)
					}
					b.Logf("%s: retrained over %d records", name, n)
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
			b.Run(fmt.Sprintf("left=%d/rows=%d/%s", left, want, label), func(b *testing.B) {
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
