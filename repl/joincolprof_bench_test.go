package repl

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/collections"
)

// Scratch: profile target. ONE arm, columnar, so a CPU profile of this binary is the columnar
// read path and not a mixture of arms.
//
// It asserts the segments were actually rewritten before timing. BuildAndEnableSchemaScan runs
// schemaScanPass, which calls ColumnarizeSealed -- but a fixture whose segments were all still
// unsealed, or whose schema did not derive, would quietly measure the row path again and look
// like "columnar made no difference".
func BenchmarkJoinColumnarProfile(b *testing.B) {
	const left = 100000
	fix, ops, want := newExpansionJoinFixture(b, left, 2, true)
	defer fix.cleanup()

	before, _ := collections.ColumnarizedSegments()
	for _, name := range []string{"history", "epoch"} {
		at, ok := fix.cat.ArchiveTable(name)
		if !ok {
			b.Fatalf("no archive table %q", name)
		}
		if !at.BuildAndEnableSchemaScan(4096, 16) {
			b.Fatalf("%s: columnar accelerator did not build", name)
		}
	}
	after, savedBytes := collections.ColumnarizedSegments()
	if after <= before {
		b.Fatalf("no segment was columnarized (%d -> %d): this would measure the ROW path", before, after)
	}
	b.Logf("columnarized %d segments, %d bytes saved", after-before, savedBytes)

	ops.reset()
	res, err := fix.srvExec.ExecString(expansionJoinSQL)
	if err != nil {
		b.Fatal(err)
	}
	if len(res.Rows) != want {
		b.Fatalf("%d rows, want %d", len(res.Rows), want)
	}
	if got := ops.waitOpsB(b, 1); got[0] != "JoinWire" {
		b.Fatalf("first op %q, not JoinWire", got[0])
	}

	b.Run(fmt.Sprintf("left=%d/rows=%d/columnar", left, want), func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, e := fix.srvExec.ExecString(expansionJoinSQL); e != nil {
				b.Fatal(e)
			}
		}
	})
}
