package flightsvc

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
)

// ONE SCHEMA-SCAN PASS DOES NOT COLUMNARIZE A TABLE PAST THE PASS BUDGET, and the accelerator's
// coverage figure does not show it.
//
// A pass rewrites a bounded number of sealed segments into columnar form -- 64 by default, so
// that enabling the accelerator over an existing archive does not stall on hours of I/O -- and a
// deployment converges over several maintenance intervals. A FIXTURE that calls it once is
// therefore mostly reading from records, and `SchemaScanInfo` reports every sealed segment
// covered the whole time: coverage tracks whether a segment has a schema, not whether its
// records were rewritten.
//
// This cost a benchmark in this package its meaning. Measured on a 478-segment archive, one pass
// left 87% of records rebuilt from the arena, and the Arrow read measured 9 MB/s where a
// converged table measures 270-350. Worse, it made the two arms of BenchmarkLocalVsFramedOverGRPC
// indistinguishable -- 9.12 against 9.09 MB/s -- because the record path dominated both, hiding
// the in-process column path's advantage entirely. That benchmark's largest size (20000 rows, 47
// segments) happens to sit under the budget, which is the only reason it measured what it claimed.
//
// Kept small and fast: tiny segments reach the budget in a few thousand rows, so this pins the
// behaviour without the minutes a realistic fixture would take.
func TestColumnarizationNeedsMorePassesThanOne(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cat.Close() }()
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId", "JobStatus"}, ZoneAttrs: []string{"CompletionDate"},
		// Small enough that a few thousand records seal well past one pass's budget.
		SegmentSize: 16 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	const rows = 8000
	for i := 0; i < rows; i++ {
		ad, perr := classad.ParseOld(benchAd(i))
		if perr != nil {
			t.Fatal(perr)
		}
		if aerr := arch.Append(ad); aerr != nil {
			t.Fatal(aerr)
		}
	}

	escape := func() (float64, int) {
		var st collections.ScanStats
		seq, qerr := arch.QueryProjectStats("true", benchProjection, &st)
		if qerr != nil {
			t.Fatal(qerr)
		}
		for range seq {
		}
		if st.RecordsVisited == 0 {
			t.Fatal("the probe visited no records")
		}
		return float64(st.RecordsReassembled) / float64(st.RecordsVisited), st.RecordsVisited
	}

	if !arch.BuildAndEnableSchemaScan(2000, 8) {
		t.Fatal("the columnar accelerator did not build")
	}
	info := arch.SchemaScanInfo()
	if info.SealedSegments < 100 {
		t.Skipf("only %d sealed segments; this test needs a table past one pass's budget", info.SealedSegments)
	}
	onePass, visited := escape()
	t.Logf("after one pass: %d sealed, %d covered, %.1f%% of %d records reassembled",
		info.SealedSegments, info.CoveredSegments, 100*onePass, visited)

	// The trap, asserted: coverage reads FULL while most records still reassemble.
	if info.CoveredSegments != info.SealedSegments {
		t.Errorf("coverage reports %d of %d sealed; this test's premise is that it reports full coverage after one pass",
			info.CoveredSegments, info.SealedSegments)
	}
	if onePass < 0.25 {
		t.Fatalf("one pass left only %.1f%% reassembled; the pass budget no longer bounds a table this size, so this test is not measuring what it describes",
			100*onePass)
	}

	// Repeating converges it.
	last := onePass
	passes := 1
	for ; passes <= 20; passes++ {
		if !arch.BuildAndEnableSchemaScan(2000, 8) {
			break
		}
		last, _ = escape()
		if last <= 0.05 {
			break
		}
	}
	t.Logf("converged to %.2f%% after %d further pass(es)", 100*last, passes)
	if last > 0.05 {
		t.Errorf("repeating the pass did not converge: still %.1f%% reassembled after %d passes", 100*last, passes)
	}
	if !(last < onePass) {
		t.Errorf("repeating the pass did not improve on one (%.1f%% vs %.1f%%)", 100*last, 100*onePass)
	}
}
