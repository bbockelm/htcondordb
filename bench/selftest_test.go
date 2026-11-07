package bench

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/PelicanPlatform/classad/db"
)

// sharedArchive is one generated, prepared `history` archive, built once and reused by every
// test that only READS it.
//
// Building it is the expensive part -- generating and appending ads, which under -race dominates
// everything else -- and it is identical for each of those tests. Rebuilding it per test cost
// this package several minutes of the repository's whole-suite timeout for no additional
// coverage. A test that needs a DIFFERENT state (an unprepared archive, a particular segment
// size) builds its own.
var (
	sharedArchiveOnce sync.Once
	sharedArchiveTbl  *db.ArchiveTable
	sharedArchiveErr  error
)

const sharedArchiveRows = 3000

func sharedArchive(t *testing.T) *db.ArchiveTable {
	t.Helper()
	sharedArchiveOnce.Do(func() {
		dir, err := os.MkdirTemp("", "bench-shared")
		if err != nil {
			sharedArchiveErr = err
			return
		}
		cat, err := db.OpenCatalog(dir)
		if err != nil {
			sharedArchiveErr = err
			return
		}
		arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
			ValueAttrs: []string{"ClusterId", "CompletionDate"}, ZoneAttrs: []string{"CompletionDate"},
			SegmentSize: 32 << 10,
		})
		if err != nil {
			sharedArchiveErr = err
			return
		}
		g := NewGenerator(DefaultGenConfig(ShapeHistory), sharedArchiveRows)
		for i := 0; i < sharedArchiveRows; i++ {
			_, ad := g.Ad(i)
			if aerr := arch.Append(ad); aerr != nil {
				sharedArchiveErr = aerr
				return
			}
		}
		if rep := PrepareArchive(arch, DefaultPrepareOptions(ShapeHistory)); !rep.SchemaScanOn {
			sharedArchiveErr = fmt.Errorf("columnar accelerator did not build: %v", rep.Notes)
			return
		}
		sharedArchiveTbl = arch
	})
	if sharedArchiveErr != nil {
		t.Fatalf("shared archive: %v", sharedArchiveErr)
	}
	return sharedArchiveTbl
}

// The self-test must produce a rate for every transport and must keep the two forms apart.
func TestSelfTestCoversEveryTransport(t *testing.T) {
	arch := sharedArchive(t)
	rep := SelfTestArchive(arch, SelfTestSpec{
		Table: "history", Constraint: "true",
		Projection: []string{"ClusterId", "Owner", "JobStatus"},
		Iterations: 1,
	})
	if len(rep.Samples) != 4 {
		t.Fatalf("%d samples, want 4", len(rep.Samples))
	}
	for _, s := range rep.Samples {
		if s.Err != nil {
			t.Fatalf("%s: %v", s.Transport, s.Err)
		}
		if s.Rows != sharedArchiveRows {
			t.Errorf("%s: %d rows, want %d", s.Transport, s.Rows, sharedArchiveRows)
		}
		if s.RowsPerSec() <= 0 {
			t.Errorf("%s: no production rate", s.Transport)
		}
	}
}

// The evaluated form produces no serialized bytes, and the report must SAY that rather than
// print 0.0 MB/s beside real rates -- a zero in a rate column reads as "delivered nothing".
func TestSelfTestDoesNotInventBytesForTheEvaluatedForm(t *testing.T) {
	arch := sharedArchive(t)
	rep := SelfTestArchive(arch, SelfTestSpec{
		Table: "history", Constraint: "true",
		Projection: []string{"ClusterId", "Owner"},
		Iterations: 1,
	})
	for _, s := range rep.Samples {
		if s.Transport.Evaluated() {
			if s.Bytes != 0 {
				t.Errorf("the evaluated form reported %d bytes; it has no serialized size here", s.Bytes)
			}
			if s.MBPerSec() != 0 {
				t.Errorf("the evaluated form reported %.1f MB/s from no bytes", s.MBPerSec())
			}
		} else if s.Bytes <= 0 {
			t.Errorf("%s produced no bytes", s.Transport)
		}
	}
	out := rep.String()
	if !strings.Contains(out, "no serialized bytes") {
		t.Errorf("the report does not say why the evaluated form has no MB/s:\n%s", out)
	}
	if !strings.Contains(out, "not how fast a consumer receives them") {
		t.Errorf("the report does not distinguish production from delivery:\n%s", out)
	}
	t.Logf("\n%s", out)
}

// The consumer pass must do REAL work: it has to be measurably slower than the producer pass for
// a form whose rows need decoding. A consume pass that matched produce would mean the work was
// skipped or optimized away, and the whole comparison would be of two empty loops.
func TestSelfTestConsumerPassDoesRealWork(t *testing.T) {
	arch := sharedArchive(t)
	rep := SelfTestArchive(arch, SelfTestSpec{
		Table: "history", Constraint: "true",
		Projection: []string{"ClusterId", "Owner", "JobStatus"},
		Iterations: 1,
	})
	for _, s := range rep.Samples {
		if s.Err != nil {
			t.Fatalf("%s produce: %v", s.Transport, s.Err)
		}
		if s.ConsumeErr != nil {
			t.Fatalf("%s consume: %v", s.Transport, s.ConsumeErr)
		}
		if s.ConsumedRows != s.Rows {
			t.Errorf("%s: consumed %d rows but produced %d", s.Transport, s.ConsumedRows, s.Rows)
		}
		t.Logf("%-22s produce %9.0f rows/s   consume %9.0f rows/s   consumer share %.0f%%",
			s.Transport, s.RowsPerSec(), s.ConsumeRowsPerSec(), 100*s.ConsumerShare())
		// Asserted only for the TEXT forms. Parsing an ad out of text is most of their cost
		// (measured at 69-79% of the consuming pass), so the margin is far outside timing
		// noise. The wire form's consumer cost is genuinely about 5% -- decoding it is nearly
		// free, which is the finding -- so requiring consume > produce there compares two
		// nearly equal durations and fails intermittently, which it did.
		//
		// That wire's consumer work RUNS at all is covered by TestConsumerSinkIsPopulated,
		// which checks the decoded result rather than a duration.
		if s.Transport == TextWhole || s.Transport == TextProjected {
			if s.ConsumeElapsed <= s.Elapsed {
				t.Errorf("%s: consuming (%v) was not slower than producing (%v); the consumer work is not running",
					s.Transport, s.ConsumeElapsed, s.Elapsed)
			}
			if s.ConsumerShare() < 0.25 {
				t.Errorf("%s: the consumer accounted for only %.0f%% of the consuming pass; parsing an ad out of text cannot be that cheap",
					s.Transport, 100*s.ConsumerShare())
			}
		}
	}
}

// The consumer's results must actually be observable, which is what stops the compiler eliding
// the work the consuming pass exists to measure.
func TestConsumerSinkIsPopulated(t *testing.T) {
	arch := sharedArchive(t)
	_ = SelfTestArchive(arch, SelfTestSpec{
		Table: "history", Constraint: "true",
		Projection: []string{"ClusterId", "Owner"}, Iterations: 1,
	})
	ad, vt := ConsumerSink()
	if ad == nil {
		t.Error("the consuming pass left no parsed ad; its work may have been elided")
	}
	if vt == classad.UndefinedValue {
		t.Errorf("the value pass left %v; it did not read a typed value", vt)
	}
}

// PREPARATION MUST CONVERGE THE COLUMNAR FORM, which one schema-scan pass does not do on a table
// bigger than the pass budget.
//
// A pass rewrites a bounded number of sealed segments (64 by default), so a single call leaves a
// large archive mostly non-columnar -- and the accelerator's own coverage figure does not show
// it: it reports every sealed segment covered after the first pass while most records are still
// rebuilt from the arena on every read. A benchmark preparing with one pass measures a table no
// deployment runs and reports it as the prepared state.
func TestPrepareConvergesColumnarization(t *testing.T) {
	// Small segments reach the pass budget in a few thousand rows, where a realistic segment
	// size needs over a hundred thousand -- the property under test is the SEGMENT count, not
	// the row count, and this package's tests run under -race in CI against a whole-repo
	// timeout.
	const rows = 3000
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId", "CompletionDate"}, ZoneAttrs: []string{"CompletionDate"},
		SegmentSize: 16 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	g := NewGenerator(DefaultGenConfig(ShapeHistory), rows)
	for i := 0; i < rows; i++ {
		_, ad := g.Ad(i)
		if aerr := arch.Append(ad); aerr != nil {
			t.Fatal(aerr)
		}
	}
	// One pass, as the naive preparation did: this is the state being guarded against.
	arch.BuildAndEnableSchemaScan(4000, 8)
	onePass, ok := sampleArchiveEscape(arch, nil, 5000)
	if !ok {
		t.Fatal("could not sample the escape rate")
	}
	info := arch.SchemaScanInfo()
	t.Logf("after ONE pass: %.1f%% reassembled, while coverage reports %d/%d segments",
		100*onePass, info.CoveredSegments, info.SealedSegments)
	if onePass < 0.2 {
		t.Skipf("one pass already converged at this size (%.1f%%); the test needs a table bigger than the pass budget", 100*onePass)
	}
	// The coverage figure is the trap: it says fully covered while most records reassemble.
	if info.CoveredSegments != info.SealedSegments {
		t.Errorf("coverage reports %d of %d; the premise of this test is that it reports FULL coverage here",
			info.CoveredSegments, info.SealedSegments)
	}

	rep := PrepareArchive(arch, DefaultPrepareOptions(ShapeHistory))
	t.Logf("after preparation: %d passes, %.2f%% reassembled", rep.ColumnarPasses, 100*rep.ResidualEscape)
	if rep.ColumnarPasses < 2 {
		t.Errorf("preparation used %d pass(es); a table past the pass budget needs more", rep.ColumnarPasses)
	}
	// Not asserted against zero: the probe reads newest-first and the active segment is never
	// columnarized, so a small residue is correct and permanent.
	if rep.ResidualEscape > columnarResidualWarn {
		t.Errorf("preparation left %.1f%% reassembled, want at most %.1f%%",
			100*rep.ResidualEscape, 100*columnarResidualWarn)
	}
	if rep.ColumnarPasses >= maxColumnarPasses {
		t.Errorf("preparation used the full %d passes; it should stop when passes stop converting anything",
			maxColumnarPasses)
	}
	if rep.ResidualEscape >= onePass {
		t.Errorf("preparation did not improve on a single pass (%.1f%% vs %.1f%%)",
			100*rep.ResidualEscape, 100*onePass)
	}
}
