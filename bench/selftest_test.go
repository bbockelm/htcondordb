package bench

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/PelicanPlatform/classad/db"
)

// benchArchive builds a generated, prepared `history` archive without serving it.
func benchArchive(t *testing.T, rows int) *db.ArchiveTable {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs:  []string{"ClusterId", "CompletionDate"},
		ZoneAttrs:   []string{"CompletionDate"},
		SegmentSize: 1 << 18,
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
	if rep := PrepareArchive(arch, DefaultPrepareOptions(ShapeHistory)); !rep.SchemaScanOn {
		t.Fatalf("columnar accelerator did not build: %v", rep.Notes)
	}
	return arch
}

// The self-test must produce a rate for every transport and must keep the two forms apart.
func TestSelfTestCoversEveryTransport(t *testing.T) {
	arch := benchArchive(t, 20000)
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
		if s.Rows != 20000 {
			t.Errorf("%s: %d rows, want 20000", s.Transport, s.Rows)
		}
		if s.RowsPerSec() <= 0 {
			t.Errorf("%s: no production rate", s.Transport)
		}
	}
}

// The evaluated form produces no serialized bytes, and the report must SAY that rather than
// print 0.0 MB/s beside real rates -- a zero in a rate column reads as "delivered nothing".
func TestSelfTestDoesNotInventBytesForTheEvaluatedForm(t *testing.T) {
	arch := benchArchive(t, 5000)
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
	arch := benchArchive(t, 20000)
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
	arch := benchArchive(t, 2000)
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
