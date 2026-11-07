package bench

import (
	"strings"
	"testing"

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
