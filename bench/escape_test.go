package bench

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// preparedArchive builds a generated, steady-state `history` archive and serves it. An archive
// rather than a mutable table because that is the shape this measurement is for -- a history read
// is where the columnar accelerator decides the cost -- and because a freshly filled mutable table
// has no sealed segments, so its scan counters come back unreported and every assertion below
// would be vacuous.
func preparedArchive(t *testing.T, rows int) Dialer {
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
	rep := PrepareArchive(arch, DefaultPrepareOptions(ShapeHistory))
	if !rep.SchemaScanOn {
		t.Fatalf("the columnar accelerator did not build; this test cannot distinguish the paths: %v", rep.Notes)
	}
	return serveCatalog(t, cat)
}

// The escape rate must be a property of the PROJECTION, not only of the store.
//
// This is the measurement that separates two runs whose reported coverage is identical. A
// projection of plain stored attributes is served from the columns; one naming an expression that
// reaches outside itself -- MemoryUsage over ResidentSetSize, the shape real job ads have -- makes
// the server rebuild each record instead. A harness reporting only "columnarized: yes" would
// present those two as the same configuration.
func TestEscapeRateDependsOnTheProjection(t *testing.T) {
	dial := preparedArchive(t, 20000)
	cn, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	c := cn.Client
	ctx := context.Background()

	plain, err := MeasureEscape(ctx, c, "history", "true",
		[]string{"ClusterId", "ProcId", "Owner", "JobStatus"}, 5000)
	if err != nil {
		t.Fatal(err)
	}
	reaching, err := MeasureEscape(ctx, c, "history", "true",
		[]string{"ClusterId", "Owner", "MemoryUsage"}, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if !plain.Measured || !reaching.Measured {
		t.Fatal("no server-side breakdown came back; this test proved nothing")
	}
	t.Logf("plain projection:    visited=%d reassembled=%d closure=%d rate=%.1f%%",
		plain.Visited, plain.Reassembled, plain.ClosureReassembled, 100*plain.Rate())
	t.Logf("reaching projection: visited=%d reassembled=%d closure=%d rate=%.1f%% closureRate=%.1f%%",
		reaching.Visited, reaching.Reassembled, reaching.ClosureReassembled,
		100*reaching.Rate(), 100*reaching.ClosureRate())

	if !(reaching.Rate() > plain.Rate()) {
		t.Fatalf("a projection naming an expression reaching outside itself did not escape more than a plain one (%.1f%% vs %.1f%%); the probe cannot distinguish the paths it exists to distinguish",
			100*reaching.Rate(), 100*plain.Rate())
	}
}

// A high escape rate must reach the operator as a warning, not only as a field: the whole point is
// that a rate published beside "columnarized: yes" misrepresents what was measured.
func TestReadinessWarnsOnHighEscape(t *testing.T) {
	dial := preparedArchive(t, 20000)
	cn, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	c := cn.Client
	r, perr := Probe(context.Background(), c, "history", "true",
		[]string{"ClusterId", "Owner", "MemoryUsage"})
	if perr != nil {
		t.Fatal(perr)
	}
	t.Logf("escape: %+v rate=%.1f%%", r.Escape, 100*r.Escape.Rate())
	if !r.Escape.Measured {
		t.Fatal("readiness reported no escape measurement")
	}
	if r.Escape.Rate() > 0.25 {
		found := false
		for _, w := range r.Warnings {
			if len(w) > 0 && (w[0] == '5' || w[0] == '6' || w[0] == '7' || w[0] == '8' || w[0] == '9' || w[0] == '1' || w[0] == '2' || w[0] == '3' || w[0] == '4') {
				found = true
			}
		}
		if !found {
			t.Fatalf("escape rate %.1f%% did not produce a warning; warnings = %v", 100*r.Escape.Rate(), r.Warnings)
		}
	}
}

// Escape splits on SHIPPING an expression-valued attribute, not on filtering one and not on
// columnar coverage. This is the table the reported rate has to be read against, and it is easy
// to misread: a 100% escape on a projection naming MemoryUsage is not a misconfigured store.
//
// A computed column answers a FILTER on MemoryUsage from the columns without touching a record.
// An ad that SHIPS MemoryUsage must carry the stored expression rather than the value it computes
// to -- the value is a property of the record, not part of it -- so the record is rebuilt.
func TestEscapeSplitsOnShippingNotFiltering(t *testing.T) {
	dial := preparedArchive(t, 20000)
	cn, err := dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	c := cn.Client
	ctx := context.Background()
	plain := []string{"ClusterId", "Owner"}

	filtered, err := MeasureEscape(ctx, c, "history", "MemoryUsage > 100", plain, 5000)
	if err != nil {
		t.Fatal(err)
	}
	shipped, err := MeasureEscape(ctx, c, "history", "true", append(append([]string{}, plain...), "MemoryUsage"), 5000)
	if err != nil {
		t.Fatal(err)
	}
	shippedInput, err := MeasureEscape(ctx, c, "history", "true", append(append([]string{}, plain...), "ResidentSetSize"), 5000)
	if err != nil {
		t.Fatal(err)
	}
	if !filtered.Measured || !shipped.Measured || !shippedInput.Measured {
		t.Fatal("no server-side breakdown came back; this test proved nothing")
	}
	t.Logf("filter on computed: escape=%.1f%% colDecided=%d", 100*filtered.Rate(), filtered.ColumnDecided)
	t.Logf("ship computed:      escape=%.1f%%", 100*shipped.Rate())
	t.Logf("ship its input:     escape=%.1f%%", 100*shippedInput.Rate())

	if filtered.Rate() > 0.05 {
		t.Errorf("filtering on a computed attribute reassembled %.1f%% of records; the computed column is not answering the filter",
			100*filtered.Rate())
	}
	if filtered.ColumnDecided == 0 {
		t.Error("filtering on a computed attribute decided nothing from the columns")
	}
	if shipped.Rate() < 0.5 {
		t.Errorf("shipping an expression-valued attribute reassembled only %.1f%%; this test no longer measures the distinction it describes",
			100*shipped.Rate())
	}
	if shippedInput.Rate() > 0.05 {
		t.Errorf("shipping a stored literal reassembled %.1f%%; the cost is not specific to expressions after all",
			100*shippedInput.Rate())
	}
}
