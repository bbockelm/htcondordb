package server

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// TestColumnarOnByDefault proves the per-segment columnar accelerator -- CountConstraint's fast
// path for COUNT(*) WHERE <numeric> -- is ON BY DEFAULT: a Maintain pass with the server's default
// options enables it (it declined before), and the columnar count matches the row query. Guards
// against SchemaScanHotTopN silently regressing to 0 (the state that left the accelerator dark).
func TestColumnarOnByDefault(t *testing.T) {
	if n := defaultMaintainOptions().SchemaScanHotTopN; n <= 0 {
		t.Fatalf("defaultMaintainOptions().SchemaScanHotTopN = %d, want > 0", n)
	}
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir(), SegmentSize: 1 << 9}) // tiny ⇒ segments seal
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	tx := d.Begin()
	for i := 0; i < 1200; i++ {
		ad, _ := classad.ParseOld(fmt.Sprintf("Memory = %d\nCpus = %d\nName = \"n%05d\"", 1024+(i%64)*256, 1+i%8, i))
		tx.NewClassAd(fmt.Sprintf("%d.0", i), ad)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Read demand on Memory so it ranks into the hot tier the schema build picks.
	for i := 0; i < 15; i++ {
		seq, err := d.QueryProject("true", []string{"Memory"})
		if err != nil {
			t.Fatal(err)
		}
		for range seq {
		}
	}
	if _, ok := d.CountConstraint("Memory > 4096"); ok {
		t.Fatal("CountConstraint should decline before Maintain enables the accelerator")
	}
	d.Maintain(defaultMaintainOptions())
	got, ok := d.CountConstraint("Memory > 4096")
	if !ok {
		t.Fatal("CountConstraint fast path NOT enabled after default Maintain (columnar accelerator off)")
	}
	seq, err := d.Query("Memory > 4096")
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for range seq {
		want++
	}
	if got != want {
		t.Fatalf("columnar count %d != row query count %d", got, want)
	}
}

// TestArchiveColumnarOnByDefault guards the archive half of the same decision. classad's
// maintenance leaves ArchiveSchemaScanHotTopN at 0, so if this regressed to 0 here the accelerator
// would silently never build on a history table -- and a history table is where a numeric
// aggregate most needs it. That the option actually causes a build is tested in classad
// (dbrpc.TestArchiveSchemaScanOnWhenConfigured); this pins that we ask for it.
func TestArchiveColumnarOnByDefault(t *testing.T) {
	opts := defaultMaintainOptions()
	if opts.ArchiveSchemaScanHotTopN <= 0 {
		t.Fatalf("defaultMaintainOptions().ArchiveSchemaScanHotTopN = %d, want > 0 so history "+
			"tables build the columnar accelerator", opts.ArchiveSchemaScanHotTopN)
	}
	if opts.ArchiveSchemaScanHotTopN != opts.SchemaScanHotTopN {
		t.Errorf("archive hot-column count %d != mutable %d; the two should stay in step so a "+
			"query behaves the same whichever table type it hits",
			opts.ArchiveSchemaScanHotTopN, opts.SchemaScanHotTopN)
	}
}
