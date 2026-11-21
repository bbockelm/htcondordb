package federate

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// BenchmarkDedupProbe times the exact query archiveSink.countHeld makes per record of a Reset
// replay it cannot hold in memory, over the hub's own index set (federate/tables.go), for the two
// identity shapes that matter:
//
//   - history and epoch_history lead with GlobalJobId, which archiveCategorical indexes;
//   - job_metrics leads with Key, which nothing indexes. ScheddName is indexed but has one value
//     per AP, so narrowing to the AP still leaves every one of its samples to scan.
//
// The arms show what that difference costs, and what indexing Key would buy.
func BenchmarkDedupProbe(b *testing.B) {
	// A small segment so the archive has many of them, as a production one does.
	const records, seg = 50000, 256 << 10
	hubCats := []string{ScheddNameAttr, "Owner", "User", "GlobalJobId"}

	record := func(i int) string {
		return fmt.Sprintf("ScheddName = \"ap1.example.org\"\nKey = \"%d.0\"\nGlobalJobId = \"ap1.example.org#%d.0#1700000000\"\n"+
			"RunInstanceID = 1\nSampleTime = %d\nSampleTrigger = \"periodic\"\nLogSeq = 3\nLogOffset = %d\n"+
			"ClusterId = %d\nProcId = 0\nOwner = \"alice\"", i, i, 1_700_000_000+i, i*512, i)
	}
	histProbe := func(i int) string {
		return fmt.Sprintf(`ScheddName == "ap1.example.org" && GlobalJobId == "ap1.example.org#%d.0#1700000000"`, i)
	}
	metricsProbe := func(i int) string {
		return fmt.Sprintf(`ScheddName == "ap1.example.org" && Key == "%d.0" && RunInstanceID == 1 && SampleTime == %d`+
			` && SampleTrigger == "periodic" && LogSeq == 3 && LogOffset == %d`, i, 1_700_000_000+i, i*512)
	}

	for _, tc := range []struct {
		name  string
		cats  []string
		probe func(int) string
	}{
		{"history/GlobalJobId-indexed", hubCats, histProbe},
		{"job_metrics/Key-unindexed", hubCats, metricsProbe},
		{"job_metrics/Key-indexed", append(append([]string{}, hubCats...), "Key"), metricsProbe},
	} {
		b.Run(tc.name, func(b *testing.B) {
			cat, err := db.OpenCatalog(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer cat.Close()
			a, err := cat.CreateArchiveTable("t", db.ArchiveConfig{
				SegmentSize: seg, CategoricalAttrs: tc.cats, ValueAttrs: []string{"ClusterId"}, ZoneAttrs: []string{"SampleTime"},
			})
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < records; i++ {
				if err := a.AppendOld(record(i)); err != nil {
					b.Fatal(err)
				}
			}
			if err := a.Flush(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seq, err := a.Query(tc.probe(i % records))
				if err != nil {
					b.Fatal(err)
				}
				n := 0
				for range seq {
					n++
				}
				if n != 1 {
					b.Fatalf("probe matched %d records, want 1", n)
				}
			}
		})
	}
}
