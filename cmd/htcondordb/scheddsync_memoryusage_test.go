package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
)

// memoryUsageJobAd is a job ad shaped the way MemoryUsage arrives from a schedd. One job in four never
// ran, so it carries neither MemoryUsage nor ResidentSetSize -- which keeps MemoryUsage under the
// presence floor, as on a production history. Of the jobs that ran, nine in ten hold the starter's
// expression and one a literal. Returns the ad text and the value MemoryUsage evaluates to (ok=false
// when it is absent).
func memoryUsageJobAd(i int) (string, int64, bool) {
	var b strings.Builder
	fmt.Fprintf(&b, "ClusterId = %d\nProcId = %d\nOwner = \"user%d\"\nQDate = %d\nJobStatus = 4\n",
		100+i/10, i%10, i%7, 1700000000+i)
	fmt.Fprintf(&b, "CompletionDate = %d\nEnteredHistoryTime = %d\n", 1700001000+i, 1700001005+i)
	if i%4 == 0 {
		return b.String(), 0, false
	}
	rss := int64(1000 + (i*7919)%9000000) // KiB
	fmt.Fprintf(&b, "ResidentSetSize = %d\nImageSize = %d\n", rss, rss+100)
	if i%10 == 3 {
		mu := int64(i % 5000)
		fmt.Fprintf(&b, "MemoryUsage = %d\n", mu)
		return b.String(), mu, true
	}
	b.WriteString("MemoryUsage = ((ResidentSetSize + 1023) / 1024)\n")
	return b.String(), (rss + 1023) / 1024, true
}

// memoryUsageWant is the brute-force answer over the first n ads: MAX(MemoryUsage) and how many have
// MemoryUsage > 4095.
func memoryUsageWant(n int) (maxMU int64, over int) {
	for i := 0; i < n; i++ {
		if _, mu, ok := memoryUsageJobAd(i); ok {
			maxMU = max(maxMU, mu)
			if mu > 4095 {
				over++
			}
		}
	}
	return maxMU, over
}

func memoryUsageField(info collections.SchemaScanInfo) (collections.SchemaScanField, bool) {
	i := slices.IndexFunc(info.Schema, func(f collections.SchemaScanField) bool {
		return strings.EqualFold(f.Name, "MemoryUsage")
	})
	if i < 0 {
		return collections.SchemaScanField{}, false
	}
	return info.Schema[i], true
}

// checkMemoryUsageAggregates asserts MAX(MemoryUsage) and COUNT(*) WHERE MemoryUsage > 4095 equal the
// brute force, and that MAX was answered from the columns rather than by reading records.
func checkMemoryUsageAggregates(t *testing.T, table string, n int,
	agg func(string, []db.AggSpec, *collections.ScanStats) ([]db.AggRow, error)) {
	t.Helper()
	wantMax, wantOver := memoryUsageWant(n)
	var st collections.ScanStats
	rows, err := agg("true", []db.AggSpec{{Func: db.AggMax, Arg: "MemoryUsage"}}, &st)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Values[0] != strconv.FormatInt(wantMax, 10) {
		t.Errorf("%s: MAX(MemoryUsage) = %v, want %d", table, rows, wantMax)
	}
	if st.RecordsReassembled != 0 || (st.Tier != collections.TierColumnar && st.Tier != collections.TierUnreported) {
		t.Errorf("%s: MAX(MemoryUsage) took tier %v and reassembled %d records; want it read from the columns",
			table, st.Tier, st.RecordsReassembled)
	}
	rows, err = agg("MemoryUsage > 4095", []db.AggSpec{{Func: db.AggCount, Arg: "*"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Values[0] != strconv.Itoa(wantOver) {
		t.Errorf("%s: COUNT(*) WHERE MemoryUsage > 4095 = %v, want %d", table, rows, wantOver)
	}
}

// TestDefaultRequireMakesMemoryUsageAColumn is why MemoryUsage is in the default required set: an
// attribute present in three records in four, holding an expression in most of them, still becomes an
// int column with computed cells on every table the schedd-sync writes, and the aggregates over it are
// answered from the columns with the brute-force values. The control arm, the same list without
// MemoryUsage, shows the fixture really is under the presence floor -- without it this would pass on a
// fixture that never needed the pin.
func TestDefaultRequireMakesMemoryUsageAColumn(t *testing.T) {
	s, _ := resolveScheddSyncSettings(mkSyncCfg(t, syncOn))
	withoutMU := func(list string) []string {
		return slices.DeleteFunc(splitAttrList(list), func(n string) bool { return strings.EqualFold(n, "MemoryUsage") })
	}
	const n = 1500

	for _, tc := range []struct {
		table   string
		require string
	}{
		{"history", s.historySchemaRequireAttrs},
		{"epoch_history", s.epochSchemaRequireAttrs},
	} {
		for _, pinned := range []bool{true, false} {
			name := tc.table + "/required"
			require := splitAttrList(tc.require)
			if !pinned {
				name, require = tc.table+"/not-required", withoutMU(tc.require)
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				cat, err := db.OpenCatalog(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = cat.Close() }()
				a, err := cat.CreateArchiveTable(tc.table, db.ArchiveConfig{SegmentSize: 32 << 10, SchemaRequireAttrs: require})
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < n; i++ {
					text, _, _ := memoryUsageJobAd(i)
					if err := a.AppendOld(text); err != nil {
						t.Fatal(err)
					}
				}
				a.BuildAndEnableSchemaScan(4000, 32)
				info := a.SchemaScanInfo()
				if info.CoveredSegments == 0 {
					t.Fatal("no segment was columnarized")
				}
				f, ok := memoryUsageField(info)
				switch {
				case !pinned && ok:
					t.Fatalf("MemoryUsage is a field without being required (%+v); the fixture is not under the "+
						"presence floor, so it cannot show what requiring it buys", f)
				case pinned && !ok:
					t.Fatalf("MemoryUsage is required but not a schema field (skipped: %v)", info.RequireSkipped)
				case pinned && f.Kind != "int":
					t.Errorf("MemoryUsage field kind = %q, want int (the kind its expression computes to)", f.Kind)
				}
				if pinned {
					checkMemoryUsageAggregates(t, tc.table, n, func(c string, aggs []db.AggSpec, st *collections.ScanStats) ([]db.AggRow, error) {
						return a.AggregateColsStats(c, nil, aggs, st)
					})
				}
			})
		}
	}

	// The live jobs table takes the shared list. Most of a queue never ran, which is the case the slot
	// has to be cheap for; it must still be a column, and answer from it.
	t.Run("jobs", func(t *testing.T) {
		t.Parallel()
		d, err := db.OpenConfig(db.Config{Dir: t.TempDir(), SegmentSize: 16 << 10,
			SchemaRequireAttrs: splitAttrList(s.schemaRequireAttrs)})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = d.Close() }()
		tx := d.Begin()
		for i := 0; i < n; i++ {
			text, _, _ := memoryUsageJobAd(i)
			ad, err := classad.ParseOld(text)
			if err != nil {
				t.Fatal(err)
			}
			tx.NewClassAd("j"+strconv.Itoa(i), ad)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if !d.EnableSchemaScan(4000, 32) {
			t.Fatal("jobs: no schema derived")
		}
		if f, ok := memoryUsageField(d.SchemaScanInfo()); !ok || f.Kind != "int" {
			t.Errorf("jobs: MemoryUsage field = %+v (present %v), want an int column", f, ok)
		}
		checkMemoryUsageAggregates(t, "jobs", n, func(c string, aggs []db.AggSpec, st *collections.ScanStats) ([]db.AggRow, error) {
			return d.AggregateColsStats(c, nil, aggs, st, nil)
		})
	})
}
