package main

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/htcondordb/repl"
)

// sample mimics `condor_status -any -long`: old-ClassAd blocks separated by blank
// lines, with mixed MyTypes.
const sample = `MyType = "Machine"
Name = "slot1@ep1.example.com"
Cpus = 8
Memory = 16384

MyType = "Job"
Name = "1.0"
Owner = "alice"

MyType = "Machine"
Cpus = 2
`

// fixedTable routes every ad to one table.
func fixedTable(name string) func(*classad.ClassAd) string {
	return func(*classad.ClassAd) string { return name }
}

func TestLoadAds(t *testing.T) {
	got := map[string][]repl.WriteOp{}
	loaded, skipped, perTable, err := loadAds(strings.NewReader(sample), "Name", fixedTable("ads"), 200,
		func(table string, ops []repl.WriteOp) error {
			got[table] = append(got[table], ops...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 2 || skipped != 1 { // the third ad has no Name -> skipped
		t.Fatalf("loaded=%d skipped=%d, want 2 and 1", loaded, skipped)
	}
	if perTable["ads"] != 2 {
		t.Fatalf("perTable = %v, want ads:2", perTable)
	}
	ads := got["ads"]
	if ads[0].Key != "slot1@ep1.example.com" || ads[1].Key != "1.0" {
		t.Fatalf("keys = %q, %q", ads[0].Key, ads[1].Key)
	}
	if !strings.Contains(ads[0].Value, `Cpus = 8`) ||
		!strings.Contains(ads[0].Value, `Key = "slot1@ep1.example.com"`) {
		t.Fatalf("ad text missing content or stamped Key:\n%s", ads[0].Value)
	}
}

// TestLoadAdsAutoRouting checks -auto routing by MyType (Machine -> machines,
// Job -> jobs).
func TestLoadAdsAutoRouting(t *testing.T) {
	route := func(ad *classad.ClassAd) string { return tableForType(ad, "misc") }
	perDest := map[string]int{}
	_, _, perTable, err := loadAds(strings.NewReader(sample), "Name", route, 200,
		func(table string, ops []repl.WriteOp) error {
			perDest[table] += len(ops)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if perTable["machines"] != 1 || perTable["jobs"] != 1 {
		t.Fatalf("perTable = %v, want machines:1 jobs:1", perTable)
	}
}

func TestTableForType(t *testing.T) {
	cases := map[string]string{"Machine": "machines", "Job": "jobs", "Scheduler": "schedulers", "Submitters": "submitters"}
	for mt, want := range cases {
		ad, _ := classad.ParseOld("MyType = " + `"` + mt + `"`)
		if got := tableForType(ad, "misc"); got != want {
			t.Errorf("tableForType(%q) = %q, want %q", mt, got, want)
		}
	}
	// No MyType -> fallback.
	ad, _ := classad.ParseOld("Cpus = 4")
	if got := tableForType(ad, "misc"); got != "misc" {
		t.Errorf("tableForType(no MyType) = %q, want misc", got)
	}
}

// TestLoadAdsBatching confirms batching flushes at the batch size and at EOF.
func TestLoadAdsBatching(t *testing.T) {
	var blocks strings.Builder
	for i := 0; i < 5; i++ {
		blocks.WriteString("MyType = \"Machine\"\nName = \"n")
		blocks.WriteByte(byte('0' + i))
		blocks.WriteString("\"\n\n")
	}
	var flushes int
	loaded, _, _, err := loadAds(strings.NewReader(blocks.String()), "Name", fixedTable("ads"), 2,
		func(table string, ops []repl.WriteOp) error {
			flushes++
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 5 {
		t.Fatalf("loaded = %d, want 5", loaded)
	}
	if flushes != 3 { // 2 + 2 + 1
		t.Fatalf("flushes = %d, want 3", flushes)
	}
}
