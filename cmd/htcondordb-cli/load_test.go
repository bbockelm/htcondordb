package main

import (
	"strings"
	"testing"

	"github.com/bbockelm/htcondordb/repl"
)

// sample mimics `condor_status -long`: old-ClassAd blocks separated by blank lines.
const sample = `MyType = "Machine"
Name = "slot1@ep1.example.com"
Cpus = 8
Memory = 16384

MyType = "Machine"
Name = "slot2@ep1.example.com"
Cpus = 4
Memory = 8192

MyType = "Machine"
Cpus = 2
`

func TestLoadAds(t *testing.T) {
	var got []repl.WriteOp
	loaded, skipped, err := loadAds(strings.NewReader(sample), "Name", 200, func(ops []repl.WriteOp) error {
		got = append(got, ops...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 2 || skipped != 1 { // the third ad has no Name -> skipped
		t.Fatalf("loaded=%d skipped=%d, want 2 and 1", loaded, skipped)
	}
	if got[0].Key != "slot1@ep1.example.com" || got[1].Key != "slot2@ep1.example.com" {
		t.Fatalf("keys = %q, %q", got[0].Key, got[1].Key)
	}
	// Each op is a NewClassAd carrying the original attributes plus a stamped Key.
	if got[0].Kind != repl.WNewClassAd {
		t.Fatalf("op kind = %v", got[0].Kind)
	}
	if !strings.Contains(got[0].Value, `Cpus = 8`) ||
		!strings.Contains(got[0].Value, `Key = "slot1@ep1.example.com"`) {
		t.Fatalf("ad text missing content or stamped Key:\n%s", got[0].Value)
	}
}

// TestLoadAdsBatching confirms batching flushes at the batch size and again at EOF.
func TestLoadAdsBatching(t *testing.T) {
	var blocks strings.Builder
	for i := 0; i < 5; i++ {
		blocks.WriteString("MyType = \"Machine\"\nName = \"n")
		blocks.WriteByte(byte('0' + i))
		blocks.WriteString("\"\n\n")
	}
	var flushes int
	loaded, _, err := loadAds(strings.NewReader(blocks.String()), "Name", 2, func(ops []repl.WriteOp) error {
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
