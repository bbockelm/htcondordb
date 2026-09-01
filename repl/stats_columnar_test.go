package repl

import (
	"bytes"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

func renderStats(t *testing.T, d *dbrpc.Diagnostics) string {
	t.Helper()
	s := &session{}
	var buf bytes.Buffer
	s.showStats(&buf, d)
	return buf.String()
}

// TestShowStatsColumnar checks .stats renders the columnar accelerator's state: on (with coverage and hot
// columns) and off.
//
// It used to assert the line was OMITTED for an archive. That was true of the renderer, never of the
// storage: an archive carries columnar blocks like any other table, so the omission told the operator
// history had no accelerator. The archive case is now asserted the other way.
func TestShowStatsColumnar(t *testing.T) {
	on := renderStats(t, &dbrpc.Diagnostics{SchemaScan: db.SchemaScanInfo{
		Enabled: true, HotFields: []string{"Memory", "Cpus"}, SchemaFields: 40, SealedSegments: 10, CoveredSegments: 10,
	}})
	if !strings.Contains(on, "columnar:   on") || !strings.Contains(on, "10/10") ||
		!strings.Contains(on, "40 schema fields") || !strings.Contains(on, "Memory, Cpus") {
		t.Fatalf("enabled render missing detail:\n%s", on)
	}
	if off := renderStats(t, &dbrpc.Diagnostics{}); !strings.Contains(off, "columnar:   off") {
		t.Fatalf("disabled render:\n%s", off)
	}
	arch := renderStats(t, &dbrpc.Diagnostics{Archive: true, SchemaScan: db.SchemaScanInfo{
		Enabled: true, HotFields: []string{"JobStatus"}, SchemaFields: 82, SealedSegments: 74, CoveredSegments: 74,
	}})
	if !strings.Contains(arch, "columnar:   on") || !strings.Contains(arch, "74/74") {
		t.Fatalf("an archive's columnar block must be reported, not hidden:\n%s", arch)
	}
}

// statsLabel matches the "label:" at the start of a rendered line, which is the unit of comparison here:
// two tables of the same engine should answer the same QUESTIONS, whatever the values.
var statsLabel = regexp.MustCompile(`(?m)^([a-z][a-z ()]*):`)

// kindOnlyLabels are the labels that legitimately appear for one kind. Everything else must appear for
// both, or the two reports have drifted again.
var kindOnlyLabels = map[string]string{
	"retention": "rotation bounds; a mutable table does not rotate",
	"zone maps": "zone maps are an append-only construct",
}

// TestShowStatsLabelsMatchAcrossKinds is the regression test for the divergence itself: an operator
// comparing `.stats jobs` with `.stats history` was reading two different reports of one engine -- a
// different count label, sidecar and disk lines on one side only, the columnar section on the other.
//
// It compares LABELS from the same Diagnostics rendered both ways, so a future line added to one branch
// fails here instead of being noticed by a human reading two screens side by side.
func TestShowStatsLabelsMatchAcrossKinds(t *testing.T) {
	// One payload, rendered as each kind. Populated across the board: a zero-valued field whose line is
	// conditional would otherwise be absent from BOTH and prove nothing.
	base := dbrpc.Diagnostics{
		Stats:             db.Stats{Ads: 4652, Segments: 115, ArenaBytes: 611 << 20, UsedBytes: 130 << 20, DeadBytes: 119 << 20},
		Codec:             db.CodecStats{Codec: "zstd+dict", Ratio: 4.24, SampleRecords: 2000, DictBytes: 1 << 16},
		SidecarSizes:      db.SidecarSizes{MappedBytes: 6 << 20},
		EncryptionEnabled: true,
		EncryptedAttrs:    []string{"Owner"},
		SchemaScan: db.SchemaScanInfo{
			Enabled: true, HotFields: []string{"JobStatus"}, SchemaFields: 82, SealedSegments: 99, CoveredSegments: 99,
		},
		ZoneAttrs: []string{"CompletionDate"},
	}
	mutable := base
	archive := base
	archive.Archive = true
	ret := db.Retention{MaxSegments: 1000}
	archive.Retention = &ret

	labels := func(out string) map[string]bool {
		got := map[string]bool{}
		for _, m := range statsLabel.FindAllStringSubmatch(out, -1) {
			got[m[1]] = true
		}
		return got
	}
	mOut, aOut := renderStats(t, &mutable), renderStats(t, &archive)
	mLabels, aLabels := labels(mOut), labels(aOut)
	if len(mLabels) < 8 {
		t.Fatalf("only %d labels parsed from the mutable render; the comparison below would be "+
			"vacuous:\n%s", len(mLabels), mOut)
	}

	var missing []string
	for l := range mLabels {
		if !aLabels[l] && kindOnlyLabels[l] == "" {
			missing = append(missing, "archive lacks "+l)
		}
	}
	for l := range aLabels {
		if !mLabels[l] && kindOnlyLabels[l] == "" {
			missing = append(missing, "mutable lacks "+l)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("the two kinds report different things: %s\n\nmutable:\n%s\narchive:\n%s",
			strings.Join(missing, "; "), mOut, aOut)
	}
	// And the kind is stated, since the labels are now identical: without this line the two reports are
	// indistinguishable, which is the opposite failure.
	if !strings.Contains(mOut, "kind:       mutable table") ||
		!strings.Contains(aOut, "kind:       append-only history archive") {
		t.Error("each report must name its table kind, now that the rest of the lines match")
	}
}

// TestShowStatsReportsEncryption checks the encryption line is stated plainly for both kinds: an
// unsealed archive reports "off", an encrypted table reports "on".
func TestShowStatsReportsEncryption(t *testing.T) {
	out := renderStats(t, &dbrpc.Diagnostics{Archive: true})
	if !strings.Contains(out, "encrypted:  off") {
		t.Errorf("an unencrypted archive must report encrypted: off:\n%s", out)
	}
	on := renderStats(t, &dbrpc.Diagnostics{EncryptionEnabled: true})
	if !strings.Contains(on, "encrypted:  on") {
		t.Errorf("an encrypted table must report it:\n%s", on)
	}
}
