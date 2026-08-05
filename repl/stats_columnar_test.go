package repl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestShowStatsColumnar checks .stats renders the columnar accelerator's state: on (with coverage
// + hot columns), off, and omitted for an archive.
func TestShowStatsColumnar(t *testing.T) {
	s := &session{}
	render := func(d *dbrpc.Diagnostics) string {
		var buf bytes.Buffer
		s.showStats(&buf, d)
		return buf.String()
	}
	on := render(&dbrpc.Diagnostics{SchemaScan: db.SchemaScanInfo{
		Enabled: true, HotFields: []string{"Memory", "Cpus"}, SchemaFields: 40, SealedSegments: 10, CoveredSegments: 10,
	}})
	if !strings.Contains(on, "columnar:   on") || !strings.Contains(on, "10/10") ||
		!strings.Contains(on, "40 schema fields") || !strings.Contains(on, "Memory, Cpus") {
		t.Fatalf("enabled render missing detail:\n%s", on)
	}
	off := render(&dbrpc.Diagnostics{})
	if !strings.Contains(off, "columnar:   off") {
		t.Fatalf("disabled render:\n%s", off)
	}
	arch := render(&dbrpc.Diagnostics{Archive: true})
	if strings.Contains(arch, "columnar:") {
		t.Fatalf("archive should not show a columnar line:\n%s", arch)
	}
}
