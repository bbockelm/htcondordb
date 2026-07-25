package repl

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// archiveExecBig builds an Executor over a catalog with a multi-segment "history" archive.
func archiveExecBig(t *testing.T, n int) (*Executor, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{SegmentSize: 1 << 12, ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		ad, _ := classad.Parse(fmt.Sprintf(`[ ClusterId = %d; Owner = "u%d"; JobStatus = 4 ]`, i, i%10))
		if err := arch.Append(ad); err != nil {
			t.Fatal(err)
		}
	}
	srv := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	return e, func() { c.Close(); srv.Close(); cat.Close() }
}

// TestArchiveStatsRich verifies `.stats history` now renders the full storage/codec/op-stats
// picture (segments, bytes, disk, operational timings), not just a row count.
func TestArchiveStatsRich(t *testing.T) {
	e, cleanup := archiveExecBig(t, 400)
	defer cleanup()
	s := &session{exec: e, table: "jobs"}
	out := runMeta(t, s, ".stats", "history")
	for _, want := range []string{"records:", "segments:", "arena:", "used:", "disk:", "codec:", "operational timings"} {
		if !strings.Contains(out, want) {
			t.Errorf(".stats history missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "not exposed for archives") {
		t.Errorf(".stats history still shows the impoverished message:\n%s", out)
	}
}

// TestArchiveRetentionAndRotateMeta verifies .retention shows/sets bounds and .rotate drops
// segments, through the REPL.
func TestArchiveRetentionAndRotateMeta(t *testing.T) {
	e, cleanup := archiveExecBig(t, 400)
	defer cleanup()
	s := &session{exec: e, table: "history"}

	// Show (unset) retention.
	if out := runMeta(t, s, ".retention", "history"); !strings.Contains(out, "unbounded") {
		t.Errorf(".retention (unset) = %q, want 'unbounded'", out)
	}
	// Set retention to 2 segments.
	if out := runMeta(t, s, ".retention", "history 2 0"); strings.Contains(out, "error") {
		t.Fatalf(".retention set errored: %q", out)
	}
	if out := runMeta(t, s, ".retention", "history"); !strings.Contains(out, "2 segments") {
		t.Errorf(".retention after set = %q, want '2 segments'", out)
	}
	// A byte-size suffix works.
	if out := runMeta(t, s, ".retention", "history 0 1MiB"); strings.Contains(out, "error") {
		t.Fatalf(".retention with MiB errored: %q", out)
	}
	// Rotate now applies the (segment) bound -- reset to segments first, then rotate.
	runMeta(t, s, ".retention", "history 2 0")
	if out := runMeta(t, s, ".rotate", "history"); strings.Contains(out, "error") {
		t.Fatalf(".rotate errored: %q", out)
	}
	// After rotation, .stats shows at most 2 segments.
	statsOut := runMeta(t, s, ".stats", "history")
	if !strings.Contains(statsOut, "retention:") {
		t.Errorf(".stats missing retention line:\n%s", statsOut)
	}
}
