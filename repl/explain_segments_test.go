package repl

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestExplainShowsUnindexedSegments: `.explain` must show how many segments an index actually
// covers. An archive index added after its older segments were sealed is backfilled only within
// the backfill horizon; the probe line reports the attribute as indexed either way, and only the
// per-segment breakdown says that most of the archive is still read in full.
func TestExplainShowsUnindexedSegments(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A one-byte horizon carries a new index to the newest segment only.
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		SegmentSize: 1 << 12, ValueAttrs: []string{"ClusterId"}, IndexBackfillBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		ad, _ := classad.Parse(fmt.Sprintf(`[ ClusterId = %d; ProcId = %d; Owner = "u%d"; JobStatus = 4 ]`, i, i%7, i%10))
		if err := arch.Append(ad); err != nil {
			t.Fatal(err)
		}
	}
	arch.Reindex()
	if !arch.AddIndex(nil, []string{"ProcId"}) {
		t.Fatal("AddIndex(ProcId) reported no change")
	}
	srv := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	defer func() { _ = c.Close(); srv.Close(); _ = cat.Close() }()
	s := &session{exec: NewExecutor(c, ExecConfig{}), table: "history"}

	out := runMeta(t, s, ".explain", "ProcId == 3")
	m := regexp.MustCompile(`segments:\s+(\d+) of (\d+) answer from the index, (\d+) ruled out by it, (\d+) unindexed`).FindStringSubmatch(out)
	if m == nil {
		t.Fatalf(".explain printed no segment breakdown:\n%s", out)
	}
	indexed, _ := strconv.Atoi(m[1])
	total, _ := strconv.Atoi(m[2])
	unindexed, _ := strconv.Atoi(m[4])
	if total < 4 || unindexed < total/2 || indexed == 0 {
		t.Errorf("segments: %d indexed, %d unindexed of %d; want most unindexed and some indexed:\n%s",
			indexed, unindexed, total, out)
	}
	for _, want := range []string{"candidates:   ~", "reason:       index probe on ProcId", "have no index on it and are scanned in full"} {
		if !strings.Contains(out, want) {
			t.Errorf(".explain missing %q:\n%s", want, out)
		}
	}

	// A scan plan has no per-segment breakdown, and must not print a line of zeros for one.
	out = runMeta(t, s, ".explain", `Owner == "u3"`)
	if strings.Contains(out, "segments:") {
		t.Errorf("a full-scan plan printed a segment breakdown:\n%s", out)
	}
	if !strings.Contains(out, "reason:       full scan") {
		t.Errorf("a full-scan plan should say why:\n%s", out)
	}
}
