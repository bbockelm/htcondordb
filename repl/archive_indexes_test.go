package repl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestArchiveIndexesEnumerated is the regression for ".indexes history" answering with a
// stock "not enumerable over the wire" apology: an archive's index set IS readable over
// opDiag, so the command must name the actual configured and zone-mapped attributes.
func TestArchiveIndexesEnumerated(t *testing.T) {
	e, cleanup := newArchiveExec(t) // "history": value index ClusterId, zone attr CompletionDate
	defer cleanup()
	s := &session{exec: e}

	var buf bytes.Buffer
	if !s.runDiagMeta(&buf, ".indexes", "history") {
		t.Fatal(".indexes not handled")
	}
	out := buf.String()
	if strings.Contains(out, "not enumerable") {
		t.Fatalf(".indexes history still refuses to enumerate:\n%s", out)
	}
	for _, want := range []string{
		"append-only history archive",
		"ClusterId",      // the configured value index
		"CompletionDate", // the zone-mapped attribute
	} {
		if !strings.Contains(out, want) {
			t.Errorf(".indexes history missing %q:\n%s", want, out)
		}
	}
	// Value-indexed attributes are zone-mapped automatically, so ClusterId belongs on the
	// zone line too -- that is what tells an operator a range query on it prunes segments.
	zoneLine := lineContaining(out, "zone-mapped")
	if !strings.Contains(zoneLine, "ClusterId") || !strings.Contains(zoneLine, "CompletionDate") {
		t.Errorf("zone-mapped line = %q, want both ClusterId and CompletionDate", zoneLine)
	}
}

// TestArchiveAddIndexReported checks the write half: adding an index to an archive over the
// wire succeeds and the subsequent .indexes reflects it.
func TestArchiveAddIndexReported(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	s := &session{exec: e, table: "jobs"}

	var buf bytes.Buffer
	s.runDiagMeta(&buf, ".addindex", "history categorical Owner")
	if out := buf.String(); strings.Contains(out, "error") {
		t.Fatalf(".addindex history categorical Owner: %s", out)
	}

	buf.Reset()
	if !s.runDiagMeta(&buf, ".indexes", "history") {
		t.Fatal(".indexes not handled")
	}
	out := buf.String()
	catLine := lineContaining(out, "categorical")
	if !strings.Contains(catLine, "Owner") {
		t.Errorf("after .addindex, categorical line = %q, want Owner:\n%s", catLine, out)
	}
}

// TestArchiveHotShowsTheSet: an archive HAS a hot set -- it is reported over opDiag like any
// other table's, and it can now be refreshed from query demand. .hot used to answer "it has
// no hot set", which sent an operator looking for a knob that was there all along.
//
// The output must also say how fast a change spreads, because the hot header is per record:
// a refreshed set governs records appended from then on, and already-sealed ones pick it up
// only as maintenance merges their segments. Without those lines, someone who runs
// .refreshhot and then times an old query cannot tell a working feature from a broken one.
func TestArchiveHotShowsTheSet(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	s := &session{exec: e}

	var buf bytes.Buffer
	if !s.runDiagMeta(&buf, ".hot", "history") {
		t.Fatal(".hot not handled")
	}
	out := buf.String()
	if strings.Contains(out, "no hot set") {
		t.Errorf(".hot history still claims an archive has no hot set:\n%s", out)
	}
	if !strings.Contains(out, "hot attributes:") {
		t.Errorf(".hot history = %q, want the archive's hot attributes", out)
	}
	if !strings.Contains(out, "appended from now on") || !strings.Contains(out, "sealed segments") ||
		!strings.Contains(out, "merges them") {
		t.Errorf(".hot history = %q, want it to say how far and how fast the set reaches", out)
	}
}

// TestArchiveRefreshHotAccepted: .refreshhot reaches an archive. The admin path used to
// reject the hot verbs on an archive outright, so the command failed rather than tuning.
func TestArchiveRefreshHotAccepted(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	s := &session{exec: e, table: "history"}

	var buf bytes.Buffer
	s.runDiagMeta(&buf, ".refreshhot", "")
	out := buf.String()
	if strings.Contains(out, "error") || strings.Contains(out, "unknown admin action") {
		t.Fatalf(".refreshhot on an archive: %s", out)
	}
	if !strings.Contains(out, "refreshed hot set") {
		t.Errorf(".refreshhot history = %q, want it to report the refreshed set", out)
	}
}

func lineContaining(out, substr string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, substr) {
			return line
		}
	}
	return ""
}

// TestArchiveIndexesShowBackfill: segments that predate an index are reported with what will
// cover them, so an operator looking at a slow query on a new index can see why and what to
// run, rather than concluding the index does not work.
func TestArchiveIndexesShowBackfill(t *testing.T) {
	s := &session{}
	var buf bytes.Buffer
	s.showArchiveIndexes(&buf, &dbrpc.Diagnostics{
		Archive: true, ValueIndexes: []string{"EnteredHistoryTime"},
		SealedSegments: 793, BackfillPendingSegments: 658, BackfillPendingBytes: 5 << 30,
	})
	line := lineContaining(buf.String(), "index backfill")
	for _, want := range []string{"658", "5.0 GiB", ".reindex"} {
		if !strings.Contains(line, want) {
			t.Errorf("backfill line %q missing %q", line, want)
		}
	}

	buf.Reset()
	s.showArchiveIndexes(&buf, &dbrpc.Diagnostics{Archive: true, SealedSegments: 793})
	if strings.Contains(buf.String(), "index backfill") {
		t.Errorf("nothing pending, yet a backfill line:\n%s", buf.String())
	}
}
