package repl

import (
	"bytes"
	"strings"
	"testing"
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

// TestArchiveHotExplains checks that .hot on an archive says so plainly rather than
// re-printing the index view.
func TestArchiveHotExplains(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	s := &session{exec: e}

	var buf bytes.Buffer
	if !s.runDiagMeta(&buf, ".hot", "history") {
		t.Fatal(".hot not handled")
	}
	if out := buf.String(); !strings.Contains(out, "no hot set") {
		t.Errorf(".hot history = %q, want an explanation that archives have no hot set", out)
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
