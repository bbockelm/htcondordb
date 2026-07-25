package repl

import (
	"bytes"
	"strings"
	"testing"
)

// TestArchiveDiagMeta verifies .stats / .indexes work on a history archive instead of erroring
// ("no such table"): .stats reports the rich storage/op stats (record count included),
// .indexes explains the archive layout.
func TestArchiveDiagMeta(t *testing.T) {
	e, cleanup := newArchiveExec(t) // seeds a "history" archive with 3 records
	defer cleanup()
	s := &session{exec: e}

	var buf bytes.Buffer
	if !s.runDiagMeta(&buf, ".stats", "history") {
		t.Fatal(".stats not handled")
	}
	out := buf.String()
	if strings.Contains(out, "no such table") {
		t.Fatalf(".stats history errored: %q", out)
	}
	// Rich stats now (not just a row count): record count + storage + op timings.
	if !strings.Contains(out, "records:    3") {
		t.Errorf(".stats history = %q, want a record count of 3", out)
	}
	if !strings.Contains(out, "operational timings") {
		t.Errorf(".stats history missing op timings (impoverished output):\n%s", out)
	}

	buf.Reset()
	if !s.runDiagMeta(&buf, ".indexes", "history") {
		t.Fatal(".indexes not handled")
	}
	out = buf.String()
	if strings.Contains(out, "no such table") || !strings.Contains(out, "archive") {
		t.Errorf(".indexes history = %q, want an archive explanation", out)
	}
}
