package repl

import (
	"bytes"
	"strings"
	"testing"
)

// TestArchiveDiagMeta verifies .stats / .indexes work on a history archive instead of erroring
// ("no such table"): .stats reports the row count, .indexes explains the fixed archive layout.
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
	if !strings.Contains(out, "rows:   3") {
		t.Errorf(".stats history = %q, want a row count of 3", out)
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
