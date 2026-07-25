package repl

import (
	"strings"
	"testing"
)

// TestTruncateMeta exercises the `.truncate` meta-command: argument validation (a table name
// is required and must exist -- so a stray `.truncate` can't wipe data) and the end-to-end
// empty-a-table path against a privileged server. It runs on a mutable table, which the
// server's admin path has always supported; the archive-specific routing is covered by the
// classad dbrpc archive-truncate test.
func TestTruncateMeta(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.0', 'alice')")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('2.0', 'bob')")
	s := &session{exec: e, table: "ads"}

	// No argument: usage, no mutation.
	if out := runMeta(t, s, ".truncate", ""); !strings.Contains(out, "usage:") {
		t.Errorf(".truncate (no arg) = %q, want usage", out)
	}
	// Unknown table: reported, never defaults to the current table.
	if out := runMeta(t, s, ".truncate", "nope"); !strings.Contains(out, "no such table") {
		t.Errorf(".truncate nope = %q, want 'no such table'", out)
	}
	if r := mustExec(t, e, "SELECT COUNT(*) FROM ads"); r.Rows[0][0] != "2" {
		t.Fatalf("validation paths mutated the table: COUNT(*) = %s, want 2", r.Rows[0][0])
	}

	// Real truncate: empties the named table.
	if out := runMeta(t, s, ".truncate", "ads"); strings.Contains(out, "error") {
		t.Fatalf(".truncate ads errored: %q", out)
	}
	if r := mustExec(t, e, "SELECT COUNT(*) FROM ads"); r.Rows[0][0] != "0" {
		t.Errorf("after .truncate ads, COUNT(*) = %s, want 0", r.Rows[0][0])
	}
}
