package repl

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// runMeta runs a meta-command and returns its console output, failing if it was not handled.
func runMeta(t *testing.T, s *session, cmd, arg string) string {
	t.Helper()
	var buf bytes.Buffer
	if !s.runDiagMeta(&buf, cmd, arg) {
		t.Fatalf("%s not handled", cmd)
	}
	return buf.String()
}

// TestArchiveMaintenanceMeta verifies the maintenance meta-commands target a history archive
// by an explicit [table] argument -- routing through the archive admin path rather than
// erroring "no such table" -- while the session's current table is a different (mutable) one.
func TestArchiveMaintenanceMeta(t *testing.T) {
	e, cleanup := newArchiveExec(t) // "history" archive (3 rows) + "jobs" mutable table
	defer cleanup()
	s := &session{exec: e, table: "jobs"} // current table is NOT history

	// .reindex history
	out := runMeta(t, s, ".reindex", "history")
	if strings.Contains(out, "no such table") || strings.Contains(out, "error") {
		t.Errorf(".reindex history: %q", out)
	}
	// .addindex history value Owner
	out = runMeta(t, s, ".addindex", "history value Owner")
	if strings.Contains(out, "no such table") || strings.Contains(out, "error") {
		t.Errorf(".addindex history value Owner: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "value index") {
		t.Errorf(".addindex history did not report a value index: %q", out)
	}
	// .dropindex history Owner
	out = runMeta(t, s, ".dropindex", "history Owner")
	if strings.Contains(out, "no such table") || strings.Contains(out, "error") {
		t.Errorf(".dropindex history Owner: %q", out)
	}
	// .rewrite history
	out = runMeta(t, s, ".rewrite", "history")
	if strings.Contains(out, "no such table") || strings.Contains(out, "error") {
		t.Errorf(".rewrite history: %q", out)
	}
	if !strings.Contains(out, "record") {
		t.Errorf(".rewrite history did not report records rewritten: %q", out)
	}

	// The archive is still queryable after maintenance.
	rows := mustExec(t, e, "SELECT ClusterId FROM history").Rows
	if len(rows) != 3 {
		t.Errorf("history has %d rows after maintenance, want 3", len(rows))
	}
}

// TestArchiveRetrainMeta verifies `.retrain history` retrains the archive dictionary through
// the REPL. It needs a corpus large enough to train a ZSTD dictionary.
func TestArchiveRetrainMeta(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		ad, _ := classad.Parse(fmt.Sprintf(
			`[ ClusterId = %d; Owner = "user_%d"; Cmd = "/usr/bin/long_repeated_command_path"; JobStatus = 4 ]`,
			i, i%20))
		if err := arch.Append(ad); err != nil {
			t.Fatal(err)
		}
	}
	srv := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	defer func() { c.Close(); srv.Close(); cat.Close() }()
	e := NewExecutor(c, ExecConfig{})
	s := &session{exec: e, table: "history"} // current table is the archive; bare .retrain

	out := runMeta(t, s, ".retrain", "")
	if strings.Contains(out, "no such table") || strings.Contains(strings.ToLower(out), "error") {
		t.Fatalf(".retrain history: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "dictionary") {
		t.Errorf(".retrain did not report a retrained dictionary: %q", out)
	}
	// Data intact after retrain.
	rows := mustExec(t, e, "SELECT ClusterId FROM history WHERE ClusterId >= 200").Rows
	if len(rows) != 200 {
		t.Errorf("ClusterId>=200 after retrain = %d rows, want 200", len(rows))
	}
}
