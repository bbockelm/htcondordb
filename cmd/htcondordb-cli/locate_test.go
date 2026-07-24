package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bbockelm/golang-htcondor/config"
)

// locateDaemon delegates to htcondor.LocalDaemonAddress (whose file-parsing and
// re-read behavior is covered in golang-htcondor's locate_test.go); this just
// confirms the CLI wires up the HTCONDORDB subsystem with the expected precedence.
func TestLocateDaemon(t *testing.T) {
	dir := t.TempDir()
	af := filepath.Join(dir, ".htcondordb_address")
	if err := os.WriteFile(af, []byte("<127.0.0.1:9618?sock=abc>\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := config.NewEmpty()
	cfg.Set("HTCONDORDB_ADDRESS_FILE", af)
	cfg.Set("HTCONDORDB_HOST", "host-fallback:9618")

	// File present -> preferred over the host knob.
	if got, err := locateDaemon(cfg); err != nil || got != "<127.0.0.1:9618?sock=abc>" {
		t.Fatalf("file present: got %q, %v", got, err)
	}

	// File gone -> fall back to HTCONDORDB_HOST.
	_ = os.Remove(af)
	if got, err := locateDaemon(cfg); err != nil || got != "host-fallback:9618" {
		t.Fatalf("host fallback: got %q, %v", got, err)
	}
}
