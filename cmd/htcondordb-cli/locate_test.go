package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbockelm/golang-htcondor/config"
)

// locateDaemon delegates to package locate (whose precedence and environment overrides are
// covered in locate_test.go there); this just confirms the CLI wires it up.
func TestLocateDaemon(t *testing.T) {
	// Clear the overrides: a developer with either exported must not change what this
	// resolves.
	t.Setenv("HTCONDORDB_ADDRESS_FILE", "")
	t.Setenv("HTCONDORDB_HOST", "")

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

	// The environment redirects the CLI too, and the failure names -addr.
	t.Setenv("HTCONDORDB_HOST", "from-env:9618")
	if got, err := locateDaemon(cfg); err != nil || got != "from-env:9618" {
		t.Fatalf("environment override: got %q, %v", got, err)
	}
	t.Setenv("HTCONDORDB_HOST", "")
	cfg.Set("HTCONDORDB_HOST", "")
	if _, err := locateDaemon(config.NewEmpty()); err == nil {
		t.Fatal("locateDaemon succeeded with nothing configured")
	} else if !strings.Contains(err.Error(), "-addr") {
		t.Errorf("error %q does not mention -addr", err)
	}
}
