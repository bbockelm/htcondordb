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

// resolveAddress picks between the three ways an invocation can name a daemon. The
// collector path itself lives in package locate (and is tested there); what matters here is
// which of the three the CLI takes, because taking the wrong one runs the user's query
// against a different database.
func TestResolveAddressPrecedence(t *testing.T) {
	t.Setenv("HTCONDORDB_ADDRESS_FILE", "")
	t.Setenv("HTCONDORDB_HOST", "")

	cfg := config.NewEmpty()
	cfg.Set("HTCONDORDB_HOST", "configured:9618")

	// -addr is the address, and it does not consult the collector or the knobs.
	got, err := resolveAddress(t.Context(), cfg, &flags{addr: "explicit:9618"})
	if err != nil || got != "explicit:9618" {
		t.Fatalf("-addr: got %q, %v", got, err)
	}

	// Nothing given: the daemon this host is configured for.
	got, err = resolveAddress(t.Context(), cfg, &flags{})
	if err != nil || got != "configured:9618" {
		t.Fatalf("no flags: got %q, %v", got, err)
	}

	// -addr with -pool/-name names two daemons at once. Honoring either silently would send
	// the query somewhere the user did not ask for, so it is an error.
	for _, fs := range []*flags{
		{addr: "explicit:9618", pool: "cm.example.com"},
		{addr: "explicit:9618", name: "db.example.com"},
	} {
		if _, err := resolveAddress(t.Context(), cfg, fs); err == nil {
			t.Errorf("-addr with -pool/-name (%+v) should be refused", fs)
		}
	}

	// -pool/-name goes to the collector rather than falling back to HTCONDORDB_HOST: the
	// caller named a pool, so a local knob is not the answer. With no collector reachable
	// this fails, and the failure must be about the collector.
	if _, err := resolveAddress(t.Context(), cfg, &flags{name: "db.example.com"}); err == nil {
		t.Error("-name with no collector configured should fail, not fall back to HTCONDORDB_HOST")
	} else if !strings.Contains(err.Error(), "COLLECTOR_HOST") {
		t.Errorf("error %q should be about the collector", err)
	}
}

func TestParseFlagsPoolAndName(t *testing.T) {
	// parseFlags reads the process arguments; put them back so a later test in this package
	// does not inherit them.
	saved := os.Args
	defer func() { os.Args = saved }()
	os.Args = []string{"htcondordb-cli", "-pool", "cm.example.com", "-name", "db.example.com",
		"-e", "SELECT 1"}
	f := parseFlags()
	if f.pool != "cm.example.com" || f.name != "db.example.com" {
		t.Fatalf("got pool=%q name=%q", f.pool, f.name)
	}
	// The statement must still be the statement -- a flag that swallowed an argument would
	// leave it in args and run "SELECT 1" as a bare statement (or not at all).
	if f.stmt != "SELECT 1" || len(f.args) != 0 {
		t.Fatalf("got stmt=%q args=%v", f.stmt, f.args)
	}
}
