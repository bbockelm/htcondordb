package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbockelm/golang-htcondor/config"
)

// testConfig writes body to a throwaway condor_config and returns a Config read from it,
// with the same subsystem openConn uses.
func testConfig(t *testing.T, body string) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "condor_config")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONDOR_CONFIG", path)
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// A caller-supplied address wins outright: nothing about the ambient configuration should
// be able to redirect a client that named its daemon.
func TestResolveAddrExplicitWins(t *testing.T) {
	cfg := testConfig(t, "HTCONDORDB_HOST = elsewhere.example.edu:9618\n")
	for _, addr := range []string{"host.example.edu:9618", "<1.2.3.4:9618?sock=collector>"} {
		got, err := resolveAddr(cfg, addr)
		if err != nil {
			t.Fatalf("resolveAddr(%q): %v", addr, err)
		}
		if got != addr {
			t.Errorf("resolveAddr(%q) = %q, want it unchanged", addr, got)
		}
	}
}

// The default path -- no address at all -- reads the daemon's address file.
func TestResolveAddrFromAddressFile(t *testing.T) {
	dir := t.TempDir()
	addrFile := filepath.Join(dir, "address")
	// Address files carry a sinful string and often trailing lines; take the first
	// non-empty one, as every other HTCondor client does.
	if err := os.WriteFile(addrFile, []byte("<127.0.0.1:12345?sock=htcondordb>\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t, "HTCONDORDB_ADDRESS_FILE = "+addrFile+"\n")

	got, err := resolveAddr(cfg, "")
	if err != nil {
		t.Fatalf("resolveAddr(\"\"): %v", err)
	}
	if want := "<127.0.0.1:12345?sock=htcondordb>"; got != want {
		t.Errorf("resolveAddr(\"\") = %q, want %q", got, want)
	}

	// Whitespace is not an address: a caller passing "" and a caller passing " " both mean
	// "locate it for me".
	if got, err = resolveAddr(cfg, "   "); err != nil || got != "<127.0.0.1:12345?sock=htcondordb>" {
		t.Errorf("resolveAddr(\"   \") = %q, %v; want the located address", got, err)
	}
}

// $(LOG)/.htcondordb_address is the convention when the knob is not set explicitly, which
// is the case that matters most: an operator who configured nothing but LOG still gets a
// working client.
func TestResolveAddrFromLogDirDefault(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".htcondordb_address"), []byte("127.0.0.1:9999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t, "LOG = "+dir+"\n")

	got, err := resolveAddr(cfg, "")
	if err != nil {
		t.Fatalf("resolveAddr(\"\"): %v", err)
	}
	if got != "127.0.0.1:9999" {
		t.Errorf("resolveAddr(\"\") = %q, want 127.0.0.1:9999", got)
	}
}

// A daemon on another host publishes no local address file, so HTCONDORDB_HOST has to work
// both on its own and as the fallback when a configured address file is absent.
func TestResolveAddrHostFallback(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent")
	for name, body := range map[string]string{
		"host only":           "HTCONDORDB_HOST = db.example.edu:9618\n",
		"file missing":        "HTCONDORDB_ADDRESS_FILE = " + missing + "\nHTCONDORDB_HOST = db.example.edu:9618\n",
		"host needs trimming": "HTCONDORDB_HOST =   db.example.edu:9618  \n",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := resolveAddr(testConfig(t, body), "")
			if err != nil {
				t.Fatalf("resolveAddr(\"\"): %v", err)
			}
			if got != "db.example.edu:9618" {
				t.Errorf("resolveAddr(\"\") = %q, want db.example.edu:9618", got)
			}
		})
	}
}

// When nothing points at a daemon the error has to name the knobs; "connection refused"
// against an empty address would send the reader looking for a network problem.
func TestResolveAddrUnlocatable(t *testing.T) {
	_, err := resolveAddr(testConfig(t, "LOG =\n"), "")
	if err == nil {
		t.Fatal("resolveAddr(\"\") succeeded with nothing configured")
	}
	for _, want := range []string{"HTCONDORDB_ADDRESS_FILE", "HTCONDORDB_HOST"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

// A configured address file that cannot be read, with no host to fall back to, must say so
// -- and say which file -- rather than reporting a generic failure.
func TestResolveAddrUnreadableFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent")
	_, err := resolveAddr(testConfig(t, "HTCONDORDB_ADDRESS_FILE = "+missing+"\n"), "")
	if err == nil {
		t.Fatal("resolveAddr(\"\") succeeded with an unreadable address file")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Errorf("error %q does not name the address file %s", err, missing)
	}
}
