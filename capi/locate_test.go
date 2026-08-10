package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/htcondordb/locate"
)

// testConfig writes body to a throwaway condor_config and returns a Config read from it, with
// the same subsystem openConn uses and both address knobs cleared from the environment so a
// developer's own exports cannot change what a test resolves.
func testConfig(t *testing.T, body string) *config.Config {
	t.Helper()
	t.Setenv(locate.AddressFileKnob, "")
	t.Setenv(locate.HostKnob, "")
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

// A caller-supplied address wins outright: nothing about the ambient configuration or the
// environment should be able to redirect a client that named its daemon.
func TestResolveAddrExplicitWins(t *testing.T) {
	cfg := testConfig(t, locate.HostKnob+" = elsewhere.example.edu:9618\n")
	t.Setenv(locate.HostKnob, "also-elsewhere.example.edu:9618")
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

// No address delegates to package locate, which owns the precedence (covered there). These
// two cases confirm the delegation happens at all -- from the configuration and from the
// environment -- and that whitespace counts as "no address".
func TestResolveAddrLocatesTheDaemon(t *testing.T) {
	dir := t.TempDir()
	addrFile := filepath.Join(dir, "address")
	if err := os.WriteFile(addrFile, []byte("<127.0.0.1:12345?sock=htcondordb>\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t, locate.AddressFileKnob+" = "+addrFile+"\n")

	for _, addr := range []string{"", "   "} {
		got, err := resolveAddr(cfg, addr)
		if err != nil {
			t.Fatalf("resolveAddr(%q): %v", addr, err)
		}
		if want := "<127.0.0.1:12345?sock=htcondordb>"; got != want {
			t.Errorf("resolveAddr(%q) = %q, want %q", addr, got, want)
		}
	}

	t.Setenv(locate.HostKnob, "db.example.edu:9618")
	if got, err := resolveAddr(cfg, ""); err != nil || got != "db.example.edu:9618" {
		t.Errorf("resolveAddr(\"\") = %q, %v; want the environment override", got, err)
	}
}

// When nothing points at a daemon the error has to name the knobs; "connection refused"
// against an empty address would send the reader looking for a network problem.
func TestResolveAddrUnlocatable(t *testing.T) {
	_, err := resolveAddr(testConfig(t, "LOG =\n"), "")
	if err == nil {
		t.Fatal("resolveAddr(\"\") succeeded with nothing configured")
	}
	for _, want := range []string{locate.AddressFileKnob, locate.HostKnob} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
