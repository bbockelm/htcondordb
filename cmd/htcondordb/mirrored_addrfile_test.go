package main

import (
	"os"
	"path/filepath"
	"testing"
)

// scheddAddressFile writes an address file the way a 25.x schedd does (daemon_core_main.cpp
// drop_addr_file): address, version, platform, then the schedd's ContactInfoExtra ad.
func scheddAddressFile(t *testing.T, dir, name, extra string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	text := "<10.0.0.1:9618?addrs=10.0.0.1-9618>\n$CondorVersion: 25.0.0 2025-01-01 $\n$CondorPlatform: x86_64_AlmaLinux9 $\n" + extra
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestMirroredNameFromAddressFile: the schedd states its own Name in its address file (HTCondor
// 25.x+), so a spoke uses that before deriving one from configuration -- the derivation cannot see a
// second schedd's local-name configuration (SCHEDD.SCHEDD2.*), and is a guess wherever the schedd
// was started with -name. HTCONDORDB_MIRRORED_SCHEDD_ADDRESS_FILE points at a schedd other than the
// primary one; HTCONDORDB_MIRRORED_SCHEDD_NAME still wins outright.
func TestMirroredNameFromAddressFile(t *testing.T) {
	dir := t.TempDir()
	primary := scheddAddressFile(t, dir, ".schedd_address", "Name = \"ap1.example.org\"\nMachine = \"ap1.example.org\"\n")
	second := scheddAddressFile(t, dir, ".schedd2_address",
		"CreddIpAddr = \"<10.0.0.1:9620>\"\nName = \"schedd2@ap1.example.org\"\nMachine = \"ap1.example.org\"\nSubmitAlwaysCheckCreds = false\n")
	base := "FULL_HOSTNAME = ap1.example.org\nHOSTNAME = ap1\nSCHEDD_ADDRESS_FILE = " + primary + "\n"

	m := resolveMirroredSchedd(mkSyncCfg(t, base+"HTCONDORDB_MIRRORED_SCHEDD_ADDRESS_FILE = "+second+"\nSCHEDD2.SCHEDD_NAME = schedd2\n"))
	if name, addr := m.current(); name != "schedd2@ap1.example.org" || addr != "<10.0.0.1:9618?addrs=10.0.0.1-9618>" {
		t.Errorf("second schedd: current() = %q, %q; want its own Name and address", name, addr)
	}
	if m.addrFile != second {
		t.Errorf("addrFile = %q, want the configured one", m.addrFile)
	}

	// A pre-25 address file (three lines) falls back to the derivation.
	old := scheddAddressFile(t, dir, ".old_address", "")
	m = resolveMirroredSchedd(mkSyncCfg(t, base+"HTCONDORDB_MIRRORED_SCHEDD_ADDRESS_FILE = "+old+"\nSCHEDD_NAME = s3\n"))
	if name, _ := m.current(); name != "s3@ap1.example.org" {
		t.Errorf("pre-25 address file: name = %q, want the derived s3@ap1.example.org", name)
	}

	// The explicit name wins over the file.
	m = resolveMirroredSchedd(mkSyncCfg(t, base+"HTCONDORDB_MIRRORED_SCHEDD_ADDRESS_FILE = "+second+"\nHTCONDORDB_MIRRORED_SCHEDD_NAME = pinned@ap7\n"))
	if name, _ := m.current(); name != "pinned@ap7" {
		t.Errorf("override: name = %q", name)
	}

	// No FULL_HOSTNAME and nothing else to go on: no name, rather than "user@".
	m = resolveMirroredSchedd(mkSyncCfg(t, "FULL_HOSTNAME =\n"))
	if name, _ := m.current(); name != "" {
		t.Errorf("no hostname: name = %q, want none", name)
	}
}
