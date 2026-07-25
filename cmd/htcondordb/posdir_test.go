package main

import (
	"path/filepath"
	"testing"
)

// TestResolveDBDir checks the database dir resolves from HTCONDORDB_DIR, else $(SPOOL)/htcondordb,
// else empty -- the single source of truth for everything under the DB dir.
func TestResolveDBDir(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"htcondordb_dir wins", "HTCONDORDB_DIR = /var/lib/htcondordb\nSPOOL = /var/spool/condor\n", "/var/lib/htcondordb"},
		{"spool fallback", "SPOOL = /var/spool/condor\n", filepath.Join("/var/spool/condor", "htcondordb")},
		// (the "in-memory" case needs both knobs empty, but the config library injects a
		// default SPOOL, so it is not reachable from a config string here.)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resolveDBDir(mkSyncCfg(t, c.body)); got != c.want {
				t.Errorf("resolveDBDir = %q, want %q", got, c.want)
			}
		})
	}
}

// TestScheddSyncPosDirFollowsSpool is the regression for the bug: with only SPOOL set (the
// common HTCondor deployment), the schedd-sync position store must live under the SPOOL DB
// dir, not be silently disabled because HTCONDORDB_DIR is unset.
func TestScheddSyncPosDirFollowsSpool(t *testing.T) {
	cfg := mkSyncCfg(t, "HTCONDORDB_SYNC_SCHEDD = true\nSPOOL = /var/spool/condor\nHTCONDORDB_HISTORY = /var/spool/condor/history\n")
	s := resolveScheddSyncSettings(cfg)
	want := filepath.Join("/var/spool/condor", "htcondordb")
	if s.posDir != want {
		t.Errorf("posDir = %q, want %q (SPOOL-configured deployments must persist the sync position)", s.posDir, want)
	}
}
