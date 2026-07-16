//go:build unix

package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"testing"

	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"
)

// TestDaemonDropsPrivilege is the run-as-root integration test for privilege drop: started
// as root (as condor_master starts its daemons), htcondordb's daemon.New must drop its
// effective uid/gid to the condor user. It exercises the REAL daemon.New path htcondordb
// uses in run().
//
// Because a privilege drop is process-global and irreversible-in-place, the test re-execs
// itself as a child (still root) that calls daemon.New and reports its resulting eUID/eGID;
// the parent asserts the child dropped to the target user.
//
// CI MUST run this as root with HTCONDORDB_REQUIRE_ROOT_TEST=1 so that a run which is NOT
// root fails loudly instead of silently skipping (the whole point of the test). Locally, a
// non-root run skips. Set HTCONDORDB_DROPPRIV_USER to the target user (default "nobody";
// CI sets it to "condor").
func TestDaemonDropsPrivilege(t *testing.T) {
	if os.Getenv("HTCONDORDB_DROPPRIV_CHILD") == "1" {
		dropPrivChild()
		return
	}

	requireRoot := os.Getenv("HTCONDORDB_REQUIRE_ROOT_TEST") != ""
	if os.Geteuid() != 0 {
		if requireRoot {
			t.Fatal("HTCONDORDB_REQUIRE_ROOT_TEST is set but the test is not running as root; " +
				"the drop-privilege integration test MUST run as root in CI (do not let it skip)")
		}
		t.Skip("drop-privilege test needs root; CI sets HTCONDORDB_REQUIRE_ROOT_TEST=1 to make skipping fatal")
	}

	targetUser := os.Getenv("HTCONDORDB_DROPPRIV_USER")
	if targetUser == "" {
		targetUser = "nobody"
	}
	u, err := user.Lookup(targetUser)
	if err != nil {
		if requireRoot {
			t.Fatalf("target user %q not found (CI must create it): %v", targetUser, err)
		}
		t.Skipf("target user %q not found: %v", targetUser, err)
	}

	// Re-exec this test binary as the child (still root), configured to drop to u.
	cmd := exec.Command(os.Args[0], "-test.run=^TestDaemonDropsPrivilege$", "-test.v")
	cmd.Env = append(os.Environ(),
		"HTCONDORDB_DROPPRIV_CHILD=1",
		"_CONDOR_CONDOR_IDS="+u.Uid+"."+u.Gid,
		"_CONDOR_DROP_PRIVILEGES=true",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child process failed: %v\n%s", err, out)
	}
	euid, egid, ok := parseDropped(string(out))
	if !ok {
		t.Fatalf("child did not report a drop; output:\n%s", out)
	}
	if euid == 0 || egid == 0 {
		t.Errorf("still privileged after drop: euid=%d egid=%d", euid, egid)
	}
	if strconv.Itoa(euid) != u.Uid {
		t.Errorf("dropped euid = %d, want %s (user %q)", euid, u.Uid, targetUser)
	}
	if strconv.Itoa(egid) != u.Gid {
		t.Errorf("dropped egid = %d, want %s", egid, u.Gid)
	}
}

// dropPrivChild runs in the re-exec'd child: it builds the daemon exactly as run() does
// (daemon.New drops privilege), then prints its effective uid/gid for the parent to check.
func dropPrivChild() {
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "HTCONDORDB"})
	if err != nil {
		fmt.Printf("CHILD_ERROR config: %v\n", err)
		os.Exit(2)
	}
	// A stderr logger so the drop path needs no LOG directory (the drop itself happens
	// inside daemon.New before any owned file is opened).
	logger, err := logging.New(&logging.Config{OutputPath: "stderr", SkipGlobalInstall: true})
	if err != nil {
		fmt.Printf("CHILD_ERROR logger: %v\n", err)
		os.Exit(2)
	}
	if _, err := daemon.New(daemon.Options{Subsys: "HTCONDORDB", Config: cfg, Logger: logger}); err != nil {
		fmt.Printf("CHILD_ERROR daemon.New: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("DROPPED euid=%d egid=%d\n", os.Geteuid(), os.Getegid())
	os.Exit(0)
}

// parseDropped extracts euid/egid from the child's "DROPPED euid=N egid=M" line.
func parseDropped(out string) (euid, egid int, ok bool) {
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "DROPPED ") {
			continue
		}
		var e, g int
		if _, err := fmt.Sscanf(line, "DROPPED euid=%d egid=%d", &e, &g); err == nil {
			return e, g, true
		}
	}
	return 0, 0, false
}
