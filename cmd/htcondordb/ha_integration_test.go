//go:build unix

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/ha/consistent"
)

// This is the realistic HA integration test: it compiles the actual htcondordb binary once,
// starts a leader and a follower as separate processes with real HTCondor configuration and
// FS authentication (same-user, no shared secret), and verifies that a write to the leader
// replicates to the follower over a real authenticated CEDAR connection.

var (
	dbBuildOnce sync.Once
	dbBinPath   string
	dbBuildErr  error
)

// htcondordbBinary builds the daemon once (per the pelican cmd/main_test.go pattern).
func htcondordbBinary(t *testing.T) string {
	dbBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "htcondordb-bin")
		if err != nil {
			dbBuildErr = err
			return
		}
		dbBinPath = filepath.Join(dir, "htcondordb")
		out, err := exec.Command("go", "build", "-o", dbBinPath, ".").CombinedOutput()
		if err != nil {
			dbBuildErr = fmt.Errorf("building htcondordb: %w\n%s", err, out)
		}
	})
	if dbBuildErr != nil {
		t.Fatalf("%v", dbBuildErr)
	}
	return dbBinPath
}

const haTrustDomain = "htcondordb.test"

// fsIdentity is the ALLOW pattern that grants the FS-authenticated identity. FS auth maps
// this process to the bare local username; an ALLOW entry's user part is matched against
// that identity and the host part against the peer IP, so "<user>@*" grants the user from
// any host (a bare "<user>" would be read as a HOSTNAME, not a user).
func fsIdentity(t *testing.T) string {
	u, err := user.Current()
	if err != nil {
		t.Fatalf("current user: %v", err)
	}
	return u.Username + "@*"
}

// writeNodeConfig writes an HTCondor config for one node: FS auth required, and DAEMON /
// WRITE / READ granted ONLY to the FS-authenticated user (not anonymous), so an anonymous
// peer stays read-only. extra holds the node's HA settings.
func writeNodeConfig(t *testing.T, dir, identity, extra string) string {
	t.Helper()
	for _, sub := range []string{"log", "db"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cfg := fmt.Sprintf(`
TRUST_DOMAIN = %s
UID_DOMAIN = %s
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_DEFAULT_INTEGRITY = REQUIRED
SEC_DEFAULT_ENCRYPTION = OPTIONAL
ALLOW_DAEMON = %s
ALLOW_WRITE = %s
ALLOW_READ = %s
ALLOW_ADMINISTRATOR = %s
LOG = %s
HTCONDORDB_DIR = %s
HTCONDORDB_ADDRESS_FILE = %s
%s
`, haTrustDomain, haTrustDomain, identity, identity, identity, identity,
		filepath.Join(dir, "log"), filepath.Join(dir, "db"), filepath.Join(dir, "addr"), extra)
	p := filepath.Join(dir, "condor_config")
	if err := os.WriteFile(p, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// startNode starts the daemon binary with the given config, capturing its stderr, and
// returns its advertised address once the address file is written.
func startNode(t *testing.T, bin, dir, cfgPath string) string {
	t.Helper()
	logFile, err := os.Create(filepath.Join(dir, "stderr.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-listen", "127.0.0.1:0")
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfgPath)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = logFile.Close()
		if t.Failed() {
			if b, err := os.ReadFile(filepath.Join(dir, "stderr.log")); err == nil && len(b) > 0 {
				t.Logf("=== %s stderr ===\n%s", filepath.Base(dir), b)
			}
			logs, _ := filepath.Glob(filepath.Join(dir, "log", "*"))
			for _, lf := range logs {
				if b, err := os.ReadFile(lf); err == nil {
					t.Logf("=== %s ===\n%s", lf, b)
				}
			}
		}
	})

	addrPath := filepath.Join(dir, "addr")
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(addrPath); err == nil && len(strings.TrimSpace(string(b))) > 0 {
			return strings.TrimSpace(string(b))
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("node %s exited before writing its address file", filepath.Base(dir))
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("node %s did not write its address file", filepath.Base(dir))
	return ""
}

// dbClient opens an authenticated dbrpc client to a node's advertised address, using FS
// auth from a client config (same identity domain as the daemons).
func dbClient(t *testing.T, addr string) *dbrpc.Client {
	t.Helper()
	dir := t.TempDir()
	cfgPath := writeNodeConfig(t, dir, fsIdentity(t), "")
	t.Setenv("CONDOR_CONFIG", cfgPath)
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "HTCONDORDB"})
	if err != nil {
		t.Fatalf("client config: %v", err)
	}
	sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
	if err != nil {
		t.Fatalf("client security: %v", err)
	}
	ctx := context.Background()
	cl, err := cedarclient.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect+auth to %s: %v", addr, err)
	}
	client := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestLeaderFollowerClusterIntegration stands up a real leader + follower and verifies a
// write to the leader replicates to the follower, all over authenticated CEDAR (FS auth).
func TestLeaderFollowerClusterIntegration(t *testing.T) {
	if os.Getenv("HTCONDORDB_HA_INTEGRATION") == "" {
		t.Skip("set HTCONDORDB_HA_INTEGRATION=1 to run the real-binary HA cluster test")
	}
	bin := htcondordbBinary(t)
	identity := fsIdentity(t)

	leaderDir := t.TempDir()
	leaderCfg := writeNodeConfig(t, leaderDir, identity,
		"HTCONDORDB_HA_MODE = leader-follower\nHTCONDORDB_ROLE = leader\n")
	leaderAddr := startNode(t, bin, leaderDir, leaderCfg)
	t.Logf("leader at %s", leaderAddr)

	// Write an ad to the leader.
	lc := dbClient(t, leaderAddr)
	tx, err := lc.Begin()
	if err != nil {
		t.Fatalf("begin on leader: %v", err)
	}
	if err := tx.NewClassAd("1.0", "Owner = \"alice\"\nJobStatus = 2"); err != nil {
		t.Fatalf("new ad: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit on leader: %v", err)
	}

	// Start the follower pointed at the leader.
	followerDir := t.TempDir()
	followerCfg := writeNodeConfig(t, followerDir, identity,
		"HTCONDORDB_HA_MODE = leader-follower\nHTCONDORDB_ROLE = follower\nHTCONDORDB_LEADER = "+leaderAddr+"\n")
	followerAddr := startNode(t, bin, followerDir, followerCfg)
	t.Logf("follower at %s", followerAddr)

	// The follower must replicate the ad from the leader's commit stream.
	fc := dbClient(t, followerAddr)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ads, err := fc.QueryTable("ads", "true", 0)
		if err == nil {
			for _, a := range ads {
				if strings.Contains(a, "1.0") || strings.Contains(a, "alice") {
					return // replicated
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("the write was not replicated to the follower")
}

// rawAddr strips the CEDAR sinful angle brackets from an address-file value,
// yielding the bare "host:port" form the daemon advertises to raft (see
// advertisedAddr / writeAddressFile: the file is bracketed, the raft address is not).
func rawAddr(sinful string) string {
	return strings.Trim(strings.TrimSpace(sinful), "<>")
}

// controlClient builds a consistent-mode ControlClient that speaks the DBControl
// ClassAd request/response protocol (leader discovery, peer registration, write
// apply) to addr over an authenticated CEDAR session, following leader redirects.
// This is the surface a master / admin tool uses to grow and drive the cluster.
func controlClient(t *testing.T, cfg *config.Config, addr string) *consistent.ControlClient {
	t.Helper()
	exchange := func(ectx context.Context, target string, req *classad.ClassAd) (*classad.ClassAd, error) {
		sec, err := htcondor.GetSecurityConfig(cfg, command.DBControl, "CLIENT")
		if err != nil {
			return nil, err
		}
		sec.Command = command.DBControl
		cl, err := cedarclient.ConnectAndAuthenticate(ectx, target, sec)
		if err != nil {
			return nil, err
		}
		defer func() { _ = cl.Close() }()
		s := cl.GetStream()
		out := message.NewMessageForStream(s)
		if err := out.PutClassAd(ectx, req); err != nil {
			return nil, err
		}
		if err := out.FinishMessage(ectx); err != nil { // flush the frame (EOM)
			return nil, err
		}
		return message.NewMessageFromStream(s).GetClassAd(ectx)
	}
	return consistent.NewControlClient(addr, exchange)
}

// clientConfig builds a client-side config (FS auth, same identity domain as the
// daemons) usable for GetSecurityConfig in control/dbrpc dials.
func clientConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	cfgPath := writeNodeConfig(t, dir, fsIdentity(t), "")
	t.Setenv("CONDOR_CONFIG", cfgPath)
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "HTCONDORDB"})
	if err != nil {
		t.Fatalf("client config: %v", err)
	}
	return cfg
}

// TestConsistentClusterIntegration stands up a real 3-node consistent (raft-over-CEDAR)
// cluster from actual binaries with FS auth: node n1 bootstraps single-node knowing only
// the quorum size (HTCONDORDB_RAFT_SIZE=3); n2 and n3 are pointed at n1 as their seed
// (HTCONDORDB_RAFT_SEED) and self-register as voters -- no explicit per-peer registration
// by the admin. A client write to the leader is routed through raft's propose hook,
// committed by quorum, and must appear on every node (reads are served locally).
func TestConsistentClusterIntegration(t *testing.T) {
	if os.Getenv("HTCONDORDB_HA_INTEGRATION") == "" {
		t.Skip("set HTCONDORDB_HA_INTEGRATION=1 to run the real-binary HA cluster test")
	}
	bin := htcondordbBinary(t)
	identity := fsIdentity(t)

	type node struct {
		name string
		addr string // bare host:port (raft form)
		dir  string
	}
	nodes := make([]node, 3)

	// Start the bootstrap node (n1) first: it starts single-node and grows as peers
	// self-register, capped at HTCONDORDB_RAFT_SIZE.
	startOne := func(i int, extra string) {
		name := fmt.Sprintf("n%d", i+1)
		dir := t.TempDir()
		cfg := writeNodeConfig(t, dir, identity,
			"HTCONDORDB_HA_MODE = consistent\nHTCONDORDB_RAFT_SIZE = 3\nHTCONDORDB_NODE_ID = "+name+"\n"+extra)
		addr := startNode(t, bin, dir, cfg)
		nodes[i] = node{name: name, addr: rawAddr(addr), dir: dir}
		t.Logf("%s at %s", name, addr)
	}
	startOne(0, "HTCONDORDB_RAFT_BOOTSTRAP = true\n")

	// Wait for n1 to win the initial single-node election, then point the joiners at it.
	cc := controlClient(t, clientConfig(t), nodes[0].addr)
	leaderDeadline := time.Now().Add(20 * time.Second)
	for {
		if addr, _, err := cc.Leader(context.Background()); err == nil && addr != "" {
			t.Logf("leader elected: %s", addr)
			break
		}
		if time.Now().After(leaderDeadline) {
			t.Fatal("no raft leader elected")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Joiners self-register against the seed (n1); the admin does not register them.
	startOne(1, "HTCONDORDB_RAFT_SEED = "+nodes[0].addr+"\n")
	startOne(2, "HTCONDORDB_RAFT_SEED = "+nodes[0].addr+"\n")

	// Write an ad to the leader. The dbrpc write routes through the propose hook ->
	// raft Apply -> quorum commit (the leader is n1, the bootstrap node).
	lc := dbClient(t, "<"+nodes[0].addr+">")
	tx, err := lc.Begin()
	if err != nil {
		t.Fatalf("begin on leader: %v", err)
	}
	if err := tx.NewClassAd("7.0", "Owner = \"carol\"\nJobStatus = 2"); err != nil {
		t.Fatalf("new ad: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit on leader (raft apply): %v", err)
	}

	// Every node -- including the two followers -- must reflect the quorum-committed
	// write in its local (read-only) view.
	for _, n := range nodes {
		fc := dbClient(t, "<"+n.addr+">")
		deadline := time.Now().Add(25 * time.Second)
		found := false
		for time.Now().Before(deadline) {
			ads, qerr := fc.QueryTable("ads", "true", 0)
			if qerr == nil {
				for _, a := range ads {
					if strings.Contains(a, "carol") || strings.Contains(a, "7.0") {
						found = true
						break
					}
				}
			}
			if found {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if !found {
			t.Fatalf("quorum-committed write not visible on %s (%s)", n.name, n.addr)
		}
		t.Logf("write visible on %s", n.name)
	}
}
