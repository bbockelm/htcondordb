//go:build unix

package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// This is the realistic cedarsync integration test: it compiles the actual htcondordb binary,
// starts TWO source nodes and ONE sink node as separate processes with real FS-authenticated
// CEDAR, configures the sink to fan-in-replicate both sources' "history" archives, and verifies
// records flow. It then pauses one source's replication via condor_reconfig (SIGHUP, dropping it
// from HTCONDORDB_REPLICATE_SOURCES), shows a new record on that source does NOT arrive while
// paused, re-enables it, and shows the record then arrives -- exercising the reconfigure
// stop/start and the durable cursor resume.

func seedArchive(t *testing.T, addr string, rows ...string) {
	t.Helper()
	c := dbClient(t, addr)
	ctx := context.Background()
	if err := c.CreateArchiveTable(ctx, "history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}}); err != nil {
		t.Fatalf("create history on %s: %v", addr, err)
	}
	for _, r := range rows {
		if err := c.ArchiveAppend(ctx, "history", r); err != nil {
			t.Fatalf("append to %s: %v", addr, err)
		}
	}
}

func archiveMatches(t *testing.T, addr, constraint string) int {
	t.Helper()
	rows, err := dbClient(t, addr).ArchiveQuery(context.Background(), "history", constraint, 0)
	if err != nil {
		t.Fatalf("query %s: %v", addr, err)
	}
	return len(rows)
}

// replicateConfig builds the sink's HTCONDORDB_REPLICATE_* config for the given source names
// (subset of {ap40, ap55}), wiring each to its address.
func replicateConfig(sources []string, addr1, addr2 string) string {
	addrOf := map[string]string{"ap40": addr1, "ap55": addr2}
	var b strings.Builder
	fmt.Fprintf(&b, "HTCONDORDB_REPLICATE_SOURCES = %s\n", strings.Join(sources, " "))
	for _, s := range sources {
		u := strings.ToUpper(s)
		fmt.Fprintf(&b, "HTCONDORDB_REPLICATE_%s_ADDRESS = %s\n", u, addrOf[s])
		fmt.Fprintf(&b, "HTCONDORDB_REPLICATE_%s_TABLE = history\n", u)
		fmt.Fprintf(&b, "HTCONDORDB_REPLICATE_%s_SRC = %s\n", u, s)
	}
	return b.String()
}

func waitUntil(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestCedarSyncIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test (builds + forks the daemon)")
	}
	bin := htcondordbBinary(t)
	id := fsIdentity(t)

	// Two source nodes, each with a history archive and one record.
	dir1, dir2 := t.TempDir(), t.TempDir()
	addr1 := startNode(t, bin, dir1, writeNodeConfig(t, dir1, id, ""))
	addr2 := startNode(t, bin, dir2, writeNodeConfig(t, dir2, id, ""))
	seedArchive(t, addr1, `GlobalJobId = "ap40#1.0"; Owner = "alice"; ClusterId = 1; CompletionDate = 1700000100`)
	seedArchive(t, addr2, `GlobalJobId = "ap55#1.0"; Owner = "bob";   ClusterId = 1; CompletionDate = 1700000200`)

	// Sink node: fan-in replicate both sources into its local history archive.
	sinkDir := t.TempDir()
	writeSink := func(sources []string) string {
		return writeNodeConfig(t, sinkDir, id, replicateConfig(sources, addr1, addr2))
	}
	sinkCfg := writeSink([]string{"ap40", "ap55"})
	sinkAddr, sinkProc := startNodeCmd(t, bin, sinkDir, sinkCfg)

	// Both sources' records fan in, stamped by Src.
	waitUntil(t, "both source records replicated", 20*time.Second, func() bool {
		return archiveMatches(t, sinkAddr, `true`) == 2
	})
	if n := archiveMatches(t, sinkAddr, `Src == "ap40" && Owner == "alice"`); n != 1 {
		t.Errorf("ap40/alice on sink = %d, want 1", n)
	}
	if n := archiveMatches(t, sinkAddr, `Src == "ap55" && Owner == "bob"`); n != 1 {
		t.Errorf("ap55/bob on sink = %d, want 1", n)
	}

	// Pause ap40: reconfigure the sink to replicate ONLY ap55, then SIGHUP.
	writeSink([]string{"ap55"})
	reconfigure(t, sinkProc)

	// While paused, a new record on ap40 must NOT arrive at the sink.
	seedArchive(t, addr1, `GlobalJobId = "ap40#2.0"; Owner = "carol"; ClusterId = 2; CompletionDate = 1700000300`)
	stableFor(t, 3*time.Second, func() bool {
		return archiveMatches(t, sinkAddr, `Owner == "carol"`) == 0
	}, "carol must not replicate while ap40 is paused")

	// Re-enable ap40 and SIGHUP: the runner restarts, resumes from its cursor, and the record
	// created while paused now arrives.
	writeSink([]string{"ap40", "ap55"})
	reconfigure(t, sinkProc)
	waitUntil(t, "carol replicates after ap40 re-enabled", 20*time.Second, func() bool {
		return archiveMatches(t, sinkAddr, `Owner == "carol"`) == 1
	})
	if n := archiveMatches(t, sinkAddr, `true`); n != 3 {
		t.Errorf("sink has %d records after resume, want 3 (no dupes)", n)
	}
}

// reconfigure sends SIGHUP (condor_reconfig) and gives the daemon a moment to re-read config and
// run its OnReconfig callbacks.
func reconfigure(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("SIGHUP: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
}

// stableFor asserts cond holds continuously for d (used to show something does NOT happen).
func stableFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("%s", msg)
		}
		time.Sleep(150 * time.Millisecond)
	}
}
