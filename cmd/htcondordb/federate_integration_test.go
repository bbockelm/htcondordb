//go:build unix

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestFederationIntegration is the end-to-end check on the federation hub: real htcondordb
// processes over real FS-authenticated CEDAR. Three spokes each run schedd sync over a synthetic
// job_queue.log and history file the test writes (no condor needed); spokes 1 and 2 both have job
// 1.0. One hub federates them as static spokes. It asserts both directions throughout -- each AP's
// rows present under its own ScheddName and nothing more -- across a spoke restart, a delete while
// the hub is down, a job_queue.log compaction, and a stopped spoke.
func TestFederationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test (builds + forks the daemon)")
	}
	if os.Geteuid() == 0 {
		t.Skip("schedd-sync refuses to run as root")
	}
	bin := htcondordbBinary(t)
	id := fsIdentity(t)

	type spoke struct {
		name, dir, jobLog, hist, cfg, addr string
		proc                               *exec.Cmd
	}
	spokes := make([]*spoke, 3)
	for i := range spokes {
		sp := &spoke{name: fmt.Sprintf("ap%d.test", i+1), dir: t.TempDir()}
		schedd := filepath.Join(sp.dir, "schedd")
		if err := os.MkdirAll(schedd, 0o755); err != nil {
			t.Fatal(err)
		}
		sp.jobLog, sp.hist = filepath.Join(schedd, "job_queue.log"), filepath.Join(schedd, "history")
		sp.cfg = writeNodeConfig(t, sp.dir, id, fmt.Sprintf(`
HTCONDORDB_SYNC_SCHEDD = true
JOB_QUEUE_LOG = %s
HISTORY = %s
HTCONDORDB_MIRRORED_SCHEDD_NAME = %s
HTCONDORDB_SYNCSTATUS_INTERVAL = 1
HTCONDORDB_SCHEDDSYNC_POLL_MS = 100
HTCONDORDB_SCHEDDSYNC_IDLE_MAX_MS = 200
HTCONDORDB_ADVERTISE = false
`, sp.jobLog, sp.hist, sp.name))
		spokes[i] = sp
	}
	// Spokes 1 and 2 share job id 1.0; spoke 3 has its own.
	writeQueue(t, spokes[0].jobLog, spokes[0].name, 1, []int{1, 2, 3})
	writeQueue(t, spokes[1].jobLog, spokes[1].name, 1, []int{1, 5})
	writeQueue(t, spokes[2].jobLog, spokes[2].name, 1, []int{7})
	for i, sp := range spokes {
		writeHistory(t, sp.hist, sp.name, 100+i*10, 100+i*10+1)
	}
	for _, sp := range spokes {
		sp.addr, sp.proc = startNodeCmd(t, bin, sp.dir, sp.cfg)
	}

	hubDir := t.TempDir()
	var hubCfg strings.Builder
	hubCfg.WriteString("HTCONDORDB_FEDERATE_SPOKES = s1 s2 s3\n")
	for i, sp := range spokes {
		fmt.Fprintf(&hubCfg, "HTCONDORDB_FEDERATE_SPOKE_S%d_ADDRESS = %s\nHTCONDORDB_FEDERATE_SPOKE_S%d_SCHEDD = %s\n", i+1, sp.addr, i+1, sp.name)
	}
	hubCfg.WriteString("HTCONDORDB_FEDERATE_STATE_INTERVAL = 1\nHTCONDORDB_FEDERATE_FRESH_SECONDS = 6\nHTCONDORDB_ADVERTISE = false\n")
	hubCfgPath := writeNodeConfig(t, hubDir, id, hubCfg.String())
	hubAddr, hubProc := startNodeCmd(t, bin, hubDir, hubCfgPath)
	hc := dbClient(t, hubAddr)

	// expect asserts the hub's jobs for one AP are exactly the given clusters (proc 0), counted.
	jobsOf := func(c *dbrpc.Client, schedd string) map[int]bool {
		rows, err := c.QueryTable(context.Background(), "jobs", fmt.Sprintf("ScheddName == %q", schedd), 0)
		if err != nil {
			return nil
		}
		out := map[int]bool{}
		for _, r := range rows {
			ad, err := parseRow(r)
			if err != nil {
				t.Fatalf("parse hub row %q: %v", r, err)
			}
			cid, _ := ad.EvaluateAttrInt("ClusterId")
			out[int(cid)] = true
		}
		return out
	}
	exact := func(got map[int]bool, want ...int) bool {
		if len(got) != len(want) {
			return false
		}
		for _, w := range want {
			if !got[w] {
				return false
			}
		}
		return true
	}
	histCount := func(c *dbrpc.Client, constraint string) int {
		rows, err := c.ArchiveQuery(context.Background(), "history", constraint, 0)
		if err != nil {
			return -1
		}
		return len(rows)
	}
	allJobs := func(c *dbrpc.Client) int {
		rows, err := c.QueryTable(context.Background(), "jobs", "true", 0)
		if err != nil {
			return -1
		}
		return len(rows)
	}
	sourceRow := func(c *dbrpc.Client, schedd string) *classad.ClassAd {
		rows, err := c.QueryTable(context.Background(), "federation_sources", fmt.Sprintf("ScheddName == %q", schedd), 0)
		if err != nil || len(rows) != 1 {
			return nil
		}
		ad, err := parseRow(rows[0])
		if err != nil {
			return nil
		}
		return ad
	}
	sourceState := func(c *dbrpc.Client, schedd string) string {
		if ad := sourceRow(c, schedd); ad != nil {
			s, _ := ad.EvaluateAttrString("State")
			return s
		}
		return ""
	}
	lastReset := func(c *dbrpc.Client, schedd string) int64 {
		if ad := sourceRow(c, schedd); ad != nil {
			v, _ := ad.EvaluateAttrInt("LastReset")
			return v
		}
		return 0
	}
	wantSteady := func(c *dbrpc.Client, what string) {
		t.Helper()
		waitUntil(t, what, 30*time.Second, func() bool {
			return exact(jobsOf(c, spokes[0].name), 1, 2, 3) && exact(jobsOf(c, spokes[1].name), 1, 5) &&
				exact(jobsOf(c, spokes[2].name), 7) && allJobs(c) == 6 &&
				histCount(c, "true") == 6 &&
				histCount(c, fmt.Sprintf("ScheddName == %q", spokes[0].name)) == 2
		})
	}

	// 1. Every AP's rows, under its own name; the shared 1.0 is two rows.
	wantSteady(hc, "initial federation")
	for _, sp := range spokes {
		waitUntil(t, sp.name+" fresh", 20*time.Second, func() bool { return sourceState(hc, sp.name) == "fresh" })
	}

	// 2. A spoke restart (new watch epoch => Reset on every table): identical counts, zero
	//    history duplicates.
	resetBefore := lastReset(hc, spokes[0].name)
	time.Sleep(1100 * time.Millisecond) // LastReset has one-second resolution
	_ = spokes[0].proc.Process.Kill()
	_, _ = spokes[0].proc.Process.Wait()
	spokes[0].addr, spokes[0].proc = restartNodeAt(t, bin, spokes[0].dir, spokes[0].cfg, spokes[0].addr)
	waitUntil(t, "ap1 fresh after its restart", 30*time.Second, func() bool { return sourceState(hc, spokes[0].name) == "fresh" })
	waitUntil(t, "ap1's restart replayed as a Reset", 30*time.Second, func() bool { return lastReset(hc, spokes[0].name) > resetBefore })
	wantSteady(hc, "steady after spoke restart")
	stableFor(t, 3*time.Second, func() bool { return histCount(hc, "true") == 6 && allJobs(hc) == 6 },
		"history duplicated or jobs changed after a spoke restart")

	// 3. A delete on a spoke while the hub is down is gone after the hub restarts.
	_ = hubProc.Process.Kill()
	_, _ = hubProc.Process.Wait()
	appendFile(t, spokes[1].jobLog, "102 5.0\n")
	sc := dbClient(t, spokes[1].addr)
	waitUntil(t, "spoke 2 applied the delete", 20*time.Second, func() bool {
		rows, err := sc.QueryTable(context.Background(), "jobs", "ClusterId == 5", 0)
		return err == nil && len(rows) == 0
	})
	hubAddr, _ = restartNodeAt(t, bin, hubDir, hubCfgPath, hubAddr)
	hc = dbClient(t, hubAddr)
	waitUntil(t, "delete made while the hub was down", 30*time.Second, func() bool {
		return exact(jobsOf(hc, spokes[1].name), 1) && exact(jobsOf(hc, spokes[0].name), 1, 2, 3)
	})

	// 4. A job_queue.log compaction (the schedd rewrites the log as a new file holding the current
	//    state, here also dropping job 2.0): no phantoms, nothing else lost.
	writeQueue(t, spokes[0].jobLog, spokes[0].name, 2, []int{1, 3})
	waitUntil(t, "compaction reconciled", 30*time.Second, func() bool {
		return exact(jobsOf(hc, spokes[0].name), 1, 3) && exact(jobsOf(hc, spokes[1].name), 1) &&
			exact(jobsOf(hc, spokes[2].name), 7) && allJobs(hc) == 4
	})

	// 5. A stopped spoke: its source goes stale and its rows stay.
	_ = spokes[2].proc.Process.Kill()
	_, _ = spokes[2].proc.Process.Wait()
	waitUntil(t, "stopped spoke goes stale", 30*time.Second, func() bool { return sourceState(hc, spokes[2].name) == "stale" })
	if got := jobsOf(hc, spokes[2].name); !exact(got, 7) {
		t.Fatalf("stopped spoke's rows changed: %v", got)
	}
	if n := histCount(hc, fmt.Sprintf("ScheddName == %q", spokes[2].name)); n != 2 {
		t.Fatalf("stopped spoke's history rows = %d, want 2", n)
	}
	if n := histCount(hc, "true"); n != 6 {
		t.Fatalf("hub history = %d records, want 6 (no duplicates across the whole run)", n)
	}
}

// parseRow parses a dbrpc query row, in whichever ClassAd syntax it arrives.
func parseRow(r string) (*classad.ClassAd, error) {
	if strings.HasPrefix(strings.TrimSpace(r), "[") {
		return classad.Parse(r)
	}
	return classad.ParseOld(r)
}

// writeQueue writes a job_queue.log holding the given proc ads, as a schedd's compacted log does
// (a 107 sequence header, then 101/103 records), replacing any existing file with a new one.
func writeQueue(t *testing.T, path, schedd string, seq int, clusters []int) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "107 %d CreationTimestamp 1700000000\n", seq)
	for _, c := range clusters {
		k := fmt.Sprintf("%d.0", c)
		fmt.Fprintf(&b, "101 %s Job Machine\n", k)
		fmt.Fprintf(&b, "103 %s ClusterId %d\n103 %s ProcId 0\n103 %s JobStatus 1\n", k, c, k, k)
		fmt.Fprintf(&b, "103 %s Owner \"alice\"\n103 %s GlobalJobId \"%s#%s#1700000000\"\n", k, k, schedd, k)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// writeHistory writes completed-job records in the schedd's history-file format.
func writeHistory(t *testing.T, path, schedd string, clusters ...int) {
	t.Helper()
	var b strings.Builder
	for _, c := range clusters {
		fmt.Fprintf(&b, "Owner = \"alice\"\nClusterId = %d\nProcId = 0\nJobStatus = 4\nCompletionDate = %d\nGlobalJobId = \"%s#%d.0#1700000000\"\n*** Offset = 0 ClusterId = %d ProcId = 0\n",
			c, 1_700_000_000+c, schedd, c, c)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(text); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// restartNodeAt restarts a node on the address it had, so peers configured with that address (the
// hub's static spokes) reach it again. The old address file is removed first so startNodeCmd waits
// for the new process's.
func restartNodeAt(t *testing.T, bin, dir, cfgPath, addr string) (string, *exec.Cmd) {
	t.Helper()
	_ = os.Remove(filepath.Join(dir, "addr"))
	hostport := strings.TrimSuffix(strings.TrimPrefix(addr, "<"), ">")
	if i := strings.IndexByte(hostport, '?'); i >= 0 {
		hostport = hostport[:i]
	}
	return startNodeCmdListen(t, bin, dir, cfgPath, hostport)
}
