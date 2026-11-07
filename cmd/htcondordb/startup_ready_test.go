package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The daemon must not publish its address until it can answer on it.
//
// The address file is how a client finds the command port, and the listener for that port is
// bound early in startup -- long before the accept loop runs. The kernel completes a TCP
// handshake from the backlog of a bound-but-unaccepted listener, so a client arriving in that
// window connects successfully and then waits on a server that has not started, which from the
// client is indistinguishable from a slow query. Everything that could make the window long --
// exporter subprocesses, schedd-sync tailers, history importers, raft -- starts inside it.
//
// This is asserted as an ordering in the daemon's own log rather than by racing the daemon from
// outside: in a configuration with nothing slow the window is a millisecond or two, which no
// out-of-process observer can reliably catch, so a test that dialled the port would pass either
// way. The log is ordered by the startup goroutine, so it answers the question exactly.
func TestTheAddressIsPublishedAfterStartupFinishes(t *testing.T) {
	bin := htcondordbBinary(t)
	dir := t.TempDir()
	cfgPath := writeNodeConfig(t, dir, fsIdentity(t), "")

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
	})

	// The daemon is up once it says it is serving, which is logged after the address goes out.
	const (
		reachable = "command address published; daemon is reachable"
		serving   = "htcondordb starting"
		managers  = "exporter manager:"
	)
	var logText string
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		logText = readAllLogs(dir)
		if strings.Contains(logText, serving) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	iReachable := strings.Index(logText, reachable)
	iManagers := strings.Index(logText, managers)
	iServing := strings.Index(logText, serving)
	if iServing < 0 {
		t.Fatalf("the daemon never reported that it was serving. Log:\n%s", logText)
	}
	if iReachable < 0 {
		t.Fatalf("the daemon never reported publishing its address. Log:\n%s", logText)
	}
	if iManagers < 0 {
		t.Fatalf("the exporter manager never reported starting, so this test cannot tell "+
			"whether the address went out before the managers. Log:\n%s", logText)
	}
	if iReachable < iManagers {
		t.Errorf("the address was published before the managers had started: a client following "+
			"it reaches a command port nothing is accepting on yet. Log:\n%s", logText)
	}
	if iReachable > iServing {
		t.Errorf("the address was published after the serve loop began, so there is a window in "+
			"which the daemon is serving but unfindable. Log:\n%s", logText)
	}
}

// readAllLogs concatenates the daemon's log files and its stderr, in that order.
func readAllLogs(dir string) string {
	var b strings.Builder
	files, _ := filepath.Glob(filepath.Join(dir, "log", "*"))
	for _, f := range append(files, filepath.Join(dir, "stderr.log")) {
		if c, err := os.ReadFile(f); err == nil {
			b.Write(c)
		}
	}
	return b.String()
}
