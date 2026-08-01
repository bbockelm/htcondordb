//go:build unix

package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// This is the realistic changefeed integration test: it starts a SOURCE htcondordb node serving
// the HTTP/SSE change feed, then forks the standalone NON-CEDAR changefeed-toysink, which pulls the
// feed and lands records into its own local classad/db store ("a destination htcondordb"). It then
// SIGTERMs the toy sink (verifying it shuts down cleanly), appends a new record on the source, and
// restarts the toy sink -- which resumes from its persisted cursor and picks up the new record.

var (
	toysinkOnce sync.Once
	toysinkBin  string
	toysinkErr  error
)

func toysinkBinary(t *testing.T) string {
	toysinkOnce.Do(func() {
		dir, err := os.MkdirTemp("", "toysink-bin")
		if err != nil {
			toysinkErr = err
			return
		}
		toysinkBin = filepath.Join(dir, "changefeed-toysink")
		out, err := exec.Command("go", "build", "-o", toysinkBin, "../changefeed-toysink").CombinedOutput()
		if err != nil {
			toysinkErr = fmt.Errorf("building toysink: %w\n%s", err, out)
		}
	})
	if toysinkErr != nil {
		t.Fatalf("%v", toysinkErr)
	}
	return toysinkBin
}

func readAddrFile(t *testing.T, path string, d time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(b))) > 0 {
			return strings.TrimSpace(string(b))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("address file %s not written in time", path)
	return ""
}

// startToySink forks the sink pointing at feedURL, writing its store under dir; returns the /count
// base URL and the process. The caller controls the lifecycle (kill/restart).
func startToySink(t *testing.T, feedURL, token, dir string) (string, *exec.Cmd) {
	t.Helper()
	addrFile := filepath.Join(dir, "http_addr")
	_ = os.Remove(addrFile)
	logf, _ := os.Create(filepath.Join(dir, "toysink.log"))
	cmd := exec.Command(toysinkBinary(t),
		"-source", feedURL, "-token", token, "-table", "history", "-target", "history",
		"-subscriber", "central", "-src", "ap40", "-dir", dir,
		"-listen", "127.0.0.1:0", "-addr-file", addrFile)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = logf.Close()
		if t.Failed() {
			if b, err := os.ReadFile(filepath.Join(dir, "toysink.log")); err == nil {
				t.Logf("=== toysink log ===\n%s", b)
			}
		}
	})
	return "http://" + readAddrFile(t, addrFile, 15*time.Second), cmd
}

func sinkCount(t *testing.T, base string) int {
	t.Helper()
	resp, err := http.Get(base + "/count")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return -1
	}
	return n
}

func TestChangeFeedIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test (builds + forks the daemon and the toy sink)")
	}
	bin := htcondordbBinary(t)
	id := fsIdentity(t)

	// Source node with the change feed enabled (dynamic port -> address file), token-gated.
	srcDir := t.TempDir()
	cfAddrFile := filepath.Join(srcDir, "cf_addr")
	const token = "s3cret-feed-token"
	extra := fmt.Sprintf(`
HTCONDORDB_CHANGEFEED_ADDRESS = 127.0.0.1:0
HTCONDORDB_CHANGEFEED_ADDRESS_FILE = %s
HTCONDORDB_CHANGEFEED_TOKEN = %s
HTCONDORDB_CHANGEFEED_AGE_ATTR = CompletionDate
`, cfAddrFile, token)
	srcAddr := startNode(t, bin, srcDir, writeNodeConfig(t, srcDir, id, extra))
	seedArchive(t, srcAddr, // reuses the cedarsync test helper (create history + append)
		`GlobalJobId = "ap40#1.0"; Owner = "alice"; ClusterId = 1; CompletionDate = 1700000100`,
		`GlobalJobId = "ap40#2.0"; Owner = "bob";   ClusterId = 2; CompletionDate = 1700000200`)
	feedURL := "http://" + readAddrFile(t, cfAddrFile, 15*time.Second)

	// Fork the standalone non-CEDAR toy sink; it pulls the feed into its own local store.
	sinkDir := t.TempDir()
	countURL, sinkProc := startToySink(t, feedURL, token, sinkDir)
	waitUntil(t, "toy sink replicated both records", 20*time.Second, func() bool {
		return sinkCount(t, countURL) == 2
	})

	// A wrong token is rejected (the feed is gated).
	badReq, _ := http.NewRequest("GET", feedURL+"/changefeed/v1/subscribe?table=history&subscriber=x", nil)
	badReq.Header.Set("Authorization", "Bearer nope")
	if resp, err := http.DefaultClient.Do(badReq); err == nil {
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("unauthenticated subscribe got %d, want 401", resp.StatusCode)
		}
		resp.Body.Close()
	}

	// Pause: SIGTERM the toy sink and verify it shuts down cleanly (within a bound).
	if err := sinkProc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM toy sink: %v", err)
	}
	waitProc(t, sinkProc, 10*time.Second)

	// A new record appears on the source while the sink is down.
	seedArchive(t, srcAddr, `GlobalJobId = "ap40#3.0"; Owner = "carol"; ClusterId = 3; CompletionDate = 1700000300`)

	// Re-enable: restart the toy sink; it resumes from its persisted cursor and picks up the new
	// record (total 3, no re-import of the first two).
	countURL2, sinkProc2 := startToySink(t, feedURL, token, sinkDir)
	waitUntil(t, "toy sink picks up the record created while it was down", 20*time.Second, func() bool {
		return sinkCount(t, countURL2) == 3
	})

	// Stop the sink (releasing its single-writer store), then confirm the destination store holds
	// exactly the expected records -- including carol, stamped with the source label.
	_ = sinkProc2.Process.Signal(syscall.SIGTERM)
	waitProc(t, sinkProc2, 10*time.Second)
	if n := destinationCount(t, sinkDir, `Owner == "carol" && Src == "ap40"`); n != 1 {
		t.Errorf("destination has carol/ap40 = %d, want 1", n)
	}
	if n := destinationCount(t, sinkDir, `true`); n != 3 {
		t.Errorf("destination has %d records, want 3 (no dupes across the resume)", n)
	}
}

// waitProc asserts the process exits within d.
func waitProc(t *testing.T, cmd *exec.Cmd, d time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("toy sink did not exit within timeout after SIGTERM")
	}
}

// destinationCount opens the toy sink's store (after it has stopped) and counts matching records.
func destinationCount(t *testing.T, dir, constraint string) int {
	t.Helper()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatalf("open destination catalog: %v", err)
	}
	defer cat.Close()
	a, ok := cat.ArchiveTable("history")
	if !ok {
		t.Fatal("destination has no history archive")
	}
	seq, err := a.Query(constraint)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}
