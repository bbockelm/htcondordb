package archivedropbox

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

func testServer(t *testing.T) (*dbrpc.Client, *db.Catalog) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	t.Cleanup(func() { c.Close(); s.Close(); cat.Close() })
	return c, cat
}

func mustAppend(t *testing.T, arch *db.ArchiveTable, adText string) {
	t.Helper()
	if err := arch.AppendOld(adText); err != nil {
		t.Fatal(err)
	}
}

func newDropboxExporter(t *testing.T, c *dbrpc.Client, name, table, dir string, rollJobs int) Config {
	t.Helper()
	cfg := Config{Table: table, Directory: dir, RollJobs: rollJobs, RollInterval: Duration(200 * time.Millisecond)}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := cfg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateExporter(context.Background(), db.ExporterDef{Name: name, Kind: Kind, Config: raw}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// countTarballs / listLoss inspect the dropbox directory.
func countTarballs(dir string) int {
	entries, _ := os.ReadDir(dir)
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tar.gz") {
			n++
		}
	}
	return n
}

func listLoss(dir string) []string {
	entries, _ := os.ReadDir(dir)
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "loss-") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

// allTarballEntries merges every tarball's entries in the dropbox.
func allTarballEntries(t *testing.T, dir string) map[string]string {
	t.Helper()
	merged := map[string]string{}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tar.gz") {
			for k, v := range readTarball(t, filepath.Join(dir, e.Name())) {
				merged[k] = v
			}
		}
	}
	return merged
}

// TestRunnerArchiveEndToEnd runs the full path against a history archive: the exporter replays the
// retained records into per-job tarball entries, then mirrors a live append. RollJobs=1 makes each
// record its own tarball so the assertions are deterministic.
func TestRunnerArchiveEndToEnd(t *testing.T) {
	c, cat := testServer(t)
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, arch, `GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; CompletionDate = 1700000100; Owner = "alice"; ClusterId = 12`)
	mustAppend(t, arch, `GlobalJobId = "ap40#13.0#1700000000"; JobStatus = 4; CompletionDate = 1700000200; Owner = "bob"; ClusterId = 13`)

	dir := t.TempDir()
	cfg := newDropboxExporter(t, c, "hist-dropbox", "history", dir, 1)
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner("hist-dropbox", cfg, c, w, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Both retained records were exported (two tarballs, RollJobs=1).
	waitFor(t, func() bool { return countTarballs(dir) >= 2 })
	merged := allTarballEntries(t, dir)
	var foundAlice bool
	for name, body := range merged {
		if strings.Contains(name, "ap40#12.0#1700000000") {
			foundAlice = true
			if !strings.Contains(body, `Owner = "alice"`) {
				t.Errorf("alice entry body wrong:\n%s", body)
			}
		}
	}
	if !foundAlice {
		t.Fatalf("alice record not exported; entries: %v", keys(merged))
	}

	// The resume cursor advanced (WatchSynced checkpoint) -- state has a cursor + progress.
	waitFor(t, func() bool {
		blob, ok, _ := c.GetExporterState(ctx, "hist-dropbox")
		if !ok {
			return false
		}
		st, _ := decodeState(blob)
		return len(st.WireCursor) > 0 && st.Status.DocsIndexed >= 2 && st.Status.Beat > 0
	})

	// A live append flows through into a new tarball.
	mustAppend(t, arch, `GlobalJobId = "ap40#14.0#1700000000"; JobStatus = 4; CompletionDate = 1700000300; Owner = "carol"; ClusterId = 14`)
	waitFor(t, func() bool {
		for name := range allTarballEntries(t, dir) {
			if strings.Contains(name, "ap40#14.0#1700000000") {
				return true
			}
		}
		return false
	})

	cancel()
	<-done
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// fakeWriter drives the backpressure test: DirSize is scripted, and writes are recorded.
type fakeWriter struct {
	mu       sync.Mutex
	size     int64
	tarballs int
	loss     int
}

func (f *fakeWriter) WriteTarball(seq uint64, recs []record) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tarballs++
	return "fake", nil
}
func (f *fakeWriter) WriteLossReport(adText string, detectedUnix int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loss++
	return "fake-loss", nil
}
func (f *fakeWriter) DirSize() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.size, nil
}
func (f *fakeWriter) setSize(n int64) { f.mu.Lock(); f.size = n; f.mu.Unlock() }
func (f *fakeWriter) tarballCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tarballs
}

// TestBackpressureBlocksWrites: while the dropbox is over the ceiling, no tarball is written and
// the cursor does not advance; once the (fake) consumer drains it, the buffered batch is written.
func TestBackpressureBlocksWrites(t *testing.T) {
	c, cat := testServer(t)
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, arch, `GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; CompletionDate = 1700000100; ClusterId = 12`)

	cfg := Config{Table: "history", Directory: t.TempDir(), RollJobs: 1, RollInterval: Duration(100 * time.Millisecond), MaxDropboxBytes: 1000}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, _ := cfg.Marshal()
	if err := c.CreateExporter(context.Background(), db.ExporterDef{Name: "bp", Kind: Kind, Config: raw}); err != nil {
		t.Fatal(err)
	}

	fw := &fakeWriter{size: 5000} // start over the 1000-byte ceiling
	r := NewRunner("bp", cfg, c, fw, nil)
	r.backpressurePoll = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Held off: no tarball while full, and the cursor stays empty.
	time.Sleep(300 * time.Millisecond)
	if n := fw.tarballCount(); n != 0 {
		t.Fatalf("wrote %d tarballs while dropbox full; want 0", n)
	}
	blob, ok, _ := c.GetExporterState(ctx, "bp")
	if ok {
		if st, _ := decodeState(blob); len(st.WireCursor) > 0 {
			t.Fatal("cursor advanced while backpressured")
		}
	}

	// Consumer drains -> the buffered record is now written.
	fw.setSize(0)
	waitFor(t, func() bool { return fw.tarballCount() >= 1 })

	cancel()
	<-done
}

// TestDataLossReport: after the exporter has checkpointed a cursor, truncating the archive prunes
// that cursor out of retention. On resume the watch re-syncs from the (now oldest) record, and the
// exporter drops a loss report describing the gap.
func TestDataLossReport(t *testing.T) {
	c, cat := testServer(t)
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, arch, `GlobalJobId = "ap40#12.0#1700000000"; JobStatus = 4; CompletionDate = 1700000100; ClusterId = 12`)
	mustAppend(t, arch, `GlobalJobId = "ap40#13.0#1700000000"; JobStatus = 4; CompletionDate = 1700000200; ClusterId = 13`)

	dir := t.TempDir()
	cfg := newDropboxExporter(t, c, "loss", "history", dir, 1)
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// First run: export the two records and checkpoint a cursor, then stop.
	ctx1, cancel1 := context.WithCancel(context.Background())
	r1 := NewRunner("loss", cfg, c, w, nil)
	done1 := make(chan error, 1)
	go func() { done1 <- r1.Run(ctx1) }()
	waitFor(t, func() bool {
		blob, ok, _ := c.GetExporterState(ctx1, "loss")
		if !ok {
			return false
		}
		st, _ := decodeState(blob)
		return len(st.WireCursor) > 0 && st.Status.DocsIndexed >= 2
	})
	cancel1()
	<-done1

	// Prune everything the cursor pointed at, then append a fresh record: the resume cursor is now
	// out of retention.
	arch.Truncate()
	mustAppend(t, arch, `GlobalJobId = "ap40#99.0#1700000000"; JobStatus = 4; CompletionDate = 1700090000; ClusterId = 99`)

	// Second run resumes from the stale cursor -> WatchReset -> loss report.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	r2 := NewRunner("loss", cfg, c, w, nil)
	done2 := make(chan error, 1)
	go func() { done2 <- r2.Run(ctx2) }()

	waitFor(t, func() bool { return len(listLoss(dir)) >= 1 })
	body, err := os.ReadFile(listLoss(dir)[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "ArchiveDropboxDataLoss") {
		t.Errorf("loss report malformed:\n%s", body)
	}

	cancel2()
	<-done2
}
