package archivedropbox

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestWriter(t *testing.T) (*dropboxWriter, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := newDropboxWriter(dir, DefaultCompressionLevel)
	if err != nil {
		t.Fatal(err)
	}
	return w, dir
}

// readTarball returns the entry name -> body map of a .tar.gz.
func readTarball(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	out := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		out[hdr.Name] = string(body)
	}
	return out
}

func TestWriteTarballAtomicAndComplete(t *testing.T) {
	w, dir := newTestWriter(t)
	recs := []record{
		{name: "000000-ap40_12.0.classad", adText: "GlobalJobId = \"ap40#12.0\"\nJobStatus = 4\n", modUnix: 1_700_000_100},
		{name: "000001-ap40_13.0.classad", adText: "GlobalJobId = \"ap40#13.0\"\nJobStatus = 4\n", modUnix: 1_700_000_200},
	}
	path, err := w.WriteTarball(1, recs)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(path) != dir {
		t.Fatalf("tarball written outside dropbox: %s", path)
	}
	if !strings.HasSuffix(filepath.Base(path), ".tar.gz") {
		t.Errorf("final name %q not a .tar.gz", filepath.Base(path))
	}

	// No temp file lingers (atomic put).
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tmpPrefix) {
			t.Errorf("leftover temp file %q", e.Name())
		}
	}

	got := readTarball(t, path)
	if len(got) != 2 {
		t.Fatalf("entries = %d, want 2", len(got))
	}
	if !strings.Contains(got["000000-ap40_12.0.classad"], "ap40#12.0") {
		t.Errorf("entry 0 body wrong: %q", got["000000-ap40_12.0.classad"])
	}
}

func TestWriteTarballEmptyIsNoop(t *testing.T) {
	w, dir := newTestWriter(t)
	path, err := w.WriteTarball(1, nil)
	if err != nil || path != "" {
		t.Fatalf("empty batch: path=%q err=%v, want empty/nil", path, err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("empty batch wrote %d files", len(entries))
	}
}

func TestDirSizeExcludesTempFiles(t *testing.T) {
	w, dir := newTestWriter(t)
	// A finished tarball counts.
	if _, err := w.WriteTarball(1, []record{{name: "a.classad", adText: strings.Repeat("x", 500), modUnix: 1}}); err != nil {
		t.Fatal(err)
	}
	// A dangling temp file (a crash mid-write) must NOT count toward backpressure.
	if err := os.WriteFile(filepath.Join(dir, tmpPrefix+"batch-leftover.tmp"), make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	size, err := w.DirSize()
	if err != nil {
		t.Fatal(err)
	}
	if size == 0 || size >= 1<<20 {
		t.Errorf("DirSize = %d; should count the tarball but not the 1MiB temp file", size)
	}
}

func TestWriteLossReport(t *testing.T) {
	w, dir := newTestWriter(t)
	ad := buildLossReport("hist-dropbox", "history", 1_700_000_000, 1_700_050_000, 1_700_060_000)
	path, err := w.WriteLossReport(ad, 1_700_060_000)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(path) != dir || !strings.HasPrefix(filepath.Base(path), "loss-") {
		t.Fatalf("loss report path wrong: %s", path)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ArchiveDropboxDataLoss", "EstimatedLossStartTime = 1700000000", "EstimatedLossEndTime = 1700050000", "EstimatedLossSeconds = 50000"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("loss report missing %q:\n%s", want, body)
		}
	}
}
