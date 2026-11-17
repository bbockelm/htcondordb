package archivedropbox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbockelm/htcondordb/dropboxmanifest"
)

// TestEveryBatchGetsAManifest, in both containers: the manifest is a property of a batch, not of
// a format.
func TestEveryBatchGetsAManifest(t *testing.T) {
	for _, container := range []ContainerFormat{ContainerTarGz, ContainerFrames} {
		t.Run(string(container), func(t *testing.T) {
			dir := t.TempDir()
			cfg := Config{Table: "history", Directory: dir, Container: container}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			w, err := NewNamedWriter(cfg, "histbackup")
			if err != nil {
				t.Fatal(err)
			}
			in := recs(12)
			path, err := w.WriteBatch(3, in)
			if err != nil {
				t.Fatal(err)
			}
			m, err := dropboxmanifest.Read(path)
			if err != nil {
				t.Fatalf("no manifest beside %s: %v", filepath.Base(path), err)
			}
			if m.Records != len(in) {
				t.Errorf("Records = %d, want %d", m.Records, len(in))
			}
			if m.Seq != 3 {
				t.Errorf("Seq = %d, want 3", m.Seq)
			}
			if m.Container != string(container) {
				t.Errorf("Container = %q, want %q", m.Container, container)
			}
			if m.Exporter != "histbackup" || m.Table != "history" {
				t.Errorf("manifest does not say what wrote it: exporter=%q table=%q", m.Exporter, m.Table)
			}
			// The digest must describe the batch as it actually landed.
			sum, n, err := dropboxmanifest.HashFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if m.SHA256 != sum || m.Bytes != n {
				t.Errorf("manifest digest/size do not match the batch on disk")
			}
		})
	}
}

// TestManifestChainsToThePreviousBatch: a consumer drains from the front, so a low sequence being
// absent is normal. PrevSeq is what distinguishes an ordinary drain from a hole between batches
// that are both still present.
func TestManifestChainsToThePreviousBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Table: "history", Directory: dir}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	w, err := NewNamedWriter(cfg, "histbackup")
	if err != nil {
		t.Fatal(err)
	}
	first, err := w.WriteBatch(10, recs(2))
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.WriteBatch(11, recs(2))
	if err != nil {
		t.Fatal(err)
	}
	m1, err := dropboxmanifest.Read(first)
	if err != nil {
		t.Fatal(err)
	}
	if m1.PrevSeq != 0 {
		t.Errorf("the first batch this writer wrote claims PrevSeq %d, want 0", m1.PrevSeq)
	}
	m2, err := dropboxmanifest.Read(second)
	if err != nil {
		t.Fatal(err)
	}
	if m2.PrevSeq != 10 {
		t.Errorf("PrevSeq = %d, want 10", m2.PrevSeq)
	}
}

// TestManifestsAreNotCountedAsDropboxBytes: the backpressure ceiling measures undelivered data.
// Counting sidecars against it would quietly shrink the ceiling an operator configured.
func TestManifestsAreNotCountedAsDropboxBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Table: "history", Directory: dir}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	w, err := NewNamedWriter(cfg, "histbackup")
	if err != nil {
		t.Fatal(err)
	}
	path, err := w.WriteBatch(1, recs(4))
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.DirSize()
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != st.Size() {
		t.Errorf("DirSize = %d, want just the batch at %d -- manifests must not count against backpressure", got, st.Size())
	}
}

// TestManifestFailureDoesNotFailTheBatch: the batch is durable before the manifest is attempted,
// and the caller advances its cursor on the strength of the batch. A metadata problem must not
// become a data one.
func TestManifestFailureDoesNotFailTheBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Table: "history", Directory: dir}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	w, err := NewNamedWriter(cfg, "histbackup")
	if err != nil {
		t.Fatal(err)
	}
	path, err := w.WriteBatch(1, recs(3))
	if err != nil {
		t.Fatal(err)
	}
	// Make the manifest path unwritable by putting a directory where the file must go.
	second := filepath.Join(dir, "batch-00000002-1700000000.tar.gz")
	if err := os.Mkdir(dropboxmanifest.Path(second), 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := w.WriteBatch(2, recs(3))
	if err != nil {
		t.Fatalf("a batch failed because its manifest could not be written: %v", err)
	}
	if !strings.HasPrefix(filepath.Base(got), "batch-00000002-") {
		t.Errorf("second batch written as %s", filepath.Base(got))
	}
	if _, err := os.Stat(got); err != nil {
		t.Errorf("the batch itself is missing: %v", err)
	}
	_ = path
}
