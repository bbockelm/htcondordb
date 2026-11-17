package archivedropbox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbockelm/htcondordb/dropboxframe"
)

func framedWriter(t *testing.T, dir string, minFrame int) *dropboxWriter {
	t.Helper()
	cfg := Config{Table: "history", Directory: dir, Container: ContainerFrames, MinFrameBytes: minFrame}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return w.(*dropboxWriter)
}

func recs(n int) []record {
	out := make([]record, n)
	for i := range out {
		out[i] = record{
			name:    "r.classad",
			body:    "GlobalJobId = \"ap.example#" + strings.Repeat("x", 8) + "\"\nOwner = \"someuser\"\nClusterId = 1\n",
			modUnix: 1700000000,
		}
	}
	return out
}

// TestFramedBatchHasItsOwnExtension: the container must be identifiable from the file name, or a
// consumer and the restore path cannot tell which decoder to use.
func TestFramedBatchHasItsOwnExtension(t *testing.T) {
	dir := t.TempDir()
	w := framedWriter(t, dir, dropboxframe.MinFrameBytesFloor)
	path, err := w.WriteBatch(7, recs(10))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(path, ".cadf") {
		t.Errorf("framed batch written as %s, want a .cadf", filepath.Base(path))
	}
	if !strings.HasPrefix(filepath.Base(path), "batch-00000007-") {
		t.Errorf("name %s does not carry the sequence", filepath.Base(path))
	}
}

// TestFramedBatchRoundTripsThroughTheSharedReader is the reason the format is one shared package:
// what the exporter writes is exactly what the restore path reads.
func TestFramedBatchRoundTripsThroughTheSharedReader(t *testing.T) {
	dir := t.TempDir()
	w := framedWriter(t, dir, dropboxframe.MinFrameBytesFloor)
	in := recs(400)
	path, err := w.WriteBatch(1, in)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got int
	res, err := dropboxframe.Read(raw, func([]byte) error { got++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got != len(in) {
		t.Errorf("wrote %d records, read back %d", len(in), got)
	}
	if res.Damaged != 0 {
		t.Errorf("a freshly written batch reported %d damaged frames", res.Damaged)
	}
	if res.Frames < 2 {
		t.Errorf("400 records made %d frame(s) at the floor; the size bound is not being applied", res.Frames)
	}
}

// TestDefaultContainerIsStillTarGz: the dropbox exists for an out-of-band consumer, and those
// consumers read gzip tarballs. The new container must be opt-in.
func TestDefaultContainerIsStillTarGz(t *testing.T) {
	cfg := Config{Table: "history", Directory: t.TempDir()}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Container != ContainerTarGz {
		t.Fatalf("default container is %q, want %q", cfg.Container, ContainerTarGz)
	}
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path, err := w.WriteBatch(1, recs(5))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(path, ".tar.gz") {
		t.Errorf("default container wrote %s, want a .tar.gz", filepath.Base(path))
	}
}

// TestFrameSizeBelowFloorIsRejected: below the floor a framed stream is larger than the tarball it
// replaces, so accepting the setting would be accepting a regression.
func TestFrameSizeBelowFloorIsRejected(t *testing.T) {
	cfg := Config{Table: "history", Directory: t.TempDir(), Container: ContainerFrames, MinFrameBytes: 512}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("a frame size below the floor was accepted")
	}
	if !strings.Contains(err.Error(), "floor") {
		t.Errorf("err = %v, want it to name the floor", err)
	}
}

// TestFrameSizeOnTarGzIsRejected keeps the two knobs from looking interchangeable.
func TestFrameSizeOnTarGzIsRejected(t *testing.T) {
	cfg := Config{Table: "history", Directory: t.TempDir(), MinFrameBytes: 1 << 20}
	if err := cfg.Validate(); err == nil {
		t.Fatal("minFrameBytes was accepted with the tar.gz container")
	}
}

// TestUnknownContainerIsRejected.
func TestUnknownContainerIsRejected(t *testing.T) {
	cfg := Config{Table: "history", Directory: t.TempDir(), Container: "zip"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("an unknown container was accepted")
	}
}
