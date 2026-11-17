package dropboxrestore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/dropboxframe"
)

// writeFramedBatch builds a batch in the framed container, the way the exporter does.
func writeFramedBatch(t *testing.T, dir string, seq uint64, minFrame int, gjids ...string) string {
	t.Helper()
	var buf bytes.Buffer
	fw, err := dropboxframe.NewWriter(&buf, minFrame)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range gjids {
		ad := classad.New()
		ad.InsertAttrString("GlobalJobId", id)
		ad.InsertAttr("ClusterId", int64(i+1))
		ad.InsertAttrString("Owner", "someuser")
		ad.InsertAttrString("Cmd", "/home/someuser/run.sh")
		if err := fw.Write([]byte(ad.MarshalOld())); err != nil {
			t.Fatal(err)
		}
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("batch-%08d-%d.cadf", seq, time.Now().Unix()))
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRestoreReadsFramedBatches(t *testing.T) {
	dir := t.TempDir()
	writeFramedBatch(t, dir, 1, dropboxframe.MinFrameBytesFloor, "a", "b")
	writeFramedBatch(t, dir, 2, dropboxframe.MinFrameBytesFloor, "c")

	l, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range l.Batches {
		if b.Container != ContainerFrames {
			t.Errorf("%s detected as %q, want frames", filepath.Base(b.Path), b.Container)
		}
	}

	w := newFakeWriter()
	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Records != 3 || res.Appended != 3 {
		t.Fatalf("records=%d appended=%d, want 3/3", res.Records, res.Appended)
	}
	if got := strings.Join(w.appended, ","); got != "a,b,c" {
		t.Errorf("appended %q, want a,b,c", got)
	}
}

// TestBothContainersInOneDropbox: a dropbox switched from tar.gz to frames holds both, and the
// restore has to read it as one sequence. This is exactly what an upgrade looks like.
func TestBothContainersInOneDropbox(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a", "b")                                        // the old container
	writeFramedBatch(t, dir, 2, dropboxframe.MinFrameBytesFloor, "c", "d") // after the switch
	writeBatch(t, dir, 3, "e")                                             // and a straggler

	w := newFakeWriter()
	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Batches != 3 || res.Appended != 5 {
		t.Fatalf("batches=%d appended=%d, want 3/5", res.Batches, res.Appended)
	}
	if got := strings.Join(w.appended, ","); got != "a,b,c,d,e" {
		t.Errorf("appended %q, want a,b,c,d,e in sequence order across both containers", got)
	}
}

// TestFramedDamageCostsOneFrameNotTheBatch is the payoff: the same damage that would cost a whole
// tarball costs only the records in one frame.
func TestFramedDamageCostsOneFrameNotTheBatch(t *testing.T) {
	dir := t.TempDir()
	ids := make([]string, 600)
	for i := range ids {
		ids[i] = fmt.Sprintf("job%04d", i)
	}
	path := writeFramedBatch(t, dir, 1, dropboxframe.MinFrameBytesFloor, ids...)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF // one flipped byte in the middle
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	w := newFakeWriter()
	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatalf("a damaged framed batch aborted the restore: %v", err)
	}
	if len(res.BadBatches) != 1 {
		t.Errorf("BadBatches = %v, want the damage reported", res.BadBatches)
	}
	// The point: most of the batch survives a bad byte. A tarball would lose everything after it.
	if res.Appended < len(ids)*3/4 {
		t.Errorf("recovered only %d of %d records from a 1-byte flip; damage should cost one frame", res.Appended, len(ids))
	}
	t.Logf("recovered %d/%d records from a framed batch after a 1-byte flip", res.Appended, len(ids))
}
