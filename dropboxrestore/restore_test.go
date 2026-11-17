package dropboxrestore

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// writeBatch builds a tarball in the exporter's format: one old-ClassAd text file per record.
func writeBatch(t *testing.T, dir string, seq uint64, gjids ...string) string {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for i, id := range gjids {
		ad := classad.New()
		ad.InsertAttrString("GlobalJobId", id)
		ad.InsertAttr("ClusterId", int64(i+1))
		body := ad.MarshalOld()
		name := fmt.Sprintf("%06d-%s.classad", i, id)
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("batch-%08d-%d.tar.gz", seq, time.Now().Unix()))
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeWriter records what a restore wrote.
type fakeWriter struct {
	have     map[string]bool
	appended []string
	failOn   string
}

func newFakeWriter(present ...string) *fakeWriter {
	w := &fakeWriter{have: map[string]bool{}}
	for _, p := range present {
		w.have[p] = true
	}
	return w
}

func (w *fakeWriter) Append(_ context.Context, _ string, ad *classad.ClassAd) error {
	id, _ := ad.EvaluateAttrString("GlobalJobId")
	if id != "" && id == w.failOn {
		return fmt.Errorf("injected failure on %s", id)
	}
	w.appended = append(w.appended, id)
	w.have[id] = true
	return nil
}

func (w *fakeWriter) Has(_ context.Context, _ string, gjid string) (bool, error) {
	return w.have[gjid], nil
}

func TestRestoreReadsEveryBatchInOrder(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a", "b")
	writeBatch(t, dir, 2, "c")
	w := newFakeWriter()

	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Batches != 2 || res.Records != 3 || res.Appended != 3 {
		t.Fatalf("batches=%d records=%d appended=%d, want 2/3/3", res.Batches, res.Records, res.Appended)
	}
	if got := strings.Join(w.appended, ","); got != "a,b,c" {
		t.Errorf("appended %q, want a,b,c in sequence order", got)
	}
}

// TestRestoreIsIdempotent: the exporter's delivery is at-least-once and a restore may be re-run,
// so running it twice must not double the archive.
func TestRestoreIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a", "b")
	w := newFakeWriter()

	if _, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"}); err != nil {
		t.Fatal(err)
	}
	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Appended != 0 || res.Duplicates != 2 {
		t.Fatalf("second run appended=%d duplicates=%d, want 0/2", res.Appended, res.Duplicates)
	}
	if len(w.appended) != 2 {
		t.Errorf("the archive holds %d records after two restores, want 2", len(w.appended))
	}
}

// TestDuplicateTarballsRestoreOnce covers the exporter's actual at-least-once artifact: a crash
// re-exports the last batch under a new sequence number, so the same records appear twice.
func TestDuplicateTarballsRestoreOnce(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a", "b")
	writeBatch(t, dir, 2, "a", "b") // the re-export after a crash
	w := newFakeWriter()

	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Appended != 2 || res.Duplicates != 2 {
		t.Fatalf("appended=%d duplicates=%d, want 2/2", res.Appended, res.Duplicates)
	}
}

// TestDamagedBatchDoesNotStopTheRestore is the property the whole format exists for: damage costs
// the damaged region, never the records around it.
func TestDamagedBatchDoesNotStopTheRestore(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a", "b")
	bad := writeBatch(t, dir, 2, "c", "d")
	writeBatch(t, dir, 3, "e")

	// Truncate the middle tarball: gzip loses the tail from the damage point.
	raw, err := os.ReadFile(bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, raw[:len(raw)/2], 0o600); err != nil {
		t.Fatal(err)
	}

	w := newFakeWriter()
	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatalf("a damaged batch aborted the restore: %v", err)
	}
	if len(res.BadBatches) != 1 {
		t.Errorf("BadBatches = %v, want the one damaged tarball named", res.BadBatches)
	}
	// The intact batches on both sides of the damage must still be there.
	got := strings.Join(w.appended, ",")
	for _, want := range []string{"a", "b", "e"} {
		if !strings.Contains(got, want) {
			t.Errorf("record %q from an intact batch was lost; appended = %q", want, got)
		}
	}
}

// TestSequenceGapsAreReported: a dropbox missing a batch is indistinguishable from a complete one
// without this check, and restoring over the gap silently is how an incomplete archive gets
// trusted.
func TestSequenceGapsAreReported(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a")
	writeBatch(t, dir, 4, "d") // 2 and 3 never arrived
	w := newFakeWriter()

	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.MissingSeqs) != 2 {
		t.Fatalf("MissingSeqs = %v, want 2 and 3", res.MissingSeqs)
	}
	var out strings.Builder
	res.Report(&out)
	if !strings.Contains(out.String(), "INCOMPLETE") {
		t.Errorf("the report does not flag the gap:\n%s", out.String())
	}
}

// TestLossReportsAreSurfaced: the windows the backup never held must reach the operator at restore
// time, because nothing else will tell them.
func TestLossReportsAreSurfaced(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a")
	ad := classad.New()
	ad.InsertAttrString("MyType", "ArchiveDropboxDataLoss")
	ad.InsertAttr("EstimatedLossStartTime", int64(1700000000))
	ad.InsertAttr("EstimatedLossEndTime", int64(1700003600))
	if err := os.WriteFile(filepath.Join(dir, "loss-1700003601.ad"), []byte(ad.MarshalOld()), 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := Restore(context.Background(), newFakeWriter(), Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Losses) != 1 {
		t.Fatalf("Losses = %v, want the one loss report", res.Losses)
	}
	var out strings.Builder
	res.Report(&out)
	if !strings.Contains(out.String(), "DATA LOSS") {
		t.Errorf("the report does not surface the loss:\n%s", out.String())
	}
}

// TestPartialFilesAreIgnored: a dot-prefixed file is one the exporter is still writing. Reading it
// would restore a half-written batch from a running exporter.
func TestPartialFilesAreIgnored(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a")
	if err := os.WriteFile(filepath.Join(dir, ".batch-00000002.tmp"), []byte("half written"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Batches) != 1 {
		t.Errorf("List picked up %d batches, want 1 (the temp file is not data)", len(l.Batches))
	}
	if len(l.Partial) != 1 {
		t.Errorf("Partial = %v, want the temp file reported", l.Partial)
	}
}

// TestDryRunWritesNothing.
func TestDryRunWritesNothing(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a", "b")
	w := newFakeWriter()
	res, err := Restore(context.Background(), w, Options{Dir: dir, Table: "history", DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Records != 2 {
		t.Errorf("Records = %d, want 2 read", res.Records)
	}
	if res.Appended != 0 || len(w.appended) != 0 {
		t.Errorf("a dry run wrote %d records", len(w.appended))
	}
}
