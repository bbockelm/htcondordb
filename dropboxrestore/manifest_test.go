package dropboxrestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbockelm/htcondordb/dropboxmanifest"
)

// manifestFor writes a correct manifest beside an existing batch, the way the exporter does.
func manifestFor(t *testing.T, path string, seq, prev uint64, records int) {
	t.Helper()
	sum, n, err := dropboxmanifest.HashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := &dropboxmanifest.Manifest{
		Format: dropboxmanifest.Format, File: filepath.Base(path),
		Seq: seq, PrevSeq: prev, Records: records,
		Bytes: n, SHA256: sum, Container: "targz",
		Exporter: "histbackup", Table: "history",
	}
	if err := dropboxmanifest.Write(path, m, nil); err != nil {
		t.Fatal(err)
	}
}

// TestManifestsAreNotMistakenForBatches: the sidecars sit in the same directory.
func TestManifestsAreNotMistakenForBatches(t *testing.T) {
	dir := t.TempDir()
	p := writeBatch(t, dir, 1, "a", "b")
	manifestFor(t, p, 1, 0, 2)

	l, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Batches) != 1 {
		t.Fatalf("List found %d batches, want 1 (the manifest is not a batch)", len(l.Batches))
	}
	if l.Batches[0].Manifest == nil {
		t.Fatal("the batch's manifest was not loaded")
	}
	if l.Batches[0].Manifest.Records != 2 || l.Batches[0].Manifest.Exporter != "histbackup" {
		t.Errorf("manifest came back wrong: %+v", l.Batches[0].Manifest)
	}
}

// TestShortReadIsDetected is the gap manifests close. A damaged batch yields fewer records; with
// no declared count, nothing notices. With one, it is an assertion failure.
func TestShortReadIsDetected(t *testing.T) {
	dir := t.TempDir()
	p := writeBatch(t, dir, 1, "a", "b", "c", "d")
	// The manifest records the truth, then the batch is damaged behind its back.
	manifestFor(t, p, 1, 0, 4)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw[:len(raw)*2/3], 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := Restore(context.Background(), newFakeWriter(), Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ShortReads) != 1 {
		t.Fatalf("ShortReads = %v, want the shortfall named", res.ShortReads)
	}
	if !strings.Contains(res.ShortReads[0], "of the 4 records it declares") {
		t.Errorf("ShortReads[0] = %q, want it to state the declared count", res.ShortReads[0])
	}
	var out strings.Builder
	res.Report(&out)
	if !strings.Contains(out.String(), "SHORT READ") {
		t.Errorf("the report does not surface the short read:\n%s", out.String())
	}
}

// TestChecksumMismatchIsDetected: damage that does not change the length, which a record count
// alone cannot catch either -- a flipped byte inside a record still parses to a record.
func TestChecksumMismatchIsDetected(t *testing.T) {
	dir := t.TempDir()
	p := writeBatch(t, dir, 1, "a", "b")
	manifestFor(t, p, 1, 0, 2)

	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := Restore(context.Background(), newFakeWriter(), Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.BadBatches) == 0 {
		t.Fatal("a corrupted batch was not reported")
	}
	joined := strings.Join(res.BadBatches, " ")
	if !strings.Contains(joined, "checksum") && !strings.Contains(joined, "bytes") {
		t.Errorf("BadBatches = %v, want the byte-level damage named", res.BadBatches)
	}
}

// TestCleanDropboxVerifiesClean: no false alarms on an intact dropbox.
func TestCleanDropboxVerifiesClean(t *testing.T) {
	dir := t.TempDir()
	p1 := writeBatch(t, dir, 1, "a", "b")
	manifestFor(t, p1, 1, 0, 2)
	p2 := writeBatch(t, dir, 2, "c")
	manifestFor(t, p2, 2, 1, 1)

	res, err := Restore(context.Background(), newFakeWriter(), Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.BadBatches) != 0 || len(res.ShortReads) != 0 {
		t.Fatalf("a clean dropbox reported problems: bad=%v short=%v", res.BadBatches, res.ShortReads)
	}
	if res.Checksums != 2 {
		t.Errorf("Checksums = %d, want both batches verified", res.Checksums)
	}
	if res.Unverifiable != 0 {
		t.Errorf("Unverifiable = %d, want 0", res.Unverifiable)
	}
	if res.Appended != 3 {
		t.Errorf("Appended = %d, want 3", res.Appended)
	}
}

// TestBatchWithoutAManifestIsUnverifiableNotDamaged: batches written before manifests existed are
// intact; they simply cannot be checked. Treating them as damage would cry wolf on every upgrade.
func TestBatchWithoutAManifestIsUnverifiableNotDamaged(t *testing.T) {
	dir := t.TempDir()
	writeBatch(t, dir, 1, "a", "b") // no manifest
	p2 := writeBatch(t, dir, 2, "c")
	manifestFor(t, p2, 2, 1, 1)

	res, err := Restore(context.Background(), newFakeWriter(), Options{Dir: dir, Table: "history"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Unverifiable != 1 {
		t.Errorf("Unverifiable = %d, want 1", res.Unverifiable)
	}
	if len(res.BadBatches) != 0 {
		t.Errorf("a batch with no manifest was reported as damaged: %v", res.BadBatches)
	}
	if res.Appended != 3 {
		t.Errorf("Appended = %d, want all 3 records restored anyway", res.Appended)
	}
	var out strings.Builder
	res.Report(&out)
	if !strings.Contains(out.String(), "no manifest") {
		t.Errorf("the report does not mention the unverifiable batch:\n%s", out.String())
	}
}

// TestSkipChecksumsSkipsTheCheck.
func TestSkipChecksumsSkipsTheCheck(t *testing.T) {
	dir := t.TempDir()
	p := writeBatch(t, dir, 1, "a", "b")
	manifestFor(t, p, 1, 0, 2)
	raw, _ := os.ReadFile(p)
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := Restore(context.Background(), newFakeWriter(), Options{Dir: dir, Table: "history", SkipChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Checksums != 0 {
		t.Errorf("Checksums = %d with SkipChecksums set", res.Checksums)
	}
}

// TestManifestRoundTripsEveryField guards the hand-rolled encoder against a field being added to
// the struct and silently not persisted.
func TestManifestRoundTripsEveryField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch-00000005-1700000000.cadf")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := &dropboxmanifest.Manifest{
		Format: dropboxmanifest.Format, File: filepath.Base(path),
		Seq: 5, PrevSeq: 4, Records: 1234,
		FirstTime: 1700000000, LastTime: 1700003600,
		Bytes: 7, SHA256: strings.Repeat("ab", 32), Container: "frames",
		Exporter: "histbackup", Table: "history",
	}
	if err := dropboxmanifest.Write(path, want, nil); err != nil {
		t.Fatal(err)
	}
	got, err := dropboxmanifest.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%+v", *got) != fmt.Sprintf("%+v", *want) {
		t.Errorf("round trip changed the manifest:\n got %+v\nwant %+v", *got, *want)
	}
}
