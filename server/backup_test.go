package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// writeSnapshotFile fakes a snapshot plus its manifest, so the manifest/verify logic is testable
// without standing up a catalog.
func writeSnapshotFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	sum, n, err := hashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manifest{
		Format: manifestFormat, File: name, Created: time.Now().UTC(),
		Bytes: n, SHA256: sum, Duration: 1500 * time.Millisecond,
		Tables: []string{"jobs", "users"}, SkippedArchives: []string{"history", "epoch_history"},
	}
	if err := writeManifest(path, m); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestManifestRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := writeSnapshotFile(t, dir, SnapshotName(time.Now()), "some snapshot bytes")

	m, err := ReadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if m.Format != manifestFormat {
		t.Errorf("Format = %d, want %d", m.Format, manifestFormat)
	}
	if m.Bytes != int64(len("some snapshot bytes")) {
		t.Errorf("Bytes = %d, want %d", m.Bytes, len("some snapshot bytes"))
	}
	if len(m.SHA256) != 64 {
		t.Errorf("SHA256 = %q, want 64 hex chars", m.SHA256)
	}
	if got := strings.Join(m.Tables, ","); got != "jobs,users" {
		t.Errorf("Tables = %q, want jobs,users", got)
	}
	// The list that an operator most needs out of a snapshot is the one naming what is NOT in it.
	if got := strings.Join(m.SkippedArchives, ","); got != "history,epoch_history" {
		t.Errorf("SkippedArchives = %q, want history,epoch_history", got)
	}
	if m.Duration != 1500*time.Millisecond {
		t.Errorf("Duration = %v, want 1.5s", m.Duration)
	}
}

func TestVerifyAcceptsAnIntactSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := writeSnapshotFile(t, dir, SnapshotName(time.Now()), "intact")
	res, err := VerifySnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Ok || !res.HasManifest {
		t.Fatalf("Ok=%v HasManifest=%v problem=%q, want a clean verify", res.Ok, res.HasManifest, res.Problem)
	}
}

func TestVerifyCatchesTruncation(t *testing.T) {
	dir := t.TempDir()
	path := writeSnapshotFile(t, dir, SnapshotName(time.Now()), "the whole snapshot")
	// A truncated copy is the realistic damage: an interrupted scp, a full disk.
	if err := os.WriteFile(path, []byte("the whole"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := VerifySnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if res.Ok {
		t.Fatal("a truncated snapshot verified clean")
	}
	if !strings.Contains(res.Problem, "truncated") {
		t.Errorf("Problem = %q, want it to name truncation", res.Problem)
	}
}

func TestVerifyCatchesSilentCorruption(t *testing.T) {
	dir := t.TempDir()
	body := "aaaaaaaaaaaaaaaaaaaa"
	path := writeSnapshotFile(t, dir, SnapshotName(time.Now()), body)
	// Same length, different bytes: size alone cannot catch this, only the digest can.
	if err := os.WriteFile(path, []byte("aaaaaaaaaabbbbbbbbbb"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := VerifySnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if res.Ok {
		t.Fatal("a corrupted snapshot of the same size verified clean")
	}
	if !strings.Contains(res.Problem, "checksum") {
		t.Errorf("Problem = %q, want it to name the checksum", res.Problem)
	}
}

func TestVerifyReportsAMissingManifestAsUnverifiableNotDamage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, SnapshotName(time.Now()))
	if err := os.WriteFile(path, []byte("written before manifests existed"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := VerifySnapshot(path)
	if err != nil {
		t.Fatalf("a snapshot with no manifest must not be an error: %v", err)
	}
	if res.HasManifest {
		t.Fatal("HasManifest true with no manifest on disk")
	}
	if res.Ok {
		t.Fatal("Ok true for a snapshot that cannot be checked")
	}
	// The distinction the caller renders: unverifiable is not damaged.
	if !strings.Contains(res.Problem, "cannot be verified") {
		t.Errorf("Problem = %q, want it to say it cannot be verified", res.Problem)
	}
}

func TestVerifyDirChecksEverySnapshotNewestFirst(t *testing.T) {
	dir := t.TempDir()
	older := writeSnapshotFile(t, dir, SnapshotName(time.Now().Add(-time.Hour)), "older")
	newest := writeSnapshotFile(t, dir, SnapshotName(time.Now()), "newest")
	// Damage the older one so the result is not uniformly clean.
	if err := os.WriteFile(older, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := VerifyDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("checked %d snapshots, want 2", len(res))
	}
	if res[0].File != newest {
		t.Errorf("first result is %s, want the newest %s", res[0].File, newest)
	}
	if !res[0].Ok {
		t.Errorf("the intact newest snapshot did not verify: %s", res[0].Problem)
	}
	if res[1].Ok {
		t.Error("the truncated older snapshot verified clean")
	}
}

func TestBackupStatsRecordSuccessAndFailure(t *testing.T) {
	var r backupStatsRecorder
	if s := r.get(); s.Total != 0 || !s.LastSuccess.IsZero() {
		t.Fatalf("a fresh recorder is not empty: %+v", s)
	}
	r.success(4096, 2*time.Second)
	s := r.get()
	if s.Total != 1 || s.Failures != 0 || s.LastBytes != 4096 || s.LastDuration != 2*time.Second {
		t.Fatalf("after one success: %+v", s)
	}
	if s.LastSuccess.IsZero() {
		t.Error("LastSuccess not stamped")
	}
	r.failure(os.ErrPermission)
	s = r.get()
	if s.Total != 2 || s.Failures != 1 {
		t.Fatalf("after a failure: %+v", s)
	}
	// A failure must not erase the last good backup: "when did a backup last work" is the
	// question the metric exists to answer.
	if s.LastSuccess.IsZero() || s.LastBytes != 4096 {
		t.Errorf("a failure clobbered the last success: %+v", s)
	}
	if s.LastError == "" {
		t.Error("LastError not recorded")
	}
}

// TestSnapshotWritesAVerifiableManifest is the end-to-end: a real snapshot of a real catalog is
// described by a manifest that verifies, names the tables it holds, and names the archive it does
// not. The last part is the point -- the artifact has to say what is missing from it.
func TestSnapshotWritesAVerifiableManifest(t *testing.T) {
	dir := t.TempDir()
	svc, err := New(Config{
		Dir:       dir,
		Authorize: allowAll,
		PoolKeys:  []db.KEK{{ID: "POOL", Material: []byte("server-test-pool-key-material-abcdef")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	tx := svc.DB().Begin()
	tx.NewClassAd("j1", mustAd(t, "N = 1"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Catalog().CreateArchiveTable("history", db.ArchiveConfig{}); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), SnapshotName(time.Now()))
	if err := svc.SnapshotToFile(path); err != nil {
		t.Fatal(err)
	}

	res, err := VerifySnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Ok {
		t.Fatalf("a freshly written snapshot did not verify: %s", res.Problem)
	}
	m := res.Manifest
	if m.Bytes == 0 || len(m.SHA256) != 64 {
		t.Errorf("manifest did not record size/digest: %+v", m)
	}
	// The archive exists in the catalog and is NOT in the file; the manifest must say so.
	found := false
	for _, a := range m.SkippedArchives {
		if a == "history" {
			found = true
		}
	}
	if !found {
		t.Errorf("SkippedArchives = %v, want it to name the history archive that is not in the snapshot", m.SkippedArchives)
	}

	// And the attempt is recorded for the metrics endpoint.
	st := svc.BackupStats()
	if st.Total != 1 || st.Failures != 0 || st.LastSuccess.IsZero() || st.LastBytes != m.Bytes {
		t.Errorf("BackupStats after one snapshot: %+v (manifest bytes %d)", st, m.Bytes)
	}
}

// TestSnapshotFailureIsCounted: a snapshot that cannot be written is recorded as a failure and
// does not stamp LastSuccess, because the metric an operator alerts on is the age of the last
// success.
func TestSnapshotFailureIsCounted(t *testing.T) {
	dir := t.TempDir()
	svc, err := New(Config{Dir: dir, Authorize: allowAll})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	// A path inside a file is not a directory, so Create fails.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := svc.SnapshotToFile(filepath.Join(blocker, "snap.cadb")); err == nil {
		t.Fatal("snapshot into a non-directory succeeded")
	}
	st := svc.BackupStats()
	if st.Total != 1 || st.Failures != 1 {
		t.Fatalf("BackupStats = %+v, want one attempt and one failure", st)
	}
	if !st.LastSuccess.IsZero() {
		t.Error("a failed snapshot stamped LastSuccess")
	}
	if st.LastError == "" {
		t.Error("LastError empty after a failure")
	}
}
