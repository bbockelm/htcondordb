package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"

	"github.com/bbockelm/htcondordb/server"
)

func newBackupTestService(t *testing.T) *server.Service {
	t.Helper()
	svc, err := server.New(server.Config{
		Dir:       t.TempDir(),
		Authorize: func(string, string, string) bool { return true },
		PoolKeys:  []db.KEK{{ID: "POOL", Material: []byte("backup-control-test-key-material-abc")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.Close() })
	return svc
}

func req(t *testing.T, attrs map[string]string) *classad.ClassAd {
	t.Helper()
	ad := classad.New()
	for k, v := range attrs {
		ad.InsertAttrString(k, v)
	}
	return ad
}

// TestBackupControllerTakesABackup: the default action writes a snapshot into the configured
// directory and reports where it went.
func TestBackupControllerTakesABackup(t *testing.T) {
	dir := t.TempDir()
	bc := &backupController{svc: newBackupTestService(t), dir: dir}

	resp := bc.handle(classad.New()) // no Action -> backup
	if ok, _ := resp.EvaluateAttrBool("Ok"); !ok {
		msg, _ := resp.EvaluateAttrString("Error")
		t.Fatalf("backup failed: %s", msg)
	}
	file, _ := resp.EvaluateAttrString("File")
	if filepath.Dir(file) != dir {
		t.Errorf("snapshot went to %s, want it in the configured directory %s", file, dir)
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("reported file does not exist: %v", err)
	}
	// It must also be verifiable straight away -- a backup command that produced an unverifiable
	// artifact would defeat the manifest.
	res, err := server.VerifySnapshot(file)
	if err != nil || !res.Ok {
		t.Fatalf("on-demand snapshot did not verify: err=%v problem=%s", err, res.Problem)
	}
}

// TestBackupControllerRefusesWithNoDirectoryAndNoPath: the one case where it cannot guess.
func TestBackupControllerRefusesWithNoDirectoryAndNoPath(t *testing.T) {
	bc := &backupController{svc: newBackupTestService(t)}
	resp := bc.handle(classad.New())
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
		t.Fatal("backup with no directory and no path succeeded")
	}
	msg, _ := resp.EvaluateAttrString("Error")
	if !strings.Contains(msg, "HTCONDORDB_SNAPSHOT_DIR") {
		t.Errorf("Error = %q, want it to name the missing setting", msg)
	}
}

// TestBackupControllerRejectsARelativePath: the daemon's working directory is not the operator's,
// so a relative path would write somewhere neither of them means.
func TestBackupControllerRejectsARelativePath(t *testing.T) {
	bc := &backupController{svc: newBackupTestService(t), dir: t.TempDir()}
	resp := bc.handle(req(t, map[string]string{"Path": "backups/now.cadb"}))
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
		t.Fatal("a relative path was accepted")
	}
	msg, _ := resp.EvaluateAttrString("Error")
	if !strings.Contains(msg, "absolute") {
		t.Errorf("Error = %q, want it to require an absolute path", msg)
	}
}

// TestBackupControllerVerifiesTheDirectory: verify with no path walks the configured directory and
// counts good, damaged and unverifiable separately -- a snapshot with no manifest is not damage.
func TestBackupControllerVerifiesTheDirectory(t *testing.T) {
	dir := t.TempDir()
	bc := &backupController{svc: newBackupTestService(t), dir: dir}

	if resp := bc.handle(classad.New()); func() bool { ok, _ := resp.EvaluateAttrBool("Ok"); return !ok }() {
		t.Fatal("setup backup failed")
	}
	// A second snapshot, then damage it.
	resp := bc.handle(classad.New())
	damaged, _ := resp.EvaluateAttrString("File")
	if err := os.WriteFile(damaged, []byte("truncated"), 0o600); err != nil {
		t.Fatal(err)
	}
	// And a snapshot with no manifest at all.
	legacy := filepath.Join(dir, "htcondordb-19990101T000000Z.cadb")
	if err := os.WriteFile(legacy, []byte("from before manifests"), 0o600); err != nil {
		t.Fatal(err)
	}

	resp = bc.handle(req(t, map[string]string{"Action": "verify"}))
	checked, _ := resp.EvaluateAttrInt("Checked")
	good, _ := resp.EvaluateAttrInt("Good")
	bad, _ := resp.EvaluateAttrInt("Bad")
	unver, _ := resp.EvaluateAttrInt("Unverifiable")
	if checked != 3 || good != 1 || bad != 1 || unver != 1 {
		detail, _ := resp.EvaluateAttrString("Detail")
		t.Fatalf("checked=%d good=%d bad=%d unverifiable=%d, want 3/1/1/1\n%s", checked, good, bad, unver, detail)
	}
	// Damage makes Ok false; an unverifiable snapshot on its own must not.
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
		t.Error("Ok true with a damaged snapshot present")
	}
	detail, _ := resp.EvaluateAttrString("Detail")
	if !strings.Contains(detail, "DAMAGED") || !strings.Contains(detail, "unverifiable") {
		t.Errorf("Detail does not distinguish damaged from unverifiable:\n%s", detail)
	}
}

// TestBackupControllerRejectsAnUnknownAction keeps the protocol honest about what it accepts.
func TestBackupControllerRejectsAnUnknownAction(t *testing.T) {
	bc := &backupController{svc: newBackupTestService(t), dir: t.TempDir()}
	resp := bc.handle(req(t, map[string]string{"Action": "restore"}))
	if ok, _ := resp.EvaluateAttrBool("Ok"); ok {
		t.Fatal("an unknown action was accepted")
	}
	msg, _ := resp.EvaluateAttrString("Error")
	if !strings.Contains(msg, "restore") {
		t.Errorf("Error = %q, want it to name the rejected action", msg)
	}
}

// TestBackupsInTheSameSecondDoNotOverwrite: the snapshot name has one-second granularity, so two
// on-demand backups taken in quick succession must not collapse onto one file. An operator who
// asks for a backup twice has two backups.
func TestBackupsInTheSameSecondDoNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	bc := &backupController{svc: newBackupTestService(t), dir: dir}

	first, _ := bc.handle(classad.New()).EvaluateAttrString("File")
	second, _ := bc.handle(classad.New()).EvaluateAttrString("File")
	if first == "" || second == "" {
		t.Fatalf("a backup did not report a file: %q %q", first, second)
	}
	if first == second {
		t.Fatalf("both backups wrote %s", first)
	}
	for _, f := range []string{first, second} {
		res, err := server.VerifySnapshot(f)
		if err != nil || !res.Ok {
			t.Errorf("%s did not verify: err=%v problem=%s", filepath.Base(f), err, res.Problem)
		}
	}
	// Both must still look like snapshots to retention, or the extras would never be pruned.
	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	snaps := 0
	for _, e := range names {
		if strings.HasPrefix(e.Name(), "htcondordb-") && strings.HasSuffix(e.Name(), ".cadb") {
			snaps++
		}
	}
	if snaps != 2 {
		t.Errorf("retention sees %d snapshots, want 2", snaps)
	}
}
