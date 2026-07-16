package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

func allowAll(string, string, string) bool { return true }

func mustAd(t *testing.T, s string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.ParseOld(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return ad
}

// TestServiceSnapshotRestoreFile covers the daemon file-based backup path: snapshot to a
// file, wipe, restore from the file. Encryption is on, so the on-disk backup is sealed.
func TestServiceSnapshotRestoreFile(t *testing.T) {
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
	for i, k := range []string{"a", "b", "c"} {
		tx.NewClassAd(k, mustAd(t, "N = "+string(rune('0'+i))+"\nClaimId = \"secret-file-backup\""))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// A second table proves the backup covers the whole catalog, not just the default.
	other, err := svc.Catalog().CreateTable("machines")
	if err != nil {
		t.Fatal(err)
	}
	otx := other.Begin()
	otx.NewClassAd("m1", mustAd(t, "Cpus = 8"))
	otx.Commit()

	backup := filepath.Join(t.TempDir(), "backup.cadb")
	if err := svc.SnapshotToFile(backup); err != nil {
		t.Fatalf("SnapshotToFile: %v", err)
	}
	// The backup file must not contain the secret in the clear.
	raw, _ := os.ReadFile(backup)
	if len(raw) == 0 {
		t.Fatal("empty backup file")
	}
	if containsSub(raw, "secret-file-backup") {
		t.Fatal("backup file leaked a private attribute")
	}

	svc.DB().Truncate()
	other.Truncate()
	if svc.DB().Len() != 0 || other.Len() != 0 {
		t.Fatal("truncate did not empty the tables")
	}
	if err := svc.RestoreFromFile(backup); err != nil {
		t.Fatalf("RestoreFromFile: %v", err)
	}
	if svc.DB().Len() != 3 {
		t.Fatalf("after restore default table Len = %d, want 3", svc.DB().Len())
	}
	machines, _ := svc.Catalog().Table("machines")
	if machines.Len() != 1 {
		t.Fatalf("after restore machines Len = %d, want 1", machines.Len())
	}
}

// TestRestoreOnStartup verifies the one-shot startup restore: it loads the trigger file
// and then moves it aside so a restart does not re-restore.
func TestRestoreOnStartup(t *testing.T) {
	dir := t.TempDir()
	poolKeys := []db.KEK{{ID: "POOL", Material: []byte("server-test-pool-key-material-ghijkl")}}
	svc, err := New(Config{Dir: dir, Authorize: allowAll, PoolKeys: poolKeys})
	if err != nil {
		t.Fatal(err)
	}
	tx := svc.DB().Begin()
	tx.NewClassAd("x", mustAd(t, "N = 42"))
	tx.Commit()

	trigger := filepath.Join(t.TempDir(), "restore-me.cadb")
	if err := svc.SnapshotToFile(trigger); err != nil {
		t.Fatal(err)
	}
	svc.Close()

	// A fresh service over the SAME dir but wiped, then restore-on-startup from the trigger.
	svc2, err := New(Config{Dir: dir, Authorize: allowAll, PoolKeys: poolKeys})
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	svc2.DB().Truncate()

	restored, err := svc2.RestoreOnStartup(trigger)
	if err != nil {
		t.Fatalf("RestoreOnStartup: %v", err)
	}
	if !restored {
		t.Fatal("RestoreOnStartup reported no restore")
	}
	if svc2.DB().Len() != 1 {
		t.Fatalf("after startup restore Len = %d, want 1", svc2.DB().Len())
	}
	// The trigger file was moved aside (one-shot).
	if _, err := os.Stat(trigger); !os.IsNotExist(err) {
		t.Error("restore trigger file should have been moved aside")
	}
	// A missing file is a no-op.
	if restored, err := svc2.RestoreOnStartup(trigger); err != nil || restored {
		t.Errorf("RestoreOnStartup on a missing file = (%v, %v), want (false, nil)", restored, err)
	}
}

func containsSub(b []byte, s string) bool {
	return len(b) >= len(s) && indexSub(string(b), s) >= 0
}

func indexSub(hay, needle string) int {
	for i := 0; i+len(needle) <= len(hay); i++ {
		if hay[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
