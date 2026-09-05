package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLockDatabaseDirExcludesSecond: a second lock on the same directory is refused while the first
// is held, and succeeds again once the first is released.
func TestLockDatabaseDirExcludesSecond(t *testing.T) {
	dir := t.TempDir()

	release1, err := lockDatabaseDir(dir)
	if err != nil {
		t.Fatalf("first lock should succeed: %v", err)
	}

	// A second acquirer (a would-be second daemon) is refused with a clear message.
	if _, err := lockDatabaseDir(dir); err == nil {
		t.Fatal("second lock on a held directory should fail")
	} else if !strings.Contains(err.Error(), "already using") {
		t.Errorf("error should explain the double-start; got %v", err)
	}

	// After release, the directory can be locked again (a clean restart).
	release1()
	release2, err := lockDatabaseDir(dir)
	if err != nil {
		t.Fatalf("lock after release should succeed: %v", err)
	}
	release2()

	// The lock file is created inside the directory.
	if _, err := os.Stat(filepath.Join(dir, dbDirLockName)); err != nil {
		t.Errorf("lock file %s should exist in the dir: %v", dbDirLockName, err)
	}
}

// TestLockDatabaseDirInMemoryNoop: an empty dir (in-memory database) needs no lock and returns a
// usable no-op release.
func TestLockDatabaseDirInMemoryNoop(t *testing.T) {
	release, err := lockDatabaseDir("")
	if err != nil {
		t.Fatalf("empty dir should be a no-op, got %v", err)
	}
	if release == nil {
		t.Fatal("release must be non-nil even for the no-op case")
	}
	release() // must not panic
}
