package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// dbDirLockName is the lock file kept in the database directory. The daemon holds an exclusive
// advisory lock (flock) on it for its whole run, so a second htcondordb pointed at the same
// directory refuses to start rather than double-opening it. Two processes with independent
// *db.Catalogs over the same mmap'd segment files corrupt them -- a silent failure, distinct from
// the in-process MVCC ConflictError two writers in ONE process raise, and not otherwise guarded
// (db.OpenCatalog takes no lock). In-memory mode (empty dir) needs no lock.
const dbDirLockName = "htcondordb.lock"

// lockDatabaseDir takes an exclusive advisory lock on dir so only one daemon uses the database at a
// time. It returns a release func (idempotent-safe to call via defer) and an error if the lock is
// already held by another process. dir == "" (in-memory) is a no-op. The lock is advisory: it only
// stops other flock-cooperating openers (all htcondordb daemons), which is exactly the double-start
// case; it releases automatically if the process dies, so a crash leaves no stale lock.
func lockDatabaseDir(dir string) (func(), error) {
	if dir == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("preparing database dir %q: %w", dir, err)
	}
	path := filepath.Join(dir, dbDirLockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("opening database lock %q: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another htcondordb process is already using database dir %q (lock %s is held); "+
				"refusing to start a second instance (two daemons on one directory would corrupt it)", dir, dbDirLockName)
		}
		return nil, fmt.Errorf("locking database dir %q: %w", dir, err)
	}
	// Record our pid in the lock file for operator diagnostics; best-effort, the lock holds
	// regardless. Keep f open for the process lifetime -- the lock releases when the fd closes
	// (release()) or the process exits.
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(fmt.Sprintf("%d\n", os.Getpid())), 0)
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
