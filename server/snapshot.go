package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Backup file naming: htcondordb-<UTC timestamp>.cadb under the snapshot directory.
const (
	snapshotPrefix = "htcondordb-"
	snapshotSuffix = ".cadb"
	snapshotStamp  = "20060102T150405Z"
)

// SnapshotToFile writes a consistent backup of the default table to path (atomically via
// a temp file + rename). The backup is encrypted at rest when the database is; decrypting
// it later needs a pool key (embedded envelope) or the escrowed backup key.
func (s *Service) SnapshotToFile(path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := s.DB().Snapshot(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// RestoreFromFile replaces the default table with the snapshot in path (truncate + reload
// under the DB-wide lock). An encrypted snapshot is opened with the database's pool keys.
func (s *Service) RestoreFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return s.DB().Restore(f)
}

// RunPeriodicSnapshots writes a timestamped backup to dir every interval until ctx is
// cancelled, keeping the most recent keep files (0 = keep all). It is a no-op if dir is
// empty or interval <= 0. Intended to be run in its own goroutine.
func (s *Service) RunPeriodicSnapshots(ctx context.Context, dir string, interval time.Duration, keep int) {
	if dir == "" || interval <= 0 {
		return
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		s.log.Warn("periodic snapshots disabled: cannot create directory", "dir", dir, "err", err.Error())
		return
	}
	s.log.Info("periodic snapshots enabled", "dir", dir, "interval", interval.String(), "keep", keep)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			name := filepath.Join(dir, snapshotPrefix+time.Now().UTC().Format(snapshotStamp)+snapshotSuffix)
			if err := s.SnapshotToFile(name); err != nil {
				s.log.Warn("periodic snapshot failed", "file", name, "err", err.Error())
				continue
			}
			s.log.Info("wrote periodic snapshot", "file", name)
			if keep > 0 {
				pruneSnapshots(dir, keep, s)
			}
		}
	}
}

// pruneSnapshots removes all but the most recent keep backups in dir (lexical order on
// the timestamped name is chronological).
func pruneSnapshots(dir string, keep int, s *Service) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), snapshotPrefix) && strings.HasSuffix(e.Name(), snapshotSuffix) {
			names = append(names, e.Name())
		}
	}
	if len(names) <= keep {
		return
	}
	sort.Strings(names) // chronological
	for _, old := range names[:len(names)-keep] {
		if err := os.Remove(filepath.Join(dir, old)); err != nil {
			s.log.Warn("could not prune old snapshot", "file", old, "err", err.Error())
		}
	}
}

// RestoreOnStartup restores from path if it exists, then renames it aside so a restart
// does not repeatedly revert the database. Returns whether a restore happened. A missing
// file is a normal no-op (nil error, false). Call before serving clients.
func (s *Service) RestoreOnStartup(path string) (bool, error) {
	if path == "" {
		return false, nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if err := s.RestoreFromFile(path); err != nil {
		return false, err
	}
	// One-shot: move the trigger file aside so the next boot serves live data, not the
	// snapshot again.
	aside := fmt.Sprintf("%s.restored-%s", path, time.Now().UTC().Format(snapshotStamp))
	if err := os.Rename(path, aside); err != nil {
		s.log.Warn("restored, but could not move the restore file aside", "file", path, "err", err.Error())
	}
	return true, nil
}
