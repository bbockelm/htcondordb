package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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

// SnapshotName is the file name a snapshot taken at t gets inside the snapshot directory. Shared
// by the periodic snapshots and the on-demand backup command so both sort and prune together.
func SnapshotName(t time.Time) string {
	return snapshotPrefix + t.UTC().Format(snapshotStamp) + snapshotSuffix
}

// SnapshotToFile writes a backup of the catalog's MUTABLE tables to path (atomically via a temp
// file + rename), and a sidecar manifest beside it carrying the size, a sha256 over the whole
// file, and the table lists -- see VerifySnapshot.
//
// Archive tables are NOT included. The catalog's snapshot walks the mutable-table registry only,
// so on a schedd-sync deployment this file holds the live queue and none of the completed-job
// history. The manifest names the archives it skipped so the artifact says so itself; back them
// up separately (see docs/backup-restore.md).
//
// The backup is encrypted at rest when the database is; decrypting it later needs a pool key
// (embedded per-table envelopes) or the escrowed backup key. It streams table by table, so a
// large multi-table DB is never buffered whole, and the digest is computed on the way past rather
// than by re-reading the file.
func (s *Service) SnapshotToFile(path string) error {
	started := time.Now()
	bytes, err := s.snapshotToFile(path)
	if err != nil {
		s.backups.failure(err)
		return err
	}
	s.backups.success(bytes, time.Since(started))
	return nil
}

// snapshotToFile does the work; SnapshotToFile records the outcome.
func (s *Service) snapshotToFile(path string) (int64, error) {
	started := time.Now()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	// Hash on the way past: a snapshot can be large, and re-reading it to digest it would double
	// the I/O and still not prove the bytes were the ones written.
	h := sha256.New()
	counted := &countingWriter{w: io.MultiWriter(f, h)}
	if err := s.Catalog().Snapshot(counted); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, err
	}
	// The snapshot is durable and in place. A manifest failure past this point is logged, not
	// returned: the backup is good, and verify reports a missing manifest as unverifiable rather
	// than as damage.
	m := &Manifest{
		Format:          manifestFormat,
		File:            filepath.Base(path),
		Created:         time.Now().UTC(),
		Bytes:           counted.n,
		SHA256:          hex.EncodeToString(h.Sum(nil)),
		Duration:        time.Since(started),
		Tables:          s.Catalog().Tables(),
		SkippedArchives: s.Catalog().ArchiveTables(),
	}
	if err := writeManifest(path, m); err != nil {
		s.log.Warn("snapshot written but its manifest was not", "file", path, "err", err.Error())
	}
	return counted.n, nil
}

// countingWriter counts the bytes written through it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// RestoreFromFile replaces the catalog's tables with those in the snapshot at path (each
// table truncated + reloaded under its DB-wide lock; missing tables created). An encrypted
// snapshot is opened with the catalog's pool keys.
func (s *Service) RestoreFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return s.Catalog().Restore(f)
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
			continue
		}
		// A manifest without its snapshot is litter that verify would report as a missing file.
		if err := os.Remove(manifestPath(filepath.Join(dir, old))); err != nil && !os.IsNotExist(err) {
			s.log.Warn("could not prune old snapshot manifest", "file", old, "err", err.Error())
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
