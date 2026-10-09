package federate

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/PelicanPlatform/classad/db/replicate"
)

// fileCursorStore persists a resume cursor like replicate.FileCursorStore, but durably: the new
// file is fsynced before it replaces the old one and the directory after, so once Save returns the
// cursor survives an OS crash. replicate.FileCursorStore renames without either sync, so after a
// crash the file can be missing or empty -- harmless here (an empty cursor replays, and the sinks
// reconcile and deduplicate) but a waste of a full replay.
type fileCursorStore struct {
	path string
	// syncFile and syncDir fsync the new cursor file and its directory; nil means
	// (*os.File).Sync. Seams for the ordering test.
	syncFile, syncDir func(*os.File) error
}

func fsync(f *os.File, hook func(*os.File) error) error {
	if hook != nil {
		return hook(f)
	}
	return f.Sync()
}

func (f fileCursorStore) Load() ([]byte, error) {
	b, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

func (f fileCursorStore) Save(cursor []byte) error {
	tmp := f.path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := fh.Write(cursor); err != nil {
		_ = fh.Close()
		return err
	}
	if err := fsync(fh, f.syncFile); err != nil {
		_ = fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(f.path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return fsync(dir, f.syncDir)
}

// clearCursor durably commits an empty cursor and clears the sink's copy (*cur, guarded by mu). A
// sink calls it when a Reset begins: until the replay reaches Synced the hub's state is part old
// and part replayed, and the only safe resume point is a full replay -- never the pre-Reset
// cursor, which a source could resume incrementally (an HA peer still on the old watch epoch),
// leaving the unfinished replay's rows unreconciled.
func clearCursor(store replicate.CursorStore, mu *sync.Mutex, cur *[]byte) error {
	if err := store.Save([]byte{}); err != nil {
		return fmt.Errorf("federate: clearing the cursor for a Reset: %w", err)
	}
	mu.Lock()
	*cur = nil
	mu.Unlock()
	return nil
}
