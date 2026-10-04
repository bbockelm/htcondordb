package federate

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// fileCursorStore persists a resume cursor like replicate.FileCursorStore, but durably: the new
// file is fsynced before it replaces the old one and the directory after, so once Save returns the
// cursor survives an OS crash. replicate.FileCursorStore renames without either sync, so after a
// crash the file can be missing or empty -- harmless here (an empty cursor replays, and the sinks
// reconcile and deduplicate) but a waste of a full replay.
type fileCursorStore struct{ path string }

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
	if err := fh.Sync(); err != nil {
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
	return dir.Sync()
}
