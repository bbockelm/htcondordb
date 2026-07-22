package scheddsync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
)

// PositionStore durably persists a syncer's opaque resume position. It is written
// at-least-once (after each committed batch), so on a crash between the DB commit and the
// save the stored position may lag the DB -- the syncers tolerate that (JobSync re-applies
// idempotently; HistorySync dedups against the DB). A nil store disables persistence.
type PositionStore interface {
	Save(blob []byte) error
	Load() (blob []byte, ok bool, err error)
}

// FileStore persists a position to a single file via a temp file + atomic rename.
type FileStore struct{ Path string }

// Save writes blob atomically.
func (f *FileStore) Save(blob []byte) error {
	if err := os.MkdirAll(filepath.Dir(f.Path), 0o750); err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	if err := os.WriteFile(tmp, blob, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}

// Load returns the stored blob; ok is false when nothing has been saved yet.
func (f *FileStore) Load() ([]byte, bool, error) {
	b, err := os.ReadFile(f.Path) //nolint:gosec // operator-controlled path
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// fileIdentity identifies a file across renames/rotations: the filesystem device + inode.
// A different (dev, ino) than a persisted one means the path now names a different file --
// i.e. the log was compacted/rotated. Size distinguishes an in-place truncation.
type fileIdentity struct {
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
	Size int64  `json:"size"`
}

// statIdentity returns the (dev, ino, size) of path.
func statIdentity(path string) (fileIdentity, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fileIdentity{}, err
	}
	return identityFromInfo(fi), nil
}

func identityFromInfo(fi os.FileInfo) fileIdentity {
	id := fileIdentity{Size: fi.Size()}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		id.Dev = uint64(st.Dev) //nolint:unconvert // Dev is int32 on darwin, uint64 on linux
		id.Ino = uint64(st.Ino)
	}
	return id
}

// sameFileIdentity reports whether two identities name the same underlying file.
func sameFileIdentity(a, b fileIdentity) bool { return a.Dev == b.Dev && a.Ino == b.Ino }

// jobPosition is JobSync's persisted resume point: how far into which job_queue.log file we
// applied. On restart a changed identity, or a size below the saved offset, means the log was
// compacted/rotated while we were down and the table must be rebuilt from scratch.
type jobPosition struct {
	File   fileIdentity `json:"file"`
	Offset int64        `json:"offset"`
}

func (p jobPosition) encode() ([]byte, error) { return json.Marshal(p) }

func decodeJobPosition(b []byte) (jobPosition, error) {
	var p jobPosition
	err := json.Unmarshal(b, &p)
	return p, err
}
