package historyimport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileCursors is a durable Cursors: per-(job, schedd) resume positions kept in a
// JSON file, written atomically (temp + rename) on every update so a crash never
// leaves a torn file. Suitable for the out-of-process importer, which must survive
// restarts without re-importing (beyond the recovery-dedup window).
type FileCursors struct {
	path string
	mu   sync.Mutex
	m    map[string]string
}

// NewFileCursors loads the cursor file (an absent file starts empty).
func NewFileCursors(path string) (*FileCursors, error) {
	fc := &FileCursors{path: path, m: map[string]string{}}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			if uerr := json.Unmarshal(data, &fc.m); uerr != nil {
				return nil, fmt.Errorf("history-import: parsing cursor file %s: %w", path, uerr)
			}
		}
	case os.IsNotExist(err):
		// fresh start
	default:
		return nil, fmt.Errorf("history-import: reading cursor file %s: %w", path, err)
	}
	return fc, nil
}

func fileCursorKey(job, schedd string) string { return job + "\x00" + schedd }

// Get returns the stored cursor for (job, schedd).
func (c *FileCursors) Get(job, schedd string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[fileCursorKey(job, schedd)]
	return v, ok
}

// Set records the cursor and persists the whole map atomically.
func (c *FileCursors) Set(job, schedd, cur string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[fileCursorKey(job, schedd)] = cur
	return c.save()
}

// save writes the map to a temp file and renames it over the target.
func (c *FileCursors) save() error {
	data, err := json.Marshal(c.m)
	if err != nil {
		return err
	}
	dir := filepath.Dir(c.path)
	tmp, err := os.CreateTemp(dir, ".history-import-cursors-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, c.path)
}
