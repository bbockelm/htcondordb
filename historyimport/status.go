package historyimport

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Status is the health/progress an import-job runner publishes to its status file
// for the daemon's importer manager to read (liveness + the collector ad). Beat is
// the runner's wall clock at its last refresh, advanced continuously while the
// process is alive (including between cycles), so the manager can treat a stale
// Beat as a wedged child. The rest is the last completed cycle's outcome plus a
// running total. Both the runner and the manager live in importable packages, so
// this one type is the shared contract -- no duplicated JSON shape.
type Status struct {
	Beat          int64  `json:"beat"`          // unix seconds, refreshed continuously while alive
	LastCycleUnix int64  `json:"lastCycle"`     // unix seconds of the last completed cycle (0 = none yet)
	Schedds       int    `json:"schedds"`       // schedds imported in the last cycle
	Failures      int    `json:"failures"`      // schedds that errored in the last cycle
	ImportedTotal uint64 `json:"importedTotal"` // cumulative records imported since the runner started
	LastError     string `json:"lastError,omitempty"`
}

// WriteStatusFile writes s to path atomically (temp + rename).
func WriteStatusFile(path string, s Status) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".history-import-status-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// ReadStatusFile reads a status file; ok is false when it is absent, unreadable,
// unparseable, or has never been beaten (Beat == 0).
func ReadStatusFile(path string) (Status, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Status{}, false
	}
	var s Status
	if json.Unmarshal(data, &s) != nil || s.Beat == 0 {
		return Status{}, false
	}
	return s, true
}

// StatusBeater maintains a runner's Status and persists it: a background Run beats
// (refreshes Beat and rewrites the file) on an interval so the manager sees the
// process is alive even during the idle wait between cycles, and RecordCycle folds
// in each cycle's outcome. Safe for concurrent use.
type StatusBeater struct {
	path string
	now  func() time.Time

	mu sync.Mutex
	s  Status
}

// NewStatusBeater returns a beater that writes to path.
func NewStatusBeater(path string) *StatusBeater {
	return &StatusBeater{path: path, now: time.Now}
}

// beat refreshes Beat and writes the current status.
func (b *StatusBeater) beat() error {
	b.mu.Lock()
	b.s.Beat = b.now().Unix()
	s := b.s
	b.mu.Unlock()
	return WriteStatusFile(b.path, s)
}

// RecordCycle folds one cycle's result into the status and writes it. It matches
// the Importer.OnCycle signature.
func (b *StatusBeater) RecordCycle(st Stats, err error) {
	b.mu.Lock()
	now := b.now().Unix()
	b.s.Beat = now
	b.s.LastCycleUnix = now
	b.s.Schedds = st.Schedds
	b.s.Failures = st.Failures
	b.s.ImportedTotal += uint64(st.Imported)
	if err != nil {
		b.s.LastError = err.Error()
	} else {
		b.s.LastError = ""
	}
	s := b.s
	b.mu.Unlock()
	_ = WriteStatusFile(b.path, s)
}

// Run beats immediately and then every interval until ctx is cancelled.
func (b *StatusBeater) Run(ctx context.Context, interval time.Duration) {
	_ = b.beat()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = b.beat()
		}
	}
}
