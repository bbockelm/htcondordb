package scheddsync

import (
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// TestSyncersAdvertiseInitialStatusAtConstruction verifies each syncer publishes a Kind-set status
// at construction, so its source appears in the very first collector ad (dbad skips a source whose
// status is still zero/empty-Kind). Without this the JobQueue*/History*/Epoch* attributes were
// absent until the first poll completed and the next advertise cycle ran.
func TestSyncersAdvertiseInitialStatusAtConstruction(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+submitJob(1, 0)) // non-empty: not yet caught up

	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	js := NewJobSync(d, JobSyncConfig{Filename: logPath})
	if st := js.Status(); st.Kind != "job_queue.log" || st.Source != logPath || st.CaughtUp {
		t.Errorf("jobs initial status = {Kind:%q Source:%q CaughtUp:%v}; want {job_queue.log %q false}",
			st.Kind, st.Source, st.CaughtUp, logPath)
	}

	arch, cleanup := newArchive(t)
	defer cleanup()

	if st := NewHistorySync(arch, HistorySyncConfig{Filename: logPath}).Status(); st.Kind != "history" {
		t.Errorf("history initial Kind=%q, want history", st.Kind)
	}
	if st := NewJobEpochSync(arch, HistorySyncConfig{Filename: logPath}).Status(); st.Kind != "job_epoch" {
		t.Errorf("epoch initial Kind=%q, want job_epoch", st.Kind)
	}
}
