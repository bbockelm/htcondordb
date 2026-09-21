package scheddsync

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// A reconcile of a large log takes minutes, and the mirror is stale for all of them. The end line
// used to assert "(source rotated/compacted)" whatever the trigger actually was, so a production
// mirror rebuilt 285 times by commit-conflict escalation reported 285 rotations that never
// happened -- and there was no line at all when one STARTED, so the daemon simply went quiet.
//
// This pins that a reconcile says why, on the way in and on the way out.
func TestReconcileLogsItsReason(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	d := persistentDB(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
		"105 \n101 1.0 Job Machine\n103 1.0 ClusterId 1\n103 1.0 JobStatus 1\n106 \n")

	s := NewJobSync(d, JobSyncConfig{Filename: logPath, Logger: logger})
	if err := s.reconcileReload(context.Background(), "unit test reason"); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	if !strings.Contains(out, "reconcile starting") {
		t.Error("no line when the reconcile STARTED: the daemon goes quiet for the whole rebuild")
	}
	if !strings.Contains(out, "reconcile reload complete") {
		t.Error("no completion line")
	}
	if strings.Count(out, "unit test reason") < 2 {
		t.Errorf("the reason does not appear on both the start and completion lines:\n%s", out)
	}
	if strings.Contains(out, "rotated/compacted") {
		t.Error("still asserting a rotation as the cause regardless of the real trigger")
	}
}
