//go:build linux

package scheddsync

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// awaitChange waits for the watcher to report a change. Everything here is event-driven,
// so the timeout is a failure deadline, never a settling delay: a passing test returns as
// soon as the kernel delivers, and only a broken watch waits out the clock.
func awaitChange(t *testing.T, w fileWatcher, why string) {
	t.Helper()
	select {
	case <-w.Changed():
	case <-time.After(10 * time.Second):
		t.Fatalf("no change reported after %s", why)
	}
}

func mustAppend(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWatcherSurvivesRotation is the reason this watcher watches a directory at all.
//
// The schedd compacts job_queue.log by writing job_queue.log.tmp and renaming it over the
// original. A watch on the file alone does not survive that: the kernel delivers
// IN_IGNORED and then reports nothing about the path ever again, because the watch is
// bound to the inode that was renamed away, not to the name. A tailer relying on it would
// work in every test that never rotates and go permanently deaf on the first real
// compaction -- so the post-rotation append here is the assertion that matters.
func TestWatcherSurvivesRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(path, []byte("107 1 CreationTimestamp 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w := newWatcher(path, false, discardLog())
	defer w.Close()
	if w.Changed() == nil {
		t.Skip("no filesystem watch available in this environment")
	}

	mustAppend(t, path, "103 1.0 JobStatus 2\n")
	awaitChange(t, w, "an append to the original file")

	// Compact: a fresh file renamed over the old one, exactly as the schedd does it.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte("107 2 CreationTimestamp 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	awaitChange(t, w, "a rotation")

	// The watch must have been re-established on the replacement inode.
	mustAppend(t, path, "103 2.0 JobStatus 2\n")
	awaitChange(t, w, "an append AFTER rotation (the watch did not re-arm)")

	// And it must keep surviving; one re-arm is not enough for a long-lived tailer.
	if err := os.WriteFile(tmp, []byte("107 3 CreationTimestamp 3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	awaitChange(t, w, "a second rotation")
	mustAppend(t, path, "103 3.0 JobStatus 2\n")
	awaitChange(t, w, "an append after a SECOND rotation")
}

// TestWatcherFileAppearsLater covers the history tailer's normal startup: HISTORY names a
// file that does not exist until the first job completes. The watch cannot be armed on it
// at construction, so the directory watch has to notice the creation and arm it.
func TestWatcherFileAppearsLater(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history")
	w := newWatcher(path, false, discardLog())
	defer w.Close()
	if w.Changed() == nil {
		t.Skip("no filesystem watch available in this environment")
	}

	if err := os.WriteFile(path, []byte("Owner = \"alice\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	awaitChange(t, w, "the file being created")

	mustAppend(t, path, "*** ClusterId=1 ProcId=0\n")
	awaitChange(t, w, "an append to the newly-created file")
}

// TestWatcherIgnoresSiblings pins the basename filter. The watched directory is the
// schedd's SPOOL, which is full of files this tailer does not read; a watch that woke on
// all of them would poll constantly on a busy machine and undo the point of the exercise.
func TestWatcherIgnoresSiblings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(path, []byte("107 1 CreationTimestamp 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w := newWatcher(path, false, discardLog())
	defer w.Close()
	if w.Changed() == nil {
		t.Skip("no filesystem watch available in this environment")
	}
	// Drain anything the setup produced.
	select {
	case <-w.Changed():
	case <-time.After(500 * time.Millisecond):
	}

	for i := 0; i < 20; i++ {
		other := filepath.Join(dir, "spool_file_"+string(rune('a'+i)))
		if err := os.WriteFile(other, []byte("noise"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-w.Changed():
		t.Fatal("woke for activity on sibling files; the basename filter is not applied")
	case <-time.After(500 * time.Millisecond):
	}

	// The real file still wakes it, so the filter is not simply rejecting everything.
	mustAppend(t, path, "103 1.0 JobStatus 2\n")
	awaitChange(t, w, "an append to the watched file")
}

// TestWatcherCoalesces checks that a burst collapses into a wake the tailer can act on
// once. The tailer reads to EOF regardless of how many writes it was told about, so what
// matters is that a flood never blocks the watcher's reader goroutine or queues work
// proportional to the write rate.
func TestWatcherCoalesces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(path, []byte("107 1 CreationTimestamp 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w := newWatcher(path, false, discardLog())
	defer w.Close()
	if w.Changed() == nil {
		t.Skip("no filesystem watch available in this environment")
	}
	for i := 0; i < 5000; i++ {
		mustAppend(t, path, "103 1.0 JobStatus 2\n")
	}
	awaitChange(t, w, "a burst of appends")
	// The channel holds at most one pending token, so a second receive must not already
	// be satisfied by the burst.
	drained := 0
	for {
		select {
		case <-w.Changed():
			drained++
			if drained > 1 {
				t.Fatalf("burst queued %d wakes; events are not coalesced", drained+1)
			}
			continue
		case <-time.After(300 * time.Millisecond):
		}
		break
	}
}

// TestWatcherCloseIsCleanAndIdempotent guards the shutdown path: Close has to stop the
// reader goroutine before closing its descriptors, and a tailer that is stopped twice
// (Run returning alongside an explicit Close) must not panic or double-close a fd that
// may have been reused by then.
func TestWatcherCloseIsCleanAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job_queue.log")
	if err := os.WriteFile(path, []byte("107 1 CreationTimestamp 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		w := newWatcher(path, false, discardLog())
		mustAppend(t, path, "103 1.0 JobStatus 2\n")
		w.Close()
		w.Close()
	}
}

// TestWatcherDisabled pins the opt-out: a disabled watcher must hand back a nil channel,
// which is what makes its select arm inert rather than a busy spin on a closed channel.
func TestWatcherDisabled(t *testing.T) {
	w := newWatcher(filepath.Join(t.TempDir(), "x"), true, discardLog())
	defer w.Close()
	if w.Changed() != nil {
		t.Fatal("a disabled watcher must report a nil channel")
	}
}
