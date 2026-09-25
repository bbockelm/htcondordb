package scheddsync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// An UnappliedError must not rewind the tailer. A conflict means "lost a race, try again"; an
// unapplied write means the store could not compose it at all, and re-applying the identical write
// cannot change that.
//
// On a production mirror this distinction was the whole sync: reported as a conflict, each refusal
// rewound to the durable offset, re-applied, failed identically, and after three attempts escalated
// to a 460-second replay of a 923 MB log that wrote nothing. 1,239 refusals produced 416 of them.
//
// This asserts the routing directly -- that the poll path treats the two errors differently -- since
// provoking a real unreadable base needs a classad-internal hook.
func TestUnappliedIsNotTreatedAsAConflict(t *testing.T) {
	var conflict *db.ConflictError
	var unapplied *db.UnappliedError

	cerr := error(&db.ConflictError{Keys: []string{"1.0"}})
	uerr := error(&db.UnappliedError{Keys: []string{"2.0"}})

	if !errors.As(cerr, &conflict) {
		t.Error("a ConflictError no longer matches the conflict branch; the retry path is dead")
	}
	if errors.As(cerr, &unapplied) {
		t.Error("a ConflictError matches the unapplied branch: conflicts would stop being retried")
	}
	if !errors.As(uerr, &unapplied) {
		t.Error("an UnappliedError does not match the unapplied branch: it would fall through as a " +
			"generic error and abort the poll")
	}
	if errors.As(uerr, &conflict) {
		t.Fatal("an UnappliedError matches the CONFLICT branch -- this is the livelock: the tailer " +
			"would rewind and re-apply a write that can never succeed, escalating to full replays")
	}
}

// TestPollDoesNotRewindOnUnapplied exercises the REAL poll path. The check above only proves the
// two error types are distinguishable -- deleting the handling from Poll entirely still passed it,
// because it tests errors.As and not this package. This one injects an UnappliedError where
// readAndApply returns and asserts the tailer keeps its offset and counts the write, where a
// conflict would rewind it.
func TestPollDoesNotRewindOnUnapplied(t *testing.T) {
	d := persistentDB(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
		"105 \n101 1.0 Job Machine\n103 1.0 ClusterId 1\n103 1.0 JobStatus 1\n106 \n")
	s := NewJobSync(d, JobSyncConfig{Filename: logPath})
	ctx := context.Background()
	if err := s.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	offsetAfterClean := s.Status().Offset
	if offsetAfterClean == 0 {
		t.Fatal("the first poll consumed nothing; the fixture is wrong")
	}

	// Next pass returns an unapplied write.
	appendFile(t, logPath, "105 \n103 1.0 JobStatus 2\n106 \n")
	hookCalls := 0
	applyErrorHook = func(error) error {
		hookCalls++
		return &db.UnappliedError{Keys: []string{"1.0"}}
	}
	defer func() { applyErrorHook = nil }()

	before := s.Status().Unapplied
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("an unapplied write aborted the poll: %v", err)
	}
	if got := s.Status().Offset; got < offsetAfterClean {
		t.Errorf("the tailer REWOUND on an unapplied write (offset %d -> %d): this is the livelock",
			offsetAfterClean, got)
	}
	if hookCalls == 0 {
		t.Fatal("the poll never reached readAndApply, so nothing was injected and this proves nothing")
	}
	if got := s.Status().Unapplied; got <= before {
		t.Errorf("the unapplied write was not counted (%d -> %d)", before, got)
	}
}

// TestUnappliedCounterIsAdvertised: the counter has to reach the collector ad, or an operator
// cannot tell a mirror that is quietly dropping updates from a healthy one.
func TestUnappliedCounterReachesStatus(t *testing.T) {
	d := persistentDB(t)
	s := NewJobSync(d, JobSyncConfig{Filename: "/nonexistent"})
	s.mUnapplied.Add(7)
	// Status() returns the last PUBLISHED snapshot, not a live read -- so publish before asserting,
	// or this passes/fails on the zero value regardless of the counter.
	s.publishStatus(true)
	if got := s.Status().Unapplied; got != 7 {
		t.Errorf("SyncStatus.Unapplied = %d, want 7", got)
	}
}

// A dropped key logged without its reason tells an operator that something was lost but not what
// to do about it -- a missing base, a chain broken at a dead link and bytes that will not decode
// are different faults with different responses, and the only way to tell them apart was to
// correlate by hand against the daemon ad's counters.
func TestUnappliedIsFormattedWithItsReason(t *testing.T) {
	got := formatUnapplied(&db.UnappliedError{
		Keys:    []string{"15781354.0", "0.830"},
		Reasons: []string{"delta-no-base", "delta-chain-broken"},
	})
	want := []string{"15781354.0 (delta-no-base)", "0.830 (delta-chain-broken)"}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %q, want %q", i, got[i], want[i])
		}
	}

	// An older classad, or a path that does not set them, must still log the keys rather than
	// dropping the line or printing a misaligned pairing.
	bare := formatUnapplied(&db.UnappliedError{Keys: []string{"1.0", "2.0"}})
	if len(bare) != 2 || bare[0] != "1.0" || bare[1] != "2.0" {
		t.Errorf("without reasons got %v, want the bare keys", bare)
	}
	// Mismatched lengths must not pair the wrong reason to a key.
	mism := formatUnapplied(&db.UnappliedError{Keys: []string{"1.0", "2.0"}, Reasons: []string{"delta-no-base"}})
	if len(mism) != 2 || mism[0] != "1.0" {
		t.Errorf("with mismatched reasons got %v, want the bare keys", mism)
	}
}

// And the WARN line must actually carry the reasons. The formatting test above passes with the
// call site reverted to bare keys, so it proves the helper works, not that anything uses it --
// and the operator-visible symptom was precisely a log line that named keys and nothing else.
func TestTheUnappliedWarnLineCarriesTheReason(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	d := persistentDB(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "107 1 CreationTimestamp 1000\n"+
		"105 \n101 1.0 Job Machine\n103 1.0 ClusterId 1\n103 1.0 JobStatus 1\n106 \n")

	s := NewJobSync(d, JobSyncConfig{Filename: logPath, Logger: logger})
	applyErrorHook = func(error) error {
		return &db.UnappliedError{Keys: []string{"1.0"}, Reasons: []string{"delta-no-base"}}
	}
	defer func() { applyErrorHook = nil }()

	if err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll returned %v: an unapplied write must not be surfaced as a failure", err)
	}
	out := buf.String()
	if !strings.Contains(out, "could not be applied") {
		t.Fatalf("no warning was logged at all; output: %s", out)
	}
	if !strings.Contains(out, "delta-no-base") {
		t.Errorf("the warning does not name the reason, which is the whole point; output: %s", out)
	}
}
