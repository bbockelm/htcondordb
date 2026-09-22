package scheddsync

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// A reload replays a LIVE log while the schedd keeps writing it, so conflicts are expected: a
// conflicting key means a newer value already landed. Aborting on the first one made
// `.resync jobs` useless on a busy access point -- it gave up 3.5 seconds into an operator
// request, on 131 conflicted keys, having replayed a fraction of the log.
//
// An unapplied write cannot be retried and is dropped either way, but the reload must still say
// so: the incremental path names every dropped key, and a reload that drops them in bulk was
// silent, so the counter moved by 380 with nothing in the log.
//
// Anything else must still abort -- a reload that swallows a real fault would leave a half-built
// mirror and call it finished.
func TestReloadSurvivesConflictsButNotRealErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		inject    error
		wantAbort bool
	}{
		{"conflict is stepped past", &db.ConflictError{Keys: []string{"1.0"}}, false},
		{"unapplied is counted and stepped past", &db.UnappliedError{Keys: []string{"1.0"}}, false},
		{"a real error still aborts", errors.New("disk on fire"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			logPath := filepath.Join(dir, "job_queue.log")
			writeFile(t, logPath, `105
101 1.0 Job Machine
103 1.0 ProcId 0
103 1.0 Owner "alice"
103 1.0 JobStatus 1
106
105
101 2.0 Job Machine
103 2.0 ProcId 0
103 2.0 Owner "bob"
103 2.0 JobStatus 1
106
`)
			target, err := db.Open("")
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			s := NewJobSync(target, JobSyncConfig{Filename: logPath})

			n := 0
			reconcileApplyHook = func(error) error {
				if n++; n == 1 {
					return tc.inject
				}
				return nil
			}
			defer func() { reconcileApplyHook = nil }()

			rerr := s.reconcileReload(context.Background(), "test")
			if tc.wantAbort {
				if rerr == nil {
					t.Fatal("reload returned nil: a real error was swallowed and the mirror is half built")
				}
				if !errors.Is(rerr, tc.inject) {
					t.Errorf("reload returned %v, want the injected error", rerr)
				}
				return
			}
			if rerr != nil {
				t.Fatalf("reload aborted on a retryable condition: %v", rerr)
			}
			if n < 2 {
				t.Fatalf("the hook fired %d time(s): the reload stopped instead of continuing", n)
			}
		})
	}
}
