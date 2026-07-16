package scheddsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

// TestJobSync covers replay of committed transactions, incremental tailing (an update +
// a destroy), and a log rotation that fully re-syncs.
func TestJobSync(t *testing.T) {
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
	ctx := context.Background()

	if err := s.Poll(ctx); err != nil {
		t.Fatalf("initial poll: %v", err)
	}
	if target.Len() != 2 {
		t.Fatalf("after initial sync Len = %d, want 2", target.Len())
	}
	ad, ok := target.LookupClassAd("1.0")
	if !ok {
		t.Fatal("job 1.0 missing")
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "alice" {
		t.Fatalf("1.0 Owner = %q, want alice", v)
	}
	if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 1 {
		t.Fatalf("1.0 JobStatus = %d, want 1", v)
	}

	// Incremental: 1.0 goes to status 4 (completed), 2.0 is destroyed.
	appendFile(t, logPath, `105
103 1.0 JobStatus 4
102 2.0
106
`)
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("incremental poll: %v", err)
	}
	if target.Len() != 1 {
		t.Fatalf("after incremental Len = %d, want 1", target.Len())
	}
	ad, _ = target.LookupClassAd("1.0")
	if v, _ := ad.EvaluateAttrInt("JobStatus"); v != 4 {
		t.Fatalf("1.0 JobStatus = %d, want 4", v)
	}
	if _, ok := target.LookupClassAd("2.0"); ok {
		t.Error("destroyed job 2.0 still present")
	}

	// Rotation: the schedd rewrites job_queue.log with only a fresh job 3.0.
	writeFile(t, logPath, `105
101 3.0 Job Machine
103 3.0 ProcId 0
103 3.0 Owner "carol"
106
`)
	if err := s.Poll(ctx); err != nil {
		t.Fatalf("post-rotation poll: %v", err)
	}
	if target.Len() != 1 {
		t.Fatalf("after rotation Len = %d, want 1 (only the rewritten job)", target.Len())
	}
	if _, ok := target.LookupClassAd("1.0"); ok {
		t.Error("pre-rotation job 1.0 survived the re-sync")
	}
	ad, ok = target.LookupClassAd("3.0")
	if !ok {
		t.Fatal("post-rotation job 3.0 missing")
	}
	if v, _ := ad.EvaluateAttrString("Owner"); v != "carol" {
		t.Fatalf("3.0 Owner = %q, want carol", v)
	}
}

// TestJobSyncNonTransactional verifies ops written outside an explicit transaction (an
// implicit batch) are still applied.
func TestJobSyncNonTransactional(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job_queue.log")
	writeFile(t, logPath, "101 5.0 Job Machine\n103 5.0 Owner \"dave\"\n103 5.0 ProcId 0\n")

	target, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	s := NewJobSync(target, JobSyncConfig{Filename: logPath})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target.Len() != 1 {
		t.Fatalf("Len = %d, want 1", target.Len())
	}
	ad, _ := target.LookupClassAd("5.0")
	if v, _ := ad.EvaluateAttrString("Owner"); v != "dave" {
		t.Fatalf("5.0 Owner = %q, want dave", v)
	}
}
