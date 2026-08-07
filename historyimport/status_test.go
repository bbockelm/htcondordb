package historyimport

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStatusFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")

	if _, ok := ReadStatusFile(path); ok {
		t.Error("absent status file should read ok=false")
	}
	if err := WriteStatusFile(path, Status{Beat: 1700, Schedds: 2, ImportedTotal: 9}); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadStatusFile(path)
	if !ok || got.Beat != 1700 || got.Schedds != 2 || got.ImportedTotal != 9 {
		t.Errorf("read %+v ok=%v, want beat=1700 schedds=2 imported=9", got, ok)
	}
	// A never-beaten status (Beat==0) reads as not-ready.
	if err := WriteStatusFile(path, Status{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadStatusFile(path); ok {
		t.Error("Beat==0 status should read ok=false")
	}
}

func TestStatusBeaterRecordCycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")
	b := NewStatusBeater(path)
	b.now = func() time.Time { return time.Unix(1000, 0) }
	_ = b.beat()

	b.RecordCycle(Stats{Schedds: 3, Failures: 1, Imported: 5}, nil)
	s, ok := ReadStatusFile(path)
	if !ok || s.Schedds != 3 || s.Failures != 1 || s.ImportedTotal != 5 || s.LastCycleUnix != 1000 {
		t.Fatalf("after cycle 1: %+v ok=%v", s, ok)
	}

	// A second cycle accumulates ImportedTotal and clears the error.
	b.RecordCycle(Stats{Schedds: 2, Imported: 4}, nil)
	s, _ = ReadStatusFile(path)
	if s.ImportedTotal != 9 || s.Schedds != 2 || s.LastError != "" {
		t.Errorf("after cycle 2: %+v, want importedTotal=9 schedds=2 no-error", s)
	}
}
