package historyimport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileCursorsPersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursors.json")

	fc, err := NewFileCursors(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fc.Get("j", "s"); ok {
		t.Error("fresh cursor store should be empty")
	}
	if err := fc.Set("j1", "schedA", "10.0"); err != nil {
		t.Fatal(err)
	}
	if err := fc.Set("j1", "schedB", "20.1"); err != nil {
		t.Fatal(err)
	}
	if err := fc.Set("j1", "schedA", "11.0"); err != nil { // overwrite
		t.Fatal(err)
	}

	// A fresh store over the same file recovers every cursor.
	fc2, err := NewFileCursors(path)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := fc2.Get("j1", "schedA"); !ok || v != "11.0" {
		t.Errorf("schedA = %q ok=%v, want 11.0", v, ok)
	}
	if v, ok := fc2.Get("j1", "schedB"); !ok || v != "20.1" {
		t.Errorf("schedB = %q ok=%v, want 20.1", v, ok)
	}
	if _, ok := fc2.Get("j1", "other"); ok {
		t.Error("unknown schedd should be absent")
	}
}

func TestFileCursorsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursors.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileCursors(path); err == nil {
		t.Error("a corrupt cursor file should error rather than silently start empty")
	}
}
