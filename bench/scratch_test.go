package bench

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An empty directory is taken and marked; a directory already marked is reused.
func TestGuardAcceptsEmptyAndItsOwn(t *testing.T) {
	dir := t.TempDir()
	if err := GuardScratchDir(dir); err != nil {
		t.Fatalf("empty directory refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ScratchMarker)); err != nil {
		t.Fatalf("no marker written: %v", err)
	}
	// Now non-empty, but ours.
	if err := os.WriteFile(filepath.Join(dir, "segment.0"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := GuardScratchDir(dir); err != nil {
		t.Fatalf("its own scratch directory refused on reuse: %v", err)
	}
}

// A missing directory is created and marked.
func TestGuardCreatesMissing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "scratch")
	if err := GuardScratchDir(dir); err != nil {
		t.Fatalf("missing directory refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ScratchMarker)); err != nil {
		t.Fatalf("no marker written: %v", err)
	}
}

// THE ONE THAT MATTERS: a directory holding someone else's data is refused, and refusing leaves
// no marker behind -- otherwise the second attempt would succeed and the guard would protect
// nothing but the first mistake.
func TestGuardRefusesForeignDirectoryAndLeavesNoMarker(t *testing.T) {
	dir := t.TempDir()
	// Something that looks like a real store.
	for _, n := range []string{"job_queue.log", "segment.0", "spool"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("real data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	err := GuardScratchDir(dir)
	if err == nil {
		t.Fatal("a directory holding foreign data was accepted as benchmark scratch")
	}
	if !errors.Is(err, ErrNotScratch) {
		t.Fatalf("error does not wrap ErrNotScratch: %v", err)
	}
	if !strings.Contains(err.Error(), "job_queue.log") {
		t.Errorf("the refusal does not name what it found: %v", err)
	}
	if !strings.Contains(err.Error(), "anonymous reads") {
		t.Errorf("the refusal does not say why it matters: %v", err)
	}
	if _, serr := os.Stat(filepath.Join(dir, ScratchMarker)); serr == nil {
		t.Fatal("a refused directory was marked; the next attempt would succeed")
	}
	// And it must still refuse on a second call.
	if err2 := GuardScratchDir(dir); err2 == nil {
		t.Fatal("the second attempt on a foreign directory succeeded")
	}
}

// An empty path is refused rather than defaulting to anything.
func TestGuardRefusesEmptyPath(t *testing.T) {
	if err := GuardScratchDir("  "); !errors.Is(err, ErrNotScratch) {
		t.Fatalf("empty path: %v, want ErrNotScratch", err)
	}
}
