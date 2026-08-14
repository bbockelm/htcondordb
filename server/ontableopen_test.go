package server

import (
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// TestNewReportsEveryTableOpen asserts the hook is actually WIRED to the catalog, which is the part that
// can silently be nil: the classad-side per-table hook existed for a release while the catalog built its
// own configs and never passed it, so the reporting worked in classad's tests and reported nothing for
// every table a daemon opened. A test that only exercised the timer would have stayed green through that.
func TestNewReportsEveryTableOpen(t *testing.T) {
	dir := t.TempDir()

	// First start: create the tables and an archive, then close so they exist on disk to be reopened.
	svc, err := New(Config{Dir: dir, Authorize: allowAll})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"jobs", "machines"} {
		if _, err := svc.Catalog().EnsureTable(name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Catalog().CreateArchiveTable("history", db.ArchiveConfig{}); err != nil {
		t.Fatal(err)
	}
	svc.Close()

	// Second start: every table and archive on disk must report itself, by name and kind.
	var (
		mu    sync.Mutex
		seen  = map[string]string{} // name -> kind
		total time.Duration
	)
	svc2, err := New(Config{
		Dir:       dir,
		Authorize: allowAll,
		OnTableOpen: func(kind, name string, d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			seen[name] = kind
			total += d
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()

	mu.Lock()
	defer mu.Unlock()
	for name, wantKind := range map[string]string{"jobs": "table", "machines": "table", "history": "archive"} {
		if got := seen[name]; got != wantKind {
			t.Errorf("open of %q reported kind %q, want %q (seen: %v)", name, got, wantKind, seen)
		}
	}
	if total <= 0 {
		t.Error("per-table durations sum to zero; the hook fired but measured nothing")
	}
}

// TestNewWithoutOnTableOpen covers the nil case: per-table reporting is opt-in.
func TestNewWithoutOnTableOpen(t *testing.T) {
	svc, err := New(Config{Dir: t.TempDir(), Authorize: allowAll})
	if err != nil {
		t.Fatal(err)
	}
	svc.Close()
}
