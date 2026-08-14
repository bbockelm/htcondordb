package server

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
)

// TestNewReportsSealMigration asserts the migration report reaches the daemon. It builds what an older
// binary left on disk -- a table whose private attributes are stored in the clear -- and checks that
// opening a service over it says so, by table and with a count.
//
// The report exists because the first start after the upgrade rewrites those segments, which on a large
// spool is slow enough to be read as a regression or as a hang. It happens once. A test that only checked
// the field is copied would not catch the failure this guards: the classad hook underneath it spent a
// release unreachable, because the catalog builds its own per-table configs.
func TestNewReportsSealMigration(t *testing.T) {
	root := t.TempDir()

	// A legacy store cannot be produced through the current API -- private attributes are now always
	// encrypted, with or without pool keys -- so write it directly through a collection with no data key,
	// into the layout the catalog expects.
	const secret = "ClaimId-legacy-capability"
	for _, name := range []string{"jobs", "machines"} {
		dir := filepath.Join(root, "tables", name)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		c, err := collections.Open(collections.Options{Shards: 1, Dir: dir, SegmentSize: 1 << 16})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 400; i++ {
			ad, err := classad.ParseOld(fmt.Sprintf("Cpus = %d\nClaimId = %q", i, secret))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Put([]byte(fmt.Sprintf("k%d", i)), ad); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu  sync.Mutex
		got = map[string]int{}
	)
	svc, err := New(Config{
		Dir:       root,
		Authorize: allowAll,
		OnSealMigration: func(table string, segments int) {
			mu.Lock()
			got[table] += segments
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("a service opened over legacy tables reported no migration; the one-time cost of the " +
			"first start after the upgrade is unexplained again")
	}
	for _, name := range []string{"jobs", "machines"} {
		if got[name] == 0 {
			t.Errorf("no migration reported for %q (reported: %v); the count has to name its table",
				name, got)
		}
	}
	t.Logf("reported migrations: %v", got)
}
