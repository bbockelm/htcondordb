package federate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// TestRetireRemovesCursorsBeforeRows: retirement forgets a source's cursors before deleting its
// rows, and fails -- deleting nothing -- when the cursors cannot be removed. The other order lost
// data: rows deleted, cursors left, and a rediscovered source resumed from its old cursor and never
// re-sent what had been deleted.
func TestRetireRemovesCursorsBeforeRows(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	a := newTestSpoke(t)
	a.putJobs(1, 2, 3)
	dir := t.TempDir()
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	cat := openCatalog(t, dir+"/db")
	t.Cleanup(func() { _ = cat.Close() })
	hub := startHub(t, Config{Catalog: cat, Discovery: disc, Dial: func(string) cedarsync.Dial { return a.dial },
		FlushInterval: 20 * time.Millisecond, CursorDir: dir + "/cursors"})
	defer hub.stop(t)
	waitFor(t, "replicated + cursor", func() bool {
		return sameKeys(jobKeysOf(t, cat, "ap1"), 1, 2, 3) && cursors(t, hub.hub, "ap1")[TableJobs] != ""
	})

	cdir := hub.hub.cursorDir("ap1")
	if err := os.Chmod(cdir, 0o555); err != nil { // the cursor files cannot be removed
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(cdir, 0o755) })
	if err := hub.hub.Retire(context.Background(), "ap1"); err == nil {
		t.Fatal("retire succeeded although the source's cursors could not be removed")
	}
	if got := jobKeysOf(t, cat, "ap1"); !sameKeys(got, 1, 2, 3) {
		t.Fatalf("a failed retire deleted rows: %v", got)
	}

	if err := os.Chmod(cdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := hub.hub.Retire(context.Background(), "ap1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cdir); !os.IsNotExist(err) {
		t.Fatalf("cursor dir after retire: %v", err)
	}
	// Still in the AP set: it comes back and is replayed from scratch.
	hub.hub.Rediscover()
	waitFor(t, "rediscovered source replayed", func() bool { return sameKeys(jobKeysOf(t, cat, "ap1"), 1, 2, 3) })
}
