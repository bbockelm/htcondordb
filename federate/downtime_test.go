package federate

import (
	"context"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/cedarsync"
)

// TestHubDowntimeDoesNotRetire: a hub down longer than RetireAfter restarts into a collector that
// (transiently) lists nothing, while ap1's spoke was up and reachable the whole time. The hub's own
// downtime is not time the source went unseen, and a spoke the hub reaches is contact: ap1 keeps its
// rows. Retirement used to run before contact was refreshed and counted the downtime, deleting them
// at the first state pass.
func TestHubDowntimeDoesNotRetire(t *testing.T) {
	a := newTestSpoke(t)
	a.putJobs(1, 2, 3)
	dir := t.TempDir()
	clock := &testClock{t: time.Unix(1_800_000_000, 0)}
	disc := &fakeDiscovery{}
	disc.set(members("ap1"))
	dial := func(string) cedarsync.Dial {
		return func(ctx context.Context) (*dbrpc.Client, func(), error) {
			time.Sleep(100 * time.Millisecond) // a real CEDAR dial + authentication takes time
			return a.dial(ctx)
		}
	}
	cfg := func(cat *db.Catalog) Config {
		return Config{Catalog: cat, Discovery: disc, Dial: dial, Now: clock.now, FlushInterval: 20 * time.Millisecond,
			RetireAfter: 7 * 24 * time.Hour, CursorDir: dir + "/cursors"}
	}
	cat := openCatalog(t, dir+"/db")
	hub := startHub(t, cfg(cat))
	waitFor(t, "replicated", func() bool { return sameKeys(jobKeysOf(t, cat, "ap1"), 1, 2, 3) })
	hub.stop(t)
	_ = cat.Close()

	clock.advance(8 * 24 * time.Hour) // hub down for 8 days, spoke up the whole time
	disc.set(members())               // the collector just restarted: an empty answer
	cat = openCatalog(t, dir+"/db")
	t.Cleanup(func() { _ = cat.Close() })
	hub = startHub(t, cfg(cat))
	defer hub.stop(t)
	waitFor(t, "first discovery after restart", func() bool { return disc.callCount() >= 2 })
	waitFor(t, "ap1 absent", func() bool { return sourceState(cat, "ap1") == StateAbsent })
	waitFor(t, "ap1's spoke reached", func() bool { v, _ := sourceRowAttrBool(cat, "ap1", "JobsConnected"); return v })
	hub.statePasses(t, 2)
	if n := len(jobKeysOf(t, cat, "ap1")); n != 3 {
		t.Fatalf("reachable spoke's rows deleted after hub downtime + empty collector: %d jobs left", n)
	}

	// A week of this run with the spoke still reachable: contact keeps it.
	clock.advance(8 * 24 * time.Hour)
	hub.statePasses(t, 2)
	if n := len(jobKeysOf(t, cat, "ap1")); n != 3 || val(hub.hub.Metrics().Retired) != 0 {
		t.Fatalf("reachable absent spoke retired: %d jobs left", n)
	}
}
