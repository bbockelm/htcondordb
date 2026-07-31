package cedarsync

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// dialFor serves cat over an in-process dbrpc server and returns a Dial that opens a fresh
// connection to it each call (so a reconnect works).
func dialFor(t *testing.T, cat *db.Catalog) Dial {
	t.Helper()
	s := dbrpc.NewServerCatalog(cat)
	t.Cleanup(func() { s.Close() })
	return func(ctx context.Context) (*dbrpc.Client, func(), error) {
		cp, sp := net.Pipe()
		go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
		c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
		return c, func() { _ = c.Close() }, nil
	}
}

func mustArchive(t *testing.T, name string) (*db.Catalog, *db.ArchiveTable) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cat.Close() })
	a, err := cat.CreateArchiveTable(name, db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		t.Fatal(err)
	}
	return cat, a
}

func appendOld(t *testing.T, a *db.ArchiveTable, text string) {
	t.Helper()
	if err := a.AppendOld(text); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, a *db.ArchiveTable, constraint string) int {
	t.Helper()
	seq, err := a.Query(constraint)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// TestCedarFanInSelective: two source htcondordbs fan into one central archive over CEDAR, each
// filtered server-agnostically (only alice's jobs) and stamped by Src, with a live tail.
func TestCedarFanInSelective(t *testing.T) {
	catA, archA := mustArchive(t, "history")
	catB, archB := mustArchive(t, "history")
	appendOld(t, archA, `GlobalJobId = "ap40#1.0"; Owner = "alice"; ClusterId = 1`)
	appendOld(t, archA, `GlobalJobId = "ap40#2.0"; Owner = "eve";   ClusterId = 2`)
	appendOld(t, archB, `GlobalJobId = "ap55#1.0"; Owner = "alice"; ClusterId = 1`)
	appendOld(t, archB, `GlobalJobId = "ap55#9.0"; Owner = "mallory"; ClusterId = 9`)

	_, central := mustArchive(t, "central")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, s := range []struct {
		cat *db.Catalog
		src string
	}{{catA, "ap40"}, {catB, "ap55"}} {
		sink, err := replicate.NewArchiveSink(central, s.src, &replicate.MemCursorStore{})
		if err != nil {
			t.Fatal(err)
		}
		r, err := NewRunner(dialFor(t, s.cat), Config{Source: "history", Src: s.src, Constraint: `Owner == "alice"`}, sink, nil)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = r.Run(ctx) }()
	}

	// Only the two alice rows (one per source) land; eve/mallory are filtered out.
	waitFor(t, 5*time.Second, func() bool { return central.Count() == 2 })
	if n := count(t, central, `Owner == "alice"`); n != 2 {
		t.Errorf("alice rows = %d, want 2", n)
	}
	if n := count(t, central, `Src == "ap40"`); n != 1 {
		t.Errorf("ap40 rows = %d, want 1", n)
	}
	if n := count(t, central, `Src == "ap55"`); n != 1 {
		t.Errorf("ap55 rows = %d, want 1", n)
	}
	if n := count(t, central, `Owner == "eve" || Owner == "mallory"`); n != 0 {
		t.Errorf("filtered rows leaked = %d, want 0", n)
	}

	// Live tail: a new matching job on source A flows through the open watch.
	appendOld(t, archA, `GlobalJobId = "ap40#3.0"; Owner = "alice"; ClusterId = 3`)
	waitFor(t, 5*time.Second, func() bool { return central.Count() == 3 })
}
