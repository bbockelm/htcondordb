package leaderfollower

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

func mustAd(t *testing.T, text string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.ParseOld(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return ad
}

func newCat(t *testing.T) *db.Catalog {
	t.Helper()
	cat, err := db.OpenCatalog("")
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// TestApplyEventLogic exercises upsert/delete/reset application directly against one
// table, without a network, so the convergence semantics are pinned independently of
// timing.
func TestApplyEventLogic(t *testing.T) {
	cat := newCat(t)
	defer cat.Close()
	r, err := NewReplicator(ReplicatorConfig{Catalog: cat, Dial: func(context.Context) (*dbrpc.Client, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ads, err := cat.CreateTable("ads")
	if err != nil {
		t.Fatal(err)
	}

	upsert := func(key, text string) {
		if err := r.applyEvent(ads, "ads", dbrpc.WatchEvent{Kind: wkUpsert, Key: key, AdText: mustAd(t, text).String(), Cursor: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}
	upsert("1.0", "Owner = \"alice\"\nCpus = 4")
	upsert("2.0", "Owner = \"bob\"\nCpus = 8")
	if ads.Len() != 2 {
		t.Fatalf("after 2 upserts Len = %d, want 2", ads.Len())
	}

	if err := r.applyEvent(ads, "ads", dbrpc.WatchEvent{Kind: wkDelete, Key: "1.0", Cursor: []byte("d")}); err != nil {
		t.Fatal(err)
	}
	if _, ok := ads.LookupClassAd("1.0"); ok {
		t.Fatal("key 1.0 should be gone after delete")
	}

	if err := r.applyEvent(ads, "ads", dbrpc.WatchEvent{Kind: wkReset, Cursor: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	if ads.Len() != 0 {
		t.Fatalf("after reset Len = %d, want 0", ads.Len())
	}
	if r.ResetCount() != 1 {
		t.Fatalf("ResetCount = %d, want 1", r.ResetCount())
	}
}

// TestReplicatorConverges runs a real replication session over an in-memory single-table
// leader and asserts the follower converges (initial replay + live commit + live delete).
func TestReplicatorConverges(t *testing.T) {
	leader, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	ls := dbrpc.NewServer(leader)
	defer ls.Close()

	commit(t, leader, "1.0", "Owner = \"alice\"\nCpus = 4") // must arrive via full replay

	dial := pipeDialer(ls)
	fcat := newCat(t)
	defer fcat.Close()
	r, err := NewReplicator(ReplicatorConfig{Catalog: fcat, Dial: dial, ReconnectMin: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	waitFor(t, 3*time.Second, func() bool { return has(fcat, "ads", "1.0") }, "follower to replay the seeded ad")

	commit(t, leader, "2.0", "Owner = \"bob\"\nCpus = 16")
	waitFor(t, 3*time.Second, func() bool { return has(fcat, "ads", "2.0") }, "follower to receive the live commit")

	del(t, leader, "1.0")
	waitFor(t, 3*time.Second, func() bool { return !has(fcat, "ads", "1.0") }, "follower to receive the live delete")
}

// TestReplicatorMultiTable replicates a multi-table leader: two seeded tables converge, and
// a table CREATED after the session starts is discovered and replicated too.
func TestReplicatorMultiTable(t *testing.T) {
	lcat := newCat(t)
	defer lcat.Close()
	jobs, _ := lcat.CreateTable("jobs")
	machines, _ := lcat.CreateTable("machines")
	commit(t, jobs, "1.0", "Owner = \"alice\"")
	commit(t, machines, "slot1", "Cpus = 8")

	ls := dbrpc.NewServerCatalog(lcat)
	defer ls.Close()

	fcat := newCat(t)
	defer fcat.Close()
	r, err := NewReplicator(ReplicatorConfig{
		Catalog: fcat, Dial: pipeDialer(ls),
		ReconnectMin: 20 * time.Millisecond, DiscoverInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	waitFor(t, 3*time.Second, func() bool { return has(fcat, "jobs", "1.0") }, "jobs table to replicate")
	waitFor(t, 3*time.Second, func() bool { return has(fcat, "machines", "slot1") }, "machines table to replicate")

	// A brand-new table on the leader must be discovered mid-session and replicated.
	docs, err := lcat.CreateTable("docs")
	if err != nil {
		t.Fatal(err)
	}
	commit(t, docs, "d1", "Title = \"readme\"")
	waitFor(t, 5*time.Second, func() bool { return has(fcat, "docs", "d1") }, "a newly-created table to be discovered and replicated")
}

// pipeDialer serves the given server over an in-memory pipe per dial.
func pipeDialer(ls *dbrpc.Server) Dialer {
	return func(context.Context) (*dbrpc.Client, error) {
		cp, sp := net.Pipe()
		go func() { _ = ls.ServeConn(dbrpc.NewStreamConn(sp)) }()
		return dbrpc.NewClient(dbrpc.NewStreamConn(cp)), nil
	}
}

// has reports whether the follower's table contains key.
func has(cat *db.Catalog, table, key string) bool {
	d, ok := cat.Table(table)
	if !ok {
		return false
	}
	_, ok = d.LookupClassAd(key)
	return ok
}

func commit(t *testing.T, d *db.DB, key, text string) {
	t.Helper()
	tx := d.Begin()
	tx.NewClassAd(key, mustAd(t, text))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func del(t *testing.T, d *db.DB, key string) {
	t.Helper()
	tx := d.Begin()
	tx.DestroyClassAd(key)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
