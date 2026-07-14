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

// TestApplyEventLogic exercises upsert/delete/reset application directly, without
// a network, so the convergence semantics are pinned independently of timing.
func TestApplyEventLogic(t *testing.T) {
	follower, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	r, err := NewReplicator(ReplicatorConfig{Local: follower, Dial: func(context.Context) (*dbrpc.Client, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}

	upsert := func(key, text string) {
		if err := r.applyEvent(dbrpc.WatchEvent{Kind: wkUpsert, Key: key, AdText: mustAd(t, text).String(), Cursor: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}
	upsert("1.0", "Owner = \"alice\"\nCpus = 4")
	upsert("2.0", "Owner = \"bob\"\nCpus = 8")
	if follower.Len() != 2 {
		t.Fatalf("after 2 upserts Len = %d, want 2", follower.Len())
	}

	// Delete one.
	if err := r.applyEvent(dbrpc.WatchEvent{Kind: wkDelete, Key: "1.0", Cursor: []byte("d")}); err != nil {
		t.Fatal(err)
	}
	if _, ok := follower.LookupClassAd("1.0"); ok {
		t.Fatal("key 1.0 should be gone after delete")
	}
	if follower.Len() != 1 {
		t.Fatalf("after delete Len = %d, want 1", follower.Len())
	}

	// Reset clears everything, then a replay repopulates.
	if err := r.applyEvent(dbrpc.WatchEvent{Kind: wkReset, Cursor: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	if follower.Len() != 0 {
		t.Fatalf("after reset Len = %d, want 0", follower.Len())
	}
	if r.ResetCount() != 1 {
		t.Fatalf("ResetCount = %d, want 1", r.ResetCount())
	}
	upsert("9.9", "Owner = \"carol\"")
	if follower.Len() != 1 {
		t.Fatalf("after replay Len = %d, want 1", follower.Len())
	}
}

// TestReplicatorConverges runs a real replication session over an in-memory
// leader and asserts the follower converges to committed leader state, for both
// pre-existing state (full replay) and live commits.
func TestReplicatorConverges(t *testing.T) {
	leader, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	ls := dbrpc.NewServer(leader)
	defer ls.Close()

	// Seed state that must arrive via the initial full replay.
	commit(t, leader, "1.0", "Owner = \"alice\"\nCpus = 4")

	dial := func(context.Context) (*dbrpc.Client, error) {
		cp, sp := net.Pipe()
		go func() { _ = ls.ServeConn(dbrpc.NewStreamConn(sp)) }()
		return dbrpc.NewClient(dbrpc.NewStreamConn(cp)), nil
	}

	follower, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	r, err := NewReplicator(ReplicatorConfig{Local: follower, Dial: dial, ReconnectMin: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// Wait for the initial replay.
	waitFor(t, 3*time.Second, func() bool {
		_, ok := follower.LookupClassAd("1.0")
		return ok
	}, "follower to replay the seeded ad")

	// A live commit on the leader must propagate.
	commit(t, leader, "2.0", "Owner = \"bob\"\nCpus = 16")
	waitFor(t, 3*time.Second, func() bool {
		_, ok := follower.LookupClassAd("2.0")
		return ok
	}, "follower to receive the live commit")

	// A live delete must propagate.
	del(t, leader, "1.0")
	waitFor(t, 3*time.Second, func() bool {
		_, ok := follower.LookupClassAd("1.0")
		return !ok
	}, "follower to receive the live delete")
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
