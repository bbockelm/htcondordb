package consistent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/bbockelm/cedar/stream"
)

func newCat(t *testing.T) *db.Catalog {
	t.Helper()
	cat, err := db.OpenCatalog("")
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// tbl returns a catalog table, failing if it does not exist yet.
func tbl(t *testing.T, cat *db.Catalog, name string) *db.DB {
	t.Helper()
	d, ok := cat.Table(name)
	if !ok {
		t.Fatalf("table %q missing", name)
	}
	return d
}

func TestBatchRoundTrip(t *testing.T) {
	b := NewBatch().
		NewClassAd("1.0", "Owner = \"alice\"").
		SetAttribute("1.0", "JobStatus", "2").
		DeleteAttribute("1.0", "Held").
		DestroyClassAd("2.0").
		NewClassAdIn("machines", "slot1", "Cpus = 4")
	data, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Ops) != 5 || got.Ops[0].Kind != OpNewClassAd || got.Ops[3].Kind != OpDestroyClassAd {
		t.Fatalf("decoded batch mismatch: %+v", got.Ops)
	}
	if got.Ops[4].Table != "machines" {
		t.Errorf("table-qualified op lost its table: %+v", got.Ops[4])
	}
}

func TestFSMApplyAndSnapshot(t *testing.T) {
	src := newCat(t)
	defer src.Close()
	f := NewFSM(src, "ads")

	batch := NewBatch().
		NewClassAd("1.0", "Owner = \"alice\"\nCpus = 4").
		NewClassAd("2.0", "Owner = \"bob\"\nCpus = 8").
		SetAttribute("1.0", "JobStatus", "2")
	if err := f.applyBatch(batch); err != nil {
		t.Fatal(err)
	}
	ads := tbl(t, src, "ads")
	if ads.Len() != 2 {
		t.Fatalf("after apply Len = %d, want 2", ads.Len())
	}
	if v, ok := lookupAttr(t, ads, "1.0", "JobStatus"); !ok || v != "2" {
		t.Fatalf("JobStatus = %q,%v want 2", v, ok)
	}

	// A delete op removes a key.
	if err := f.applyBatch(NewBatch().DestroyClassAd("2.0")); err != nil {
		t.Fatal(err)
	}
	if ads.Len() != 1 {
		t.Fatalf("after destroy Len = %d, want 1", ads.Len())
	}

	// Snapshot -> Persist -> Restore into a fresh catalog reproduces the state.
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	dst := newCat(t)
	defer dst.Close()
	// Seed dst with junk that Restore must clear.
	seed := NewFSM(dst, "ads")
	_ = seed.applyBatch(NewBatch().NewClassAd("9.9", "Owner = \"stale\""))
	if err := seed.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	dads := tbl(t, dst, "ads")
	if dads.Len() != 1 {
		t.Fatalf("restored Len = %d, want 1", dads.Len())
	}
	if _, ok := dads.LookupClassAd("1.0"); !ok {
		t.Fatal("restored table missing key 1.0")
	}
	if _, ok := dads.LookupClassAd("9.9"); ok {
		t.Fatal("restore did not clear stale key 9.9")
	}
}

// TestFSMMultiTable proves replication covers EVERY table: a batch touching two tables
// applies to both, and a snapshot/restore reproduces both tables' state.
func TestFSMMultiTable(t *testing.T) {
	src := newCat(t)
	defer src.Close()
	f := NewFSM(src, "ads")

	batch := NewBatch().
		NewClassAd("1.0", "Owner = \"alice\"").         // default table "ads"
		NewClassAdIn("machines", "slot1", "Cpus = 8").  // another table
		NewClassAdIn("machines", "slot2", "Cpus = 16"). //
		SetAttributeIn("machines", "slot1", "State", "\"Idle\"")
	if err := f.applyBatch(batch); err != nil {
		t.Fatal(err)
	}
	if got := tbl(t, src, "ads").Len(); got != 1 {
		t.Fatalf("ads Len = %d, want 1", got)
	}
	machines := tbl(t, src, "machines")
	if machines.Len() != 2 {
		t.Fatalf("machines Len = %d, want 2", machines.Len())
	}
	if v, ok := lookupAttr(t, machines, "slot1", "State"); !ok || v != "\"Idle\"" {
		t.Fatalf("machines slot1 State = %q,%v want \"Idle\"", v, ok)
	}

	// Snapshot both tables, restore into a fresh catalog.
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	dst := newCat(t)
	defer dst.Close()
	if err := NewFSM(dst, "ads").Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if got := tbl(t, dst, "ads").Len(); got != 1 {
		t.Errorf("restored ads Len = %d, want 1", got)
	}
	if got := tbl(t, dst, "machines").Len(); got != 2 {
		t.Errorf("restored machines Len = %d, want 2", got)
	}
}

// TestSingleNodeRaftApply drives a real (single-node) raft cluster: an Apply on the leader
// must commit and reach the FSM, proving Batch -> raft log -> FSM -> catalog works end to
// end, including a table-qualified op.
func TestSingleNodeRaftApply(t *testing.T) {
	cat := newCat(t)
	defer cat.Close()

	noDial := func(context.Context, string, time.Duration) (*stream.Stream, error) {
		return nil, errors.New("single-node: no peers")
	}
	c, err := NewCoordinator(CoordinatorConfig{
		NodeID:    "node1",
		Advertise: "127.0.0.1:1",
		Catalog:   cat,
		Dial:      noDial,
		DataDir:   t.TempDir(),
		Bootstrap: true,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if !c.IsLeader() {
		t.Fatal("single node should be the leader")
	}

	batch := NewBatch().
		NewClassAd("1.0", "Owner = \"alice\"\nCpus = 4").
		NewClassAdIn("machines", "slot1", "Cpus = 4")
	if err := c.Apply(batch); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := tbl(t, cat, "ads").LookupClassAd("1.0"); !ok {
		t.Fatal("committed batch did not reach the ads table")
	}
	if _, ok := tbl(t, cat, "machines").LookupClassAd("slot1"); !ok {
		t.Fatal("committed batch did not reach the machines table")
	}

	// A malformed op surfaces the FSM error to the caller.
	bad := &Batch{Ops: []Op{{Kind: OpNewClassAd, Key: "x", Value: "this is not { valid"}}}
	if err := c.Apply(bad); err == nil {
		t.Fatal("expected an apply error for a malformed ad")
	}
}

// TestControlProtocol drives the ClassAd control protocol against a single-node leader.
func TestControlProtocol(t *testing.T) {
	cat := newCat(t)
	defer cat.Close()
	noDial := func(context.Context, string, time.Duration) (*stream.Stream, error) {
		return nil, errors.New("no peers")
	}
	c, err := NewCoordinator(CoordinatorConfig{
		NodeID: "n1", Advertise: "127.0.0.1:1", Catalog: cat, Dial: noDial,
		DataDir: t.TempDir(), Bootstrap: true, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	resp := c.HandleControl(BuildLeaderRequest())
	if ok, _ := resp.EvaluateAttr(AttrResult).BoolValue(); !ok {
		t.Fatal("leader discovery Result should be true")
	}
	if addr := attrString(resp, AttrLeaderAddr); addr != "127.0.0.1:1" {
		t.Fatalf("leader address = %q, want 127.0.0.1:1", addr)
	}

	req, err := BuildApplyRequest(NewBatch().NewClassAd("1.0", "Owner = \"alice\""))
	if err != nil {
		t.Fatal(err)
	}
	resp = c.HandleControl(req)
	if ok, _ := resp.EvaluateAttr(AttrResult).BoolValue(); !ok {
		t.Fatalf("apply Result should be true; err=%q", attrString(resp, AttrErrorString))
	}
	if _, ok := tbl(t, cat, "ads").LookupClassAd("1.0"); !ok {
		t.Fatal("apply via control protocol did not reach the catalog")
	}
}

// TestRaftRestartDurability proves the boltdb log survives a restart: a node bootstrapped
// in a data dir, given a write, then reopened over the SAME data dir with a fresh (empty)
// catalog, replays its durable log and reconstructs the committed state.
func TestRaftRestartDurability(t *testing.T) {
	dataDir := t.TempDir()
	noDial := func(context.Context, string, time.Duration) (*stream.Stream, error) {
		return nil, errors.New("no peers")
	}
	open := func(cat *db.Catalog) *Coordinator {
		c, err := NewCoordinator(CoordinatorConfig{
			NodeID: "n1", Advertise: "127.0.0.1:1", Catalog: cat, Dial: noDial,
			DataDir: dataDir, Bootstrap: true, Timeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.WaitForLeader(5 * time.Second); err != nil {
			t.Fatal(err)
		}
		return c
	}

	cat1 := newCat(t)
	c1 := open(cat1)
	if err := c1.Apply(NewBatch().NewClassAd("1.0", "Owner = \"alice\"")); err != nil {
		t.Fatal(err)
	}
	_ = c1.Close()
	cat1.Close()

	cat2 := newCat(t)
	defer cat2.Close()
	c2 := open(cat2)
	defer c2.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if d, ok := cat2.Table("ads"); ok {
			if _, ok := d.LookupClassAd("1.0"); ok {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("restarted node did not replay its durable log into the fresh catalog")
}

func lookupAttr(t *testing.T, d *db.DB, key, name string) (string, bool) {
	t.Helper()
	tx := d.Begin()
	defer tx.Abort()
	return tx.LookupAttr(key, name)
}

// memSink is an in-memory raft.SnapshotSink for tests.
type memSink struct{ bytes.Buffer }

func (m *memSink) ID() string    { return "test-snapshot" }
func (m *memSink) Cancel() error { return nil }
func (m *memSink) Close() error  { return nil }
