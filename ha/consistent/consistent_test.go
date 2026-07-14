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

func TestBatchRoundTrip(t *testing.T) {
	b := NewBatch().
		NewClassAd("1.0", "Owner = \"alice\"").
		SetAttribute("1.0", "JobStatus", "2").
		DeleteAttribute("1.0", "Held").
		DestroyClassAd("2.0")
	data, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Ops) != 4 || got.Ops[0].Kind != OpNewClassAd || got.Ops[3].Kind != OpDestroyClassAd {
		t.Fatalf("decoded batch mismatch: %+v", got.Ops)
	}
}

func TestFSMApplyAndSnapshot(t *testing.T) {
	src, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	f := NewFSM(src)

	batch := NewBatch().
		NewClassAd("1.0", "Owner = \"alice\"\nCpus = 4").
		NewClassAd("2.0", "Owner = \"bob\"\nCpus = 8").
		SetAttribute("1.0", "JobStatus", "2")
	if err := f.applyBatch(batch); err != nil {
		t.Fatal(err)
	}
	if src.Len() != 2 {
		t.Fatalf("after apply Len = %d, want 2", src.Len())
	}
	if v, ok := lookupAttr(t, src, "1.0", "JobStatus"); !ok || v != "2" {
		t.Fatalf("JobStatus = %q,%v want 2", v, ok)
	}

	// A delete op removes a key.
	if err := f.applyBatch(NewBatch().DestroyClassAd("2.0")); err != nil {
		t.Fatal(err)
	}
	if src.Len() != 1 {
		t.Fatalf("after destroy Len = %d, want 1", src.Len())
	}

	// Snapshot -> Persist -> Restore into a fresh DB reproduces the state.
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	dst, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	// Seed dst with junk that Restore must clear.
	seed := NewFSM(dst)
	_ = seed.applyBatch(NewBatch().NewClassAd("9.9", "Owner = \"stale\""))

	if err := seed.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != 1 {
		t.Fatalf("restored Len = %d, want 1", dst.Len())
	}
	if _, ok := dst.LookupClassAd("1.0"); !ok {
		t.Fatal("restored DB missing key 1.0")
	}
	if _, ok := dst.LookupClassAd("9.9"); ok {
		t.Fatal("restore did not clear stale key 9.9")
	}
}

// TestSingleNodeRaftApply drives a real (single-node) raft cluster: an Apply on
// the leader must commit and reach the FSM, proving Batch -> raft log -> FSM ->
// database works end to end.
func TestSingleNodeRaftApply(t *testing.T) {
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	noDial := func(context.Context, string, time.Duration) (*stream.Stream, error) {
		return nil, errors.New("single-node: no peers")
	}
	c, err := NewCoordinator(CoordinatorConfig{
		NodeID:    "node1",
		Advertise: "127.0.0.1:1",
		Local:     d,
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

	batch := NewBatch().NewClassAd("1.0", "Owner = \"alice\"\nCpus = 4")
	if err := c.Apply(batch); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := d.LookupClassAd("1.0"); !ok {
		t.Fatal("committed batch did not reach the FSM/database")
	}

	// A malformed op surfaces the FSM error to the caller.
	bad := &Batch{Ops: []Op{{Kind: OpNewClassAd, Key: "x", Value: "this is not { valid"}}}
	if err := c.Apply(bad); err == nil {
		t.Fatal("expected an apply error for a malformed ad")
	}
}

// TestControlProtocol drives the ClassAd control protocol against a single-node
// leader: leader-discovery reports self, and an apply commits through raft.
func TestControlProtocol(t *testing.T) {
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	noDial := func(context.Context, string, time.Duration) (*stream.Stream, error) {
		return nil, errors.New("no peers")
	}
	c, err := NewCoordinator(CoordinatorConfig{
		NodeID: "n1", Advertise: "127.0.0.1:1", Local: d, Dial: noDial,
		DataDir: t.TempDir(), Bootstrap: true, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	// leader-discovery
	resp := c.HandleControl(BuildLeaderRequest())
	if ok, _ := resp.EvaluateAttr(AttrResult).BoolValue(); !ok {
		t.Fatal("leader discovery Result should be true")
	}
	if addr := attrString(resp, AttrLeaderAddr); addr != "127.0.0.1:1" {
		t.Fatalf("leader address = %q, want 127.0.0.1:1", addr)
	}

	// apply a batch through the control protocol
	req, err := BuildApplyRequest(NewBatch().NewClassAd("1.0", "Owner = \"alice\""))
	if err != nil {
		t.Fatal(err)
	}
	resp = c.HandleControl(req)
	if ok, _ := resp.EvaluateAttr(AttrResult).BoolValue(); !ok {
		t.Fatalf("apply Result should be true; err=%q", attrString(resp, AttrErrorString))
	}
	if _, ok := d.LookupClassAd("1.0"); !ok {
		t.Fatal("apply via control protocol did not reach the database")
	}
}

func lookupAttr(t *testing.T, d *db.DB, key, name string) (string, bool) {
	t.Helper()
	tx := d.Begin()
	defer tx.Abort()
	return tx.LookupAttr(key, name)
}

// memSink is an in-memory raft.SnapshotSink for tests.
type memSink struct{ bytes.Buffer }

func (m *memSink) ID() string     { return "test-snapshot" }
func (m *memSink) Cancel() error  { return nil }
func (m *memSink) Close() error   { return nil }
