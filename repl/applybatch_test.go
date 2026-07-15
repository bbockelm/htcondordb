package repl

import (
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestApplyBatchRouting verifies that when ApplyBatch is configured, writes are
// routed to it (as WriteOps) instead of the local dbrpc transaction, while reads
// (WHERE key discovery) still use the dbrpc client.
func TestApplyBatchRouting(t *testing.T) {
	// Backing DB, seeded directly so reads find rows.
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	seed := func(key, text string) {
		tx := d.Begin()
		ad, err := classad.ParseOld(text)
		if err != nil {
			t.Fatal(err)
		}
		tx.NewClassAd(key, ad)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	seed("1.0", "Key = \"1.0\"\nOwner = \"alice\"\nJobStatus = 1")
	seed("2.0", "Key = \"2.0\"\nOwner = \"alice\"\nJobStatus = 1")
	seed("3.0", "Key = \"3.0\"\nOwner = \"bob\"\nJobStatus = 1")

	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConn(dbrpc.NewStreamConn(sp)) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	defer func() { c.Close(); s.Close() }()

	var captured [][]WriteOp
	e := NewExecutor(c, ExecConfig{ApplyBatch: func(ops []WriteOp) error {
		captured = append(captured, ops)
		return nil
	}})

	// INSERT routes one WNewClassAd op (and does NOT touch the backing DB).
	if _, err := e.ExecString("INSERT INTO ads (Key, Owner) VALUES ('9.9', 'carol')"); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.LookupClassAd("9.9"); ok {
		t.Fatal("INSERT should have gone to ApplyBatch, not the backing DB")
	}
	if len(captured) != 1 || len(captured[0]) != 1 || captured[0][0].Kind != WNewClassAd || captured[0][0].Key != "9.9" {
		t.Fatalf("INSERT ops = %+v", captured)
	}

	// UPDATE discovers the two alice keys (via a dbrpc read) and routes two
	// WSetAttribute ops.
	captured = nil
	r, err := e.ExecString(`UPDATE ads SET JobStatus = 2 WHERE Owner == "alice"`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Affected != 2 {
		t.Fatalf("UPDATE affected %d, want 2", r.Affected)
	}
	if len(captured) != 1 || len(captured[0]) != 2 {
		t.Fatalf("UPDATE should route 2 ops, got %+v", captured)
	}
	for _, op := range captured[0] {
		if op.Kind != WSetAttribute || op.Name != "JobStatus" || op.Value != "2" {
			t.Fatalf("unexpected UPDATE op %+v", op)
		}
	}

	// DELETE routes one WDestroyClassAd for the bob row.
	captured = nil
	if _, err := e.ExecString(`DELETE FROM ads WHERE Owner == "bob"`); err != nil {
		t.Fatal(err)
	}
	if len(captured) != 1 || len(captured[0]) != 1 || captured[0][0].Kind != WDestroyClassAd || captured[0][0].Key != "3.0" {
		t.Fatalf("DELETE ops = %+v", captured)
	}
}
