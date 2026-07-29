package repl

import (
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestApplyBatchKeylessRows: in consistent mode (ApplyBatch set), UPDATE/DELETE address matched
// rows by their REAL storage key, resolved server-side -- so rows that carry no "Key" attribute
// (crufty Owner records, rows written before key-stamping) can be targeted. This previously failed
// with "a matched row has no Key attribute".
func TestApplyBatchKeylessRows(t *testing.T) {
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	seed := func(key, text string) {
		ad, err := classad.ParseOld(text)
		if err != nil {
			t.Fatal(err)
		}
		tx := d.Begin()
		tx.NewClassAd(key, ad)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// The Owner rows carry NO "Key" attribute; their storage keys are 0O.-1 / 0O.-2.
	seed("0O.-1", "MyType = \"Owner\"\nName = \"alice\"")
	seed("0O.-2", "MyType = \"Owner\"\nName = \"bob\"")
	seed("1.0", "MyType = \"Job\"\nOwner = \"alice\"")

	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	defer func() { c.Close(); s.Close() }()

	var captured [][]WriteOp
	e := NewExecutor(c, ExecConfig{ApplyBatch: func(ops []WriteOp) error {
		captured = append(captured, ops)
		return nil
	}})

	// DELETE the Key-less Owner rows in consistent mode.
	captured = nil
	r, err := e.ExecString(`DELETE FROM ads WHERE MyType == "Owner"`)
	if err != nil {
		t.Fatalf("consistent DELETE of Key-less rows failed: %v", err)
	}
	if r.Affected != 2 {
		t.Fatalf("DELETE affected %d, want 2", r.Affected)
	}
	gotDel := map[string]bool{}
	if len(captured) != 1 {
		t.Fatalf("DELETE should route one batch, got %d", len(captured))
	}
	for _, op := range captured[0] {
		if op.Kind != WDestroyClassAd {
			t.Fatalf("unexpected DELETE op %+v", op)
		}
		gotDel[op.Key] = true
	}
	if !gotDel["0O.-1"] || !gotDel["0O.-2"] || len(gotDel) != 2 {
		t.Fatalf("DELETE addressed keys %v, want the two real Owner storage keys", gotDel)
	}

	// UPDATE likewise addresses the Key-less rows by their storage key.
	captured = nil
	r, err = e.ExecString(`UPDATE ads SET Reviewed = true WHERE MyType == "Owner"`)
	if err != nil {
		t.Fatalf("consistent UPDATE of Key-less rows failed: %v", err)
	}
	if r.Affected != 2 {
		t.Fatalf("UPDATE affected %d, want 2", r.Affected)
	}
	gotUpd := map[string]bool{}
	for _, op := range captured[0] {
		if op.Kind != WSetAttribute || op.Name != "Reviewed" {
			t.Fatalf("unexpected UPDATE op %+v", op)
		}
		gotUpd[op.Key] = true
	}
	if !gotUpd["0O.-1"] || !gotUpd["0O.-2"] || len(gotUpd) != 2 {
		t.Fatalf("UPDATE addressed keys %v, want the two real Owner storage keys", gotUpd)
	}
}
