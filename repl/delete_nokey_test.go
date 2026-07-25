package repl

import (
	"context"
	"testing"
)

// TestDeleteWithoutKeyAttribute is the regression for "a matched row has no Key attribute":
// DELETE must address matching rows by their real storage key server-side, so it works on
// rows that carry no "Key" attribute -- Owner/User records, or rows synced before key-stamping.
func TestDeleteWithoutKeyAttribute(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	ctx := context.Background()

	// Insert rows directly via the client with NO "Key" attribute (like the crufty Owner
	// records the operator wants to remove). The storage key is supplied separately.
	tx, err := e.c.BeginTable(ctx, "ads")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.NewClassAd(ctx, "0O.-1", "MyType = \"Owner\"\nName = \"alice\""); err != nil {
		t.Fatal(err)
	}
	if err := tx.NewClassAd(ctx, "0O.-2", "MyType = \"Owner\"\nName = \"bob\""); err != nil {
		t.Fatal(err)
	}
	if err := tx.NewClassAd(ctx, "1.0", "MyType = \"Job\"\nOwner = \"alice\""); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if r := mustExec(t, e, "SELECT COUNT(*) FROM ads"); r.Rows[0][0] != "3" {
		t.Fatalf("setup: COUNT(*) = %s, want 3", r.Rows[0][0])
	}

	// The exact query the operator ran -- previously failed with the "no Key attribute" error.
	r, err := e.ExecString(`DELETE FROM ads WHERE MyType =?= "Owner"`)
	if err != nil {
		t.Fatalf("DELETE errored (the bug): %v", err)
	}
	if r.Affected != 2 {
		t.Errorf("DELETE affected %d rows, want 2", r.Affected)
	}

	// Only the Job row remains.
	if r := mustExec(t, e, "SELECT COUNT(*) FROM ads"); r.Rows[0][0] != "1" {
		t.Errorf("after DELETE, COUNT(*) = %s, want 1", r.Rows[0][0])
	}
	if r := mustExec(t, e, `SELECT COUNT(*) FROM ads WHERE MyType =?= "Owner"`); r.Rows[0][0] != "0" {
		t.Errorf("Owner rows still present after DELETE: %s", r.Rows[0][0])
	}
}
