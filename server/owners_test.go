package server

import (
	"slices"
	"testing"
)

func TestTableOwnersSetReplacesAndReleases(t *testing.T) {
	o := NewTableOwners()
	o.Set("schedd-sync", []string{"jobs", "history"})
	o.Set("replication", []string{"History", "remote"})

	if got := o.Owners("HISTORY"); !slices.Equal(got, []string{"replication", "schedd-sync"}) {
		t.Errorf("Owners(HISTORY) = %v, want both owners, sorted", got)
	}
	if !o.Owned("Jobs") || o.Writable("jobs", "") {
		t.Error("jobs should be owned and read-only (case-insensitive)")
	}
	if o.Owned("scratch") || !o.Writable("scratch", "") {
		t.Error("an unregistered table must be writable")
	}

	// Set replaces the owner's whole set: history drops out of schedd-sync's claim.
	o.Set("schedd-sync", []string{"jobs"})
	if got := o.Owners("history"); !slices.Equal(got, []string{"replication"}) {
		t.Errorf("after replace Owners(history) = %v, want [replication]", got)
	}
	o.Set("replication", nil)
	o.Set("schedd-sync", nil)
	for _, tbl := range []string{"jobs", "history", "remote"} {
		if o.Owned(tbl) {
			t.Errorf("%s still owned after every owner released", tbl)
		}
	}
}

func TestTableOwnersSessionGrant(t *testing.T) {
	o := NewTableOwners()
	o.Set("history-import", []string{"pool_history"})
	o.Set("schedd-sync", []string{"history"})
	o.GrantSession("sid", "history-import")

	if !o.Writable("pool_history", "sid") {
		t.Error("granted session must write its owner's table")
	}
	if o.Writable("history", "sid") {
		t.Error("granted session must not write another owner's table")
	}
	if o.Writable("pool_history", "other") {
		t.Error("an ungranted session must not write an owned table")
	}
	o.RevokeSession("sid")
	if o.Writable("pool_history", "sid") {
		t.Error("revoked session still writes")
	}
}

func TestTableOwnersNil(t *testing.T) {
	var o *TableOwners
	o.Set("x", []string{"t"})
	if o.Owned("t") || !o.Writable("t", "") || o.Owners("t") != nil {
		t.Error("a nil registry owns nothing")
	}
}
