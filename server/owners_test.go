package server

import (
	"errors"
	"slices"
	"strings"
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

// TestTableOwnersClaimRefusesOtherOwner: Claim never lets two owners hold one table, in either
// order, changes nothing when it refuses, and succeeds once the holder releases.
func TestTableOwnersClaimRefusesOtherOwner(t *testing.T) {
	o := NewTableOwners()
	if err := o.Claim("federation", []string{"jobs", "federation_sources"}); err != nil {
		t.Fatal(err)
	}
	err := o.Claim("schedd-sync", []string{"users", "JOBS"})
	if !errors.Is(err, ErrTableClaimed) || !strings.Contains(err.Error(), "federation") {
		t.Fatalf("claiming a held table: err = %v, want ErrTableClaimed naming the holder", err)
	}
	if o.Owned("users") {
		t.Error("a refused claim took part of its set")
	}
	if got := o.Owners("jobs"); !slices.Equal(got, []string{"federation"}) {
		t.Errorf("Owners(jobs) = %v after a refused claim", got)
	}
	// An owner re-claiming (growing or shrinking) its own set is never a conflict.
	if err := o.Claim("federation", []string{"jobs"}); err != nil {
		t.Fatal(err)
	}
	if o.Owned("federation_sources") {
		t.Error("Claim did not replace the owner's set")
	}
	if got := o.Held("federation"); !slices.Equal(got, []string{"jobs"}) {
		t.Errorf("Held(federation) = %v", got)
	}
	o.Set("federation", nil)
	if err := o.Claim("schedd-sync", []string{"users", "jobs"}); err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if err := o.Claim("federation", []string{"jobs"}); !errors.Is(err, ErrTableClaimed) {
		t.Fatalf("reverse order: err = %v, want ErrTableClaimed", err)
	}
	var nilOwners *TableOwners
	if err := nilOwners.Claim("x", []string{"t"}); err != nil {
		t.Errorf("nil registry Claim: %v", err)
	}
}
