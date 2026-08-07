package repl

import "testing"

// TestTxProjectedSelectSeesOwnWrite is the regression for a read that silently missed rows:
// a plain-column SELECT is served by the projected query op, which is connection-level and
// reads the committed store, so inside a transaction it did not see the transaction's own
// INSERT. It returned no row rather than failing -- and a read-modify-write built on it
// would overwrite a value it never saw.
func TestTxProjectedSelectSeesOwnWrite(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO jobs (Key, Owner, JobStatus) VALUES ('k1', 'alice', 2)")

	star := mustExec(t, e, "SELECT * FROM jobs")
	proj := mustExec(t, e, "SELECT Owner FROM jobs")
	if len(star.Rows) != 1 {
		t.Fatalf("SELECT * inside the transaction saw %d rows, want the one just inserted", len(star.Rows))
	}
	if len(proj.Rows) != 1 || proj.Rows[0][0] != "alice" {
		t.Errorf("SELECT Owner saw %v, want one row for alice -- the projection must not bypass the transaction",
			proj.Rows)
	}

	// An ORDER BY / LIMIT over the projection reads the same rows.
	lim := mustExec(t, e, "SELECT Owner FROM jobs ORDER BY Owner LIMIT 1")
	if len(lim.Rows) != 1 {
		t.Errorf("ordered+limited projection saw %d rows, want 1", len(lim.Rows))
	}

	// After the commit the connection-level projected path serves it again.
	mustExec(t, e, "COMMIT")
	after := mustExec(t, e, "SELECT Owner FROM jobs")
	if len(after.Rows) != 1 || after.Rows[0][0] != "alice" {
		t.Errorf("after COMMIT the projected SELECT saw %v, want alice", after.Rows)
	}
}
