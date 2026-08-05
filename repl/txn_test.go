package repl

import (
	"strings"
	"testing"
)

// count returns the row count of a table, as the executor sees it right now.
func count(t *testing.T, e *Executor, table string) string {
	t.Helper()
	return mustExec(t, e, "SELECT COUNT(*) FROM "+table).Rows[0][0]
}

func TestTransactionCommitApplies(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.0', 'alice')")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('2.0', 'bob')")
	mustExec(t, e, "COMMIT")

	if got := count(t, e, "ads"); got != "2" {
		t.Fatalf("after COMMIT, COUNT(*) = %s, want 2", got)
	}
}

func TestTransactionRollbackDiscards(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('keep', 'alice')")
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('drop', 'bob')")
	mustExec(t, e, "ROLLBACK")

	if got := count(t, e, "ads"); got != "1" {
		t.Fatalf("after ROLLBACK, COUNT(*) = %s, want 1 (only the pre-transaction row)", got)
	}
}

// A SELECT inside a transaction sees the transaction's own uncommitted writes, and the
// committed store still does not until COMMIT.
func TestTransactionSelectSeesOwnWrites(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.0', 'alice')")
	if got := count(t, e, "ads"); got != "1" {
		t.Fatalf("inside the transaction, COUNT(*) = %s, want 1 (its own insert is invisible)", got)
	}
	mustExec(t, e, "COMMIT")
	if got := count(t, e, "ads"); got != "1" {
		t.Fatalf("after COMMIT, COUNT(*) = %s, want 1", got)
	}
}

// A rolled-back transaction's reads were of rows that never existed.
func TestTransactionSelectAfterRollback(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.0', 'alice')")
	if got := count(t, e, "ads"); got != "1" {
		t.Fatalf("inside the transaction, COUNT(*) = %s, want 1", got)
	}
	mustExec(t, e, "ROLLBACK")
	if got := count(t, e, "ads"); got != "0" {
		t.Fatalf("after ROLLBACK, COUNT(*) = %s, want 0", got)
	}
}

// A SELECT against a table the transaction did not bind to reads committed state: a dbrpc
// transaction covers one table, so there is nothing transactional to read there.
func TestTransactionSelectOnAnotherTableIsCommitted(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()

	mustExec(t, e, "CREATE TABLE other")
	mustExec(t, e, "INSERT INTO other (Key, Owner) VALUES ('seed', 'carol')")

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.0', 'alice')")
	if got := count(t, e, "other"); got != "1" {
		t.Errorf("COUNT(*) on the unbound table = %s, want 1", got)
	}
	if got := count(t, e, "ads"); got != "1" {
		t.Errorf("COUNT(*) on the transaction's table = %s, want 1", got)
	}
	mustExec(t, e, "ROLLBACK")
}

// UPDATE and DELETE inside a transaction address rows by key, resolved through the
// transaction -- so they compose with the transaction's own staged inserts.
func TestTransactionUpdateAndDelete(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "INSERT INTO ads (Key, Owner, Cpus) VALUES ('1.0', 'alice', 4)")

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "UPDATE ads SET Cpus = 8 WHERE Key == \"1.0\"")
	mustExec(t, e, "COMMIT")

	r := mustExec(t, e, `SELECT Cpus FROM ads WHERE Key == "1.0"`)
	if got := r.Rows[0][0]; got != "8" {
		t.Fatalf("Cpus = %s, want 8", got)
	}

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "DELETE FROM ads WHERE Key == \"1.0\"")
	mustExec(t, e, "ROLLBACK")

	if got := count(t, e, "ads"); got != "1" {
		t.Fatalf("after rolling back the DELETE, COUNT(*) = %s, want 1", got)
	}
}

func TestTransactionCannotSpanTables(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()

	mustExec(t, e, "CREATE TABLE other")
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.0', 'alice')")

	_, err := e.ExecString("INSERT INTO other (Key, Owner) VALUES ('2.0', 'bob')")
	if err == nil {
		t.Fatal("writing a second table inside a transaction was accepted; want an error")
	}
	// The message has to name both tables and say what to do -- a dbrpc transaction is
	// scoped to one table, so this is a permanent constraint the caller must design around.
	for _, want := range []string{"ads", "other", "cannot span tables"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}

	// The transaction is still open and still usable for its own table.
	if !e.InTransaction() {
		t.Fatal("the refused statement closed the transaction")
	}
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('3.0', 'carol')")
	mustExec(t, e, "COMMIT")
	if got := count(t, e, "ads"); got != "2" {
		t.Fatalf("COUNT(*) = %s, want 2", got)
	}
}

func TestTransactionErrors(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	if _, err := e.ExecString("COMMIT"); err == nil {
		t.Error("COMMIT without BEGIN was accepted")
	}
	if _, err := e.ExecString("ROLLBACK"); err == nil {
		t.Error("ROLLBACK without BEGIN was accepted")
	}

	mustExec(t, e, "BEGIN")
	if _, err := e.ExecString("BEGIN"); err == nil {
		t.Error("nested BEGIN was accepted")
	}
	mustExec(t, e, "ROLLBACK")
}

// An empty transaction commits without touching the server: there is nothing to apply.
// A connection in non-autocommit mode produces this shape constantly (BEGIN, SELECT,
// COMMIT), so it must not be an error.
func TestEmptyTransactionCommits(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "SELECT COUNT(*) FROM ads")
	mustExec(t, e, "COMMIT")

	mustExec(t, e, "BEGIN")
	mustExec(t, e, "ROLLBACK")

	// Each verb reports itself, so a caller echoing Note (the REPL, the CLI's -e) shows
	// something useful rather than a blank line.
	for _, tc := range [][2]string{{"BEGIN", "BEGIN"}, {"ROLLBACK", "ROLLBACK"}} {
		if got := mustExec(t, e, tc[0]).Note; got != tc[1] {
			t.Errorf("%s reported Note %q, want %q", tc[0], got, tc[1])
		}
	}
}

func TestInTransactionTracksState(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	if e.InTransaction() {
		t.Error("a fresh executor reports an open transaction")
	}
	mustExec(t, e, "BEGIN")
	if !e.InTransaction() {
		t.Error("after BEGIN, InTransaction is false")
	}
	mustExec(t, e, "COMMIT")
	if e.InTransaction() {
		t.Error("after COMMIT, InTransaction is true")
	}
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "ROLLBACK")
	if e.InTransaction() {
		t.Error("after ROLLBACK, InTransaction is true")
	}
}

// The SQL noise words other dialects require are accepted, so a statement pasted from
// Postgres or SQLite parses.
func TestTransactionSyntaxVariants(t *testing.T) {
	for _, sql := range []string{
		"BEGIN", "BEGIN TRANSACTION", "BEGIN WORK", "START TRANSACTION",
	} {
		e, cleanup := newTestExec(t)
		if _, err := e.ExecString(sql); err != nil {
			t.Errorf("%q: %v", sql, err)
		}
		cleanup()
	}
	for _, pair := range [][2]string{
		{"BEGIN", "COMMIT"}, {"BEGIN", "COMMIT TRANSACTION"}, {"BEGIN", "COMMIT WORK"},
		{"BEGIN", "END"}, {"BEGIN", "ROLLBACK"}, {"BEGIN", "ROLLBACK TRANSACTION"},
		{"BEGIN", "ABORT"},
	} {
		e, cleanup := newTestExec(t)
		mustExec(t, e, pair[0])
		if _, err := e.ExecString(pair[1]); err != nil {
			t.Errorf("%q: %v", pair[1], err)
		}
		cleanup()
	}
}

// Transactions batch writes; a bulk load through one is the main reason to want them, and
// it must apply atomically rather than per row.
func TestTransactionBatchesManyWrites(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "BEGIN")
	for _, key := range []string{"1.0", "2.0", "3.0", "4.0", "5.0"} {
		mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('"+key+"', 'alice')")
	}
	// Visible to the transaction itself, and to nobody else until COMMIT.
	if got := count(t, e, "ads"); got != "5" {
		t.Fatalf("mid-transaction COUNT(*) = %s, want 5", got)
	}
	mustExec(t, e, "COMMIT")
	if got := count(t, e, "ads"); got != "5" {
		t.Fatalf("after COMMIT, COUNT(*) = %s, want 5", got)
	}
}
