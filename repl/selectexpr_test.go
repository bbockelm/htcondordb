package repl

import (
	"testing"
)

// TestSelectExpressionColumns covers computed SELECT columns: arithmetic on
// attributes, an aliased expression, and that plain columns + aggregates still parse.
func TestSelectExpressionColumns(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "INSERT INTO jobs (Key, ClusterId, ProcId, A, B) VALUES ('1.0', 5, 0, 100, 30)")

	// Arithmetic expression column (the reported use case shape: A - B).
	r := mustExec(t, e, "SELECT ClusterId, ProcId, A - B FROM jobs")
	if len(r.Columns) != 3 || r.Columns[2] != "A - B" {
		t.Fatalf("columns = %v, want [ClusterId ProcId 'A - B']", r.Columns)
	}
	if len(r.Rows) != 1 || r.Rows[0][0] != "5" || r.Rows[0][2] != "70" {
		t.Fatalf("row = %v, want ClusterId=5 ... A-B=70", r.Rows[0])
	}

	// Aliased expression.
	r = mustExec(t, e, "SELECT A - B AS diff FROM jobs")
	if r.Columns[0] != "diff" || r.Rows[0][0] != "70" {
		t.Fatalf("aliased expr: columns=%v row=%v, want diff=70", r.Columns, r.Rows[0])
	}

	// Plain columns + AS alias still work unchanged.
	r = mustExec(t, e, "SELECT ClusterId AS cid FROM jobs")
	if r.Columns[0] != "cid" || r.Rows[0][0] != "5" {
		t.Fatalf("plain column + alias: columns=%v row=%v", r.Columns, r.Rows[0])
	}

	// Aggregate still parses/works (unchanged path).
	r = mustExec(t, e, "SELECT COUNT(*) FROM jobs")
	if r.Rows[0][0] != "1" {
		t.Fatalf("COUNT(*) = %s, want 1", r.Rows[0][0])
	}

	// A function-call expression column.
	r = mustExec(t, e, "SELECT ClusterId, (A + B) * 2 AS scaled FROM jobs")
	if r.Columns[1] != "scaled" || r.Rows[0][1] != "260" {
		t.Fatalf("paren expr: columns=%v row=%v, want scaled=260", r.Columns, r.Rows[0])
	}
}
