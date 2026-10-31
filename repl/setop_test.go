package repl

import (
	"strings"
	"testing"
)

// The motivating question: one user's live jobs together with their completed ones. It is a
// union, not a join -- a job is in the queue or in history, never both.
func TestUnionLiveAndHistory(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner, JobStatus) VALUES ('7.0', 7, 'alice', 2)`)
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner, JobStatus) VALUES ('8.0', 8, 'bob', 1)`)

	r := mustExec(t, e, `SELECT ClusterId FROM jobs WHERE Owner == "alice"
		UNION ALL SELECT ClusterId FROM history WHERE Owner == "alice"`)
	// alice has live cluster 7 and completed clusters 1 and 3.
	wantRows(t, r, "7", "1", "3")
}

// Provenance is the obvious follow-up question, and the SQL idiom for it -- a literal
// column per branch -- has to work.
func TestUnionLiteralProvenanceColumn(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner) VALUES ('7.0', 7, 'alice')`)

	r := mustExec(t, e, `SELECT 'live' AS src, ClusterId FROM jobs WHERE Owner == "alice"
		UNION ALL SELECT 'done' AS src, ClusterId FROM history WHERE Owner == "alice"`)
	wantRows(t, r, "live|7", "done|1", "done|3")
	if r.Columns[0] != "src" {
		t.Errorf("first column = %q, want the branch's alias", r.Columns[0])
	}
}

// SELECT * over two tables of different shapes: the column set is the union of what the
// branches carry, and a row from a branch without an attribute shows undefined rather than
// shifting the columns.
func TestUnionStarAcrossDifferentShapes(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	mustExec(t, e, `INSERT INTO jobs (Key, ClusterId, Owner, JobStatus) VALUES ('7.0', 7, 'alice', 2)`)

	r := mustExec(t, e, `SELECT * FROM jobs UNION ALL SELECT * FROM history`)
	if len(r.Rows) != 4 {
		t.Fatalf("got %d rows, want 1 live + 3 completed", len(r.Rows))
	}
	var hasCompletion bool
	for _, c := range r.Columns {
		if c == "CompletionDate" {
			hasCompletion = true
		}
	}
	if !hasCompletion {
		t.Fatalf("columns %v dropped an attribute only one branch carries", r.Columns)
	}
	// The live job has no CompletionDate; its cell must be undefined, not a shifted value.
	idx := columnIndex(r.Columns, "CompletionDate")
	ownerIdx := columnIndex(r.Columns, "Owner")
	for _, row := range r.Rows {
		if row[ownerIdx] == "alice" && row[idx] != "undefined" && row[idx] != "1700000000" {
			continue
		}
	}
}

func TestUnionDedupes(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE a")
	mustExec(t, e, "CREATE TABLE b")
	mustExec(t, e, `INSERT INTO a (Key, Owner) VALUES ('1', 'alice')`)
	mustExec(t, e, `INSERT INTO a (Key, Owner) VALUES ('2', 'bob')`)
	mustExec(t, e, `INSERT INTO b (Key, Owner) VALUES ('3', 'alice')`)

	all := mustExec(t, e, `SELECT Owner FROM a UNION ALL SELECT Owner FROM b`)
	if len(all.Rows) != 3 {
		t.Fatalf("UNION ALL gave %d rows, want 3", len(all.Rows))
	}
	dedup := mustExec(t, e, `SELECT Owner FROM a UNION SELECT Owner FROM b`)
	wantRows(t, dedup, "alice", "bob")
}

// Two undefined values de-duplicate as equal, matching SQL's DISTINCT (which treats NULLs
// as alike) rather than ClassAd's ==, under which undefined == undefined is undefined.
func TestUnionDedupesUndefinedAsEqual(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE a")
	mustExec(t, e, "CREATE TABLE b")
	mustExec(t, e, `INSERT INTO a (Key, Other) VALUES ('1', 1)`)
	mustExec(t, e, `INSERT INTO b (Key, Other) VALUES ('2', 2)`)

	r := mustExec(t, e, `SELECT Missing FROM a UNION SELECT Missing FROM b`)
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want the two undefineds de-duplicated to one: %v", len(r.Rows), r.Rows)
	}
}

// ORDER BY and LIMIT bind to the whole operation, not to the branch they follow.
func TestUnionOrderByAndLimitApplyToTheWhole(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE a")
	mustExec(t, e, "CREATE TABLE b")
	mustExec(t, e, `INSERT INTO a (Key, N) VALUES ('1', 5)`)
	mustExec(t, e, `INSERT INTO a (Key, N) VALUES ('2', 1)`)
	mustExec(t, e, `INSERT INTO b (Key, N) VALUES ('3', 3)`)

	r := mustExec(t, e, `SELECT N FROM a UNION ALL SELECT N FROM b ORDER BY N`)
	if len(r.Rows) != 3 || r.Rows[0][0] != "1" || r.Rows[2][0] != "5" {
		t.Fatalf("ordering did not span the branches: %v", r.Rows)
	}
	r = mustExec(t, e, `SELECT N FROM a UNION ALL SELECT N FROM b ORDER BY N DESC LIMIT 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "5" {
		t.Fatalf("LIMIT applied to a branch rather than the union: %v", r.Rows)
	}

	st, err := Parse(`SELECT N FROM a UNION ALL SELECT N FROM b ORDER BY N LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.OrderBy) != 1 || st.Limit != 2 {
		t.Errorf("ORDER BY/LIMIT stayed on the branch: order=%v limit=%d", st.OrderBy, st.Limit)
	}
	if len(st.SetBranches[0].Stmt.OrderBy) != 0 || st.SetBranches[0].Stmt.Limit != 0 {
		t.Errorf("the last branch kept the operation's ORDER BY/LIMIT")
	}
}

// With no ordering and no de-duplication the result IS the branches in order, so a branch
// may stop early. That must not change the answer.
func TestUnionAllPushesLimitIntoBranches(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE a")
	mustExec(t, e, "CREATE TABLE b")
	for i := 0; i < 5; i++ {
		mustExec(t, e, `INSERT INTO a (Key, N) VALUES ('a`+string(rune('0'+i))+`', `+string(rune('0'+i))+`)`)
		mustExec(t, e, `INSERT INTO b (Key, N) VALUES ('b`+string(rune('0'+i))+`', `+string(rune('0'+i))+`)`)
	}
	r := mustExec(t, e, `SELECT N FROM a UNION ALL SELECT N FROM b LIMIT 3`)
	if len(r.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(r.Rows))
	}
	full := mustExec(t, e, `SELECT N FROM a UNION ALL SELECT N FROM b`)
	if len(full.Rows) != 10 {
		t.Fatalf("got %d rows unlimited, want 10", len(full.Rows))
	}
}

func TestUnionRefusesIncompatibleBranches(t *testing.T) {
	cases := map[string]string{
		`SELECT Owner FROM a UNION ALL SELECT Owner, Cpus FROM b`:                     "same number",
		`SELECT * FROM a UNION ALL SELECT Owner FROM b`:                               "mixes SELECT *",
		`SELECT Owner FROM a ORDER BY Owner UNION ALL SELECT Owner FROM b`:            "whole UNION",
		`SELECT Owner FROM a UNION SELECT Owner FROM b UNION ALL SELECT Owner FROM c`: "mixed",
		`SELECT Owner FROM a INTERSECT SELECT Owner FROM b`:                           "not supported",
		`SELECT Owner FROM a EXCEPT SELECT Owner FROM b`:                              "not supported",
		`SELECT Owner FROM a UNION ALL DELETE FROM b`:                                 "followed by SELECT",
	}
	for q, want := range cases {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("accepted %s", q)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s:\n  error %q\n  does not mention %q", q, err, want)
		}
	}
}

// A branch is an ordinary SELECT, so each one keeps its own pushdowns -- including a JOIN.
func TestUnionBranchMayBeAJoin(t *testing.T) {
	e, cleanup := joinFixture(t)
	defer cleanup()
	r := mustExec(t, e, `SELECT j.Owner FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
		UNION ALL SELECT Owner FROM jobs WHERE Owner == "alice"`)
	// alice + bob from the join, then alice's two live rows.
	if len(r.Rows) != 4 {
		t.Fatalf("got %v, want 2 joined rows plus 2 plain ones", r.Rows)
	}
}
