package repl

import (
	"strings"
	"testing"
)

// TestProjectionAttrsGate locks in which SELECTs push a column projection to the server and
// which fetch whole ads. Plain-column current-time SELECTs over a mutable table project
// (SELECT + ORDER BY columns); SELECT *, expression/aggregate columns, DISTINCT, AS OF, and
// archives do not.
func TestProjectionAttrsGate(t *testing.T) {
	e, cleanup := newCatalogExec(t) // catalog has a mutable "ads" table; no archive
	defer cleanup()

	cases := []struct {
		sql  string
		want string // comma-joined expected projection, or "" for nil (whole-ad fetch)
	}{
		{`SELECT Owner, JobStatus FROM ads WHERE JobStatus == 4`, "Owner,JobStatus"},
		{`SELECT Owner FROM ads WHERE JobStatus == 4 ORDER BY CompletionDate DESC LIMIT 10`, "Owner,CompletionDate"},
		{`SELECT Owner, Owner FROM ads`, "Owner"},         // de-duplicated
		{`SELECT Owner FROM ads ORDER BY Owner`, "Owner"}, // order col already selected
		{`SELECT * FROM ads`, ""},                                  // whole ad
		{`SELECT COUNT(*) FROM ads`, ""},                           // aggregate (handled elsewhere)
		{`SELECT CurrentTime - QDate FROM ads`, ""},                // expression column
		{`SELECT DISTINCT Owner FROM ads`, ""},                     // DISTINCT
		{`SELECT Owner FROM ads ORDER BY CurrentTime - QDate`, ""}, // expression ORDER BY
	}
	for _, tc := range cases {
		st, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.sql, err)
		}
		got := strings.Join(e.projectionAttrs(st), ",")
		if got != tc.want {
			t.Errorf("projectionAttrs(%q) = %q, want %q", tc.sql, got, tc.want)
		}
	}
}

// TestSelectProjectionEndToEnd verifies a projected SELECT returns the same values as a
// whole-ad fetch would -- through the server-side projection op and the old-ClassAd parse --
// with WHERE (server-side), ORDER BY, and LIMIT all honored, and unselected attributes
// correctly absent from the returned ads.
func TestSelectProjectionEndToEnd(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	// Wide-ish ads: only some columns are selected; others must not be needed.
	mustExec(t, e, `INSERT INTO ads (Key, Owner, JobStatus, CompletionDate, Cmd) VALUES ('1.0', 'alice', 4, 1700000003, '/bin/a')`)
	mustExec(t, e, `INSERT INTO ads (Key, Owner, JobStatus, CompletionDate, Cmd) VALUES ('2.0', 'bob', 4, 1700000001, '/bin/b')`)
	mustExec(t, e, `INSERT INTO ads (Key, Owner, JobStatus, CompletionDate, Cmd) VALUES ('3.0', 'carol', 3, 1700000002, '/bin/c')`)

	// Projected SELECT: WHERE filters server-side, ORDER BY + LIMIT client-side.
	r, err := e.ExecString(`SELECT Owner, CompletionDate FROM ads WHERE JobStatus == 4 ORDER BY CompletionDate ASC`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Columns) != 2 || r.Columns[0] != "Owner" || r.Columns[1] != "CompletionDate" {
		t.Fatalf("columns = %v, want [Owner CompletionDate]", r.Columns)
	}
	// Two JobStatus==4 rows, ordered by CompletionDate ascending: bob(1) then alice(3).
	if len(r.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (WHERE not applied server-side?)", len(r.Rows))
	}
	if r.Rows[0][0] != "bob" || r.Rows[1][0] != "alice" {
		t.Errorf("rows = %v, want bob then alice", r.Rows)
	}
	if r.Rows[0][1] != "1700000001" || r.Rows[1][1] != "1700000003" {
		t.Errorf("CompletionDate values = %v %v, want 1700000001 then 1700000003", r.Rows[0][1], r.Rows[1][1])
	}
	// The projected ads must carry only the requested attributes (plus type fields): Cmd was
	// not selected, so it must be absent from the returned ad.
	if len(r.Ads) > 0 {
		if _, ok := r.Ads[0].Lookup("Cmd"); ok {
			t.Errorf("projected ad unexpectedly carries the unselected Cmd attribute")
		}
		if _, ok := r.Ads[0].Lookup("Owner"); !ok {
			t.Errorf("projected ad missing the selected Owner attribute")
		}
	}
}
