package repl

import (
	"fmt"
	"strings"
	"testing"
)

// distinctExec seeds rows where values repeat, so a distinct count and a row count differ and
// a host is shared across owners (making the ungrouped count less than the sum of the groups).
//
//	alice: hosts a,a,b   bob: c,c   carol: a
func distinctExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	rows := []struct {
		owner, host string
		status      int
	}{
		{"alice", "a", 4}, {"alice", "a", 4}, {"alice", "b", 2},
		{"bob", "c", 2}, {"bob", "c", 2}, {"carol", "a", 4},
	}
	for i, r := range rows {
		mustExec(t, e, fmt.Sprintf(
			"INSERT INTO jobs (Key, Owner, Host, JobStatus) VALUES ('k%d', '%s', '%s', %d)",
			i, r.owner, r.host, r.status))
	}
	return e, cleanup
}

// TestCountDistinct covers the counts themselves, grouped and not.
func TestCountDistinct(t *testing.T) {
	e, cleanup := distinctExec(t)
	defer cleanup()

	r := mustExec(t, e, `SELECT COUNT(DISTINCT Owner) AS owners, COUNT(DISTINCT Host) AS hosts, `+
		`COUNT(*) AS n FROM jobs`)
	if !eqStrs(r.Rows[0], []string{"3", "3", "6"}) {
		t.Errorf("ungrouped = %v, want [3 3 6]", r.Rows[0])
	}

	// Per owner. The host shared between alice and carol counts once in each group and once
	// overall -- so the ungrouped 3 is not the sum of the per-group counts (2+1+1).
	r = mustExec(t, e, `SELECT Owner, COUNT(DISTINCT Host) AS hosts FROM jobs GROUP BY Owner`)
	got := rowsByKey(r, 0)
	if got["alice"] != "2" || got["bob"] != "1" || got["carol"] != "1" {
		t.Errorf("per-owner distinct hosts = %v, want alice 2, bob 1, carol 1", got)
	}

	// The default header names the query as written.
	if r := mustExec(t, e, `SELECT COUNT(DISTINCT Owner) FROM jobs`); r.Columns[0] != "COUNT(DISTINCT Owner)" {
		t.Errorf("header = %q, want COUNT(DISTINCT Owner)", r.Columns[0])
	}
}

// TestCountDistinctAgreesWithGroupBy pins the identity that makes the count trustworthy: the
// number of distinct values of an attribute is the number of groups GROUP BY produces over it.
func TestCountDistinctAgreesWithGroupBy(t *testing.T) {
	e, cleanup := distinctExec(t)
	defer cleanup()

	for _, attr := range []string{"Owner", "Host", "JobStatus"} {
		d := mustExec(t, e, `SELECT COUNT(DISTINCT `+attr+`) FROM jobs`)
		g := mustExec(t, e, `SELECT `+attr+`, COUNT(*) FROM jobs GROUP BY `+attr)
		if d.Rows[0][0] != fmt.Sprint(len(g.Rows)) {
			t.Errorf("%s: COUNT(DISTINCT) = %s but GROUP BY produced %d groups",
				attr, d.Rows[0][0], len(g.Rows))
		}
	}
}

// TestCountDistinctIgnoresUndefined checks that a row missing the attribute contributes no
// value, matching COUNT(col).
func TestCountDistinctIgnoresUndefined(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "INSERT INTO jobs (Key, Host) VALUES ('1', 'a')")
	mustExec(t, e, "INSERT INTO jobs (Key, Host) VALUES ('2', 'b')")
	mustExec(t, e, "INSERT INTO jobs (Key, Owner) VALUES ('3', 'nobody')") // no Host

	r := mustExec(t, e, `SELECT COUNT(*) AS n, COUNT(Host) AS defined, COUNT(DISTINCT Host) AS d FROM jobs`)
	if !eqStrs(r.Rows[0], []string{"3", "2", "2"}) {
		t.Errorf("values = %v, want [3 2 2]: undefined is not a distinct value", r.Rows[0])
	}
}

// TestCountDistinctComposes checks that it behaves like any other aggregate: it takes a
// FILTER, sits inside an expression column, and works in a HAVING.
func TestCountDistinctComposes(t *testing.T) {
	e, cleanup := distinctExec(t)
	defer cleanup()

	// Completed rows are on host a only.
	r := mustExec(t, e, `SELECT COUNT(DISTINCT Host) AS all_hosts, `+
		`COUNT(DISTINCT Host) FILTER (WHERE JobStatus = 4) AS done_hosts FROM jobs`)
	if !eqStrs(r.Rows[0], []string{"3", "1"}) {
		t.Errorf("with filter = %v, want [3 1]", r.Rows[0])
	}

	// Inside an expression column.
	r = mustExec(t, e, `SELECT 100 * COUNT(DISTINCT Host) / COUNT(*) AS pct FROM jobs`)
	if r.Rows[0][0] != "50" { // 3 hosts over 6 rows
		t.Errorf("expression over a distinct count = %v, want 50", r.Rows[0])
	}

	// In a HAVING: only alice ran on more than one host.
	r = mustExec(t, e, `SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner `+
		`HAVING COUNT(DISTINCT Host) > 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "alice" {
		t.Errorf("HAVING over a distinct count = %v, want just alice", r.Rows)
	}
}

// TestCountDistinctErrors pins the refusals.
func TestCountDistinctErrors(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT COUNT(DISTINCT *) FROM jobs`, "not meaningful"},
		{`SELECT SUM(DISTINCT Cpus) FROM jobs`, "only supported for COUNT"},
		{`SELECT MAX(DISTINCT Cpus) FROM jobs`, "only supported for COUNT"},
	} {
		_, err := Parse(tc.sql)
		if err == nil {
			t.Errorf("%s: expected an error", tc.sql)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q, want it to mention %q", tc.sql, err, tc.want)
		}
	}
}

// TestCountDistinctParse pins the parsed shape, so the wire mapping is reviewable without a
// server.
func TestCountDistinctParse(t *testing.T) {
	st, err := Parse(`SELECT COUNT(DISTINCT Host) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	it := st.Items[0]
	if !it.IsAggregate() || it.Agg != aggCountDistinct || it.Col != "Host" {
		t.Errorf("item = %+v, want a distinct count over Host", it)
	}
	// A plain COUNT is untouched.
	st, err = Parse(`SELECT COUNT(Host) FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	if it := st.Items[0]; it.Agg != "COUNT" {
		t.Errorf("plain COUNT became %q", it.Agg)
	}
}
