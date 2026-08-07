package repl

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// The client-side scan paths fetch only the attributes they read (scanAttrs). Projecting
// short would not fail loudly -- the missing attribute evaluates to undefined, so the query
// returns a plausible wrong answer. These tests therefore query attributes that are NOT
// selected, which is exactly where an incomplete projection shows up.

// attrSet renders scanAttrs' result as a lowercase, sorted, comparable string.
func attrSet(t *testing.T, e *Executor, sql string) string {
	t.Helper()
	st, err := Parse(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	attrs := e.scanAttrs(st)
	if attrs == nil {
		return "<whole ad>"
	}
	low := make([]string, len(attrs))
	for i, a := range attrs {
		low[i] = strings.ToLower(a)
	}
	sort.Strings(low)
	return strings.Join(low, ",")
}

// TestScanAttrsCoversEveryRead pins the attribute set for the constructs that read an ad
// client-side: a computed group key, a time_bucket key, aggregate arguments, FILTER
// predicates, HAVING's own aggregates, and a window's partition/order terms.
func TestScanAttrsCoversEveryRead(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()

	for _, tc := range []struct{ sql, want string }{
		// A computed key reads the expression's attributes, not the alias.
		{`SELECT CASE WHEN Memory > 4096 THEN 'big' ELSE 'small' END AS sz, COUNT(*) FROM jobs GROUP BY sz`,
			"memory"},
		// Aggregate arguments and their FILTER predicates.
		{`SELECT Memory / 1024 AS gb, SUM(Cpus) AS c, ` +
			`COUNT(*) FILTER (WHERE JobStatus == 4) AS done FROM jobs GROUP BY gb`,
			"cpus,jobstatus,memory"},
		// HAVING's aggregate reads an attribute nothing else mentions.
		{`SELECT Memory / 1024 AS gb, COUNT(*) FROM jobs GROUP BY gb HAVING MAX(RemoteWallClockTime) > 60`,
			"memory,remotewallclocktime"},
		// A window reads its PARTITION BY and ORDER BY terms even though neither is selected.
		{`SELECT ClusterId, ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate DESC) AS n FROM jobs`,
			"clusterid,owner,qdate"},
		// time_bucket groups by the bucketed attribute.
		{`SELECT time_bucket(QDate, '1h') AS h, COUNT(*) FROM jobs GROUP BY time_bucket(QDate, '1h')`, "qdate"},
		// The statement's own ORDER BY is read client-side too when it names an attribute.
		{`SELECT Owner, ROW_NUMBER() OVER (ORDER BY QDate) AS n FROM jobs ORDER BY Owner`,
			"owner,qdate"},
	} {
		if got := attrSet(t, e, tc.sql); got != tc.want {
			t.Errorf("%s\n  scanAttrs = %s\n       want = %s", tc.sql, got, tc.want)
		}
	}
}

// TestScanAttrsFallsBack checks that anything it cannot analyze fetches whole ads rather than
// guessing at a projection.
func TestScanAttrsFallsBack(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()

	for _, sql := range []string{
		// AS OF has no projected query variant.
		`SELECT Owner, COUNT(*) FROM jobs AS OF '2020-01-01T00:00:00Z' GROUP BY Owner`,
		// A star item may read any attribute.
		`SELECT * FROM jobs`,
	} {
		if got := attrSet(t, e, sql); got != "<whole ad>" {
			t.Errorf("%s\n  projected %s, want the whole-ad fetch", sql, got)
		}
	}
}

// scanProjExec seeds rows whose interesting attributes are deliberately NOT the selected
// ones, so an under-projected fetch changes the answer.
//
//	Memory 1024 2048 8192 16384 512, Cpus 1..5, JobStatus 4 for the two big ones
func scanProjExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	for i, m := range []int{1024, 2048, 8192, 16384, 512} {
		status := 2
		if m > 4096 {
			status = 4
		}
		mustExec(t, e, fmt.Sprintf(
			"INSERT INTO jobs (Key, Owner, Memory, Cpus, JobStatus, QDate) "+
				"VALUES ('k%d', 'u%d', %d, %d, %d, %d)",
			i, i%2, m, i+1, status, 100-i))
	}
	return e, cleanup
}

// TestClientScanProjectionKeepsResults runs a computed-group query whose group key, aggregate
// argument, FILTER and HAVING each read a different unselected attribute.
func TestClientScanProjectionKeepsResults(t *testing.T) {
	e, cleanup := scanProjExec(t)
	defer cleanup()

	// Group by a CASE over Memory, sum Cpus, and count only the completed rows -- none of
	// Memory, Cpus or JobStatus is a selected column.
	r := mustExec(t, e, `SELECT CASE WHEN Memory > 4096 THEN 'big' ELSE 'small' END AS sz, `+
		`SUM(Cpus) AS cpus, COUNT(*) FILTER (WHERE JobStatus == 4) AS done FROM jobs GROUP BY sz`)
	got := map[string][]string{}
	for _, row := range r.Rows {
		got[row[0]] = row[1:]
	}
	// big: 8192 (Cpus 3) + 16384 (Cpus 4) = 7, both completed.
	if !eqStrs(got["big"], []string{"7", "2"}) {
		t.Errorf("big = %v, want [7 2]", got["big"])
	}
	// small: 1024 (1) + 2048 (2) + 512 (5) = 8, none completed.
	if !eqStrs(got["small"], []string{"8", "0"}) {
		t.Errorf("small = %v, want [8 0]", got["small"])
	}

	// HAVING over an attribute that appears nowhere else in the statement.
	r = mustExec(t, e, `SELECT CASE WHEN Memory > 4096 THEN 'big' ELSE 'small' END AS sz, `+
		`COUNT(*) AS n FROM jobs GROUP BY sz HAVING MAX(Cpus) > 4`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "small" {
		t.Errorf("HAVING MAX(Cpus) > 4 = %v, want just small (Cpus 5)", r.Rows)
	}
}

// TestWindowProjectionUnselectedTerms ranks by a partition and order neither of which is
// selected: under-projecting either one collapses every row into one partition, or ranks them
// in scan order, and the numbering would be wrong rather than absent.
func TestWindowProjectionUnselectedTerms(t *testing.T) {
	e, cleanup := scanProjExec(t)
	defer cleanup()

	// QDate descends as the key index rises, so per owner the newest row is k0 / k1.
	r := mustExec(t, e, `SELECT Cpus, ROW_NUMBER() OVER (PARTITION BY Owner ORDER BY QDate DESC) AS n `+
		`FROM jobs QUALIFY n = 1 ORDER BY Cpus`)
	if len(r.Rows) != 2 {
		t.Fatalf("newest per owner = %v, want two rows", r.Rows)
	}
	if r.Rows[0][0] != "1" || r.Rows[1][0] != "2" {
		t.Errorf("newest per owner = %v, want the Cpus 1 and Cpus 2 rows", r.Rows)
	}

	// Ordering by an unselected attribute: largest memory first.
	r = mustExec(t, e, `SELECT Cpus, ROW_NUMBER() OVER (ORDER BY Memory DESC) AS n FROM jobs QUALIFY n = 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "4" {
		t.Errorf("largest-memory row = %v, want the Cpus 4 row (Memory 16384)", r.Rows)
	}
}
