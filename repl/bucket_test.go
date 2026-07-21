package repl

import (
	"fmt"
	"testing"
)

func TestParseBucketWidth(t *testing.T) {
	ok := map[string]int64{
		"30s": 30, "5m": 300, "1h": 3600, "2h": 7200,
		"1d": 86400, "1w": 604800, "45": 45, " 10m ": 600,
	}
	for in, want := range ok {
		got, err := parseBucketWidth(in)
		if err != nil {
			t.Fatalf("parseBucketWidth(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("parseBucketWidth(%q) = %d, want %d", in, got, want)
		}
	}
	for _, bad := range []string{"", "0", "-5m", "abc", "5x", "1.5h"} {
		if _, err := parseBucketWidth(bad); err == nil {
			t.Errorf("parseBucketWidth(%q) should error", bad)
		}
	}
}

func TestParseTimeBucketSelect(t *testing.T) {
	st, err := Parse(`SELECT time_bucket(CompletionDate, '1h') AS time, COUNT(*) AS metric_jobs ` +
		`FROM ads GROUP BY time_bucket(CompletionDate, '1h')`)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(st.Items))
	}
	b := st.Items[0]
	if !b.Bucket || b.Col != "CompletionDate" || b.BucketWidth != 3600 || b.Alias != "time" {
		t.Fatalf("bucket item = %+v", b)
	}
	if !st.Items[1].IsAggregate() {
		t.Fatalf("second item should be an aggregate: %+v", st.Items[1])
	}
	if len(st.GroupBy) != 1 || st.GroupBy[0] != "time_bucket(completiondate,3600)" {
		t.Fatalf("GroupBy = %v, want [time_bucket(completiondate,3600)]", st.GroupBy)
	}
}

func TestParseTimeBucketErrors(t *testing.T) {
	bad := []string{
		// time_bucket in SELECT but no GROUP BY
		`SELECT time_bucket(CompletionDate, '1h') AS t, COUNT(*) FROM ads`,
		// width mismatch between SELECT and GROUP BY
		`SELECT time_bucket(CompletionDate, '1h') AS t, COUNT(*) FROM ads GROUP BY time_bucket(CompletionDate, '5m')`,
		// GROUP BY buckets a column that isn't selected
		`SELECT COUNT(*) FROM ads GROUP BY time_bucket(CompletionDate, '1h')`,
		// invalid width literal
		`SELECT time_bucket(CompletionDate, 'xyz') AS t FROM ads GROUP BY time_bucket(CompletionDate, 'xyz')`,
	}
	for _, q := range bad {
		if _, err := Parse(q); err == nil {
			t.Errorf("expected a parse/validate error for: %s", q)
		}
	}
}

// TestExecTimeBucket inserts rows with unix-epoch CompletionDate across several hours
// and verifies time_bucket groups them into epoch-aligned buckets, that COUNT/SUM
// reduce per bucket, and that a row with an undefined bucket attribute drops out.
func TestExecTimeBucket(t *testing.T) {
	e, _, cleanup := newPrivCatalogExec(t)
	defer cleanup()

	// CompletionDate (unix seconds), RequestCpus. 1h buckets align at 3600 boundaries.
	rows := []struct {
		key  string
		when int64
		cpus int
	}{
		{"1.0", 3600, 1},  // bucket 3600
		{"1.1", 3700, 2},  // bucket 3600
		{"2.0", 7200, 4},  // bucket 7200
		{"2.1", 7300, 8},  // bucket 7200
		{"3.0", 10800, 3}, // bucket 10800
	}
	for _, r := range rows {
		q := fmt.Sprintf(`INSERT INTO %s (Key, CompletionDate, RequestCpus) VALUES ('%s', %d, %d)`,
			DefaultTable, r.key, r.when, r.cpus)
		if _, err := e.ExecString(q); err != nil {
			t.Fatalf("insert %s: %v", r.key, err)
		}
	}
	// A row with no CompletionDate must fall out of the series, not crash or bucket at 0.
	if _, err := e.ExecString(fmt.Sprintf(`INSERT INTO %s (Key, RequestCpus) VALUES ('9.9', 5)`, DefaultTable)); err != nil {
		t.Fatal(err)
	}

	res, err := e.ExecString(fmt.Sprintf(
		`SELECT time_bucket(CompletionDate, '1h') AS time, COUNT(*) AS metric_jobs, SUM(RequestCpus) AS metric_cpus `+
			`FROM %s GROUP BY time_bucket(CompletionDate, '1h') ORDER BY time`, DefaultTable))
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Columns; len(got) != 3 || got[0] != "time" {
		t.Fatalf("columns = %v", got)
	}
	want := [][]string{
		{"3600", "2", "3"},
		{"7200", "2", "12"},
		{"10800", "1", "3"},
	}
	if len(res.Rows) != len(want) {
		t.Fatalf("rows = %v, want %v", res.Rows, want)
	}
	for i, w := range want {
		if res.Rows[i][0] != w[0] || res.Rows[i][1] != w[1] || res.Rows[i][2] != w[2] {
			t.Errorf("row %d = %v, want %v", i, res.Rows[i], w)
		}
	}
}

// TestExecTimeBucketWithLabel checks a second grouping column (a label) composes with
// the time bucket, producing one series per (bucket, label).
func TestExecTimeBucketWithLabel(t *testing.T) {
	e, _, cleanup := newPrivCatalogExec(t)
	defer cleanup()

	insert := func(key string, when int64, owner string) {
		q := fmt.Sprintf(`INSERT INTO %s (Key, CompletionDate, Owner) VALUES ('%s', %d, '%s')`,
			DefaultTable, key, when, owner)
		if _, err := e.ExecString(q); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}
	insert("1.0", 3600, "alice")
	insert("1.1", 3700, "alice")
	insert("1.2", 3800, "bob")
	insert("2.0", 7200, "alice")

	res, err := e.ExecString(fmt.Sprintf(
		`SELECT time_bucket(CompletionDate, '1h') AS time, Owner AS label_owner, COUNT(*) AS metric_jobs `+
			`FROM %s GROUP BY time_bucket(CompletionDate, '1h'), Owner ORDER BY time, label_owner`, DefaultTable))
	if err != nil {
		t.Fatal(err)
	}
	// bucket 3600: alice=2, bob=1 ; bucket 7200: alice=1
	want := [][]string{
		{"3600", "alice", "2"},
		{"3600", "bob", "1"},
		{"7200", "alice", "1"},
	}
	if len(res.Rows) != len(want) {
		t.Fatalf("rows = %v, want %v", res.Rows, want)
	}
	for i, w := range want {
		for j := range w {
			if res.Rows[i][j] != w[j] {
				t.Errorf("row %d = %v, want %v", i, res.Rows[i], w)
			}
		}
	}
}
