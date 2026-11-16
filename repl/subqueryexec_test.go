package repl

import (
	"sort"
	"strings"
	"testing"
)

// subqueryFixture builds two related tables: jobs, and the epoch rows recording that a job
// ran. Cluster 4 has no epoch row at all, which is what the NOT EXISTS cases turn on, and
// cluster 2 has two of them, which is what tells a semi join from an inner one.
func subqueryFixture(t *testing.T) (*Executor, func()) {
	t.Helper()
	e, cleanup := newCatalogExec(t)
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "CREATE TABLE epochs")
	mustExec(t, e, "CREATE TABLE users")
	for _, q := range []string{
		`INSERT INTO jobs (Key, ClusterId, Owner, JobStatus) VALUES ('1', 1, 'alice', 4)`,
		`INSERT INTO jobs (Key, ClusterId, Owner, JobStatus) VALUES ('2', 2, 'bob', 2)`,
		`INSERT INTO jobs (Key, ClusterId, Owner, JobStatus) VALUES ('3', 3, 'alice', 5)`,
		`INSERT INTO jobs (Key, ClusterId, Owner, JobStatus) VALUES ('4', 4, 'carol', 5)`,
		`INSERT INTO epochs (Key, ClusterId, Restarts) VALUES ('e1', 1, 0)`,
		`INSERT INTO epochs (Key, ClusterId, Restarts) VALUES ('e2', 2, 3)`,
		`INSERT INTO epochs (Key, ClusterId, Restarts) VALUES ('e3', 2, 1)`,
		`INSERT INTO epochs (Key, ClusterId, Restarts) VALUES ('e4', 3, 0)`,
		`INSERT INTO users (Key, Owner, Priority) VALUES ('u1', 'alice', 500)`,
		`INSERT INTO users (Key, Owner, Priority) VALUES ('u2', 'bob', 10)`,
	} {
		mustExec(t, e, q)
	}
	return e, cleanup
}

// clusters runs a query and returns its ClusterId column, sorted, as one string.
func clusters(t *testing.T, e *Executor, query string) string {
	t.Helper()
	r, err := e.ExecString(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	var got []string
	for _, row := range r.Rows {
		got = append(got, row[0])
	}
	sort.Strings(got)
	return strings.Join(got, ",")
}

func TestSubqueryPredicatesSelectTheRightRows(t *testing.T) {
	e, cleanup := subqueryFixture(t)
	defer cleanup()

	for _, tc := range []struct{ name, query, want string }{
		{
			name:  "correlated EXISTS",
			query: `SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
			want:  "1,2,3",
		},
		{
			// Cluster 2 has TWO epoch rows and must still appear once: a semi join
			// keeps a row once however many times the other side matches, which is
			// what makes EXISTS safe where a join would multiply.
			name:  "EXISTS does not multiply rows",
			query: `SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId AND e.Restarts >= 0)`,
			want:  "1,2,3",
		},
		{
			name:  "correlated NOT EXISTS",
			query: `SELECT ClusterId FROM jobs WHERE NOT EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
			want:  "4",
		},
		{
			name:  "NOT EXISTS beside an ordinary conjunct",
			query: `SELECT ClusterId FROM jobs WHERE JobStatus == 5 AND NOT EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`,
			want:  "4",
		},
		{
			name:  "the subquery's own filter narrows it",
			query: `SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId AND e.Restarts > 0)`,
			want:  "2",
		},
		{
			name:  "IN over a subquery",
			query: `SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Priority > 100)`,
			want:  "1,3",
		},
		{
			name:  "NOT IN over a subquery",
			query: `SELECT ClusterId FROM jobs WHERE Owner NOT IN (SELECT Owner FROM users WHERE Priority > 100)`,
			want:  "2,4",
		},
		{
			name:  "two predicates at once",
			query: `SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId) AND Owner IN (SELECT Owner FROM users WHERE Priority > 100)`,
			want:  "1,3",
		},
		{
			name:  "uncorrelated EXISTS that holds",
			query: `SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.Restarts > 2)`,
			want:  "1,2,3,4",
		},
		{
			name:  "uncorrelated EXISTS that does not hold",
			query: `SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.Restarts > 99)`,
			want:  "",
		},
		{
			name:  "uncorrelated NOT EXISTS that holds",
			query: `SELECT ClusterId FROM jobs WHERE NOT EXISTS (SELECT 1 FROM epochs e WHERE e.Restarts > 99)`,
			want:  "1,2,3,4",
		},
		{
			// An empty key set is not a degenerate case: IN over nothing matches
			// nothing, and NOT IN over nothing matches everything.
			name:  "IN over an empty subquery",
			query: `SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Priority > 9000)`,
			want:  "",
		},
		{
			name:  "NOT IN over an empty subquery",
			query: `SELECT ClusterId FROM jobs WHERE Owner NOT IN (SELECT Owner FROM users WHERE Priority > 9000)`,
			want:  "1,2,3,4",
		},
		{
			// A conjunct of the subquery's WHERE that reads only the OUTER row filters
			// the outer scan; it does not depend on the subquery's table at all.
			name:  "an outer-only conjunct inside the subquery",
			query: `SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId AND jobs.Owner == 'alice')`,
			want:  "1,3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := clusters(t, e, tc.query); got != tc.want {
				t.Errorf("query: %s\n  got  %q\n  want %q", tc.query, got, tc.want)
			}
		})
	}
}

// The predicate composes with everything the statement already did: it is a filter on the
// rows, applied before grouping, ordering and limiting.
func TestSubqueryComposesWithTheRestOfTheStatement(t *testing.T) {
	e, cleanup := subqueryFixture(t)
	defer cleanup()

	r, err := e.ExecString(`SELECT COUNT(*) FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0] != "3" {
		t.Errorf("COUNT(*) with EXISTS = %s, want 3", r.Rows[0][0])
	}

	r, err = e.ExecString(`SELECT Owner, COUNT(*) FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.ClusterId == jobs.ClusterId) GROUP BY Owner ORDER BY Owner`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 || r.Rows[0][0] != "alice" || r.Rows[0][1] != "2" {
		t.Errorf("GROUP BY with EXISTS = %v, want alice=2 then bob=1", r.Rows)
	}
}

func TestSubqueryExecRefusals(t *testing.T) {
	e, cleanup := subqueryFixture(t)
	defer cleanup()

	for _, tc := range []struct{ query, want string }{
		{`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM epochs e WHERE e.Restarts > jobs.ClusterId)`,
			"other than an equality"},
		{`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner, Priority FROM users)`,
			"exactly one column"},
		{`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT * FROM users)`,
			"not `*`"},
		{`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT COUNT(*) FROM users)`,
			"aggregate subquery"},
		{`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users LIMIT 1)`,
			"LIMIT inside a subquery"},
		{`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users ORDER BY Owner)`,
			"ORDER BY inside a subquery"},
		{`SELECT ClusterId FROM jobs WHERE EXISTS (SELECT 1 FROM jobs WHERE jobs.ClusterId == jobs.ClusterId)`,
			"the same as the outer query's"},
	} {
		if _, err := e.ExecString(tc.query); err == nil {
			t.Errorf("accepted %s", tc.query)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s:\n  error %q\n  want it to mention %q", tc.query, err, tc.want)
		}
	}
}

// Past the cap the predicate refuses rather than building a constraint that would degrade
// the outer read to a full scan.
func TestSubqueryRefusesAnOversizeKeySet(t *testing.T) {
	e, cleanup := subqueryFixture(t)
	defer cleanup()
	e.maxInList = 1

	// Two distinct owners in users, against a cap of one.
	_, err := e.ExecString(`SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users)`)
	if err == nil {
		t.Fatal("accepted a key set over the cap")
	}
	if !strings.Contains(err.Error(), "more than 1 distinct Owner values") {
		t.Errorf("error %q does not name the cap and the column", err)
	}
	// Under the cap it still answers.
	e.maxInList = 8
	if got := clusters(t, e, `SELECT ClusterId FROM jobs WHERE Owner IN (SELECT Owner FROM users)`); got != "1,2,3" {
		t.Errorf("under the cap: got %q, want %q", got, "1,2,3")
	}
}
