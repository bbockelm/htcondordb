package repl

import (
	"fmt"
	"strings"
	"testing"
)

// seedHistory fills a jobs table with a day/owner mix, the shape a daily roll-up reads.
//
//	alice: 2 jobs on day 1 (3 cpus), 1 job on day 2 (4 cpus)
//	bob:   1 job  on day 1 (2 cpus)
func seedHistory(t *testing.T, e *Executor) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE history")
	rows := []struct {
		key   string
		owner string
		day   int
		cpus  int
	}{
		{"1.0", "alice", 1, 1},
		{"2.0", "alice", 1, 2},
		{"3.0", "alice", 2, 4},
		{"4.0", "bob", 1, 2},
	}
	for _, r := range rows {
		mustExec(t, e, fmt.Sprintf(
			"INSERT INTO history (Key, Owner, Day, RequestCpus) VALUES ('%s', '%s', %d, %d)",
			r.key, r.owner, r.day, r.cpus))
	}
}

// The motivating statement, end to end through the SQL surface.
func TestInsertSelectRollup(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	seedHistory(t, e)
	mustExec(t, e, "CREATE TABLE daily_summaries PRIMARY KEY (Day, Owner)")

	r := mustExec(t, e, `INSERT INTO daily_summaries
		SELECT Day, Owner, COUNT(*) AS Jobs, SUM(RequestCpus) AS Cpus
		FROM history GROUP BY Day, Owner`)
	if r.Affected != 3 {
		t.Fatalf("INSERT ... SELECT wrote %d rows, want 3 (alice/1, alice/2, bob/1)", r.Affected)
	}

	// The summary is a queryable table, and its numbers are NUMBERS: a count stored as
	// text would still print as "2" here but would not survive a numeric comparison, so
	// the WHERE is the part of this that matters.
	got := mustExec(t, e, "SELECT Owner, Jobs, Cpus FROM daily_summaries WHERE Day == 1 && Jobs > 1")
	if len(got.Rows) != 1 {
		t.Fatalf("numeric filter over the summary returned %d rows, want 1: %v", len(got.Rows), got.Rows)
	}
	if got.Rows[0][0] != "alice" || got.Rows[0][1] != "2" || got.Rows[0][2] != "3" {
		t.Fatalf("summary row = %v, want [alice 2 3]", got.Rows[0])
	}
}

// Re-running converges rather than accumulating -- the property the declared key buys.
func TestInsertSelectRerunConverges(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	seedHistory(t, e)
	mustExec(t, e, "CREATE TABLE daily_summaries PRIMARY KEY (Day, Owner)")

	stmt := `INSERT INTO daily_summaries
		SELECT Day, Owner, COUNT(*) AS Jobs FROM history GROUP BY Day, Owner`
	mustExec(t, e, stmt)
	mustExec(t, e, stmt)
	mustExec(t, e, stmt)

	if r := mustExec(t, e, "SELECT COUNT(*) FROM daily_summaries"); r.Rows[0][0] != "3" {
		t.Fatalf("after three runs the summary holds %s rows, want 3", r.Rows[0][0])
	}

	// A new job in the source updates the existing summary row rather than adding one.
	mustExec(t, e, "INSERT INTO history (Key, Owner, Day, RequestCpus) VALUES ('5.0', 'bob', 1, 8)")
	mustExec(t, e, stmt)
	if r := mustExec(t, e, "SELECT COUNT(*) FROM daily_summaries"); r.Rows[0][0] != "3" {
		t.Fatalf("summary holds %s rows after the source grew, want 3", r.Rows[0][0])
	}
	r := mustExec(t, e, "SELECT Jobs FROM daily_summaries WHERE Owner == \"bob\" && Day == 1")
	if len(r.Rows) != 1 || r.Rows[0][0] != "2" {
		t.Fatalf("bob/day1 Jobs = %v, want 2", r.Rows)
	}
}

func TestCreateTablePrimaryKeyParses(t *testing.T) {
	st, err := Parse("CREATE TABLE t PRIMARY KEY (Day, Owner)")
	if err != nil {
		t.Fatal(err)
	}
	if st.Kind != StmtCreateTable {
		t.Fatalf("kind = %v, want CREATE TABLE", st.Kind)
	}
	if strings.Join(st.PrimaryKey, ",") != "Day,Owner" {
		t.Fatalf("primary key = %v, want [Day Owner]", st.PrimaryKey)
	}
}

func TestInsertSelectRefusals(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{{
		name: "unaliased aggregate",
		sql:  "INSERT INTO s SELECT Owner, COUNT(*) FROM history GROUP BY Owner",
		want: "alias",
	}, {
		name: "plain row copy",
		sql:  "INSERT INTO s SELECT Owner, Cmd FROM history",
		want: "collapse duplicate rows",
	}, {
		name: "same table",
		sql:  "INSERT INTO history SELECT Owner, COUNT(*) AS n FROM history GROUP BY Owner",
		want: "same table",
	}, {
		// The parser refuses this before the insert ever sees it, and says why more
		// precisely than the insert would; asserted here so the refusal stays.
		name: "star",
		sql:  "INSERT INTO s SELECT * FROM history GROUP BY Owner",
		want: "cannot be used with GROUP BY",
	}, {
		name: "having",
		sql:  "INSERT INTO s SELECT Owner, COUNT(*) AS n FROM history GROUP BY Owner HAVING COUNT(*) > 1",
		want: "HAVING",
	}, {
		name: "order by",
		sql:  "INSERT INTO s SELECT Owner, COUNT(*) AS n FROM history GROUP BY Owner ORDER BY Owner",
		want: "ORDER BY",
	}, {
		name: "duplicate target column",
		sql:  "INSERT INTO s SELECT Owner, COUNT(*) AS Owner FROM history GROUP BY Owner",
		want: "one attribute",
	}}

	e, cleanup := newCatalogExec(t)
	defer cleanup()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := Parse(tc.sql)
			if err != nil {
				// A parse-level refusal is fine so long as it explains itself.
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("parse error %v should mention %q", err, tc.want)
				}
				return
			}
			_, err = e.Exec(st)
			if err == nil {
				t.Fatalf("%s was accepted, want a refusal", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v should mention %q", err, tc.want)
			}
		})
	}
}

// A roll-up into a table that declares no key is refused, not silently given generated
// keys -- which would make the second run duplicate every row.
func TestInsertSelectRefusedWithoutPrimaryKey(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	seedHistory(t, e)
	mustExec(t, e, "CREATE TABLE daily_summaries")

	st, err := Parse(`INSERT INTO daily_summaries
		SELECT Owner, COUNT(*) AS Jobs FROM history GROUP BY Owner`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Exec(st); err == nil {
		t.Fatal("roll-up into a table with no declared key succeeded, want a refusal")
	} else if !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("error %v should say the table declares no primary key", err)
	}
}

// INSERT ... VALUES is untouched by any of this.
func TestInsertValuesStillWorks(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "INSERT INTO jobs (Key, Owner) VALUES ('1.0', 'alice')")
	if r := mustExec(t, e, "SELECT COUNT(*) FROM jobs"); r.Rows[0][0] != "1" {
		t.Fatalf("COUNT(*) = %s, want 1", r.Rows[0][0])
	}
	// And a missing column list is still an error.
	if _, err := Parse("INSERT INTO jobs VALUES ('1.0')"); err == nil {
		t.Fatal("INSERT without a column list was accepted, want a refusal")
	}
}

// The motivating query against an ARCHIVE, bucketed by day -- "jobs per owner per day"
// rolled up into a summary table. This is the shape the feature exists for: the source is
// history (append-only, columnar, zone-mapped), the grouping is a time bucket, and the
// target is an ordinary queryable table.
func TestInsertSelectFromArchiveBucketed(t *testing.T) {
	const days, perDay = 3, 9
	e, cleanup, want := archiveExecBucketed(t, days, perDay)
	defer cleanup()

	mustExec(t, e, "CREATE TABLE daily_summaries PRIMARY KEY (Day, Owner)")
	r := mustExec(t, e, `INSERT INTO daily_summaries
		SELECT time_bucket(CompletionDate, '1d') AS Day, Owner, COUNT(*) AS Jobs
		FROM history GROUP BY Day, Owner`)
	if r.Affected != len(want) {
		t.Fatalf("rolled up %d rows, want %d groups", r.Affected, len(want))
	}

	// Every group's count must match the fixture's own tally, read back through SQL.
	got := mustExec(t, e, "SELECT Day, Owner, Jobs FROM daily_summaries")
	if len(got.Rows) != len(want) {
		t.Fatalf("summary holds %d rows, want %d", len(got.Rows), len(want))
	}
	for _, row := range got.Rows {
		key := row[0] + "|" + row[1]
		if fmt.Sprint(want[key]) != row[2] {
			t.Errorf("group %s: stored Jobs = %s, want %d", key, row[2], want[key])
		}
	}

	// And it is idempotent against the archive too.
	mustExec(t, e, `INSERT INTO daily_summaries
		SELECT time_bucket(CompletionDate, '1d') AS Day, Owner, COUNT(*) AS Jobs
		FROM history GROUP BY Day, Owner`)
	if again := mustExec(t, e, "SELECT COUNT(*) FROM daily_summaries"); again.Rows[0][0] != fmt.Sprint(len(want)) {
		t.Fatalf("after a second run the summary holds %s rows, want %d", again.Rows[0][0], len(want))
	}
}

func TestAlterTableAddPrimaryKeyEnablesRollup(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	seedHistory(t, e)
	mustExec(t, e, "CREATE TABLE summaries") // created without a key

	// Without a declaration the roll-up is refused...
	st, err := Parse(`INSERT INTO summaries SELECT Owner, COUNT(*) AS Jobs FROM history GROUP BY Owner`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Exec(st); err == nil {
		t.Fatal("roll-up succeeded with no declared key, want a refusal")
	}

	// ...and declaring one afterwards is what ALTER TABLE is for.
	mustExec(t, e, "ALTER TABLE summaries ADD PRIMARY KEY (Owner)")
	r, err := e.Exec(st)
	if err != nil {
		t.Fatalf("roll-up after ALTER TABLE failed: %v", err)
	}
	if r.Affected != 2 {
		t.Fatalf("wrote %d rows, want 2", r.Affected)
	}
}

func TestAlterTableDropPrimaryKey(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE summaries PRIMARY KEY (Owner)")
	r := mustExec(t, e, "ALTER TABLE summaries DROP PRIMARY KEY")
	if !strings.Contains(r.Note, "was Owner") {
		t.Fatalf("note %q should say what was dropped", r.Note)
	}
	// Dropping twice is an error, not a silent no-op.
	st, _ := Parse("ALTER TABLE summaries DROP PRIMARY KEY")
	if _, err := e.Exec(st); err == nil {
		t.Fatal("dropping an absent key succeeded, want a refusal")
	}
}

// ADD refuses to overwrite; changing a key is DROP then ADD.
func TestAlterTableAddRefusesToOverwrite(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE summaries PRIMARY KEY (Day, Owner)")

	st, _ := Parse("ALTER TABLE summaries ADD PRIMARY KEY (Owner)")
	_, err := e.Exec(st)
	if err == nil {
		t.Fatal("ADD over an existing key succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), "DROP PRIMARY KEY first") {
		t.Fatalf("error %v should point at DROP PRIMARY KEY", err)
	}

	// Re-declaring the SAME key is accepted, so a setup script is re-runnable.
	same := mustExec(t, e, "ALTER TABLE summaries ADD PRIMARY KEY (day, owner)")
	if !strings.Contains(same.Note, "already declared") {
		t.Fatalf("note %q should say it was already declared", same.Note)
	}

	// DROP then ADD is the way through.
	mustExec(t, e, "ALTER TABLE summaries DROP PRIMARY KEY")
	mustExec(t, e, "ALTER TABLE summaries ADD PRIMARY KEY (Owner)")
}

// Declaring a key on a POPULATED table cannot rekey what is stored, and the result says so
// -- otherwise the next roll-up writes beside the old rows with nothing reporting it.
func TestAlterTableAddWarnsOnPopulatedTable(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE summaries")
	mustExec(t, e, "INSERT INTO summaries (Key, Owner, Jobs) VALUES ('old-1', 'alice', 1)")
	mustExec(t, e, "INSERT INTO summaries (Key, Owner, Jobs) VALUES ('old-2', 'bob', 2)")

	r := mustExec(t, e, "ALTER TABLE summaries ADD PRIMARY KEY (Owner)")
	if !strings.Contains(r.Note, "2 existing row(s)") {
		t.Fatalf("note %q should name how many rows predate the declaration", r.Note)
	}
	if !strings.Contains(r.Note, "write beside them") {
		t.Fatalf("note %q should say a roll-up writes beside them", r.Note)
	}
}

// CREATE TABLE is idempotent, so it reaches existing tables -- but it must not silently
// CHANGE a declaration that is already there.
func TestCreateTableRefusesToChangeExistingPrimaryKey(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE summaries PRIMARY KEY (Day, Owner)")

	// Re-running the same setup is fine.
	mustExec(t, e, "CREATE TABLE summaries PRIMARY KEY (Day, Owner)")

	// A different key is refused, pointing at the statement that makes it deliberate.
	st, _ := Parse("CREATE TABLE summaries PRIMARY KEY (Owner)")
	_, err := e.Exec(st)
	if err == nil {
		t.Fatal("CREATE TABLE silently changed the key, want a refusal")
	}
	for _, want := range []string{"already declares", "ALTER TABLE"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %v should mention %q", err, want)
		}
	}
}

func TestAlterTableParsing(t *testing.T) {
	add, err := Parse("ALTER TABLE t ADD PRIMARY KEY (Day, Owner)")
	if err != nil {
		t.Fatal(err)
	}
	if add.Kind != StmtAlterTable || add.AlterAction != "add-primary-key" ||
		strings.Join(add.PrimaryKey, ",") != "Day,Owner" {
		t.Fatalf("parsed ADD as %+v", add)
	}
	drop, err := Parse("ALTER TABLE t DROP PRIMARY KEY")
	if err != nil {
		t.Fatal(err)
	}
	if drop.Kind != StmtAlterTable || drop.AlterAction != "drop-primary-key" {
		t.Fatalf("parsed DROP as %+v", drop)
	}
	for _, bad := range []string{"ALTER t ADD PRIMARY KEY (a)", "ALTER TABLE t ADD KEY (a)", "ALTER TABLE t RENAME TO u"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%q parsed, want a refusal", bad)
		}
	}
}

func TestAlterTableUnknownTable(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	st, _ := Parse("ALTER TABLE nosuch ADD PRIMARY KEY (Owner)")
	if _, err := e.Exec(st); err == nil {
		t.Fatal("ALTER on a missing table succeeded, want a refusal")
	}
}
