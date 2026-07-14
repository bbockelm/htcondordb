package repl

import (
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

func newTestExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConn(dbrpc.NewStreamConn(sp)) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	return e, func() { c.Close(); s.Close(); d.Close() }
}

func mustExec(t *testing.T, e *Executor, sql string) *Result {
	t.Helper()
	r, err := e.ExecString(sql)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return r
}

func TestInsertSelectCount(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	mustExec(t, e, "INSERT INTO ads (Key, Owner, Cpus) VALUES ('1.0', 'alice', 4)")
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Cpus) VALUES ('2.0', 'bob', 16)")
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Cpus) VALUES ('3.0', 'alice', 8)")

	r := mustExec(t, e, "SELECT COUNT(*) FROM ads")
	if got := r.Rows[0][0]; got != "3" {
		t.Fatalf("COUNT(*) = %s, want 3", got)
	}

	r = mustExec(t, e, "SELECT COUNT(*) FROM ads WHERE Owner = 'alice'")
	if got := r.Rows[0][0]; got != "2" {
		t.Fatalf("COUNT(*) WHERE Owner=alice = %s, want 2", got)
	}
}

func TestSelectProjectionAndWhere(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Cpus) VALUES ('1.0', 'alice', 4)")
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Cpus) VALUES ('2.0', 'bob', 16)")

	r := mustExec(t, e, "SELECT Owner, Cpus FROM ads WHERE Cpus >= 8")
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1: %v", len(r.Rows), r.Rows)
	}
	if r.Rows[0][0] != "bob" || r.Rows[0][1] != "16" {
		t.Fatalf("row = %v, want [bob 16]", r.Rows[0])
	}
	if r.Columns[0] != "Owner" || r.Columns[1] != "Cpus" {
		t.Fatalf("columns = %v", r.Columns)
	}
}

func TestAggregates(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	for _, v := range []string{"4", "8", "16"} {
		mustExec(t, e, "INSERT INTO ads (Key, Cpus) VALUES ('k"+v+"', "+v+")")
	}
	r := mustExec(t, e, "SELECT COUNT(*), SUM(Cpus), AVG(Cpus), MIN(Cpus), MAX(Cpus) FROM ads")
	row := r.Rows[0]
	want := []string{"3", "28", "9.333333333333334", "4", "16"}
	for i := range want {
		if row[i] != want[i] {
			t.Errorf("agg[%d] (%s) = %s, want %s", i, r.Columns[i], row[i], want[i])
		}
	}
}

func TestUpdate(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Owner, JobStatus) VALUES ('1.0', 'alice', 1)")
	mustExec(t, e, "INSERT INTO ads (Key, Owner, JobStatus) VALUES ('2.0', 'alice', 1)")
	mustExec(t, e, "INSERT INTO ads (Key, Owner, JobStatus) VALUES ('3.0', 'bob', 1)")

	r := mustExec(t, e, "UPDATE ads SET JobStatus = 2 WHERE Owner = 'alice'")
	if r.Affected != 2 {
		t.Fatalf("UPDATE affected %d, want 2", r.Affected)
	}
	r = mustExec(t, e, "SELECT COUNT(*) FROM ads WHERE JobStatus = 2")
	if r.Rows[0][0] != "2" {
		t.Fatalf("after update COUNT(JobStatus=2) = %s, want 2", r.Rows[0][0])
	}
}

func TestUpdateRejectsKeyAttr(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.0', 'alice')")
	if _, err := e.ExecString("UPDATE ads SET Key = '9.9' WHERE Owner = 'alice'"); err == nil {
		t.Fatal("UPDATE of the key attribute should be rejected")
	}
}

func TestDelete(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	mustExec(t, e, "INSERT INTO ads (Key, JobStatus) VALUES ('1.0', 4)")
	mustExec(t, e, "INSERT INTO ads (Key, JobStatus) VALUES ('2.0', 2)")

	r := mustExec(t, e, "DELETE FROM ads WHERE JobStatus = 4")
	if r.Affected != 1 {
		t.Fatalf("DELETE affected %d, want 1", r.Affected)
	}
	r = mustExec(t, e, "SELECT COUNT(*) FROM ads")
	if r.Rows[0][0] != "1" {
		t.Fatalf("after delete COUNT(*) = %s, want 1", r.Rows[0][0])
	}
}

func TestInsertAutoKey(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	// No Key column -> auto-generated; the row is still addressable by SELECT *.
	r := mustExec(t, e, "INSERT INTO ads (Owner) VALUES ('carol')")
	if r.Affected != 1 || !strings.Contains(r.Note, "key ") {
		t.Fatalf("auto-key INSERT note = %q", r.Note)
	}
	sr := mustExec(t, e, "SELECT * FROM ads")
	if len(sr.Rows) != 1 {
		t.Fatalf("SELECT * rows = %d, want 1", len(sr.Rows))
	}
	if sr.Columns[0] != "Key" {
		t.Fatalf("SELECT * first column = %q, want Key", sr.Columns[0])
	}
}

func TestParseRejectsUnsupported(t *testing.T) {
	cases := []string{
		"SELECT * FROM a JOIN b ON a.x = b.x",
		"SELECT Owner, COUNT(*) FROM ads",           // mix without GROUP BY
		"SELECT Owner FROM ads GROUP BY Owner",      // group by
		"SELECT * FROM ads ORDER BY Cpus",           // order by
		"SELECT * FROM a, b",                        // comma join
		"SELECT * FROM ads WHERE Owner LIKE 'a%'",   // LIKE
		"SELECT *, Owner FROM ads",                  // star + column
		"MERGE INTO ads",                            // unknown verb
	}
	for _, c := range cases {
		if _, err := Parse(c); err == nil {
			t.Errorf("Parse(%q) should have errored", c)
		}
	}
}

func TestParseTranslatesWhere(t *testing.T) {
	st, err := Parse("SELECT * FROM ads WHERE Owner = 'alice' AND Cpus >= 4 OR NOT Held")
	if err != nil {
		t.Fatal(err)
	}
	want := `Owner == "alice" && Cpus >= 4 || ! Held`
	if st.Where != want {
		t.Fatalf("translated WHERE = %q, want %q", st.Where, want)
	}
}
