package repl

import (
	"fmt"
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// newPersistentCatalogExec is newCatalogExec over a PERSISTENT catalog, returning the
// catalog so a test can seed ads directly. The difference matters here and nowhere else in
// this suite: only a persistent table can serve wire-form rows, so the in-memory fixture the
// other tests use exercises the text fallback exclusively.
func newPersistentCatalogExec(t *testing.T) (*Executor, *db.Catalog, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return NewExecutor(c, ExecConfig{}), cat, func() { c.Close(); s.Close(); cat.Close() }
}

// seedJobs writes rows carrying every value shape the decode has to reproduce: integers,
// strings, a real, a boolean, a list, a nested record, and an EXPRESSION over a sibling.
// They go in through the catalog rather than SQL so the ad text is exactly what is stored.
func seedJobs(t *testing.T, cat *db.Catalog, table string) {
	t.Helper()
	tbl, err := cat.CreateTable(table)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		held := "false"
		if i%2 == 0 {
			held = "true"
		}
		ad, perr := classad.Parse(fmt.Sprintf(
			`[ Key = "k%d"; ClusterId = %d; Owner = "u%d"; JobStatus = %d; Memory = %d; `+
				`Ratio = %d.5; Held = %s; Args = {1, 2, 3}; Meta = [ a = 1 ]; Doubled = Memory * 2 ]`,
			i, i, i%3, (i%4)+1, (i+1)*1024, i, held))
		if perr != nil {
			t.Fatal(perr)
		}
		if err := tbl.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
}

// rowsOf runs a SELECT and returns its rows.
func rowsOf(t *testing.T, e *Executor, sql string) [][]string {
	t.Helper()
	return mustExec(t, e, sql).Rows
}

// TestWireAndTextAgree is the load-bearing test: every query shape must return exactly the
// same rows over the wire relay as over the old-ClassAd text path. A decode that dropped an
// attribute, mangled a type, or reordered rows would show up here and nowhere else -- the
// wire path answers plausibly wrong rather than failing.
func TestWireAndTextAgree(t *testing.T) {
	e, cat, cleanup := newPersistentCatalogExec(t)
	defer cleanup()
	seedJobs(t, cat, "jobs")

	queries := []string{
		`SELECT * FROM jobs`,
		`SELECT ClusterId, Owner FROM jobs ORDER BY ClusterId`,
		`SELECT Owner, Memory FROM jobs WHERE JobStatus == 1 ORDER BY Memory`,
		`SELECT ClusterId FROM jobs ORDER BY ClusterId LIMIT 3`,
		`SELECT Ratio, Held FROM jobs ORDER BY Ratio`,      // real + boolean
		`SELECT Args, Meta FROM jobs ORDER BY ClusterId`,   // list + nested record
		`SELECT Doubled FROM jobs ORDER BY ClusterId`,      // expression over a sibling
		`SELECT Memory / 1024 AS gb FROM jobs ORDER BY gb`, // expression column
		`SELECT DISTINCT Owner FROM jobs ORDER BY Owner`,
		`SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner ORDER BY Owner`,
	}
	for _, sql := range queries {
		e.wireRowsOff = false
		wireRows := rowsOf(t, e, sql)
		e.wireRowsOff = true
		textRows := rowsOf(t, e, sql)
		e.wireRowsOff = false

		if len(wireRows) != len(textRows) {
			t.Errorf("%s\n  wire returned %d rows, text returned %d", sql, len(wireRows), len(textRows))
			continue
		}
		for i := range textRows {
			if !eqStrs(wireRows[i], textRows[i]) {
				t.Errorf("%s\n  row %d: wire %v, text %v", sql, i, wireRows[i], textRows[i])
			}
		}
	}
}

// TestWireExpressionAttributeFallsBack covers the one semantic difference between the two
// transports. The wire relay projects to exactly the named attributes; the text path asks
// the server to chase references first. Selecting an attribute whose value is an expression
// over a sibling therefore has to notice the sibling is missing and re-run on the text path
// -- otherwise it evaluates to undefined and the query answers wrong without erroring.
func TestWireExpressionAttributeFallsBack(t *testing.T) {
	e, cat, cleanup := newPersistentCatalogExec(t)
	defer cleanup()
	seedJobs(t, cat, "jobs")

	// Doubled = Memory * 2, and Memory is NOT selected.
	r := mustExec(t, e, `SELECT Doubled FROM jobs ORDER BY Doubled LIMIT 1`)
	if len(r.Rows) != 1 || r.Rows[0][0] != "2048" {
		t.Errorf("SELECT Doubled = %v, want 2048 (Memory 1024 * 2), not undefined", r.Rows)
	}
	// Selecting the sibling too keeps it on the fast path and must give the same answer.
	r = mustExec(t, e, `SELECT Memory, Doubled FROM jobs ORDER BY Memory LIMIT 1`)
	if len(r.Rows) != 1 || r.Rows[0][1] != "2048" {
		t.Errorf("SELECT Memory, Doubled = %v, want the second column 2048", r.Rows)
	}
}

// TestWireFallsBackForMemoryTable is the guard on the trap the relay sets: an in-memory
// table cannot produce self-contained rows and serves NO wire rows. If the executor read
// that as "nothing matched" instead of falling back, every query against a RAM table would
// return empty.
func TestWireFallsBackForMemoryTable(t *testing.T) {
	e, _, cleanup := newPersistentCatalogExec(t)
	defer cleanup()

	mustExec(t, e, "CREATE TABLE scratch MEMORY")
	mustExec(t, e, "INSERT INTO scratch (Key, Owner, Memory) VALUES ('k1', 'alice', 2048)")
	mustExec(t, e, "INSERT INTO scratch (Key, Owner, Memory) VALUES ('k2', 'bob', 4096)")

	r := mustExec(t, e, `SELECT Owner FROM scratch ORDER BY Owner`)
	if len(r.Rows) != 2 || r.Rows[0][0] != "alice" || r.Rows[1][0] != "bob" {
		t.Errorf("projected SELECT on a RAM table = %v, want alice and bob", r.Rows)
	}
	r = mustExec(t, e, `SELECT * FROM scratch`)
	if len(r.Rows) != 2 {
		t.Errorf("SELECT * on a RAM table returned %d rows, want 2", len(r.Rows))
	}
}

// TestWireNotUsedInTransaction pins the other place the connection-level relay must stand
// aside: a read inside a transaction has to see the transaction's own writes, and the wire
// op reads the committed store. This asserts the routing decision directly -- whether the
// text path itself honours the transaction is a separate question (and a separate fix).
func TestWireNotUsedInTransaction(t *testing.T) {
	e, cat, cleanup := newPersistentCatalogExec(t)
	defer cleanup()
	seedJobs(t, cat, "jobs")

	if !e.wireEligible("jobs") {
		t.Fatal("a persistent table should be wire-eligible outside a transaction")
	}
	mustExec(t, e, "BEGIN")
	mustExec(t, e, "INSERT INTO jobs (Key, Owner) VALUES ('new', 'zed')")
	if e.wireEligible("jobs") {
		t.Error("a read inside a transaction must not use the connection-level wire relay: " +
			"it reads the committed store and would miss the transaction's own writes")
	}
	mustExec(t, e, "ROLLBACK")
	if !e.wireEligible("jobs") {
		t.Error("after ROLLBACK the table should be wire-eligible again")
	}
}

// TestSelfContained unit-tests the check that decides whether a projected wire row can be
// evaluated on its own.
func TestSelfContained(t *testing.T) {
	for _, tc := range []struct {
		name string
		ad   string
		want bool
	}{
		{"literals only", `[ Owner = "alice"; Memory = 2048 ]`, true},
		{"expression over a present sibling", `[ Memory = 2048; Doubled = Memory * 2 ]`, true},
		{"expression over a missing sibling", `[ Doubled = Memory * 2 ]`, false},
		{"case-insensitive sibling", `[ memory = 2048; Doubled = Memory * 2 ]`, true},
		{"eval() hides its reads", `[ Memory = 2048; X = eval("Mem" + "ory") ]`, false},
		{"empty ad", `[ ]`, true},
	} {
		ad, err := classad.Parse(tc.ad)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := selfContained(ad.AST()); got != tc.want {
			t.Errorf("%s: selfContained = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// BenchmarkReadTransport runs the same queries over both transports on one fixture, which is
// the only honest way to read the difference: same rows, same server, same client, only the
// encoding between them changes.
func BenchmarkReadTransport(b *testing.B) {
	const n = 5000
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	tbl, err := cat.CreateTable("jobs")
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		ad, perr := classad.Parse(fmt.Sprintf(
			`[ Key = "%d"; ClusterId = %d; ProcId = 0; Owner = "u%d"; JobStatus = %d; `+
				`RequestMemory = %d; RequestCpus = %d; QDate = %d; Cmd = "/bin/sleep"; `+
				`Iwd = "/home/u%d"; RemoteHost = "slot1@node%d.example.edu" ]`,
			i, i, i%5, (i%5)+1, ((i%16)+1)*512, (i%8)+1, 1700000000+i, i%5, i%64))
		if perr != nil {
			b.Fatal(perr)
		}
		if err := tbl.Put(fmt.Sprintf("%d", i), ad); err != nil {
			b.Fatal(err)
		}
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	defer func() { c.Close(); s.Close(); cat.Close() }()

	for _, q := range []struct{ name, sql string }{
		{"whole_ad", `SELECT * FROM jobs`},
		{"projected_3", `SELECT ClusterId, Owner, JobStatus FROM jobs`},
		{"projected_1", `SELECT Owner FROM jobs`},
		{"filtered", `SELECT Owner, RequestMemory FROM jobs WHERE JobStatus == 2`},
	} {
		for _, mode := range []struct {
			name string
			off  bool
		}{{"wire", false}, {"text", true}} {
			b.Run(q.name+"/"+mode.name, func(b *testing.B) {
				e.wireRowsOff = mode.off
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := e.ExecString(q.sql); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	e.wireRowsOff = false
}
