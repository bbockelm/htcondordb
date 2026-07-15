package repl

import (
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

func newCatalogExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	cat, err := db.OpenCatalog("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable("ads"); err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConn(dbrpc.NewStreamConn(sp)) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	return e, func() { c.Close(); s.Close(); cat.Close() }
}

func TestMultiTableDDLAndRouting(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()

	// Create tables and route writes by table.
	mustExec(t, e, "CREATE TABLE machines")
	mustExec(t, e, "CREATE TABLE jobs")
	mustExec(t, e, "INSERT INTO machines (Key, Cpus) VALUES ('slot1', 8)")
	mustExec(t, e, "INSERT INTO jobs (Key, Owner) VALUES ('1.0', 'alice')")

	if r := mustExec(t, e, "SELECT COUNT(*) FROM machines"); r.Rows[0][0] != "1" {
		t.Fatalf("machines count = %s, want 1", r.Rows[0][0])
	}
	if r := mustExec(t, e, "SELECT COUNT(*) FROM jobs"); r.Rows[0][0] != "1" {
		t.Fatalf("jobs count = %s, want 1", r.Rows[0][0])
	}
	// ads (default) is empty; machines data is isolated from it.
	if r := mustExec(t, e, "SELECT COUNT(*) FROM ads"); r.Rows[0][0] != "0" {
		t.Fatalf("ads count = %s, want 0", r.Rows[0][0])
	}

	// A query on a nonexistent table errors.
	if _, err := e.ExecString("SELECT * FROM nope"); err == nil {
		t.Fatal("query on a missing table should error")
	}

	// Tables listing.
	names, err := e.Tables()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 3 || names[0] != "ads" || names[1] != "jobs" || names[2] != "machines" {
		t.Fatalf("Tables() = %v, want [ads jobs machines]", names)
	}

	// CREATE INDEX routes to the named table and is built (Explain shows it).
	mustExec(t, e, "CREATE VALUE INDEX ON machines (Cpus)")
	ex, err := e.Explain("machines", "Cpus >= 4")
	if err != nil {
		t.Fatal(err)
	}
	if len(ex.Probes) != 1 || !ex.Probes[0].Indexed {
		t.Fatalf("machines Explain = %+v, want an indexed probe", ex)
	}
	// The index is on machines only, not jobs.
	exj, _ := e.Explain("jobs", "Cpus >= 4")
	if len(exj.Probes) == 1 && exj.Probes[0].Indexed {
		t.Fatal("jobs should not have a Cpus index")
	}

	// DROP TABLE removes it.
	mustExec(t, e, "DROP TABLE jobs")
	names, _ = e.Tables()
	if len(names) != 2 || names[0] != "ads" || names[1] != "machines" {
		t.Fatalf("after drop Tables() = %v, want [ads machines]", names)
	}
}
