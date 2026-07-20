package repl

import (
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

func TestParseCreateTableMemory(t *testing.T) {
	st, err := Parse("CREATE TABLE ephemeral MEMORY")
	if err != nil {
		t.Fatal(err)
	}
	if st.Kind != StmtCreateTable || st.Table != "ephemeral" || !st.InMemory {
		t.Fatalf("parsed = %+v, want CreateTable ephemeral InMemory", st)
	}
	// Without MEMORY the table is persistent.
	st2, err := Parse("CREATE TABLE persisted")
	if err != nil {
		t.Fatal(err)
	}
	if st2.InMemory {
		t.Fatal("CREATE TABLE without MEMORY should not be in-memory")
	}
}

// newCatalogExec builds an Executor on a DAEMON-privileged connection to a persistent
// catalog server, returning the executor and the catalog (to inspect table backing).
func newPrivCatalogExec(t *testing.T) (*Executor, *db.Catalog, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(dbrpc.DefaultTable); err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	return e, cat, func() { c.Close(); s.Close(); cat.Close() }
}

// TestExecCreateAndConvertMemory drives the two CLI paths end-to-end over dbrpc: CREATE
// TABLE ... MEMORY makes a RAM-only table, and ConvertTableToMemory drops an existing
// persistent table's backing while preserving its data.
func TestExecCreateAndConvertMemory(t *testing.T) {
	e, cat, cleanup := newPrivCatalogExec(t)
	defer cleanup()

	// CREATE TABLE <name> MEMORY -> RAM-only.
	if _, err := e.ExecString("CREATE TABLE ephemeral MEMORY"); err != nil {
		t.Fatalf("CREATE TABLE ... MEMORY: %v", err)
	}
	if d, ok := cat.Table("ephemeral"); !ok || !d.InMemory() {
		t.Fatalf("ephemeral not RAM-only (ok=%v)", ok)
	}

	// A persistent table with data, then converted.
	if _, err := e.ExecString("CREATE TABLE jobs"); err != nil {
		t.Fatal(err)
	}
	if d, _ := cat.Table("jobs"); d.InMemory() {
		t.Fatal("jobs should start persistent")
	}
	if _, err := e.ExecString(`INSERT INTO jobs (Key, Owner) VALUES ('1.0', 'alice')`); err != nil {
		t.Fatal(err)
	}
	if err := e.ConvertTableToMemory("jobs"); err != nil {
		t.Fatalf("ConvertTableToMemory: %v", err)
	}
	if d, _ := cat.Table("jobs"); !d.InMemory() {
		t.Fatal("jobs should be RAM-only after convert")
	}
	// Data preserved through the conversion.
	r, err := e.ExecString("SELECT COUNT(*) FROM jobs")
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0] != "1" {
		t.Fatalf("COUNT(*) after convert = %s, want 1", r.Rows[0][0])
	}
}

// TestConvertMemoryRefusedUnprivileged: over a WRITE-level (non-DAEMON) connection, convert
// is refused while ordinary data writes still work.
func TestConvertMemoryRefusedUnprivileged(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable("jobs"); err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConn(dbrpc.NewStreamConn(sp)) }() // WRITE-level (not privileged)
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	defer func() { c.Close(); s.Close(); cat.Close() }()

	if err := e.ConvertTableToMemory("jobs"); err == nil {
		t.Fatal("convert should be refused on a WRITE-level connection")
	}
	// Data writes still work on the same connection.
	if _, err := e.ExecString(`INSERT INTO jobs (Key, Owner) VALUES ('1.0', 'bob')`); err != nil {
		t.Fatalf("ordinary write should still work: %v", err)
	}
}
