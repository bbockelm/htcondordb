package repl

import (
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// newArchiveExec builds an Executor over a catalog holding a mutable table plus a "history"
// archive pre-loaded with completed-job ads -- the schedd-sync history shape.
func newArchiveExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable("jobs"); err != nil {
		t.Fatal(err)
	}
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		ValueAttrs: []string{"ClusterId"},
		ZoneAttrs:  []string{"CompletionDate"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{
		`[ ClusterId = 1; ProcId = 0; Owner = "alice"; JobStatus = 4; CompletionDate = 1700000000 ]`,
		`[ ClusterId = 2; ProcId = 0; Owner = "bob"; JobStatus = 4; CompletionDate = 1700000100 ]`,
		`[ ClusterId = 3; ProcId = 0; Owner = "alice"; JobStatus = 3; CompletionDate = 1700000200 ]`,
	} {
		ad, perr := classad.Parse(text)
		if perr != nil {
			t.Fatal(perr)
		}
		if aerr := arch.Append(ad); aerr != nil {
			t.Fatal(aerr)
		}
	}

	s := dbrpc.NewServerCatalog(cat)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	return e, func() { c.Close(); s.Close(); cat.Close() }
}

// TestSelectFromArchiveTable is the regression for "history isn't queryable": the archive is
// populated but SELECT must route to the archive query path (the regular table query op does
// not resolve append-only tables), so a plain SELECT, a WHERE, and COUNT all work on history.
func TestSelectFromArchiveTable(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()

	if r := mustExec(t, e, "SELECT COUNT(*) FROM history"); r.Rows[0][0] != "3" {
		t.Fatalf("history count = %s, want 3", r.Rows[0][0])
	}
	// WHERE (server-side constraint on the archive).
	if r := mustExec(t, e, `SELECT ClusterId FROM history WHERE Owner == "alice"`); len(r.Rows) != 2 {
		t.Fatalf(`history WHERE Owner=="alice" returned %d rows, want 2`, len(r.Rows))
	}
	// GROUP BY aggregate, computed client-side over the archive rows.
	r := mustExec(t, e, "SELECT Owner, COUNT(*) FROM history GROUP BY Owner ORDER BY Owner")
	if len(r.Rows) != 2 || r.Rows[0][0] != "alice" || r.Rows[0][1] != "2" || r.Rows[1][0] != "bob" || r.Rows[1][1] != "1" {
		t.Fatalf("GROUP BY Owner = %v, want alice=2, bob=1", r.Rows)
	}

	// .tables surfaces the archive so it is discoverable.
	tables, err := e.Tables()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range tables {
		if n == "history" {
			found = true
		}
	}
	if !found {
		t.Errorf(".tables did not include the history archive: %v", tables)
	}
}
