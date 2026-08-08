package repl

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// schemaSession builds a session over a PERSISTENT single-table db with a small segment size, so
// segments actually seal and can carry a columnar block. An in-memory table never gets one, which
// would make every assertion below vacuous.
func schemaSession(t *testing.T, n int) (*session, *db.DB, *bytes.Buffer, func()) {
	t.Helper()
	d, err := db.OpenConfig(db.Config{Dir: t.TempDir(), SegmentSize: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		ad, perr := classad.ParseOld(fmt.Sprintf(
			"ClusterId = %d\nProcId = %d\nOwner = \"u%d\"\nRequestMemory = %d\nWallClock = %d.5",
			i/10, i%10, i%8, ((i%16)+1)*512, i))
		if perr != nil {
			t.Fatal(perr)
		}
		if err := d.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	e := NewExecutor(c, ExecConfig{})
	var out bytes.Buffer
	sess := &session{exec: e, table: dbrpc.DefaultTable}
	return sess, d, &out, func() { c.Close(); s.Close(); d.Close() }
}

// TestSchemaCommandOff is what an operator sees before the accelerator exists: a plain statement
// plus where one comes from, not an error or an empty table.
func TestSchemaCommandOff(t *testing.T) {
	sess, _, out, cleanup := schemaSession(t, 2000)
	defer cleanup()

	sess.schemaCmd(out, "")
	got := out.String()
	if !strings.Contains(got, "off") {
		t.Errorf("output should say the accelerator is off:\n%s", got)
	}
	if !strings.Contains(got, "maintenance") {
		t.Errorf("output should say where a schema comes from:\n%s", got)
	}
}

// TestSchemaCommandShowsFields covers the display: every schema field with its kind, width and
// tier, which is the thing that was previously unreachable.
func TestSchemaCommandShowsFields(t *testing.T) {
	sess, d, out, cleanup := schemaSession(t, 3000)
	defer cleanup()
	if !d.EnableSchemaScan(4000, 4) {
		t.Skip("no sealed segments to sample")
	}

	sess.schemaCmd(out, "")
	got := out.String()
	if !strings.Contains(got, "on —") {
		t.Errorf("should report the accelerator on:\n%s", got)
	}
	for _, attr := range []string{"ProcId", "RequestMemory", "WallClock"} {
		if !strings.Contains(got, attr) {
			t.Errorf("%s missing from the schema display:\n%s", attr, got)
		}
	}
	if !strings.Contains(got, "real") {
		t.Errorf("WallClock is a real; the kind column should say so:\n%s", got)
	}
	if !strings.Contains(got, "HOT") {
		t.Errorf("no hot tier marked:\n%s", got)
	}
}

// TestSchemaFitCommand covers the fit report, including that it renders the JSON the admin action
// returns rather than dumping it raw.
func TestSchemaFitCommand(t *testing.T) {
	sess, d, out, cleanup := schemaSession(t, 3000)
	defer cleanup()
	if !d.EnableSchemaScan(4000, 4) {
		t.Skip("no sealed segments to sample")
	}

	sess.schemaCmd(out, "fit")
	got := out.String()
	if strings.Contains(got, "{\"sampled\"") {
		t.Errorf("raw JSON leaked into the console:\n%s", got)
	}
	if !strings.Contains(got, "sampled record") {
		t.Errorf("no fit header:\n%s", got)
	}
	for _, want := range []string{"escaped", "missing", "unstorable", "ProcId"} {
		if !strings.Contains(got, want) {
			t.Errorf("%q missing from the fit report:\n%s", want, got)
		}
	}
}

// TestSchemaRebuildCommand covers the rebuild, and that it reports coverage afterwards.
func TestSchemaRebuildCommand(t *testing.T) {
	sess, d, out, cleanup := schemaSession(t, 3000)
	defer cleanup()
	if !d.EnableSchemaScan(4000, 4) {
		t.Skip("no sealed segments to sample")
	}

	sess.schemaCmd(out, "rebuild")
	got := out.String()
	if !strings.Contains(got, "schema rebuilt") {
		t.Errorf("rebuild did not report success:\n%s", got)
	}
	if !d.SchemaScanInfo().Enabled {
		t.Error("accelerator disabled after a rebuild through the repl")
	}
	// A bad argument count is refused rather than passed to the server.
	out.Reset()
	sess.schemaCmd(out, "rebuild 100")
	if !strings.Contains(out.String(), "usage:") {
		t.Errorf("a lone numeric argument should show usage:\n%s", out.String())
	}
}

// TestSchemaTableArgument pins the argument peeling: a leading token that is not a number names
// the table, so `.schema fit <table>` and `.schema fit <sampleMax>` both work.
func TestSchemaTableArgument(t *testing.T) {
	sess, _, _, cleanup := schemaSession(t, 100)
	defer cleanup()

	if tbl, rest := sess.tableAndArgs([]string{"history", "500"}); tbl != "history" || len(rest) != 1 || rest[0] != "500" {
		t.Errorf("table+args = %q/%v, want history/[500]", tbl, rest)
	}
	if tbl, rest := sess.tableAndArgs([]string{"500"}); tbl != sess.table || len(rest) != 1 {
		t.Errorf("numeric-only args = %q/%v, want the current table and [500]", tbl, rest)
	}
	if tbl, rest := sess.tableAndArgs(nil); tbl != sess.table || len(rest) != 0 {
		t.Errorf("no args = %q/%v, want the current table and nothing", tbl, rest)
	}
}
