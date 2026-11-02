package flightsvc

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

func decl(name string, dt arrow.DataType, nullable bool) arrowtype.Decision {
	return arrowtype.Decision{
		Name:    name,
		Verdict: arrowtype.Typed,
		Field:   arrow.Field{Name: name, Type: dt, Nullable: nullable},
		Reason:  "single-class",
	}
}

func names(ds []arrowtype.Decision) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.Name)
	}
	return out
}

// TestDeclarationKeepsACachedSchemaAPrefix is the guarantee the whole file is for. A client
// reads an Arrow batch by position, so what must never happen is a column moving. What is
// allowed to happen is a column appearing at the END, which a client that does not know about
// it simply does not read.
//
// The measurement deliberately reorders and adds between the two calls, which is exactly what a
// re-sampled live table does.
func TestDeclarationKeepsACachedSchemaAPrefix(t *testing.T) {
	d := newDeclarations("", nil)

	first := []arrowtype.Decision{
		decl("Owner", arrow.BinaryTypes.String, true),
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
		arrowtype.TailDecision(),
	}
	got := names(d.reconcile("jobs", first, false))
	if want := []string{"Owner", "ClusterId", "_ad"}; !equalStrings(got, want) {
		t.Fatalf("first derivation gave %v, want %v", got, want)
	}

	// A later sample: the same two attributes in the OTHER order, plus a new one.
	second := []arrowtype.Decision{
		decl("Site", arrow.BinaryTypes.String, true),
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
		decl("Owner", arrow.BinaryTypes.String, true),
		arrowtype.TailDecision(),
	}
	got = names(d.reconcile("jobs", second, false))
	want := []string{"Owner", "ClusterId", "Site", "_ad"}
	if !equalStrings(got, want) {
		t.Errorf("second derivation gave %v, want %v -- the declared columns keep their "+
			"positions and the new one is appended", got, want)
	}
}

// TestDeclarationOutranksARetype: a measurement that changes its mind about a column does not
// get to change the column. The sample moved; the table's contract did not.
//
// The premise is asserted first: without the declaration, the same input really would publish
// the new type, so this is not passing because the fixture is inert.
func TestDeclarationOutranksARetype(t *testing.T) {
	asInt := []arrowtype.Decision{decl("ImageSize", arrow.PrimitiveTypes.Int64, false)}
	asText := []arrowtype.Decision{decl("ImageSize", arrow.BinaryTypes.String, true)}

	// Premise: with no declaration the second measurement publishes utf8.
	bare := newDeclarations("", nil)
	if got := bare.reconcile("jobs", asText, false)[0].Field.Type.ID(); got != arrow.STRING {
		t.Fatalf("without a prior declaration the measurement gave %v, want utf8; "+
			"the fixture does not pose the question", got)
	}

	d := newDeclarations("", nil)
	d.reconcile("jobs", asInt, false)
	out := d.reconcile("jobs", asText, false)
	if got := out[0].Field.Type.ID(); got != arrow.INT64 {
		t.Errorf("ImageSize republished as %v; a declared column keeps its declared type", got)
	}
	if out[0].Reason != "declared" {
		t.Errorf("the column's reason is %q, want declared -- a reader has to be able to tell "+
			"a declared type from a measured one", out[0].Reason)
	}
}

// TestDeclarationKeepsAVanishedColumn: an attribute absent from this minute's sample still has
// a column, because dropping it would renumber every column after it -- which is the one thing
// a positional reader cannot survive.
func TestDeclarationKeepsAVanishedColumn(t *testing.T) {
	d := newDeclarations("", nil)
	d.reconcile("jobs", []arrowtype.Decision{
		decl("Owner", arrow.BinaryTypes.String, true),
		decl("Site", arrow.BinaryTypes.String, true),
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
	}, false)

	// Site is gone from the sample.
	got := names(d.reconcile("jobs", []arrowtype.Decision{
		decl("Owner", arrow.BinaryTypes.String, true),
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
	}, false))
	if want := []string{"Owner", "Site", "ClusterId"}; !equalStrings(got, want) {
		t.Errorf("got %v, want %v -- ClusterId must not move up into Site's position", got, want)
	}
}

// TestDeclarationDoesNotRecordAPrivilegedReadersColumns is the disclosure this file could
// become. The declaration is a file on disk and it is shared by every reader of the table, so
// recording what only a DAEMON peer can see would both leak the attribute's NAME to anyone who
// can read the file and publish a column to READ peers that they cannot fill.
func TestDeclarationDoesNotRecordAPrivilegedReadersColumns(t *testing.T) {
	d := newDeclarations("", nil)
	privileged := []arrowtype.Decision{
		decl("Owner", arrow.BinaryTypes.String, true),
		decl("ClaimId", arrow.BinaryTypes.String, true),
	}
	// A privileged reader is SERVED both columns...
	if got := names(d.reconcile("jobs", privileged, true)); !equalStrings(got, []string{"Owner", "ClaimId"}) {
		t.Fatalf("a privileged reader got %v, want both columns", got)
	}
	// ...and neither is written down, because the two cannot be told apart later.
	for _, col := range d.columns("jobs") {
		t.Errorf("a privileged read declared %q; the declaration must name only what an "+
			"unprivileged reader could already list", col.Name)
	}
}

// TestDeclarationSurvivesARestart is the half a cache cannot do. A consumer that cached a
// catalog yesterday is holding column positions; if a daemon restart re-measured from scratch,
// a column could land somewhere else and every cached schema would be wrong at once.
func TestDeclarationSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "flight-schema.json")

	d := newDeclarations(path, nil)
	d.reconcile("jobs", []arrowtype.Decision{
		decl("Owner", arrow.BinaryTypes.String, true),
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
	}, false)
	d.flush()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("nothing was written: %v", err)
	}

	// A new process, the same file, and a measurement that would otherwise reorder.
	again := newDeclarations(path, nil)
	got := names(again.reconcile("jobs", []arrowtype.Decision{
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
		decl("Owner", arrow.BinaryTypes.String, true),
	}, false))
	if want := []string{"Owner", "ClusterId"}; !equalStrings(got, want) {
		t.Errorf("after a restart the order is %v, want %v", got, want)
	}
}

// TestDeclarationIsNeverACorrectnessDependency: every way the file can fail has to leave a
// working endpoint, because a schema declaration is an optimization for stability and a daemon
// that would not serve reads without one is a worse daemon.
func TestDeclarationIsNeverACorrectnessDependency(t *testing.T) {
	dir := t.TempDir()

	// Unreadable JSON.
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	var warned []string
	d := newDeclarations(bad, func(m string) { warned = append(warned, m) })
	if got := names(d.reconcile("jobs", []arrowtype.Decision{decl("Owner", arrow.BinaryTypes.String, true)}, false)); len(got) != 1 {
		t.Errorf("a corrupt declaration broke the derivation: %v", got)
	}
	if len(warned) == 0 {
		t.Error("a corrupt declaration reset the schema silently; the symptom shows up far from here")
	}

	// A path that cannot be written (a directory where the file should be).
	blocked := filepath.Join(dir, "blocked.json")
	if err := os.Mkdir(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	warned = nil
	d2 := newDeclarations(blocked, func(m string) { warned = append(warned, m) })
	d2.reconcile("jobs", []arrowtype.Decision{decl("Owner", arrow.BinaryTypes.String, true)}, false)
	d2.flush()
	if len(warned) == 0 {
		t.Error("an unwritable declaration was not reported; an operator would never learn that " +
			"column types are not surviving restarts")
	}
	// And it still serves.
	if got := names(d2.reconcile("jobs", []arrowtype.Decision{decl("Owner", arrow.BinaryTypes.String, true)}, false)); len(got) != 1 {
		t.Errorf("an unwritable declaration broke the derivation: %v", got)
	}
}

// TestDeclarationIsBounded: table names and attribute names both come from the data, and a pool
// that puts a job id in an attribute name would otherwise grow this file without limit -- and
// an unbounded file is read at every start.
func TestDeclarationIsBounded(t *testing.T) {
	var warned []string
	d := newDeclarations("", func(m string) { warned = append(warned, m) })
	d.limit = 4

	var measured []arrowtype.Decision
	for _, n := range []string{"a", "b", "c", "d", "e", "f"} {
		measured = append(measured, decl(n, arrow.BinaryTypes.String, true))
	}
	got := d.reconcile("jobs", measured, false)
	// Everything is SERVED -- the cap bounds what is written down, not what a client sees.
	if len(got) != 6 {
		t.Errorf("%d columns served, want all 6: the cap must not hide data", len(got))
	}
	if n := len(d.columns("jobs")); n != 4 {
		t.Errorf("%d columns declared, want the limit of 4", n)
	}
	if len(warned) == 0 {
		t.Error("the limit was reached silently")
	}
}

// TestDeclarationReachesTheEndpoint wires the unit above to the thing it is for: two
// derivations over a real server, with the table gaining an attribute in between, must leave
// the original columns where they were.
func TestDeclarationReachesTheEndpoint(t *testing.T) {
	ctx := context.Background()
	_, svc, d, done := harnessWithStore(t, 20, arrowtype.Options{})
	defer done()
	svc.declared = newDeclarations("", nil)

	before, err := svc.planStatement(ctx, "SELECT * FROM "+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	// A new attribute, inserted with a name that sorts FIRST, which is where an
	// alphabetical re-derivation would put it.
	ad, err := classad.ParseOld("Owner = \"newcomer\"\nClusterId = 99999\nAaaFirst = 7\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put("newcomer", ad); err != nil {
		t.Fatal(err)
	}

	after, err := svc.planStatement(ctx, "SELECT * FROM "+dbrpc.DefaultTable)
	if err != nil {
		t.Fatal(err)
	}
	if after.schema.NumFields() <= before.schema.NumFields() {
		t.Fatalf("the new attribute produced no column (%d -> %d); the fixture proves nothing",
			before.schema.NumFields(), after.schema.NumFields())
	}
	// The premise and the point: every ATTRIBUTE column that existed before is still in the
	// same position.
	//
	// The tail is excluded deliberately, and it is the one exemption from the prefix
	// guarantee. `_ad` is not an attribute -- it is this endpoint's own column -- and it is
	// always last, so appending an attribute moves it. A client that reads it by position
	// has to re-read the schema; one that reads it by name does not. Asserting it here
	// would be asserting the opposite of the design.
	for i := 0; i < before.schema.NumFields(); i++ {
		if before.schema.Field(i).Name == arrowtype.TailColumnName {
			continue
		}
		if got, want := after.schema.Field(i).Name, before.schema.Field(i).Name; got != want {
			t.Errorf("position %d was %q and is now %q", i, want, got)
		}
	}
	// And the tail really is still last, which is what makes reading it by name work.
	if last := after.schema.Field(after.schema.NumFields() - 1).Name; last != arrowtype.TailColumnName {
		t.Errorf("the last column is %q, want the tail", last)
	}
	found := false
	for _, f := range after.schema.Fields() {
		if strings.EqualFold(f.Name, "AaaFirst") {
			found = true
		}
	}
	if !found {
		t.Error("the new attribute is missing entirely; appending is not the same as dropping")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
