package flightsvc

import (
	"context"
	"fmt"
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

// TestRememberedColumnsKeepACachedSchemaAPrefix is the guarantee the whole file is for. A client
// reads an Arrow batch by position, so what must never happen is a column moving. What is
// allowed to happen is a column appearing at the END, which a client that does not know about
// it simply does not read.
//
// The measurement deliberately reorders and adds between the two calls, which is exactly what a
// re-sampled live table does.
func TestRememberedColumnsKeepACachedSchemaAPrefix(t *testing.T) {
	d := newTypeMemory("", nil, nil)

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
		t.Errorf("second derivation gave %v, want %v -- the remembered columns keep their "+
			"positions and the new one is appended", got, want)
	}
}

// TestAStrayMeasurementDoesNotRetypeAColumn: one sample that changes its mind does not get to
// change the column. It takes retypeAgreement successive derivations saying the same new thing.
//
// The premise is asserted first: without the memory, the same input really would publish the new
// type, so this is not passing because the fixture is inert.
func TestAStrayMeasurementDoesNotRetypeAColumn(t *testing.T) {
	asInt := []arrowtype.Decision{decl("ImageSize", arrow.PrimitiveTypes.Int64, false)}
	asText := []arrowtype.Decision{decl("ImageSize", arrow.BinaryTypes.String, true)}

	// Premise: with nothing remembered the measurement publishes utf8.
	bare := newTypeMemory("", nil, nil)
	if got := bare.reconcile("jobs", asText, false)[0].Field.Type.ID(); got != arrow.STRING {
		t.Fatalf("with nothing remembered the measurement gave %v, want utf8; "+
			"the fixture does not pose the question", got)
	}

	d := newTypeMemory("", nil, nil)
	d.reconcile("jobs", asInt, false)
	gen := d.generation("jobs")
	out := d.reconcile("jobs", asText, false)
	if got := out[0].Field.Type.ID(); got != arrow.INT64 {
		t.Errorf("ImageSize republished as %v after ONE disagreeing sample; it takes %d",
			got, retypeAgreement)
	}
	if out[0].Reason != "settled" {
		t.Errorf("the column's reason is %q, want settled -- a reader has to be able to tell "+
			"the type the endpoint settled on from the one this sample measured", out[0].Reason)
	}
	if d.generation("jobs") != gen {
		t.Errorf("the generation moved on a sample that changed nothing: %d -> %d",
			gen, d.generation("jobs"))
	}
}

// TestSuccessiveAgreementRetypesAColumn: a column that has genuinely become a string does move,
// once the evidence is in -- and the move is counted and logged, which is the whole deal. A
// memory that could never retype would be a schema an operator has to maintain by hand.
func TestSuccessiveAgreementRetypesAColumn(t *testing.T) {
	asInt := []arrowtype.Decision{decl("ImageSize", arrow.PrimitiveTypes.Int64, false)}
	asText := []arrowtype.Decision{decl("ImageSize", arrow.BinaryTypes.String, true)}

	d := newTypeMemory("", nil, nil)
	d.reconcile("jobs", asInt, false)
	first := d.generation("jobs")

	for i := 1; i < retypeAgreement; i++ {
		if got := d.reconcile("jobs", asText, false)[0].Field.Type.ID(); got != arrow.INT64 {
			t.Fatalf("retyped after %d of %d derivations", i, retypeAgreement)
		}
		if d.generation("jobs") != first {
			t.Fatalf("the generation moved before the retype was adopted")
		}
	}
	out := d.reconcile("jobs", asText, false)
	if got := out[0].Field.Type.ID(); got != arrow.STRING {
		t.Fatalf("still %v after %d successive derivations measured utf8", got, retypeAgreement)
	}
	if d.generation("jobs") != first+1 {
		t.Errorf("generation is %d, want %d -- an adopted change bumps it exactly once",
			d.generation("jobs"), first+1)
	}
	// And it stays retyped: the evidence is spent, not re-gathered every sample.
	if got := d.reconcile("jobs", asText, false)[0].Field.Type.ID(); got != arrow.STRING {
		t.Errorf("the adopted type did not stick: %v", got)
	}
	if d.generation("jobs") != first+1 {
		t.Errorf("the generation moved again on a sample that agreed with the new type")
	}

	ch := d.changes("jobs")
	last := ch[len(ch)-1]
	if len(last.Retyped) != 1 || last.Retyped[0].Name != "ImageSize" ||
		last.Retyped[0].Before != "int" || last.Retyped[0].After != "string" {
		t.Errorf("the change log says %+v, want one ImageSize int->string entry", last.Retyped)
	}
	if !strings.Contains(last.Reason, "successive derivations") {
		t.Errorf("the reason is %q; it has to say what the evidence was", last.Reason)
	}
}

// TestAnOscillatingColumnNeverRetypes is the case the agreement rule exists for, and the one a
// simple counter would get wrong. A column measured int, string, int, string... never settles on
// either, because a sample that AGREES clears the candidate -- so the count is successive rather
// than cumulative. Counting disagreements instead would retype this column on the third wobble.
func TestAnOscillatingColumnNeverRetypes(t *testing.T) {
	asInt := []arrowtype.Decision{decl("ImageSize", arrow.PrimitiveTypes.Int64, false)}
	asText := []arrowtype.Decision{decl("ImageSize", arrow.BinaryTypes.String, true)}

	d := newTypeMemory("", nil, nil)
	d.reconcile("jobs", asInt, false)
	gen := d.generation("jobs")
	for i := 0; i < retypeAgreement*4; i++ {
		d.reconcile("jobs", asText, false)
		d.reconcile("jobs", asInt, false)
	}
	if got := d.reconcile("jobs", asInt, false)[0].Field.Type.ID(); got != arrow.INT64 {
		t.Errorf("an oscillating column settled on %v; neither type ever had the evidence", got)
	}
	if d.generation("jobs") != gen {
		t.Errorf("the generation moved %d time(s) on a column that never settled",
			d.generation("jobs")-gen)
	}
}

// TestTwoCompetingRetypesAccumulateForNeither: a column the measurement reads as a string one
// sample and a double the next is not two thirds of the way to anything. The candidate is
// replaced rather than merged.
func TestTwoCompetingRetypesAccumulateForNeither(t *testing.T) {
	asInt := []arrowtype.Decision{decl("ImageSize", arrow.PrimitiveTypes.Int64, false)}
	asText := []arrowtype.Decision{decl("ImageSize", arrow.BinaryTypes.String, true)}
	asReal := []arrowtype.Decision{decl("ImageSize", arrow.PrimitiveTypes.Float64, true)}

	d := newTypeMemory("", nil, nil)
	d.reconcile("jobs", asInt, false)
	for i := 0; i < retypeAgreement*2; i++ {
		d.reconcile("jobs", asText, false)
		d.reconcile("jobs", asReal, false)
	}
	if got := d.reconcile("jobs", asInt, false)[0].Field.Type.ID(); got != arrow.INT64 {
		t.Errorf("two competing candidates retyped the column to %v between them", got)
	}
}

// TestTheGenerationRidesInTheSchemaMetadata: the number is only useful if a consumer sees it
// without asking, which means the Arrow schema's own metadata -- the one thing every client
// already reads, and which survives the IPC stream to whatever opens the file at the far end.
func TestTheGenerationRidesInTheSchemaMetadata(t *testing.T) {
	cols := []arrowtype.Decision{decl("Owner", arrow.BinaryTypes.String, true), arrowtype.TailDecision()}
	schema := tableSchemaOf(cols, "jobs", 7)
	md := schema.Metadata()
	if got, ok := md.GetValue("htcondordb.schema_generation"); !ok || got != "7" {
		t.Errorf("schema_generation is %q (present=%v), want 7", got, ok)
	}
	if got, ok := md.GetValue("htcondordb.table"); !ok || got != "jobs" {
		t.Errorf("table is %q (present=%v), want jobs", got, ok)
	}
	// The fingerprint rides alongside and is NOT the generation: it names the content, which
	// is what the pin compares, and two different tables at generation 7 are not the same
	// schema.
	fp, ok := md.GetValue("htcondordb.schema_version")
	if !ok || fp == "" {
		t.Fatalf("no schema_version in the metadata")
	}
	if fp == "7" {
		t.Errorf("schema_version is the generation; it has to name the content")
	}
	if want := arrowtype.Fingerprint(cols); fp != want {
		t.Errorf("schema_version is %q, want the fingerprint %q", fp, want)
	}
	// Changing a column changes it; changing only the generation does not.
	other := []arrowtype.Decision{decl("Owner", arrow.PrimitiveTypes.Int64, true), arrowtype.TailDecision()}
	if got, _ := tableSchemaOf(other, "jobs", 7).Metadata().GetValue("htcondordb.schema_version"); got == fp {
		t.Errorf("a retyped column kept the same schema_version")
	}
	if got, _ := tableSchemaOf(cols, "jobs", 8).Metadata().GetValue("htcondordb.schema_version"); got != fp {
		t.Errorf("the fingerprint moved when only the generation did")
	}
}

// TestAVanishedColumnKeepsItsPlace: an attribute absent from this minute's sample still has
// a column, because dropping it would renumber every column after it -- which is the one thing
// a positional reader cannot survive.
func TestAVanishedColumnKeepsItsPlace(t *testing.T) {
	d := newTypeMemory("", nil, nil)
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

// TestAPrivilegedReadersColumnsAreNotRecorded is the disclosure this file could
// become. The file is on disk and it is shared by every reader of the table, so
// recording what only a DAEMON peer can see would both leak the attribute's NAME to anyone who
// can read the file and publish a column to READ peers that they cannot fill.
func TestAPrivilegedReadersColumnsAreNotRecorded(t *testing.T) {
	d := newTypeMemory("", nil, nil)
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
		t.Errorf("a privileged read recorded %q; the file must name only what an "+
			"unprivileged reader could already list", col.Name)
	}
}

// TestColumnTypesSurviveARestart is the half a cache cannot do. A consumer that cached a
// catalog yesterday is holding column positions; if a daemon restart re-measured from scratch,
// a column could land somewhere else and every cached schema would be wrong at once.
func TestColumnTypesSurviveARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "flight-schema.json")

	d := newTypeMemory(path, nil, nil)
	d.reconcile("jobs", []arrowtype.Decision{
		decl("Owner", arrow.BinaryTypes.String, true),
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
	}, false)
	d.flush()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("nothing was written: %v", err)
	}

	// A new process, the same file, and a measurement that would otherwise reorder.
	again := newTypeMemory(path, nil, nil)
	got := names(again.reconcile("jobs", []arrowtype.Decision{
		decl("ClusterId", arrow.PrimitiveTypes.Int64, false),
		decl("Owner", arrow.BinaryTypes.String, true),
	}, false))
	if want := []string{"Owner", "ClusterId"}; !equalStrings(got, want) {
		t.Errorf("after a restart the order is %v, want %v", got, want)
	}
}

// TestColumnTypeMemoryIsNeverACorrectnessDependency: every way the file can fail has to leave a
// working endpoint, because the column-type memory is an optimization for stability and a daemon
// that would not serve reads without one is a worse daemon.
func TestColumnTypeMemoryIsNeverACorrectnessDependency(t *testing.T) {
	dir := t.TempDir()

	// Unreadable JSON.
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	var warned []string
	d := newTypeMemory(bad, func(m string) { warned = append(warned, m) }, nil)
	if got := names(d.reconcile("jobs", []arrowtype.Decision{decl("Owner", arrow.BinaryTypes.String, true)}, false)); len(got) != 1 {
		t.Errorf("a corrupt file broke the derivation: %v", got)
	}
	if len(warned) == 0 {
		t.Error("a corrupt file reset the schema silently; the symptom shows up far from here")
	}

	// A path that cannot be written (a directory where the file should be).
	blocked := filepath.Join(dir, "blocked.json")
	if err := os.Mkdir(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	warned = nil
	d2 := newTypeMemory(blocked, func(m string) { warned = append(warned, m) }, nil)
	d2.reconcile("jobs", []arrowtype.Decision{decl("Owner", arrow.BinaryTypes.String, true)}, false)
	d2.flush()
	if len(warned) == 0 {
		t.Error("an unwritable file was not reported; an operator would never learn that " +
			"column types are not surviving restarts")
	}
	// And it still serves.
	if got := names(d2.reconcile("jobs", []arrowtype.Decision{decl("Owner", arrow.BinaryTypes.String, true)}, false)); len(got) != 1 {
		t.Errorf("an unwritable file broke the derivation: %v", got)
	}
}

// TestColumnTypeMemoryIsBounded: table names and attribute names both come from the data, and a pool
// that puts a job id in an attribute name would otherwise grow this file without limit -- and
// an unbounded file is read at every start.
func TestColumnTypeMemoryIsBounded(t *testing.T) {
	var warned []string
	d := newTypeMemory("", func(m string) { warned = append(warned, m) }, nil)
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

// TestColumnTypeMemoryReachesTheEndpoint wires the unit above to the thing it is for: two
// derivations over a real server, with the table gaining an attribute in between, must leave
// the original columns where they were.
func TestColumnTypeMemoryReachesTheEndpoint(t *testing.T) {
	ctx := context.Background()
	_, svc, d, done := harnessWithStore(t, 20, arrowtype.Options{})
	defer done()
	svc.types = newTypeMemory("", nil, nil)

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

// TestAnAdoptedRetypeIsAnnounced: a change log only a file holds is not visible -- an operator
// would have to know the file exists and go and read it, having first wondered. The daemon log is
// what they already watch.
//
// And it announces a RETYPE only. A table's first derivation appends every column it has and says
// nothing anyone needs to read; an attribute appearing later and being appended costs a client
// nothing, because the schema it cached is still a prefix. A column changing type is the one that
// can make a cached catalog wrong about a value, so it is the one worth a line -- and a log that
// also announced the other two would be one an operator learns to skip.
func TestAnAdoptedRetypeIsAnnounced(t *testing.T) {
	var notes []string
	d := newTypeMemory("", nil, func(m string) { notes = append(notes, m) })

	asInt := []arrowtype.Decision{decl("ImageSize", arrow.PrimitiveTypes.Int64, false)}
	d.reconcile("jobs", asInt, false)
	if len(notes) != 0 {
		t.Errorf("the first derivation announced %q; it appends every column and tells nobody "+
			"anything", notes)
	}

	// An appended attribute: a generation, but not a line.
	withMore := append(append([]arrowtype.Decision(nil), asInt...),
		decl("Site", arrow.BinaryTypes.String, true))
	d.reconcile("jobs", withMore, false)
	if len(notes) != 0 {
		t.Errorf("appending an attribute announced %q; a cached schema is still a prefix", notes)
	}
	gen := d.generation("jobs")

	asText := append([]arrowtype.Decision{decl("ImageSize", arrow.BinaryTypes.String, true)},
		decl("Site", arrow.BinaryTypes.String, true))
	for range retypeAgreement {
		d.reconcile("jobs", asText, false)
	}
	if len(notes) != 1 {
		t.Fatalf("an adopted retype announced %d line(s), want exactly 1: %q", len(notes), notes)
	}
	if !strings.Contains(notes[0], "ImageSize") {
		t.Errorf("the line does not name the column that changed: %q", notes[0])
	}
	if !strings.Contains(notes[0], "jobs") {
		t.Errorf("the line does not name the table: %q", notes[0])
	}
	if want := fmt.Sprintf("%d", gen+1); !strings.Contains(notes[0], want) {
		t.Errorf("the line does not carry the new generation %s: %q", want, notes[0])
	}

	// And it says it once, not every sample thereafter.
	d.reconcile("jobs", asText, false)
	if len(notes) != 1 {
		t.Errorf("the retype was announced again on a later agreeing sample: %q", notes)
	}
}
