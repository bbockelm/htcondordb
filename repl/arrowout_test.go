package repl

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// readArrow reads back what WriteArrow produced, as a consumer would. Asserting on the bytes
// this package wrote using this package's own writer would prove only that it is
// self-consistent; reading with the IPC reader is what says a pyarrow or DuckDB on the other
// end sees a stream at all.
func readArrow(t *testing.T, b []byte) ([]arrow.Record, *arrow.Schema) {
	t.Helper()
	rdr, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("the bytes are not an Arrow IPC stream: %v", err)
	}
	defer rdr.Release()
	var recs []arrow.Record
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		recs = append(recs, rec)
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	return recs, rdr.Schema()
}

// TestWriteArrowTypesAProjection is the point of the format: the cells reach a consumer as
// int64 and utf8 rather than as text for somebody's loader to guess at, which is the guess
// this whole workstream exists to delete.
func TestWriteArrowTypesAProjection(t *testing.T) {
	e, cleanup := arrowExecutor(t, 20)
	defer cleanup()

	st, err := Parse("SELECT Owner, ClusterId FROM " + arrowTestTable + " ORDER BY ClusterId")
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec(st)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.WriteArrow(&buf, st, res); err != nil {
		t.Fatal(err)
	}
	recs, schema := readArrow(t, buf.Bytes())
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	if schema.NumFields() != 2 {
		t.Fatalf("%d fields, want 2", schema.NumFields())
	}
	if got := schema.Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("Owner is %v, want utf8", got)
	}
	if got := schema.Field(1).Type.ID(); got != arrow.INT64 {
		t.Errorf("ClusterId is %v, want int64 -- as text it would be a string column", got)
	}
	if len(recs) == 0 || recs[0].NumRows() != 20 {
		t.Fatalf("got %d batches, %d rows; want 20 rows", len(recs), totalArrowRows(recs))
	}
	// The values survive, in order: an encoder that produced the right types and the
	// wrong cells would pass everything above.
	ids := recs[0].Column(1).(*array.Int64)
	if ids.Value(0) != 1000 || ids.Value(19) != 1019 {
		t.Errorf("ClusterId runs %d..%d, want 1000..1019", ids.Value(0), ids.Value(19))
	}
}

// TestWriteArrowReadsAProjectionFromTheAds: a projection has ads behind it, so each cell is
// evaluated and reaches Arrow as a classad.Value rather than as the text the table prints.
// "undefined" is how a display row spells a missing value, so an attribute whose VALUE is
// that word is the cell that tells an evaluated path from a rendered one -- everything else
// in an ordinary fixture round-trips through text identically.
func TestWriteArrowReadsAProjectionFromTheAds(t *testing.T) {
	e, cleanup := arrowExecutor(t, 4)
	defer cleanup()

	st, err := Parse("SELECT Note FROM " + arrowTestTable + " ORDER BY ClusterId")
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec(st)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.WriteArrow(&buf, st, res); err != nil {
		t.Fatal(err)
	}
	recs, _ := readArrow(t, buf.Bytes())
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	notes := recs[0].Column(0).(*array.String)
	if notes.IsNull(0) {
		t.Fatal(`the first row's Note is null; its value is the STRING "undefined", which ` +
			`means the cells were parsed from display text rather than read from the ads`)
	}
	if got := notes.Value(0); got != "undefined" {
		t.Errorf("Note is %q, want the literal string undefined", got)
	}
	// The premise: the other rows are ordinary strings, so a path that returned
	// everything non-null would not pass by accident.
	if notes.Value(1) != "note-1" {
		t.Errorf("row 1's Note is %q, want note-1", notes.Value(1))
	}
}

// TestWriteArrowTypesAnAggregate covers the shape with no ads behind it: its rows belong to
// groups. The columns are the SELECT's, not the table's -- COUNT is an int64 nothing measured
// and AVG is a float64 whatever it averaged.
func TestWriteArrowTypesAnAggregate(t *testing.T) {
	e, cleanup := arrowExecutor(t, 20)
	defer cleanup()

	st, err := Parse("SELECT Owner, COUNT(*), AVG(Score) FROM " + arrowTestTable + " GROUP BY Owner")
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec(st)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.WriteArrow(&buf, st, res); err != nil {
		t.Fatal(err)
	}
	recs, schema := readArrow(t, buf.Bytes())
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	want := []arrow.Type{arrow.STRING, arrow.INT64, arrow.FLOAT64}
	for i, w := range want {
		if got := schema.Field(i).Type.ID(); got != w {
			t.Errorf("field %d (%s) is %v, want %v", i, schema.Field(i).Name, got, w)
		}
	}
	if totalArrowRows(recs) != 4 {
		t.Fatalf("%d group rows, want 4", totalArrowRows(recs))
	}
	counts := recs[0].Column(1).(*array.Int64)
	for i := 0; i < counts.Len(); i++ {
		if counts.Value(i) != 5 {
			t.Errorf("group %d counted %d, want 5", i, counts.Value(i))
		}
	}
	// The mean keeps its fractional part, which an int64 column could not hold. This is
	// the assertion that fails if an aggregate is typed from its source column.
	avg := recs[0].Column(2).(*array.Float64)
	whole := true
	for i := 0; i < avg.Len(); i++ {
		if avg.Value(i) != float64(int64(avg.Value(i))) {
			whole = false
		}
	}
	if whole {
		t.Error("every AVG came out whole, so this fixture cannot tell a float column from an int one")
	}
}

// TestWriteArrowStarCarriesTheTail: `SELECT *` is the table's schema plus the one column that
// holds what no typed column took. Without the tail, an attribute missing from the file and an
// attribute the table does not have look identical to whoever reads it.
func TestWriteArrowStarCarriesTheTail(t *testing.T) {
	e, cleanup := arrowExecutor(t, 10)
	defer cleanup()

	st, err := Parse("SELECT * FROM " + arrowTestTable)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec(st)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.WriteArrow(&buf, st, res); err != nil {
		t.Fatal(err)
	}
	recs, schema := readArrow(t, buf.Bytes())
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	found := false
	for _, f := range schema.Fields() {
		if f.Name == "_ad" {
			found = true
		}
	}
	if !found {
		t.Error("SELECT * produced no tail column, so an untyped attribute is simply absent")
	}
	if totalArrowRows(recs) != 10 {
		t.Errorf("%d rows, want 10", totalArrowRows(recs))
	}
}

func totalArrowRows(recs []arrow.Record) int {
	n := 0
	for _, r := range recs {
		n += int(r.NumRows())
	}
	return n
}

// noteFor gives one ad a string whose VALUE is the word the executor also uses to spell "no
// value". It is the cell that tells an evaluated encoding path from one that re-reads the
// display text: read from the value it is the string "undefined", and read from the text it
// is a null. Everything else in this fixture round-trips through text identically.
func noteFor(i int) string {
	if i == 0 {
		return "undefined"
	}
	return fmt.Sprintf("note-%d", i)
}

// arrowTestTable is the default table the fixture writes into.
const arrowTestTable = "ads"

// arrowExecutor builds an executor over a real store with n job-shaped ads, four owners deep
// so a GROUP BY has something to group.
func arrowExecutor(t *testing.T, n int) (*Executor, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		// Score is deliberately half-integral. A regular fixture averages to whole
		// numbers -- 20 ads over 4 owners means five consecutive ClusterIds per group
		// -- and a whole mean cannot tell a float64 column from an int64 one, which is
		// exactly what the aggregate test is for. Caught by that test's own guard.
		ad, perr := classad.ParseOld(fmt.Sprintf(
			"Owner = \"user%d\"\nClusterId = %d\nJobStatus = %d\nRequestMemory = 2048\n"+
				"Score = %d.5\nNote = %q\n",
			i%4, 1000+i, 2+i%3, i, noteFor(i)))
		if perr != nil {
			t.Fatal(perr)
		}
		if err := d.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() {
		_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp),
			dbrpc.ServeOptions{Privileged: true, IncludePrivate: true})
	}()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return NewExecutor(c, ExecConfig{}), func() { c.Close(); s.Close(); d.Close() }
}

// arrowOddStrings are the values of the fixture's S column: three strings that spell a
// number, and the three cells a display row writes for something that is not a string at all
// -- "undefined" for a missing value, "error", and an empty cell. Every one of them is a
// STRING here, and a reader that types the column by looking at its text gets all six wrong.
var arrowOddStrings = []string{"100", "101", "102", "undefined", "error", ""}

// arrowOddStringExec builds a table whose S column holds arrowOddStrings, one per ad, so a
// DISTINCT over it returns each exactly once.
func arrowOddStringExec(t *testing.T) (*Executor, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range arrowOddStrings {
		ad, perr := classad.ParseOld(fmt.Sprintf(
			"Owner = \"user%d\"\nClusterId = %d\nS = %q\n", i%2, 1000+i, s))
		if perr != nil {
			t.Fatal(perr)
		}
		if err := d.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() {
		_ = s.ServeConnOpts(dbrpc.NewStreamConn(sp),
			dbrpc.ServeOptions{Privileged: true, IncludePrivate: true})
	}()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	return NewExecutor(c, ExecConfig{}), func() { c.Close(); s.Close(); d.Close() }
}

// arrowRows runs a statement and reads its Arrow stream back.
func arrowRows(t *testing.T, e *Executor, query string) ([]arrow.Record, *arrow.Schema, func()) {
	t.Helper()
	st, err := Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec(st)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.WriteArrow(&buf, st, res); err != nil {
		t.Fatal(err)
	}
	recs, schema := readArrow(t, buf.Bytes())
	return recs, schema, func() {
		for _, r := range recs {
			r.Release()
		}
	}
}

// arrowStringCol reads column c of the records as strings, with a marker for a null, so a
// test can compare a whole column against what the fixture put in.
func arrowStringCol(t *testing.T, recs []arrow.Record, c int) []string {
	t.Helper()
	var out []string
	for _, rec := range recs {
		col, ok := rec.Column(c).(*array.String)
		if !ok {
			t.Fatalf("column %d is %v, want utf8", c, rec.Column(c).DataType())
		}
		for r := 0; r < int(rec.NumRows()); r++ {
			if col.IsNull(r) {
				out = append(out, "<null>")
				continue
			}
			out = append(out, col.Value(r))
		}
	}
	return out
}

// TestWriteArrowDistinctKeepsEveryStringAString is the defect this path had: a DISTINCT has
// no ads behind it, and reading its cells back from the display row loses three of these six
// values outright. "undefined", "error" and an empty cell are how a row SPELLS a missing
// value, a failed one and an empty one, so a string whose value is one of those words came
// back as a null -- not a wrong type, a wrong answer.
func TestWriteArrowDistinctKeepsEveryStringAString(t *testing.T) {
	e, cleanup := arrowOddStringExec(t)
	defer cleanup()

	recs, schema, release := arrowRows(t, e, "SELECT DISTINCT S FROM "+arrowTestTable)
	defer release()

	if got := schema.Field(0).Type.ID(); got != arrow.STRING {
		t.Fatalf("S is %v, want utf8", got)
	}
	got := arrowStringCol(t, recs, 0)
	sort.Strings(got)
	want := append([]string(nil), arrowOddStrings...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DISTINCT S gave %q, want %q", got, want)
	}
}

// TestWriteArrowDistinctKeepsANumericStringAString: the cells of a column whose values all
// spell numbers are indistinguishable from a number once they are text, and the column is
// utf8 because the table says so -- so a path that types a cell from its characters and a
// path that types it from the column's declaration disagree about this exact column. The
// values are the assertion: a quoted "100" that came back as the number 100 would still be
// in a utf8 column here, rendered.
func TestWriteArrowDistinctKeepsANumericStringAString(t *testing.T) {
	e, cleanup := arrowOddStringExec(t)
	defer cleanup()

	recs, schema, release := arrowRows(t, e,
		"SELECT DISTINCT S FROM "+arrowTestTable+" WHERE S == \"100\"")
	defer release()

	if got := schema.Field(0).Type.ID(); got != arrow.STRING {
		t.Fatalf("S is %v, want utf8 -- a numeric-looking string column is still a string column", got)
	}
	if got := arrowStringCol(t, recs, 0); !reflect.DeepEqual(got, []string{"100"}) {
		t.Errorf("DISTINCT S gave %q, want the one string 100", got)
	}
}

// TestWriteArrowRepeatedHeaderKeepsEachCellsOwnValue: `SELECT a AS x, b AS x` names one
// attribute twice, so the row's projected ad holds ONE x -- the later item's value. Reading
// the projection for both columns would put that one value in both, which is a wrong value
// rather than a wrong type, so a shared header is typed from the row's own cells instead.
//
// THE PRICE IS STATED HERE RATHER THAN HIDDEN: those cells are display text, so the three
// strings a row cannot spell apart from a missing value go back to being nulls. A null where
// a string was is a loss; the other column holding this column's value is a lie, and a query
// that names one attribute twice has asked for the one an ad cannot answer.
func TestWriteArrowRepeatedHeaderKeepsEachCellsOwnValue(t *testing.T) {
	e, cleanup := arrowOddStringExec(t)
	defer cleanup()

	recs, _, release := arrowRows(t, e,
		"SELECT Owner AS x, S AS x FROM "+arrowTestTable+" GROUP BY Owner, S")
	defer release()

	owners, esses := arrowStringCol(t, recs, 0), arrowStringCol(t, recs, 1)
	if len(owners) != len(arrowOddStrings) {
		t.Fatalf("%d groups, want %d", len(owners), len(arrowOddStrings))
	}
	for i, o := range owners {
		if !strings.HasPrefix(o, "user") {
			t.Errorf("row %d's first x is %q, want an Owner -- the second item's value "+
				"reached the first item's column", i, o)
		}
	}
	sort.Strings(esses)
	want := []string{"100", "101", "102", "<null>", "<null>", "<null>"}
	if !reflect.DeepEqual(esses, want) {
		t.Errorf("the second x gave %q, want %q -- a shared header has only its text", esses, want)
	}
}

// TestWriteArrowAggregateOutputCrossesAsRenderedText pins what this path still cannot type.
// A group key reaches the result as a value and keeps its kind, but an aggregate's own
// output crosses the wire already RENDERED -- there is no typed counterpart to prefer, so
// COUNT and AVG are read back from their text against the column's declared type. That is
// harmless for the aggregates the server computes, which produce a number or nothing, and it
// is the whole of the remaining gap: an aggregate returning a string that spells a number
// would be retyped, and closing that takes a typed aggregate cell on the wire.
func TestWriteArrowAggregateOutputCrossesAsRenderedText(t *testing.T) {
	e, cleanup := arrowOddStringExec(t)
	defer cleanup()

	recs, schema, release := arrowRows(t, e,
		"SELECT S, COUNT(*) FROM "+arrowTestTable+" GROUP BY S")
	defer release()

	if got := schema.Field(1).Type.ID(); got != arrow.INT64 {
		t.Fatalf("COUNT(*) is %v, want int64", got)
	}
	// The group keys keep their kind, including the three a display row cannot spell.
	got := arrowStringCol(t, recs, 0)
	sort.Strings(got)
	want := append([]string(nil), arrowOddStrings...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the group keys came back as %q, want %q", got, want)
	}
	counts := recs[0].Column(1).(*array.Int64)
	for i := 0; i < counts.Len(); i++ {
		if counts.Value(i) != 1 {
			t.Errorf("group %d counted %d, want 1", i, counts.Value(i))
		}
	}
}
