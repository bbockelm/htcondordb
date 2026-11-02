package repl

import (
	"bytes"
	"fmt"
	"net"
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

// TestWriteArrowReadsAProjectionFromTheAds is why a projection does not go through the
// aggregate branch, even though both would produce a column of the right type.
//
// A projection has ads behind it, so each cell is evaluated and reaches Arrow as a
// classad.Value. The aggregate branch has no ads and must parse the executor's DISPLAY text,
// where "undefined" is how a missing value is spelled -- so an attribute whose VALUE is that
// word comes back as a null. Nothing else in an ordinary fixture tells the two apart, which is
// exactly why this needs stating rather than being left to the branch order.
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

// TestWriteArrowTypesAnAggregate covers the shape with no ads behind it. Its cells come back
// from the executor as display text, so this is the one path that parses -- against the
// DECLARED type, which is what keeps it from being the inference it replaces.
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
// value". It is the cell that tells the two encoding paths apart: read from the ad it is the
// string "undefined", and parsed back from display text it is a null. Everything else in this
// fixture round-trips through text identically, so without it a projection routed through the
// aggregate path would pass every assertion here.
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
