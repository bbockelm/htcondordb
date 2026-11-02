package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
	"github.com/bbockelm/htcondordb/repl"
)

// colTypeFixture is a real store behind a real executor, because what is being tested is a
// derivation that samples records: a stub would answer whatever it was told to.
func colTypeFixture(t *testing.T, n int) (*repl.Executor, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		text := fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nQDate = %d\nCpusUsage = %d.5\n"+
			"Running = true\n", i%4, 1000+i, 1700000000+i, i%3)
		if err := d.UpdateOld(fmt.Sprintf("k%d", i), text); err != nil {
			t.Fatal(err)
		}
	}
	srv := dbrpc.NewServer(d)
	a, b := net.Pipe()
	go func() { _ = srv.ServeConn(dbrpc.NewStreamConn(a)) }()
	cl := dbrpc.NewClient(dbrpc.NewStreamConn(b))
	ex := repl.NewExecutor(cl, repl.ExecConfig{})
	return ex, func() { cl.Close(); srv.Close(); d.Close() }
}

func kindsFor(t *testing.T, ex *repl.Executor, cache *colTypeCache, sql string, headers []string) []string {
	t.Helper()
	st, err := repl.Parse(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return columnKinds(ex, cache, st, headers)
}

// TestColumnKindsNamesWhatAColumnHolds is the point of the whole file: a caller learns a
// column's type from the schema rather than from whichever value happened to arrive first.
func TestColumnKindsNamesWhatAColumnHolds(t *testing.T) {
	ex, done := colTypeFixture(t, 20)
	defer done()
	cache := newColTypeCache()

	got := kindsFor(t, ex, cache,
		"SELECT Owner, ClusterId, CpusUsage, Running FROM "+dbrpc.DefaultTable,
		[]string{"Owner", "ClusterId", "CpusUsage", "Running"})
	want := []string{"string", "int", "real", "bool"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("column %d is %q, want %q", i, got[i], want[i])
		}
	}
}

// TestColumnKindsFollowTheHeadersNotTheTable is the alignment bug this would otherwise have.
// A type landing against the wrong header is worse than no type at all: a caller reads
// description[2] and believes it describes column 2.
func TestColumnKindsFollowTheHeadersNotTheTable(t *testing.T) {
	ex, done := colTypeFixture(t, 20)
	defer done()
	cache := newColTypeCache()

	// Deliberately the reverse of the table's own order, with an aggregate in the middle
	// whose type comes from the SELECT list rather than from any attribute.
	got := kindsFor(t, ex, cache,
		"SELECT Owner, COUNT(*), ClusterId FROM "+dbrpc.DefaultTable+" GROUP BY Owner, ClusterId",
		[]string{"Owner", "COUNT(*)", "ClusterId"})
	want := []string{"string", "int", "int"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// TestColumnKindsReportNothingRatherThanGuessing: every way this can fail has to yield no
// types, so the caller falls back to reading values exactly as it did before. A driver that
// could not report rows because it could not report their types would be a worse driver.
func TestColumnKindsReportNothingRatherThanGuessing(t *testing.T) {
	ex, done := colTypeFixture(t, 20)
	defer done()
	cache := newColTypeCache()

	for _, c := range []struct{ what, sql string }{
		{"a table that does not exist", "SELECT Owner FROM nosuchtable"},
		{"a statement that is not a SELECT", "DELETE FROM " + dbrpc.DefaultTable},
		{"a column the table does not have", "SELECT NoSuchColumn FROM " + dbrpc.DefaultTable},
	} {
		if got := kindsFor(t, ex, cache, c.sql, []string{"x"}); got != nil {
			t.Errorf("%s: reported %v, want no types at all", c.what, got)
		}
	}

	// A nil executor is the embedder with no connection, and must not panic.
	st, _ := repl.Parse("SELECT Owner FROM " + dbrpc.DefaultTable)
	if got := columnKinds(nil, cache, st, []string{"Owner"}); got != nil {
		t.Errorf("a nil executor reported %v", got)
	}
}

// TestColumnKindsAreCachedPerConnection: deriving samples records, and a driver runs many
// statements on one connection. Without the cache every execute() would put a sampling scan
// through the daemon.
func TestColumnKindsAreCachedPerConnection(t *testing.T) {
	ex, done := colTypeFixture(t, 20)
	defer done()
	cache := newColTypeCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	first := kindsFor(t, ex, cache, "SELECT Owner FROM "+dbrpc.DefaultTable, []string{"Owner"})
	if len(first) != 1 || first[0] != "string" {
		t.Fatalf("first derivation gave %v", first)
	}
	if _, ok := cache.get(dbrpc.DefaultTable); !ok {
		t.Fatal("nothing was cached, so every statement re-derives")
	}

	// Close the store out from under it: a second call that still answers can only be
	// reading the cache, which is the assertion. (A live store would answer either way.)
	done()
	if got := kindsFor(t, ex, cache, "SELECT Owner FROM "+dbrpc.DefaultTable, []string{"Owner"}); len(got) != 1 || got[0] != "string" {
		t.Errorf("after the store went away the answer is %v; it was not served from the cache", got)
	}

	// And the entry expires, or a column added to the table would be invisible for the
	// connection's life.
	now = now.Add(colTypeTTL)
	if _, ok := cache.get(dbrpc.DefaultTable); ok {
		t.Error("an expired entry was served")
	}
}

// TestColumnKindsCoverEveryKindArrowtypeNames keeps this mapping and the derivation in step.
// A kind arrowtype can produce that never reaches a caller is a column silently reported as
// untyped, which is the failure this file exists to remove.
func TestColumnKindsCoverEveryKindArrowtypeNames(t *testing.T) {
	for _, kind := range arrowtype.KnownKinds() {
		dt, ok := arrowtype.TypeOfKind(kind)
		if !ok {
			t.Errorf("KnownKinds names %q and TypeOfKind does not know it", kind)
			continue
		}
		back, ok := arrowtype.KindName(dt)
		if !ok || back != kind {
			t.Errorf("kind %q round-trips to %q (ok=%v); a type that cannot be named back "+
				"is a column reported as untyped", kind, back, ok)
		}
	}
}

// TestNoTypesIsNilNotAListOfBlanks pins the distinction the whole JSON contract rests on.
//
// An ABSENT column_types field means "this library does not report types", and a caller
// falls back to reading values. A PRESENT list with an empty string in it means "this column
// has no type", and the caller reports None for it and does not go looking. Returning a list
// of blanks where nothing was derived collapses the two, so an older caller's fallback would
// never fire and every column would silently report no type.
//
// Reached by passing headers the statement's own columns do not cover, which is the shape a
// SELECT * takes when the matched ads carry attributes the declaration has not typed.
func TestNoTypesIsNilNotAListOfBlanks(t *testing.T) {
	ex, done := colTypeFixture(t, 20)
	defer done()
	cache := newColTypeCache()

	got := kindsFor(t, ex, cache, "SELECT Owner FROM "+dbrpc.DefaultTable,
		[]string{"NothingMatchesThis", "NorThis"})
	if got != nil {
		t.Errorf("got %v, want nil: a list of blanks reads as \"every column is untyped\" "+
			"rather than as \"no types were reported\"", got)
	}

	// And the contrast, so this does not pass by never reporting anything: one header that
	// DOES match yields a list, with a blank in the slot that does not.
	got = kindsFor(t, ex, cache, "SELECT Owner FROM "+dbrpc.DefaultTable,
		[]string{"NothingMatchesThis", "Owner"})
	if len(got) != 2 || got[0] != "" || got[1] != "string" {
		t.Errorf("got %v, want [\"\", \"string\"]", got)
	}
}
