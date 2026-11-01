package repl

import (
	"strconv"
	"strings"
	"testing"
)

// seedDialect writes ads whose Owner values differ only in case, which is the whole point: the
// two dialects disagree about exactly these rows.
// dialectRows is the row count of a SELECT, named distinctly from setop.go's rowCount.
func dialectRows(t *testing.T, e *Executor, sql string) int {
	t.Helper()
	return len(mustExec(t, e, sql).Rows)
}

func seedDialect(t *testing.T, e *Executor) {
	t.Helper()
	for _, v := range []struct {
		key, owner string
		cpus       int
	}{
		{"1.0", "alice", 4},
		{"2.0", "ALICE", 8},
		{"3.0", "Alice", 16},
		{"4.0", "bob", 2},
	} {
		mustExec(t, e, "INSERT INTO ads (Key, Owner, Cpus) VALUES ('"+v.key+"', '"+v.owner+"', "+
			strconv.Itoa(v.cpus)+")")
	}
}

// TestDefaultDialectIsClassAd: the default must not change what anyone has already typed.
// ClassAd's `=` folds case, so all three spellings of alice match.
func TestDefaultDialectIsClassAd(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedDialect(t, e)

	if d := e.Dialect(); d != DialectClassAd {
		t.Fatalf("default dialect is %v, want classad", d)
	}
	if n := dialectRows(t, e, "SELECT Key FROM ads WHERE Owner = 'alice'"); n != 3 {
		t.Errorf("classad dialect matched %d rows, want 3 -- ClassAd's = folds case", n)
	}
}

// TestANSIDialectIsCaseSensitive is the divergence the dialect exists to expose. The same text
// means a different query, and the only way to see which you got is to ask.
func TestANSIDialectIsCaseSensitive(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedDialect(t, e)

	e.SetDialect(DialectANSI)
	if n := dialectRows(t, e, "SELECT Key FROM ads WHERE Owner = 'alice'"); n != 1 {
		t.Errorf("ansi dialect matched %d rows, want 1 -- SQL's = does not fold case", n)
	}
	if n := dialectRows(t, e, "SELECT Key FROM ads WHERE Owner = 'ALICE'"); n != 1 {
		t.Errorf("ansi dialect matched %d rows for ALICE, want 1", n)
	}
	// And switching back restores the old meaning, so the setting is a setting and not a
	// one-way door.
	e.SetDialect(DialectClassAd)
	if n := dialectRows(t, e, "SELECT Key FROM ads WHERE Owner = 'alice'"); n != 3 {
		t.Errorf("after switching back, matched %d rows, want 3", n)
	}
}

// TestNumericPredicatesAgreeAcrossDialects: numbers compare the same way in both languages, so
// a dialect switch must not move a numeric result. If it did, the lowering would be doing
// something beyond what it claims.
func TestNumericPredicatesAgreeAcrossDialects(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedDialect(t, e)

	// IN and BETWEEN are deliberately absent: the REPL's own SQL has no such operator, so
	// they are things the ansi dialect ADDS rather than cases where the two agree.
	for _, q := range []string{
		"SELECT Key FROM ads WHERE Cpus > 4",
		"SELECT Key FROM ads WHERE Cpus = 8",
		"SELECT Key FROM ads WHERE Cpus >= 4 AND Cpus <= 8",
	} {
		e.SetDialect(DialectClassAd)
		want := dialectRows(t, e, q)
		e.SetDialect(DialectANSI)
		got := dialectRows(t, e, q)
		if got != want {
			t.Errorf("%q: classad %d rows, ansi %d", q, want, got)
		}
		if want == 0 {
			t.Errorf("%q matched nothing in either dialect; the comparison proves nothing", q)
		}
	}
}

// TestANSIDialectRefusesWhatItCannotLower: the refusals are the feature, and they have to reach
// the user rather than being silently approximated into a ClassAd expression that means
// something else.
func TestANSIDialectRefusesWhatItCannotLower(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedDialect(t, e)
	e.SetDialect(DialectANSI)

	for _, c := range []struct{ q, want string }{
		{"SELECT Key FROM ads WHERE Owner > 'R'", "case-insensitively"},
		{"SELECT Key FROM ads WHERE Owner <> 'alice'", "three-valued"},
		{"SELECT Key FROM ads WHERE NOT Owner = 'alice'", "negated equality"},
		{"SELECT Key FROM ads WHERE NoSuchColumn = 'x'", "unknown column"},
		{"SELECT Key FROM ads WHERE Cpus = 'four'", "non-numeric literal"},
	} {
		_, err := e.ExecString(c.q)
		if err == nil {
			t.Errorf("%q: accepted under ansi, want a refusal", c.q)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q: error %q does not mention %q", c.q, err, c.want)
		}
		// The refusal must say which clause, since a statement has more than one place a
		// predicate can be.
		if !strings.Contains(err.Error(), "WHERE clause") {
			t.Errorf("%q: error %q does not say where the problem is", c.q, err)
		}
	}

	// Each of those works again in the ClassAd dialect, which is what makes the refusal a
	// dialect property rather than a broken query.
	e.SetDialect(DialectClassAd)
	if _, err := e.ExecString("SELECT Key FROM ads WHERE Owner > 'R'"); err != nil {
		t.Errorf("a predicate refused by ansi is also refused by classad: %v", err)
	}
}

// TestANSIDialectRefusesAJoin: a bare column in a joined predicate could belong to either
// table, and lowering it against the first table's types would type it from the wrong one.
func TestANSIDialectRefusesAJoin(t *testing.T) {
	e, cleanup := newCatalogExec(t)
	defer cleanup()
	mustExec(t, e, "CREATE TABLE ads")
	seedDialect(t, e)
	mustExec(t, e, "CREATE TABLE sites")
	mustExec(t, e, "INSERT INTO sites (Key, Owner, Region) VALUES ('s1', 'alice', 'us')")
	e.SetDialect(DialectANSI)

	_, err := e.ExecString("SELECT ads.Key FROM ads JOIN sites ON ads.Owner = sites.Owner WHERE Cpus > 2")
	if err == nil {
		t.Fatal("a JOIN was lowered under the ansi dialect")
	}
	if !strings.Contains(err.Error(), "JOIN") || !strings.Contains(err.Error(), "classad") {
		t.Errorf("error %q should name the construct and the way out", err)
	}
}

// TestDialectLowersBeforeExplain: the translation happens once, before anything executes, so the
// plan EXPLAIN shows is the plan that ran. Lowering at each point of use instead would let the
// two differ, which is the sort of thing that makes a performance report untrustworthy.
func TestDialectLowersBeforeExplain(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()
	seedDialect(t, e)
	e.SetDialect(DialectANSI)

	r, err := e.ExecString("EXPLAIN SELECT Key FROM ads WHERE Owner = 'alice'")
	if err != nil {
		t.Fatalf("EXPLAIN under ansi: %v", err)
	}
	var text strings.Builder
	for _, row := range r.Rows {
		for _, cell := range row {
			text.WriteString(cell)
			text.WriteByte(' ')
		}
	}
	// =?= is what a string equality lowers to, and seeing it in the plan is how we know
	// the plan describes the lowered query rather than the text the user typed.
	if !strings.Contains(text.String(), "=?=") {
		t.Errorf("the EXPLAIN plan does not show the lowered predicate:\n%s", text.String())
	}
}

func TestParseDialect(t *testing.T) {
	for _, c := range []struct {
		in   string
		want Dialect
		ok   bool
	}{
		{"classad", DialectClassAd, true},
		{"", DialectClassAd, true},
		{"ansi", DialectANSI, true},
		{"sql", DialectANSI, true},
		{"ANSI", DialectANSI, true},
		{"  ansi  ", DialectANSI, true},
		{"nonsense", DialectClassAd, false},
	} {
		got, err := ParseDialect(c.in)
		if (err == nil) != c.ok {
			t.Errorf("ParseDialect(%q) err = %v, want ok=%v", c.in, err, c.ok)
			continue
		}
		if err == nil && got != c.want {
			t.Errorf("ParseDialect(%q) = %v, want %v", c.in, got, c.want)
		}
	}
	if DialectClassAd.String() != "classad" || DialectANSI.String() != "ansi" {
		t.Errorf("String() = %q, %q", DialectClassAd, DialectANSI)
	}
}

// TestDialectMetaCommand drives the `.dialect` command the way a user does, since the Executor
// setter and the meta command are different surfaces and only one of them is what anybody types.
func TestDialectMetaCommand(t *testing.T) {
	e, cleanup := newTestExec(t)
	defer cleanup()

	var out strings.Builder
	s := &session{exec: e, base: &out, out: &out, format: FormatTable, table: DefaultTable}

	s.runMeta(&out, ".dialect")
	if !strings.Contains(out.String(), "dialect: classad") {
		t.Errorf("`.dialect` with no argument did not report the current one:\n%s", out.String())
	}

	out.Reset()
	s.runMeta(&out, ".dialect ansi")
	if e.Dialect() != DialectANSI {
		t.Error("`.dialect ansi` did not switch the executor")
	}
	if !strings.Contains(out.String(), "case-sensitive") {
		t.Errorf("switching to ansi did not say what changed:\n%s", out.String())
	}

	out.Reset()
	s.runMeta(&out, ".dialect nonsense")
	if e.Dialect() != DialectANSI {
		t.Error("a bad argument changed the dialect")
	}
	if !strings.Contains(out.String(), "error") {
		t.Errorf("a bad argument was not reported:\n%s", out.String())
	}

	out.Reset()
	s.runMeta(&out, ".dialect classad")
	if e.Dialect() != DialectClassAd {
		t.Error("`.dialect classad` did not switch back")
	}
}

// TestDialectIsInTheHelp: a mode nobody can discover is a mode nobody uses.
func TestDialectIsInTheHelp(t *testing.T) {
	if !strings.Contains(helpText, ".dialect") {
		t.Error(".dialect is missing from .help")
	}
	if !strings.Contains(helpText, "ansi") {
		t.Error("the help does not name the ansi dialect")
	}
}

// TestANSIDialectOnAnArchive: history is an ARCHIVE, not a mutable table, and the two reach the
// store through different APIs — a plain table query reports "no such table" for it. The dialect
// needs a type profile to lower anything, so if profiling did not cover archives the dialect
// would be useless on the one table most queries are about. It does; this is the check.
func TestANSIDialectOnAnArchive(t *testing.T) {
	e, cleanup := newArchiveExec(t)
	defer cleanup()
	e.SetDialect(DialectANSI)

	// A string equality, case-sensitively: the fixture holds Owner = "alice" twice.
	if n := dialectRows(t, e, "SELECT ClusterId FROM history WHERE Owner = 'alice'"); n != 2 {
		t.Errorf("matched %d rows for alice, want 2", n)
	}
	if n := dialectRows(t, e, "SELECT ClusterId FROM history WHERE Owner = 'ALICE'"); n != 0 {
		t.Errorf("matched %d rows for ALICE, want 0 -- SQL's = does not fold case", n)
	}
	// And a numeric one, so the archive's value-indexed attribute is exercised too.
	if n := dialectRows(t, e, "SELECT ClusterId FROM history WHERE ClusterId > 1"); n != 2 {
		t.Errorf("matched %d rows for ClusterId > 1, want 2", n)
	}
	// A refusal must still be a refusal here, not a different error from the archive path.
	if _, err := e.ExecString("SELECT ClusterId FROM history WHERE Owner > 'b'"); err == nil {
		t.Error("string ordering was accepted on an archive")
	} else if !strings.Contains(err.Error(), "case-insensitively") {
		t.Errorf("error %q is not the ansi refusal", err)
	}
}
