package userfunc

import (
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// A STORED AGGREGATE IS A RECORD IN A TABLE THAT A `GROUP BY` CAN NAME WHERE `SUM` WOULD GO.
//
// Every name below is used by exactly one test. db's aggregate registry does take a withdrawal,
// unlike the scalar one, but it is still process-wide: two tests sharing a name would interfere
// through it whichever order they ran in, and the fact that one of them could clean up afterwards
// is not a reason to find out.

// writeScript puts some script text in a file, for the tests that need the CONFIGURED surface to
// hold a name before the stored one reaches for it.
func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.cas")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// aggSamples is what db.CheckAggregate is given for the mean below: what a row can really carry,
// not only what the aggregate was written for. The awkward values are the point -- a string where
// a number is expected, and an attribute a row does not have at all -- because that is where merge
// and the identity usually part company.
func aggSamples() []classad.Value {
	return []classad.Value{
		classad.NewIntValue(1024),
		classad.NewIntValue(2048),
		classad.NewRealValue(4096.5),
		classad.NewStringValue("lots"),
		classad.NewUndefinedValue(),
	}
}

// meanScript is the aggregate a scalar state cannot carry: the answer needs a sum and a count at
// once, so the state is a two-element list. A row whose value is not a number contributes nothing,
// which is the aggregate's own decision exactly as it is a built-in's -- and it is what makes
// COUNT(*) and the mean's own count disagree, which is what a `GROUP BY` test needs in order to
// tell a real reduction from a per-row call.
func meanScript(name string) string {
	return strings.ReplaceAll(`
def NAME_init():
    return {0, 0}

def NAME_add(s, v):
    if isReal(v) || isInteger(v):
        return {s[0] + v, s[1] + 1}
    return s

def NAME_merge(a, b):
    return {a[0] + b[0], a[1] + b[1]}

def NAME_result(s):
    if s[1] > 0:
        return real(s[0]) / s[1]
    return undefined
`, "NAME", name)
}

// sumScript is a second aggregate over the same values, used where a replacement has to be seen to
// have taken effect: a mean and a sum over the same rows differ, so an assertion on the answer
// cannot be satisfied by the old code.
func sumScript(name string) string {
	return strings.ReplaceAll(`
def NAME_init():
    return 0

def NAME_add(s, v):
    if isReal(v) || isInteger(v):
        return s + v
    return s

def NAME_merge(a, b):
    return a + b

def NAME_result(s):
    return s
`, "NAME", name)
}

// checkLaws runs the registered aggregate against the laws the reduction relies on.
//
// THE LAWS ARE THE AUTHOR'S TO KEEP and nothing in the loader can check them, because the samples
// that would check them are the author's too: values generated here would be integers for an
// aggregate over host names and would prove nothing. So a stored aggregate is tested where its
// source is known, which is here, and the daemon registers what it is given.
func checkLaws(t *testing.T, name string) db.Aggregate {
	t.Helper()
	agg, ok := db.LookupAggregate(name)
	if !ok {
		t.Fatalf("%s is not registered as an aggregate", name)
	}
	if err := db.CheckAggregate(agg, aggSamples()); err != nil {
		t.Fatalf("%s does not keep the laws the reduction relies on: %v", name, err)
	}
	return agg
}

// foldAll reduces some values through one accumulator, for the assertions that do not need a whole
// query to make their point.
func foldAll(agg db.Aggregate, vs ...classad.Value) string {
	acc := agg.NewAccumulator()
	for _, v := range vs {
		acc.Add(v)
	}
	return acc.Result().String()
}

// groupMeans runs the e2e `GROUP BY` and returns each group's row, sorted by the group key.
//
// It inserts two rows per group that the aggregate must not count -- one whose attribute holds a
// string and one that does not carry the attribute at all -- so COUNT(*) and the aggregate
// disagree. That disagreement is what distinguishes a real reduction from the failure this is here
// to catch: a name the parser does not know as an aggregate is read as an ordinary function call
// and evaluated per row, which answers successfully and wrongly.
func groupMeans(t *testing.T, call string) [][]string {
	t.Helper()
	e := newAggExec(t)
	for _, row := range []string{
		"('1.0', 'alice', 100)",
		"('1.1', 'alice', 200)",
		"('2.0', 'bob', 300)",
		"('2.1', 'bob', 400)",
	} {
		mustExec(t, e, "INSERT INTO ads (Key, Owner, Mem) VALUES "+row)
	}
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Mem) VALUES ('2.2', 'bob', 'lots')")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.2', 'alice')")

	r := mustExec(t, e, "SELECT Owner, COUNT(*), "+call+" FROM ads GROUP BY Owner")
	rows := append([][]string(nil), r.Rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
	if len(rows) != 2 {
		t.Fatalf("got %d groups, want 2: %v", len(rows), rows)
	}
	return rows
}

// wantGroups checks the two groups' counts and aggregate values. The counts are three apiece
// because COUNT(*) sees every row in the group, including the two the aggregate skips.
func wantGroups(t *testing.T, rows [][]string, alice, bob float64) {
	t.Helper()
	for i, want := range []struct {
		owner string
		count string
		value float64
	}{{"alice", "3", alice}, {"bob", "3", bob}} {
		got := rows[i]
		if got[0] != want.owner || got[1] != want.count {
			t.Errorf("group %d = %v, want owner %s with %s rows", i, got, want.owner, want.count)
		}
		v, err := strconv.ParseFloat(got[2], 64)
		if err != nil {
			t.Errorf("%s: the aggregate answered %q, which is not a number", want.owner, got[2])
			continue
		}
		if math.Abs(v-want.value) > 1e-9 {
			t.Errorf("%s: aggregate = %v, want %v", want.owner, v, want.value)
		}
	}
}

// TestStoredAggregateAnswersAGroupBy is the whole point: a record in a table becomes an aggregate a
// `GROUP BY` can name, with no configuration anywhere.
//
// It runs a real query rather than folding an accumulator by hand, because the way this fails is
// specific and only a query shows it. A name the parser does not know as an aggregate is read as
// an ordinary per-row function call, which answers successfully and wrongly -- so the rows below
// include two per group that contribute nothing, which makes COUNT(*) and the aggregate's own
// count disagree and a per-row reading impossible to mistake for a reduction.
func TestStoredAggregateAnswersAGroupBy(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.LoadStored(h.cat)
	rec, err := h.mgr.Register(Record{
		Name: "storedAggMean", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggMean"), Arity: -1, MaxArity: -1,
		RegisteredBy: "admin@test",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !strings.HasPrefix(rec.Checksum, "sha256:") || rec.RegisteredAt == 0 {
		t.Errorf("the stored record has no identity or provenance: %+v", rec)
	}
	// An aggregate registration carries no argument count, so the record states none rather than
	// stating zero: zero is a legitimate count for a function and the two have to be tellable
	// apart by anything that reads the table.
	if ad := rec.Ad(); func() bool { _, ok := ad.EvaluateAttrInt(AttrArity); return ok }() {
		t.Error("the record states an argument count for an aggregate")
	}
	if got := h.record("storedAggMean"); got.Role != RoleAggregate || got.State != StateOK {
		t.Errorf("the record does not describe a registered aggregate: %+v", got)
	}

	checkLaws(t, "storedAggMean")
	wantGroups(t, groupMeans(t, "storedAggMean(Mem)"), 150, 350)

	// The listing names the role, because "script" alone would leave an operator unable to tell
	// which of two things in one name space they are looking at.
	st := h.status("storedAggMean")
	if st.Kind != "script aggregate" || !st.Aggregate || st.Origin != "stored" || st.State != StateOK {
		t.Errorf("listing: %+v", st)
	}
	if _, ok := st.Ad().EvaluateAttrInt(AttrArity); ok {
		t.Error("the listing reports an argument count for an aggregate")
	}
}

// TestStoredAggregateRegistersAtStartup: the table is the source of truth, so a daemon registers
// the aggregates it holds when it starts, exactly as it does the functions.
func TestStoredAggregateRegistersAtStartup(t *testing.T) {
	h := newStoredHarness(t)
	h.put(Record{
		Name: "storedAggStartup", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggStartup"), Arity: -1, MaxArity: -1,
	})
	h.mgr.LoadStored(h.cat)

	if st := h.status("storedAggStartup"); st.State != StateOK {
		t.Fatalf("the daemon found it %s: %s", st.State, st.Reason)
	}
	checkLaws(t, "storedAggStartup")
	wantGroups(t, groupMeans(t, "storedAggStartup(Mem)"), 150, 350)
}

// TestStoredAggregateIsCorrectedInPlace is the claim the aggregate registry makes good on: a
// stored aggregate can be replaced while the daemon runs, and -- unlike a stored function -- the
// replacement is not pinned to an argument count, because an aggregate registration has none.
func TestStoredAggregateIsCorrectedInPlace(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.LoadStored(h.cat)
	first, err := h.mgr.Register(Record{
		Name: "storedAggCorrectMe", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggCorrectMe"), Arity: -1, MaxArity: -1,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := foldAll(checkLaws(t, "storedAggCorrectMe"),
		classad.NewIntValue(10), classad.NewIntValue(20)); got != "15" {
		t.Fatalf("before the correction: %s, want the mean 15", got)
	}

	after, err := h.mgr.Replace(Record{
		Name: "storedAggCorrectMe", Kind: KindScript, Role: RoleAggregate,
		Source: sumScript("storedAggCorrectMe"), Arity: -1, MaxArity: -1,
		RegisteredBy: "admin@test",
	})
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	// THE NEW CODE REDUCES, which is the claim worth making: the name was registered before the
	// replacement and now resolves to the new implementation.
	if got := foldAll(checkLaws(t, "storedAggCorrectMe"),
		classad.NewIntValue(10), classad.NewIntValue(20)); got != "30" {
		t.Errorf("after the correction: %s, want the sum 30", got)
	}
	if after.Checksum == first.Checksum {
		t.Error("the instance identity did not change, so a result cannot be traced to the code that produced it")
	}
	if rec := h.record("storedAggCorrectMe"); rec.Checksum != after.Checksum || !strings.Contains(rec.Source, "s + v") {
		t.Errorf("the record still describes the old code: %+v", rec)
	}
	if st := h.status("storedAggCorrectMe"); st.Checksum != after.Checksum {
		t.Errorf("the listing still names %s, want %s", st.Checksum, after.Checksum)
	}
	// And it reduces a real query, not only a hand-folded accumulator: 100+200 and 300+400.
	wantGroups(t, groupMeans(t, "storedAggCorrectMe(Mem)"), 300, 700)
}

// TestStoredAggregateRoleCannotBeReplaced: a replacement points a name at new source. A name that
// is an aggregate and becomes a scalar function is a different thing, in a different registry,
// answering a differently parsed call -- so it is a new name, not a replacement.
func TestStoredAggregateRoleCannotBeReplaced(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.LoadStored(h.cat)
	if _, err := h.mgr.Register(Record{
		Name: "storedAggFixedRole", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggFixedRole"), Arity: -1, MaxArity: -1,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := h.mgr.Replace(Record{
		Name: "storedAggFixedRole", Kind: KindScript, Role: RoleFunction, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedAggFixedRole", "(n):\n    return n\n"),
	})
	if err == nil {
		t.Fatal("a replacement that changes a name's role was accepted")
	}
	for _, want := range []string{"aggregate", "function", "Remove it"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	// A replacement that names no role keeps the one the name has, since the thing it says
	// nothing about is the thing it is not changing.
	if _, err := h.mgr.Replace(Record{
		Name: "storedAggFixedRole", Kind: KindScript,
		Source: sumScript("storedAggFixedRole"), Arity: -1, MaxArity: -1,
	}); err != nil {
		t.Errorf("a replacement that names no role: %v", err)
	}
	if got := foldAll(checkLaws(t, "storedAggFixedRole"),
		classad.NewIntValue(10), classad.NewIntValue(20)); got != "30" {
		t.Errorf("after the replacement: %s, want the sum 30", got)
	}
}

// TestStoredBadAggregateIsNotRegisteredAndSaysSo is the deliberate difference from a bad stored
// FUNCTION, which stays registered answering ERROR because leaving the name out would change how a
// call site compiles.
//
// Nothing captures an aggregate name, so there is no shape to keep -- and the two outcomes are not
// equally good. A registered aggregate that errors gives a query that SUCCEEDS with a column of
// ERROR in it, which looks like an answer. An unregistered one fails the query, naming the
// aggregate, which is the same answer db gives for a name nobody ever registered.
func TestStoredBadAggregateIsNotRegisteredAndSaysSo(t *testing.T) {
	h := newStoredHarness(t)
	// Three of the four roles: `merge` is the one the reduction cannot do without, and a stored
	// record that omits a `def` is the ordinary way an aggregate arrives broken.
	broken := strings.Replace(meanScript("storedAggBad"), "def storedAggBad_merge(a, b):",
		"def storedAggBad_notMerge(a, b):", 1)
	h.put(Record{
		Name: "storedAggBad", Kind: KindScript, Role: RoleAggregate,
		Source: broken, Arity: -1, MaxArity: -1,
	})
	// Loading does not panic, does not return, and does not stop anything: that IS the daemon
	// still starting.
	h.mgr.LoadStored(h.cat)

	// 1. The name is NOT registered, so a query naming it fails rather than answering a column.
	if _, ok := db.LookupAggregate("storedAggBad"); ok {
		t.Error("a bad aggregate was registered, so a query naming it answers a column of ERROR")
	}
	// 2. In the log, at start-up, and the message says which `def` it went looking for.
	logs := h.logs.String()
	if !strings.Contains(logs, "BAD") || !strings.Contains(logs, "storedAggBad") {
		t.Errorf("the start-up log does not say the aggregate is bad:\n%s", logs)
	}
	if !strings.Contains(logs, "storedAggBad_merge") {
		t.Errorf("the log does not name the definition that is missing:\n%s", logs)
	}
	// 3. In the record.
	rec := h.record("storedAggBad")
	if rec.State != StateBad || rec.StateReason == "" || rec.StateAt == 0 {
		t.Errorf("the record does not carry the bad state: %+v", rec)
	}
	// 4. In the listing.
	st := h.status("storedAggBad")
	if st.State != StateBad || st.Reason == "" || st.Kind != "script aggregate" {
		t.Errorf("the listing does not report the bad state: %+v", st)
	}
	// And the name is still HELD, by this manager's own bookkeeping: nothing registered it, and
	// nothing else may take it either.
	if _, err := h.mgr.Register(Record{
		Name: "storedAggBad", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedAggBad", "(n):\n    return n\n"),
	}); err == nil || !strings.Contains(err.Error(), "already a stored user function") {
		t.Errorf("a bad aggregate's name was available to something else: %v", err)
	}
}

// TestStoredBadAggregateIsRepairedByReplacement: the repair path for a name db does not hold is
// the same call that corrects a working one, because a replacement registers an absent name rather
// than requiring a present one.
func TestStoredBadAggregateIsRepairedByReplacement(t *testing.T) {
	h := newStoredHarness(t)
	h.put(Record{
		Name: "storedAggRepairMe", Kind: KindScript, Role: RoleAggregate,
		Source: "def storedAggRepairMe_init(:\n    return", Arity: -1, MaxArity: -1,
	})
	h.mgr.LoadStored(h.cat)
	if st := h.status("storedAggRepairMe"); st.State != StateBad {
		t.Fatalf("state is %q, want %q", st.State, StateBad)
	}
	if _, ok := db.LookupAggregate("storedAggRepairMe"); ok {
		t.Fatal("a bad aggregate was registered")
	}

	if _, err := h.mgr.Replace(Record{
		Name: "storedAggRepairMe", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggRepairMe"), Arity: -1, MaxArity: -1,
	}); err != nil {
		t.Fatalf("repairing a bad aggregate: %v", err)
	}
	if got := foldAll(checkLaws(t, "storedAggRepairMe"),
		classad.NewIntValue(10), classad.NewIntValue(20)); got != "15" {
		t.Errorf("after the repair: %s, want the mean 15", got)
	}
	if st := h.status("storedAggRepairMe"); st.State != StateOK || st.Reason != "" {
		t.Errorf("the listing still reports it bad: %+v", st)
	}
	if rec := h.record("storedAggRepairMe"); rec.State != StateOK {
		t.Errorf("the record still reports it bad: %+v", rec)
	}
}

// TestStoredAggregateRemovalGivesTheNameBack is where a stored aggregate and a stored function
// part company for good. A scalar registration cannot be withdrawn, so a removed function's name
// stays claimed and the listing keeps a tombstone saying so. An aggregate registration CAN be
// withdrawn, so removal withdraws it: a query naming it afterwards fails, there is nothing left to
// report, and the name is immediately free -- for an aggregate or for anything else.
func TestStoredAggregateRemovalGivesTheNameBack(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.LoadStored(h.cat)
	if _, err := h.mgr.Register(Record{
		Name: "storedAggTransient", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggTransient"), Arity: -1, MaxArity: -1,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	checkLaws(t, "storedAggTransient")

	note, err := h.mgr.Remove("storedAggTransient")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !strings.Contains(note, "free again with no restart") {
		t.Errorf("the note does not say the name is free: %s", note)
	}
	if _, ok := db.LookupAggregate("storedAggTransient"); ok {
		t.Error("a removed aggregate is still registered")
	}
	if _, ok := h.tbl.LookupClassAd("storedaggtransient"); ok {
		t.Error("the record survived removal")
	}
	for _, st := range h.mgr.List() {
		if strings.EqualFold(st.Name, "storedAggTransient") {
			t.Errorf("the listing keeps a removed aggregate, whose name is free: %+v", st)
		}
	}
	// The name really is free: registering it again works, with no restart.
	if _, err := h.mgr.Register(Record{
		Name: "storedAggTransient", Kind: KindScript, Role: RoleAggregate,
		Source: sumScript("storedAggTransient"), Arity: -1, MaxArity: -1,
	}); err != nil {
		t.Fatalf("re-registering a removed aggregate's name: %v", err)
	}
	if got := foldAll(checkLaws(t, "storedAggTransient"),
		classad.NewIntValue(10), classad.NewIntValue(20)); got != "30" {
		t.Errorf("the re-registered aggregate answered %s, want the sum 30", got)
	}
}

// TestStoredAggregateSharesOneNameSpace. An aggregate goes into a different registry from a scalar
// function and shares the name space anyway, because a client's parser has to decide whether
// p95(x) is an aggregate call or an ordinary one before any request exists. So the rule is enforced
// across both surfaces at once, from each of the four directions it can be approached from.
func TestStoredAggregateSharesOneNameSpace(t *testing.T) {
	t.Run("a stored aggregate cannot take a stored function's name", func(t *testing.T) {
		h := newStoredHarness(t)
		h.mgr.reg = newFakeRegistry()
		h.mgr.LoadStored(h.cat)
		if _, err := h.mgr.Register(Record{
			Name: "storedAggClashA", Kind: KindScript, Arity: 1, MaxArity: 1,
			Source: scriptSource("storedAggClashA", "(n):\n    return n\n"),
		}); err != nil {
			t.Fatalf("register the function: %v", err)
		}
		_, err := h.mgr.Register(Record{
			Name: "storedAggClashA", Kind: KindScript, Role: RoleAggregate,
			Source: meanScript("storedAggClashA"), Arity: -1, MaxArity: -1,
		})
		if err == nil || !strings.Contains(err.Error(), "already a stored user function") {
			t.Errorf("an aggregate took a stored function's name: %v", err)
		}
		if _, ok := db.LookupAggregate("storedAggClashA"); ok {
			t.Error("the aggregate was registered anyway")
		}
	})

	t.Run("a stored function cannot take a stored aggregate's name", func(t *testing.T) {
		h := newStoredHarness(t)
		h.mgr.reg = newFakeRegistry()
		h.mgr.LoadStored(h.cat)
		if _, err := h.mgr.Register(Record{
			Name: "storedAggClashB", Kind: KindScript, Role: RoleAggregate,
			Source: meanScript("storedAggClashB"), Arity: -1, MaxArity: -1,
		}); err != nil {
			t.Fatalf("register the aggregate: %v", err)
		}
		_, err := h.mgr.Register(Record{
			Name: "storedAggClashB", Kind: KindScript, Arity: 1, MaxArity: 1,
			Source: scriptSource("storedAggClashB", "(n):\n    return n\n"),
		})
		if err == nil || !strings.Contains(err.Error(), "already a stored user function") {
			t.Errorf("a function took a stored aggregate's name: %v", err)
		}
	})

	t.Run("a stored aggregate cannot take a configured name", func(t *testing.T) {
		h := newStoredHarness(t)
		path := writeScript(t, scriptSource("storedAggVsConfigured", "(n):\n    return n + 1\n"))
		if err := h.mgr.Apply(wasmCfg(t, "HTCONDORDB_SCRIPT_FUNCTIONS = storedAggVsConfigured\n"+
			"HTCONDORDB_SCRIPT_STOREDAGGVSCONFIGURED_PATH = "+path+"\n")); err != nil {
			t.Fatalf("apply: %v", err)
		}
		h.mgr.LoadStored(h.cat)
		_, err := h.mgr.Register(Record{
			Name: "storedAggVsConfigured", Kind: KindScript, Role: RoleAggregate,
			Source: meanScript("storedAggVsConfigured"), Arity: -1, MaxArity: -1,
		})
		if err == nil || !strings.Contains(err.Error(), "configured") {
			t.Errorf("an aggregate took a configured function's name: %v", err)
		}
	})

	t.Run("a configured function cannot take a stored aggregate's name", func(t *testing.T) {
		h := newStoredHarness(t)
		h.mgr.LoadStored(h.cat)
		if _, err := h.mgr.Register(Record{
			Name: "storedAggFirstClaim", Kind: KindScript, Role: RoleAggregate,
			Source: meanScript("storedAggFirstClaim"), Arity: -1, MaxArity: -1,
		}); err != nil {
			t.Fatalf("register the aggregate: %v", err)
		}
		path := writeScript(t, scriptSource("storedAggFirstClaim", "(n):\n    return n\n"))
		err := h.mgr.Apply(wasmCfg(t, "HTCONDORDB_SCRIPT_FUNCTIONS = storedAggFirstClaim\n"+
			"HTCONDORDB_SCRIPT_STOREDAGGFIRSTCLAIM_PATH = "+path+"\n"))
		if err == nil || !strings.Contains(err.Error(), "stored") {
			t.Errorf("a configured function took a stored aggregate's name: %v", err)
		}
	})

	t.Run("a built-in aggregate may not be shadowed", func(t *testing.T) {
		h := newStoredHarness(t)
		h.mgr.LoadStored(h.cat)
		// A registration under one of these succeeds in db's registry and is then unreachable,
		// because a query's parser reads the name as a built-in aggregate before it consults any
		// registry. All five are refused; what each message says depends on which rule catches it,
		// and COUNT is the one that NEEDS the aggregate rule, since the other four are also
		// built-in scalar functions and are refused as those.
		for _, name := range []string{"sum", "COUNT", "Avg", "min", "MAX"} {
			_, err := h.mgr.Register(Record{
				Name: name, Kind: KindScript, Role: RoleAggregate,
				Source: meanScript(name), Arity: -1, MaxArity: -1,
			})
			if err == nil || !strings.Contains(err.Error(), "may not shadow") {
				t.Errorf("registering %q as an aggregate: %v", name, err)
			}
			if _, ok := db.LookupAggregate(name); ok {
				t.Errorf("%q was registered as an aggregate and is unreachable", name)
			}
		}
		// COUNT is not a built-in scalar function, so nothing but the aggregate rule refuses it.
		_, err := h.mgr.Register(Record{
			Name: "count", Kind: KindScript, Role: RoleAggregate,
			Source: meanScript("count"), Arity: -1, MaxArity: -1,
		})
		if err == nil || !strings.Contains(err.Error(), "built-in aggregate") {
			t.Errorf("registering count as an aggregate: %v", err)
		}
	})
}

// TestStoredAggregateDefNamesFollowTheName pins the rule the configured surface already uses: the
// name picks the four `def`s out of the source by prefix, and a source that spells them otherwise
// names each one for the role it fills. One rule for both surfaces is what lets a file written for
// a config file be registered out of the table unchanged.
func TestStoredAggregateDefNamesFollowTheName(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.LoadStored(h.cat)

	// The default: nothing is named, so the prefix rule spells all four.
	rec := Record{Name: "storedAggPrefix", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggPrefix"), Arity: -1, MaxArity: -1}
	want := aggDefs{Init: "storedAggPrefix_init", Add: "storedAggPrefix_add",
		Merge: "storedAggPrefix_merge", Result: "storedAggPrefix_result"}
	if got := rec.defs(); got != want {
		t.Errorf("defs = %+v, want %+v", got, want)
	}
	if _, err := h.mgr.Register(rec); err != nil {
		t.Fatalf("register: %v", err)
	}
	checkLaws(t, "storedAggPrefix")

	// The override: a source whose definitions fit no prefix, with each `def` named for the role
	// it fills. The record carries only what it names, so the prefix rule still answers the rest.
	named := Record{
		Name: "storedAggNamed", Kind: KindScript, Role: RoleAggregate, Arity: -1, MaxArity: -1,
		Defs: aggDefs{Init: "mkZero", Add: "foldIn", Merge: "joinIn", Result: "takeOut"},
		Source: `
def mkZero():
    return {0, 0}

def foldIn(s, v):
    if isReal(v) || isInteger(v):
        return {s[0] + v, s[1] + 1}
    return s

def joinIn(a, b):
    return {a[0] + b[0], a[1] + b[1]}

def takeOut(s):
    if s[1] > 0:
        return real(s[0]) / s[1]
    return undefined
`,
	}
	if _, err := h.mgr.Register(named); err != nil {
		t.Fatalf("register with named defs: %v", err)
	}
	checkLaws(t, "storedAggNamed")
	// The names reached the record, so a restart builds the same aggregate out of the same source.
	if got := h.record("storedAggNamed").Defs; got != named.Defs {
		t.Errorf("the record does not carry the named defs: %+v", got)
	}
	wantGroups(t, groupMeans(t, "storedAggNamed(Mem)"), 150, 350)
}

// TestStoredWasmAggregateReducesAGroupBy: the other kind of source in the aggregate role, loaded
// under a key carrying its checksum so that a replacement can load a second module under the same
// query-visible name.
func TestStoredWasmAggregateReducesAGroupBy(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.LoadStored(h.cat)
	if _, err := h.mgr.Register(Record{
		Name: "storedAggWasmMean", Kind: KindWasm, Role: RoleAggregate,
		Source: base64.StdEncoding.EncodeToString(fixture(t, "mean.wasm")),
		Arity:  -1, MaxArity: -1,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	checkLaws(t, "storedAggWasmMean")
	wantGroups(t, groupMeans(t, "storedAggWasmMean(Mem)"), 150, 350)

	// The module behind it is INSPECTABLE, and it is untrusted: trust is configuration and a
	// stored module is exactly a module that can ask for things.
	impl := h.mgr.stored["storedaggwasmmean"].impl
	if impl == nil || impl.udaf == nil {
		t.Fatal("no module behind the registered aggregate")
	}
	if impl.udaf.Trusted() {
		t.Error("a stored module was loaded trusted")
	}
	if st := h.status("storedAggWasmMean"); st.Kind != "wasm aggregate" {
		t.Errorf("the listing says %q, want %q", st.Kind, "wasm aggregate")
	}
}

// TestStoredRecordWithNoRoleIsAFunction: a record written before the role existed states none, so
// absent has to mean a scalar function. Anything else would change what every such record
// registers.
func TestStoredRecordWithNoRoleIsAFunction(t *testing.T) {
	h := newStoredHarness(t)
	ad := classad.New()
	ad.InsertAttrString(AttrName, "storedAggNoRole")
	ad.InsertAttrString(AttrKind, KindScript)
	ad.InsertAttrString(AttrSource, scriptSource("storedAggNoRole", "(n):\n    return n + 1\n"))
	ad.InsertAttr(AttrArity, int64(1))
	ad.InsertAttr(AttrMaxArity, int64(1))
	tx := h.tbl.Begin()
	tx.NewClassAd("storedaggnorole", ad)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	h.mgr.LoadStored(h.cat)

	if st := h.status("storedAggNoRole"); st.State != StateOK || st.Aggregate || st.Kind != KindScript {
		t.Errorf("a record stating no role did not register as a function: %+v", st)
	}
	if got := evalExpr(t, "storedAggNoRole(10)"); got != "11" {
		t.Errorf("the function answered %s, want 11", got)
	}
	if _, ok := db.LookupAggregate("storedAggNoRole"); ok {
		t.Error("a record stating no role was registered as an aggregate")
	}
}

// TestStoredAggregateKindAndRoleAreSeparate: "aggregate" in the Kind field is the mistake an
// administrator makes by reasoning from the configured surface, where an aggregate is a list of
// its own. The refusal names both fields, because the fix is to fill in a second one rather than
// to correct the first.
func TestStoredAggregateKindAndRoleAreSeparate(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.LoadStored(h.cat)
	_, err := h.mgr.Register(Record{
		Name: "storedAggWrongField", Kind: RoleAggregate,
		Source: meanScript("storedAggWrongField"), Arity: -1, MaxArity: -1,
	})
	if err == nil {
		t.Fatal("a record naming a role in its kind was accepted")
	}
	for _, want := range []string{"role", AttrKind, AttrRole, KindScript, KindWasm} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	// And a role that is neither is refused for what it is, rather than being read as a function.
	if _, err := h.mgr.Register(Record{
		Name: "storedAggWrongRole", Kind: KindScript, Role: "window",
		Source: meanScript("storedAggWrongRole"), Arity: -1, MaxArity: -1,
	}); err == nil || !strings.Contains(err.Error(), "not a role") {
		t.Errorf("registering an unknown role: %v", err)
	}
}

// fakeAggRegistry is an aggregate registry a test owns, so a name can be registered again in a
// later "daemon".
//
// db's own registry takes a withdrawal, unlike the scalar one, so a test COULD clean up after
// itself there -- but it is still process-wide, and a restart means two managers alive at once
// registering one name, which the real registry refuses from the second. A registry per manager is
// what makes two daemons in one process mean what it says.
type fakeAggRegistry struct {
	mu   sync.Mutex
	aggs map[string]db.Aggregate
}

func newFakeAggRegistry() *fakeAggRegistry {
	return &fakeAggRegistry{aggs: map[string]db.Aggregate{}}
}

func (r *fakeAggRegistry) RegisterAggregate(name string, agg db.Aggregate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.aggs[strings.ToLower(name)]; dup {
		return fmt.Errorf("already registered: %s", name)
	}
	r.aggs[strings.ToLower(name)] = agg
	return nil
}

func (r *fakeAggRegistry) ReplaceAggregate(name string, agg db.Aggregate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.aggs[strings.ToLower(name)] = agg
	return nil
}

func (r *fakeAggRegistry) UnregisterAggregate(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	folded := strings.ToLower(name)
	_, ok := r.aggs[folded]
	delete(r.aggs, folded)
	return ok
}

func (r *fakeAggRegistry) lookup(name string) (db.Aggregate, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.aggs[strings.ToLower(name)]
	return a, ok
}

// TestStoredAggregateBecomesBadAcrossARestart: an aggregate that was fine, and a daemon that comes
// back to find it is not. Nothing about the record changed except its source, which is what a
// damaged record or a runtime that no longer accepts a module looks like from here.
//
// The second daemon leaves the name OUT of its registry, which is the whole of the difference from
// the scalar case: there the restart has to re-register the name under its old argument count,
// because a call site's shape cannot be allowed to change. Here there is no shape, so the name is
// simply absent and a query naming it fails.
func TestStoredAggregateBecomesBadAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	h1 := openStoredHarness(t, dir)
	reg1 := newFakeAggRegistry()
	h1.mgr.aggReg = reg1
	h1.put(Record{
		Name: "storedAggDecays", Kind: KindScript, Role: RoleAggregate,
		Source: meanScript("storedAggDecays"), Arity: -1, MaxArity: -1,
	})
	h1.mgr.LoadStored(h1.cat)
	if st := h1.status("storedAggDecays"); st.State != StateOK {
		t.Fatalf("the first daemon found it %s: %s", st.State, st.Reason)
	}
	agg, ok := reg1.lookup("storedAggDecays")
	if !ok {
		t.Fatal("the first daemon did not register it")
	}
	if err := db.CheckAggregate(agg, aggSamples()); err != nil {
		t.Fatalf("the aggregate does not keep the laws the reduction relies on: %v", err)
	}

	// Corrupt the stored source, leaving the recorded identity as it was -- which is exactly the
	// damage the checksum exists to notice.
	rec := h1.record("storedAggDecays")
	rec.Source = "def storedAggDecays_init(:\n    return"
	tx := h1.tbl.Begin()
	tx.NewClassAd("storedaggdecays", rec.Ad())
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// The restart: a new manager, a new registry, the same records on disk.
	h2 := openStoredHarness(t, dir)
	reg2 := newFakeAggRegistry()
	h2.mgr.aggReg = reg2
	h2.mgr.LoadStored(h2.cat)

	st := h2.status("storedAggDecays")
	if st.State != StateBad {
		t.Fatalf("after the restart the state is %q, want %q", st.State, StateBad)
	}
	if !strings.Contains(st.Reason, "checksum") {
		t.Errorf("the reason does not say the source no longer matches its identity: %s", st.Reason)
	}
	if _, ok := reg2.lookup("storedAggDecays"); ok {
		t.Error("the restarted daemon registered a bad aggregate")
	}
	if rec := h2.record("storedAggDecays"); rec.State != StateBad {
		t.Errorf("the restarted daemon did not write the state back: %+v", rec)
	}
	if logs := h2.logs.String(); !strings.Contains(logs, "BAD") {
		t.Errorf("the restart's log does not say so:\n%s", logs)
	}
}
