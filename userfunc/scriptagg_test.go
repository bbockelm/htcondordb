package userfunc

import (
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/htcondordb/repl"
)

// As everywhere else in this package, every name below is used by exactly one test: classad's
// aggregate registry is process-global and has no unregister.

// newAggExec returns a query path: a database, a server over a pipe, and the executor an
// administrator's `SELECT` goes through. The aggregate registry the server resolves a name
// against is this process's, which is the one Apply registered into.
func newAggExec(t *testing.T) *repl.Executor {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	s := dbrpc.NewServer(d)
	cp, sp := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sp), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cp))
	t.Cleanup(func() { c.Close(); s.Close(); d.Close() })
	return repl.NewExecutor(c, repl.ExecConfig{})
}

func mustExec(t *testing.T, e *repl.Executor, sql string) *repl.Result {
	t.Helper()
	r, err := e.ExecString(sql)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return r
}

// TestScriptAggregateAnswersAGroupBy is the whole point of the key: a path in the configuration
// becomes an aggregate a `GROUP BY` can name where `SUM` would go. Anything short of running the
// query would test that the manager called a registration function, not that an administrator got
// what they configured -- and the way this fails is specific: a name the parser does not know as
// an aggregate is read as an ordinary function call and evaluated per row, which answers
// successfully and wrongly.
func TestScriptAggregateAnswersAGroupBy(t *testing.T) {
	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgMean
HTCONDORDB_SCRIPT_SCRIPTCFGMEAN_PATH = %s
`, fixturePath(t, "aggregates.cas")))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// A daemon whose user functions are all scripts has no business holding a WebAssembly
	// runtime, aggregates included.
	if m.loader != nil {
		t.Error("a wasm runtime was built for a daemon configuring script aggregates only")
	}

	agg, ok := db.LookupAggregate("scriptCfgMean")
	if !ok {
		t.Fatal("the aggregate was not registered")
	}

	// THE LAWS ARE THE SCRIPT AUTHOR'S TO KEEP and nothing in the loader can check them, so a
	// fixture here checks its own. The columnar pass fans out across segment windows and which
	// worker draws which segment is a race, so a merge that depended on order would be right in
	// every query this test runs and wrong in production, plausibly and only sometimes. The
	// sample holds what a row can really carry, not only what the aggregate was written for.
	if err := db.CheckAggregate(agg, []classad.Value{
		classad.NewIntValue(1024),
		classad.NewIntValue(2048),
		classad.NewRealValue(4096.5),
		classad.NewStringValue("lots"),
		classad.NewUndefinedValue(),
	}); err != nil {
		t.Fatalf("the aggregate does not keep the laws the reduction relies on: %v", err)
	}

	e := newAggExec(t)
	for _, row := range []string{
		"('1.0', 'alice', 100)",
		"('1.1', 'alice', 200)",
		"('2.0', 'bob', 300)",
		"('2.1', 'bob', 400)",
	} {
		mustExec(t, e, "INSERT INTO ads (Key, Owner, Mem) VALUES "+row)
	}
	// Two rows the mean must not count: one whose attribute is not a number and one that does
	// not carry it at all. They are in the group, so COUNT sees them and the mean does not.
	mustExec(t, e, "INSERT INTO ads (Key, Owner, Mem) VALUES ('2.2', 'bob', 'lots')")
	mustExec(t, e, "INSERT INTO ads (Key, Owner) VALUES ('1.2', 'alice')")

	r := mustExec(t, e, "SELECT Owner, COUNT(*), scriptCfgMean(Mem) FROM ads GROUP BY Owner")
	rows := append([][]string(nil), r.Rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
	if len(rows) != 2 {
		t.Fatalf("got %d groups, want 2: %v", len(rows), rows)
	}
	for i, want := range []struct {
		owner string
		count string
		mean  float64
	}{
		{"alice", "3", 150},
		{"bob", "3", 350},
	} {
		got := rows[i]
		if got[0] != want.owner || got[1] != want.count {
			t.Errorf("group %d = %v, want owner %s with %s rows", i, got, want.owner, want.count)
		}
		mean, err := strconv.ParseFloat(got[2], 64)
		if err != nil {
			t.Errorf("%s: the aggregate answered %q, which is not a number", want.owner, got[2])
			continue
		}
		if math.Abs(mean-want.mean) > 1e-9 {
			t.Errorf("%s: mean = %v, want %v", want.owner, mean, want.mean)
		}
	}
}

// TestScriptAggregateDefNamesFollowTheConfiguredName pins the rule: the configured name picks the
// four `def`s out of the file by prefix, which is the scalar rule -- the configured name is the
// name of the `def` -- carried to a thing that takes four of them.
func TestScriptAggregateDefNamesFollowTheConfiguredName(t *testing.T) {
	s, err := resolveSettings(wasmCfg(t, `
HTCONDORDB_SCRIPT_AGGREGATES = jobMean
HTCONDORDB_SCRIPT_JOBMEAN_PATH = /etc/condor/scripts/policy.cas
`))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(s.modules) != 1 {
		t.Fatalf("resolved %d modules, want 1", len(s.modules))
	}
	mod := s.modules[0]
	if !mod.Script || !mod.Aggregate {
		t.Fatalf("resolved script=%v aggregate=%v, want both", mod.Script, mod.Aggregate)
	}
	want := aggDefs{Init: "jobMean_init", Add: "jobMean_add", Merge: "jobMean_merge", Result: "jobMean_result"}
	if mod.Defs != want {
		t.Errorf("defs = %+v, want %+v", mod.Defs, want)
	}
	if mod.MinArgs != 0 || mod.MaxArgs != 0 {
		t.Errorf("the aggregate carries an arity (%d..%d); each `def` states its own",
			mod.MinArgs, mod.MaxArgs)
	}
}

// TestScriptAggregateDefNamesCanBeOverridden -- a file whose definitions fit no prefix still
// configures, by naming each `def` for the role it fills. This is what makes the prefix rule a
// default rather than a requirement on how a site writes its scripts.
func TestScriptAggregateDefNamesCanBeOverridden(t *testing.T) {
	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgMax
HTCONDORDB_SCRIPT_SCRIPTCFGMAX_PATH = %s
HTCONDORDB_SCRIPT_SCRIPTCFGMAX_INIT = mkFloor
HTCONDORDB_SCRIPT_SCRIPTCFGMAX_ADD = foldMax
HTCONDORDB_SCRIPT_SCRIPTCFGMAX_MERGE = joinMax
HTCONDORDB_SCRIPT_SCRIPTCFGMAX_RESULT = takeMax
`, fixturePath(t, "aggregates.cas")))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	agg, ok := db.LookupAggregate("scriptCfgMax")
	if !ok {
		t.Fatal("the aggregate was not registered under the configured name")
	}
	if err := db.CheckAggregate(agg, []classad.Value{
		classad.NewIntValue(7),
		classad.NewIntValue(42),
		classad.NewRealValue(3.5),
		classad.NewStringValue("lots"),
		classad.NewUndefinedValue(),
	}); err != nil {
		t.Fatalf("the aggregate does not keep the laws the reduction relies on: %v", err)
	}
	acc := agg.NewAccumulator()
	for _, v := range []classad.Value{classad.NewIntValue(7), classad.NewIntValue(42), classad.NewIntValue(9)} {
		acc.Add(v)
	}
	if got := acc.Result().String(); got != "42" {
		t.Errorf("max = %s, want 42", got)
	}
}

// TestScriptAggregateMissingDefNamesIt -- nothing is guessed, so the message has to say which
// `def` the loader went looking for. An administrator reading it can open the file and see that
// the name is not there; a message saying only that the aggregate did not load cannot be acted on.
func TestScriptAggregateMissingDefNamesIt(t *testing.T) {
	m := newWasmMgr(t)
	path := fixturePath(t, "aggregates.cas")
	err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgNoMerge
HTCONDORDB_SCRIPT_SCRIPTCFGNOMERGE_PATH = %s
`, path)))
	if err == nil {
		t.Fatal("an aggregate missing one of its four definitions was registered")
	}
	for _, want := range []string{"scriptCfgNoMerge_merge", "merge", "scriptCfgNoMerge", path} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message does not contain %q: %v", want, err)
		}
	}
	if _, ok := db.LookupAggregate("scriptCfgNoMerge"); ok {
		t.Error("a name was registered for an aggregate that did not load")
	}
}

// TestScriptAggregateWrongArityIsRefused -- the engine supplies the arguments, so a `def` that
// declares the wrong number of parameters cannot fill its role. Catching it at load turns what
// would be a column of ERROR into a start-up failure naming the `def`.
func TestScriptAggregateWrongArityIsRefused(t *testing.T) {
	m := newWasmMgr(t)
	err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgBadArity
HTCONDORDB_SCRIPT_SCRIPTCFGBADARITY_PATH = %s
`, fixturePath(t, "aggregates.cas"))))
	if err == nil {
		t.Fatal("an aggregate whose `add` takes one parameter was registered")
	}
	if !strings.Contains(err.Error(), "scriptCfgBadArity_add") {
		t.Errorf("the message does not name the definition that does not fit: %v", err)
	}
	if _, ok := db.LookupAggregate("scriptCfgBadArity"); ok {
		t.Error("a name was registered for an aggregate that did not load")
	}
}

// TestScriptAggregateThatReadsTheAdIsRefused -- the purity gate applies to each of the four
// definitions, and its refusal has to reach the operator. This is a start-up failure and the text
// is all they get, so it has to name the function, the attribute it reached for and the file.
func TestScriptAggregateThatReadsTheAdIsRefused(t *testing.T) {
	m := newWasmMgr(t)
	path := fixturePath(t, "aggreadsad.cas")
	err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgImpureAgg
HTCONDORDB_SCRIPT_SCRIPTCFGIMPUREAGG_PATH = %s
`, path)))
	if err == nil {
		t.Fatal("an aggregate whose `add` reads the ad was registered")
	}
	if !errors.Is(err, vm.ErrReadsAd) {
		t.Errorf("the refusal did not survive as vm.ErrReadsAd: %v", err)
	}
	for _, want := range []string{"scriptCfgImpureAgg_add", "Memory", "pass what it needs as a parameter", path} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message does not contain %q: %v", want, err)
		}
	}
	if _, ok := db.LookupAggregate("scriptCfgImpureAgg"); ok {
		t.Error("a name was registered for an aggregate that did not pass the purity gate")
	}
}

// TestScriptAggregatePathIsRequired -- a name with no file behind it is caught while reading the
// configuration, which is where the message can name the key to set.
func TestScriptAggregatePathIsRequired(t *testing.T) {
	_, err := resolveSettings(wasmCfg(t, "HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgAggNoPath\n"))
	if err == nil {
		t.Fatal("a script aggregate with no path was accepted")
	}
	if !strings.Contains(err.Error(), "HTCONDORDB_SCRIPT_SCRIPTCFGAGGNOPATH_PATH") {
		t.Errorf("the error does not name the missing key: %v", err)
	}
}

// TestScriptAggregateNameIsNotAvailableElsewhere -- an aggregate goes into a different registry
// from a scalar function, but the name space is one: a client's parser decides whether a name is
// an aggregate call or an ordinary one before any request exists, so a name can only be one of
// them. Catching the collision while reading the configuration names the lists to look at.
func TestScriptAggregateNameIsNotAvailableElsewhere(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"a script function", `
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgAggClashA
HTCONDORDB_SCRIPT_SCRIPTCFGAGGCLASHA_PATH = /tmp/x.cas
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgAggClashA
`},
		{"a wasm aggregate", `
HTCONDORDB_WASM_AGGREGATES = scriptCfgAggClashB
HTCONDORDB_WASM_SCRIPTCFGAGGCLASHB_PATH = /tmp/x.wasm
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgAggClashB
HTCONDORDB_SCRIPT_SCRIPTCFGAGGCLASHB_PATH = /tmp/x.cas
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveSettings(wasmCfg(t, tc.body))
			if err == nil {
				t.Fatal("one name was accepted twice")
			}
			if !strings.Contains(err.Error(), "twice") {
				t.Errorf("the error does not say what is wrong: %v", err)
			}
		})
	}
}

// TestScriptAggregateReconfigureOnlyAdds pins the append-only rules for this kind, which are the
// rules for every other kind because they are the registry's: a reconfigure may ADD an aggregate
// and nothing else, and what it cannot apply it has to say, once.
func TestScriptAggregateReconfigureOnlyAdds(t *testing.T) {
	m, log := newLoggingMgr(t)
	path := fixturePath(t, "aggregates.cas")
	first := fmt.Sprintf(`
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgStableAgg
HTCONDORDB_SCRIPT_SCRIPTCFGSTABLEAGG_PATH = %s
`, path)
	if err := m.Apply(wasmCfg(t, first)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, ok := db.LookupAggregate("scriptCfgStableAgg"); !ok {
		t.Fatal("the aggregate was not registered")
	}
	// Reapplying the SAME configuration is a no-op, not a second registration.
	if err := m.Apply(wasmCfg(t, first)); err != nil {
		t.Fatalf("reapplying an unchanged configuration failed: %v", err)
	}

	// A changed path is reported, not applied, and does not fail the reconfigure: the daemon
	// keeps serving with the aggregate it has.
	changed := strings.Replace(first, "aggregates.cas", "aggreadsad.cas", 1)
	for range 3 {
		if err := m.Apply(wasmCfg(t, changed)); err != nil {
			t.Fatalf("a changed path should be reported, not fail the reconfigure: %v", err)
		}
	}
	if got := m.loaded["scriptcfgstableagg"].Path; got != path {
		t.Errorf("the manager believes it loaded %s; the originally loaded file is the one running", got)
	}
	if n := strings.Count(log.String(), `the configuration for \"scriptCfgStableAgg\" changed`); n != 1 {
		t.Errorf("the changed path was reported %d times; a reconfigure loop must say it once", n)
	}

	// Renaming one role is a change too: swapping which `def` folds a row makes it a different
	// aggregate, and a running registration cannot take that any more than a new file.
	renamed := first + "HTCONDORDB_SCRIPT_SCRIPTCFGSTABLEAGG_ADD = foldMax\n"
	if err := m.Apply(wasmCfg(t, renamed)); err != nil {
		t.Fatalf("a renamed definition should be reported, not fail the reconfigure: %v", err)
	}
	if n := strings.Count(log.String(), `the configuration for \"scriptCfgStableAgg\" changed`); n != 1 {
		t.Errorf("the change was reported %d times; a reconfigure loop must say it once", n)
	}

	// Adding a name alongside it still works.
	plus := first + fmt.Sprintf(`
HTCONDORDB_SCRIPT_AGGREGATES = scriptCfgStableAgg scriptCfgAddedAgg
HTCONDORDB_SCRIPT_SCRIPTCFGADDEDAGG_PATH = %s
`, path)
	if err := m.Apply(wasmCfg(t, plus)); err != nil {
		t.Fatalf("adding an aggregate on reconfigure: %v", err)
	}
	if _, ok := db.LookupAggregate("scriptCfgAddedAgg"); !ok {
		t.Error("an aggregate added by reconfigure was not registered")
	}
}
