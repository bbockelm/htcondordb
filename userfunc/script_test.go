package userfunc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
)

// As in the wasm tests: classad's registry is process-global and has no unregister, so every name
// used here is used by exactly one test. That is the contract, not a wart of the test.

// newLoggingMgr returns a manager whose log lines land in a buffer, for the tests that assert what
// an operator is told. The messages are the whole point of the reconfigure rules -- a change that
// cannot be applied and is not reported leaves an operator believing it was.
func newLoggingMgr(t *testing.T) (*Manager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	m := New(context.Background(), slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(m.Close)
	return m, &buf
}

// TestScriptFunctionIsCallableFromAnExpression is the whole point of the manager for this kind, and
// it covers the one-file-several-functions case at the same time: both names below name the same
// file and pick a different `def` out of it.
func TestScriptFunctionIsCallableFromAnExpression(t *testing.T) {
	m := newWasmMgr(t)
	path := fixturePath(t, "policy.cas")
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgPct scriptCfgDouble
HTCONDORDB_SCRIPT_SCRIPTCFGPCT_PATH = %s
HTCONDORDB_SCRIPT_SCRIPTCFGDOUBLE_PATH = %s
`, path, path))); err != nil {
		t.Fatalf("apply: %v", err)
	}

	ad, err := classad.Parse(`[ Used = 3; Total = 8; Label = "eight" ]`)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ src, want string }{
		{`scriptCfgPct(Used, Total)`, "37.5"},
		{`SCRIPTCFGPCT(Used, Total)`, "37.5"}, // a registered name folds like every other
		{`scriptCfgPct(Used, Absent)`, "undefined"},
		{`scriptCfgPct(Used, Label)`, "error"},
		{`scriptCfgDouble(Used)`, "6"}, // the second `def` out of the same file
	} {
		expr, perr := classad.ParseExpr(c.src)
		if perr != nil {
			t.Fatalf("parse %s: %v", c.src, perr)
		}
		if got := expr.Eval(ad).String(); got != c.want {
			t.Errorf("%s = %s, want %s", c.src, got, c.want)
		}
	}

	// The arity the registry holds is the `def`'s own, which is the simplification over wasm:
	// there is no MIN_ARGS in the configuration above, and nothing for one to be wrong about.
	if !classad.StrictFunction("scriptCfgPct", 2) {
		t.Error("the registry does not accept scriptCfgPct/2")
	}
	if classad.StrictFunction("scriptCfgPct", 1) {
		t.Error("the registry accepts scriptCfgPct/1; the def declares two parameters")
	}
}

// TestScriptPathIsRequired -- a name with no file behind it is caught while reading the
// configuration, which is where the message can name the key to set.
func TestScriptPathIsRequired(t *testing.T) {
	_, err := resolveSettings(wasmCfg(t, `
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgNoPath
`))
	if err == nil {
		t.Fatal("a script function with no path was accepted")
	}
	if !strings.Contains(err.Error(), "HTCONDORDB_SCRIPT_SCRIPTCFGNOPATH_PATH") {
		t.Errorf("the error does not name the missing key: %v", err)
	}
}

// TestScriptOneFileBacksSeveralFunctions -- two names, one file, each picking its own `def`.
// Resolution has to produce two modules sharing a path; the reading side is what dedupes.
func TestScriptOneFileBacksSeveralFunctions(t *testing.T) {
	s, err := resolveSettings(wasmCfg(t, `
HTCONDORDB_SCRIPT_FUNCTIONS = alpha beta
HTCONDORDB_SCRIPT_ALPHA_PATH = /etc/condor/scripts/policy.cas
HTCONDORDB_SCRIPT_BETA_PATH = /etc/condor/scripts/policy.cas
`))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(s.modules) != 2 {
		t.Fatalf("resolved %d modules, want 2", len(s.modules))
	}
	for _, mod := range s.modules {
		if !mod.Script {
			t.Errorf("%s resolved as a wasm module", mod.Name)
		}
		if mod.Path != "/etc/condor/scripts/policy.cas" {
			t.Errorf("%s resolved path %s", mod.Name, mod.Path)
		}
		if mod.MinArgs != 0 || mod.MaxArgs != 0 {
			t.Errorf("%s carries an arity (%d..%d); a def states its own",
				mod.Name, mod.MinArgs, mod.MaxArgs)
		}
	}
}

// TestScriptAndWasmShareOneNameSpace -- a query calling a name reaches one registered function, and
// which kind of file it came from is not part of the call. Catching the collision while reading the
// configuration names the two lists; letting it through would fail the second registration with a
// message about the registry and nothing about the config line that caused it.
func TestScriptAndWasmShareOneNameSpace(t *testing.T) {
	_, err := resolveSettings(wasmCfg(t, `
HTCONDORDB_WASM_FUNCTIONS = scriptCfgClash
HTCONDORDB_WASM_SCRIPTCFGCLASH_PATH = /tmp/x.wasm
HTCONDORDB_WASM_SCRIPTCFGCLASH_MIN_ARGS = 1
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgClash
HTCONDORDB_SCRIPT_SCRIPTCFGCLASH_PATH = /tmp/x.cas
`))
	if err == nil {
		t.Fatal("one name was accepted as both a wasm module and a script function")
	}
	if !strings.Contains(err.Error(), "twice") {
		t.Errorf("the error does not say what is wrong: %v", err)
	}
}

// TestScriptThatReadsTheAdIsRefused is the failure with no counterpart in a wasm registration. A
// bare name a script's body does not bind is an attribute of the subject ad, which is the point of
// it inside a script and impossible in a registration -- the compiled form has no subject, so the
// read has no answer that could be right.
//
// The assertions are about what an OPERATOR sees, because this is a start-up failure and that text
// is all they get: it has to name the function, the attribute it reached for and the file.
func TestScriptThatReadsTheAdIsRefused(t *testing.T) {
	m := newWasmMgr(t)
	path := fixturePath(t, "readsad.cas")
	err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgReadsAd
HTCONDORDB_SCRIPT_SCRIPTCFGREADSAD_PATH = %s
`, path)))
	if err == nil {
		t.Fatal("a script function that reads the ad was registered")
	}
	if !errors.Is(err, vm.ErrReadsAd) {
		t.Errorf("the refusal did not survive as vm.ErrReadsAd: %v", err)
	}
	for _, want := range []string{"scriptCfgReadsAd", "Memory", "pass what it needs as a parameter", path} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message does not contain %q: %v", want, err)
		}
	}
	for _, n := range classad.RegisteredFunctions() {
		if strings.EqualFold(n, "scriptCfgReadsAd") {
			t.Error("a name was registered for a function that did not pass the purity gate")
		}
	}
}

// TestScriptMissingFileIsNamed -- a file that cannot be read fails the apply, and nothing is
// registered. At start-up that fails the daemon, for the reason the wasm case does.
func TestScriptMissingFileIsNamed(t *testing.T) {
	m := newWasmMgr(t)
	err := m.Apply(wasmCfg(t, `
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgMissing
HTCONDORDB_SCRIPT_SCRIPTCFGMISSING_PATH = /nonexistent/nope.cas
`))
	if err == nil {
		t.Fatal("a file that does not exist was accepted")
	}
	if !strings.Contains(err.Error(), "/nonexistent/nope.cas") {
		t.Errorf("the error does not name the file: %v", err)
	}
	for _, n := range classad.RegisteredFunctions() {
		if strings.EqualFold(n, "scriptCfgMissing") {
			t.Error("a name was registered for a file that did not load")
		}
	}
}

// TestScriptOnlyConfigBuildsNoWasmRuntime -- a daemon whose user functions are all scripts has no
// business holding a WebAssembly runtime. The loader costs memory and goroutines, and a guest that
// never arrives does not justify either.
func TestScriptOnlyConfigBuildsNoWasmRuntime(t *testing.T) {
	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgNoWasm
HTCONDORDB_SCRIPT_SCRIPTCFGNOWASM_PATH = %s
`, fixturePath(t, "policy.cas")))); err == nil {
		// policy.cas has no `def scriptCfgNoWasm`, so this apply fails -- which is the harder
		// case: even a FAILED script load must not have built a runtime.
		t.Fatal("a name the file does not define was accepted")
	}
	if m.loader != nil {
		t.Error("a wasm runtime was built for a daemon configuring script functions only")
	}

	// And the successful path too.
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgNoWasmOK
HTCONDORDB_SCRIPT_SCRIPTCFGNOWASMOK_PATH = %s
`, fixturePath(t, "noWasm.cas")))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if m.loader != nil {
		t.Error("a wasm runtime was built for a daemon configuring script functions only")
	}
}

// TestScriptReconfigureOnlyAdds pins the append-only rules for this kind, which are the wasm rules
// because they are the registry's rules: a reconfigure may ADD a function and nothing else, and
// what it cannot apply it has to say, once.
func TestScriptReconfigureOnlyAdds(t *testing.T) {
	m, log := newLoggingMgr(t)
	first := fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgStable
HTCONDORDB_SCRIPT_SCRIPTCFGSTABLE_PATH = %s
`, fixturePath(t, "policy.cas"))
	if err := m.Apply(wasmCfg(t, first)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Reapplying the SAME configuration is a no-op, not a second registration.
	if err := m.Apply(wasmCfg(t, first)); err != nil {
		t.Fatalf("reapplying an unchanged configuration failed: %v", err)
	}

	// A changed path is reported, not applied, and does not fail the reconfigure: the daemon keeps
	// serving with the function it has.
	changed := strings.Replace(first, "policy.cas", "readsad.cas", 1)
	if err := m.Apply(wasmCfg(t, changed)); err != nil {
		t.Fatalf("a changed path should be reported, not fail the reconfigure: %v", err)
	}
	if err := m.Apply(wasmCfg(t, changed)); err != nil {
		t.Fatalf("reapplying the changed configuration: %v", err)
	}
	if got := m.loaded["scriptcfgstable"].Path; !strings.HasSuffix(got, "policy.cas") {
		t.Errorf("the manager believes it loaded %s; the originally loaded file is the one running", got)
	}
	if n := strings.Count(log.String(), `the configuration for \"scriptCfgStable\" changed`); n != 1 {
		t.Errorf("the changed path was reported %d times; a reconfigure loop must say it once", n)
	}

	// Adding a name alongside it still works.
	plus := changed + fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgStable scriptCfgAdded
HTCONDORDB_SCRIPT_SCRIPTCFGADDED_PATH = %s
`, fixturePath(t, "policy.cas"))
	if err := m.Apply(wasmCfg(t, plus)); err != nil {
		t.Fatalf("adding a function on reconfigure: %v", err)
	}
	if !classad.StrictFunction("scriptCfgAdded", 1) {
		t.Error("a function added by reconfigure was not registered")
	}

	// Removing is reported and not applied, once however often the configuration is reread.
	for range 3 {
		if err := m.Apply(wasmCfg(t, "HTCONDORDB_DIR = /tmp\n")); err != nil {
			t.Fatalf("removing everything should be reported, not fail: %v", err)
		}
	}
	if n := strings.Count(log.String(), "user functions were removed from the configuration"); n != 1 {
		t.Errorf("the removal was reported %d times; a reconfigure loop must say it once", n)
	}
}

// TestScriptKindChangeIsReported -- a name loaded out of a script file and then reconfigured as a
// wasm module of the same name has CHANGED, and the manager has to say so like any other change it
// cannot apply. The kind is part of the comparison because nothing else reliably is: a wasm entry
// whose path and argument count happened to match the script's would otherwise read as "already
// loaded" and the operator would be told nothing at all.
func TestScriptKindChangeIsReported(t *testing.T) {
	m, log := newLoggingMgr(t)
	path := fixturePath(t, "policy.cas")
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = scriptCfgKind
HTCONDORDB_SCRIPT_SCRIPTCFGKIND_PATH = %s
`, path))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// The same name, the same file, and an argument count chosen so that everything else about
	// the two entries matches.
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_WASM_FUNCTIONS = scriptCfgKind
HTCONDORDB_WASM_SCRIPTCFGKIND_PATH = %s
HTCONDORDB_WASM_SCRIPTCFGKIND_MIN_ARGS = 0
`, path))); err != nil {
		t.Fatalf("a changed kind should be reported, not fail the reconfigure: %v", err)
	}
	if n := strings.Count(log.String(), `the configuration for \"scriptCfgKind\" changed`); n != 1 {
		t.Errorf("the changed kind was reported %d times, want once", n)
	}
	if m.loaded["scriptcfgkind"].Script != true {
		t.Error("the manager believes a wasm module replaced the registered script function")
	}
}
