package userfunc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	config "github.com/bbockelm/golang-htcondor/config"
)

// classad's function and aggregate registries are process-global and have no unregister, so every
// test here uses a name no other test uses. That is the contract, not a wart of the test.

func wasmCfg(t *testing.T, body string) *config.Config {
	t.Helper()
	cfg, err := config.NewFromReaderWithOptions(strings.NewReader(body), config.ConfigOptions{Subsystem: "HTCONDORDB"})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

func newWasmMgr(t *testing.T) *Manager {
	t.Helper()
	m := New(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(m.Close)
	return m
}

func fixturePath(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestWasmFunctionIsCallableFromAnExpression is the whole point of the manager: a path in the
// configuration becomes a function an ordinary ClassAd expression can call. Anything short of
// evaluating an expression would be testing that the manager called a registration function, not
// that an administrator got what they configured.
func TestWasmFunctionIsCallableFromAnExpression(t *testing.T) {
	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_WASM_FUNCTIONS = wasmCfgPct
HTCONDORDB_WASM_WASMCFGPCT_PATH = %s
HTCONDORDB_WASM_WASMCFGPCT_MIN_ARGS = 2
`, fixturePath(t, "pctused.wasm")))); err != nil {
		t.Fatalf("apply: %v", err)
	}

	ad, err := classad.Parse(`[ Used = 3; Total = 8; Label = "eight" ]`)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ src, want string }{
		{`wasmCfgPct(Used, Total)`, "37.5"},
		{`WASMCFGPCT(Used, Total)`, "37.5"}, // a registered name folds like every other
		{`wasmCfgPct(Used, Absent)`, "undefined"},
		{`wasmCfgPct(Used, Label)`, "error"},
	} {
		expr, perr := classad.ParseExpr(c.src)
		if perr != nil {
			t.Fatalf("parse %s: %v", c.src, perr)
		}
		if got := expr.Eval(ad).String(); got != c.want {
			t.Errorf("%s = %s, want %s", c.src, got, c.want)
		}
	}
}

// TestWasmAggregateIsRegistered checks the other registry. Reducing by hand rather than running a
// query keeps the test about the manager; db's own tests cover the reduction.
func TestWasmAggregateIsRegistered(t *testing.T) {
	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_WASM_AGGREGATES = wasmCfgMean
HTCONDORDB_WASM_WASMCFGMEAN_PATH = %s
`, fixturePath(t, "mean.wasm")))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	agg, ok := db.LookupAggregate("wasmCfgMean")
	if !ok {
		t.Fatal("the aggregate was not registered")
	}
	acc := agg.NewAccumulator()
	for _, v := range []classad.Value{classad.NewIntValue(2), classad.NewIntValue(4), classad.NewIntValue(9), classad.NewIntValue(1)} {
		acc.Add(v)
	}
	if got := acc.Result().String(); got != "4" {
		t.Errorf("mean = %s, want 4", got)
	}
}

// TestWasmArityMustBeDeclared pins the decision that a function's argument count is required rather
// than defaulted. The registry checks arity before the module runs, so a default wider than the
// module would send calls to a guest that refuses them, and the operator would see ERROR with
// nothing naming the arity as the cause.
func TestWasmArityMustBeDeclared(t *testing.T) {
	m := newWasmMgr(t)
	err := m.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_WASM_FUNCTIONS = wasmCfgNoArity
HTCONDORDB_WASM_WASMCFGNOARITY_PATH = %s
`, fixturePath(t, "pctused.wasm"))))
	if err == nil {
		t.Fatal("a function with no declared arity was accepted")
	}
	if !strings.Contains(err.Error(), "MIN_ARGS") {
		t.Errorf("the error does not name the missing setting: %v", err)
	}
}

// TestWasmMissingPathIsNamed -- a module that cannot be read fails the apply, and the error names
// which one. At start-up that fails the daemon, which is deliberate: a configured function that did
// not load is not a degraded service, it is one answering ERROR where the administrator expected a
// number.
func TestWasmMissingPathIsNamed(t *testing.T) {
	m := newWasmMgr(t)
	err := m.Apply(wasmCfg(t, `
HTCONDORDB_WASM_FUNCTIONS = wasmCfgMissing
HTCONDORDB_WASM_WASMCFGMISSING_PATH = /nonexistent/nope.wasm
HTCONDORDB_WASM_WASMCFGMISSING_MIN_ARGS = 1
`))
	if err == nil {
		t.Fatal("a module that does not exist was accepted")
	}
	for _, n := range classad.RegisteredFunctions() {
		if strings.EqualFold(n, "wasmCfgMissing") {
			t.Error("a name was registered for a module that did not load")
		}
	}
}

// TestWasmNoConfigDoesNothing -- the manager must be inert when nothing is configured, including
// not building a runtime. Every daemon that never uses this pays for whatever apply does.
func TestWasmNoConfigDoesNothing(t *testing.T) {
	m := newWasmMgr(t)
	if err := m.Apply(wasmCfg(t, "HTCONDORDB_DIR = /tmp\n")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if m.loader != nil {
		t.Error("a wasm runtime was built for a daemon with no user functions configured")
	}
	if len(m.Names()) != 0 {
		t.Errorf("registered %v with nothing configured", m.Names())
	}
}

// TestWasmReconfigureOnlyAdds pins the consequence of an append-only registry. A reconfigure that
// changes a loaded module's path cannot take effect, and the manager must not pretend it did --
// re-registering would fail with a name collision, and silently keeping the old one would leave an
// operator believing the new module is running.
func TestWasmReconfigureOnlyAdds(t *testing.T) {
	m := newWasmMgr(t)
	first := fmt.Sprintf(`
HTCONDORDB_WASM_FUNCTIONS = wasmCfgStable
HTCONDORDB_WASM_WASMCFGSTABLE_PATH = %s
HTCONDORDB_WASM_WASMCFGSTABLE_MIN_ARGS = 2
`, fixturePath(t, "pctused.wasm"))
	if err := m.Apply(wasmCfg(t, first)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Reapplying the SAME configuration is a no-op, not a second registration.
	if err := m.Apply(wasmCfg(t, first)); err != nil {
		t.Fatalf("reapplying an unchanged configuration failed: %v", err)
	}

	// A changed path is reported, not applied, and does not fail the reconfigure: the daemon
	// keeps serving with the module it has.
	changed := strings.Replace(first, "pctused.wasm", "mean.wasm", 1)
	if err := m.Apply(wasmCfg(t, changed)); err != nil {
		t.Fatalf("a changed path should be reported, not fail the reconfigure: %v", err)
	}
	if got := m.loaded["wasmcfgstable"].Path; !strings.HasSuffix(got, "pctused.wasm") {
		t.Errorf("the manager believes it loaded %s; the originally loaded module is the one running", got)
	}

	// Adding a name alongside it still works.
	plus := changed + fmt.Sprintf(`
HTCONDORDB_WASM_AGGREGATES = wasmCfgAdded
HTCONDORDB_WASM_WASMCFGADDED_PATH = %s
`, fixturePath(t, "mean.wasm"))
	if err := m.Apply(wasmCfg(t, plus)); err != nil {
		t.Fatalf("adding a module on reconfigure: %v", err)
	}
	if _, ok := db.LookupAggregate("wasmCfgAdded"); !ok {
		t.Error("a module added by reconfigure was not registered")
	}
}

// TestWasmDuplicateNameIsRefused -- naming one module in both lists would register it twice, and
// the second failure's message is about a name collision rather than about the config that caused
// it. Catching it while reading the configuration says what to fix.
func TestWasmDuplicateNameIsRefused(t *testing.T) {
	_, err := resolveSettings(wasmCfg(t, `
HTCONDORDB_WASM_FUNCTIONS = wasmCfgDup
HTCONDORDB_WASM_AGGREGATES = wasmCfgDup
HTCONDORDB_WASM_WASMCFGDUP_PATH = /tmp/x.wasm
HTCONDORDB_WASM_WASMCFGDUP_MIN_ARGS = 1
`))
	if err == nil {
		t.Fatal("a name in both lists was accepted")
	}
	if !strings.Contains(err.Error(), "twice") {
		t.Errorf("the error does not say what is wrong: %v", err)
	}
}
