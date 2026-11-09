package userfunc

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// As everywhere else in this package, a test that registers into classad's own registry uses a
// name no other test uses: the registry is process-wide and has no unregister. A test that wants
// to register the SAME name twice -- which is what a restart is -- hands the manager a registry of
// its own instead (see fakeRegistry), because the real one cannot be asked twice and a test that
// could only ever run once would be a test nobody dares change.

// syncLog collects log lines for the assertions that badness is visible in the log. Safe for the
// logger's concurrent writes.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// fakeRegistry is a scalar registry a test owns, so a name can be registered again in a later
// "daemon".
type fakeRegistry struct {
	mu   sync.Mutex
	fns  map[string]classad.Func
	min  map[string]int
	max  map[string]int
	errs map[string]error // names this registry refuses, by folded name
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{fns: map[string]classad.Func{}, min: map[string]int{}, max: map[string]int{}, errs: map[string]error{}}
}

func (r *fakeRegistry) RegisterFunction(name string, minArgs, maxArgs int, fn func(args []classad.Value) classad.Value) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	folded := strings.ToLower(name)
	if err := r.errs[folded]; err != nil {
		return err
	}
	if _, dup := r.fns[folded]; dup {
		return fmt.Errorf("already registered: %s", name)
	}
	r.fns[folded], r.min[folded], r.max[folded] = fn, minArgs, maxArgs
	return nil
}

func (r *fakeRegistry) call(name string, args ...classad.Value) (classad.Value, bool) {
	r.mu.Lock()
	fn := r.fns[strings.ToLower(name)]
	r.mu.Unlock()
	if fn == nil {
		return classad.NewUndefinedValue(), false
	}
	return fn(args), true
}

func (r *fakeRegistry) arity(name string) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	folded := strings.ToLower(name)
	if _, ok := r.fns[folded]; !ok {
		return 0, 0, false
	}
	return r.min[folded], r.max[folded], true
}

// storedHarness is a catalog with a user_functions table, a manager over it, and the log.
type storedHarness struct {
	t    *testing.T
	dir  string
	cat  *db.Catalog
	tbl  *db.DB
	mgr  *Manager
	logs *syncLog
}

func newStoredHarness(t *testing.T) *storedHarness {
	t.Helper()
	dir := t.TempDir()
	return openStoredHarness(t, dir)
}

// openStoredHarness opens a catalog in an existing directory, which is how a restart is
// simulated: the records stay on disk and a NEW manager loads them.
func openStoredHarness(t *testing.T, dir string) *storedHarness {
	t.Helper()
	cat, err := db.OpenCatalog(dir)
	if err != nil {
		t.Fatalf("opening the catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	tbl, err := cat.CreateTable(Table)
	if err != nil {
		t.Fatalf("creating %s: %v", Table, err)
	}
	logs := &syncLog{}
	m := New(context.Background(), slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(m.Close)
	return &storedHarness{t: t, dir: dir, cat: cat, tbl: tbl, mgr: m, logs: logs}
}

// put writes a record straight into the table, which is what an earlier daemon's administrative
// command left behind.
func (h *storedHarness) put(rec Record) {
	h.t.Helper()
	if rec.Checksum == "" {
		src, err := rec.sourceBytes()
		if err != nil {
			h.t.Fatalf("source: %v", err)
		}
		rec.Checksum = checksumOf(src)
	}
	tx := h.tbl.Begin()
	tx.NewClassAd(strings.ToLower(rec.Name), rec.Ad())
	if err := tx.Commit(); err != nil {
		h.t.Fatalf("writing the record: %v", err)
	}
}

// record reads a record back out of the table, which is where the state has to be visible.
func (h *storedHarness) record(name string) Record {
	h.t.Helper()
	ad, ok := h.tbl.LookupClassAd(strings.ToLower(name))
	if !ok {
		h.t.Fatalf("no record for %s", name)
	}
	return recordFromAd(strings.ToLower(name), ad)
}

func (h *storedHarness) status(name string) Status {
	h.t.Helper()
	for _, st := range h.mgr.List() {
		if strings.EqualFold(st.Name, name) {
			return st
		}
	}
	h.t.Fatalf("%s is not in the listing %v", name, h.mgr.List())
	return Status{}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// scriptSource is a one-function script module under a name of the caller's choosing.
func scriptSource(name, body string) string {
	return "def " + name + body
}

// TestStoredFunctionsRegisterAtStartupAndAreCallable is the whole point: a record in a table
// becomes a function an ordinary query can call, for both kinds, with no configuration anywhere.
//
// It runs a real QUERY and not only an expression. The query path is the one that compiles and
// caches a program, which is the path the whole design is shaped around, and an expression
// evaluated by hand would not touch it.
func TestStoredFunctionsRegisterAtStartupAndAreCallable(t *testing.T) {
	h := newStoredHarness(t)
	h.put(Record{
		Name:   "storedPct",
		Kind:   KindScript,
		Source: scriptSource("storedPct", "(used, total):\n    if total <= 0:\n        return error\n    return used * 100.0 / total\n"),
		Arity:  2, MaxArity: 2,
		RegisteredBy: "admin@test",
	})
	h.put(Record{
		Name:   "storedWasmPct",
		Kind:   KindWasm,
		Source: base64.StdEncoding.EncodeToString(fixture(t, "pctused.wasm")),
		Arity:  2, MaxArity: 2,
	})
	h.mgr.LoadStored(h.cat)

	rows, err := h.cat.CreateTable("machines")
	if err != nil {
		t.Fatal(err)
	}
	for key, text := range map[string]string{
		"busy": `Used = 7; Total = 8`,
		"idle": `Used = 1; Total = 8`,
	} {
		ad, perr := classad.ParseOld(strings.ReplaceAll(text, "; ", "\n"))
		if perr != nil {
			t.Fatal(perr)
		}
		tx := rows.Begin()
		tx.NewClassAd(key, ad)
		if cerr := tx.Commit(); cerr != nil {
			t.Fatal(cerr)
		}
	}

	for _, c := range []struct{ constraint, want string }{
		{"storedPct(Used, Total) > 50", "busy"},
		{"storedWasmPct(Used, Total) < 50", "idle"},
	} {
		seq, qerr := rows.Query(c.constraint)
		if qerr != nil {
			t.Fatalf("query %s: %v", c.constraint, qerr)
		}
		var got []string
		for ad := range seq {
			u, _ := ad.EvaluateAttrInt("Used")
			got = append(got, map[int64]string{7: "busy", 1: "idle"}[u])
		}
		if len(got) != 1 || got[0] != c.want {
			t.Errorf("%s matched %v, want [%s]", c.constraint, got, c.want)
		}
	}

	// The state reached the record, so a restart does not have to work it out again and an
	// operator reading the table sees what an operator reading the log sees.
	if rec := h.record("storedPct"); rec.State != StateOK {
		t.Errorf("the record says State = %q, want %q", rec.State, StateOK)
	}
	if st := h.status("storedPct"); st.State != StateOK || st.Origin != "stored" ||
		!strings.HasPrefix(st.Checksum, "sha256:") || st.RegisteredBy != "admin@test" {
		t.Errorf("listing: %+v", st)
	}
}

// TestStoredReplaceInPlace is the trampoline doing its job: the name is registered once and what
// it runs changes underneath, with no restart and nothing invalidated.
func TestStoredReplaceInPlace(t *testing.T) {
	h := newStoredHarness(t)
	rec := Record{
		Name: "storedReplaceMe", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedReplaceMe", "(n):\n    return n + 1\n"),
	}
	h.put(rec)
	h.mgr.LoadStored(h.cat)
	first := h.status("storedReplaceMe").Checksum

	if got := evalExpr(t, "storedReplaceMe(10)"); got != "11" {
		t.Fatalf("before the replacement: %s, want 11", got)
	}

	rec.Source = scriptSource("storedReplaceMe", "(n):\n    return n + 100\n")
	rec.RegisteredBy = "admin@test"
	after, err := h.mgr.Replace(rec)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	// THE NEW CODE ANSWERS, which is the claim worth making: a name registered before the
	// replacement now reaches the new implementation.
	if got := evalExpr(t, "storedReplaceMe(10)"); got != "110" {
		t.Errorf("after the replacement: %s, want 110", got)
	}
	if after.Checksum == first {
		t.Error("the instance identity did not change, so a result cannot be traced to the code that produced it")
	}
	if rec := h.record("storedReplaceMe"); rec.Checksum != after.Checksum || !strings.Contains(rec.Source, "100") {
		t.Errorf("the record still describes the old code: %+v", rec)
	}
	if st := h.status("storedReplaceMe"); st.Checksum != after.Checksum {
		t.Errorf("the listing still names %s, want %s", st.Checksum, after.Checksum)
	}
}

// TestStoredReplaceCannotChangeArity pins the one rule the trampoline cannot absorb. A compiled
// query captured the argument count, so changing it is a different function and needs a different
// name.
func TestStoredReplaceCannotChangeArity(t *testing.T) {
	h := newStoredHarness(t)
	h.put(Record{
		Name: "storedFixedArity", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedFixedArity", "(n):\n    return n + 1\n"),
	})
	h.mgr.LoadStored(h.cat)

	_, err := h.mgr.Replace(Record{
		Name: "storedFixedArity", Kind: KindScript, Arity: 2, MaxArity: 2,
		Source: scriptSource("storedFixedArity", "(n, m):\n    return n + m\n"),
	})
	if err == nil {
		t.Fatal("a replacement that changes the argument count was accepted")
	}
	if !errors.Is(err, errArityChanged) {
		t.Errorf("the refusal is not the arity guard: %v", err)
	}
	// The message has to SAY WHY, since the fix (a different name, or a restart) is not something
	// an administrator can guess from "refused".
	for _, want := range []string{"1 argument", "2 argument", "different name", "restart"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	// Nothing moved: the old code is still what the name runs, and the record still describes it.
	if got := evalExpr(t, "storedFixedArity(10)"); got != "11" {
		t.Errorf("after the refused replacement: %s, want 11", got)
	}
	if rec := h.record("storedFixedArity"); rec.Arity != 1 || !strings.Contains(rec.Source, "n + 1") {
		t.Errorf("the refused replacement changed the record: %+v", rec)
	}
}

// TestStoredScriptArityMustMatchTheRecord: the record's count is what the name was registered
// under, so a source whose `def` takes a different number is the thing that is wrong.
func TestStoredScriptArityMustMatchTheRecord(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.reg = newFakeRegistry()
	h.put(Record{
		Name: "storedArityLies", Kind: KindScript, Arity: 2, MaxArity: 2,
		Source: scriptSource("storedArityLies", "(n):\n    return n\n"),
	})
	h.mgr.LoadStored(h.cat)
	st := h.status("storedArityLies")
	if st.State != StateBad {
		t.Fatalf("state is %q, want %q", st.State, StateBad)
	}
	if !strings.Contains(st.Reason, "def takes 1") {
		t.Errorf("the reason does not name the disagreement: %s", st.Reason)
	}
}

// TestStoredBadModuleIsRegisteredAndErrorsWithoutStoppingTheDaemon is the deliberate difference
// between the stored path and the configured one, and the three places it has to be visible.
func TestStoredBadModuleIsRegisteredAndErrorsWithoutStoppingTheDaemon(t *testing.T) {
	h := newStoredHarness(t)
	reg := newFakeRegistry()
	h.mgr.reg = reg

	broken := append([]byte{}, fixture(t, "pctused.wasm")...)
	broken = broken[:len(broken)/2] // a module the runtime will not have
	h.put(Record{
		Name: "storedBadWasm", Kind: KindWasm, Arity: 2, MaxArity: 2,
		Source: base64.StdEncoding.EncodeToString(broken),
	})
	// Loading does not panic, does not return, and does not stop anything: that IS the daemon
	// still starting.
	h.mgr.LoadStored(h.cat)

	// 1. The name is REGISTERED, under its stored arity. Leaving it out would change how a call
	// site compiles, which is the decision that cannot be taken back.
	minA, maxA, ok := reg.arity("storedBadWasm")
	if !ok {
		t.Fatal("a bad function was left unregistered, so a call site compiles differently")
	}
	if minA != 2 || maxA != 2 {
		t.Errorf("registered taking %d..%d, want 2..2", minA, maxA)
	}
	// ...and a call to it is ERROR, which is what calling a name that does not exist already gives.
	got, _ := reg.call("storedBadWasm", classad.NewIntValue(1), classad.NewIntValue(2))
	if got.String() != "error" {
		t.Errorf("calling a bad function gave %s, want error", got.String())
	}

	// 2. In the log, at start-up.
	if logs := h.logs.String(); !strings.Contains(logs, "BAD") || !strings.Contains(logs, "storedBadWasm") {
		t.Errorf("the start-up log does not say the function is bad:\n%s", logs)
	}
	// 3. In the record.
	rec := h.record("storedBadWasm")
	if rec.State != StateBad || rec.StateReason == "" || rec.StateAt == 0 {
		t.Errorf("the record does not carry the bad state: %+v", rec)
	}
	// 4. In the listing.
	st := h.status("storedBadWasm")
	if st.State != StateBad || st.Reason == "" {
		t.Errorf("the listing does not report the bad state: %+v", st)
	}
}

// TestStoredFunctionBecomesBadAcrossARestart: a function that was fine, and a daemon that comes
// back to find it is not. Nothing about the record changed except its source, which is what a
// damaged record or a runtime that no longer accepts a module looks like from here.
func TestStoredFunctionBecomesBadAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	h1 := openStoredHarness(t, dir)
	h1.mgr.reg = newFakeRegistry()
	h1.put(Record{
		Name: "storedDecays", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedDecays", "(n):\n    return n + 1\n"),
	})
	h1.mgr.LoadStored(h1.cat)
	if st := h1.status("storedDecays"); st.State != StateOK {
		t.Fatalf("the first daemon found it %s: %s", st.State, st.Reason)
	}

	// Corrupt the stored source, leaving the recorded identity as it was -- which is exactly the
	// damage the checksum exists to notice.
	rec := h1.record("storedDecays")
	rec.Source = "def storedDecays(n:\n    return"
	tx := h1.tbl.Begin()
	tx.NewClassAd("storeddecays", rec.Ad())
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// The restart: a new manager, a new registry, the same records on disk.
	h2 := openStoredHarness(t, dir)
	reg := newFakeRegistry()
	h2.mgr.reg = reg
	h2.mgr.LoadStored(h2.cat)

	st := h2.status("storedDecays")
	if st.State != StateBad {
		t.Fatalf("after the restart the state is %q, want %q", st.State, StateBad)
	}
	if !strings.Contains(st.Reason, "checksum") {
		t.Errorf("the reason does not say the source no longer matches its identity: %s", st.Reason)
	}
	if minA, maxA, ok := reg.arity("storedDecays"); !ok || minA != 1 || maxA != 1 {
		t.Errorf("after the restart it is registered %d..%d (present=%v), want 1..1 present", minA, maxA, ok)
	}
	if got, _ := reg.call("storedDecays", classad.NewIntValue(1)); got.String() != "error" {
		t.Errorf("calling it gave %s, want error", got.String())
	}
	if rec := h2.record("storedDecays"); rec.State != StateBad {
		t.Errorf("the restarted daemon did not write the state back: %+v", rec)
	}
	if logs := h2.logs.String(); !strings.Contains(logs, "BAD") {
		t.Errorf("the restart's log does not say so:\n%s", logs)
	}
}

// TestStoredSourceThatDoesNotParseIsBadNotFatal: the other half of becoming bad, where the
// checksum agrees and the source is simply wrong. The reason has to tell the two apart, because
// one is fixed by replacing the source and the other is not.
func TestStoredSourceThatDoesNotParseIsBad(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.reg = newFakeRegistry()
	h.put(Record{
		Name: "storedUnparseable", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: "def storedUnparseable(n:\n    return",
	})
	h.mgr.LoadStored(h.cat)
	st := h.status("storedUnparseable")
	if st.State != StateBad {
		t.Fatalf("state is %q, want %q", st.State, StateBad)
	}
	if strings.Contains(st.Reason, "checksum") {
		t.Errorf("a source that matches its identity should not be reported as a checksum failure: %s", st.Reason)
	}
}

// TestStoredBadFunctionIsRepairedByReplacement: replacing the source is a write the trampoline can
// absorb, so the repair path does not need a restart.
func TestStoredBadFunctionIsRepairedByReplacement(t *testing.T) {
	h := newStoredHarness(t)
	h.put(Record{
		Name: "storedRepairMe", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: "def storedRepairMe(n:\n    return",
	})
	h.mgr.LoadStored(h.cat)
	if st := h.status("storedRepairMe"); st.State != StateBad {
		t.Fatalf("state is %q, want %q", st.State, StateBad)
	}
	if got := evalExpr(t, "storedRepairMe(10)"); got != "error" {
		t.Fatalf("a bad function answered %s, want error", got)
	}
	if _, err := h.mgr.Replace(Record{
		Name: "storedRepairMe", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedRepairMe", "(n):\n    return n * 3\n"),
	}); err != nil {
		t.Fatalf("repairing a bad function: %v", err)
	}
	if got := evalExpr(t, "storedRepairMe(10)"); got != "30" {
		t.Errorf("after the repair: %s, want 30", got)
	}
	if st := h.status("storedRepairMe"); st.State != StateOK || st.Reason != "" {
		t.Errorf("the listing still reports it bad: %+v", st)
	}
	if rec := h.record("storedRepairMe"); rec.State != StateOK {
		t.Errorf("the record still reports it bad: %+v", rec)
	}
}

// TestStoredTrustIsNotStorable: a record can ask to be trusted and does not get it. Trust removes
// a module's deadline and makes it uninterruptible, which is a capability an operator grants a
// file they pointed at -- a capability a module can claim is a request, not a capability.
func TestStoredTrustIsNotStorable(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.reg = newFakeRegistry()
	ad := Record{
		Name: "storedWantsTrust", Kind: KindWasm, Arity: 2, MaxArity: 2,
		Source: base64.StdEncoding.EncodeToString(fixture(t, "pctused.wasm")),
	}.Ad()
	ad.InsertAttrBool("Trusted", true)
	tx := h.tbl.Begin()
	tx.NewClassAd("storedwantstrust", ad)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// The checksum is left out above, so the record is one that never went through the
	// administrative path -- which is the record most likely to carry an attribute like this.
	h.mgr.LoadStored(h.cat)

	if st := h.status("storedWantsTrust"); st.State != StateOK {
		t.Fatalf("the module did not load: %s", st.Reason)
	}
	impl := h.mgr.stored["storedwantstrust"].tramp.impl.Load()
	if impl.udf == nil {
		t.Fatal("no module behind the trampoline")
	}
	if impl.udf.Trusted() {
		t.Error("a stored module asked to be trusted and got it")
	}
	if logs := h.logs.String(); !strings.Contains(logs, "untrusted") {
		t.Errorf("the request for trust was ignored silently:\n%s", logs)
	}
}

// TestStoredAndConfiguredShareOneNameSpace, from the stored side: the configured function is
// loaded first and keeps the name, and the stored record is bad rather than quietly absent.
func TestStoredRecordCannotTakeAConfiguredName(t *testing.T) {
	h := newStoredHarness(t)
	path := filepath.Join(t.TempDir(), "policy.cas")
	if err := os.WriteFile(path, []byte(scriptSource("storedVsConfigured", "(n):\n    return n + 1\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.mgr.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = storedVsConfigured
HTCONDORDB_SCRIPT_STOREDVSCONFIGURED_PATH = %s
`, path))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	h.put(Record{
		Name: "storedVsConfigured", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedVsConfigured", "(n):\n    return n + 500\n"),
	})
	h.mgr.LoadStored(h.cat)

	// The configured function kept the name...
	if got := evalExpr(t, "storedVsConfigured(1)"); got != "2" {
		t.Errorf("the name runs %s, want the configured 2", got)
	}
	// ...and the stored record says why it did not get it.
	st := h.status("storedVsConfigured")
	found := 0
	for _, s := range h.mgr.List() {
		if strings.EqualFold(s.Name, "storedVsConfigured") {
			found++
		}
	}
	if found != 2 {
		t.Errorf("the listing shows %d entries for one contested name, want both origins", found)
	}
	_ = st
	if rec := h.record("storedVsConfigured"); rec.State != StateBad || !strings.Contains(rec.StateReason, "configured") {
		t.Errorf("the record does not say the name is configured elsewhere: %+v", rec)
	}
	// And registering it administratively is refused for the same reason, rather than failing
	// deeper down with a message about a collision that names neither side.
	if _, err := h.mgr.Register(Record{
		Name: "storedVsConfigured", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedVsConfigured", "(n):\n    return n\n"),
	}); err == nil || !strings.Contains(err.Error(), "configured") {
		t.Errorf("registering over a configured name: %v", err)
	}
}

// TestConfiguredCannotTakeAStoredName is the same name space from the other side: a reconfigure
// that adds a name the table already claimed is reported rather than applied.
func TestConfiguredCannotTakeAStoredName(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.reg = newFakeRegistry()
	h.put(Record{
		Name: "storedFirstClaim", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedFirstClaim", "(n):\n    return n\n"),
	})
	h.mgr.LoadStored(h.cat)

	path := filepath.Join(t.TempDir(), "policy.cas")
	if err := os.WriteFile(path, []byte(scriptSource("storedFirstClaim", "(n):\n    return n\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	err := h.mgr.Apply(wasmCfg(t, fmt.Sprintf(`
HTCONDORDB_SCRIPT_FUNCTIONS = storedFirstClaim
HTCONDORDB_SCRIPT_STOREDFIRSTCLAIM_PATH = %s
`, path)))
	if err == nil {
		t.Fatal("a configured name that a stored function holds was accepted")
	}
	if !strings.Contains(err.Error(), "stored") {
		t.Errorf("the error does not say the name is a stored function: %v", err)
	}
}

// TestStoredRegisterRefusesAModuleThatDoesNotLoad: at registration nothing is promised yet, so a
// source that does not load is refused outright and the name stays free. That is the opposite of
// the start-up load, and the asymmetry is the point: start-up has a name an earlier run already
// claimed and has to keep its shape.
func TestStoredRegisterRefusesAModuleThatDoesNotLoad(t *testing.T) {
	h := newStoredHarness(t)
	reg := newFakeRegistry()
	h.mgr.reg = reg
	h.mgr.LoadStored(h.cat)
	_, err := h.mgr.Register(Record{
		Name: "storedNeverClaimed", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: "def storedNeverClaimed(n:\n    return",
	})
	if err == nil {
		t.Fatal("a source that does not compile was registered")
	}
	if _, _, ok := reg.arity("storedNeverClaimed"); ok {
		t.Error("the name was claimed for a source that did not load")
	}
	if _, ok := h.tbl.LookupClassAd("storedneverclaimed"); ok {
		t.Error("a record was written for a registration that was refused")
	}
}

// TestStoredRegisterThenRemove: removal deletes the record and does NOT give the name back, which
// is the thing an administrator has to be told rather than discover.
func TestStoredRegisterAndRemove(t *testing.T) {
	h := newStoredHarness(t)
	reg := newFakeRegistry()
	h.mgr.reg = reg
	h.mgr.LoadStored(h.cat)
	rec, err := h.mgr.Register(Record{
		Name: "storedTransient", Kind: KindScript, Arity: -1, MaxArity: -1,
		Source:       scriptSource("storedTransient", "(a, b):\n    return a + b\n"),
		RegisteredBy: "admin@test",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	// The argument count was read off the `def`, which is the only place it can be wrong.
	if rec.Arity != 2 || rec.MaxArity != 2 {
		t.Errorf("arity %d..%d, want 2..2", rec.Arity, rec.MaxArity)
	}
	if !strings.HasPrefix(rec.Checksum, "sha256:") || rec.RegisteredAt == 0 {
		t.Errorf("the stored record has no identity or provenance: %+v", rec)
	}
	if got, _ := reg.call("storedTransient", classad.NewIntValue(2), classad.NewIntValue(3)); got.String() != "5" {
		t.Errorf("the registered function answered %s, want 5", got.String())
	}

	note, err := h.mgr.Remove("storedTransient")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !strings.Contains(note, "restarts") {
		t.Errorf("the note does not say the name stays claimed until a restart: %s", note)
	}
	if _, ok := h.tbl.LookupClassAd("storedtransient"); ok {
		t.Error("the record survived removal")
	}
	if got, _ := reg.call("storedTransient", classad.NewIntValue(2), classad.NewIntValue(3)); got.String() != "error" {
		t.Errorf("a removed function answered %s, want error", got.String())
	}
	if st := h.status("storedTransient"); st.State != StateRemoved {
		t.Errorf("the listing dropped the removed name instead of reporting it: %+v", st)
	}
}

// TestStoredRecordWithNoArityIsNotRegistered: there is no count to register the name under, and
// guessing one would fix the wrong shape for the life of the process.
func TestStoredRecordWithNoArityIsNotRegistered(t *testing.T) {
	h := newStoredHarness(t)
	reg := newFakeRegistry()
	h.mgr.reg = reg
	ad := classad.New()
	ad.InsertAttrString(AttrName, "storedNoArity")
	ad.InsertAttrString(AttrKind, KindScript)
	ad.InsertAttrString(AttrSource, scriptSource("storedNoArity", "(n):\n    return n\n"))
	tx := h.tbl.Begin()
	tx.NewClassAd("storednoarity", ad)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	h.mgr.LoadStored(h.cat)

	if _, _, ok := reg.arity("storedNoArity"); ok {
		t.Error("a record with no argument count was registered under a guessed one")
	}
	if st := h.status("storedNoArity"); st.State != StateInvalid {
		t.Errorf("state is %q, want %q", st.State, StateInvalid)
	}
	if logs := h.logs.String(); !strings.Contains(logs, "storedNoArity") {
		t.Errorf("an unusable record was passed over in silence:\n%s", logs)
	}
}

// TestStoredNameMustBeCallable: a built-in may not be shadowed and a name no expression can spell
// is refused where the reason is known, rather than as a registration failure further down.
func TestStoredNameIsChecked(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.reg = newFakeRegistry()
	h.mgr.LoadStored(h.cat)
	for _, c := range []struct{ name, want string }{
		{"strcat", "built-in"},
		{"not a name", "expression can call"},
	} {
		_, err := h.mgr.Register(Record{Name: c.name, Kind: KindScript, Arity: 1, MaxArity: 1,
			Source: "def f(n):\n    return n\n"})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("registering %q: %v, want a refusal mentioning %q", c.name, err, c.want)
		}
	}
}

// TestStoredWasmArityMustBeDeclared: a module cannot be asked what it takes, and the count is
// checked before the guest runs.
func TestStoredWasmArityMustBeDeclared(t *testing.T) {
	h := newStoredHarness(t)
	h.mgr.reg = newFakeRegistry()
	h.mgr.LoadStored(h.cat)
	_, err := h.mgr.Register(Record{
		Name: "storedWasmNoArity", Kind: KindWasm, Arity: -1, MaxArity: -1,
		Source: base64.StdEncoding.EncodeToString(fixture(t, "pctused.wasm")),
	})
	if err == nil || !strings.Contains(err.Error(), "declared") {
		t.Errorf("a wasm registration with no argument count: %v", err)
	}
}

// TestStoredWithoutACatalogIsRefusedCleanly: a tool that holds a manager but no database says so
// rather than panicking on a nil table.
func TestStoredWithoutACatalogIsRefused(t *testing.T) {
	m := New(context.Background(), slog.New(slog.NewTextHandler(&syncLog{}, nil)))
	t.Cleanup(m.Close)
	m.LoadStored(nil)
	if _, err := m.Register(Record{Name: "storedNoStore", Kind: KindScript, Arity: 1, MaxArity: 1,
		Source: scriptSource("storedNoStore", "(n):\n    return n\n")}); !errors.Is(err, ErrNoStore) {
		t.Errorf("register with no catalog: %v, want %v", err, ErrNoStore)
	}
}

// evalExpr evaluates an expression against an empty ad, which is enough for a call whose arguments
// are literals.
func evalExpr(t *testing.T, src string) string {
	t.Helper()
	expr, err := classad.ParseExpr(src)
	if err != nil {
		t.Fatalf("parse %s: %v", src, err)
	}
	return expr.Eval(classad.New()).String()
}
