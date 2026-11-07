package userfunc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/collections/scriptudf"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/wasmudf"
	config "github.com/bbockelm/golang-htcondor/config"
)

// SANDBOXED USER FUNCTIONS, LOADED FROM CONFIGURATION.
//
// classad takes user-defined scalar functions and aggregates. wasmudf runs one that has been
// compiled to WebAssembly; scriptudf runs one written in ClassAd Script. None of that is reachable
// by an administrator: they are Go APIs, called by the program that owns the process. This manager
// is that caller -- it turns a path in the config file into a function an ordinary query can use.
//
// WHY THESE TWO KINDS AND NOT A GO ONE. A Go function has to be compiled into this binary, which is
// not something a site can do. Both kinds here are bytes on disk that a site writes, and each earns
// its way into the database daemon differently.
//
// A WASM MODULE IS SANDBOXED. The host offers a guest exactly one import, so there is no I/O, no
// clock, no randomness and no WASI to reach for -- not as a policy, but as the absence of anything
// to call. A module asking for more cannot be instantiated and is refused at load, naming what it
// asked for. That sandbox is what makes running someone else's compiled bytes reasonable, and the
// knobs that bound it -- memory, steps, a wall-clock deadline, instances -- are the price of it.
//
// A SCRIPT FUNCTION HAS NOTHING TO SANDBOX. The language has no I/O to offer in the first place, and
// registration refuses any function the compiler cannot prove reads nothing but its arguments, so it
// cannot reach the subject ad either. What is left is a step budget, which the library fixes and
// exposes through no public API, so there is no limit here for configuration to set. A `def` also
// states its own arity, so there is no argument count to declare and get wrong. The script surface
// is one list and one path per name, and it is short because there is nothing else to say.
//
// ONE NAME SPACE FOR BOTH, which is why one manager loads both rather than two managers each loading
// their own. A query calling pctUsed reaches one registered function; which kind of file it came
// from is not part of the call. Both kinds therefore share the bookkeeping below, and a name claimed
// by a wasm module cannot also be claimed by a script function.
//
// REGISTRATION IS APPEND-ONLY, WHICH SHAPES EVERYTHING BELOW. classad's registries have no
// unregister: compiling an expression records that a name was a user function, and compiled
// programs are cached and shared, so a name that has been seen cannot stop existing. A reconfigure
// can therefore ADD a function and nothing else. Changing a module's path or arity, or removing it,
// takes a restart -- this manager says so in the log rather than pretending to have applied it,
// because an operator who edits a path, reconfigures, and is told nothing would reasonably believe
// the new module is running.
//
// Config:
//
//	HTCONDORDB_WASM_FUNCTIONS   = pctUsed busyFraction     # scalar functions
//	HTCONDORDB_WASM_AGGREGATES  = p95                      # aggregates
//
//	HTCONDORDB_WASM_PCTUSED_PATH     = /etc/condor/wasm/pctused.wasm   # required
//	HTCONDORDB_WASM_PCTUSED_MIN_ARGS = 2                               # required for a function
//	HTCONDORDB_WASM_PCTUSED_MAX_ARGS = 2                               # default: MIN_ARGS; -1 is variadic
//	HTCONDORDB_WASM_PCTUSED_TRUSTED  = false
//
//	HTCONDORDB_WASM_MEMORY_PAGES  = 128        # 64 KiB pages; 0 takes wasmudf's default
//	HTCONDORDB_WASM_STEPS         = 10000000   # the deterministic step budget
//	HTCONDORDB_WASM_TIMEOUT       = 1          # seconds; the wall-clock backstop
//	HTCONDORDB_WASM_MAX_INSTANCES = 0          # 0 takes GOMAXPROCS
//
//	HTCONDORDB_SCRIPT_FUNCTIONS      = effMemory scoreJob              # scalar functions
//
//	HTCONDORDB_SCRIPT_EFFMEMORY_PATH = /etc/condor/scripts/policy.cas  # required
//	HTCONDORDB_SCRIPT_SCOREJOB_PATH  = /etc/condor/scripts/policy.cas  # one file may back several
//
// The configured name is the name of the `def` to register out of that file, so one file holding
// several definitions backs several functions and is read and parsed once per apply however many
// names point at it.

// Manager owns the wasm loader and the set of names registered from configuration, of either kind.
//
// It holds the loader for the life of the daemon rather than closing it after loading, because a
// registered wasm function IS a call into a loaded module: closing the runtime would leave every
// registration pointing at a module that can no longer run, and there is no way to take the
// registrations back. The loader is built on the first wasm module that needs one and not before,
// so a daemon configured with script functions alone never builds a WebAssembly runtime.
type Manager struct {
	parent context.Context
	logger *slog.Logger

	mu        sync.Mutex
	loader    *wasmudf.Loader
	loaderSig string            // the sandbox settings the loader was built with
	loaded    map[string]module // by folded name, what is actually registered -- both kinds
	seen      map[string]bool   // reported "needs a restart" already, so a reconfigure loop says it once
}

// module is one registered function, as configured. The settings are kept so a later
// reconfigure can tell "already loaded" from "changed, and cannot be".
//
// One struct covers both kinds rather than two, because everything the manager does with one -- the
// name it occupies, the path it came from, whether its configuration has changed since it was
// loaded -- is the same question for both. Script is what the kind-specific fields hang off: a
// script function has none of MinArgs, MaxArgs, Trusted or Aggregate, and they stay zero.
type module struct {
	Name      string
	Path      string
	Script    bool // ClassAd Script source rather than a WebAssembly module
	Aggregate bool
	MinArgs   int
	MaxArgs   int
	Trusted   bool
}

// sig is the part of a module's configuration that cannot change without a restart.
func (m module) sig() string {
	return fmt.Sprintf("%s|%v|%v|%d|%d|%v", m.Path, m.Script, m.Aggregate, m.MinArgs, m.MaxArgs, m.Trusted)
}

// kind names the module in an operator message, where "user function" alone would leave someone
// reading the log unsure which file to go and look at.
func (m module) kind() string {
	if m.Script {
		return "script"
	}
	return "wasm"
}

type settings struct {
	modules []module
	opts    wasmudf.Options
	optsSig string
}

// resolveSettings reads the configuration. It does not touch the filesystem: a path that does
// not exist is a load error, reported per module, not a reason to reject the whole configuration.
func resolveSettings(cfg *config.Config) (settings, error) {
	// Checked before anything else, and checked even when nothing is configured, because the
	// question it answers is "why did the key I set do nothing". HTCONDORDB_SCRIPT_AGGREGATES is
	// the knob an administrator writes by analogy with the wasm pair, and there is no such thing:
	// scriptudf registers scalar functions only. Failing here says that; ignoring the key would
	// leave an aggregate name unregistered and every query using it answering ERROR.
	if strings.TrimSpace(cfgStr(cfg, "HTCONDORDB_SCRIPT_AGGREGATES")) != "" {
		return settings{}, fmt.Errorf("HTCONDORDB_SCRIPT_AGGREGATES is set, but there is no such " +
			"thing as a script aggregate: ClassAd Script defines scalar functions, and an aggregate " +
			"has to be a WebAssembly module named in HTCONDORDB_WASM_AGGREGATES")
	}

	fns := strings.Fields(cfgStr(cfg, "HTCONDORDB_WASM_FUNCTIONS"))
	aggs := strings.Fields(cfgStr(cfg, "HTCONDORDB_WASM_AGGREGATES"))
	scripts := strings.Fields(cfgStr(cfg, "HTCONDORDB_SCRIPT_FUNCTIONS"))
	if len(fns) == 0 && len(aggs) == 0 && len(scripts) == 0 {
		return settings{}, nil
	}

	var out settings
	// One seen set across all three lists, because a name is one function. A client's parser has
	// to decide whether a name is an aggregate call or an ordinary one before any request exists,
	// so a name claimed by a wasm module is not available to a script function or the other way
	// round, whichever registry it would end up in.
	seen := map[string]bool{}
	named := func(name string) error {
		folded := strings.ToLower(name)
		if seen[folded] {
			// One name cannot be two functions, and loading it twice would fail the second
			// registration with a message about a name collision that says nothing
			// about the config line that caused it.
			return fmt.Errorf("%q is named twice across HTCONDORDB_WASM_FUNCTIONS, "+
				"HTCONDORDB_WASM_AGGREGATES and HTCONDORDB_SCRIPT_FUNCTIONS; a name is one function", name)
		}
		seen[folded] = true
		return nil
	}

	// addScript takes only a path. There is no arity to declare -- the `def` states its own -- and
	// no sandbox to relax, so there is nothing else a script function could be configured with.
	addScript := func(name string) error {
		if err := named(name); err != nil {
			return err
		}
		key := "HTCONDORDB_SCRIPT_" + strings.ToUpper(name) + "_PATH"
		path := cfgStr(cfg, key)
		if path == "" {
			return fmt.Errorf("%s is not set, so there is nothing to load for %q", key, name)
		}
		out.modules = append(out.modules, module{Name: name, Path: path, Script: true})
		return nil
	}

	add := func(name string, aggregate bool) error {
		if err := named(name); err != nil {
			return err
		}
		p := "HTCONDORDB_WASM_" + strings.ToUpper(name) + "_"
		m := module{
			Name:      name,
			Path:      cfgStr(cfg, p+"PATH"),
			Aggregate: aggregate,
			Trusted:   cfgBool(cfg, p+"TRUSTED"),
		}
		if m.Path == "" {
			return fmt.Errorf("%s is not set, so there is nothing to load for %q", p+"PATH", name)
		}
		if !aggregate {
			// Arity is REQUIRED rather than defaulted, because the registry checks it
			// before the guest runs. A default that was wider than the module would let
			// a call through to a guest that refuses it, and the operator would see
			// ERROR with nothing naming the arity as the cause.
			minArgs, ok := cfgIntSet(cfg, p+"MIN_ARGS")
			if !ok {
				return fmt.Errorf("%s is not set; a function's argument count has to be declared, "+
					"since it is checked before the module runs", p+"MIN_ARGS")
			}
			m.MinArgs = minArgs
			if maxArgs, set := cfgIntSet(cfg, p+"MAX_ARGS"); set {
				m.MaxArgs = maxArgs
			} else {
				m.MaxArgs = minArgs
			}
		}
		out.modules = append(out.modules, m)
		return nil
	}
	for _, n := range fns {
		if err := add(n, false); err != nil {
			return settings{}, err
		}
	}
	for _, n := range aggs {
		if err := add(n, true); err != nil {
			return settings{}, err
		}
	}
	for _, n := range scripts {
		if err := addScript(n); err != nil {
			return settings{}, err
		}
	}

	out.opts = wasmudf.Options{
		MemoryLimitPages: uint32(cfgInt(cfg, "HTCONDORDB_WASM_MEMORY_PAGES")),
		Steps:            uint64(cfgInt(cfg, "HTCONDORDB_WASM_STEPS")),
		MaxInstances:     cfgInt(cfg, "HTCONDORDB_WASM_MAX_INSTANCES"),
	}
	if secs := cfgInt(cfg, "HTCONDORDB_WASM_TIMEOUT"); secs > 0 {
		out.opts.Timeout = time.Duration(secs) * time.Second
	}
	out.optsSig = fmt.Sprintf("%d|%d|%d|%s", out.opts.MemoryLimitPages, out.opts.Steps,
		out.opts.MaxInstances, out.opts.Timeout)
	return out, nil
}

// apply loads and registers every configured module that is not already loaded.
//
// It is called once at start-up and again on every reconfigure. The first call's errors fail
// start-up, which is deliberate: a function an administrator configured and that did not load is
// not a degraded service but a WRONG one -- an expression calling a name nobody registered
// evaluates to ERROR, and a Requirements expression that is ERROR rejects the machine. Better to
// refuse to start than to serve answers that are quietly wrong.
func (m *Manager) Apply(cfg *config.Config) error {
	s, err := resolveSettings(cfg)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(s.modules) == 0 {
		if len(m.loaded) > 0 {
			m.warnOnce("", "user functions were removed from the configuration but stay registered "+
				"until the daemon restarts; a registration cannot be withdrawn")
		}
		return nil
	}
	if m.loaded == nil {
		m.loaded = map[string]module{}
		m.seen = map[string]bool{}
	}

	if m.loader != nil && m.loaderSig != s.optsSig {
		// The sandbox is fixed when the runtime is built, and the modules already loaded
		// into it cannot be moved to another one.
		m.warnOnce("sandbox", "the wasm sandbox settings changed; the running modules keep the "+
			"settings they were loaded with until the daemon restarts")
	}

	// One parse per distinct file per apply, not one per name: several functions may be registered
	// out of one script file, and reading and parsing it once each would multiply the work and
	// report the same syntax error several times over.
	parsed := map[string]scriptFile{}

	var firstErr error
	for _, mod := range s.modules {
		folded := strings.ToLower(mod.Name)
		if prev, ok := m.loaded[folded]; ok {
			if prev.sig() != mod.sig() {
				m.warnOnce(folded, fmt.Sprintf("the configuration for %q changed, but a "+
					"registered function cannot be replaced; the module loaded at start-up "+
					"is still the one running. Restart to pick up the change.", mod.Name))
			}
			continue
		}
		if lerr := m.load(mod, s, parsed); lerr != nil {
			m.logger.Error(mod.kind()+" user function not loaded", "name", mod.Name, "path", mod.Path,
				"err", lerr.Error())
			if firstErr == nil {
				// Name the function and the file alongside the error. A wasm failure usually
				// carries the path already, but a script refusal names the `def` and not the file
				// it was read from, and start-up fails with this text and nothing else.
				firstErr = fmt.Errorf("%s user function %s (%s): %w", mod.kind(), mod.Name, mod.Path, lerr)
			}
			continue
		}
		m.loaded[folded] = mod
		if mod.Script {
			m.logger.Info("script user function registered", "name", mod.Name, "path", mod.Path)
		} else {
			m.logger.Info("wasm user function registered", "name", mod.Name, "path", mod.Path,
				"aggregate", mod.Aggregate, "trusted", mod.Trusted)
		}
	}
	return firstErr
}

// load reads and registers one module. Reading happens here rather than in resolveSettings so
// that a file appearing between a failed start-up and a reconfigure is picked up.
func (m *Manager) load(mod module, s settings, parsed map[string]scriptFile) error {
	if mod.Script {
		return m.loadScript(mod, parsed)
	}
	// os.ReadFile, not one of the safefile wrappers: this is an administrator's own path from
	// the daemon's own configuration, read once at start-up, not a path influenced by a
	// request.
	wasm, err := os.ReadFile(mod.Path)
	if err != nil {
		return err
	}
	loader, err := m.wasmLoader(s)
	if err != nil {
		return err
	}
	lo := wasmudf.LoadOptions{Trusted: mod.Trusted}
	if mod.Aggregate {
		_, err = wasmudf.RegisterAggregateWith(m.parent, loader, wasmudf.DBRegistry(), mod.Name, wasm, lo)
	} else {
		_, err = wasmudf.RegisterWith(m.parent, loader, wasmudf.ClassAdRegistry(), mod.Name,
			mod.MinArgs, mod.MaxArgs, wasm, lo)
	}
	return err
}

// scriptFile is one script file's parse, kept whichever way it went. The failure is cached as well
// as the success so that two names pointing at a file with a syntax error read it once and fail
// identically, rather than reporting the same error twice with different line numbers' worth of
// work behind each.
type scriptFile struct {
	mod *vm.ScriptModule
	err error
}

// loadScript registers one `def` out of a script file.
//
// There is nothing to pass beyond the name. The arity comes from the `def`, and purity is decided by
// the compiler: a function that reaches the subject ad is refused with vm.ErrReadsAd and never
// reaches the registry, which matters because a registration cannot be withdrawn. That refusal is
// returned exactly as the library phrased it -- it names the function and the attribute it reached
// for, and says to pass that value as a parameter instead, which is the whole of the fix.
func (m *Manager) loadScript(mod module, parsed map[string]scriptFile) error {
	sf, ok := parsed[mod.Path]
	if !ok {
		// Same os.ReadFile, for the same reason as above.
		src, err := os.ReadFile(mod.Path)
		if err != nil {
			sf = scriptFile{err: err}
		} else {
			sf.mod, sf.err = vm.ParseScriptModule(string(src))
		}
		parsed[mod.Path] = sf
	}
	if sf.err != nil {
		return sf.err
	}
	_, err := scriptudf.Register(sf.mod, scriptudf.ClassAdRegistry(), mod.Name)
	return err
}

// wasmLoader returns the WebAssembly runtime, building it on first use.
//
// On FIRST USE rather than when the manager is created, and not at all when nothing wasm is
// configured: a daemon whose user functions are all scripts has no business holding a WebAssembly
// runtime, and building one would cost memory and goroutines for a guest that never arrives.
func (m *Manager) wasmLoader(s settings) (*wasmudf.Loader, error) {
	if m.loader != nil {
		return m.loader, nil
	}
	l, err := wasmudf.NewLoader(m.parent, m.withFailureLog(s.opts))
	if err != nil {
		return nil, fmt.Errorf("building the wasm runtime: %w", err)
	}
	m.loader, m.loaderSig = l, s.optsSig
	return l, nil
}

// withFailureLog attaches the per-call failure callback, if the caller has not set one.
//
// A guest that fails every call is otherwise INVISIBLE: each failure is the ClassAd value ERROR,
// which is an ordinary value, so a Requirements expression using the function simply stops matching
// and nothing is logged anywhere. The first failure of each kind is worth a line.
func (m *Manager) withFailureLog(o wasmudf.Options) wasmudf.Options {
	if o.OnFailure != nil {
		return o
	}
	var mu sync.Mutex
	reported := map[string]bool{}
	o.OnFailure = func(name string, r wasmudf.Reason, err error) {
		key := name + "/" + strconv.Itoa(int(r))
		mu.Lock()
		first := !reported[key]
		reported[key] = true
		mu.Unlock()
		if !first {
			return
		}
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		m.logger.Warn("wasm user function failed a call; further failures of this kind are not logged",
			"name", name, "reason", r.String(), "err", msg)
	}
	return o
}

// warnOnce logs a message about configuration that cannot be applied, once per key, so that a
// reconfigure every few minutes does not repeat it forever.
func (m *Manager) warnOnce(key, msg string) {
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	if m.seen[key] {
		return
	}
	m.seen[key] = true
	m.logger.Warn(msg)
}

// Names returns the registered names, for operator messages. The order is unspecified.
func (m *Manager) Names() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.loaded))
	for _, mod := range m.loaded {
		out = append(out, mod.Name)
	}
	return out
}

// Close releases the runtime. It is only correct at shutdown: see the note on the loader field.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loader == nil {
		return
	}
	if err := m.loader.Close(m.parent); err != nil && !errors.Is(err, context.Canceled) {
		m.logger.Error("closing the wasm runtime", "err", err.Error())
	}
	m.loader = nil
}

// cfgIntSet reads an integer, reporting whether the key was set at all. configInt cannot: it returns 0
// both for "zero" and for "absent", and the difference matters for an argument count.
func cfgIntSet(cfg *config.Config, key string) (int, bool) {
	v := strings.TrimSpace(cfgStr(cfg, key))
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

// New returns a manager that has loaded nothing yet. parent bounds the lifetime of the wasm
// runtime, so it should be the process's context rather than a request's.
func New(parent context.Context, logger *slog.Logger) *Manager {
	return &Manager{parent: parent, logger: logger}
}

func cfgStr(cfg *config.Config, key string) string {
	v, _ := cfg.Get(key)
	return v
}

func cfgInt(cfg *config.Config, key string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(cfgStr(cfg, key)))
	return n
}

func cfgBool(cfg *config.Config, key string) bool {
	switch strings.ToLower(strings.TrimSpace(cfgStr(cfg, key))) {
	case "true", "t", "yes", "y", "1":
		return true
	}
	return false
}
