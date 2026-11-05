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

	"github.com/PelicanPlatform/classad/wasmudf"
	config "github.com/bbockelm/golang-htcondor/config"
)

// SANDBOXED USER FUNCTIONS, LOADED FROM CONFIGURATION.
//
// classad takes user-defined scalar functions and aggregates, and wasmudf runs one that has been
// compiled to WebAssembly. Neither is reachable by an administrator: both are Go APIs, called by
// the program that owns the process. This manager is that caller -- it turns a path in the config
// file into a function an ordinary query can use.
//
// WHY WEBASSEMBLY IS THE ONLY KIND THIS LOADS. A Go function would have to be compiled into this
// binary, which is not something a site can do. A wasm module is bytes on disk, and the sandbox is
// what makes loading someone else's bytes into the database daemon a reasonable thing to do: the
// host offers a guest exactly one import, so there is no I/O, no clock, no randomness and no WASI
// to reach for -- not as a policy, but as the absence of anything to call. A module asking for more
// cannot be instantiated and is refused at load, naming what it asked for.
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

// Manager owns the loader and the set of names registered from it.
//
// It holds the loader for the life of the daemon rather than closing it after loading, because a
// registered function IS a call into a loaded module: closing the runtime would leave every
// registration pointing at a module that can no longer run, and there is no way to take the
// registrations back.
type Manager struct {
	parent context.Context
	logger *slog.Logger

	mu        sync.Mutex
	loader    *wasmudf.Loader
	loaderSig string            // the sandbox settings the loader was built with
	loaded    map[string]module // by folded name, what is actually registered
	seen      map[string]bool   // reported "needs a restart" already, so a reconfigure loop says it once
}

// module is one registered module, as configured. The settings are kept so a later
// reconfigure can tell "already loaded" from "changed, and cannot be".
type module struct {
	Name      string
	Path      string
	Aggregate bool
	MinArgs   int
	MaxArgs   int
	Trusted   bool
}

// sig is the part of a module's configuration that cannot change without a restart.
func (m module) sig() string {
	return fmt.Sprintf("%s|%v|%d|%d|%v", m.Path, m.Aggregate, m.MinArgs, m.MaxArgs, m.Trusted)
}

type settings struct {
	modules []module
	opts    wasmudf.Options
	optsSig string
}

// resolveSettings reads the configuration. It does not touch the filesystem: a path that does
// not exist is a load error, reported per module, not a reason to reject the whole configuration.
func resolveSettings(cfg *config.Config) (settings, error) {
	fns := strings.Fields(cfgStr(cfg, "HTCONDORDB_WASM_FUNCTIONS"))
	aggs := strings.Fields(cfgStr(cfg, "HTCONDORDB_WASM_AGGREGATES"))
	if len(fns) == 0 && len(aggs) == 0 {
		return settings{}, nil
	}

	var out settings
	seen := map[string]bool{}
	add := func(name string, aggregate bool) error {
		folded := strings.ToLower(name)
		if seen[folded] {
			// One name cannot be both, and loading it twice would fail the second
			// registration with a message about a name collision that says nothing
			// about the config line that caused it.
			return fmt.Errorf("%q is named twice in HTCONDORDB_WASM_FUNCTIONS/HTCONDORDB_WASM_AGGREGATES", name)
		}
		seen[folded] = true
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

	if m.loader == nil {
		l, lerr := wasmudf.NewLoader(m.parent, m.withFailureLog(s.opts))
		if lerr != nil {
			return fmt.Errorf("wasm user functions: %w", lerr)
		}
		m.loader, m.loaderSig = l, s.optsSig
	} else if m.loaderSig != s.optsSig {
		// The sandbox is fixed when the runtime is built, and the modules already loaded
		// into it cannot be moved to another one.
		m.warnOnce("sandbox", "the wasm sandbox settings changed; the running modules keep the "+
			"settings they were loaded with until the daemon restarts")
	}

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
		if lerr := m.load(mod); lerr != nil {
			m.logger.Error("wasm user function not loaded", "name", mod.Name, "path", mod.Path, "err", lerr.Error())
			if firstErr == nil {
				firstErr = lerr
			}
			continue
		}
		m.loaded[folded] = mod
		m.logger.Info("wasm user function registered", "name", mod.Name, "path", mod.Path,
			"aggregate", mod.Aggregate, "trusted", mod.Trusted)
	}
	return firstErr
}

// load reads and registers one module. Reading happens here rather than in resolveSettings so
// that a file appearing between a failed start-up and a reconfigure is picked up.
func (m *Manager) load(mod module) error {
	// os.ReadFile, not one of the safefile wrappers: this is an administrator's own path from
	// the daemon's own configuration, read once at start-up, not a path influenced by a
	// request.
	wasm, err := os.ReadFile(mod.Path)
	if err != nil {
		return err
	}
	lo := wasmudf.LoadOptions{Trusted: mod.Trusted}
	if mod.Aggregate {
		_, err = wasmudf.RegisterAggregateWith(m.parent, m.loader, wasmudf.DBRegistry(), mod.Name, wasm, lo)
	} else {
		_, err = wasmudf.RegisterWith(m.parent, m.loader, wasmudf.ClassAdRegistry(), mod.Name,
			mod.MinArgs, mod.MaxArgs, wasm, lo)
	}
	return err
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
