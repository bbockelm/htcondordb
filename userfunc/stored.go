package userfunc

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/wasmudf"
)

// USER FUNCTIONS STORED IN THE DATABASE.
//
// The other half of this manager turns a path in the configuration into a registered function.
// This half turns a RECORD IN A TABLE into one, so a site can add a function without an edit on
// every host and without filesystem access to the daemon's config directory.
//
// ONE NAME SPACE, ONE MANAGER. A query calling pctUsed reaches one registered function, and where
// the bytes came from is not part of the call. So the stored path lives here, beside the
// configured one, and every claim on a name goes through the same bookkeeping -- a name a
// configured module holds is not available to a stored record, and the other way round. Two
// managers would each believe the name was theirs, and the second registration would fail with a
// message about a collision that named neither.
//
// THE REGISTRY IS APPEND-ONLY, SO A STORED FUNCTION IS REGISTERED ONCE AND THEN REDIRECTED.
// classad's scalar registry has no unregister, because compiling an expression records that a
// name is a user function and compiled programs are cached and shared. A configured function can
// live with that: changing one means editing a file and restarting. A stored one cannot -- a
// function an administrator can add but never correct is barely better than the config file it
// replaces.
//
// So a stored name is registered against a TRAMPOLINE: an indirection this manager owns, holding
// an atomic pointer to the implementation of the moment. The decision a compiled program captured
// -- this name is a user function taking N arguments -- stays true forever. What a replacement
// changes is only what sits behind the pointer, which nothing has cached.
//
// That makes one rule sharp, and it is the rule an administrator meets:
//
//	ARITY IS THE ONLY IMMUTABLE PART. A stored function may be replaced in place as long as its
//	argument count does not change. Changing the argument count means a new name, or a restart.
//
// It is not a restriction chosen for tidiness: the argument count is precisely what the compiled
// call site captured, so it is the one thing a replacement cannot move without making a cached
// program wrong.
//
// A MODULE THAT DOES NOT LOAD MARKS THE FUNCTION BAD. IT DOES NOT STOP THE DAEMON. This is where
// the stored path deliberately differs from the configured one, where a module that fails to load
// fails start-up (see Apply). The difference is not an inconsistency, and three things make it the
// right way round:
//
//   - A configured module is a file on the host, editable by whoever is about to restart the
//     daemon. A stored one is not. A daemon that refuses to start cannot be used to fix the record
//     that stops it starting, so the repair path would run through the thing that is broken.
//   - A module can stop loading without anyone touching it: a wasm guest valid under one runtime
//     version and rejected by the next is the obvious case. Tying start-up to that makes an
//     upgrade a coin toss.
//   - Otherwise anyone who may register a function could stop the daemon starting, which turns an
//     administrative privilege into an availability one.
//
// A BAD FUNCTION IS REGISTERED, under its stored arity, pointing at an implementation that answers
// ERROR. It is NOT left unregistered. Leaving the name out would change how the CALL SITE compiles
// -- an unregistered name compiles to something else entirely -- and that decision is the one that
// cannot be taken back. Registering it keeps the name's shape fixed and puts the failure in the
// value, where a query can see it, which is what calling a name that does not exist already gives.
//
// And bad is a STATE, NOT A SILENCE: it is logged at start-up, written into the function's own
// record, and reported by the listing. An operator should be able to find out that pctUsed is bad
// without first noticing that a column has gone to ERROR.
//
// TRUST IS NEVER STORED. A trusted wasm module runs with no wall-clock deadline and cannot be
// interrupted, which is a capability an operator grants a file they pointed at. A stored module is
// exactly a module that can ask for things, and a capability a module can claim is a request
// rather than a capability -- so a record carrying a trust attribute is loaded untrusted, and the
// attribute is reported as ignored rather than quietly honoured. What bounds a stored module is
// then what bounds an untrusted configured one: the step budget, the wall-clock deadline, the
// memory cap, and -- for a script -- the purity gate.
//
// WHO MAY WRITE THE TABLE. Registering a name is an administrative act: the function name space is
// global, every query that names a function runs it, and a name that has been claimed cannot be
// given back. The daemon therefore OWNS this table (an in-process writer, like a mirror's), so the
// records change only through the administrative command and never through an ordinary WRITE-level
// session -- a record written by an analyst would be registered by the next restart, which is the
// name space claimed at WRITE by another route.

// Table is the table a stored function's record lives in, one record per function, keyed by the
// folded function name -- the name as a query would spell it, since that is what a lookup has.
const Table = "user_functions"

// The attributes of a stored function's record.
const (
	AttrName     = "Name"     // the name as registered, in the spelling the administrator chose
	AttrKind     = "Kind"     // KindScript or KindWasm
	AttrSource   = "Source"   // ClassAd Script text, or base64-encoded wasm bytes
	AttrArity    = "Arity"    // the argument count; the part that cannot change
	AttrMaxArity = "MaxArity" // the largest accepted count; absent means exactly Arity, -1 variadic
	// AttrChecksum is the instance identity: a checksum over the source, computed by the daemon.
	//
	// A name is not enough to explain a result, because the thing behind a name changes -- two
	// queries seconds apart can run different code under one word. The checksum names the code
	// that actually answered, and it means the same thing on two daemons loaded separately, which
	// a generated id would not.
	AttrChecksum = "Checksum"
	// Provenance: who claimed the name and when. Not authorization -- the command that wrote the
	// record did that -- but the answer to "where did this come from", which is the question an
	// operator has when a function nobody remembers turns up in the listing.
	AttrRegisteredBy = "RegisteredBy"
	AttrRegisteredAt = "RegisteredAt"
	// AttrState and AttrStateReason are the record's half of "bad is a state, not a silence":
	// what happened the last time the daemon tried to load this record, written back by the
	// daemon that tried.
	AttrState       = "State"
	AttrStateReason = "StateReason"
	AttrStateAt     = "StateAt"
)

// The kinds of stored function. They are the two kinds the configured path loads, minus the
// aggregate: see Register for why a stored aggregate is refused rather than registered.
const (
	KindScript = "script"
	KindWasm   = "wasm"
)

// The states a stored function can be in.
const (
	// StateOK: the source loaded and the name answers it.
	StateOK = "ok"
	// StateBad: the name is registered, under its stored arity, and answers ERROR. The reason
	// says why, and is the thing to fix.
	StateBad = "bad"
	// StateRemoved: the record is gone but the NAME IS NOT FREE until the daemon restarts, because
	// a registration cannot be withdrawn. It stays in the listing saying exactly that, rather than
	// disappearing and leaving an administrator to discover by experiment that the name is taken.
	StateRemoved = "removed"
	// StateInvalid: the record does not say what to register -- no usable name, kind, or argument
	// count. Nothing is registered for it, because there is no arity to register it under, and a
	// record that never loaded has never changed how a call site compiles. It is listed and logged
	// so it is not a silence either.
	StateInvalid = "invalid"
)

// errArityChanged is what a replacement that moves the argument count fails with. A sentinel, so
// the administrative path can phrase the refusal and a test can name the guard.
var errArityChanged = errors.New("a stored function's argument count cannot change")

// ErrNoStore means this daemon has no stored-function table to work with (nothing has opened one).
var ErrNoStore = errors.New("stored user functions are not available on this daemon")

// Record is one stored function as the table holds it.
//
// Arity is CARRIED rather than derived, including for a script, whose `def` states its own. The
// reason is the bad case: a record whose source will not parse still has to be registered under
// its argument count, and a source that will not parse cannot be asked what that count is. So the
// count is written down at registration, when the source did parse, and the load then checks the
// source against it.
type Record struct {
	Name         string
	Kind         string
	Source       string
	Arity        int
	MaxArity     int
	Checksum     string
	RegisteredBy string
	RegisteredAt int64
	State        string
	StateReason  string
	StateAt      int64

	// TrustRequested records that the record asked to be trusted. It is never honoured; it is
	// kept so the answer can say the attribute was ignored instead of silently dropping it.
	TrustRequested bool
}

// sourceBytes returns the source as the loader consumes it: the decoded module bytes for wasm,
// the text itself for a script.
//
// The checksum is taken over THESE bytes rather than over the stored string, so that a module
// re-encoded on its way into the table is still the same function. The identity is of the code,
// not of the record that carries it.
func (r Record) sourceBytes() ([]byte, error) {
	if r.Kind != KindWasm {
		return []byte(r.Source), nil
	}
	// Strict standard encoding: a module that arrived with whitespace in it has been handled by
	// something that does not know it is carrying bytes, and decoding it leniently would hide
	// that from whoever has to explain a checksum that moved.
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(r.Source))
	if err != nil {
		return nil, fmt.Errorf("the source is not valid base64-encoded WebAssembly: %w", err)
	}
	return b, nil
}

// checksumOf is the instance identity of some source bytes: "sha256:<hex>".
//
// The prefix is there so the identity can outlive this hash. A bare hex string gives a later
// reader nothing to notice a change by, and a stored record is read by daemons that were built at
// different times.
func checksumOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// maxArity returns the largest accepted argument count, defaulting to exactly Arity.
func (r Record) maxArity() int {
	if r.MaxArity == 0 && r.Arity != 0 {
		return r.Arity
	}
	return r.MaxArity
}

// Ad renders a record as the ad the table holds.
func (r Record) Ad() *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString(AttrName, r.Name)
	ad.InsertAttrString(AttrKind, r.Kind)
	ad.InsertAttrString(AttrSource, r.Source)
	ad.InsertAttr(AttrArity, int64(r.Arity))
	ad.InsertAttr(AttrMaxArity, int64(r.maxArity()))
	ad.InsertAttrString(AttrChecksum, r.Checksum)
	if r.RegisteredBy != "" {
		ad.InsertAttrString(AttrRegisteredBy, r.RegisteredBy)
	}
	if r.RegisteredAt != 0 {
		ad.InsertAttr(AttrRegisteredAt, r.RegisteredAt)
	}
	if r.State != "" {
		ad.InsertAttrString(AttrState, r.State)
	}
	if r.StateReason != "" {
		ad.InsertAttrString(AttrStateReason, r.StateReason)
	}
	if r.StateAt != 0 {
		ad.InsertAttr(AttrStateAt, r.StateAt)
	}
	return ad
}

// recordFromAd reads a record out of the table. key is the record's key, used for diagnostics when
// the ad does not even name itself.
func recordFromAd(key string, ad *classad.ClassAd) Record {
	r := Record{}
	r.Name, _ = ad.EvaluateAttrString(AttrName)
	if r.Name == "" {
		r.Name = key
	}
	r.Kind, _ = ad.EvaluateAttrString(AttrKind)
	r.Source, _ = ad.EvaluateAttrString(AttrSource)
	if n, ok := ad.EvaluateAttrInt(AttrArity); ok {
		r.Arity = int(n)
	} else {
		// Marked impossible rather than zero: zero is a legitimate argument count, and a record
		// that does not state one has to be distinguishable from one that states none.
		r.Arity = -1
	}
	if n, ok := ad.EvaluateAttrInt(AttrMaxArity); ok {
		r.MaxArity = int(n)
	} else {
		r.MaxArity = r.Arity
	}
	r.Checksum, _ = ad.EvaluateAttrString(AttrChecksum)
	r.RegisteredBy, _ = ad.EvaluateAttrString(AttrRegisteredBy)
	r.RegisteredAt, _ = ad.EvaluateAttrInt(AttrRegisteredAt)
	r.State, _ = ad.EvaluateAttrString(AttrState)
	r.StateReason, _ = ad.EvaluateAttrString(AttrStateReason)
	r.StateAt, _ = ad.EvaluateAttrInt(AttrStateAt)
	// Any way of asking for trust is one way of asking for trust. The attribute is read only so
	// that the answer can say it was ignored.
	if b, ok := ad.EvaluateAttrBool("Trusted"); ok && b {
		r.TrustRequested = true
	}
	return r
}

// registry is classad's scalar registry, as the stored path uses it.
//
// An interface for the reason wasmudf and scriptudf declare one: the registry is process-wide and
// append-only, so a test cannot undo a registration in it, and a test of what gets registered --
// which is most of what this file decides -- would otherwise be a test that may be run once. The
// daemon always has the real one.
type registry interface {
	RegisterFunction(name string, minArgs, maxArgs int, fn func(args []classad.Value) classad.Value) error
}

type classAdRegistry struct{}

func (classAdRegistry) RegisterFunction(name string, minArgs, maxArgs int, fn func(args []classad.Value) classad.Value) error {
	return classad.RegisterFunction(name, minArgs, maxArgs, fn)
}

// registryOrDefault is the registry to register into: the process-wide one unless a test supplied
// another. Called with the lock held.
func (m *Manager) registryOrDefault() registry {
	if m.reg != nil {
		return m.reg
	}
	return classAdRegistry{}
}

// implementation is what sits behind a trampoline: the function of the moment, and the identity of
// the code it runs.
type implementation struct {
	fn       classad.Func
	checksum string
	// udf is the loaded module a wasm implementation came from, nil for a script. Kept so that
	// what is running can be INSPECTED and not only called -- its trust, its per-call statistics
	// -- which a bare func value cannot answer for.
	udf *wasmudf.UDF
	// reason, when set, is why this implementation answers ERROR: the function is bad, or its
	// record has been removed. It is carried with the implementation rather than beside it so that
	// a call and a listing cannot disagree about which code is running.
	reason string
}

// trampoline is the indirection a stored name is registered against. Registered once, for the life
// of the process; what it points at is what changes.
type trampoline struct {
	name             string
	minArgs, maxArgs int
	impl             atomic.Pointer[implementation]
}

// call is what classad's registry holds. It is on the per-record path of every query that names
// the function, so it does one atomic load and no locking.
func (t *trampoline) call(args []classad.Value) classad.Value {
	cur := t.impl.Load()
	if cur == nil || cur.fn == nil {
		return classad.NewErrorValue()
	}
	return cur.fn(args)
}

// install points the trampoline at a new implementation, refusing one whose argument count differs
// from the registered one.
//
// THIS IS WHERE ARITY IS ENFORCED, and it is the only place. Every path that changes what a name
// runs -- the start-up load, a replacement, a removal -- goes through here, so there is one guard
// rather than one per caller, and the guard sits next to the registered numbers it is comparing
// against rather than next to a caller's idea of them.
func (t *trampoline) install(impl *implementation, minArgs, maxArgs int) error {
	if minArgs != t.minArgs || maxArgs != t.maxArgs {
		return fmt.Errorf("%w: %s is registered taking %s and the replacement takes %s; "+
			"a compiled query has already captured the old count, so this needs a different name "+
			"(or a daemon restart, after which the name is registered afresh)",
			errArityChanged, t.name, arityText(t.minArgs, t.maxArgs), arityText(minArgs, maxArgs))
	}
	t.impl.Store(impl)
	return nil
}

// arityText phrases an argument count for an operator.
func arityText(minArgs, maxArgs int) string {
	switch {
	case maxArgs < 0:
		return fmt.Sprintf("%d or more arguments", minArgs)
	case maxArgs == minArgs:
		return fmt.Sprintf("%d argument(s)", minArgs)
	default:
		return fmt.Sprintf("%d to %d arguments", minArgs, maxArgs)
	}
}

// storedFn is one stored function as this manager holds it: the trampoline the name is registered
// against, and the record that last described it.
type storedFn struct {
	tramp *trampoline
	rec   Record
	// state and reason are the live state, which is what the listing reports. The record carries
	// the same pair, written back so it survives a restart; this copy is what a listing can report
	// without reading the table.
	state  string
	reason string
}

// Status is one line of the listing: what is registered, in what state, running which code.
type Status struct {
	Name     string
	Kind     string
	Origin   string // "stored" or "configured"
	Arity    int
	MaxArity int
	Checksum string // a stored function's instance identity
	Path     string // a configured function's file
	State    string
	Reason   string

	RegisteredBy string
	RegisteredAt int64
}

// Catalog is the part of the database catalog the stored path needs: open the table if it is
// there, and create it when a function is first registered.
//
// An interface, and a narrow one, because this package is loaded by the client-side tools as well
// as the daemon and has no business holding a whole catalog's API. It also lets a test hand the
// manager a catalog it built.
type Catalog interface {
	Table(name string) (*db.DB, bool)
	CreateTable(name string) (*db.DB, error)
}

// LoadStored registers every function the table holds. It is called once, at start-up, after
// Apply, so that a configured name is claimed before a stored record can contend for it.
//
// NOTHING HERE FAILS START-UP. A record that will not load marks its function bad and the daemon
// carries on; see the note at the top of this file for why the stored path and the configured one
// differ on exactly this point. The failure is in the log, in the record, and in the listing.
//
// It keeps cat for the administrative path, which creates the table on first use: a daemon that
// has never had a stored function has no business holding an empty table.
func (m *Manager) LoadStored(cat Catalog) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cat = cat
	if cat == nil {
		return
	}
	tbl, ok := cat.Table(Table)
	if !ok {
		return // nothing has ever been registered here
	}
	m.store = tbl
	if m.stored == nil {
		m.stored = map[string]*storedFn{}
	}

	keys := tbl.Keys()
	// Sorted, so the start-up log reads the same way on every daemon holding the same records and
	// two daemons contending for one name resolve it identically.
	sort.Strings(keys)
	for _, key := range keys {
		if db.IsSystemKey(key) {
			continue
		}
		ad, ok := tbl.LookupClassAd(key)
		if !ok {
			continue
		}
		m.loadStoredRecord(key, recordFromAd(key, ad))
	}
}

// loadStoredRecord registers one stored record. Called with the lock held.
func (m *Manager) loadStoredRecord(key string, rec Record) {
	folded := strings.ToLower(rec.Name)

	// A record that does not describe a registration is the one case with nothing to register:
	// there is no argument count to claim the name under. Registering it under a guessed count
	// would fix the wrong shape for the life of the process, which is the one decision that cannot
	// be revisited.
	if err := rec.validShape(); err != nil {
		m.logger.Error("stored user function record is unusable", "key", key, "name", rec.Name,
			"err", err.Error())
		m.stored[folded] = &storedFn{rec: rec, state: StateInvalid, reason: err.Error()}
		m.markState(folded, StateInvalid, err.Error())
		return
	}

	// The configured path claimed first, so a collision here is a stored record asking for a name
	// an operator's own configuration holds. The configured function keeps the name -- an operator
	// editing config files on the host is the more authoritative claim, and it is also the one
	// already registered by the time this runs -- and the stored one is bad, saying so.
	if prev, ok := m.loaded[folded]; ok {
		why := fmt.Sprintf("the name is already a configured %s user function (%s), and one name is one function",
			prev.kind(), prev.Path)
		m.logger.Error("stored user function not registered", "name", rec.Name, "err", why)
		m.stored[folded] = &storedFn{rec: rec, state: StateBad, reason: why}
		m.markState(folded, StateBad, why)
		return
	}

	t := &trampoline{name: rec.Name, minArgs: rec.Arity, maxArgs: rec.maxArity()}
	// The name is registered BEFORE the source is compiled, and whether or not the source
	// compiles. That is the point of a bad function being a state: the call site's shape is fixed
	// here, once, and the outcome of the load lands in the value instead.
	if err := m.registryOrDefault().RegisterFunction(rec.Name, t.minArgs, t.maxArgs, t.call); err != nil {
		why := fmt.Sprintf("the name could not be registered: %v", err)
		m.logger.Error("stored user function not registered", "name", rec.Name, "err", err.Error())
		m.stored[folded] = &storedFn{rec: rec, state: StateBad, reason: why}
		m.markState(folded, StateBad, why)
		return
	}
	m.stored[folded] = &storedFn{tramp: t, rec: rec}

	impl, err := m.compile(rec)
	if err != nil {
		m.badLocked(folded, err)
		return
	}
	if err := t.install(impl, rec.Arity, rec.maxArity()); err != nil {
		// Unreachable from here, since the trampoline was just built from this record's own
		// numbers -- but install is the one arity gate, and a load that quietly skipped it would
		// be the way a future caller gets around it.
		m.badLocked(folded, err)
		return
	}
	m.stored[folded].state = StateOK
	m.stored[folded].reason = ""
	m.logger.Info("stored user function registered", "name", rec.Name, "kind", rec.Kind,
		"arity", arityText(t.minArgs, t.maxArgs), "checksum", impl.checksum)
	if rec.TrustRequested {
		m.logger.Warn("stored user function asked to be trusted and is loaded untrusted; "+
			"trust is configuration, not something a stored module can claim", "name", rec.Name)
	}
	if rec.State != StateOK {
		m.markState(folded, StateOK, "")
	}
}

// badLocked marks a registered stored function bad: in the log, in the live state the listing
// reports, in the record, and in the value a call produces. Called with the lock held.
func (m *Manager) badLocked(folded string, err error) {
	sf := m.stored[folded]
	reason := err.Error()
	sf.state, sf.reason = StateBad, reason
	// Pointed at an erroring implementation EXPLICITLY rather than left pointing at nothing. The
	// nil case answers ERROR too, but a bad function and a function whose implementation was never
	// installed are different situations, and only one of them has a reason to report.
	if sf.tramp != nil {
		sf.tramp.impl.Store(&implementation{checksum: sf.rec.Checksum, reason: reason})
	}
	m.logger.Error("stored user function is BAD and answers ERROR when called; the name stays "+
		"registered so queries using it keep their shape", "name", sf.rec.Name, "err", reason)
	m.markState(folded, StateBad, reason)
}

// validShape reports whether the record says enough to register a name at all: everything
// validDescription covers, plus the argument count the name would be registered under.
func (r Record) validShape() error {
	if err := r.validDescription(); err != nil {
		return err
	}
	if r.Arity < 0 {
		return errors.New("the record does not state an argument count, and the count is what the " +
			"name has to be registered under")
	}
	if mx := r.maxArity(); mx >= 0 && mx < r.Arity {
		return fmt.Errorf("the argument count %d..%d is impossible", r.Arity, mx)
	}
	return nil
}

// validDescription reports whether the record describes a function at all, leaving the argument
// count out of it. The administrative path checks this before it works out the count, since for a
// script the count comes from the source and a source that will not parse has no count to give.
func (r Record) validDescription() error {
	if !validName(r.Name) {
		return fmt.Errorf("%q is not a name an expression can call", r.Name)
	}
	if classad.BuiltinFunctionName(r.Name) {
		return fmt.Errorf("%q is a built-in function, which a user function may not shadow", r.Name)
	}
	switch r.Kind {
	case KindScript, KindWasm:
	case "":
		return fmt.Errorf("the record does not say what kind of function this is (%s or %s)", KindScript, KindWasm)
	case "aggregate":
		// Worth its own message, because an administrator reaching for this is reasoning by
		// analogy with the configured surface, where an aggregate is a module like any other.
		// An aggregate name is resolved once per query rather than captured by a call site, so
		// replacing one would be perfectly well defined -- but the aggregate registry has no
		// replace to do it with, and a stored aggregate that could never be corrected would be
		// the configuration file again with more moving parts.
		return errors.New("an aggregate cannot be stored: the aggregate registry has no way to " +
			"replace one, so a stored aggregate could never be corrected. Configure it with " +
			"HTCONDORDB_WASM_AGGREGATES")
	default:
		return fmt.Errorf("%q is not a kind of user function (%s or %s)", r.Kind, KindScript, KindWasm)
	}
	if strings.TrimSpace(r.Source) == "" {
		return errors.New("the record carries no source")
	}
	return nil
}

// validName mirrors what classad's registry will accept, so a bad name is refused with the reason
// rather than as a registration failure further down. The registry's own check is not exported,
// and duplicating four lines is better than registering to find out.
func validName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// compile turns a record into a callable implementation. It registers nothing: what is callable
// and what is registered are separate steps, which is what lets a replacement change the first
// without touching the second.
//
// The checksum is verified here, against the source the record carries. A record whose source no
// longer matches its stated identity has been damaged or changed by something that did not go
// through the administrative path, and running it would attribute an answer to code that is not
// the code that produced it. Called with the lock held.
func (m *Manager) compile(rec Record) (*implementation, error) {
	src, err := rec.sourceBytes()
	if err != nil {
		return nil, err
	}
	sum := checksumOf(src)
	if rec.Checksum != "" && rec.Checksum != sum {
		return nil, fmt.Errorf("the stored source does not match its recorded checksum "+
			"(record says %s, source is %s): the record has been damaged or changed outside the "+
			"administrative path", rec.Checksum, sum)
	}
	switch rec.Kind {
	case KindScript:
		return m.compileScript(rec, string(src), sum)
	default:
		return m.compileWasm(rec, src, sum)
	}
}

// compileScript compiles one `def` out of stored script text.
//
// The purity gate applies exactly as it does to a configured script: a function the compiler
// cannot prove reads nothing but its arguments is refused, with the attribute it reached for
// named. That refusal is the library's own words, which say what to do about it.
func (m *Manager) compileScript(rec Record, text, sum string) (*implementation, error) {
	mod, err := vm.ParseScriptModule(text)
	if err != nil {
		return nil, err
	}
	f, err := mod.Compile(rec.Name)
	if err != nil {
		return nil, err
	}
	// A `def` states its own argument count, and the record states one too, because a source that
	// will not parse still has to be registered under a count. When they disagree the SOURCE is
	// wrong, not the record: the record is what the name was registered under and what a compiled
	// query captured.
	if f.Arity() != rec.Arity || rec.maxArity() != rec.Arity {
		return nil, fmt.Errorf("the record registers %s under %s but its def takes %d: %w",
			rec.Name, arityText(rec.Arity, rec.maxArity()), f.Arity(), errArityChanged)
	}
	return &implementation{fn: f.Func(), checksum: sum}, nil
}

// compileWasm loads stored module bytes.
//
// The LOADER KEY carries the checksum, not just the name. The loader refuses to load one name
// twice, which is right for a configured module (loaded once, at start-up) and wrong for a stored
// one, whose whole point is that a second module can take the first one's place. Keying by
// instance keeps both loadable; the name a query calls is the trampoline's, which this does not
// touch.
//
// LoadOptions carries no Trusted, here or anywhere else in this file. That is the enforcement of
// trust not being storable: there is no path from a record to that field.
func (m *Manager) compileWasm(rec Record, wasm []byte, sum string) (*implementation, error) {
	loader, err := m.wasmLoader()
	if err != nil {
		return nil, err
	}
	key := rec.Name + "@" + strings.TrimPrefix(sum, "sha256:")[:12]
	u, err := loader.LoadWith(m.parent, key, rec.Arity, rec.maxArity(), wasm, wasmudf.LoadOptions{})
	if err != nil {
		return nil, err
	}
	return &implementation{fn: u.Func(), checksum: sum, udf: u}, nil
}

// markState writes the state back to the record, so that "this function is bad" survives the
// daemon that discovered it and an operator reading the table sees what an operator reading the
// log sees. Called with the lock held.
//
// A failure to write is logged and nothing more. The state is already in the log and the listing,
// and a daemon that cannot write this table -- a read-only replica, a table another writer holds
// -- must still serve.
func (m *Manager) markState(folded, state, reason string) {
	if m.store == nil {
		return
	}
	sf := m.stored[folded]
	if sf == nil {
		return
	}
	if sf.rec.State == state && sf.rec.StateReason == reason {
		return // already says this; a restart that finds nothing changed writes nothing
	}
	ad, ok := m.store.LookupClassAd(folded)
	if !ok {
		return
	}
	ad.InsertAttrString(AttrState, state)
	ad.InsertAttrString(AttrStateReason, reason)
	ad.InsertAttr(AttrStateAt, time.Now().Unix())
	tx := m.store.Begin()
	tx.NewClassAd(folded, ad)
	if err := tx.Commit(); err != nil {
		m.logger.Warn("could not record a stored user function's state in its record",
			"name", sf.rec.Name, "state", state, "err", err.Error())
		return
	}
	sf.rec.State, sf.rec.StateReason = state, reason
}

// Register claims a new name for a stored function.
//
// The source is COMPILED FIRST and the name claimed second, which is the opposite way round from
// the start-up load and for a reason that only applies here: at start-up the name was claimed by
// an earlier run and the daemon has to keep its shape, while here nothing is promised yet. A
// module that does not load is therefore refused outright, and the administrator fixes it before
// any query has seen the name -- which is strictly better than claiming a name for an
// implementation that errors.
//
// It returns the record as stored, checksum and provenance filled in.
func (m *Manager) Register(rec Record) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	folded := strings.ToLower(rec.Name)
	if err := m.claimable(folded, rec); err != nil {
		return Record{}, err
	}
	if sf, ok := m.stored[folded]; ok {
		return Record{}, fmt.Errorf("%q is already a stored user function (%s); replace it instead",
			sf.rec.Name, sf.rec.Checksum)
	}
	rec, impl, err := m.prepare(rec)
	if err != nil {
		return Record{}, err
	}
	store, err := m.openStore()
	if err != nil {
		return Record{}, err
	}

	t := &trampoline{name: rec.Name, minArgs: rec.Arity, maxArgs: rec.maxArity()}
	if err := m.registryOrDefault().RegisterFunction(rec.Name, t.minArgs, t.maxArgs, t.call); err != nil {
		return Record{}, fmt.Errorf("registering %s: %w", rec.Name, err)
	}
	if err := t.install(impl, rec.Arity, rec.maxArity()); err != nil {
		return Record{}, err
	}
	// The record is written AFTER the registration succeeds. A record for a name this process
	// could not register would be registered by the next restart, and the failure that stopped it
	// here -- a name a built-in or another registry already holds -- is not one a restart fixes.
	rec.State, rec.StateReason, rec.StateAt = StateOK, "", time.Now().Unix()
	tx := store.Begin()
	tx.NewClassAd(folded, rec.Ad())
	if err := tx.Commit(); err != nil {
		// The name is registered and answering in this process, and cannot be withdrawn. Say so,
		// rather than reporting a clean failure that leaves the name quietly claimed.
		m.stored[folded] = &storedFn{tramp: t, rec: rec, state: StateOK}
		return Record{}, fmt.Errorf("%s is registered in this daemon but its record could not be "+
			"written, so it will not survive a restart: %w", rec.Name, err)
	}
	m.stored[folded] = &storedFn{tramp: t, rec: rec, state: StateOK}
	m.logger.Info("stored user function registered", "name", rec.Name, "kind", rec.Kind,
		"arity", arityText(t.minArgs, t.maxArgs), "checksum", rec.Checksum, "by", rec.RegisteredBy)
	return rec, nil
}

// Replace points an existing stored function at new source, in place.
//
// This is what the trampoline is for, and the argument count is what it cannot absorb. It is also
// the repair path for a bad function: new source that loads clears the state. Source that does not
// load changes nothing -- the record and whatever is running stay as they were -- and the error
// says why, which is the difference between "fix the source" and "this runtime will not have it".
func (m *Manager) Replace(rec Record) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	folded := strings.ToLower(rec.Name)
	sf, ok := m.stored[folded]
	if !ok {
		return Record{}, fmt.Errorf("%q is not a stored user function here; register it first", rec.Name)
	}
	if sf.tramp == nil {
		return Record{}, fmt.Errorf("%q was never registered in this daemon (%s), so there is "+
			"nothing to replace in place; fix the record and restart", rec.Name, sf.reason)
	}
	if err := m.claimable(folded, rec); err != nil {
		return Record{}, err
	}
	store, err := m.openStore()
	if err != nil {
		return Record{}, err
	}
	// The name as registered, not as the replacement spells it: the registration is what it is,
	// and a replacement that only differs in case is not a rename.
	rec.Name = sf.rec.Name
	rec, impl, err := m.prepare(rec)
	if err != nil {
		return Record{}, err
	}
	if err := sf.tramp.install(impl, rec.Arity, rec.maxArity()); err != nil {
		return Record{}, err
	}

	prev := sf.rec.Checksum
	rec.State, rec.StateReason, rec.StateAt = StateOK, "", time.Now().Unix()
	tx := store.Begin()
	tx.NewClassAd(folded, rec.Ad())
	if err := tx.Commit(); err != nil {
		// The new code is already answering: the install is what a query sees and it has happened.
		// Reporting the write failure without saying that would leave an operator believing the
		// old code is still running.
		sf.rec, sf.state, sf.reason = rec, StateOK, ""
		return Record{}, fmt.Errorf("%s now runs the new source in this daemon, but the record "+
			"could not be written, so a restart brings back %s: %w", rec.Name, prev, err)
	}
	sf.rec, sf.state, sf.reason = rec, StateOK, ""
	m.logger.Info("stored user function replaced", "name", rec.Name, "was", prev,
		"checksum", rec.Checksum, "by", rec.RegisteredBy)
	return rec, nil
}

// Remove deletes a stored function's record.
//
// THE NAME IS NOT GIVEN BACK. A registration cannot be withdrawn, so for the rest of this
// daemon's life the name stays registered and answers ERROR, and the listing keeps saying so. What
// removal actually does is stop the next restart from registering it, after which the name is
// free. The returned note says exactly that, because an administrator who removes a function and
// immediately registers another under the same name needs to know why it is refused.
func (m *Manager) Remove(name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	folded := strings.ToLower(name)
	sf, ok := m.stored[folded]
	if !ok {
		return "", fmt.Errorf("%q is not a stored user function here", name)
	}
	store, err := m.openStore()
	if err != nil {
		return "", err
	}
	tx := store.Begin()
	tx.DestroyClassAd(folded)
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("removing the record for %s: %w", sf.rec.Name, err)
	}
	note := fmt.Sprintf("%s is removed from the table", sf.rec.Name)
	if sf.tramp != nil {
		sf.tramp.impl.Store(&implementation{checksum: sf.rec.Checksum,
			reason: "the function has been removed; the name stays registered until this daemon restarts"})
		note += "; the name stays registered in this daemon and answers ERROR until it restarts, " +
			"because a registration cannot be withdrawn. After a restart the name is free again"
	}
	sf.state, sf.reason = StateRemoved, "removed; the name is registered until this daemon restarts"
	m.logger.Info("stored user function removed", "name", sf.rec.Name, "checksum", sf.rec.Checksum)
	return note, nil
}

// claimable reports whether rec may take the name, on grounds that have nothing to do with its
// source: the name's shape, the built-ins, and the configured functions it shares a name space
// with.
func (m *Manager) claimable(folded string, rec Record) error {
	if err := rec.validDescription(); err != nil {
		return err
	}
	if prev, ok := m.loaded[folded]; ok {
		return fmt.Errorf("%q is a configured %s user function (%s); a name is one function, so "+
			"pick another name or remove it from the configuration and restart", rec.Name, prev.kind(), prev.Path)
	}
	return nil
}

// prepare fills in a record's computed parts and compiles it, leaving nothing to do but register
// and store. It is the half Register and Replace share.
func (m *Manager) prepare(rec Record) (Record, *implementation, error) {
	// A script `def` states its own argument count, so a registration need not repeat it and
	// cannot get it wrong. A wasm module cannot be asked -- the count is checked before the guest
	// runs -- so there it has to be declared.
	if rec.Arity < 0 {
		if rec.Kind != KindScript {
			return rec, nil, errors.New("a wasm function's argument count has to be declared: it is " +
				"checked before the module runs, and the module cannot be asked for it")
		}
		n, err := ScriptArity(rec.Source, rec.Name)
		if err != nil {
			return rec, nil, err
		}
		rec.Arity, rec.MaxArity = n, n
	}
	if err := rec.validShape(); err != nil {
		return rec, nil, err
	}
	src, err := rec.sourceBytes()
	if err != nil {
		return rec, nil, err
	}
	// Computed here, never taken from the caller. An identity the registrant chose would name
	// whatever they wanted rather than the code that runs.
	rec.Checksum = checksumOf(src)
	rec.MaxArity = rec.maxArity()
	if rec.RegisteredAt == 0 {
		rec.RegisteredAt = time.Now().Unix()
	}
	impl, err := m.compile(rec)
	if err != nil {
		return rec, nil, err
	}
	if rec.TrustRequested {
		m.logger.Warn("stored user function asked to be trusted and is loaded untrusted; trust is "+
			"configuration, not something a stored module can claim", "name", rec.Name)
	}
	return rec, impl, nil
}

// ScriptArity reports the argument count of a `def` in some script text, so a registration can
// state the count the source actually has rather than making the administrator count parameters.
// It compiles nothing into the registry.
func ScriptArity(text, name string) (int, error) {
	mod, err := vm.ParseScriptModule(text)
	if err != nil {
		return 0, err
	}
	f, err := mod.Compile(name)
	if err != nil {
		return 0, err
	}
	return f.Arity(), nil
}

// openStore returns the stored-function table, creating it on first use. Called with the lock held.
func (m *Manager) openStore() (*db.DB, error) {
	if m.store != nil {
		return m.store, nil
	}
	if m.cat == nil {
		return nil, ErrNoStore
	}
	tbl, err := m.cat.CreateTable(Table)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", Table, err)
	}
	m.store = tbl
	if m.stored == nil {
		m.stored = map[string]*storedFn{}
	}
	return tbl, nil
}

// List reports every user function this daemon has registered, of either origin, with its state
// and the identity of the code behind it.
//
// Both origins in one listing, because they are one name space: an administrator asking what
// pctUsed is wants the answer whichever way it got here. It is the third place a bad function is
// visible, and the only one that does not require having been watching at start-up.
func (m *Manager) List() []Status {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Status, 0, len(m.loaded)+len(m.stored))
	for _, mod := range m.loaded {
		st := Status{Name: mod.Name, Kind: mod.kind(), Origin: "configured", State: StateOK,
			Arity: mod.MinArgs, MaxArity: mod.MaxArgs, Path: mod.Path}
		if mod.Aggregate {
			st.Kind = "wasm aggregate"
		}
		out = append(out, st)
	}
	for _, sf := range m.stored {
		st := Status{Name: sf.rec.Name, Kind: sf.rec.Kind, Origin: "stored",
			Arity: sf.rec.Arity, MaxArity: sf.rec.maxArity(), Checksum: sf.rec.Checksum,
			State: sf.state, Reason: sf.reason,
			RegisteredBy: sf.rec.RegisteredBy, RegisteredAt: sf.rec.RegisteredAt}
		if st.State == "" {
			st.State = StateOK
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name) })
	return out
}

// Ad renders one listing line, for the administrative response.
func (s Status) Ad() *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString(AttrName, s.Name)
	ad.InsertAttrString(AttrKind, s.Kind)
	ad.InsertAttrString("Origin", s.Origin)
	ad.InsertAttr(AttrArity, int64(s.Arity))
	ad.InsertAttr(AttrMaxArity, int64(s.MaxArity))
	if s.Checksum != "" {
		ad.InsertAttrString(AttrChecksum, s.Checksum)
	}
	if s.Path != "" {
		ad.InsertAttrString("Path", s.Path)
	}
	ad.InsertAttrString(AttrState, s.State)
	if s.Reason != "" {
		ad.InsertAttrString(AttrStateReason, s.Reason)
	}
	if s.RegisteredBy != "" {
		ad.InsertAttrString(AttrRegisteredBy, s.RegisteredBy)
	}
	if s.RegisteredAt != 0 {
		ad.InsertAttr(AttrRegisteredAt, s.RegisteredAt)
	}
	return ad
}
