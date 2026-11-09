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
	"github.com/PelicanPlatform/classad/db/scriptagg"
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
// AN AGGREGATE IS A ROLE, NOT A KIND, AND NONE OF THE PARAGRAPH ABOVE APPLIES TO IT. A record says
// two independent things: what its source IS -- ClassAd Script text or a WebAssembly module, which
// is Kind and decides how the source is compiled -- and what the name is registered AS, which is
// Role and decides which registry it goes into and what may later be done to it. Script and wasm
// each take either role, so the two are separate attributes rather than four kinds; and a record
// that names no role is a scalar function, which is what every record written before the role
// existed is.
//
// The aggregate registry takes a replacement and a withdrawal, because an aggregate name is
// resolved ONCE PER QUERY, when the reduction is set up, and nothing caches that resolution past
// the query that made it. An aggregate registration also carries NO ARITY at all -- one spec, one
// argument attribute, whatever the aggregate -- so a replacement has no signature it could
// disagree with. Two consequences, both of which this file takes:
//
//   - A stored aggregate needs NO TRAMPOLINE. db.ReplaceAggregate installs the new implementation
//     under the name directly, and db.UnregisterAggregate takes the name back.
//   - A stored aggregate's ARITY IS NOT IMMUTABLE, because there is no arity. A record that is an
//     aggregate carries none, and a replacement may change anything about the source at all.
//
// What an aggregate's author owes instead is the laws the reduction relies on: `merge` has to be
// associative AND commutative, and `result` has to repeat without disturbing the state. The
// columnar pass fans out across segment windows and which worker draws which segment is a race, so
// an order-sensitive merge is right in every test its author writes and wrong in production. No
// registration can check that -- db.CheckAggregate tests one against the laws, over samples only
// the author can choose, since samples picked here would be integers for an aggregate over host
// names and would prove nothing.
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
// A BAD SCALAR FUNCTION IS REGISTERED, under its stored arity, pointing at an implementation that
// answers ERROR. It is NOT left unregistered. Leaving the name out would change how the CALL SITE
// compiles -- an unregistered name compiles to something else entirely -- and that decision is the
// one that cannot be taken back. Registering it keeps the name's shape fixed and puts the failure
// in the value, where a query can see it, which is what calling a name that does not exist already
// gives.
//
// A BAD AGGREGATE IS NOT REGISTERED, and the opposite answer has the same cause: nothing captures
// an aggregate name, so there is no shape to keep. What decides it is which failure an operator
// gets. A registered aggregate that answers ERROR for every group produces a column of ERROR in a
// query that SUCCEEDS -- a result that looks like a result. An unregistered one fails the query
// outright, naming the aggregate, which is also what db does with an aggregate nobody ever
// registered. Between a plausible wrong answer and a loud refusal that says which name is the
// problem, the refusal is the one to hand an operator.
//
// And bad is a STATE, NOT A SILENCE, in either role: it is logged at start-up, written into the
// function's own record, and reported by the listing. An operator should be able to find out that
// pctUsed is bad without first noticing that a column has gone to ERROR, or that p95 is bad
// without first running a query that fails.
//
// THE NAME IS HELD EITHER WAY. Bad or good, scalar or aggregate, the name stays claimed in THIS
// manager's bookkeeping, so a configured module and a second record cannot contend for it. That
// bookkeeping is this file's and not classad's, which is why a name whose aggregate was never
// registered is still not available to anyone else.
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
	AttrName   = "Name"   // the name as registered, in the spelling the administrator chose
	AttrKind   = "Kind"   // KindScript or KindWasm: what the source IS
	AttrSource = "Source" // ClassAd Script text, or base64-encoded wasm bytes
	// AttrRole is what the name is registered AS: RoleFunction or RoleAggregate. ABSENT MEANS
	// RoleFunction, which is what makes every record written without it a scalar function and
	// keeps it loading unchanged.
	AttrRole = "Role"
	// The argument count. A scalar function's, and the part of it that cannot change; an
	// aggregate carries neither attribute, because an aggregate registration has no arity for
	// anything to disagree with.
	AttrArity    = "Arity"
	AttrMaxArity = "MaxArity" // the largest accepted count; absent means exactly Arity, -1 variadic
	// The four `def`s a SCRIPT AGGREGATE is built from, each optional. The default is the
	// configured surface's prefix rule -- <name>_init, <name>_add, <name>_merge, <name>_result --
	// so a file written for one surface loads on the other, and a record names a `def` only where
	// its file spells things otherwise. They are suffixed Def because a record attribute called
	// "Add" beside Name, Kind and Source reads like an instruction rather than a name.
	AttrInitDef   = "InitDef"
	AttrAddDef    = "AddDef"
	AttrMergeDef  = "MergeDef"
	AttrResultDef = "ResultDef"
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

// The kinds of source a record can carry, which is what decides how it is compiled.
const (
	KindScript = "script"
	KindWasm   = "wasm"
)

// The roles a stored name can be registered in, which is what decides which registry it goes into
// and what may later be done to it. A record that states no role is a function.
const (
	RoleFunction  = "function"
	RoleAggregate = "aggregate"
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
// source against it. An AGGREGATE has no argument count to carry: it is named in one spec against
// one argument attribute whatever it is, so both fields are left unset and nothing reads them.
type Record struct {
	Name   string
	Kind   string
	Role   string // RoleFunction (the default when empty) or RoleAggregate
	Source string
	// Defs names the four `def`s a script aggregate is built from. An empty role falls back to
	// the prefix rule; the field is ignored entirely for anything but a script aggregate.
	Defs         aggDefs
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

// role is the role the record claims, with the empty spelling resolved. Every test of what a
// record is goes through this or through aggregate, so "absent means a function" is stated once.
func (r Record) role() string {
	if r.Role == "" {
		return RoleFunction
	}
	return strings.ToLower(r.Role)
}

// aggregate reports whether this record registers an aggregate rather than a scalar function.
func (r Record) aggregate() bool { return r.role() == RoleAggregate }

// defs resolves the four `def` names a script aggregate is built from, filling in the prefix rule
// for each role the record does not name. Resolved HERE rather than left to the library's own
// defaults so that one rule -- the configured surface's -- spells them for both surfaces: a file
// written for HTCONDORDB_SCRIPT_AGGREGATES registers from this table unchanged.
func (r Record) defs() aggDefs {
	pick := func(given, dflt string) string {
		if strings.TrimSpace(given) == "" {
			return dflt
		}
		return strings.TrimSpace(given)
	}
	return aggDefs{
		Init:   pick(r.Defs.Init, r.Name+"_init"),
		Add:    pick(r.Defs.Add, r.Name+"_add"),
		Merge:  pick(r.Defs.Merge, r.Name+"_merge"),
		Result: pick(r.Defs.Result, r.Name+"_result"),
	}
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
	if r.aggregate() {
		// Written only for an aggregate, so that a record carrying no Role attribute at all is
		// exactly a scalar function's and reads back as one.
		ad.InsertAttrString(AttrRole, RoleAggregate)
		// The four `def` names, each written only where the record names one. The prefix rule
		// fills the rest in at load, so writing the resolved names would freeze into the record
		// an answer that the rule already gives.
		for _, d := range []struct{ attr, name string }{
			{AttrInitDef, r.Defs.Init}, {AttrAddDef, r.Defs.Add},
			{AttrMergeDef, r.Defs.Merge}, {AttrResultDef, r.Defs.Result},
		} {
			if strings.TrimSpace(d.name) != "" {
				ad.InsertAttrString(d.attr, d.name)
			}
		}
	} else {
		// An aggregate carries NO argument count, so the attributes are absent rather than zero:
		// zero is a legitimate count for a function, and a reader has to be able to tell a record
		// that states none from one that states none-at-all.
		ad.InsertAttr(AttrArity, int64(r.Arity))
		ad.InsertAttr(AttrMaxArity, int64(r.maxArity()))
	}
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
	r.Role, _ = ad.EvaluateAttrString(AttrRole)
	r.Source, _ = ad.EvaluateAttrString(AttrSource)
	r.Defs.Init, _ = ad.EvaluateAttrString(AttrInitDef)
	r.Defs.Add, _ = ad.EvaluateAttrString(AttrAddDef)
	r.Defs.Merge, _ = ad.EvaluateAttrString(AttrMergeDef)
	r.Defs.Result, _ = ad.EvaluateAttrString(AttrResultDef)
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

// aggRegistry is db's aggregate registry, as the stored path uses it.
//
// An interface for the reason the scalar one is, and for one more: this registry DOES take a
// withdrawal, so a test can register a name, take it back, and register it again -- but the
// registry is still process-wide, so two tests sharing a name would still interfere through it.
// A test that wants a registry to itself supplies one.
//
// The three methods are the three things a stored aggregate needs, and each is a different
// question. RegisterAggregate claims a free name and FAILS if something else holds it, which is
// how a first registration finds out about a collision this manager did not know of.
// ReplaceAggregate installs over whatever is there, which is the correction path and also the
// repair path for a name that is bad and therefore absent. UnregisterAggregate takes the name
// back, which is what makes removing a stored aggregate give the name up immediately.
type aggRegistry interface {
	RegisterAggregate(name string, agg db.Aggregate) error
	ReplaceAggregate(name string, agg db.Aggregate) error
	UnregisterAggregate(name string) bool
}

type classAdAggRegistry struct{}

func (classAdAggRegistry) RegisterAggregate(name string, agg db.Aggregate) error {
	return db.RegisterAggregate(name, agg)
}

func (classAdAggRegistry) ReplaceAggregate(name string, agg db.Aggregate) error {
	return db.ReplaceAggregate(name, agg)
}

func (classAdAggRegistry) UnregisterAggregate(name string) bool {
	return db.UnregisterAggregate(name)
}

// aggRegistryOrDefault is the aggregate registry to register into. Called with the lock held.
func (m *Manager) aggRegistryOrDefault() aggRegistry {
	if m.aggReg != nil {
		return m.aggReg
	}
	return classAdAggRegistry{}
}

// implementation is what sits behind a trampoline: the function of the moment, and the identity of
// the code it runs.
type implementation struct {
	// fn is a scalar function's callable, and agg an aggregate's registration. Exactly one of
	// them is set, decided by the record's role; they share a struct because everything else here
	// -- the identity of the code, the module it came from, why it errors -- is the same question
	// for both, and a second struct would duplicate all of it to separate two fields.
	fn       classad.Func
	agg      db.Aggregate
	checksum string
	// udf and udaf are the loaded module a wasm implementation came from, nil for a script. Kept
	// so that what is running can be INSPECTED and not only called -- its trust, its per-call
	// statistics -- which a bare func value cannot answer for.
	udf  *wasmudf.UDF
	udaf *wasmudf.UDAF
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
	// impl is an AGGREGATE's implementation, held here because an aggregate has no trampoline to
	// hold it: db's registry takes the replacement directly, so the indirection a scalar name
	// needs would be a pointer nothing ever reads through. It is what the listing reports the
	// identity of, and nil while an aggregate is bad.
	impl *implementation
	rec  Record
	// state and reason are the live state, which is what the listing reports. The record carries
	// the same pair, written back so it survives a restart; this copy is what a listing can report
	// without reading the table.
	state  string
	reason string
}

// Status is one line of the listing: what is registered, in what state, running which code.
type Status struct {
	Name string
	// Kind is for a reader, not for a parser: it names the source and the role together --
	// "script", "wasm", "script aggregate", "wasm aggregate" -- because those four are what an
	// operator scanning a listing wants to tell apart, and splitting them into two columns would
	// put a mostly empty one beside a mostly redundant one.
	Kind   string
	Origin string // "stored" or "configured"
	// Aggregate is the role as a FACT rather than as text, so that a renderer can leave the
	// argument count out instead of printing a zero for a thing that has no count.
	Aggregate bool
	Arity     int
	MaxArity  int
	Checksum  string // a stored function's instance identity
	Path      string // a configured function's file
	State     string
	Reason    string

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

	if rec.aggregate() {
		m.loadStoredAggregate(folded, rec)
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

// loadStoredAggregate registers one stored aggregate. Called with the lock held.
//
// THE SOURCE IS COMPILED FIRST AND THE NAME REGISTERED SECOND, which is the opposite order from
// the scalar path two functions up, and the reason is the whole of what an aggregate registration
// is not. There, the name has to be claimed whether or not the source compiles, because leaving it
// out changes how a call site compiles and that decision cannot be revisited. Here nothing
// captures the name, so there is no shape to fix in advance -- and registering an aggregate that
// does not work would turn a query that fails, naming the aggregate, into a query that succeeds
// with a column of ERROR in it.
func (m *Manager) loadStoredAggregate(folded string, rec Record) {
	m.stored[folded] = &storedFn{rec: rec}
	impl, err := m.compile(rec)
	if err != nil {
		m.badLocked(folded, err)
		return
	}
	// RegisterAggregate, which fails on a name already held, rather than ReplaceAggregate, which
	// does not. The names this manager knows about have been checked already; what this catches is
	// a name held by something else in this process, which is a collision nobody here can see.
	if err := m.aggRegistryOrDefault().RegisterAggregate(rec.Name, impl.agg); err != nil {
		m.badLocked(folded, fmt.Errorf("the name could not be registered as an aggregate: %w", err))
		return
	}
	sf := m.stored[folded]
	sf.impl, sf.state, sf.reason = impl, StateOK, ""
	m.logger.Info("stored aggregate registered", "name", rec.Name, "kind", rec.Kind,
		"checksum", impl.checksum)
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
	if sf.rec.aggregate() {
		// Nothing is registered, and the implementation is dropped rather than replaced by one
		// that errors. A query naming the aggregate then fails, naming it, which is the same
		// answer db gives for an aggregate nobody ever registered -- and a better one to hand an
		// operator than a column of ERROR inside a query that reports success.
		sf.impl = nil
		m.logger.Error("stored aggregate is BAD and is not registered; a query naming it fails, "+
			"rather than answering a column of ERROR that looks like an answer",
			"name", sf.rec.Name, "err", reason)
		m.markState(folded, StateBad, reason)
		return
	}
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
	if r.aggregate() {
		// There is no argument count to check and no name to register under one. An aggregate is
		// named in one spec against one argument attribute whatever it is, so a record that
		// describes an aggregate at all describes it completely.
		return nil
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
	case RoleAggregate:
		// Worth its own message, because the two axes are easy to conflate and an administrator
		// who reaches for this has the right idea in the wrong field. Script and wasm each take
		// either role, so what the source is and what it is registered as are separate things.
		return fmt.Errorf("%q is a role rather than a kind: set %s to %s or %s -- what the source "+
			"is -- and %s to %s", RoleAggregate, AttrKind, KindScript, KindWasm, AttrRole, RoleAggregate)
	default:
		return fmt.Errorf("%q is not a kind of user function (%s or %s)", r.Kind, KindScript, KindWasm)
	}
	switch r.role() {
	case RoleFunction:
	case RoleAggregate:
		// A built-in aggregate name is not available either, and for a reason the scalar
		// built-in check does not cover: a client's parser reads COUNT, SUM, AVG, MIN and MAX as
		// aggregates before it consults any registry, so a registration under one of those names
		// succeeds and is then unreachable. Refusing it here says so, where a silent success
		// would leave an administrator to discover it by writing a query.
		if builtinAggregateName(r.Name) {
			return fmt.Errorf("%q is a built-in aggregate, which a stored one may not shadow", r.Name)
		}
	default:
		return fmt.Errorf("%q is not a role a user function can have (%s or %s)", r.Role, RoleFunction, RoleAggregate)
	}
	if strings.TrimSpace(r.Source) == "" {
		return errors.New("the record carries no source")
	}
	return nil
}

// builtinAggregateName reports whether a query's parser reads name as one of the aggregates the
// language already has, in which case nothing registered under it is ever resolved.
//
// Four of the five are built-in scalar FUNCTIONS as well, and are refused as those whatever role
// they are asked for; COUNT is not, so this is the only rule that catches it.
//
// The list is written out here rather than asked of the parser, because the parser lives in the
// package that reads SQL and this one is loaded by the daemon that has none. It is five words and
// it has not moved; what keeps it honest is that a name wrongly on it is merely refused, which an
// administrator reports, while a name wrongly off it is refused by nothing and silently does
// nothing, which they cannot.
func builtinAggregateName(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	}
	return false
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
	switch {
	case rec.aggregate() && rec.Kind == KindScript:
		return m.compileScriptAggregate(rec, string(src), sum)
	case rec.aggregate():
		return m.compileWasmAggregate(rec, src, sum)
	case rec.Kind == KindScript:
		return m.compileScript(rec, string(src), sum)
	default:
		return m.compileWasm(rec, src, sum)
	}
}

// compileScriptAggregate builds one aggregate out of the four `def`s in stored script text.
//
// It builds and does not register, which is what lets the start-up load decide afterwards whether
// to claim the name and lets a replacement prove the new source before the old one stops running.
// The library refuses three things here, each of which would otherwise be a column of ERROR for
// every caller with nothing saying why: a `def` the record names and the source does not define,
// one whose parameter count does not fit the role the engine calls it in, and an `init` that does
// not produce an identity. Those refusals are the library's own words and name the `def` and the
// role, which is what says which line of the source to open.
//
// The purity gate applies to all four exactly as it does to a scalar `def`. There is no Columnar
// opt-in: it is a promise about value order and about which rows are offered that only the author
// can make, the configured surface does not offer it either, and an aggregate that does not make
// it is served by the record scan, which is correct and slower.
func (m *Manager) compileScriptAggregate(rec Record, text, sum string) (*implementation, error) {
	mod, err := vm.ParseScriptModule(text)
	if err != nil {
		return nil, err
	}
	defs := rec.defs()
	a, err := scriptagg.New(mod, scriptagg.Spec{
		Init:   defs.Init,
		Add:    defs.Add,
		Merge:  defs.Merge,
		Result: defs.Result,
	})
	if err != nil {
		return nil, err
	}
	return &implementation{agg: a, checksum: sum}, nil
}

// compileWasmAggregate loads stored module bytes as an aggregate.
//
// THE LOADER KEY CARRIES THE CHECKSUM, for the reason compileWasm's does: the loader refuses to
// load one name twice and has no unload, and a stored aggregate's whole point is that a second
// module can take the first one's place. The name a query reduces with is the bare one, under
// which this registers in db; the loader's is only a key, since an aggregate module's exports are
// fixed names the key does not reach.
//
// The residue is that every superseded module stays compiled for the life of the daemon. That is
// the price of a loader whose only withdrawal is closing the runtime out from under every
// registration still pointing into it, and a daemon that replaces an aggregate a handful of times
// pays it in compiled modules nobody calls.
//
// LoadOptions carries no Trusted, here as everywhere else in this file.
func (m *Manager) compileWasmAggregate(rec Record, wasm []byte, sum string) (*implementation, error) {
	loader, err := m.wasmLoader()
	if err != nil {
		return nil, err
	}
	key := rec.Name + "@" + strings.TrimPrefix(sum, "sha256:")[:12]
	u, err := loader.LoadAggregateWith(m.parent, key, wasm, wasmudf.LoadOptions{})
	if err != nil {
		return nil, err
	}
	return &implementation{agg: u.Aggregate(), checksum: sum, udaf: u}, nil
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
	if rec.aggregate() {
		return m.registerAggregate(folded, rec, impl, store)
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

// registerAggregate is Register's second half for an aggregate: claim the name, write the record,
// and -- unlike the scalar case -- be able to undo the first if the second fails.
//
// A SCALAR THAT CANNOT BE WRITTEN STAYS REGISTERED AND SAYS SO, because a registration cannot be
// withdrawn and reporting a clean failure would leave the name quietly claimed. An aggregate CAN
// be withdrawn, so the failure is made clean instead: the name is given back and the registrant is
// told the registration did not happen, which is both true and the state they would want. Called
// with the lock held.
func (m *Manager) registerAggregate(folded string, rec Record, impl *implementation, store *db.DB) (Record, error) {
	reg := m.aggRegistryOrDefault()
	if err := reg.RegisterAggregate(rec.Name, impl.agg); err != nil {
		return Record{}, fmt.Errorf("registering the aggregate %s: %w", rec.Name, err)
	}
	rec.State, rec.StateReason, rec.StateAt = StateOK, "", time.Now().Unix()
	tx := store.Begin()
	tx.NewClassAd(folded, rec.Ad())
	if err := tx.Commit(); err != nil {
		reg.UnregisterAggregate(rec.Name)
		return Record{}, fmt.Errorf("the record for %s could not be written, so it is not "+
			"registered and the name is free: %w", rec.Name, err)
	}
	m.stored[folded] = &storedFn{impl: impl, rec: rec, state: StateOK}
	m.logger.Info("stored aggregate registered", "name", rec.Name, "kind", rec.Kind,
		"checksum", rec.Checksum, "by", rec.RegisteredBy)
	return rec, nil
}

// Replace points an existing stored function at new source, in place.
//
// This is what the trampoline is for, and the argument count is what it cannot absorb. It is also
// the repair path for a bad function: new source that loads clears the state. Source that does not
// load changes nothing -- the record and whatever is running stay as they were -- and the error
// says why, which is the difference between "fix the source" and "this runtime will not have it".
//
// AN AGGREGATE IS REPLACED OUTRIGHT, with nothing fixed. There is no trampoline because db's
// registry takes the new implementation under the name directly, and no argument count to keep
// because an aggregate registration carries none -- so a replacement may change everything about
// the source, and a bad aggregate, which is a name db does not hold at all, is repaired by the
// same call that corrects a working one.
//
// A ROLE CANNOT BE REPLACED, either way round. A name that is an aggregate and becomes a scalar
// function, or the reverse, is a different thing in a different registry answering a differently
// parsed call, and nothing about pointing one at new source describes that. It is a new name, or a
// removal and a registration.
func (m *Manager) Replace(rec Record) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	folded := strings.ToLower(rec.Name)
	sf, ok := m.stored[folded]
	if !ok {
		return Record{}, fmt.Errorf("%q is not a stored user function here; register it first", rec.Name)
	}
	// A replacement that names no role takes the one the name already has. "Absent means a
	// function" is how a RECORD reads, because a record describes a registration completely; a
	// replacement describes a change to one, and the thing it says nothing about is the thing it
	// is not changing. An explicitly different role is still refused, below.
	if rec.Role == "" {
		rec.Role = sf.rec.role()
	}
	if rec.role() != sf.rec.role() {
		return Record{}, fmt.Errorf("%q is a stored %s and the replacement is a %s; a replacement "+
			"points a name at new source and cannot change what the name IS. Remove it and register "+
			"the other, or pick another name", sf.rec.Name, sf.rec.role(), rec.role())
	}
	if !sf.rec.aggregate() && sf.tramp == nil {
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
	if rec.aggregate() {
		return m.replaceAggregate(folded, sf, rec, impl, store)
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

// replaceAggregate is Replace's second half for an aggregate. Called with the lock held.
//
// THE RECORD IS WRITTEN AND THE REGISTRATION IS ROLLED BACK IF IT CANNOT BE, which is the opposite
// of what the scalar path does with the same failure. There, the new code is already answering and
// cannot be taken back, so the only honest thing is to say the record and the running daemon now
// disagree. Here the registry takes a second replacement as readily as the first, so the old
// implementation goes back under the name and the daemon and its record agree again -- which is
// worth more than the fact that a query running in the window reduced with the new code, something
// that is true of any replacement and that no ordering here could prevent.
func (m *Manager) replaceAggregate(folded string, sf *storedFn, rec Record, impl *implementation, store *db.DB) (Record, error) {
	reg := m.aggRegistryOrDefault()
	prevImpl, prev := sf.impl, sf.rec.Checksum
	if err := reg.ReplaceAggregate(rec.Name, impl.agg); err != nil {
		return Record{}, fmt.Errorf("registering the aggregate %s: %w", rec.Name, err)
	}
	rec.State, rec.StateReason, rec.StateAt = StateOK, "", time.Now().Unix()
	tx := store.Begin()
	tx.NewClassAd(folded, rec.Ad())
	if err := tx.Commit(); err != nil {
		was := prev + " is still what runs"
		if prevImpl != nil {
			_ = reg.ReplaceAggregate(sf.rec.Name, prevImpl.agg)
		} else {
			// It was bad, so nothing held the name before this call and nothing holds it now.
			reg.UnregisterAggregate(sf.rec.Name)
			was = "it is still bad, and a query naming it still fails"
		}
		return Record{}, fmt.Errorf("the record for %s could not be written, so the replacement "+
			"is undone and %s: %w", rec.Name, was, err)
	}
	sf.impl, sf.rec, sf.state, sf.reason = impl, rec, StateOK, ""
	m.logger.Info("stored aggregate replaced", "name", rec.Name, "was", prev,
		"checksum", rec.Checksum, "by", rec.RegisteredBy)
	return rec, nil
}

// Remove deletes a stored function's record.
//
// A SCALAR FUNCTION'S NAME IS NOT GIVEN BACK. A scalar registration cannot be withdrawn, so for
// the rest of this daemon's life the name stays registered and answers ERROR, and the listing
// keeps saying so. What removal actually does is stop the next restart from registering it, after
// which the name is free. The returned note says exactly that, because an administrator who
// removes a function and immediately registers another under the same name needs to know why it is
// refused.
//
// AN AGGREGATE'S NAME IS GIVEN BACK, IMMEDIATELY. db's registry takes a withdrawal, so removing a
// stored aggregate withdraws it: a query naming it afterwards fails, exactly as it would for a
// name nobody ever registered, and the name is free for anything -- a new aggregate, a scalar
// function, a configured module at the next reconfigure. Nothing is left in the listing to report,
// because there is nothing left: the entry goes, where a removed scalar leaves a tombstone saying
// the name is still taken.
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
	if sf.rec.aggregate() {
		// Withdrawn whether or not it was registered: a bad aggregate never reached the registry,
		// and asking for a name it does not hold is how this call reports that.
		m.aggRegistryOrDefault().UnregisterAggregate(sf.rec.Name)
		delete(m.stored, folded)
		m.logger.Info("stored aggregate removed", "name", sf.rec.Name, "checksum", sf.rec.Checksum)
		return note + " and withdrawn from this daemon; a query naming it now fails, and the name " +
			"is free again with no restart", nil
	}
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
	// Normalized to one spelling before anything is written or compared: a role is a fixed word
	// and not a name the registrant chose, so a record that says "Aggregate" and one that says
	// "aggregate" are the same record and should read back as the same record.
	rec.Role = rec.role()
	if rec.aggregate() {
		// Normalized to "no count" rather than carried, so that nothing downstream -- the record
		// that gets written, the listing, a later replacement -- is handed a number that looks
		// like an argument count and constrains nothing. A request that STATED one is refused
		// where a stated count can still be told from an unstated one, which is the command that
		// read it; by here they are the same.
		rec.Arity, rec.MaxArity = -1, -1
	}
	// A script `def` states its own argument count, so a registration need not repeat it and
	// cannot get it wrong. A wasm module cannot be asked -- the count is checked before the guest
	// runs -- so there it has to be declared.
	if !rec.aggregate() && rec.Arity < 0 {
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
	if !rec.aggregate() {
		rec.MaxArity = rec.maxArity()
	}
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
			st.Kind, st.Aggregate = mod.kind()+" aggregate", true
		}
		out = append(out, st)
	}
	for _, sf := range m.stored {
		st := Status{Name: sf.rec.Name, Kind: sf.rec.Kind, Origin: "stored",
			Arity: sf.rec.Arity, MaxArity: sf.rec.maxArity(), Checksum: sf.rec.Checksum,
			State: sf.state, Reason: sf.reason,
			RegisteredBy: sf.rec.RegisteredBy, RegisteredAt: sf.rec.RegisteredAt}
		if sf.rec.aggregate() {
			st.Kind, st.Aggregate = sf.rec.Kind+" aggregate", true
		}
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
	if s.Aggregate {
		// The role, and no argument count: an aggregate has none, and a renderer that found a
		// zero here would print one.
		ad.InsertAttrString(AttrRole, RoleAggregate)
	} else {
		ad.InsertAttrString(AttrRole, RoleFunction)
		ad.InsertAttr(AttrArity, int64(s.Arity))
		ad.InsertAttr(AttrMaxArity, int64(s.MaxArity))
	}
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
