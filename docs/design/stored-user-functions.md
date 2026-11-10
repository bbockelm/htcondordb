# Design sketch: user functions stored in the collection

**Status:** BUILT, server side. The questions that decide the shape are answered in §6; §7 is what
remains open, and the client half of it is untouched: a client still loads its own functions from
its own configuration.
**Problem owner:** "an admin client should be able to register a function, and the modules should
live in the collection rather than only in a config file".
**Siblings:** [user-functions.md](../user-functions.md) (what configuration does today).

## 1. Why configuration alone is limiting

A user function today is a path in a config file, read at start-up. That has three costs, and the
first is the one sites actually hit.

**The same module has to be configured twice, and half a query fails if it is not.** A query is
evaluated in two places: a `WHERE` clause runs on the server, a `SELECT` item is computed by
whoever asked. Both load modules from the same knobs. So a module configured only on the server
makes `WHERE pctUsed(...)` work and `SELECT pctUsed(...)` return a column of `error` — with, as the
end-to-end tests record, **no diagnostic on either stream**, because a client that was never
configured has nothing to fail to load. The failure mode the documentation warns about is the one
that says nothing.

Storing the module where the data is removes the duplication: a client fetches what it needs from
the server it is already talking to.

**Adding a function needs a config edit on every host**, which is a different change-management
path from everything else this database holds.

**Only an operator with filesystem access can add one.** A site that wants its analysts to share a
scoring function has to route it through whoever owns `/etc/condor`.

## 2. Where a module lives

A table, `user_functions`, one record per module, keyed by the function name as a query would
spell it. A record carries: the name; the kind (`script` or `wasm`); the source — ClassAd Script
text, or wasm bytes base64-encoded, since an attribute holds a string; the arity; provenance (who
registered it, when); and an **instance identity**, a checksum over the source.

The identity is what makes a result explicable. A name is not enough, because the thing behind a
name changes; two queries seconds apart can run different code under one word. The checksum names
the code that actually ran, so a diagnostic, a log line or an `EXPLAIN` can say *which* `pctUsed`
answered. A checksum rather than a generated id, because it also makes "the same function" mean the
same thing on two nodes that were loaded separately.

Nothing here is a new storage mechanism. It is an ordinary table with an in-process writer, which
is the existing answer to "who may write this".

## 3. The constraint that shapes everything: the scalar registry is append-only

`classad`'s scalar registry has no unregister, and the reason is specific: **compiling an
expression records that a name is a user function**, and compiled programs are cached and shared.
Withdraw a registration and a cached program calls something that is gone.

That makes "a client registers a function" easy and "a client *changes* one" hard, which is the
wrong half to be easy — a stored function that can never be corrected is barely better than a
config file.

**The way out is a trampoline.** Register each name once, permanently, against an indirection the
daemon owns; let the indirection point at the current implementation. The cached decision — *this
name is a user function of arity N* — stays true forever, so nothing cached is invalidated. What
changes is only what sits behind the pointer.

This gives a sharp and checkable rule:

> **Arity is the only immutable part.** A stored function may be replaced, in place, as long as its
> arity does not change. Changing arity means a new name, or a restart.

That falls out of what the compiled program actually captured, rather than being a restriction we
chose, which is the kind of rule that survives contact with users.

## 4. Aggregates are the easier half

The aggregate registry offers replacement and withdrawal, and the scalar one does not. Two things
make that safe. An aggregate name is resolved **once per query**, and nothing caches that
resolution, so withdrawing a name cannot strand a compiled program the way §3 describes. And an
aggregate registration carries **no arity** — one spec, one argument — so a replacement has no
signature that could disagree with what a call site already captured.

Aggregates therefore need no trampoline: `db.ReplaceAggregate` and `db.UnregisterAggregate` change
a stored aggregate outright, arity included.

So a record says two independent things. **Kind** is what the source *is*, script or wasm, and
decides how it is compiled. **Role** is what the name is registered *as*, and decides which registry
it goes into and what may later be done to it. Script and wasm each take either role, so these are
two attributes and not four kinds — and a record that states no role is a scalar function, which is
what makes every record written against §2 alone go on meaning what it meant.

Three things follow, and all three run the other way from §3:

- **A stored aggregate carries no arity**, so nothing about it is immutable. A replacement may
  change everything.
- **A bad stored aggregate is not registered.** §6 says why a bad *function* has to be, and the
  reason — the call site's shape — does not exist here. What is left is which failure to hand an
  operator: a registered aggregate that errors makes a query *succeed* with a column of `error` in
  it, where an unregistered one fails the query and names the aggregate, which is also what `db`
  does with a name nobody ever registered.
- **Removing one gives the name back**, immediately and with no restart, because the registration
  can be withdrawn. A removed function leaves a tombstone in the listing saying the name is still
  taken; a removed aggregate leaves nothing, because nothing is left.

So the two halves land differently, and the documentation says so rather than implying a uniform
feature.

**The merge laws stay the author's.** `merge` has to be associative and commutative and `result`
has to repeat, and no registration can check either: `db.CheckAggregate` tests one over samples
only the author can choose, since values invented in the daemon would be integers for an aggregate
over host names and would prove nothing. The columnar pass fans out across segment windows, so an
order-sensitive `merge` is right in every test its author writes and wrong in production.

## 5. Authorization

Three separate permissions hide inside "register a function", and collapsing them is the main way
this goes wrong.

**Writing the record** is an ordinary table write, governed by whoever owns `user_functions`.

**Registering it into a running daemon** is not an ordinary write. A user function runs inside
every query that names it, on the server and on every client that loads it. The function name space
is global and shared: whoever can register `pctUsed` decides what that word means for everyone, and
— because a name cannot be withdrawn — can take a name permanently. That is a denial of service on
the vocabulary, available to anyone allowed to do it.

So registration is an **administrative** action, at DAEMON or ADMINISTRATOR, not at WRITE. Reading
a table and claiming a word in the query language are not the same privilege, and WRITE is where
the analysts are.

**Trust stays configuration, and a stored module can never claim it.** `HTCONDORDB_WASM_<NAME>_TRUSTED`
removes the wall-clock deadline and runs a module uninterruptibly; the existing design is explicit
that nothing inside a module can ask for it, "because a capability a module can claim is a request
rather than a capability". A stored module is exactly a module that can claim things, so the rule
extends unchanged: **trust is never stored**. A trusted module remains a file an operator pointed
at.

What a stored module is bounded by is then the same as an untrusted configured one: the step
budget, the wall-clock deadline, the memory cap, and — for a script — the purity gate, which proves
at registration that the function reaches no ad and the language gives it no I/O.

### The client side is a trust decision, not just a fetch

A client that loads a module from the server is running code the server handed it. For wasm that is
sandboxed and for a script it is a proven-pure function, so the blast radius is bounded — but
"bounded" is not "nothing", and a federation hub serving many access points means the server handing
out code is not always the one the user thinks they are talking to.

Two things follow. A client should be able to **refuse** to load stored modules, by configuration,
and get the current behaviour. And a client that loads them should say so somewhere a user can see,
rather than silently executing whatever arrived.

## 6. Decided

**Registration is administrative.** Writing the record is an ordinary table write; registering a
name is not, for the reasons in §5.

**A function instance is identified by a checksum over its source** (§2), so a result can be traced
to the code that produced it rather than to a name that has since moved.

**Registration survives a restart.** The table is the source of truth, so a daemon registers what it
holds when it starts. Nothing else would be coherent: a function that vanished on restart would make
a pool's behaviour depend on uptime.

**A module that does not load marks the function BAD. It does not stop the daemon.** This is the
one place where the stored path deliberately differs from the configured one, where a module that
fails to load fails start-up. Three things justify the difference:

- A configured module is a file on the host, editable by whoever is about to restart the daemon. A
  stored module is not, and a daemon that will not start cannot be used to fix the record that stops
  it starting. The repair path would run through the thing that is broken.
- **A module can stop loading without anyone touching it** — a wasm guest valid under one runtime
  version and rejected by the next is the obvious case. Tying start-up to that makes an upgrade a
  coin toss.
- Otherwise anyone who may register could stop the daemon starting, which turns an administrative
  privilege into an availability one.

**A bad scalar function is an error when called**, which is what a name that does not exist already gives.
It is registered, under its arity, pointing at an implementation that errors — not left
unregistered. Leaving it out would change how the *call site* compiles, which is exactly the
decision §3 says cannot be taken back; registering it keeps the name's shape fixed and puts the
failure in the value, where a query can see it.

A **bad aggregate is unregistered**, for the reasons in §4, and a query naming it fails.

Bad is a **state, not a silence**, in either role: it is in the daemon's log at start-up, in the
function's record, and visible to an admin listing what is registered. An operator should be able
to find out that `pctUsed` is bad without first noticing that a column has gone to `error`, or that
`jobMean` is bad without first running a query that fails.

**The name is held by this daemon's own bookkeeping, bad or good.** That is what keeps one name
space across the configured and stored surfaces whichever registry a name ends up in, and it is why
a name whose aggregate was never registered is still not available to anything else.

## 7. What this does not settle

- **When a client notices a change.** Modules fetched per session, per query, or cached with
  invalidation are three different designs, and the trampoline only fixes the server half. The
  checksum gives the cache a key.
- **Whether a bad module can be repaired without a restart.** Replacing the source is a write the
  trampoline can absorb and a write the aggregate registry takes outright, so it is possible for
  both — but a module that is bad because the *runtime* rejects it will stay bad however often it
  is re-registered, and the error has to say which of the two it is.
- **Whether a client refuses stored modules.** §5 argues it should be able to; nothing here says
  what the default is.
