# Design sketch: user functions stored in the collection

**Status:** PROPOSED. Nothing below is built. This document exists to settle the questions that
decide the shape, because two of them are not implementation choices.
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
text, or wasm bytes base64-encoded, since an attribute holds a string; the arity; and provenance
(who registered it, when).

Nothing here is a new storage mechanism. It is an ordinary table with an in-process writer, which
is the existing answer to "who may write this".

## 3. The constraint that shapes everything: the registry is append-only

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

## 4. Aggregates are the easier half, and already say so

`db.RegisterAggregate` is append-only too, but for a different reason, and it says so in as many
words: an aggregate name is resolved **once per query**, so withdrawing one would be perfectly well
defined; it is append-only "because nothing needs the other thing — a reason that would stop
applying if a caller turned up".

A caller has turned up. Aggregates therefore need no trampoline: give the registry a replace and a
remove, and a stored aggregate can be changed outright, arity included.

So the two halves land differently, and the documentation has to say so rather than implying a
uniform feature.

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

## 6. What this does not settle

- **When a client notices a change.** Modules fetched per session, per query, or cached with
  invalidation are three different designs, and the trampoline only fixes the server half.
- **Whether a replaced function's old results are explicable.** Two queries seconds apart can run
  different code under one name. Provenance in the record helps a reader after the fact; nothing
  makes a result self-describing.
- **Whether registration should survive a restart automatically.** If the table is the source of
  truth, a daemon should register everything in it at start-up — which makes a bad stored module a
  start-up failure, exactly as a bad configured one is today. That is probably right and it is worth
  saying out loud, because it means a client with DAEMON can stop the daemon from starting.
- **Script aggregates do not exist yet.** `scriptudf` registers scalar functions only, so the
  aggregate half of this applies to wasm until that changes.
