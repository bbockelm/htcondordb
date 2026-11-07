# User functions

The query language has a fixed set of functions and aggregates. You can add your
own as a WebAssembly module: a file on disk, named in the configuration, loaded
at start-up. After that a query calls it like any built-in.

This is for the function your site needs and nobody else does — a scoring
expression, a site-specific name decoding, a percentile your reports want.

## Adding one

```
HTCONDORDB_WASM_FUNCTIONS           = pctUsed
HTCONDORDB_WASM_PCTUSED_PATH        = /etc/condor/wasm/pctused.wasm
HTCONDORDB_WASM_PCTUSED_MIN_ARGS    = 2
```

Restart the daemon and the function is available:

```sql
SELECT Machine, pctUsed(MemoryUsage, Memory) AS used
  FROM machines
 WHERE pctUsed(MemoryUsage, Memory) > 90
```

**Put the same configuration where your clients read it.** A query is evaluated
in two places: a `WHERE` clause runs on the server, and a `SELECT` item is
computed by whatever asked the question — `htcondordb-cli`, or the Python driver
through its shared library. Both load the same modules from the same knobs, so
if the file is only readable on the server, `WHERE pctUsed(...)` will work and
`SELECT pctUsed(...)` will not.

A client that cannot load a module warns and carries on, since the queries that
do not call it are unaffected.

An aggregate is the same, in the other list, and takes no arity:

```
HTCONDORDB_WASM_AGGREGATES    = p95
HTCONDORDB_WASM_P95_PATH      = /etc/condor/wasm/p95.wasm
```

It is then usable wherever `SUM` is:

```sql
SELECT Owner, p95(RemoteWallClockTime) FROM history GROUP BY Owner
```

The aggregate runs on the **server**, which is where the rows are, so the module
has to be configured there. The client needs the same configuration too, because
it is the client's parser that has to decide whether `p95(x)` is an aggregate
call or an ordinary function call before any request exists — a name it does not
know is read as an ordinary function and fails.

`MIN_ARGS` is required for a function, with no default, because the argument
count is checked before the module runs. A default wider than the module really
accepts would send it calls it refuses, and you would see `error` in a column
with nothing saying why.

## What a module can do, and what it cannot

A module gets argument values and nothing else. It cannot read an attribute you
did not pass it, reach the rest of the ad, or see the query.

It also cannot reach the machine: no files, no network, no clock, no randomness.
That is not a restriction applied to it — the host simply offers nothing to call,
so a module compiled against anything else fails to load and the log says what it
asked for.

Two limits bound how long a call can run:

- A **step budget** (`HTCONDORDB_WASM_STEPS`) the module charges itself against.
  It is deterministic: the same input runs out at the same point on a busy
  machine and an idle one, so a module that hits it fails the same way every
  time.
- A **wall-clock deadline** (`HTCONDORDB_WASM_TIMEOUT`). This is the backstop,
  and it is what stops a module that charges itself nothing.

Memory is capped by `HTCONDORDB_WASM_MEMORY_PAGES`.

## Trusting a module

`HTCONDORDB_WASM_<NAME>_TRUSTED = true` removes the wall-clock deadline and runs
the module uninterruptibly. That is most of the per-call cost: a scalar call
drops from roughly 780ns to 110ns.

What you give up is the ability to stop it. A trusted module that loops forever
hangs a goroutine and nothing can interrupt it. Set this only for a module you
built yourself, and note that nothing in a module can ask for it — trust is
configuration, and there is deliberately no way for a file to claim it.

Everything else still applies to a trusted module: the step budget, the memory
cap, and every check on what it hands back.

## Changing or removing one

**A loaded function cannot be replaced, changed or removed without restarting the
daemon.** `condor_reconfig` can only *add* a module.

The reason is that compiling a query records that a name is a user function, and
compiled queries are cached and shared, so a name that has been used cannot stop
existing. Rather than pretend, the daemon logs that the configuration changed and
keeps running the module it has. Check the log after a reconfigure if you edited
a path and the behaviour did not change.

## When something is wrong

A module that does not load **fails start-up**, naming the module and the file.
This is deliberate: a function you configured and that is not there would make
every query calling it answer `error`, which is worse than not starting.

A module that loads but then fails a call produces `error` for that value. The
first failure of each kind is logged with the reason — a trap, the step budget,
the deadline, a malformed result — and later ones are counted rather than
repeated.

## Writing a module

Any language that compiles to WebAssembly. The contract a module has to meet —
the exports, how values cross, the batch entry point that makes it fast — is
[the WebAssembly UDF ABI][abi] in the `classad` repository, with worked examples
beside it in [`wasmudf/testdata/`][testdata], one per point of the contract.

The short version: export a `memory`, a version, and `classad_udf_call`; values
cross as 16-byte tagged slots. An aggregate exports a different set of five
functions instead.

A module that exports the optional **batch** entry point is called once per batch
rather than once per record, which is worth roughly 28x. For a function that runs
over every row of a large table, that is the difference that matters.

## Beyond this page

[User-defined functions and aggregates][udf] is the library-side companion. It is
written for a Go program that embeds `classad` rather than for a daemon you
configure, so its mechanism is a function call where this page's is a knob — but
it is where the detail lives. Go there for the loader options this page does not
expose, including the call-frame and result-size bounds, how many instances of one
module may run at once, and the per-failure callback a host can install for logs
and metrics; for the five exports an aggregate module provides, which the ABI
specifies and this page only counts; and for the measured cost of each path, which
is what makes the case for batching and for trust.

[abi]: https://github.com/PelicanPlatform/classad/blob/main/docs/wasm-udf-abi.md
[testdata]: https://github.com/PelicanPlatform/classad/tree/main/wasmudf/testdata
[udf]: https://github.com/PelicanPlatform/classad/blob/main/docs/user-defined-functions.md
