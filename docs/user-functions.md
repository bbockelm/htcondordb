# User functions

The query language has a fixed set of functions and aggregates. You can add your
own: a file on disk, named in the configuration, loaded at start-up. After that
a query calls it like any built-in.

This is for the function your site needs and nobody else does — a scoring
expression, a site-specific name decoding, a percentile your reports want.

## Two kinds, and which to pick

A user function is either a **ClassAd Script** file or a **WebAssembly module**.
Once registered the two are indistinguishable to a query: same call, same name
folding, and the same three kinds of answer — a value, `undefined`, or `error`.

|                        | ClassAd Script                         | WebAssembly                                |
| ---------------------- | -------------------------------------- | ------------------------------------------ |
| Written in             | ClassAd Script                         | anything that compiles to WebAssembly      |
| What you need to build | nothing; it is a text file             | a compiler and a build                     |
| Argument count         | the `def` states it                    | you declare it in the configuration        |
| Can be an aggregate    | no                                     | yes                                        |
| Reaching the rest of the ad | refused at start-up, by the compiler | impossible; it is handed values and nothing else |
| What bounds a call     | a step budget fixed in the engine      | memory, steps, a deadline, all configurable |

**Start with a script.** There is nothing to install and nothing to compile, the
argument count cannot disagree with the function, and a function that reaches for
an attribute you did not pass it is refused at start-up with the attribute named
rather than answering `error` for the life of the daemon.

**Reach for WebAssembly** when you want an aggregate, which only a module can be;
when the function is one you already have in a language that compiles to wasm; or
when it runs over every row of a large table and the module's batch entry point
is worth the build.

## Put the same configuration where your clients read it

A query is evaluated in two places: a `WHERE` clause runs on the server, and a
`SELECT` item is computed by whatever asked the question — `htcondordb-cli`, or
the Python driver through its shared library. Both load the same files from the
same knobs, so if the file is only readable on the server, `WHERE f(...)` will
work and `SELECT f(...)` will not.

A client that cannot load a function warns and carries on, since the queries that
do not call it are unaffected.

## Adding a script function

```
HTCONDORDB_SCRIPT_FUNCTIONS         = effMemory
HTCONDORDB_SCRIPT_EFFMEMORY_PATH    = /etc/condor/scripts/policy.cas
```

The configured name is the name of the `def` to take out of that file:

```
def effMemory(requested, used):
    if used is undefined:
        return requested
    if used > requested:
        return used
    return requested
```

Restart the daemon and the function is available:

```sql
SELECT Owner, effMemory(RequestMemory, MemoryUsage) AS mem FROM history
```

**One file can back several functions.** Each name gets its own `_PATH`, and two
names may give the same path and pick a different `def` out of it, which is how a
site keeps its policy functions in one place:

```
HTCONDORDB_SCRIPT_FUNCTIONS         = effMemory scoreJob
HTCONDORDB_SCRIPT_EFFMEMORY_PATH    = /etc/condor/scripts/policy.cas
HTCONDORDB_SCRIPT_SCOREJOB_PATH     = /etc/condor/scripts/policy.cas
```

There is no `MIN_ARGS`. A `def` declares its parameters, so the registered
function has exactly that arity and there is nothing for the configuration to get
wrong. There is no memory cap, no timeout, no instance count and no `TRUSTED`
either: a script has no sandbox to relax. It cannot reach the machine, because the
language has no I/O, and it cannot reach the ad, because registration refuses a
function that does. What is left is a step budget, which the engine fixes and does
not expose — a function that loops forever ends that call with `error` rather than
hanging the scan.

## Adding a WebAssembly module

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

## What a function can do, and what it cannot

Neither kind can read an attribute you did not pass it, reach the rest of the ad,
or see the query. They get there differently.

A **module** gets argument values and nothing else. It also cannot reach the
machine: no files, no network, no clock, no randomness. That is not a restriction
applied to it — the host simply offers nothing to call, so a module compiled
against anything else fails to load and the log says what it asked for.

A **script** could in principle read the ad — inside a script a bare name is an
attribute, and that is the point of it there — so the compiler checks, and a
function that reads one is refused at start-up rather than registered. The message
names the function and the attribute it reached for; pass that value in as a
parameter. The language has no I/O at all, so there is nothing to say about files
or the network.

Two limits bound how long a **module** call can run:

- A **step budget** (`HTCONDORDB_WASM_STEPS`) the module charges itself against.
  It is deterministic: the same input runs out at the same point on a busy
  machine and an idle one, so a module that hits it fails the same way every
  time.
- A **wall-clock deadline** (`HTCONDORDB_WASM_TIMEOUT`). This is the backstop,
  and it is what stops a module that charges itself nothing.

Memory is capped by `HTCONDORDB_WASM_MEMORY_PAGES`.

A **script** call is bounded by a step budget alone, fixed in the engine and not
configurable. A call that exhausts it is `error`.

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

This is a WebAssembly knob. A script function has no deadline to drop.

## Changing or removing one

**A loaded function cannot be replaced, changed or removed without restarting the
daemon.** `condor_reconfig` can only *add* one, of either kind.

The reason is that compiling a query records that a name is a user function, and
compiled queries are cached and shared, so a name that has been used cannot stop
existing. Rather than pretend, the daemon logs that the configuration changed and
keeps running the function it has. Check the log after a reconfigure if you edited
a path and the behaviour did not change.

A name belongs to one function. A name in `HTCONDORDB_SCRIPT_FUNCTIONS` that is
also in one of the wasm lists is refused while the configuration is read, which
says which lists to look at.

## When something is wrong

A function that does not load **fails start-up**, naming the function and the
file. This is deliberate: a function you configured and that is not there would
make every query calling it answer `error`, which is worse than not starting.

For a script, the three ways that happens are a file that cannot be read, a
syntax error, and a function that reaches the ad. The third reads, in part,
`reads the attribute Memory: a registered function is given its arguments and
nothing else`, and the fix is in the message.

A function that loads but then fails a call produces `error` for that value. For
a module the first failure of each kind is logged with the reason — a trap, the
step budget, the deadline, a malformed result — and later ones are counted rather
than repeated.

## Writing a script

A file of `def`s, indented like Python, over the values and operators the query
language already has:

```
# Score a job: shorter queues first, with a floor so a long wait never scores
# below a short one.
def scoreJob(queued, requested):
    if queued is undefined:
        return error
    wait = queued / 3600.0
    if wait > 48.0:
        wait = 48.0
    return 100.0 - wait - requested / 1024.0
```

Statements are what a script buys over an expression: `if`, assignment to a local,
a loop. `return undefined` and `return error` are how a function says "no value"
and "no answer" — both are values in this language, and choosing between them is
the function's job. An attribute the ad does not carry arriving as `undefined` is
not a mistake, and a `Requirements` expression that treated it as one would reject
the machine instead of ignoring the term.

A body may call the module's other `def`s. Each call is expanded where it is
written, so a cycle of calls — one that would have to contain itself — is refused
at start-up with the cycle named.

The one thing that is not yours to choose is reading the ad. A bare name the body
does not bind is an attribute, which inside a script is the point of it; in a
registered function it is refused, because the function is given its arguments and
has no subject ad to resolve the name against. Pass the value as a parameter.

## Worked examples

Four functions a site plausibly writes, each doing something no built-in does.
They are the whole of one file, `/etc/condor/scripts/policy.cas`, which four
configured names pick out of:

```conf
HTCONDORDB_SCRIPT_FUNCTIONS          = cpuEfficiency wasteBand claimedCpus memBucket
HTCONDORDB_SCRIPT_CPUEFFICIENCY_PATH = /etc/condor/scripts/policy.cas
HTCONDORDB_SCRIPT_WASTEBAND_PATH     = /etc/condor/scripts/policy.cas
HTCONDORDB_SCRIPT_CLAIMEDCPUS_PATH   = /etc/condor/scripts/policy.cas
HTCONDORDB_SCRIPT_MEMBUCKET_PATH     = /etc/condor/scripts/policy.cas
```

For the language itself — the statements, how a local is bound, what `for` walks,
where a `def` may call itself — the reference is [User-defined functions and
aggregates][udf] in the `classad` repository. It is written for a Go program that
embeds the library, so the ClassAd Script material sits among Go registration
calls you have no use for here; the language half of it is what you want, and it
is the only place the language is written down in full.

The results below are the answers these functions give for one row of each table
they read:

```classad
# FROM history
[
  Owner = "alice"; ClusterId = 884;
  RequestCpus = 4; RequestMemory = 3000;
  RemoteWallClockTime = 3600; RemoteUserCpu = 2000; RemoteSysCpu = 160
]
```

```classad
# FROM machines
[
  Name = "slot1@ep042"; SlotType = "Partitionable"; Cpus = 16;
  ChildCpus = {4, 4, 8}; ChildState = {"Claimed", "Unclaimed", "Claimed"}
]
```

### A ratio, with the absent case chosen

How much of the CPU time a job was allocated it actually burned. A job that asked
for four cores and used one is a core-hour bill four times its work.

```
def cpuEfficiency(wallClock, cpuSecs, cpus):
    if wallClock is undefined:
        return undefined
    if cpuSecs is undefined:
        return undefined
    # A job that never said how many cores it wanted got one.
    if cpus is undefined:
        cpus = 1
    allocated = wallClock * cpus
    if allocated <= 0:
        return error
    return cpuSecs * 100.0 / allocated
```

The three absences are three different answers, and picking between them is the
function's job: a missing `RequestCpus` has a right default, a missing wall clock
has none, and a job with no wall clock at all is a question with no answer rather
than a job that was perfectly efficient.

```sql
SELECT Owner, ClusterId,
       cpuEfficiency(RemoteWallClockTime, RemoteUserCpu + RemoteSysCpu, RequestCpus) AS eff
  FROM history
 WHERE cpuEfficiency(RemoteWallClockTime, RemoteUserCpu + RemoteSysCpu, RequestCpus) < 20
```

```text
# history
cpuEfficiency(RemoteWallClockTime, RemoteUserCpu + RemoteSysCpu, RequestCpus)   15
cpuEfficiency(RemoteWallClockTime, RemoteUserCpu + RemoteSysCpu, NoSuchAttr)    60
cpuEfficiency(RemoteWallClockTime, NoSuchAttr, RequestCpus)                     undefined
cpuEfficiency(0, 100, 4)                                                        error
```

### Bands, so the conditional is written once

The same number, as a name you can group by. The nesting lives in the file
instead of in every query, and every report that groups by it cuts the bands in
the same place.

```
def wasteBand(wallClock, cpuSecs, cpus):
    eff = cpuEfficiency(wallClock, cpuSecs, cpus)
    if eff is undefined:
        return "unknown"
    if eff < 10.0:
        return "idle"
    if eff < 60.0:
        return "partial"
    return "busy"
```

A `def` calls the other `def`s in its file, which is the reason one file holds
several: the thresholds are a policy and the ratio is arithmetic, and only the
thresholds move.

```sql
SELECT wasteBand(RemoteWallClockTime, RemoteUserCpu + RemoteSysCpu, RequestCpus) AS band,
       COUNT(*)
  FROM history
 GROUP BY wasteBand(RemoteWallClockTime, RemoteUserCpu + RemoteSysCpu, RequestCpus)
```

```text
# history
wasteBand(RemoteWallClockTime, RemoteUserCpu + RemoteSysCpu, RequestCpus)   "partial"
wasteBand(3600, 100, 4)                                                     "idle"
wasteBand(3600, 12000, 4)                                                   "busy"
wasteBand(NoSuchAttr, 1, 1)                                                 "unknown"
```

### A loop over a list attribute

A partitionable slot carries one entry per dynamic slot cut from it, in parallel
lists: `ChildCpus` and `ChildState`. How many cores are committed is the sum of
one list where the other says `Claimed` — two lists walked together, which is
what a statement language buys over an expression.

```
def claimedCpus(childCpus, childState):
    # A static slot has no children, so nothing is committed below it.
    if childCpus is undefined:
        return 0
    total = 0
    i = 0
    for c in childCpus:
        if childState[i] == "Claimed":
            total = total + c
        i = i + 1
    return total
```

```sql
SELECT Name, Cpus, claimedCpus(ChildCpus, ChildState) AS busy,
       Cpus - claimedCpus(ChildCpus, ChildState) AS idle
  FROM machines
 WHERE SlotType == "Partitionable"
```

```text
# machines
claimedCpus(ChildCpus, ChildState)          12
Cpus - claimedCpus(ChildCpus, ChildState)   4
claimedCpus(NoSuchAttr, NoSuchAttr)         0
```

### A helper the query never names

Request sizes spread over every value a user typed, so a histogram of them has a
bar per value. Rounding each up to the next power of two gives a histogram with a
dozen bars that stay the same from month to month.

```
def bucketFrom(mb, bucket):
    if bucket >= mb:
        return bucket
    return bucketFrom(mb, bucket * 2)

def memBucket(mb):
    if mb is undefined:
        return undefined
    if mb <= 0:
        return error
    return bucketFrom(mb, 128)
```

`bucketFrom` doubles until it is large enough, calling itself in the position
where the call's value is the whole answer; `memBucket` is the one in the
configuration, and seeds it with the smallest bucket the site reports.

```sql
SELECT memBucket(RequestMemory) AS bucket, COUNT(*)
  FROM history
 GROUP BY memBucket(RequestMemory)
 ORDER BY bucket
```

```text
# history
memBucket(RequestMemory)   4096
memBucket(100)             128
memBucket(4096)            4096
memBucket(NoSuchAttr)      undefined
memBucket(0)               error
```

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
