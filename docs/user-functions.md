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
| Can be an aggregate    | yes; four `def`s                       | yes                                        |
| Can be stored in the database | yes, either role                | yes, either role                           |
| Reaching the rest of the ad | refused at start-up, by the compiler | impossible; it is handed values and nothing else |
| What bounds a call     | a step budget fixed in the engine      | memory, steps, a deadline, all configurable |

**Start with a script.** There is nothing to install and nothing to compile, the
argument count cannot disagree with the function, and a function that reaches for
an attribute you did not pass it is refused at start-up with the attribute named
rather than answering `error` for the life of the daemon.

**Reach for WebAssembly** when the function is one you already have in a language
that compiles to wasm, or when it runs over every row of a large table and the
module's batch entry point is worth the build.

Either kind can be an aggregate — the thing a `GROUP BY` names where `SUM` would
go.

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

## Adding a script aggregate

An aggregate is four `def`s threading a state value, and the configured name
picks them out of the file by prefix:

```
HTCONDORDB_SCRIPT_AGGREGATES        = jobMean
HTCONDORDB_SCRIPT_JOBMEAN_PATH      = /etc/condor/scripts/policy.cas
```

```
def jobMean_init():
    return {0, 0}

def jobMean_add(s, v):
    if isReal(v) || isInteger(v):
        return {s[0] + v, s[1] + 1}
    return s

def jobMean_merge(a, b):
    return {a[0] + b[0], a[1] + b[1]}

def jobMean_result(s):
    if s[1] > 0:
        return real(s[0]) / s[1]
    return undefined
```

It is then usable wherever `SUM` is:

```sql
SELECT Owner, jobMean(RequestMemory) FROM history GROUP BY Owner
```

The mean is the example worth copying because its state is a **sum and a count**,
and a single number cannot carry both. That is what a `def` buys you over a
`SELECT` expression: the state is an ordinary ClassAd value, so a list holds as
many numbers as the answer needs.

Each of the four does one thing, and the engine supplies their arguments, so each
has a fixed shape:

| `def`           | Takes                       | Returns                                         |
| --------------- | --------------------------- | ----------------------------------------------- |
| `<name>_init`   | nothing                     | the identity — the answer for a group with no rows |
| `<name>_add`    | the state, one row's value  | the state with that value folded in             |
| `<name>_merge`  | two states                  | the two combined                                |
| `<name>_result` | the state                   | the group's answer                              |

`add` sees **every value a row can hold**, including `undefined` for a row that
does not carry the attribute and `error` for one whose expression failed. Deciding
what to do with those is the aggregate's job, exactly as it is a built-in's: the
example above folds numbers and ignores everything else, so a group of five rows
two of which have no `RequestMemory` averages the three that do.

**One file backs several aggregates**, as it does several functions, because the
prefix keeps their definitions apart. A file whose definitions are spelled some
other way names them for the role each fills:

```
HTCONDORDB_SCRIPT_AGGREGATES        = p95
HTCONDORDB_SCRIPT_P95_PATH          = /etc/condor/scripts/percentile.cas
HTCONDORDB_SCRIPT_P95_INIT          = emptySketch
HTCONDORDB_SCRIPT_P95_ADD           = observe
HTCONDORDB_SCRIPT_P95_MERGE         = combine
HTCONDORDB_SCRIPT_P95_RESULT        = percentile95
```

Nothing is guessed. A `def` the configuration names and the file does not define
fails start-up, naming the `def` it looked for and the role that `def` fills, and
so does one whose parameter count does not match the role. `init` is **called** at
load too, so an aggregate whose identity is itself a failure is a start-up error
rather than a column of `error` later.

### Test the merge laws before you configure it

The reduction relies on three things no loader can check, because they are your
code:

- `merge` is **associative and commutative** — it gives the same answer whatever
  order and grouping the partial states arrive in.
- `result` is **repeatable** and does not change the state it is given.
- `add` gives the same answer whatever order it sees a group's values in, because
  a group is folded by several workers and each sees only part of it.

Getting this wrong is the bad kind of wrong. Which worker reads which part of the
table is a race, so an order-sensitive `merge` is right in every query you try and
wrong in production, plausibly, and only sometimes.

`db.CheckAggregate`, in the [`classad`][udf] library, tests an aggregate against
those laws over a sample of values — four values at least, since fewer cannot
split three ways. It is a Go call, so reach for it if your site builds against the
library; otherwise read the three laws above against your four `def`s before you
configure one, and pick an `add` that ignores what it cannot fold rather than one
that depends on what arrives first. Try the sample over the values your rows
really hold, including a string and an `undefined`, not only the numbers you wrote
it for.

### Where it runs

The aggregate runs on the **server**, which is where the rows are, so the file has
to be readable there. Configure the client too: it is the client's parser that has
to decide whether `jobMean(x)` is an aggregate call or an ordinary one before any
request exists, and a name it does not know is read as an ordinary function and
fails.

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

A name belongs to one function. A name that appears in more than one of the four
lists is refused while the configuration is read, which says which lists to look
at. An aggregate goes into a registry of its own, and shares the name space
anyway: a client's parser has to decide whether `p95(x)` is an aggregate call
before any request exists, so one name cannot be both.

## Storing a function in the database

A function can live in the database instead of in a config file, in the table
`user_functions`, one record per function. The daemon registers what the table
holds when it starts, so a stored function behaves exactly like a configured one
from a query's point of view — and unlike a configured one, **it can be replaced
while the daemon runs**.

```
htcondordb-cli function register pctUsed -file pct.cas
htcondordb-cli function register mean -kind wasm -file mean.wasm -arity 2
htcondordb-cli function register jobMean -file aggregates.cas -role aggregate
htcondordb-cli function replace pctUsed -file pct2.cas
htcondordb-cli function remove pctUsed
htcondordb-cli function list
```

A script states its own argument count in its `def`, so `-arity` is only needed
for a module, where the count is checked before the guest runs and cannot be
asked of it.

### Kind and role are two different things

`-kind` is what the source **is** — ClassAd Script text or a WebAssembly module —
and `-role` is what the name is registered **as**: a scalar function, or an
aggregate a `GROUP BY` names where `SUM` would go. Script and wasm each take
either role, so the two are separate and `-role` defaults to `function`.

A stored script aggregate is the same four `def`s as a configured one, picked out
of the source by the same prefix rule: `jobMean` is built from `jobMean_init`,
`jobMean_add`, `jobMean_merge` and `jobMean_result`. A file that spells them some
other way names each one — `-init`, `-add`, `-merge`, `-result` — so a file
written for `HTCONDORDB_SCRIPT_AGGREGATES` registers out of the table unchanged.

**An aggregate has no argument count.** It is named against one argument
attribute whatever it is, so `-arity` is refused rather than ignored: a count you
declared would constrain nothing, and nothing would tell you so.

**The merge laws are yours to keep.** `merge` has to be associative *and*
commutative and `result` has to repeat without disturbing the state. Nothing in
the daemon can check that — the samples that would check it are yours, since
values invented here would be integers for an aggregate over host names and would
prove nothing. Test one with [`db.CheckAggregate`][udf] before you register it,
and keep the test. The columnar pass fans out across segment windows, so an
order-sensitive `merge` is right in every test you write and wrong in production.

**Registering requires DAEMON or ADMINISTRATOR, not WRITE.** A user function runs
inside every query that names it, the name space is global and shared, and a name
that has been claimed cannot be given back — so whoever may register `pctUsed`
decides what that word means for everyone, permanently. That is a different
privilege from writing rows, which is where your analysts are. The table itself is
owned by the daemon and is read-only to every session, at every level: the command
above is the only way in.

### What a replacement may and may not change

**For a function, the argument count is fixed and everything else can change.**
Compiling a query records that a name is a user function *of a given arity*, and
compiled queries are cached and shared, so that much cannot be taken back. The
implementation behind the name can: `function replace` points it at new source and
the next call runs it, with no restart.

A replacement that changes the count is refused, naming both counts. Use a
different name, or restart the daemon, after which the name is registered afresh.

**For an aggregate, everything can change**, because there is no argument count to
keep. An aggregate name is resolved once per query, when the reduction is set up,
and nothing caches that resolution — so the registry takes a replacement outright.
A query already running finishes with the implementation it started with, and one
started afterwards reduces with the new code. Two queries seconds apart can
therefore compute different things, which is inherent in being able to correct a
definition at all; the checksum is what says which code answered.

**A name's role is fixed either way.** A name that is an aggregate and becomes a
function, or the reverse, is a different thing in a different registry answering a
differently parsed call, and pointing a name at new source does not describe that.
Remove it and register the other, or pick another name.

Each function carries a **checksum over its source**, which `function list` shows
and every registration and replacement reports. It is what makes an answer
explicable: a name says what was called, the checksum says which code answered,
and two daemons that loaded the same function separately agree on it.

### A stored module is never trusted

`HTCONDORDB_WASM_<NAME>_TRUSTED` is configuration and stays configuration. A
stored record asking for trust is loaded untrusted and told so, because a
capability a module can claim is a request rather than a capability. A stored
module is bounded exactly as an untrusted configured one is: the step budget, the
deadline, the memory cap, and — for a script — the purity gate.

### A stored module that does not load marks it bad

A *configured* module that does not load fails start-up. A *stored* one does not:
it is marked **bad** and the daemon starts anyway. Three things make that the
right way round. A stored record cannot be edited on the host, so a daemon that
refused to start could not be used to fix the record stopping it. A module can
stop loading without anyone touching it — a wasm guest valid under one runtime
version and rejected by the next — which would make an upgrade a coin toss. And
otherwise anyone who may register a function could stop the daemon starting.

A bad **function** is registered, under its stored argument count, and answers
`error` when called — the same answer a name that does not exist gives. It is not
left out, because leaving it out would change how the *call site* compiles, which
is the decision that cannot be revisited.

A bad **aggregate** is not registered at all, and the opposite answer has the same
cause: nothing captures an aggregate name, so there is no call-site shape to keep.
What is left to decide is which failure you would rather have. An aggregate that
errors for every group gives you a query that *succeeds* with a column of `error`
in it, which looks like an answer; an unregistered one fails the query and names
the aggregate, which is what db does with an aggregate nobody ever registered.

Either way bad is a state, not a silence. It is in the daemon's log at start-up,
in the record (`State` and `StateReason`), and in `function list`:

```
NAME      KIND             ORIGIN   ARGS  STATE  IDENTITY
jobMean   script aggregate stored   -     bad    sha256:4d1e7a08c33b
pctUsed   wasm             stored   2     bad    sha256:0f3a9c71be42

jobMean: scriptagg: the module does not define a usable aggregate: merge: no such def: jobMean_merge
pctUsed: wasmudf: pctUsed@0f3a9c71be42: invalid magic number
```

Replacing the source repairs either — for a function because the argument count is
unchanged, for an aggregate because the registry installs an absent name as
readily as a present one. A module that is bad because the *runtime* rejects it
will stay bad however often it is re-registered, and the reason says which of the
two it is.

`function remove` deletes the record, which stops the next restart registering it.
For a **function** it does not give the name back in the running daemon: the name
stays registered and answers `error`, and the listing says so, until a restart.
For an **aggregate** it does: the registration is withdrawn, a query naming it
fails as it would for a name nobody ever registered, and the name is free
immediately — for an aggregate, for a function, or for the configuration.

### One name space

Configured and stored functions share one name space, because a query calling a
name reaches one function and does not know where it came from. A configured name
is claimed first, at start-up; a stored record asking for a name the
configuration holds is marked bad saying so, and a reconfigure that adds a name
the table already holds is reported rather than applied. `function list` shows
both, with an `ORIGIN` column.

**Aggregates are in it too**, although they go into a registry of their own: a
client's parser has to decide whether `jobMean(x)` is an aggregate call before any
request exists, so one name cannot be both. `COUNT`, `SUM`, `AVG`, `MIN` and `MAX`
are not available either — a parser reads those as built-in aggregates before it
consults any registry, so a registration under one of them would be unreachable.

### What clients do with a stored function

Nothing, yet. A `SELECT` item is computed by whoever asked the question, so a
client still loads its own functions from its own configuration: a stored
function works in a `WHERE` clause, which the server evaluates, and a `SELECT`
item naming it answers `error` on a client that has not configured it. Whether a
client fetches stored modules from the server — and whether it should be able to
refuse to — is a separate question, with its own trust decision, and it is not
settled here.

## When something is wrong

A function that does not load **fails start-up**, naming the function and the
file. This is deliberate: a function you configured and that is not there would
make every query calling it answer `error`, which is worse than not starting.

For a script, the three ways that happens are a file that cannot be read, a
syntax error, and a function that reaches the ad. The third reads, in part,
`reads the attribute Memory: a registered function is given its arguments and
nothing else`, and the fix is in the message.

A script aggregate has three more, all of them about the four `def`s: one the
configuration names and the file does not define, one whose parameter count does
not fit the role it fills, and an `init` that does not produce an identity. Each
message names the `def` and the role, so it says which line of the file to open.

A *stored* module that does not load does not fail start-up: it is marked bad and
says so in the log, in its record and in `function list`. A bad stored function
answers `error`; a bad stored aggregate is not registered, so a query naming it
fails.

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
