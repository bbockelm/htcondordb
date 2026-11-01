# Design sketch: Arrow output, and how far up the Flight stack to climb

**Status:** the type-inference layer (§5) is implemented in `arrowtype/`; the Flight and transport
sections below remain a sketch. Measurements in §3 stand as taken.
**Grounded in:** `repl/sql.go`, `repl/exec.go`, `repl/rowstream.go`, `grafana/pkg/plugin/frame.go`,
and in classad's `collections/adschema.go`, `collections/vm/vecexec.go`, `dbrpc/proto.go`,
`db/aggregate.go`. Binary sizes in §3 are measured, not estimated; §3.1 says how.
**Audience:** whoever implements this, in htcondordb and in the classad storage/dbrpc layers.
Assumes familiarity with the columnar segment layout and the vectorized matcher, not with
Arrow Flight.

## 1. Goal, and two findings that set the shape

Make htcondordb tables (the live job queue, history archives, collector ads) consumable by
the Arrow ecosystem — DuckDB first, then pandas/polars and the ADBC/JDBC client family — so
that joins, window functions, `PIVOT` and CSV/Parquet export work against pool data without
an export step.

Two findings reorder the obvious plan.

**Finding 1: DuckDB never sends SQL to a Flight server.** Via the Airport extension, DuckDB
parses, binds and plans the query itself; the server receives a catalog listing, a column-id
list, and a *serialized DuckDB bound expression tree* as JSON. No SQL string is on the wire
at any point. So an ANSI SQL dialect is **not** a prerequisite for DuckDB. It *is* a
prerequisite for Arrow Flight **SQL**, a different and standard protocol, which reaches far
more clients (§10).

**Finding 2: Arrow is cheap; gRPC and Flight are not.** Measured against this daemon
(§3): the Arrow array and IPC packages add **+3.1 MB (+9.6%)**, while gRPC + the Flight
protobufs + msgpack add a further **+11.2 MB (+35% of base)**. The expensive dependency is
the *transport*, not Arrow.

Together these say: get Arrow out of the daemon over the transport we already have, and put
Flight — if we want it at all — somewhere else.

## 2. Protocol choice: why not just Arrow Flight, and why Airport at all

The reason Airport comes up is narrow: **DuckDB has no built-in Arrow Flight client.** Plain
Flight is not a thing DuckDB can `ATTACH`. So "serve Arrow Flight directly" and "serve
DuckDB" are different goals, and there are four rungs, not one.

| Rung | What we serve | Extra deps | Who can read it | Who does the filtering |
|------|---------------|-----------|-----------------|------------------------|
| **0** | Arrow IPC over the existing dbrpc/CEDAR stream | arrow arrays+IPC (**+3.1 MB**) | our CLI/Python/Go clients; DuckDB via the `nanoarrow` community extension's `read_arrow()` on a file or stream buffer; pyarrow → DuckDB zero-copy; pandas/polars | **us**, in our own language — full existing power: ClassAd constraints, server-side aggregates, TopK, AS OF |
| **1** | Standard Arrow Flight (`ListFlights`/`GetFlightInfo`/`DoGet`) | + gRPC + Flight protos (**+11.2 MB**) | any Flight client, any language | us — the ticket format is ours to define |
| **2** | Arrow **Flight SQL** (standard, Arrow project) | same as rung 1, plus the dialect work | ADBC (Python/Go/Java/R), the Flight SQL JDBC driver, BI tools; DuckDB via its own ADBC extension | **us**, from SQL text we receive |
| **3** | Airport (Flight + custom `DoAction`s) | rung 1 + msgpack + DuckDB's expression JSON | DuckDB only | **DuckDB's planner**, pushed to us as serialized expressions |

Reading that table changes the recommendation I would have made from the DuckDB goal alone:

- **Rung 0 delivers most of the practical value for 9.6% of the binary and almost none of the
  protocol risk.** No gRPC, no Flight protobufs, no msgpack, no community extension, no
  unversioned expression format. And critically, the *client asks in our language*, so our
  constraint engine, server-side `GROUP BY`, TopK and time travel all stay reachable.
- **Rung 1 buys essentially nothing over rung 0 for the DuckDB goal.** It is network
  convenience for non-DuckDB Flight clients. Skip it as a destination; it falls out of rung 2.
- **Rung 2 is the right standard target.** It is a versioned Arrow-project protocol, it reaches
  the widest client set, and it hands us **SQL text** — which plays to this store's strengths
  (we have a dialect, a planner, pushdown and `EXPLAIN`) rather than against them.
- **Rung 3 is the only way to get DuckDB `ATTACH` plus planner pushdown**, and it is also the
  most coupled option on every axis: a custom msgpack protocol, a community extension, an
  expression serialization that DuckDB's own documentation reserves the right to change in any
  release, and a server role reduced to *dumb scan source* — our constraint engine and TopK
  become unreachable (there is no LIMIT in Airport's endpoint parameters at all). The docs site
  currently serves an expired TLS certificate, which is a fair signal about maintenance posture
  even though the extension itself is MIT and active.

**Recommendation: 0, then 2, with 3 as an optional adapter if it is ever wanted.** Rung 3 is
not wrong — it is the only route to a genuinely seamless `SELECT ... FROM ospool.jobs.queue` —
but it should be the thing we add once the Arrow encoder and schema contract (which every rung
needs) are already proven, and it should never be the reason the daemon grows a gRPC stack.

### 2.1 What rung 0 actually looks like to a user

```sql
-- DuckDB, with the nanoarrow community extension
INSTALL nanoarrow FROM community; LOAD nanoarrow;
SELECT Owner, count(*) FROM read_arrow('jobs.arrows') GROUP BY Owner;
```
```python
# or no extension at all: pyarrow -> DuckDB, zero copy
import duckdb, htcondordb
tbl = htcondordb.connect().arrow("SELECT Owner, Cpus FROM jobs WHERE JobStatus == 2")
duckdb.sql("SELECT Owner, sum(Cpus) FROM tbl GROUP BY Owner")
```

The filtering, grouping and limiting happen in the daemon, in the language the daemon is good
at. What crosses the boundary is a typed Arrow batch.

## 3. Measured binary cost

One pinned dependency set, three arms differing only by which source file is present, built
from an exact copy of `cmd/htcondordb` (the no-Arrow arm reproduced the shipped daemon
byte-for-byte, confirming the copy is faithful). darwin/arm64, `go1.26.0`, unstripped, no
`-ldflags` beyond the version stamp — matching `make daemon`.

| Arm | Size | Delta |
|-----|------|-------|
| daemon as-is | 31,842,514 | — |
| + Arrow arrays + IPC (rung 0) | 34,900,178 | **+3,057,664 (+9.6%)** |
| + Flight + gRPC + msgpack (rungs 1–3) | 46,063,954 | **+11,163,776 more (+35.1% of base)** |
| full stack | 46,063,954 | +14,221,440 (+44.7%) |

**Measured again once it was really wired in.** The table above comes from synthetic arms that
link the dependencies without using them. With `flightsvc` actually serving from the daemon --
flightsql, the catalog schemas, the per-identity session machinery -- the daemon goes from
35,803,010 to 51,962,834 bytes: **+16.2 MB (+45.1%)**, against the +14.2 MB the synthetic arm
predicted. The estimate was good to about two megabytes and low, which is the direction an
estimate from a program that merely imports a library usually errs.

**No cgo.** Verified, not assumed: `arrowtype`, the daemon, and the full stack (arrow + array
+ ipc + flight + flightsql + grpc + msgpack linked into a copy of the daemon) all build with
`CGO_ENABLED=0`, and every package we would use declares zero cgo files. The only cgo in
arrow-go is `arrow/cdata`, the C Data Interface for handing arrays to C or Python Arrow
in-process, which we do not import. So static builds, musl images and the RPM cross-build are
unaffected.

The `modernc.org/sqlite` version arrow-go drags in points the opposite way from how it looks:
modernc's SQLite is the *pure-Go* one, C transpiled to Go precisely to avoid cgo. It is also
go.mod-only -- sqlite appears in the dependency tree of none of the packages we use (flightsql,
flight, ipc, array all report zero), because arrow-go needs it for its own Flight SQL example
server. MVS raises the version; nothing links it.

Notes that matter for interpreting these:

- The daemon **already links protobuf** (2,826 symbols, via `prometheus/client_golang` →
  `client_model`), so the gRPC figure is already net of the protobuf runtime. A naive
  measurement against a toy baseline over-counts by ~3.6 MB.
- Arrow's cost is lower here than in isolation (~6.6 MB on a bare baseline) because the daemon
  already links compression and hashing code Arrow shares.
- Width-6 integer columns have no Arrow equivalent and widen to `int64`; that is a runtime
  copy, not a size concern.

### 3.1 Reproducing this (and the trap in it)

Copy the non-test files of `cmd/htcondordb` into a scratch module with
`replace github.com/bbockelm/htcondordb => <repo>`, add the Arrow/Flight source files, and
build with `GOWORK=off` (a worktree otherwise inherits `go.work`).

**The trap:** running `go mod tidy` between arms silently bumped `modernc.org/sqlite`,
`modernc.org/libc`, `protobuf` and `x/net`, which made the *larger* arm measure 686 KB
**smaller** than the baseline. Tidy once, with every arm's imports present, then build each arm
against that single frozen `go.mod`/`go.sum` and diff `go version -m` output across the
binaries to confirm the only version differences are the modules under test.

## 4. What the code already gives us

Types exist at the bottom of the stack and at the top, and are **erased in the middle**. That
erasure is the whole engineering problem, and it is the same problem at every rung.

**At the bottom — already Arrow-shaped.**

- `adField` (`collections/adschema.go:48`) carries per-column `kind` (`akBool`/`akInt`/
  `akReal`/`akString`), `width` (int: 1/2/4/6/8; real/string: 8; bool: bit-packed), `unsigned`,
  and offset — plus a per-record **escape bitmap** whose bit index is the field's position in
  `s.fields`. This is an Arrow schema with different field names.
- `vm.Vec` (`collections/vm/vecexec.go:60`) is close to an Arrow array already: `Raw []byte` +
  `RawWidth` is a fixed-width Arrow buffer; `Dict` is an Arrow `DictionaryArray` (fold-sorted,
  which Arrow permits via its ordered flag); `Hi/Lo` bitplanes are three-valued logic, i.e. a
  nullable Arrow boolean; `St []uint8` (`VsInt`/`VsReal`/`VsBool`/`VsUndef`/`VsError`/
  `VsString`/`VsDictCode`, `vecexec.go:48`) is nearly a validity bitmap.
- A real is stored as `math.Float64bits` in the same int64 slot, so loading a real column is a
  copy, not a conversion — the property that makes a columnar Arrow encoder cheap.

**At the top — typed.** `RowStreamer` (`repl/rowstream.go:32`) hands back
`func(*classad.ClassAd) []classad.Value`, and `classad.ValueType` distinguishes undefined /
error / bool / int / real / string / list / nested ad.

**In the middle — strings.** `QueryRaw*` returns `[]string` of ad text;
`AggRow{Group []string, Values []string}` (`db/aggregate.go:64`); `repl.Result.Rows` is
`[][]string` (`repl/exec.go:340`). Visible consequences today: the Grafana plugin re-derives
types by parsing every cell and bailing on the first non-numeric (`inferField`,
`grafana/pkg/plugin/frame.go:48`) with a hardcoded epoch-attribute list (`knownTimeAttrs`,
`frame.go:15`), and the Python driver documents `description` as "what arrived, not a schema."

So this is not "add types." It is **stop discarding them**.

**What does not exist.** `adKind` is private to package `collections` and never escapes.
`GroupSchemaInfo` (`collections/colgroupschema.go:63`, via `db.DB.GroupSchemas`,
`db/db.go:1205`) reports attribute *presence* fractions but **no types**. The declared Arrow
schema needs a new introspection path out of `collections` → `db` → `dbrpc` (§5.7).

## 5. The schema contract (the hard part, and it is rung-independent)

Arrow fixes a schema for a whole stream. A schema derived from whichever rows happened to show
up is a bug factory, and this repository has already been bitten twice by the general shape:
expression-valued columnar fields read as UNDEFINED by fast COUNT paths (inflating counts), and
persisted field order re-derived from process-local intern ids. So: **declare the schema, do
not infer it** — and keep the declaration with the columnar schema, which already carries most
of what it needs.

Three decisions govern everything below:

1. **Arrow carries evaluated values**, never unevaluated expressions (§5.1).
2. **Coerce exceptional values where the coercion is exact**; null only what cannot be
   coerced (§5.3).
3. **Pick the Arrow type that minimizes exceptions**, even where storage chose a narrower one
   (§5.2).

The effect of all three is that the storage schema and the Arrow schema are deliberately *not*
the same schema. Storage optimizes bytes on disk and accepts exceptions as the price; Arrow
optimizes for a column that is wholly one type, because that is the only thing a consumer can
use. Same source of truth, different objective function.

### 5.1 Arrow columns carry evaluated values

`nodeKind` (`collections/adschema.go:139`) classifies a computed expression as `akNone` — it is
not a storable scalar, so an expression-valued attribute is an **escape**, held in the record
rather than the fixed slot. For Arrow we evaluate it and encode the result.

This has three consequences worth stating:

- **Evaluation needs the ad, not just the column.** An expression's own references are expanded
  transitively while decoding, and `vm.ReadPlan`'s seed set is not closed over them — the
  reason `colproject` refuses to serve a narrowed ad to a match that reads attributes
  (`collections/colproject.go`). So a record with an expression escape in a projected column
  forces a row reassembly for *that record*. Escapes are rare by construction, so this is a
  slope, not a cliff; but the encoder must be able to fall back per record, and the cost should
  show up in the scan stats rather than being invisible.
- **An evaluated expression is usually not an exception at all.** `WallClockTime` stored as an
  expression that evaluates to an integer is an integer. This is most of why decision 1 matters:
  it converts a large class of apparent type exceptions into ordinary values.
- **Evaluation is not necessarily stable.** An expression referencing `CurrentTime` yields a
  different value on each read, so an Arrow batch is a snapshot of evaluation, not of stored
  state. That matches how every other read of this store behaves, but it means two Arrow dumps
  of an unchanged table can legitimately differ.
- **Evaluation MUST go through the redacted path for an unprivileged reader.** This is a
  security requirement, not a detail. `db.PrivateConstraintRef`'s own comment records the hole
  it cannot close: an ad may store a *public* attribute whose value is an expression over a
  private one (`Leak = ClaimId`). A constraint on `Leak` references nothing private and passes
  the gate, while evaluation resolves `ClaimId` -- and the stated fix is that the private value
  must be ABSENT AT EVALUATION TIME for such a reader, which the redacted decode walk already
  arranges for rendering. An encoder that evaluates with the private values present turns every
  Arrow read into the oracle `refusePrivateConstraint` exists to prevent, and it is the same
  hazard class as the wire scan that shipped plaintext of encrypted attributes once already.
  So: evaluate with the reader's own view, never the privileged one, and make that the default
  rather than a flag someone can forget to set.

**Corollary — do not promote predominantly-expression attributes to typed columns.**
`Requirements` evaluates to a boolean, and that boolean is almost never what anyone wants; the
*text* is the point. Attributes whose stored form is `akNone` across the sample stay out of the
typed schema and live in the tail column (§5.6), which preserves source text. So the division
is: **typed columns hold evaluated values; the tail column preserves what was written.**

### 5.2 Choosing the Arrow type: widen to minimize exceptions

`chooseIntWidth` (`collections/adschema.go:113`) picks the smallest width covering **≥ `fit`**
of the sampled values — 0.95 for a base-schema column, 0.999 for a hot one
(`colscan.go:638`) — and deliberately accepts the rest as per-record exceptions, because two
bytes per record per column is worth real disk.

For Arrow that trade does not exist: the width buys almost nothing (IPC compresses, and DuckDB
widens to `BIGINT` for arithmetic anyway) while every exception costs an escape read and a
coercion. So the Arrow width is chosen to cover **every** value, not a fit fraction:

- **Integer columns default to `int64`.** A value that escaped a 2-byte storage column is still
  an integer; widening makes it an ordinary value and the exception count goes to zero.
- **Narrowing is an optimization that needs an exact bound, which we do not have yet.** Zone
  maps hold per-segment `[min,max]` but only for configured `ZoneAttrs`, and they hold them as
  **float64** (`zoneRange{Min, Max float64}`, `collections/zonemap.go`), which cannot bound an
  int64 column exactly above 2^53. If narrowing is wanted later, the describe-table op should
  track an exact integer min/max per column at seal time — sealing already walks every value, so
  it is close to free there and cannot be reconstructed cheaply afterwards.
- **Signedness follows the data, not the storage flag.** `adField.unsigned` is per-segment; if
  any segment holds a negative value, the Arrow type is signed.
- **Width 6 has no Arrow equivalent** and becomes `int64` regardless.

The same principle applies one level up, to the *kind*: if declaring an `akInt` column as
`double` would eliminate more exceptions than it creates, do that — **but only when every
integer value in the column round-trips through float64 exactly.** `vm.Vec` keeps the int64
payload specifically to preserve integers above 2^53, and silently trading that away at the
Arrow boundary would discard the property the storage layer went to trouble to keep. Without
an exact bound (above), "every value round-trips" is not provable, so a genuinely mixed int/real
column stays Tier C (§5.4) until the bound exists.

### 5.3 The coercion lattice

Applied per escaped value, after evaluation. "Exact" means the coercion is information-
preserving and reversible; anything else is counted, and refused under strict mode (§5.5).

| Declared column | Escaped value | Action | Exact? |
|---|---|---|---|
| `double` | integer | widen to float64 | exact for \|v\| < 2^53; **counted as lossy** above |
| `int64` | boolean | 1 / 0 | exact (matches `colagg`'s existing `akBool` handling) |
| `double` | boolean | 1.0 / 0.0 | exact |
| `int64` | real with integral value | truncate to integer | exact |
| `int64` | real with fractional value | **no coercion** — see §5.2 on widening the column, else null | — |
| `utf8` | any scalar | ClassAd unparse | exact (text is what `utf8` means here) |
| any | undefined / attribute absent | **null** | not a coercion; this is the validity bitmap |
| any | ERROR | **null**, counted *separately* from undefined | — |
| any numeric | list / nested ad | null (or unparse if the column is `utf8`) | — |

**What we deliberately refuse to coerce: strings into numbers.** Parsing `"42"` into an
`int64` column is the bug class this codebase has already hit, where a stringified number was
written into an attribute and read back as the wrong type. A string in a numeric column means
the data is heterogeneous, which is information — it belongs in the exception count and, if it
is common, in a Tier C declaration. Silently parsing it would make a type error look like clean
data, which is precisely what decision 2 is trying to avoid. (Numeric → `utf8` is fine and goes
the other way: `utf8` is a faithful representation of any ClassAd value.)

### 5.4 The residual tiers

With §5.1–5.3 absorbing most of what used to be exceptional, only three outcomes remain:

| Tier | Condition | Arrow type |
|---|---|---|
| A | one kind after evaluation; every value representable or exactly coercible | `int64` / `double` / `bool` / `utf8`, non-nullable if presence is total |
| B | as A, but some values are absent, undefined, ERROR, or lossily coerced | same type, **nullable** |
| C | kinds genuinely disagree and no exact widening unifies them | `utf8` holding the unparse of the evaluated value |

Tier C is now a statement about the *data* ("this attribute really does hold both strings and
numbers"), not an artifact of how storage fitted a width. That is the point of the three
decisions: the tier should describe the pool, not our encoder.

**Measured, Tier C is empty** (§5.8): after evaluation, every mixed attribute on a real AP is
int/real, and those are `double`. Keep the tier -- it is the honest answer for data that really
is mixed, and an empty tier costs nothing -- but do not build the machinery around it as though
it were the common case, and do not let §5.3's "no exact bound, so Tier C" rule stand in for the
bound. Without the bound, `BytesRecvd`, `CumulativeSlotTime` and `CpusUsage` all land in Tier C
wrongly.

### 5.5 Exceptions must be measurable, and strictness must be available

A column that silently nulls 3% of its values is the failure mode where a server-side clamp hid
a unit bug: plausible output, no error. So the encoder counts, **per column per stream**, and
reports in stream metadata, `EXPLAIN` and the query log:

- values coerced exactly (free, informational),
- values coerced **lossily** (an int64 above 2^53 into a `double`),
- values nulled, split into absent / undefined / ERROR / uncoercible-type,
- records that fell back to row reassembly to evaluate an expression (§5.1).

A **strict mode**, per table or per session, fails the read on any lossy coercion or
uncoercible value instead of nulling. Default lenient; strict is what a correctness
investigation uses, and what a scheduled export should set.

### 5.6 `SELECT *` is "schema columns plus a tail column"

A jobs table has thousands of distinct attributes across its rows; the union is useless as an
Arrow schema and expensive to serialize. Mirror the storage layout instead: the
declared/columnar fields become typed Arrow columns, and everything else — including the
predominantly-expression attributes of §5.1 — becomes **one** column, `_ad` as `utf8` ClassAd
text (simplest) or `map<utf8, utf8>` (queryable from DuckDB, larger). This falls out of the
base-schema-plus-cold-tail layout we already have, preserves source text for everything not
promoted, and gives users a correct escape hatch rather than a schema that lies by omission.

### 5.7 Where the declaration lives, and how it invalidates

Per the decision to keep this with the columnar info: a **per-table Arrow type map**,
`attr -> {arrow type, nullable, strict?}`, stored alongside the columnar schema and sharing its
**generation counter**. Seeded from the introspection below; overridable by an admin.

- The **epoch-to-timestamp mapping becomes a declaration**, not a guess. `knownTimeAttrs`
  (`frame.go:15`) is the right instinct with the wrong mechanism: seed from it, let an admin add
  `MyCustomDate`, advertise those columns as `timestamp[s]` so date functions work downstream.
  Map the key attribute to the rowid role.
- The generation counter is what a rung-3 catalog hashes (§6), so DuckDB's `airport_cache`
  invalidates exactly when the advertised schema changes and never otherwise.

**A schema change must not silently retype a column mid-flight.** The columnar schema evolves
(demand-driven promotion, hot-column refit, group-schema derivation), and a stream already in
progress must keep the type it announced — pin the generation for the life of a stream and let
the next one pick up the new map.

**New dbrpc op** (next free opcode is 66; highest in use is `opArchiveAggregateStats = 65`,
`dbrpc/proto.go:244`): *describe table*, returning per attribute
`{kind-after-evaluation, exact integer min/max, unsigned, presence fraction, escape fraction
by cause, cardinality estimate}`. Requires exporting kind information out of `collections`
(`adKind` is package-private today and never escapes). Cardinality and range estimates come
from the value-index histograms already maintained for the planner — which also, nearly for
free, answers Airport's optional column-statistics action later.

### 5.8 Measured: what ap2001 actually holds

The tiers above were written before anyone had looked. `AttrTypeProfiles` (classad #439) was
pointed at 2000 ads sampled from each of ap2001's `jobs` and `history` tables -- pulled with an
ordinary `SELECT ... LIMIT` and profiled in a LOCAL store, because a novel read shape does not
go at a production daemon.

| | `jobs` | `history` |
|---|---|---|
| distinct attributes | 336 | 242 |
| more than one stored class, counting undefined as one | 17 | 12 |
| more than one *value* class (undefined/error excluded) | **10** | **10** |
| integers past 2^53 | **0** | **0** |
| NaN reals | **0** | **0** |
| predominantly expression-valued | 8 | 6 |

The first two rows are the §5.4 point made empirically: counting stored `undefined` as a
competing class invents 7 mixed attributes in `jobs` and 2 in `history` that are not mixed.

**Every heterogeneous attribute fell into one of three patterns, and all three resolve without
a fallback type.**

1. **int/real mixing -- the large majority.** `BytesRecvd` 67 int to 1933 real, `BytesSent`
   1948 to 52, `CpusUsage` 2 to 1998, `Rank` 1 to 1999, plus `CommittedSlotTime`,
   `CumulativeSlotTime`, `GPUsAverageUsage`, `NetworkInputMb`, `NetworkOutputMb`. This is
   mostly not heterogeneous data at all: ClassAd writes an integral float without a decimal
   point, so a genuinely float-valued attribute arrives as a mix of int and real NODES. It is
   safely `double` -- and it is safe *because* no integer is past 2^53, which is exactly what
   §5.2's exact-bounds measurement is for.
2. **scalar + `undefined`.** `MachineAttrGLIDEIN_ResourceName0` and `..._Site0` are 25 strings
   against 1975 `undefined` in history. These are uniformly string attributes that usually have
   no value, not mixed ones -- see §5.4 on why undefined is a null and not a type.
3. **scalar + expression.** `Requirements` is 1995 expressions to 5 booleans; `RequestMemory`
   and `RequestDisk` are 1994 integers to 1 expression; `MemoryUsage` is 1996 expressions to 4
   integers. Under §5.1 these evaluate, and the evaluated type is uniform.

Of the 10 residual in each table, `jobs` splits 3 int/real to 7 scalar+expression and
`history` splits 8 to 2. Apply §5.1's evaluation and the expression ones collapse, leaving
**int/real as the only real heterogeneity: 3 attributes in `jobs`, 8 in `history`** -- all of
them `double`.

**So Tier C may be empty in practice.** §5.3's conservative rule -- a mixed int/real column
goes to `utf8` because the round-trip cannot be proven -- would have sent `BytesRecvd`,
`CumulativeSlotTime` and `CpusUsage` to string columns. The exact bound is what rescues them,
which upgrades it from an optimization to the thing that makes the common case work. Build it.

**Then measured again, after evaluation.** The paragraph above used to end by saying pattern 3's
resolution was a claim about the encoder rather than something the sample proved. So the same
ads were re-profiled by *evaluating* each attribute and classifying the result:

| | `jobs` | `history` |
|---|---|---|
| attributes | 337 | 243 |
| evaluated to a **single** value class | 330 | 232 |
| evaluated **mixed** (>1 value class) | **4** | **8** |
| evaluated to undefined/error in every ad | 3 | 3 |

**Every mixed-after-evaluation attribute is int/real.** Not one string/number mix, not one
list/scalar mix, in either table. `Rank`, `CommittedSlotTime`, `CumulativeSlotTime`,
`RequestDisk` in `jobs`; `CpusUsage`, `BytesRecvd`, `BytesSent`, `CumulativeSlotTime`,
`CommittedSlotTime`, `GPUsAverageUsage`, `NetworkInputMb`, `NetworkOutputMb` in `history`. So
**Tier C is empty on this data**, and §5.1's evaluation is what empties it: the
expression-valued attributes collapse exactly as predicted -- `MemoryUsage` evaluates to an
integer in all 2000 history ads (it is an expression in 1996 of them), `RequestMemory` likewise,
`OnExitHold` to a boolean.

**And the §5.1 corollary is confirmed quantitatively.** `Requirements` evaluates to a boolean in
1685 of 2000 `jobs` ads -- plus **141 errors** and 174 undefined, because evaluating a match
expression with no machine to match against is not a meaningful question. A typed column for it
would be 85% meaningless booleans and 15% null. Its *text* is what anyone wants, which is why
predominantly-expression attributes stay out of the typed schema and live in the tail column.

**What this still does not establish.** One AP, one moment, 2000 ads per table. `BytesSent`
exceeding 2^53 is 9 PB in a single job -- implausible now, not impossible, and the guard costs a
comparison, so it stays. The evaluation here ran with the ad as its own scope and no target,
which is what an Arrow encoder would do; an attribute whose value depends on a target would
evaluate differently in a matchmaking context, and none of this says what SHOULD happen there.
And it was evaluated with full privileges, so it says nothing about the redacted path §5.1
requires.

## 6. The rung-3 wire contract, for when we get there

Recorded here so it does not have to be rediscovered; **not** needed for rungs 0–2. Everything
is msgpack-serialized C++ structs; `FlightInfo` and schemas are Arrow IPC. Pin these against a
real DuckDB build rather than trusting this summary.

- **`DoAction("list_schemas")`** (required). In: `{catalog_name}`. Out: msgpack
  `{length, data}`, `data` = zstd-compressed msgpack of `{contents, schemas[]}`; each schema's
  `contents` is `{sha256, url?, serialized?}` wrapping a zstd msgpack array of serialized
  `FlightInfo`s, one per table. Each `FlightInfo.app_metadata` holds msgpack
  `SerializedFlightAppMetadata{type: "table"|"table_function"|"scalar_function", schema,
  catalog, name, comment, input_schema?, action_name?, description?}`. **Caching is keyed on the
  SHA256** — if present, DuckDB caches on disk indefinitely and will not re-fetch, so the hash
  must change exactly when the schema does (§5.7).
- **`DoAction("endpoints")`** (required). In: `{descriptor, parameters{json_filters,
  column_ids, table_function_parameters, table_function_input_schema, at_unit, at_value}}`.
  Out: msgpack `vector<string>` of serialized `FlightEndpoint`s, fetched **in parallel** by
  DuckDB's thread pool; their union is the whole flight. **All endpoints must therefore read the
  same snapshot** — embed a snapshot/AS OF token in every ticket. `at_unit`/`at_value` map onto
  our AS OF reads (`Statement.AsOf`, `repl/sql.go:209`); very few Flight servers can honor it.
  There is **no LIMIT** here, so our server-side TopK (`dbrpc/topk.go:26`, via `tryTopK`,
  `repl/exec.go:1411`) is unreachable from DuckDB.
- **Identifier case.** DuckDB does not fold identifiers before sending: `SELECT * FROM FooBar`
  asks for `FooBar`. The server must match DuckDB's case-insensitive behavior itself. ClassAd
  attribute names are already case-insensitive (`classad/evaluator.go:490`); table names in our
  catalog are not, so fold there.

## 7. Pushdown (rung 3 only)

`column_ids` maps onto existing projection pushdown (`projectionAttrs`, `repl/exec.go:863`).
`json_filters` is `{filters: [...], column_binding_names_by_index: [...]}` over DuckDB bound
expressions: `BOUND_COMPARISON` (`COMPARE_EQUAL`, `COMPARE_GREATERTHAN`, ...),
`BOUND_CONJUNCTION` (`CONJUNCTION_AND`/`OR`), `BOUND_OPERATOR` (`COMPARE_IN`),
`BOUND_COLUMN_REF`, `BOUND_CONSTANT`, `BOUND_FUNCTION` (`name: "prefix"`, `"length"`, ...).

**Airport re-filters on the client**, so a translator that handles what it understands and
**drops** what it does not is correct, merely slower. That is what makes rung 3 tractable at
all. But the rule has one sharp edge:

- unknown `expression_class`, function or type → **drop that filter**; never fail a read;
- a dropped conjunct inside an `AND` is fine (we return a superset); a dropped conjunct inside
  an `OR` means **the whole disjunction must be dropped**, not one arm. This is the easiest
  place in the design to introduce silent wrong answers.

DuckDB's documentation states outright that this serialization may change in any release, so
pin a DuckDB version in CI and expect to chase it.

### 7.1 String comparison is the sharp edge (and it also hits rung 2)

ClassAd's `==`, `!=`, `<`, `<=`, `>`, `>=` compare strings **case-insensitively**
(`compareStringsFold`, `classad/evaluator.go:1142`; `valuesEqual`, `:1258`), matching the C++
reference engine; only `=?=`/`=!=` are case-sensitive. The segment dictionaries are fold-sorted
too, so this is not skin-deep. SQL's `=` is case-sensitive. Therefore:

- **Equality:** translating `=` to ClassAd `==` yields a **superset**, which a client-side
  re-filter corrects — safe at rung 3. The exact form is `=?=`, which is case-sensitive AND
  carries an exact-case index run (§9.5); a `strcmp` idiom would be invisible to the planner.
  **Verify with EXPLAIN** that match reordering puts the indexed term first; if not, that is a
  planner fix, not a reason to skip the exact term.
- **Ordering (`<`, `>`):** fold-order and byte-order results are neither a subset nor a superset
  of one another — compare `"apple"` against `"R"` under each. **Do not push these down.**

ClassAd also has **ERROR** as well as UNDEFINED, which SQL lacks; a cross-type non-numeric
comparison yields ERROR. Harmless for pushdown (the row fails the constraint), but a decision
to make and document for the dialect (§10).

## 8. Topology and authorization (W5) — gRPC in process

**Decided: the gRPC server runs inside the daemon.** No gateway process, no byte relay. This
costs the measured **+11.2 MB** on every daemon that enables it (§3) and gives up the crash and
memory isolation a separate process would have provided. What it buys is worth more than both,
and it resolves two open questions rather than deferring them.

### 8.1 Why this is the better call, not just the simpler one

**The delegated-auth problem disappears instead of being mitigated.** A gateway holds *one*
CEDAR identity, so either every Flight user collapses to the gateway's access level — a
non-starter on a shared AP, where READ-level private-attribute stripping is exactly what a
mis-scoped relay would leak — or the gateway forwards caller identity and the daemon trusts a
token that is not the caller's unless it validates it as such. In process, each gRPC connection
carries its own credentials and is authorized directly. There is nothing to delegate.

**The authorization layer is already transport-independent.** `server.Authorizer` is
`func(perm, peerAddr, user string) bool`, documented as having the same shape as
`cedarserver.Server.Authorizer` and `authz.Policy.Authorize` (`server/server.go:40`). Nothing
about `effectiveLevel` or `levelOptions` (`server/server.go:379-405`) knows it is sitting on a
CEDAR connection — they take an address and a mapped user. So an in-process gRPC path reuses
the level escalation and the `dbrpc.ServeOptions` (read-only, private stripping, privileged)
**verbatim**, and a peer gets the same access over Flight as over CEDAR by construction rather
than by a second policy that has to be kept in agreement.

**It settles §9.5's fork.** An internal ANSI opcode has no text form, so a gateway front end
would have forced `dbrpc` to carry a serialized AST. In process there is no wire between the
binder and the evaluator: the front end parses, binds against the type map and lowers to a
`vm` program in the same address space that runs it. The internal opcode stays genuinely
internal, and `dbrpc` needs no new capability at all. **The two decisions fit together** —
in-process is what makes the internal opcode free.

**It removes the relay from the hot path.** Phase 2's point is an Arrow batch built straight
from columnar segments; a gateway would have re-encoded or relayed every byte of it.

### 8.2 The listener

Not through HTCondor's shared port. `Daemon.Listener` resolves the command socket through the
inherited shared-port endpoint, an inherited listener, or a self-registered named socket
(`golang-htcondor/daemon/daemon.go:259`) — all CEDAR routing a gRPC client cannot speak. So:

- **A dedicated TCP port, opt-in by config**, following the precedent already in the daemon:
  the metrics/pprof HTTP server is a second in-process listener gated on
  `HTCONDORDB_METRICS_ADDRESS` (`cmd/htcondordb/main.go:479`). Name this one the same way
  (`HTCONDORDB_FLIGHT_ADDRESS`), default empty, so a daemon that does not set it is unchanged
  and pays nothing but the binary size.
- **A Unix-domain-socket listener as well**, for local clients: peer credentials over a UDS are
  the only thing that reproduces CEDAR's FS authentication, which is how local tools
  authenticate today without a token. Caveat from experience: `sun_path` is ~100 bytes, and a
  glidein's sandbox path blows it — bind a bare short name in the socket directory and publish
  the short path, never a long absolute one.

**One difference from the metrics listener that matters.** Its comment says it "carries no
secrets -- storage sizes and timings -- but bind it to a trusted interface". A Flight listener
carries **ad contents**. "Trusted interface" is not an adequate control for it; authentication
is mandatory (§8.3), and that distinction should be in the config documentation so nobody
reasons from the metrics precedent to an open port.

### 8.3 Authentication: produce a `user`, then reuse everything

The only CEDAR-specific thing in the path is *authentication* — turning a connection into a
mapped fully-qualified user. Three mechanisms, in the order they are worth building:

1. **Bearer token in gRPC metadata, validated as an HTCondor IDTOKEN / SciToken** by the same
   code CEDAR uses. This reuses the existing trust root (signing keys under `passwords.d`, read
   as root) and yields the same identity string the CEDAR path produces, so no new trust
   infrastructure and no second notion of who a caller is. It is also what every ADBC client
   can already send.
2. **Peer credentials on the UDS**, mapping uid to a user in `UID_DOMAIN` — the FS equivalent,
   for local tooling.
3. **mTLS**, mapping the client certificate subject, mirroring CEDAR's SSL method. Reuse
   HTCondor's existing `AUTH_SSL_*` parameters for the server's own key and chain rather than
   inventing a parallel set.

**Authentication is OPTIONAL by default, following CEDAR rather than being stricter.** An
earlier draft of this section argued the opposite -- fail closed, no anonymous access, on the
grounds that a new endpoint has not inherited HTCondor's `OPTIONAL` compatibility concession.
That was overruled, and the counter-argument is the better one.

CEDAR already serves an anonymous peer: `peerUser` returns `""` for a connection that
negotiated no authentication (`server/server.go:518`), the `ALLOW_`/`DENY_` policy is asked
about that empty identity, and whatever it answers is the answer. An endpoint that refused the
same caller would not be safer, it would be *inconsistent* -- two doors onto one store with two
different postures, where an operator who tightened `ALLOW_READ` would have tightened only one
of them and could not easily tell which. Matching CEDAR means the Flight endpoint adds no
exposure of its own: what an anonymous caller may read here is exactly what it may already read
there, decided by the same policy.

Permissive is not open. An anonymous caller still has to satisfy `ALLOW_READ`, still gets READ
level only, and still has private attributes stripped. What changed is *who decides* -- the
pool's policy rather than this code. `REQUIRED` remains available for a pool that wants this
door stricter than its CEDAR one.

**The distinction that does hold is between presenting nothing and presenting something bad.**
No credential is anonymous. A credential that is present and fails to verify is an error that
refuses the connection, because downgrading it would leave a caller who meant to authenticate
puzzled by what is missing -- and would turn a revoked or expired token into a silent loss of
privilege rather than a reported one.

### 8.4 Authorization, and one thing that must not leak

Reuse `effectiveLevel` and `levelOptions` unchanged. Two additional gates:

- **Flight is READ-only in v1 regardless of the peer's level.** Writes are a non-goal (§11), so
  the Flight path asks for read-only `ServeOptions` even for a peer authorized at WRITE or
  DAEMON. Enabling an endpoint should never be the thing that widens what a principal can do.
- **The describe-table op must filter private attributes at this boundary.** The type profile
  reports no values, but it does report numeric **bounds**, and a bound on a private attribute
  is a disclosure even though the value is not — `AttrTypeProfiles`' doc comment says so and
  deliberately does not filter, because the filter belongs where the serving level is known.
  This is that place. A profile served at READ carries the same private-attribute stripping as
  an ad does.

### 8.5 What in-process costs, and the mitigations that are part of the work

A pathological read now runs in the daemon's address space. This is not hypothetical: a novel
`COUNT` shape panicked htcondordb in production for a 15-second outage. A gateway would have
contained that. In process, containment has to be built:

- **A panic barrier per RPC.** DONE. A deferred `recover()` *in the handler goroutine itself* —
  a helper that wraps `recover()` in another function catches nothing, which is a mistake
  already made once in this codebase. One query must not take the store down. **Still
  untested**: removing it passes the suite, because nothing in the fixtures panics and inducing
  one would mean an injection point in production code. Correct by inspection is not the same
  as covered.
- **A bounded Arrow allocator per stream.** DONE (`arrowtype.Budget`). One wrinkle worth
  recording: `memory.Allocator` is `Allocate(size int) []byte` with no error return, and a nil
  slice makes a builder write into nothing. **Panicking is the only refusal the interface
  permits**, so the budget panics with a typed error and the handler's barrier turns it into a
  failed request. A budget refusal reports `ResourceExhausted` with the numbers in it, kept
  distinct from an internal fault, because the client's recourse is a narrower query rather
  than a bug report.
- **A concurrency cap on streams.** DONE. It **refuses rather than queues**: a queue of scans
  inside the daemon is the thing the cap exists to prevent, and a refused client can retry
  while one left hanging cannot tell whether to.
- **Cancellation that actually reaches the scan.** The handler's context is the one the scan
  takes, so a client hanging up stops the work. **Not independently tested** -- the flush select
  honours `ctx.Done()`, but nothing proves a disconnect mid-stream actually stops the scan
  rather than running it to completion for nobody.
- **Watch retained allocators.** Arrow batches are large allocations in the same heap as the
  block cache and the mmap'd segments; a retained encoder pool here is the shape that already
  produced a 3 GB heap once.

### 8.6 Open

Port advertisement — address file, collector ad, both, or neither? Whether the UDS listener
should be on by default when the TCP one is not. And whether `HTCONDORDB_FLIGHT_ADDRESS` alone
should enable the endpoint or whether it needs a separate boolean, as pprof has.

## 9. Plan: ADBC (rung 2) is the target

**Decision: ADBC.** That means an Arrow Flight **SQL** server. Rung 3 (Airport) is not being
built; §6 and §7 stay as reference should that change. Rung 0 is not a deliverable (§9.4) —
its pieces are prerequisites that happen to be independently useful.

### 9.1 What arrow-go already provides

`arrow/flight/flightsql` carries most of the protocol:

- **`flightsql.Server`** is a wide interface (~45 methods), but **`BaseServer`** supplies
  not-implemented defaults for all of them, so we implement only what we serve.
- **`BaseServer` implements `SqlInfo` for real** — `RegisterSqlInfo(id, value)` plus
  `GetFlightInfoSqlInfo`/`DoGetSqlInfo`. This is what an ADBC client reads at connect.
- **`flightsql/schema_ref`** holds the spec-fixed Arrow schemas for every metadata command
  (`Catalogs`, `DBSchemas`, `Tables`, `TablesWithIncludedSchema`, `TableTypes`, `PrimaryKeys`,
  `SqlInfo`, `XdbcTypeInfo`, ...). Metadata is **conformance work, not design work**.
- **A complete SQLite reference server** ships in the package (`example/`,
  `sqlite_server_test.go`). Read it before writing anything.

### 9.2 Minimum viable ADBC server

Everything else stays on `BaseServer`'s defaults.

| Method | Why |
|---|---|
| `GetFlightInfoStatement` | SQL text in → `FlightInfo` with the result schema and one endpoint. Needs §5 to answer the schema **without executing**. |
| `GetSchemaStatement` | lets a client ask for the schema alone |
| `DoGetStatement` | ticket → `(*arrow.Schema, <-chan flight.StreamChunk)`; this is where the encoder lands |
| `RegisterSqlInfo` | the handful of entries ADBC reads at connect |
| `DoGetCatalogs` / `DoGetDBSchemas` / `DoGetTables` / `DoGetTableTypes` | catalog browsing; schemas come from `schema_ref` |

**Deliberately deferred:** Substrait (both plan variants), prepared statements, transactions
and savepoints, primary/imported/exported keys and cross-reference, `DoPutCommandStatementUpdate`
(writes), `StatementIngest`, `XdbcTypeInfo`, `CancelFlightInfo`.

Prepared statements are the first thing to add after v1, because that is how ADBC binds
parameters; they are not v1-blocking (a client can inline literals), and `CreatePreparedStatement`
+ `DoPutPreparedStatementQuery` + `DoGetPreparedStatement` is the whole increment.

### 9.3 Workstreams and what actually blocks what

```
W1  ANSI SQL dialect        (repl only)            ─┐
W2  Type/schema contract §5 (collections→db→dbrpc) ─┼─ all three can start now
W5  In-process gRPC §8      (listener + authn)      ─┘
                                   │
W3  Arrow encoder  ────────────────┘ (needs W2)
                                   │
W4  Flight SQL server ─────────────┘ (needs W3)
```

- **W1 — ANSI dialect.** A compiler from ANSI SQL to a ClassAd expression: no Arrow, no gRPC,
  no wire change, no storage change (§9.5). Ships on its own as `.dialect ansi` (§10) and
  improves the REPL and Python driver independently of everything here. It binds against W2's
  type map, so it wants W2 first; and because ADBC always speaks ANSI, W4 needs *some* W1 —
  though not a complete one, since unsupported constructs are refused rather than lowered.
- **W2 — type/schema contract (§5).** Export `adKind` out of `collections`; the describe-table
  op (opcode 66); the type map beside the columnar schema with its generation; exact integer
  min/max captured at seal; the coercion lattice and its counters. Ships on its own: it is
  what lets `GetFlightInfoStatement` answer a schema without executing, **and** it fixes the
  inference divergence between the DB-API driver and the Grafana plugin today.
- **W3 — Arrow encoder.** Rows → Arrow against W2's declared schema; later columns → Arrow
  (the old Phase 2, below). Carries one hard security requirement: because it evaluates
  expressions (§5.1), it must evaluate through the **redacted** path for an unprivileged
  reader, or it becomes the private-attribute oracle `private_gate.go` exists to prevent.
- **W4 — Flight SQL server.** The §9.2 method set over the existing `repl` executor, in the
  daemon (§8), behind an opt-in listener. Needs enough of W1 to lower whatever SQL it accepts,
  and refuses the rest (§9.5).
- **W5 — in-process gRPC topology and authorization (§8).** Settled: the gRPC server runs in
  the daemon, so there is no gateway identity to delegate and the existing `Authorizer` is
  reused verbatim. What remains is implementation, not design: the opt-in listener (TCP and
  UDS), token/peer-cred/mTLS authentication producing a mapped user, and the containment that
  in-process requires -- per-RPC panic barrier, bounded Arrow allocator, stream concurrency cap,
  cancellation that reaches the scan.

**Then, separately: the columnar encoder.** Emit Arrow batches built directly from columnar
segments rather than from reassembled ads. This is where the vectorized engine pays off, and it
is real work in classad/collections, not plumbing: there is **no "column batch out" API** today.
`colproject` and `colvec` feed the **matcher**, not an external consumer, and vectorization
**declines per block** (string columns, expression escapes, heavily-superseded blocks), so the
producer must handle a stream where some blocks arrive columnar and others reassemble row-wise
— while emitting one stable Arrow schema throughout (§5.7). Measure against
`QueryRawWireStream` (`dbrpc/queryraw.go:467`), not against the string path, or the comparison
will flatter the result: mutable reads are decompression-bound and per-record, so part of the
win is available without Arrow at all.

### 9.4 Rung 0 is a prerequisite, not a deliverable

Rung 0 adds **no capability that is impossible today**. pandas already works through the
DB-API (`python/README.md`), DuckDB is already reachable via a pandas hop, and the capi cursor
already bounds memory by batching at `rowChunkBytes`. What it changes is that types stop being
guessed (§5) and that five representations — ad text, ClassAd parse, JSON, Python objects,
pandas inference — collapse to one. The first is a correctness fix; the second is a large
constant factor. Neither is a feature.

Its value is that W2 and W3 are unavoidable for ADBC, and this is where they can be built and
verified against real pool data with no gRPC and no protocol conformance in the way. Expose
them (`.arrow`/`.arrows` in the CLI beside `.format`, `arrow()` on the Python client) because
it is nearly free once the encoder exists — not because it is the ADBC milestone.

### 9.5 The dialect is a property of the front door, not of a connection

**The wire form stays ClassAd. ADBC is always ANSI.** The ANSI front-end is a *compiler*: it
parses SQL, binds it against the §5 type map, and lowers the expression to a ClassAd
expression (or straight to a `vm` program) before anything crosses dbrpc. The store keeps
evaluating exactly one language.

This is better than making the dialect a connection option, which an earlier draft of this
section proposed:

- **There is no default to get wrong and no flip to stage.** Flight SQL is ANSI from its first
  release, so no query ever changes meaning underneath a client. The undiscoverability problem
  disappears with it — a client never has to ask which dialect it got, because the endpoint
  only ever speaks one.
- **The wire protocol does not change**, so there is one constraint language to version, test
  and fuzz, and the existing `dbrpc` surface is untouched.
- **`.dialect ansi` in the REPL becomes the same code path**, which is how the lowering gets
  tested without a gRPC stack anywhere in the picture.

Four consequences, all of them real:

**(1) W1 becomes a prerequisite for W4 — but a gradual one.** The ADBC endpoint cannot ship
without *some* ANSI front-end. It can ship without a *complete* one, because an unsupported
construct is **refused at bind time**, not approximated. Note the asymmetry with rung 3 (§7):
there, dropping a filter we could not translate was safe because Airport re-filters on the
client. Here **nothing downstream corrects us**, so the rule inverts — *refuse, never
approximate*. A construct we cannot lower faithfully is an error, and that is the whole reason
partial ANSI is safe to ship.

**(2) Case-sensitive equality needs no new operator: `=?=` is it, and it is already indexed.**
ClassAd's identity operator is case-sensitive (`evaluateIs`, `classad/evaluator.go:1317`), and
the storage layer optimizes it directly — there is a dedicated **exact-case key run** for
`=?=`/`=!=` in the in-memory index (`collections/index.go:315`, with a case-uniform-bucket
optimization so it does not double memory) and in the archive sidecar
(`collections/archive_mmapindex.go:185`, binary-searched by `catFindExact`). The planner
already routes it (`collections/vm/probe.go:267`). So ANSI `Owner = 'x'` lowers to
`Owner =?= "x"`: exact, case-sensitive, and an index probe. No grammar extension, and no
fold-term hint — an earlier draft of this section proposed
`Owner == "x" && strcmp(Owner, "x") == 0`, which was solving a problem the index already solves.

For the record on why a `strcmp` idiom would be merely *worse* and not catastrophic: with
`OpCallFunc`, a strict built-in compiles natively and the program stays `Native()`, so it keeps
the column prefilter and the vector executor. What it does not get is an **index probe**,
because `probe.go` matches operator shapes, not function calls. That is also the reason not to
reach for it elsewhere: a function call is a comparison the planner cannot see.

**The remaining gap is three-valued logic, and an opcode closes it better than a lowering
trick.** `=?=` returns **false** on a type mismatch, so `Owner =?= "x"` with `Owner` undefined
is false where SQL yields NULL. Under `AND` and `OR` that is unobservable for a WHERE clause
(`NULL AND TRUE` and `false && true` both drop; `NULL OR TRUE` and `false || true` both keep).
It diverges in exactly two positions: under `NOT`, where SQL drops a NULL row and ClassAd keeps
it since `!false` is true; and wherever the value is *surfaced* rather than filtered on
(`CASE WHEN`, `IS NOT TRUE`, a projected boolean).

An earlier draft proposed a **polarity-aware lowering** -- bare `=?=` in positive position,
a guarded `isUndefined(x) ? undefined : (x =?= lit)` under negation. That works and is the
wrong trade: it produces two forms of every predicate, one of which loses the index, and
polarity is exactly the kind of analysis that is correct in review and wrong inside a `CASE`
inside a `NOT` inside an `OR`.

**So equality gets an internal opcode too, and the family is uniform:** ANSI `=`, `<>`, `<`,
`<=`, `>`, `>=` as opcodes in the AST/`vm` space, case-sensitive and NULL-propagating, with no
surface syntax. One lowering per operator, no polarity analysis, and the semantics stated once
in the opcode rather than encoded in an idiom. Equality maps onto the same exact-case index run
`=?=` already uses, so it keeps the probe while carrying SQL's three-valued logic; ordering
scans, because no fold-sorted structure can prune a byte-ordered range.

**String *ordering* needs an operator, not a function.** There is no case-sensitive relational
operator to lower to: `=?=` answers equality only, and the dictionary is fold-sorted so its
ordering cannot serve a byte-ordered range anyway. Lowering `Owner > 'R'` to
`strcmp(Owner, "R") > 0` would work and is the wrong shape -- `probe.go` matches operator
SHAPES, not function calls, so an idiom pretending to be a comparison is invisible to the
planner, and every future reader has to recognise the idiom to know what it means.

So this is the second place an **internal opcode** is the answer (§9.5): a case-sensitive
relational comparison in the AST/`vm` space, with no surface syntax. It will still scan -- no
fold-sorted structure can prune a byte-ordered range -- but it is a comparison the planner and
the vector executor can SEE, rather than a call they must pattern-match, and the semantics are
stated once in the opcode instead of encoded in an idiom. Being a scan should be visible in
`EXPLAIN` rather than discovered.

**An INTERNAL opcode is the recommended direction — better than the polarity analysis above.**
Not a grammar extension: nothing reachable from the external syntax, which would be a public
API that cannot be withdrawn and would leak through the query log, `EXPLAIN` output and
copy-paste. An opcode in the AST/`vm` space only, corresponding *directly* to ANSI `=` —
case-sensitive, type-strict, **undefined-propagating**. Then there is **one** lowering rather
than two, so no polarity analysis and nowhere to get it wrong inside a `CASE` inside a `NOT`
inside an `OR`; and it is always indexable, because the opcode carries the 3VL semantics
instead of trading them away.

**The cost is the hazard class this tree already knows: an operator must be implemented in
every evaluation path or it silently misbehaves on one** — the scalar interpreter, the
vectorized executor, the compiler, the probe/index planner, and the archive sidecar, which are
the same mutable/archive and in-memory/persistent splits that have produced divergence bugs
here before. The in-flight `OpCallFunc` work is the scale model: one opcode touches
`opcode.go`, `compile.go`, `interp.go`, `program.go`, `vecexec.go` and `classad/vm_support.go`,
plus a strictness taxonomy to justify what may and may not be lowered. A conformance suite
running every operator through every path, the vectorized one included, is part of the work.

**W1 must be based on that in-flight work, not run parallel to it** — it occupies exactly these
files.

**Where W1 runs is settled by §8, and that is what makes the opcode free.** An internal opcode
has no text form, so a gateway front end would have forced `dbrpc` to carry a serialized AST.
With gRPC in process there is no wire between the binder and the evaluator: the front end
parses, binds against the type map (§5.7) and lowers to a `vm` program in the same address space
that runs it. The opcode stays genuinely internal, `dbrpc` needs no new capability, and
`EXPLAIN` reports the real plan. W1's output is an expression tree, not a string.

**(3) The §5 type map makes a real binder possible, which is strictly better than lowering.**
ClassAd yields ERROR for a cross-type comparison (`valuesEqual`, `classad/evaluator.go:1258`);
SQL rejects it when the statement is bound. With declared column types in hand, the ANSI
front-end can refuse `WHERE Owner > 5` at bind time with a type error, the way a SQL engine
does, instead of lowering it and quietly filtering every row out. This is a genuine W2 → W1
dependency and an argument for doing W2 first.

**(4) Three-valued logic lines up; the gaps are types.** `!undefined` is undefined
(`classad/evaluator.go:725`), `undefined == x` is undefined, and a row whose constraint is
undefined fails it — which matches SQL dropping a NULL predicate, including through `NOT`.
The lowering vocabulary is good: `substr`, `size`, `regexp`, `replace`,
`strcat`, `int`/`real`/`string`, `toupper`/`tolower`, `split`/`join`, `member`, `formattime`,
`interval`, and the Elvis operator for `COALESCE`. What has **no** faithful ClassAd form, and
must therefore be refused rather than approximated: `DECIMAL` arithmetic (no exact decimal
type), true `DATE`/`TIME`/`TIMESTAMP` types as distinct from epoch integers, and any explicit
`COLLATE`.

**Record both forms.** Every logged statement and every `EXPLAIN` carries the ANSI source *and*
the lowered ClassAd expression. Without that, debugging a wrong result means guessing at the
lowering — and the index hint in (2) is only verifiable if the lowered form is visible.

## 10. The ANSI SQL dialect (W1)

Today a WHERE clause is a **ClassAd** expression captured verbatim, with a short list of SQL
spellings translated at the token level (`repl/sqlexpr.go`): `CASE`, `'text'`, `=`, `<>`,
`AND`/`OR`/`NOT`, `IS [NOT] NULL`. Everything else passes through byte for byte.

W1 replaces that with a real front end: parse to an AST, **bind against the §5 type map**, and
lower to a ClassAd expression (or straight to `vm`) — `LIKE`, `BETWEEN`, `IN`, `CAST`,
`COALESCE`, `||`, string and date functions, SQL's NULL propagation — refusing at bind time
anything it cannot lower faithfully. §9.5 is the contract it implements and the lowering
vocabulary it has to work with.

**ADBC does not choose a dialect**; the Flight SQL endpoint is ANSI-only (§9.5). The REPL keeps
a choice, because it is the one place both dialects are useful — ClassAd stays the default for
everyone who already types it, and `.dialect ansi` beside `.format` (`repl/repl.go:131`) is how
the lowering gets exercised without a gRPC stack:

```
.dialect classad     # today's behavior (default)
.dialect ansi        # the W1 front end, same code path ADBC uses
```

**Shipped so far (the `ansi` package), and the one place it departs from this section.** The
predicate subset is lowered: `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`/`OR`/`NOT`, parentheses,
`IS [NOT] NULL`, `IN`, `BETWEEN`, and the literals. String equality lowers to `=?=` per §9.5.

The departure is polarity. §9.5 proposes a **guarded form** under negation; the implementation
**refuses** there instead. Two forms of every predicate means one of them loses the index, and
polarity is the analysis that is right in review and wrong three levels into a nested
expression. Refusing is still a polarity analysis, but its failure mode is "you cannot write
that yet" rather than silently wrong rows — and it is positional, so a string equality outside a
`NOT` is unaffected and double negation returns to positive. The internal opcodes remain the way
to lift the restriction, and nothing here blocks them.

Also refused, each for a reason rather than for want of effort: string **ordering** (fold-order
and byte-order are neither subset nor superset, and the dictionaries are fold-sorted so no index
could serve a byte range), string **inequality** (its 3VL guard is correct but unverified
against the index, and an unindexed inequality over a large table is better introduced
deliberately than discovered), a **type-invalid** comparison (refused at bind time as a SQL
engine does, rather than lowered into ClassAd's ERROR where it would quietly filter out every
row), and an **unknown or untypeable** column.

A caveat the in-process decision did NOT remove: the lowered constraint crosses `dbrpc` as
**text**, because the Flight endpoint reads through an in-process loopback to reuse the
authorization layer (§8.1). So an internal opcode with no surface syntax could not travel it as
things stand — a second reason the refusal above is the right first move rather than a stopgap.

**One caution, for the REPL only.** `.format` changes how results *print*; `.dialect` changes
what a query *means* — `WHERE Owner = 'bbockelm'` folds case under ClassAd and does not under
ANSI. So the mode has to be visible (prompt, `EXPLAIN`, and recorded on every logged statement
alongside the lowered form) and set explicitly by scripts rather than inherited. ERROR needs no
separate decision: §9.5(3) rejects the comparisons that would produce it at bind time.

The existing dialect is otherwise in good shape for Flight SQL: GROUP BY, HAVING, DISTINCT,
`COUNT(DISTINCT)`, `FILTER (WHERE ...)`, ranking window functions, `QUALIFY`, `time_bucket`,
`EXPLAIN [ANALYZE]`, and server-side aggregate pushdown (`execSelect`, `repl/exec.go:1250`).
Flight SQL additionally wants the catalog metadata commands (`CommandGetTables`,
`CommandGetDbSchemas`, `CommandGetSqlInfo`) and prepared statements, mechanical once §5 exists.

## 11. Non-goals, risks, open questions

**Non-goals:** writes (Airport's `DoExchange`) — a second write path with its own transaction
semantics for much less value than reads; scalar/table UDFs; `ALTER TABLE`; rung 1 as a
destination.

**Implemented so far.** The measurement (`collections.AttrTypeProfiles`, dbrpc op 68), the type
decision and encoder (`arrowtype`), an Arrow IPC export (`arrowstream`, `htcondordb-cli
arrow`), the ANSI predicate lowering (`ansi`), and a Flight SQL endpoint served in process
(`flightsvc`) accepting `SELECT <cols|*> FROM <table> [WHERE <predicate>] [LIMIT n]` and
refusing the rest. Verified against a non-Go consumer: 2000 real
ap2001 history ads read back by pyarrow as 226 typed columns, with arithmetic over the columns
the stray-expression bug would have made text.

**Risks.** DuckDB's filter JSON is unversioned by policy (§7) — only if rung 3 is ever built.
Catalog serialization cost for wide tables — measure a real jobs table early; §5.6 and SHA256
caching are the answer. **An encoder that evaluates expressions must evaluate through the redacted
path** (§5.1) or it becomes the private-attribute oracle `private_gate.go` exists to prevent.
**In-process gRPC means a pathological read runs in the daemon's
address space** (§8.5): the panic barrier, bounded allocator, concurrency cap and real
cancellation are not polish, they are what replaces the isolation a separate process would have
given. Phase 2 must not let an Arrow schema vary as blocks alternate between columnar and
row-wise. And a novel query shape against production has crashed this daemon before, so every
new read path is exercised against a local store first.

**Open questions.** `_ad` as `utf8` or `map<utf8,utf8>`? Does the type map belong in the table
catalog or a sidecar? At rung 0, is the unit of delivery a stream op or a file the client points
`read_arrow()` at — and if a file, who cleans it up?

## 12. Testing

The hazard is **vacuous green**: a test passes identically whether pushdown worked or every
filter was dropped, and whether a column came back `int64` or `utf8`. Row counts cannot tell the
difference. So assert on:

- the **Arrow type** of every column, against the declared type map;
- coercion/null counters at zero for Tier A columns, non-zero where a fixture plants an escape;
- a differential oracle — the same logical query through the REPL and through Arrow must agree,
  including on case sensitivity;
- in Phase 2, that blocks actually vectorized (the existing `vecSplit` counters) rather than
  every block declining to the row path.

Keep the integration suite inside the usual runtime budget; DuckDB-in-the-loop tests run on
demand, not on every push.

## 13. Parked: the one Airport-only opportunity worth remembering

Not being built (§9), recorded so it is not rediscovered.

**`data://` endpoints (rung 3).** An Airport endpoint's location may be
`data:application/x-msgpack-duckdb-function-call;base64,...`, embedding a DuckDB function call
(`read_parquet`, `read_csv`, Iceberg/Delta) instead of a `grpc://` address. So: add a **Parquet
exporter for sealed archive segments** (the exporter framework already exists), and `endpoints`
can return a mix — `data://` endpoints pointing DuckDB at Parquet files it reads at full native
speed, plus one `grpc://` endpoint for the hot, mutable tail.

Bulk history analytics then runs with the daemon **out of the data path entirely**, and DuckDB
does its own file-level predicate and projection pushdown. For "scan five years of history" this
beats any amount of encoder work, and it reuses retention and sealing machinery we already have.
Worth noting the exporter is independently useful at rung 0 too: a Parquet dump that DuckDB
reads directly needs no Flight stack at all.
