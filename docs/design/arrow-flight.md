# Arrow output over Flight SQL

**Status:** implemented and read-only. Type derivation in `arrowtype/`, the endpoint in
`flightsvc/`, the ANSI front end in `ansi/`, verified in CI against the DuckDB CLI and a real
ADBC driver. A projected read ships typed, evaluated values straight from the store with no
text in the path. §7 is the status record: what is shipped, what a read costs, and what is
left.

**Audience:** whoever works on this, in htcondordb and in the classad storage/dbrpc layers.
Assumes familiarity with the columnar segment layout and the vectorized matcher, not with
Arrow Flight.

**Grounded in** `repl/sql.go`, `repl/exec.go`, `repl/rowstream.go`, `grafana/pkg/plugin/frame.go`,
and classad's `collections/adschema.go`, `collections/vm/vecexec.go`, `dbrpc/proto.go`,
`db/aggregate.go`. Every number below is measured; the sections say how.

---

## 1. Goal, and the two findings that set the shape

Make htcondordb tables — the live job queue, history archives, collector ads — consumable by
the Arrow ecosystem, so that joins, window functions, `PIVOT` and CSV/Parquet export work
against pool data without an export step.

Two findings reordered the obvious plan.

**Finding 1: DuckDB never sends SQL to a Flight server.** Through the Airport extension DuckDB
parses, binds and plans the query itself; the server receives a catalog listing, a column-id
list, and a serialized DuckDB bound expression tree as JSON. No SQL string is on the wire. So
an ANSI dialect is *not* a prerequisite for DuckDB. It *is* a prerequisite for Arrow Flight
**SQL**, a different and standard protocol that reaches far more clients.

**Finding 2: Arrow is cheap; gRPC and Flight are not.** Arrow arrays and IPC add **+3.1 MB
(+9.6%)** to this daemon; gRPC plus the Flight protobufs adds a further **+11.2 MB (+35% of
base)** (§3). The expensive dependency is the transport, not Arrow.

## 2. Protocol: four rungs, and the one we took

The reason Airport comes up at all is narrow: **DuckDB has no built-in Arrow Flight client.**
Plain Flight is not something DuckDB can `ATTACH`. So "serve Arrow Flight" and "serve DuckDB"
are different goals.

| Rung | What we serve | Extra deps | Who can read it | Who filters |
|------|---------------|-----------|-----------------|-------------|
| **0** | Arrow IPC over the existing dbrpc/CEDAR stream | arrow arrays+IPC (**+3.1 MB**) | our CLI/Python/Go clients; DuckDB via `nanoarrow`'s `read_arrow()`; pyarrow → DuckDB zero-copy | **us**, in our own language: ClassAd constraints, server-side aggregates, TopK, AS OF |
| **1** | Standard Arrow Flight | + gRPC + Flight protos (**+11.2 MB**) | any Flight client | us — the ticket format is ours |
| **2** | Arrow **Flight SQL** | same as rung 1, plus the dialect work | ADBC (Python/Go/Java/R), the Flight SQL JDBC driver, BI tools, DuckDB's own ADBC extension | **us**, from SQL text we receive |
| **3** | Airport (Flight + custom `DoAction`s) | rung 1 + msgpack + DuckDB's expression JSON | DuckDB only | **DuckDB's planner**, pushed to us |

**We built 0 and 2.** Rung 0's pieces — the type contract and the encoder — are prerequisites
for rung 2 that happen to be independently useful, so they ship as `htcondordb-cli arrow` and
`arrowstream/` rather than as a milestone. Rung 1 falls out of rung 2. Rung 3 is the only route
to DuckDB `ATTACH` with planner pushdown, and it is the most coupled option on every axis: a
custom msgpack protocol, a community extension, an expression serialization DuckDB reserves the
right to change in any release, and a server role reduced to dumb scan source — our constraint
engine and TopK become unreachable, since Airport's endpoint parameters have no LIMIT at all.
Appendix A keeps what was learned about it.

## 3. Measured binary cost

One pinned dependency set, three arms differing only by which source file is present, built
from an exact copy of `cmd/htcondordb`. The no-Arrow arm reproduced the shipped daemon
byte for byte, which is what says the copy is faithful. darwin/arm64, `go1.26.0`, unstripped.

| Arm | Size | Delta |
|-----|------|-------|
| daemon as-is | 31,842,514 | — |
| + Arrow arrays + IPC | 34,900,178 | **+3,057,664 (+9.6%)** |
| + Flight + gRPC + msgpack | 46,063,954 | **+11,163,776 more (+35.1% of base)** |

**Re-measured once it was really wired in.** Those arms link the dependencies without using
them. With `flightsvc` actually serving — flightsql, the catalog schemas, the per-identity
session machinery — the daemon goes from 35,803,010 to 51,962,834 bytes: **+16.2 MB (+45.1%)**
against the +14.2 MB the synthetic arm predicted. An estimate taken from a program that merely
imports a library errs low, and did.

**No cgo**, verified rather than assumed: every package used declares zero cgo files and the
full stack builds with `CGO_ENABLED=0`. arrow-go's only cgo is `arrow/cdata`, which we do not
import. Static builds, musl images and the RPM cross-build are unaffected. The
`modernc.org/sqlite` in arrow-go's go.mod is the pure-Go transpiled SQLite and is in the
dependency tree of none of the packages we use.

## 4. Where types are erased

Types exist at the bottom of the stack and at the top, and are erased in the middle. That
erasure is the whole engineering problem.

**At the bottom — already Arrow-shaped.** `adField` (`collections/adschema.go`) carries
per-column kind, width, signedness and offset, plus a per-record escape bitmap: an Arrow schema
with different field names. `vm.Vec` (`collections/vm/vecexec.go`) is close to an Arrow array —
`Raw`+`RawWidth` is a fixed-width buffer, `Dict` is a `DictionaryArray` (fold-sorted, which
Arrow permits via its ordered flag), `Hi`/`Lo` bitplanes are a nullable boolean, `St` is nearly
a validity bitmap. A real is stored as `math.Float64bits` in the same int64 slot, so loading a
real column is a copy rather than a conversion.

**At the top — typed.** `RowStreamer` (`repl/rowstream.go`) hands back
`func(*classad.ClassAd) []classad.Value`, and `classad.ValueType` distinguishes undefined /
error / bool / int / real / string / list / nested ad.

**In the middle — strings.** `QueryRaw*` returns ad text; `AggRow` holds `[]string`;
`repl.Result.Rows` is `[][]string`. The visible consequences are that the Grafana plugin
re-derives types by parsing every cell and bailing on the first non-numeric, with a hardcoded
epoch-attribute list, and that the Python driver documents `description` as "what arrived, not
a schema".

So this is not "add types". It is **stop discarding them**.

## 5. The schema contract

Arrow fixes a schema for a whole stream, and a schema derived from whichever rows happened to
show up is a bug factory — this repository has been bitten twice by the general shape already
(expression-valued columnar fields reading as UNDEFINED in fast COUNT paths; persisted field
order re-derived from process-local intern ids). So **declare the schema, do not infer it**.

Three decisions govern everything below:

1. **Arrow carries evaluated values**, never unevaluated expressions (§5.1).
2. **Coerce exceptional values where the coercion is exact**; null only what cannot be
   coerced (§5.3).
3. **Pick the Arrow type that minimizes exceptions**, even where storage chose a narrower
   one (§5.2).

The effect is that the storage schema and the Arrow schema are deliberately *not* the same
schema. Storage optimizes bytes on disk and accepts exceptions as the price; Arrow optimizes
for a column that is wholly one type, because that is the only thing a consumer can use. Same
source of truth, different objective function.

### 5.1 Arrow columns carry evaluated values

`nodeKind` classifies a computed expression as `akNone` — not a storable scalar — so an
expression-valued attribute is an escape, held in the record rather than the fixed slot. For
Arrow we evaluate it and encode the result.

**True of the schema as well as the encoder.** For a long time only the encoder obeyed this:
the type report classified what the cells *stored*, so an expression-valued attribute was ruled
untypeable and got no column for the encoder to fill. The evaluation also moved — it now
happens in the store, where the record's reference closure and its precomputed values already
are, rather than in this process after a render and a reparse (§7.2).

Four consequences:

- **Evaluation needs the ad, not just the column.** An expression's references are expanded
  transitively while decoding, and `vm.ReadPlan`'s seed set is not closed over them. So a record
  with an expression escape in a projected column forces a row reassembly for *that* record.
  Escapes are rare by construction, so this is a slope rather than a cliff — but the encoder
  must fall back per record, and the cost must appear in the scan stats rather than being
  invisible.
- **An evaluated expression is usually not an exception at all.** `WallClockTime` stored as an
  expression that evaluates to an integer is an integer. This is most of why decision 1 matters:
  it converts a large class of apparent type exceptions into ordinary values.
- **Evaluation is not stable.** An expression referencing `CurrentTime` yields a different value
  on each read, so an Arrow batch is a snapshot of evaluation, not of stored state. That matches
  every other read of this store, but two Arrow dumps of an unchanged table can legitimately
  differ.
- **Evaluation MUST go through the redacted path for an unprivileged reader.** This is a
  security requirement, not a detail. An ad may store a *public* attribute whose value is an
  expression over a private one (`Leak = ClaimId`): a constraint on `Leak` references nothing
  private and passes the gate, while evaluation resolves `ClaimId`. The private value must be
  **absent at evaluation time** for such a reader, which the redacted decode walk already
  arranges. An encoder that evaluates with private values present turns every Arrow read into
  the oracle `private_gate.go` exists to prevent — the same hazard class as the wire scan that
  shipped plaintext of encrypted attributes once already.

**Corollary — do not promote predominantly-expression attributes to typed columns.**
`Requirements` evaluates to a boolean, and that boolean is almost never what anyone wants; the
*text* is the point. Such attributes stay out of the typed schema and live in the tail column
(§5.6). The division is: **typed columns hold evaluated values; the tail column preserves what
was written.**

This corollary is mandatory rather than advisory, and §5.8 measures why. Typing from evaluated
values types `Requirements` too — it evaluates to a boolean in 1685 of 2000 ads, so it is not
rescued by evaluating badly; it gets a perfectly good column of 85% meaningless booleans and
15% null. It is the same *shape* as `MemoryUsage`, and the difference is which of the value and
the expression a reader is after, which no measurement reaches. So the daemon seeds the policy
expressions as excluded (`knownFlightPolicyExprs`: `Requirements`, `Rank`, `START`, the
`Periodic*` and `OnExit*` family, `IsOwner`, `WithinResourceLimits`) the way it seeds the epoch
attributes, and the knob adds to that rather than replacing it. Excluding is not hiding — the
tail carries the text, which is the only reason excluding is an acceptable answer at all, so
the two had to land together.

### 5.2 Choosing the Arrow type: widen to minimize exceptions

`chooseIntWidth` picks the smallest width covering ≥ `fit` of the sampled values (0.95 for a
base-schema column, 0.999 for a hot one) and deliberately accepts the rest as per-record
exceptions, because two bytes per record per column is worth real disk.

For Arrow that trade does not exist: the width buys almost nothing (IPC compresses, and DuckDB
widens to `BIGINT` for arithmetic anyway) while every exception costs an escape read and a
coercion. So the Arrow width covers **every** value, not a fit fraction.

- **Integer columns default to `int64`.** A value that escaped a 2-byte storage column is still
  an integer; widening makes it ordinary and the exception count goes to zero.
- **Narrowing would need an exact bound we do not have.** Zone maps hold per-segment `[min,max]`
  but only for configured `ZoneAttrs`, and they hold them as float64, which cannot bound an
  int64 column exactly above 2^53. If narrowing is ever wanted, capture an exact integer min/max
  at seal time — sealing already walks every value, so it is nearly free there and cannot be
  reconstructed cheaply afterwards.
- **Signedness follows the data, not the storage flag.** `adField.unsigned` is per-segment; if
  any segment holds a negative value the Arrow type is signed.
- **Width 6 has no Arrow equivalent** and becomes `int64`.

The same principle applies to the *kind*: if declaring an `akInt` column as `double` eliminates
more exceptions than it creates, do that — **but only when every integer in the column
round-trips through float64 exactly.** `vm.Vec` keeps the int64 payload specifically to preserve
integers above 2^53, and trading that away silently at the Arrow boundary would discard a
property the storage layer went to trouble to keep.

### 5.3 The coercion lattice

Applied per escaped value, after evaluation. "Exact" means information-preserving and
reversible; anything else is counted, and refused under strict mode (§5.5).

| Declared column | Escaped value | Action | Exact? |
|---|---|---|---|
| `double` | integer | widen to float64 | exact for \|v\| < 2^53; **counted as lossy** above |
| `int64` | boolean | 1 / 0 | exact |
| `double` | boolean | 1.0 / 0.0 | exact |
| `int64` | real with integral value | truncate | exact |
| `int64` | real with fractional value | **no coercion** — widen the column, else null | — |
| `utf8` | any scalar | ClassAd unparse | exact |
| any | undefined / absent | **null** | not a coercion; this is the validity bitmap |
| any | ERROR | **null**, counted *separately* from undefined | — |
| any numeric | list / nested ad | null (or unparse if the column is `utf8`) | — |

**What we deliberately refuse to coerce: strings into numbers.** Parsing `"42"` into an `int64`
column is a bug class this codebase has already hit, where a stringified number was written into
an attribute and read back as the wrong type. A string in a numeric column means the data is
heterogeneous, which is information — it belongs in the exception count and, if common, in a
Tier C declaration. Silently parsing it would make a type error look like clean data. Numeric →
`utf8` is fine and goes the other way.

### 5.4 The residual tiers

| Tier | Condition | Arrow type |
|---|---|---|
| A | one kind after evaluation; every value representable or exactly coercible | `int64` / `double` / `bool` / `utf8`, non-nullable if presence is total |
| B | as A, but some values are absent, undefined, ERROR, or lossily coerced | same type, **nullable** |
| C | kinds genuinely disagree and no exact widening unifies them | `utf8` holding the unparse of the evaluated value |

Tier C is a statement about the *data*, not an artifact of how storage fitted a width. That is
the point of the three decisions: the tier should describe the pool, not our encoder.

**Measured, Tier C is empty** (§5.8). Keep it — it is the honest answer for data that really is
mixed, and an empty tier costs nothing — but do not build machinery around it as though it were
the common case.

### 5.5 Exceptions must be measurable, and strictness must be available

A column that silently nulls 3% of its values is the failure mode where a server-side clamp hid
a unit bug: plausible output, no error. So the encoder counts, per column per stream: values
coerced exactly, values coerced **lossily**, values nulled split into absent / undefined / ERROR
/ uncoercible-type, and records that fell back to row reassembly.

A **strict mode**, per table or per session, fails the read on any lossy coercion or uncoercible
value instead of nulling. Default lenient; strict is what a correctness investigation uses, and
what a scheduled export should set.

### 5.6 `SELECT *` is "schema columns plus a tail column" — built

A jobs table has thousands of distinct attributes across its rows; the union is useless as an
Arrow schema and expensive to serialize. So the typed columns are joined by **one** `_ad` column
of `utf8` holding a bracketed ClassAd — bracketed rather than the newline-separated old form
because this is one cell of one row, and a self-delimiting value survives a CSV or Parquet
export that a multi-line one does not. Null when there is nothing left over.

`arrowtype.TailDecision()` is a `Decision` like any other, so schema building, projection by
name and the encoder's field list handle it with no special case, and `Options.Tail` appends it
last so a client reading by position is unaffected by turning it on. The Flight endpoint sets it
unconditionally: a host that could switch it off could only switch the schema back to lying.

Three things building it settled that the specification did not anticipate:

- **It had to be the complement of the COLUMNS, not of the table's schema**, and that interacts
  with projection pushdown. `SELECT Owner, _ad` with the projection pushed down delivers exactly
  the attributes that are already columns, so every tail would be empty — a wrong answer that
  looks like a table with nothing left over rather than like a failure. Asking for the tail
  therefore turns the projection off, which is a real cost and is the cost the query asked for.
- **It keeps the STORED form.** Evaluating on the way in would discard the only thing the column
  is for: an attribute lands here precisely because its evaluated form was not worth a column.
- **It changed what an untyped attribute means for privacy.** Before, the encoder dropped
  anything with no column, so a private attribute that reached this process was discarded on the
  way out whatever the session was — by accident, not by policy. The tail keeps everything, so
  the stripping now has to be real. It is, and it is the loopback's: an unprivileged dbrpc
  session never puts the attribute in the row. That is the property routing Flight through dbrpc
  exists to buy, and exactly the kind that stops holding the moment something downstream starts
  keeping what it used to throw away — so it is pinned by a test that searches a READ peer's
  whole batch for the value, with a DAEMON peer proving the fixture has one to withhold.

### 5.7 The declaration, and how it invalidates

The intent was a per-table Arrow type map — `attr -> {arrow type, nullable, strict?}` — stored
alongside the columnar schema and sharing its generation counter, seeded by introspection and
overridable by an admin. **Half of that is built and half is not**, and the half that is built
came out differently from the plan.

**Built: the pin** (`arrowtype.Fingerprint`, `flightsvc/pin.go`). Flight answers a query in two
RPCs: one promises a shape, the other delivers rows. Both derived the schema independently from
a sample of a table still being written to, so a column appearing between them did not shift one
value for a client reading by position — it shifted every value after it, and nothing in the
protocol said so. `GetFlightInfo` now puts the fingerprint of the schema it promised into the
ticket; `DoGet` re-derives, compares, and refuses with `FAILED_PRECONDITION` and an instruction
to re-plan rather than serving rows under a superseded shape. A prepared statement carries the
same pin server-side, re-set by every call that reports a schema.

Three things that differ from the paragraph above, each found by building it:

- **The exposure is the statement, not the stream.** *Within* a stream the schema was never at
  risk: it is derived once, before the scan. The FlightInfo/DoGet pair is where the gap was, so
  the pin is for the life of a statement.
- **A content hash, not a generation counter.** A counter needs an owner to bump it, and a
  schema derived from a sample has none — the table's writers do not know they changed it. A
  hash needs no state, agrees across processes and restarts, and has the property a counter does
  not: a re-derivation that produces the same schema produces the same name, so an unchanged
  table never invalidates anything.
- **The name travels, not the schema.** The ticket is in the client's hands, so anything in it is
  something the client can edit. A fingerprint it invents cannot make the server produce a shape
  of its choosing — the only reachable outcome is refusing its own request.

A prepared statement re-pins rather than holding the pin from prepare. Handles live half an hour
against a thirty-second schema cache, so a pin fixed at prepare would start refusing a working
statement as soon as the pool set a new attribute — a worse failure than the one being
prevented. The guarantee is the one that was wanted: what the client was last told is what it
gets, or it is told why not.

**A cache, privilege-scoped** (`flightsvc/schemacache.go`, default 30s) stops a derivation per
RPC and makes the promised and delivered schema one object in the common case. Keyed by
privilege, which is not a detail: an unprivileged type report has private attributes filtered out
server-side, because an attribute's numeric bounds are a disclosure even where its value is not,
so two levels derive different schemas from one table and a shared entry would hand private
column names to whoever asked second.

**Built: the declaration** (`flightsvc/declared.go`). The pin makes instability *visible*; this
is what makes it rare. Every schema published is otherwise MEASURED — a sample of a table still
being written to, re-taken whenever the cache expires — which is sound for one read and wrong
for everything a consumer does with a catalog.

**The rule is monotone: a declaration is a floor, not a ceiling.** A declared column keeps its
type and its position for as long as the declaration stands; the measurement cannot retype it
and an admin can. An attribute nobody has declared is typed by measurement and **appended**,
never inserted. A declared column whose attribute has vanished from the sample stays, as a
column of nulls, because dropping it would renumber everything after it.

The consequence is the one that matters: **a schema a client cached is always a prefix of the
current one.** A positional reader cannot be made to read the wrong column by anything that
happens to the table; at worst it does not know about a new column at the end.

Four things worth recording:

- **One column is exempt, deliberately.** The tail is not an attribute — it is this endpoint's
  own column — and it is always last, so appending an attribute moves it. A client reading the
  tail by position must re-read the schema; one reading it by name, which is what a special
  column deserves, is unaffected. Putting it first would make *turning the tail on* shift every
  other column, which is worse.
- **It records only what an unprivileged reader could already list.** The file is shared by
  every reader of the table, so writing down a column only a DAEMON peer can see would both
  leak the attribute's name to whoever can read the file and publish a column READ peers cannot
  fill. A privileged reader's extra columns are served and not recorded.
- **Never a correctness dependency.** An unreadable file starts from the measurement; an
  unwritable one declares for this process only. Both warn, because the symptom of a silent
  reset — clients re-fetching catalogs — points nowhere near here. A daemon with no on-disk
  directory gets the in-process half, which still stops a schema moving between two RPCs of one
  session.
- **Bounded.** Attribute names come from the data, and a pool that puts a job id in one would
  grow the file without limit. Past the cap, attributes are still *served* — the cap bounds what
  is written down, never what a client is shown — and their positions simply stop surviving a
  restart.

`HTCONDORDB_FLIGHT_SCHEMA_FILE` chooses the path; the default is beside the database, which is
where it belongs, since it describes that database's tables and is meaningless without them.
`none` turns persistence off for a host that would rather re-measure.

**Still wanted from the original plan:** the epoch-to-timestamp mapping as a declaration rather
than a guess — `knownTimeAttrs` in the Grafana plugin is the right instinct with the wrong
mechanism — and the describe-table op the declaration could be *seeded* from, which wants per
attribute `{kind-after-evaluation, exact integer min/max, unsigned, presence fraction, escape
fraction by cause, cardinality estimate}` and requires exporting kind information out of
`collections` (`adKind` is package-private today). What ships now seeds itself from the ordinary
type report instead, which is enough to make the schema stable and not enough to narrow an
integer column.

### 5.8 Measured: what ap2001 actually holds

The tiers above were written before anyone had looked. `AttrTypeProfiles` was pointed at 2000 ads
sampled from each of ap2001's `jobs` and `history` tables — pulled with an ordinary
`SELECT ... LIMIT` and profiled in a **local** store, because a novel read shape does not go at a
production daemon.

| | `jobs` | `history` |
|---|---|---|
| distinct attributes | 336 | 242 |
| more than one stored class, counting undefined as one | 17 | 12 |
| more than one *value* class (undefined/error excluded) | **10** | **10** |
| integers past 2^53 | **0** | **0** |
| NaN reals | **0** | **0** |
| predominantly expression-valued | 8 | 6 |

The first two rows make §5.4's point empirically: counting stored `undefined` as a competing
class invents 7 mixed attributes in `jobs` and 2 in `history` that are not mixed.

Every heterogeneous attribute fell into one of three patterns, and all three resolve without a
fallback type.

1. **int/real mixing — the large majority.** `BytesRecvd` 67 int to 1933 real, `BytesSent` 1948
   to 52, `CpusUsage` 2 to 1998, `Rank` 1 to 1999, plus `CommittedSlotTime`, `CumulativeSlotTime`,
   `GPUsAverageUsage`, `NetworkInputMb`, `NetworkOutputMb`. Mostly not heterogeneous data at all:
   ClassAd writes an integral float without a decimal point, so a genuinely float-valued
   attribute arrives as a mix of int and real *nodes*. Safely `double` — and safe *because* no
   integer is past 2^53.
2. **scalar + `undefined`.** `MachineAttrGLIDEIN_ResourceName0` and `..._Site0` are 25 strings
   against 1975 undefined in history: uniformly string attributes that usually have no value.
3. **scalar + expression.** `Requirements` is 1995 expressions to 5 booleans; `RequestMemory` and
   `RequestDisk` are 1994 integers to 1 expression; `MemoryUsage` is 1996 expressions to 4
   integers. Under §5.1 these evaluate, and the evaluated type is uniform.

**Then measured again, after evaluation**, because pattern 3's resolution was a claim about the
encoder rather than something the sample proved:

| | `jobs` | `history` |
|---|---|---|
| attributes | 337 | 243 |
| evaluated to a **single** value class | 330 | 232 |
| evaluated **mixed** (>1 value class) | **4** | **8** |
| evaluated to undefined/error in every ad | 3 | 3 |

**Every mixed-after-evaluation attribute is int/real.** Not one string/number mix, not one
list/scalar mix, in either table. So **Tier C is empty on this data**, and §5.1's evaluation is
what empties it: `MemoryUsage` evaluates to an integer in all 2000 history ads though it is an
expression in 1996 of them, `RequestMemory` likewise, `OnExitHold` to a boolean.

**What this does not establish.** One AP, one moment, 2000 ads per table. `BytesSent` exceeding
2^53 is 9 PB in a single job — implausible now, not impossible, and the guard costs a comparison,
so it stays. The evaluation ran with the ad as its own scope and no target, which is what an
Arrow encoder does; an attribute whose value depends on a target would evaluate differently in a
matchmaking context, and none of this says what should happen there. And it ran with full
privileges, so it says nothing about the redacted path §5.1 requires.

## 6. Topology and authorization: gRPC in process

**Decided: the gRPC server runs inside the daemon.** No gateway process, no byte relay. This
costs the measured +11.2 MB on every daemon that enables it and gives up the crash and memory
isolation a separate process would have provided. What it buys is worth more than both.

**The delegated-auth problem disappears instead of being mitigated.** A gateway holds *one*
CEDAR identity, so either every Flight user collapses to the gateway's access level — a
non-starter on a shared AP, where READ-level private-attribute stripping is exactly what a
mis-scoped relay would leak — or the gateway forwards caller identity and the daemon trusts a
token that is not the caller's. In process, each gRPC connection carries its own credentials and
is authorized directly. There is nothing to delegate.

**The authorization layer is already transport-independent.** `server.Authorizer` is
`func(perm, peerAddr, user string) bool`; nothing about `effectiveLevel` or `levelOptions` knows
it is sitting on a CEDAR connection. So the in-process path reuses the level escalation and the
`dbrpc.ServeOptions` verbatim, and a peer gets the same access over Flight as over CEDAR by
construction rather than by a second policy that has to be kept in agreement.

**It also removes the relay from the hot path**, which is what makes the typed value stream
(§7.2) possible at all.

### 6.1 The listener

Not through HTCondor's shared port: `Daemon.Listener` resolves the command socket through CEDAR
routing a gRPC client cannot speak. So a **dedicated TCP port, opt-in by config**
(`HTCONDORDB_FLIGHT_ADDRESS`, default empty), following the precedent of the metrics/pprof
listener — a daemon that does not set it is unchanged and pays only the binary size. A
**Unix-domain-socket listener** as well, for local clients: peer credentials over a UDS are the
only thing that reproduces CEDAR's FS authentication. Caveat from experience: `sun_path` is ~100
bytes and a glidein's sandbox path blows it, so bind a bare short name in the socket directory
and publish the short path, never a long absolute one.

**One difference from the metrics listener matters.** Its comment says it "carries no secrets —
storage sizes and timings — but bind it to a trusted interface". A Flight listener carries **ad
contents**. "Trusted interface" is not an adequate control for it; authentication is mandatory,
and that distinction belongs in the config documentation so nobody reasons from the metrics
precedent to an open port.

### 6.2 Authentication

The only CEDAR-specific thing in the path is authentication — turning a connection into a mapped
fully-qualified user. Three mechanisms: a **bearer token** in gRPC metadata validated as an
HTCondor IDTOKEN / SciToken by the same code CEDAR uses (reusing the existing trust root, and
what every ADBC client can already send); **peer credentials on the UDS**, mapping uid to a user
in `UID_DOMAIN`; and **mTLS** mapping the certificate subject, reusing HTCondor's `AUTH_SSL_*`
parameters rather than inventing a parallel set.

**Authentication is OPTIONAL by default, following CEDAR rather than being stricter.** An
earlier draft argued the opposite — fail closed, no anonymous access. The counter-argument is
better: CEDAR already serves an anonymous peer, `peerUser` returns `""` for a connection that
negotiated nothing, the `ALLOW_`/`DENY_` policy is asked about that empty identity, and whatever
it answers is the answer. An endpoint that refused the same caller would not be safer, it would
be *inconsistent* — two doors onto one store with two postures, where an operator who tightened
`ALLOW_READ` would have tightened only one and could not easily tell which. Matching CEDAR means
the Flight endpoint adds no exposure of its own.

Permissive is not open. An anonymous caller still satisfies `ALLOW_READ`, still gets READ level
only, and still has private attributes stripped. What changed is *who decides*. `REQUIRED`
remains available for a pool that wants this door stricter.

**The distinction that does hold is between presenting nothing and presenting something bad.**
No credential is anonymous. A credential that is present and fails to verify refuses the
connection, because downgrading it would leave a caller who meant to authenticate puzzled, and
would turn a revoked or expired token into a silent loss of privilege rather than a reported one.

### 6.3 Authorization, and one thing that must not leak

`effectiveLevel` and `levelOptions` are reused unchanged, with two additional gates:

- **Flight is READ-only regardless of the peer's level.** Writes are a non-goal, so the Flight
  path asks for read-only `ServeOptions` even for a peer authorized at WRITE or DAEMON. Enabling
  an endpoint should never widen what a principal can do.
- **The type report filters private attributes at this boundary.** The profile reports no values,
  but it does report numeric **bounds**, and a bound on a private attribute is a disclosure even
  though the value is not. `AttrTypeProfiles` deliberately does not filter, because the filter
  belongs where the serving level is known. This is that place.

That second gate grew teeth when the type report started **evaluating** (§5.1). The op used to
classify wire nodes, and a sealed node classifies as sealed, so privilege never mattered to it.
Evaluating resolves references, so `Derived = Salary * 2` would carry `Salary`'s exact numeric
bounds into a profile named `Derived`, past the name filter. Two rules, both at the source: a
private reference resolves to undefined *inside the resolver*, where it is exact (a static walk
of the expression misses references reached by selection or subscript), and the connection's
entitlement reaches the op, so an unprivileged peer evaluates with no key.

### 6.4 What in-process costs, and the containment that replaces isolation

A pathological read now runs in the daemon's address space. This is not hypothetical: a novel
`COUNT` shape panicked htcondordb in production for a 15-second outage. A gateway would have
contained that; in process, containment has to be built.

- **A panic barrier per RPC.** A deferred `recover()` *in the handler goroutine itself* — a
  helper that wraps `recover()` in another function catches nothing, a mistake already made once
  in this codebase. **Still untested**: removing it passes the suite, because nothing in the
  fixtures panics and inducing one would mean an injection point in production code. Correct by
  inspection is not the same as covered.
- **A bounded Arrow allocator per stream** (`arrowtype.Budget`). `memory.Allocator` is
  `Allocate(size int) []byte` with no error return, and a nil slice makes a builder write into
  nothing, so **panicking is the only refusal the interface permits**. The budget panics with a
  typed error and the handler's barrier turns it into a failed request, reported as
  `ResourceExhausted` with the numbers in it and kept distinct from an internal fault, because
  the client's recourse is a narrower query rather than a bug report.
- **A concurrency cap on streams** that **refuses rather than queues**: a queue of scans inside
  the daemon is what the cap exists to prevent, and a refused client can retry while one left
  hanging cannot tell whether to.
- **Cancellation that reaches the scan.** The handler's context is the one the scan takes.
  **Not independently tested** — the flush select honours `ctx.Done()`, but nothing proves a
  disconnect mid-stream stops the scan rather than running it to completion for nobody.
- **Watch retained allocators.** Arrow batches are large allocations in the same heap as the
  block cache and the mmap'd segments; a retained encoder pool here is the shape that already
  produced a 3 GB heap once.

**Open:** port advertisement — address file, collector ad, both, or neither? Whether the UDS
listener should be on by default when the TCP one is not.

## 7. Status

### 7.1 Shipped

| | Where | Note |
|---|---|---|
| Type derivation (§5.1–§5.5) | `arrowtype/derive.go` | evaluate, coerce, widen int/real on exact bounds, text fallback |
| Row encoder | `arrowtype/encode.go` | per-column coercion counters; the caller owes it redacted ads |
| Value encoder | `arrowtype/encode.go` | `AppendValues` runs the same coercion, counting and null rules as `Append`, so a row encoded from values and the same row from its ad land identically |
| Schema fingerprint and pin (§5.7) | `arrowtype/fingerprint.go`, `flightsvc/pin.go` | the promised schema is the delivered one, or the fetch refuses |
| Tail column (§5.6) | `arrowtype/derive.go` | `_ad`, the row's residue as bracketed ClassAd text |
| Memory budget | `arrowtype/budget.go` | an allocator that panics, turned into one failed request |
| Serverless export (rung 0) | `arrowstream/`, `htcondordb-cli arrow` | an `.arrows` file, read by DuckDB's `read_arrow()` |
| In-process gRPC (§6) | `cmd/htcondordb/flight.go`, `flightsvc/session.go` | opt-in listener; a dbrpc loopback per connection |
| Authn and authz (§6.2–§6.3) | `flightsvc/authn.go`, `session.go` | token or anonymous → CEDAR's own `Authorizer`; READ-only whatever the peer holds |
| Containment (§6.4) | `flightsvc/statement.go`, `server.go` | panic barrier per stream, byte budget, stream cap that refuses rather than queues |
| Flight SQL method set | `flightsvc/server.go`, `statement.go`, `prepared.go` | catalogs, schemas, tables, table types, statement, prepared + bind |
| Typed value scan | `flightsvc/statement.go` | `opQueryValues`: evaluate in the store, ship columns, append to Arrow — no text |
| Aggregates, GROUP BY, ORDER BY, DISTINCT (§7.4) | `flightsvc/analytic.go` | the `repl` executor, with result columns typed from the SELECT list |
| Projected scan, archives included | `flightsvc/statement.go` | the refs-chasing projection op reaches an archive, pushes the projection down, keeps expressions evaluable |
| Schema cache, privilege-scoped | `flightsvc/schemacache.go` | stops a derivation per RPC |
| Declared columns (§5.7) | `flightsvc/declared.go`, `HTCONDORDB_FLIGHT_SCHEMA_FILE` | a column keeps its type and position across samples and restarts; new attributes append |
| Admin type overrides | `cmd/htcondordb/flight.go` | `HTCONDORDB_FLIGHT_EXCLUDE_ATTRS`, `_ATTR_KINDS`, `_STRICT_MIXED_KINDS`, `_TIME_ATTRS` |
| SqlInfo conformance | `flightsvc/server.go` | the entries a real driver asks for at connect, one of which failed every ADBC client while Go was green |
| ANSI → ClassAd lowering (§8) | `ansi/` | the comparisons, `IN`, `BETWEEN`, `IS [NOT] NULL`, `AND`/`OR`/`NOT`, and `?` holes |
| The same dialect in the REPL | `repl/dialect.go`, `-dialect` | one Arrow-column→SQL-kind mapping serves both front doors |
| Result-column typing | `repl/arrowcols.go` | the SELECT list typed against a table's declarations; shared, so the two front doors cannot disagree about what a query returns |
| Arrow output in the REPL | `repl/arrowout.go`, `.format arrow` | a SELECT's result as Arrow IPC; refuses a terminal |

**Prepared statements with bound parameters** are served. Two decisions in that increment are
worth keeping, both arrived at by trying the obvious thing first:

- **The lowered predicate is a FRAGMENT LIST, not a string with substitution markers.** A marker
  scheme has to prove no other part of the expression can contain the marker, and the one
  caller-controlled part is a string literal, whose rendered form can hold any byte that is not a
  quote or a backslash. Binding is a concatenation of pieces that were never adjacent, so there
  is nothing to search.
- **A parameter slot covers the WHOLE comparison, not the value.** A NULL bound value does not
  compare the column against null; it makes the comparison yield NULL, which drops the row —
  `false` in a ClassAd WHERE clause, and `Cpus == false` is not it. So the slot renders either
  `Cpus == 4` or `false`, decided where the operator is still in scope.

**A real ADBC driver is in CI, and it is not redundant with the Go tests.** Every Go test drives
arrow-go's own client against arrow-go's own server, which checks that two halves of one library
agree — not that the endpoint is conformant. Connecting `adbc_driver_flightsql` for the first
time failed immediately with `NOT_FOUND: no information for sql info number 1`:
`FLIGHT_SQL_SERVER_VERSION` was never registered, and **one missing SqlInfo entry fails a
client's whole `adbc_get_info()` call** — so the endpoint looked broken to every ADBC client
while the Go suite was green, because arrow-go's client never asks for it.

### 7.2 What a read costs

This is the measurement that decides the order of everything left.

`BenchmarkReadStages` and `BenchmarkReadShapes` read a 20 000-record columnarized archive with a
six-column projection, medians of three to five runs, Apple M2 Pro. Ad width is a dimension of
the benchmark rather than a detail: a six-column projection out of a nine-attribute record asks a
different question from the same projection out of a job ad, and the narrow case flatters
whichever path reads the whole record. The 45-attribute figures are the realistic ones.

**Where the text path's CPU went**, on the columnar tier:

| | ns/row | share |
|---|---|---|
| render each record to text | 7 827 | **47.9%** |
| ClassAd parse it back | 6 434 | **39.3%** |
| storage read + projection + loopback | 1 610 | 9.8% |
| evaluate + Arrow append | 484 | 3.0% |

**Render and parse were 87% of a read, and neither is work** — only the cost of a relay that
rendered text its consumer immediately parsed back. "Render" is literal: the text projection op
walked each matched record and built ClassAd source bytes, `Owner = "user43"\nClusterId = 1029\n…`,
which crossed dbrpc as a string. And that 87% is CPU, not payload: the text form is 156.3
bytes/row against 139.8, a ratio of 1.12x, so sixteen extra bytes a row through an in-process
pipe accounts for well under 1% of the gap. What the text path spent was the per-attribute work
of *formatting*.

**The typed value frame deleted it.** `opQueryValues` ships a column as a kind, a validity
bitmap, a dense slice of that kind's values, and an exceptions list keyed by row — which is the
shape an Arrow buffer wants. UNDEFINED and ERROR stay distinct rather than collapsing to null,
and a list or nested ad crosses as the unparser's text with a `NonScalar` flag, because text is
the only useful form of such a value but it must not arrive indistinguishable from a string.

| tier | projection | text ns/row | values ns/row | |
|---|---|---|---|---|
| record | literals | 12 592 | **6 220** | 2.0x |
| record | with an expression | 17 715 | **10 357** | 1.7x |
| columnar | literals | 5 561 | **2 113** | 2.6x |
| columnar | with an expression | 23 424 | **2 246** | **10.4x** |

End to end over gRPC, which is the number a user feels rather than the CPU it costs — same
fixture, same process, the only difference being which op the scan uses:

| | ns/row | allocations | bytes |
|---|---|---|---|
| text projection, parsed back | 13 882 | 2 522 920 | 251 MB |
| typed value stream | **2 234** | **365 927** | **41 MB** |
| | **6.2x** | **6.9x** | **6.1x** |

**The first version of that table said 2.06x, and it was measuring the consumer rather than the
change.** `ValueBatch.ValueAt` recounts the validity bitmap from row 0 on every call, so reading
a whole batch through it is quadratic in the batch size — and it is the obvious way to read one.
Walking each column once with a running dense index took the same read from 6 257 to 2 254
ns/row. Nothing about the op or the data changed; the accessor was three quarters of the cost. It
is worth recording how it was caught, because the green tests could not: splitting the read put
the store's own scan at 2 213 ns/row and the whole read at 6 257, which made "turning values into
Arrow" cost seven times what turning *ads* into Arrow had cost. A component getting dramatically
slower while doing strictly less work is not a result, it is a bug report.

**So the remaining attribution is:** store side (scan, projection, evaluation, column packing,
loopback) **2 213 ns/row**; everything this process adds to reach Arrow buffers, **41 ns/row**.
**~98% of a projected read is now the storage scan.**

Two things follow and are worth stating plainly. **The Arrow encoder is not the cost** — 3% of
CPU and a tenth of an allocation per row, with an expression-valued column among the six
deliberately, since that is the case that must evaluate rather than read. And **a columnar
producer's entire upside is inside `collections`**; there is nothing left to win on the consumer
side.

**One attribute was costing a projection its fast path, and fixing that was the largest single
win.** Projecting `MemoryUsage` reassembled 20 000 of 20 000 records against 349 for the same
projection with a literal in its place. An expression cell is escaped out of its column, so it
came back in the narrowed ad, the closure check correctly rejected it — it reads
`ResidentSetSize`, which the projection did not name — and after eight consecutive misses the
scan abandoned the columnar path for the rest of the segment. Columnarizing already computes a
qualifying expression cell once and keeps the literal beside the block, which is how the
aggregates read these attributes from columns; the projection resolved through the ad's wire
bytes and never saw it. Substituting that literal took the read from 19 092 to 2 246 ns/row and
246 to 18 allocations.

**The gate on that substitution is the whole safety argument, and the obvious choice was wrong.**
The storage layer's rule is that a computed value never appears in an ad as if it were the
attribute's stored literal — a consumer that *relays* what it is given would ship an ad different
from the record. `projPlan.closure` looks like the right licence and is not: it means the
consumer follows references, and `QueryRawWireRefs` does that and then ships the entries. Gating
on it put `Mem = 1` into relayed rows where the record holds `Mem = (Rss+1023)/1024`. The licence
is a separate, strictly stronger flag — the consumer sees only the evaluated result.

**`SELECT *` does not take the typed path, and the reason is structural.** The value stream
returns one column per *requested* attribute, as values; the tail column (§5.6) is the complement
of a whole record in its *stored* form, and the stream has neither the set nor the form. So the
two are mutually exclusive, and the endpoint picks by whether the schema carries a tail: an
explicit projection — what a client aggregating or joining actually issues — takes the fast path,
and `SELECT *` keeps the text one. Pinned by a test that asserts which op each query shape
reaches, because the rows are identical either way and the costs differ by 2x. Closing the gap
means a frame that can carry a record's residue alongside the typed columns, which is an upstream
change rather than a wiring one.

**What this does not establish.** One machine, one dataset, one read shape: six columns out of a
34-field schema. A wider projection moves work toward the scan and away from the per-record
overheads; `SELECT *` more so. The archive here was columnarized on demand in one pass with every
sealed segment covered, where a production table carries a backlog and an uncovered segment reads
the old way.

### 7.3 What is left

Ordered by value per unit of work.

1. **Two of the three type guesses §4 set out to replace are still there.** The Grafana plugin
   parses cells and keeps its own `knownTimeAttrs`; the DB-API cursor still documents
   `description` as "what arrived, not a schema". Both need typed columns to reach them over
   their own transports, so neither is the "nearly free" job this item used to claim — the
   plugin runs in Grafana's process and would have to derive the schema itself through
   `AttrTypeProfiles`, which is real work rather than wiring. `arrow()` on the Python client is
   the same shape. **`.format arrow` in the REPL is done** (`repl/arrowout.go`): a SELECT's
   result as an Arrow IPC stream, refusing a terminal because it is binary and every other
   format here is text. Three sources, because the honest typed form of each result differs —
   `SELECT *` encodes the ads against the table's schema with the tail, a projection evaluates
   each item per ad through the same `RowStreamer` the other formats use, and an aggregate,
   which has no ads, parses its cells back from display text against their declared types.

   **The branch order is load-bearing and nothing ordinary proves it.** Routing a projection
   through the aggregate branch passes every type and value assertion, because an ordinary
   fixture round-trips through text unchanged. What separates them is an attribute whose VALUE
   is the word `undefined`, which the display form also uses to spell "no value": read from the
   ad it is that string, parsed back from text it is a null. That is also a standing limitation
   of the aggregate branch, stated here rather than discovered.

2. **Then the columnar producer.** Last, and for a measured reason rather than an argued one:
   what is left of a read is the storage read itself, and it should be measured against the
   wire-native read rather than against the text path it replaced. The remaining blockers are all
   inside `collections`:

   - **There is no column-out API, and no selection.** `blockVecSource.LoadColumn` is private, and
     the only exported vectorized entries are `VectorEvalCount`, `ColumnarEvalCount` and the
     grouped aggregates — counts and group accumulators. Nothing hands back a `vm.Vec`, and
     nothing applies the match mask to produce the surviving rows' values. The compaction is
     genuinely new code, a gather per kind. The grouped path is the precedent to copy rather than
     a thing to extend: it reads the group and aggregate columns through the block's columns in
     **every** tier, including tiers where the predicate fell back to per-record evaluation,
     exactly so that group identity cannot depend on which tier answered. A column producer needs
     the same property for values.
   - **A column handed out aliases the block.** `loadStr` deliberately does not copy — one
     allocation per record made a string predicate slower than the row path — and the dictionary
     is per block and fold-sorted. So a producer either copies or pins the block for the life of
     the batch, and an Arrow `DictionaryArray` cannot span blocks without remapping codes. Sealed
     is not alive, which is the same lifetime question that has bitten a read taken off a segment
     here before.
   - **The computed region is a precomputed location, not a column.** Measured per block on a
     20 000-record fixture: `ResidentSetSize` and `JobStatus` are escape-free in 53 of 53 blocks,
     so a raw contiguous width-native vector is available for them today. `MemoryUsage` is
     escape-free in **0 of 53**, with **19 717 of 19 717 cells escaped**. It vectorizes —
     `fixEscapes` reads the computed region, which is why the aggregates can see it — but
     `escapeFree` is false, so it can never take the `rawColumn` path and instead repairs every
     record one at a time. That is the difference between a loop and a memcpy, on exactly the
     attributes anyone wants, and making a computed cell an ordinary column value is the single
     change that most increases what a column producer could hand out.

   What it wanted from the declaration it now has: a producer emitting one Arrow schema across
   blocks that vectorize and blocks that do not cannot derive that schema from whichever block
   it is looking at, and it no longer has to — the schema is declared before the scan starts.

   One blocker that was on this list is **gone**: vectorization declining per block. Measured on
   the same fixture, `blocks: vector=53 declined=0 churn=0 pruned=0 empty=0 rowWindows=1` — not
   one block fell back. It remains a correctness constraint on the producer (one stable schema
   across tiers) rather than a performance one.

   A second is gone too: **the dbrpc text boundary, and redaction with it.** `opQueryValues` is a
   columnar, typed frame, and because it lives inside the dbrpc server the private-attribute
   stripping and the private-constraint gate hold by construction. What remains is that its
   producer is per record: the server packs those columns one row at a time and then ships them
   column-major. So this is no longer "design a columnar protocol and re-solve redaction"; it is
   "put a vector producer behind a wire format that already exists". A vector producer must sit
   **inside** that boundary too, or Arrow becomes the private-attribute oracle.

**Still deliberately not built:** writes, Substrait, transactions and savepoints, key metadata,
`XdbcTypeInfo`, `CancelFlightInfo`, scalar/table UDFs, `ALTER TABLE`.

### 7.4 The analytic path

`SELECT <cols> FROM t [WHERE] [LIMIT]` is a scan with a filter, so the endpoint serves it by
scanning. An aggregate, a `GROUP BY`, an `ORDER BY` or a `DISTINCT` is not: the value of a row
depends on the other rows, so somebody has to hold the result set. `repl`'s executor already
does all of it, with the server-side TopK and the archive's own `GROUP BY` underneath, and the
endpoint used to refuse the lot — so a client wanting `COUNT(*) GROUP BY Owner` pulled every row
and counted them locally, over an endpoint whose store could have answered.

Two things make this more than routing, and both are about the schema.

**The result schema is not the table's.** `COUNT(*)` is an int64 column no attribute produced;
`AVG(x)` is a float64 whatever `x` is; `MIN`/`MAX`/`SUM` and `time_bucket` take their column's
own declared type; a window function is a count. So the columns are typed from the **SELECT
list**, against the table's declarations — which is what makes answering GetFlightInfo without
executing possible at all, and that is a protocol requirement rather than an optimization.
Everything is nullable except a count, which is defined for every group, zero included.

**The executor returns display text.** `repl.Result.Rows` is `[][]string`, so a cell is parsed
on the way into Arrow — the text round trip the scan path just finished deleting. It is
acceptable here and not there for a reason worth stating: an aggregate result is group rows
rather than table rows, and the parse is against a **declared** type rather than an inferred
one, so it cannot silently retype a column the way inference does. A cell that does not parse as
its declared type is a null, not a string. If an analytic result ever grows large enough for the
parse to matter, the fix is a typed row API out of `repl`, not a heuristic here.

**Read-only, structurally.** Only `StmtSelect` reaches the executor, checked before anything
else. The executor also runs INSERT, UPDATE, DELETE and DDL, and the endpoint's read-only
promise must not depend on the ANSI front end happening to refuse those spellings.

Three limits, each deliberate:

- **A JOIN or a set operation is refused**, the same refusal the REPL's ANSI dialect gives and
  for the same reason: a bare column could belong to either side, and typing it from the first
  table's declarations would type it from the wrong one.
- **No bound parameters.** The executor parses once and holds a ClassAd predicate thereafter, so
  a placeholder would have to be substituted before parsing, and the schema is promised before
  that. Such a statement still *prepares* — preparing is how a client learns its SQL will work,
  and this SQL will — and reports an empty parameter schema; binding to it is refused at the
  bind, saying so, rather than at the fetch saying "unsupported SQL" about a statement the same
  endpoint just prepared.
- **A computed expression becomes `utf8`.** Its value cannot be typed from a measurement — nothing
  measured `CurrentTime - QDate` — so it takes the answer the tail column takes: text, which is
  faithful for any ClassAd value. Refusing instead would make this endpoint strictly less capable
  than the REPL for a shape the REPL answers correctly. The cost is that arithmetic downstream
  sees a string, and the declared type map is what would fix it.

The executor materializes the whole result before the first cell reaches Arrow, because that is
what an aggregate *is*. The stream cap and byte budget apply around it, but the executor's own
working set is not bounded by them, so a `GROUP BY` over a high-cardinality attribute is the
shape to watch — the same way it is in the REPL.

**An aggregate is a disclosure channel of its own shape**, and building this found a hole that
the scan path does not have. Nobody has to see a value to learn it: `SELECT ClaimId, COUNT(*)
GROUP BY ClaimId` names every distinct value as a group key, `MIN`/`MAX` report two of them
exactly, and `COUNT(DISTINCT x)` reports the cardinality. The closure is the same one the scan
relies on — an unprivileged peer's type report does not carry a private attribute, so there is
no column to type against — but it only holds if every column the statement NAMES is resolved,
not only the ones it projects. The first implementation resolved the projected ones, which let
a READ peer through on `COUNT(DISTINCT ClaimId)` (a count's result type does not depend on its
argument, so the argument was never looked at), on `GROUP BY` and `ORDER BY` over a column that
is never printed, and on `HAVING MAX(ClaimId) > 'a'`. Caught by a test with a DAEMON arm proving
the fixture has something to withhold. The refusal says the column is not there, which is what
it also says about a column that genuinely is not there — naming it as private would answer the
question the refusal exists to refuse.

**The mutant that mattered** was the executor keeping its default dialect. ClassAd's `==` folds
case and ANSI's `=` does not, so an analytic query whose predicate took the default would answer
a different question — same columns, same types, more rows — and no type assertion anywhere
would catch it. Six were checked to fail named tests: that one, AVG typed from its column, a
non-SELECT admitted to the executor, an unparseable cell retyped to a string instead of nulled,
a nullable COUNT, and bound values silently ignored.

## 8. The ANSI dialect

**The wire form stays ClassAd; ADBC is always ANSI.** The ANSI front end is a *compiler*: it
parses SQL, binds it against the type map, and lowers to a ClassAd expression before anything
crosses dbrpc. The store keeps evaluating exactly one language.

That is better than making the dialect a connection option, which an earlier draft proposed.
There is no default to get wrong and no flip to stage — Flight SQL is ANSI from its first
release, so no query ever changes meaning underneath a client, and a client never has to ask
which dialect it got. The wire protocol does not change, so there is one constraint language to
version, test and fuzz. And `.dialect ansi` in the REPL is the same code path, which is how the
lowering gets tested without a gRPC stack anywhere in the picture.

**REFUSE, NEVER APPROXIMATE.** Note the asymmetry with Airport: there, dropping a filter we
could not translate was safe because the client re-filters. Here nothing downstream corrects us,
so the rule inverts. A construct we cannot lower faithfully is an error — and that is the whole
reason partial ANSI is safe to ship.

### 8.1 Case sensitivity, and why `=?=` is the answer

ClassAd's `==`, `!=`, `<`, `<=`, `>`, `>=` compare strings **case-insensitively**, matching the
C++ reference engine; only `=?=`/`=!=` are case-sensitive. The segment dictionaries are
fold-sorted too, so this is not skin-deep. SQL's `=` is case-sensitive.

ClassAd's identity operator is case-sensitive *and* the storage layer optimizes it directly:
there is a dedicated exact-case key run for `=?=`/`=!=` in the in-memory index, with a
case-uniform-bucket optimization so it does not double memory, and in the archive sidecar,
binary-searched. The planner already routes it. So ANSI `Owner = 'x'` lowers to `Owner =?= "x"`:
exact, case-sensitive, and an index probe. No grammar extension.

An earlier draft proposed `Owner == "x" && strcmp(Owner, "x") == 0`, which was solving a problem
the index already solves. For the record on why that would be merely *worse* and not
catastrophic: a strict built-in compiles natively and the program stays `Native()`, so it keeps
the column prefilter and the vector executor. What it does not get is an index probe, because the
planner matches operator *shapes*, not function calls. That is the reason not to reach for an
idiom elsewhere either: a function call is a comparison the planner cannot see.

**String *ordering* is refused** rather than lowered. Fold-order and byte-order results are
neither a subset nor a superset of one another — compare `"apple"` against `"R"` under each — and
the dictionaries are fold-sorted, so no index could serve a byte-ordered range anyway.

### 8.2 Three-valued logic, and the internal opcode that would close it

`=?=` returns **false** on a type mismatch, so `Owner =?= "x"` with `Owner` undefined is false
where SQL yields NULL. Under `AND` and `OR` that is unobservable for a WHERE clause (`NULL AND
TRUE` and `false && true` both drop; `NULL OR TRUE` and `false || true` both keep). It diverges
in exactly two positions: under `NOT`, where SQL drops a NULL row and ClassAd keeps it since
`!false` is true; and wherever the value is *surfaced* rather than filtered on (`CASE WHEN`,
`IS NOT TRUE`, a projected boolean).

An earlier draft proposed a **polarity-aware lowering** — bare `=?=` in positive position, a
guarded `isUndefined(x) ? undefined : (x =?= lit)` under negation. That works and is the wrong
trade: it produces two forms of every predicate, one of which loses the index, and polarity is
exactly the analysis that is correct in review and wrong inside a `CASE` inside a `NOT` inside an
`OR`. **The implementation refuses there instead.** Refusing is still a polarity analysis, but
its failure mode is "you cannot write that yet" rather than silently wrong rows, and it is
positional, so a string equality outside a `NOT` is unaffected and double negation returns to
positive.

**The way to lift the restriction is an internal opcode**, not a lowering trick: ANSI `=`, `<>`,
`<`, `<=`, `>`, `>=` as opcodes in the AST/`vm` space, case-sensitive and NULL-propagating, with
**no surface syntax**. Not a grammar extension — anything reachable from the external syntax is a
public API that cannot be withdrawn and would leak through the query log, `EXPLAIN` and
copy-paste. One lowering per operator, no polarity analysis, and the semantics stated once in the
opcode rather than encoded in an idiom. Equality maps onto the same exact-case index run `=?=`
uses, so it keeps the probe while carrying SQL's three-valued logic; ordering scans, because no
fold-sorted structure can prune a byte-ordered range, and being a scan should be visible in
`EXPLAIN` rather than discovered.

**Two costs, both real.** An operator must be implemented in **every** evaluation path or it
silently misbehaves on one — the scalar interpreter, the vectorized executor, the compiler, the
probe/index planner, and the archive sidecar, which are the same mutable/archive and
in-memory/persistent splits that have produced divergence bugs here before. A conformance suite
running every operator through every path is part of the work. And the lowered constraint crosses
`dbrpc` as **text**, because the Flight endpoint reads through an in-process loopback to reuse
the authorization layer (§6), so an internal opcode with no surface syntax cannot travel it as
things stand.

### 8.3 What is lowered, and what is refused

Lowered today (`ansi/`): `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`/`OR`/`NOT`, parentheses,
`IS [NOT] NULL`, `IN`, `BETWEEN`, the literals, and `?` placeholders. String equality lowers to
`=?=`.

Refused, each for a reason rather than for want of effort: string **ordering** (§8.1), string
**inequality** (its 3VL guard is correct but unverified against the index, and an unindexed
inequality over a large table is better introduced deliberately than discovered), a
**type-invalid** comparison (refused at bind time as a SQL engine does, rather than lowered into
ClassAd's ERROR where it would quietly filter out every row), an **unknown or untypeable**
column, and string equality **under a negation** (§8.2).

The lowering vocabulary available for the rest: `substr`, `size`, `regexp`, `replace`, `strcat`,
`int`/`real`/`string`, `toupper`/`tolower`, `split`/`join`, `member`, `formattime`, `interval`,
and the Elvis operator for `COALESCE`. What has **no** faithful ClassAd form and must be refused:
`DECIMAL` arithmetic, true `DATE`/`TIME`/`TIMESTAMP` types as distinct from epoch integers, and
any explicit `COLLATE`.

**The column types come from `arrowtype.Columns`**, the same mapping the Flight endpoint uses,
deliberately in one place: two surfaces lower SQL against these types, and a second copy would
let them disagree about a column's type and so about what a query means. Types are derived per
statement rather than cached, so a column added during a session is visible immediately.

### 8.4 The REPL's `.dialect`

ANSI is **strictly less capable** in the REPL, on purpose: `WHERE Owner > 'R'` works today and is
refused under ANSI. The dialect is a compatibility and debugging tool — the only way to see what
an ADBC client will get without an ADBC client in hand — not an upgrade path.

Three things about it are worth recording:

- **It translates at the PARSE boundary, not after it.** `Statement.Where` is "the translated
  ClassAd constraint", so by the time the executor runs, `Owner = 'alice'` is already
  `Owner == "alice"`, and handing that to a SQL parser produces nonsense rather than an error
  anyone can act on. The first implementation did exactly that. The parser now keeps the clause's
  raw text, which is what the ANSI lowering reads, and the translation happens once in `Exec` so
  the scan, a DELETE's selector and `EXPLAIN` all work from one constraint.
- **It recurses into wrapped statements** — `EXPLAIN` wraps the statement it reports on and
  `CREATE MATERIALIZED VIEW` embeds the SELECT that defines it.
- **A JOIN or a set operation is refused under ANSI**, because a bare column could belong to
  either table and lowering it against the first table's types would type it from the wrong one.

**One caution.** `.format` changes how results *print*; `.dialect` changes what a query *means* —
`WHERE Owner = 'bbockelm'` folds case under ClassAd and does not under ANSI. So the mode has to
be visible (prompt, `EXPLAIN`, and recorded on every logged statement alongside the lowered form)
and set explicitly by scripts rather than inherited.

## 9. Testing

The hazard is **vacuous green**: a test passes identically whether pushdown worked or every
filter was dropped, and whether a column came back `int64` or `utf8`. Row counts cannot tell the
difference. So assert on the **Arrow type** of every column against the declared map; on
coercion and null counters, at zero for Tier A columns and non-zero where a fixture plants an
escape; on a differential oracle, where the same logical query through the REPL and through
Arrow must agree including on case sensitivity; and, for a columnar producer, that blocks
actually vectorized rather than every block declining to the row path.

Two lessons the suite has already paid for:

- **A reader that is the writer's own library agrees with it by construction.** Two *external*
  clients check the endpoint in CI: the DuckDB CLI through `nanoarrow`, and
  `adbc_driver_flightsql` through gRPC, against the session daemon's own Flight listener — the
  same process that serves CEDAR, because that is how the endpoint deploys and a separate daemon
  would test a configuration nobody runs. `HTCONDORDB_REQUIRE_ADBC=1` turns a skipped driver into
  a failure, and the CI step greps for named passes, because neither pytest's exit status nor a
  REQUIRE flag can speak for a test that was never collected.
- **A new read surface needs an archive in its fixtures from the first test.** `SELECT * FROM
  history` returned an error for a while: the endpoint listed the archive table, derived its
  schema, and then failed the read, because `opQuery` refuses an archive by design. Nothing
  caught it — every Flight test ran against a single mutable table, and the one test that
  mentioned an archive name only checked that the recogniser accepts a `/`.

Keep the integration suite inside the usual runtime budget; DuckDB-in-the-loop tests run on
demand, not on every push.

## 10. Risks

**An encoder that evaluates expressions must evaluate through the redacted path** (§5.1) or it
becomes the private-attribute oracle `private_gate.go` exists to prevent.

**In-process gRPC means a pathological read runs in the daemon's address space** (§6.4): the
panic barrier, bounded allocator, concurrency cap and real cancellation are not polish, they are
what replaces the isolation a separate process would have given. A novel query shape against
production has crashed this daemon before, so every new read path is exercised against a local
store first.

**A columnar producer must not let an Arrow schema vary** as blocks alternate between columnar
and row-wise.

**Catalog serialization cost for wide tables** — measure against a real jobs table early.

---

## Appendix A. The road not taken: Airport (rung 3)

Recorded so it does not have to be rediscovered. Everything is msgpack-serialized C++ structs;
`FlightInfo` and schemas are Arrow IPC. Pin these against a real DuckDB build rather than
trusting this summary.

- **`DoAction("list_schemas")`.** In: `{catalog_name}`. Out: msgpack `{length, data}`, `data` =
  zstd-compressed msgpack of `{contents, schemas[]}`; each schema's `contents` is
  `{sha256, url?, serialized?}` wrapping a zstd msgpack array of serialized `FlightInfo`s, one per
  table, each with msgpack `SerializedFlightAppMetadata` in `app_metadata`. **Caching is keyed on
  the SHA256** — if present, DuckDB caches on disk indefinitely and will not re-fetch, so the hash
  must change exactly when the schema does.
- **`DoAction("endpoints")`.** In: `{descriptor, parameters{json_filters, column_ids,
  table_function_parameters, table_function_input_schema, at_unit, at_value}}`. Out: msgpack
  vector of serialized `FlightEndpoint`s, fetched **in parallel** by DuckDB's thread pool; their
  union is the whole flight, so **all endpoints must read the same snapshot** — embed an AS OF
  token in every ticket. `at_unit`/`at_value` map onto our AS OF reads, which very few Flight
  servers can honor. There is **no LIMIT**, so our server-side TopK is unreachable from DuckDB.
- **Identifier case.** DuckDB does not fold before sending: `SELECT * FROM FooBar` asks for
  `FooBar`. ClassAd attribute names are already case-insensitive; table names in our catalog are
  not, so fold there.

**Pushdown.** `column_ids` maps onto existing projection pushdown. `json_filters` is a tree of
DuckDB bound expressions: `BOUND_COMPARISON`, `BOUND_CONJUNCTION`, `BOUND_OPERATOR`,
`BOUND_COLUMN_REF`, `BOUND_CONSTANT`, `BOUND_FUNCTION`. **Airport re-filters on the client**, so a
translator that handles what it understands and drops what it does not is correct, merely slower —
which is what would make rung 3 tractable at all. One sharp edge: an unknown expression class,
function or type means **drop that filter**, never fail a read; a dropped conjunct inside an `AND`
is fine because we return a superset, but a dropped conjunct inside an `OR` means **the whole
disjunction must be dropped**, not one arm. That is the easiest place in the design to introduce
silent wrong answers.

DuckDB's documentation states outright that this serialization may change in any release.
