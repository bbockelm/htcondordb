# Design sketch: sub-selects

**Status:** draft; both arms are implemented for a single correlated predicate.
**Problem owner:** the `repl` SQL frontend (and every surface built on it: `htcondordb-cli`,
the Python DB-API driver, the Grafana datasource).

## 1. The problem

`repl` has `JOIN`. What it does not have is a way to ask whether a row has a counterpart
somewhere else *without* folding that somewhere-else into the answer:

```sql
-- "which held jobs have never actually run?"
-- As a join this is wrong twice over: it multiplies a job by its epoch rows, and there is
-- no join that keeps the rows which matched NOTHING except a LEFT join plus a null test.
SELECT j.ClusterId FROM jobs j LEFT JOIN epoch_history e ON e.GlobalJobId == j.GlobalJobId
 WHERE j.JobStatus == 5 AND e.GlobalJobId is undefined
```

That spelling also forces every other reference in the statement to become alias-qualified,
so adding the test changes how the rest of the query must be written.

`EXISTS` and `IN` are the constructs for this. They filter one table and contribute no
column, so the result is shaped exactly like the unfiltered scan and nothing else in the
statement has to change.

## 2. What exists to build on

| Mechanism | Where | What it gives |
|---|---|---|
| Semi / anti join | `classad/db/joinsemi.go` (v0.40.1) | exactly `EXISTS` / `NOT EXISTS`: a row kept once when a side matches, carrying **nothing** from that side |
| Join opcodes, server side | served by every daemon — `server/server.go` builds `dbrpc.NewServerCatalog(cat)` | `opJoin`, `opJoinChain`, `opJoinAgg`, and their wire-row forms, already deployed |
| `Statement` → `db.JoinSpec` | `repl/join.go` `joinServerSide` | the store arm of the existing SQL `JOIN`; semi/anti is a `Kind` it does not yet send |
| Placement model | `repl/joinplace.go` | which arm pairs the rows, from the expansion factor and the link capacity |
| Typed value stream | `dbrpc.QueryValuesStream` | one column, evaluated server-side, shipped column-major — no record decode anywhere |
| Conjunct classification | `repl/join.go` `conjunctsOf`, `sidesOf`, `stripSide` | split a predicate by which table each conjunct reads |
| Literal rendering | `repl/join.go` `literalOf` | a `classad.Value` as the ClassAd literal naming it |

`repl`'s own `JoinKind` has only `JoinInner`, `JoinLeft` and `JoinCross`, and the store arm
sends only the two-table spelling (`unsendableJoinWhy` refuses a chain). Both are small gaps
rather than missing machinery.

## 3. Surface

```sql
-- IN over a value list (shipped)
SELECT * FROM jobs WHERE JobStatus IN (1, 5)

-- IN over a sub-select
SELECT * FROM jobs WHERE Owner IN (SELECT Owner FROM users WHERE Priority > 100)

-- correlated EXISTS
SELECT Name FROM machines m
 WHERE EXISTS (SELECT 1 FROM jobs j WHERE j.GlobalJobId == m.JobId AND j.Owner == "alice")

-- correlated NOT EXISTS: held jobs that never ran
SELECT ClusterId, ProcId FROM jobs
 WHERE JobStatus == 5
   AND NOT EXISTS (SELECT 1 FROM epoch_history e WHERE e.GlobalJobId == jobs.GlobalJobId)
```

`FROM <table> [AS] <alias>` already exists, and the alias defaults to the table name, so the
last example needs no alias to say `jobs.GlobalJobId`.

### 3.1 Scope resolution: a bare name is always the inner table

In the subquery's `WHERE`, `GlobalJobId` means the *subquery's* attribute, always. An outer
reference must be qualified. SQL's implicit outward fallback is not adopted:

1. There is no schema to check it against. ClassAd rows are heterogeneous, so "the inner
   table lacks it" is not a question with an answer, and a typo would silently become a
   correlation and change what the query means.
2. It matches the engine's own rule — `db.JoinEqui`'s halves are in different scopes, `Right`
   a bare path into the folded table and `Left` qualified — so the lowering is a
   transcription rather than a translation.

## 4. Semantics decisions

**All four spellings are one question.** `x IN (SELECT y FROM t WHERE p)` asks exactly what
`EXISTS (SELECT 1 FROM t WHERE p AND t.y == x)` asks; the `NOT` forms are the same question
answered the other way round. One lowering, four spellings.

**Undefined follows ClassAd, not SQL's three-valued logic — and this is forced, not chosen.**
The two arms must agree, and neither can produce SQL's answer: `literalOf` has no literal for
undefined, so such an inner row contributes nothing to the key set; on the join arm a ClassAd
equality against undefined does not hold, so such a row matches nothing.

Two consequences, both pinned by `TestExistsAndNotExistsPartitionTheTable`:

- **On the inner side**, an undefined value contributes nothing to the set, so `NOT IN` keeps
  the rows that are not in what remains — where SQL would return no rows at all, because the
  set contains a NULL.
- **On the outer side**, a row whose own correlation key is undefined matches nothing, so
  `EXISTS` drops it and `NOT EXISTS` keeps it. **The two partition the table**, and a job with
  no `ClusterId` *is* reported as a job that never ran.

The second one is where the arms had to be made to agree rather than found to agree. The
store's anti join keeps a row that matched nothing, as it should; the obvious membership
spelling `!member(k, {...})` does not, because `!undefined` is undefined and an undefined
constraint keeps no row — so it silently dropped exactly the rows a `NOT EXISTS` is usually
asked about. The membership arm therefore tests `!(member(k, {...}) =?= true)`, which says
"the membership is not definitely true" and gives the same answer for a key that is
undefined, an error, or a value with no literal. Nothing is lost: a negated membership is not
an index probe either way.

**This was found by the differential test, and only after the store arm was made to actually
run** — its first version passed while both arms were the membership arm, and the semantics
originally written into this document were the buggy arm's.

**Duplicates do not multiply.** A semi join emits each outer row once however many inner rows
match. This is the property that makes the construct safe: no statement can grow its own
result set, so nothing downstream has to change.

**A semi join is a filter, so its result is shaped like the scan.** The predicate is folded
into the statement's `WHERE` and the statement stays single-table as far as projection and
scoping are concerned — `IsJoin()` is false, references stay unqualified, `SELECT *` returns
job rows. When the store arm lands it will return a composite `[ jobs = [...] ]` carrying one
member, and the executor must unwrap it rather than let it reach the projection.

**Hoisting an outer-only conjunct is sound only for a positive predicate.** A conjunct of the
subquery's `WHERE` that reads only the outer row does not depend on the subquery's table, so
it can filter the outer scan instead — but only for `EXISTS`, which returns something *only*
when the conjunct holds and an inner row matches, making the conjunct necessary.
`NOT EXISTS (... AND o.P)` is true when `!o.P` **or** nothing matched: a disjunction that
neither a conjunct of the outer filter nor a step of a join can express. Hoisting it anyway
answered the opposite question on every row where `o.P` does not hold — it returned *no rows
at all* for the obvious test case.

Both arms had that fault, so the differential test could not see it. It was found by
re-deriving what the predicate means, which is the only thing that finds a fault two
implementations share.

## 5. Where a subquery may appear

Only as a **top-level conjunct of `WHERE`**. This is not a parser limitation; it is what
lifting a predicate means. With the pieces `a OR EXISTS (...)` and `c`, the clause means
`a || (EXISTS && c)`, and lifting the `EXISTS` out would answer `(a || c) && EXISTS`. So a
clause that lifts anything must be a pure AND-spine, and one that is not is refused.

| Form | state | why |
|---|---|---|
| `WHERE a AND EXISTS (...) AND NOT EXISTS (...)` | done | two predicates, both folded in |
| `WHERE x IN (SELECT ...)` / `NOT IN` | done | the same lowering with a synthesized equality |
| uncorrelated `EXISTS` | done | a constant: one row settles it |
| `WHERE EXISTS (...) OR x == 1` | refused | not a conjunct |
| correlated on a non-equality (`e.Ts > j.QDate`) | refused | a nested loop bounded only by the pair budget, which returns a partial answer when it hits it |
| correlated on more than one equality | refused | a composite key; the store arm will take it |
| an outer-only conjunct inside a **negated** subquery | refused | see §4 |
| aggregate / `LIMIT` / `ORDER BY` inside the subquery | refused | each changes *which* rows the predicate is drawn from |
| scalar `WHERE x > (SELECT AVG(...))` | done | constant folding: run once, written into the predicate as a literal. It is an operand rather than a postfix keyword, so it needs nothing already written through and is accepted anywhere in a `WHERE`, including inside an `OR` |
| correlated scalar | refused | a different value per row is a per-row aggregate, not a constant |
| `FROM (SELECT ...)`, `WITH` | not planned here | derived tables cost the outer query its pushdown; separate design |

## 6. The two arms

### 6.1 The membership arm (implemented)

Collect the inner side's key values and give the outer scan
`member(<outer key>, {<values>})`, or its negation. One extra round trip, no new opcode,
works against every server.

What it reads is the point: `QueryValuesStream` projects the **one** key attribute, evaluates
it server-side and ships the results column-major and typed. Nothing on this side decodes a
record, builds a `ClassAd` or parses text. That matters precisely because this is the arm
that runs when the inner side is big enough to care about.

Its limit is the constraint it builds. `member(k, {...})` folds to a single "in" index probe,
but the probe's candidate estimate runs over as the set grows — `collections/batchlookup.go`
measures 1024 keys estimated at the whole collection where the truth is 8.5% — so past some
size the outer read degrades to a full scan carrying a very large constraint. Hence a cap
(`ExecConfig.MaxInListValues`, default 1024), past which the predicate refuses.

**The cap is now measured** (`BenchmarkSubqueryArms`, 20,000 outer rows, every row matching,
in-process pipe):

| distinct keys | membership | store (wire) | store (text) |
|---|---|---|---|
| 16 | 17.1 ms | 17.2 ms | 52.6 ms |
| 256 | 17.2 ms | 17.2 ms | 50.1 ms |
| 1024 | 18.9 ms | 18.4 ms | 57.6 ms |
| 4096 | 21.9 ms | 19.3 ms | 53.1 ms |
| 16384 | 35.9 ms | 21.5 ms | 55.7 ms |

The crossover is around a thousand keys, which is where the cap already sat. Below it the
arms are indistinguishable — both pay for the same outer read. Above it the membership arm
degrades with the set (2.1× from 16 to 16,384 keys) while the store arm is nearly flat
(1.25×), so the store wins by 1.67× at the top of the sweep and by more past it.

Both of the sweep's biases favour the membership arm — an in-process pipe charges nothing for
bytes, and every outer row matches, which is the most the store arm can ever ship — so the
crossover on a real link is at or below this.

### 6.2 The store arm

Send the predicate as `db.JoinSpec{Kind: JoinSemi | JoinAnti}`, with the outer table as
`Left` carrying the residual `WHERE` and the projection, and the subquery's table as `Right`
carrying its own filter and the correlating equality as `Equi`.

The placement question that `joinplace.go` answers for an ordinary join **does not arise
here**, and that is worth stating as a rule rather than rediscovering per statement. The
model prices the store arm's extra bytes — the left row repeated once per output row — against
its CPU advantage, and finds the crossover is a function of the expansion factor `k` alone,
with `need = joinLinkCrossoverMbps * (k - 1)`. A semi or anti join emits each outer row **at
most once** and carries nothing from the other side, so `k <= 1` by construction, `need <= 0`,
and the store arm ships fewer bytes *and* spends less CPU at every link capacity. In
`placeJoin`'s own words for that case: "the store ships no more than this would at any link
capacity."

So: a subquery predicate goes to the store whenever the store can serve **wire** rows for it,
and the membership arm is what runs otherwise — for an older server, a read inside a
transaction, an `AS OF` statement, an aggregate query, and as the fast path for a small key
set.

**The arms are ordered by the measurement, not by preference for the store.** A store that
cannot serve wire rows does not get the predicate, because its rendered reply is the slowest
of the three paths. The one case that reply still answers is the one the membership arm
cannot — a key set past the inlining cap — and there it is retried, because slow beats
refusing.

**It needs a `classad` release to be worth taking at all.** The third column above is this
same store arm on the text reply, which is what a server without the fix below serves: 2.4× to
3.1× *slower* than the membership arm at every size, because every joined row is rendered at
one end and parsed at the other — about 35 ms for 20,000 rows, more than the whole query. On
the text path the store arm is a pessimization.

 `db.JoinWire` refuses a redacted
wire-form join unless every side carries a projection, and a semi join's tested side must
carry none — `refuseSemiSideRefs` rejects a spec that projects from a side contributing no
member. The two rules are each sound alone and contradict together, so every redacted semi
join fell back to the text row stream, which is the render-and-reparse path the wire op
exists to avoid. A connection is redacted unless privileged, which for the daemon means
DAEMON level, so that is every ordinary user. The one-line fix is prepared on `classad`'s
`semi-wire-redact` branch (skip a side that contributes no member); with it in place the
store arm sends `JoinWire`, no row is rendered or parsed, and the numbers above are the wire
column. **Until it is released, the store arm runs on the text reply and is slower than the
membership arm** — which is an argument for releasing it, not for preferring the text path.

Several predicates in one statement go as the **chain** spelling, one step each, which
`dbrpc` routes to its own opcode when `Spec.Steps` is set. Past the first step the equality's
outer half reads the *accumulated* row rather than the left table, so it has to name the
member it reads — and because every step here is a semi or anti join, folding in no member,
that row still holds exactly the left side. Prefixing its alias is sound for a bare attribute
name; a predicate past the first that correlates on an expression falls to the membership arm
rather than having every reference in it rewritten.

What it still needs:

- `JoinAggregate` for `GROUP BY` over a predicate, which today falls to the membership arm
  (where the single-table aggregate still runs server-side, so this is a smaller gap than it
  looks).

### 6.3 The arms must be held to the same answer

Two implementations of one question that merely look right is a failure mode this codebase
has paid for before. Every subquery fixture runs through both arms — the cap forced to zero
(always the store) and to infinity (always membership) — and the rows must be identical.
That test is what lets the cap be tuned later without re-arguing correctness.

One difference the test cannot cover, which is therefore documented instead: the membership
arm issues **two reads**, so a concurrent write can land between them, where the store arm is
one operation over one view. `FOR SYSTEM_TIME AS OF` pins both when it matters.

## 7. Authorization

htcondordb authorizes per connection, not per table (`server.Level` is READ / WRITE /
DAEMON), so reading a second table through a subquery grants nothing a plain
`SELECT ... FROM thattable` did not. What still needs a test is that private-attribute
stripping applies on a join *side* when the store arm lands.

## 8. EXPLAIN

`EXPLAIN` must say which arm ran and, for the membership arm, how large the key set was —
those have very different costs and the user's only other signal is the clock. Note that
resolving a predicate means *running* the subquery, so `EXPLAIN` over one does read the inner
table; that is what decides the plan it is reporting.

## 9. Staging

1. `IN` value lists, the subquery parser, the predicate lifted out of `WHERE`. **Done.**
2. The membership arm: correlated `EXISTS` / `NOT EXISTS` / `IN` / `NOT IN`, uncorrelated
   `EXISTS`, the cap. **Done.**
3. The store arm, with the differential test of §6.3 standing between them.
4. Scalar uncorrelated subqueries (`> (SELECT AVG(...))`). **Done.**
5. Measure the cap against the store arm and set it to the crossover.
