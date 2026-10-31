# Design: SQL `JOIN` for HTCondorDB

**Status:** draft — for discussion, not yet a spec.
**Scope:** `repl` (parser/planner/executor), `classad/collections` + `dbrpc` (server-side join).

## 1. The problem

The SQL layer refuses joins on purpose. `rejectJoins` (`repl/sql.go`) rejects
`JOIN/INNER/LEFT/…` with "the store is a single ClassAd collection", and the package
comment says cross-table work is matchmaking, not a join. `MATCH` (op 22) is the only
two-table operator.

That was the right call when the store held one collection. It no longer is: the catalog
holds many tables, the mirror carries `jobs`, `history`, `job_metrics` and the collector
ads, and the questions people actually ask span them. We want standard SQL `JOIN`.

The constraint that shapes everything below: **the join predicate must be a ClassAd
expression, exactly as `WHERE` already is.** Not a SQL dialect with ClassAd-flavoured
operators — the same expression language, the same engine, the same three-valued logic.

## 2. The central idea: a joined row is a ClassAd

```
[ j = [ ClusterId = 42; Owner = "alice"; ... ];
  h = [ ClusterId = 42; ExitCode = 0;    ... ] ]
```

Once the row has that shape, `ON`, `WHERE`, `SELECT`, `GROUP BY`, `HAVING`, `ORDER BY`,
`CASE` and window `PARTITION BY` are **unchanged** — they stay ClassAd expressions
evaluated against one ad. `ast.SelectExpr` already evaluates `j.Owner` that way
(`classad/evaluator.go`, `evaluateSelectExpr`), and `SELECT j.Owner` already routes into
the general-expression path in `parseSelectItem`, because `.` lexes as an operator.

So the language gains **no new evaluation rules** — only a new row shape. Three
consequences fall out, and they are the argument for doing it this way rather than
grafting on a SQL join:

- **Outer joins need no NULL.** An unmatched `LEFT JOIN` emits a row with no `h` member,
  so every `h.X` is `undefined` — ClassAd's own three-valued logic.
- **`ON` is as expressive as `WHERE` already is.** `ON j.RequestMemory <= m.Memory &&
  j.RequestCpus <= m.Cpus`, `ON member(h.Site, j.AllowedSites)`, `ON regexp(...)`.
  Non-equi joins are first-class, not an error.
- **`=?=` is the escape hatch** SQL spells `IS NOT DISTINCT FROM`, and users here already
  know it.

The composite ad is the **semantic reference**. It is not necessarily how the hot path
evaluates (§5, §6); where a faster path exists it must be proved equal to this one.

## 3. Grammar

```sql
SELECT <items>
FROM <table> [[AS] alias]
  [INNER | LEFT [OUTER] | CROSS] JOIN <table> [[AS] alias] ON <classad-expr>
  [ ... further JOINs ... ]
[WHERE <classad-expr>] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT n]
```

`ON` is captured verbatim by the existing `captureRawExpr`, through the same SQL→ClassAd
adaptation `WHERE` gets (`=`, `<>`, `AND`/`OR`/`NOT`, `IS NULL`, single quotes, `CASE`) —
see `sqlexpr.go`. An absent alias defaults to the table name. `SELECT *` expands to every
attribute of both sides, alias-prefixed; `SELECT j.*` to one side's.

**Name resolution.** In a join query every attribute reference must be alias-qualified.
An unqualified `Owner` is **refused**, naming the aliases, rather than resolving to the
driving table. The codebase already takes this line for `HAVING` (a reference that is
neither grouped nor aggregated is refused rather than silently evaluating to undefined and
dropping every group), and the failure here is identical: `WHERE ExitCode == 0` meaning
`h.ExitCode` would silently drop every row. For two-table joins `MY.`/`TARGET.` are
accepted as synonyms for left/right, since that is the HTCondor idiom.

`RIGHT`/`FULL OUTER`, subqueries and `USING (col)` are refused with a clear message.
Self-joins (one table, two aliases) and N-way left-deep chains follow from §2 for free.

## 4. Alias normalization is mandatory, not an optimization

`j.Owner` parses as `ast.SelectExpr`, and `vm`'s compiler emits `OpEvalNode` for a
`SelectExpr` (`collections/vm/compile.go`), which makes `Query.Native()` false
(`vm/wirematch.go`). `Native()` is not a minor gate — it controls the wire-native match,
the columnar prefilter, vectorization, and projection pushdown at seven call sites
(`wireOK: q.Native() && plan.PartialSafe`).

Worse, `walkPlan`'s `SelectExpr` case walks only `v.Record`, so `j.Owner` seeds the read
plan with **`j`** and never `Owner`: the projection would fetch the wrong attributes.

So the first pass after parsing rewrites alias-qualified references into **scoped
`ast.AttributeReference`s** — `j.` → `MY.`, `h.` → `TARGET.`. One rewrite then serves:

- the scalar fast path (`MatchClassAd.EvaluateExprLeft`, which sets up `MY`/`TARGET`),
- `Query.Native()`, and with it every pushdown the store already has,
- the vectorized executor (§6),
- the read plan, hence per-side projection pushdown.

**Test it directly:** assert `Native()` is true for every join predicate the planner
accepts. Without that assertion a join silently falls off every fast path and merely looks
inexplicably slow — which is exactly the failure `vecSplit`'s counters exist to catch
("three earlier measurements in this package produced plausible and wrong conclusions").
The join needs its own equivalents (§7, §9).

## 5. Planning: conjunct splitting, and why ClassAd's left bias permits it

### 5.1 The splitter

One pass over the `&&` spine of `ON` and `WHERE` (the `dedupConjuncts` walk in
`collections/match_index.go` is the idiom) classifies each conjunct as left-only,
right-only, or genuinely cross-table. Single-side conjuncts become **server-side
constraints on that side's scan**, so `WHERE j.Owner == "alice"` never streams another
owner's ads to the client. Projection pushdown follows: each side fetches only the
attributes its clauses reference.

### 5.2 Outer-join pushdown is asymmetric on **both** clauses

This is the classic silent wrong answer in every join implementation, so it is stated
exactly and pinned by a test:

| | `ON` conjunct | `WHERE` conjunct |
|---|---|---|
| `INNER JOIN` | pushes to either side | pushes to either side |
| `LEFT JOIN` | pushes to the **inner (right)** side only | pushes to the **preserved (left)** side only |

A left-only `ON` conjunct must **not** filter the left scan: a left row failing `ON` still
appears, null-extended. A right-only `WHERE` conjunct must **not** filter the right scan:
it belongs after null-extension, where it correctly turns the outer join into an inner one
only if that is what the user wrote.

### 5.3 Left bias constrains the splitter, but does not break it

ClassAd's `&&` is genuinely non-commutative (`classad/evaluator.go`, `evaluateAnd`):

```
false && anything  -> false
error && anything  -> error        (the right side is never consulted)
undefined && false -> false ; undefined && true/undefined -> undefined
```

So `error && false` is error while `false && error` is false. Reordering is **not**
value-preserving.

Pushdown survives anyway, for one narrow reason worth writing into the code. A record
matches only on a **true boolean**:

```go
func (q *Query) Matches(scope *classad.ClassAd) bool {
	v := Run(q.prog, scope)
	b, err := v.BoolValue()
	return err == nil && b
}
```

Undefined and error both mean "no match". And in that table `true` is produced only by
`l == lsTrue && r == lsTrue`. Therefore `A && B` is true iff both are true,
order-independently — splitting the spine preserves the match set exactly, reordering
preserves it exactly, and residual evaluation on survivors is correct (`true && B == B`).

**The invariant: the split is sound for predicates, not for values.** Which gives three
rules:

1. **Gate the splitter on clause kind.** `ON`/`WHERE`/`HAVING`/`QUALIFY` are predicates.
   `SELECT` items, `CASE` arms, `ORDER BY` keys and aggregate arguments are *values*, where
   error-vs-undefined-vs-false is observable. The splitter must not touch an `&&` there
   just because it found one.
2. **Order conjuncts by selectivity, not by cost of the right operand.** The vectorized
   executor makes `OpShortAnd`/`OpShortOr` no-ops — both sides always evaluate and
   `OpCombineAnd` merges them through the same `evaluateAnd(left, right)` in the original
   order. Bias is preserved; only the *skipping* is lost. So there is no short-circuit
   saving to plan around.
3. **Partial evaluation must widen and re-verify.** Binding a left row's values into the
   `ON` AST substitutes `undefined` for what it cannot resolve, and `undefined && true` is
   undefined, hence excluded, where the real value might have matched. `slotMatchExpr`
   already handles this by ORing in one `name isnt undefined` disjunct per assumed
   attribute and then **re-verifying every surviving candidate** against the full
   predicate. The join inherits both halves. Treating the rewritten residual as the answer
   rather than as a candidate filter is the unsound shortcut, and this package has already
   shipped match-reorder and zone-map soundness bugs of that family.

## 6. Execution

A planner picks among three strategies; `EXPLAIN` names which it picked, what it pushed to
each side, and the estimated rows.

**(a) Hash join — the workhorse.** Extract equi-conjuncts `leftExpr == rightExpr` from the
`ON` spine; build from the smaller/more selective side, stream the other, re-verify
residual non-equi conjuncts per candidate pair. Keyed on evaluated `classad.Value`s, so
the hash key honours ClassAd coercion rather than Go types.

**(b) Key-batched lookup.** When the right side's equi-key is its primary key attribute or
a value/categorical-indexed attribute, probe instead of scanning: batch left keys into
blocks and issue one `member(ClusterId, {…})` constraint per block. The common case
(`jobs JOIN history ON j.ClusterId == h.ClusterId WHERE j.Owner == "alice"`) costs a
bounded number of right-side probes rather than a full scan.

**(c) Indexed nested loop — reuse the matchmaking machinery.** For an arbitrary ClassAd
`ON` with no equi-conjunct, do what the negotiator pushdown does: bind the left row's
values into the `ON` AST (`rewriteForSlot`/`slotMatchExpr`), compile the residual with
`vm.Compile(...).ProbePlan()`, and let `planIndexGroups` turn it into index candidates. A
non-equi `ON j.RequestMemory <= m.Memory && j.RequestCpus <= m.Cpus` becomes an indexed
range probe per left row. No other SQL engine gets this for free; here it is already
written and tested — subject to §5.3 rule 3.

Every strategy carries a hard candidate-pair budget that **refuses** with a message rather
than running away. A novel `COUNT` shape panicked htcondordb in production for 15 seconds;
an operator that can generate quadratic work needs its brake designed in, not added later.

### 6.1 The vectorized executor

`vm.ColumnSource` is deliberately storage-agnostic:

```go
type ColumnSource interface {
	LoadColumn(name string, scope ast.AttributeScope, dst *Vec) bool
}
```

documented as "the collections package implements it over a columnar block; **anything
that can produce n values per attribute can**". It already takes a scope, and the sole
implementation declines everything self-unscoped:

```go
func (s *blockVecSource) LoadColumn(name string, scope ast.AttributeScope, dst *vm.Vec) bool {
	if scope != ast.NoScope && scope != ast.MyScope {
		return false
	}
```

**`TargetScope` is an unimplemented slot, not a missing concept.** A join `ColumnSource`
serving `MY.`→left and `TARGET.`→right lets `VecEval` evaluate a join predicate over a
batch with no new executor and no second IR; the compiler already carries scope into the
program (`c.addRef(v.Name, v.Scope)`).

The best shape is better than generic batch-vs-batch. In a block-nested loop **a left
row's attribute is a constant across the right batch**, so the join predicate degenerates
to the one shape this engine is fastest at — a column compared against a literal, which
`simdCompareRaw` specializes width-native, and which for a dictionary-encoded string
becomes a contiguous integer range test.

Honest limits: strings decline unless dictionary-encoded, so numeric keys
(`ClusterId`/`ProcId`) are the sweet spot and `Owner` vectorizes only where the block
dictionary covers it; and vectorization lives on columnar blocks, so `jobs ⋈ history`
vectorizes on the history side — which is, fortunately, the big one.

## 7. Block-looping and zone-map block skipping

This is where a join inside the store beats any join above it, and it is only reachable
from inside `collections`: a client cannot see block metadata. It is therefore a
server-side capability — **Phase 2** for MATCH, **Phase 3** for the join (§10).

### 7.1 What the storage already records

- **Segment zone map** — per sealed segment, `zoneRange{Min,Max}` per configured
  `ZoneAttr`, persisted in the sidecar (`zonemap.go`, `adoptPersistedZones`). Append-only
  collections only; the active segment has nil zones and is always scanned.
- **Block zone** — per columnar block (row group), `blockZone{zoneRange, escaped}` per
  numeric schema field, looked up by binary search (`colblock.go`, `zone(idx)`).
- **`escAbsent`** — a bit per schema field, set when **no** record in the block carries it:
  an exact whole-block proof that the attribute is undefined there.
- **Block string dictionary** (`strDictField`) — a fold-sorted dictionary per string
  column, with `DictResolver.Range(lit)` giving a contiguous code range.
- **Group blocks** — `colGroupBlock.blk` is built by `encodeColumnarBlock`, so a group
  column carries **its own zones and its own string dictionary**, over the group's member
  records within a base block.

So the hierarchy to prune against is: **segment zone → block zone → group-block zone**,
coarse to fine, each cheaper than the level below it.

### 7.2 The pair predicate

`zoneMayMatch(z, op, vals)` answers "could a record in this zone satisfy `attr OP
literal`". The join needs its two-sided generalization: given left zone `[a1,a2]` and
right zone `[b1,b2]`, could **any** pair satisfy `L.x OP R.y`?

| op | pair test |
|---|---|
| `==` | `a1 <= b2 && b1 <= a2` (intervals overlap) |
| `<`  | `a1 < b2` |
| `<=` | `a1 <= b2` |
| `>`  | `a2 > b1` |
| `>=` | `a2 >= b1` |
| `!=` | prunable only when both zones are the same single point |

A false result skips the **entire block pair** without reading a value. The table reduces
exactly to `zoneMayMatch` when one side is a point, which is the right way to implement
it: one predicate, the existing one as its degenerate case.

**Soundness conditions, every one of them already recorded in the block:**

- both zones must be **non-escaped**. `blockZone.escaped` means some record's value is not
  in the column, so the range does not cover it. Pruning on an inexact zone drops records
  that match — "the worst shape of wrong answer here: silent, and faster than the correct
  one."
- both fields must be numeric in **their own** segment's schema (`resolveFields` already
  demands this, and notes that segments sealed under different schemas coexist).
- the comparison must be a **required** conjunct — top level of the `&&` spine, not under
  a `||` and not inside a guard. Reuse the `ExactProbes` discipline from
  `numPredsOnFields`.
- **`=?=` must not be pruned this way.** `undefined =?= undefined` is **true**, so a block
  pair in which both attributes are absent contains real matches that no numeric zone
  represents. Prune `=?=` only when both sides prove presence.

### 7.3 Skipping without enumerating the pair product

Testing every (left block, right block) pair is itself quadratic, which would defeat the
point. Three filters, cheapest first:

1. **Global envelope.** Compute the right side's `[min, max]` over all its blocks once. A
   left block disjoint from it is skipped with one comparison. This is the highest-value
   filter for the shape people actually run — "this week's jobs ⋈ all of history" — where
   most history blocks are outside the week.
2. **Sorted sweep.** Sort the right blocks by `Min` and precompute a suffix-max of `Max`.
   For left block `[a1,a2]`, binary-search the last right block with `b1 <= a2`, then walk
   back while the suffix-max of `Max` is `>= a1`. Enumeration is proportional to the
   overlap, not to the product.
3. **Per-pair test** (§7.2) on what survives.

This works as well as it does because HTCondor's join keys are **near-monotone with
insertion order** — `ClusterId`, `QDate`, `CompletionDate` — and an append-only archive
builds blocks in arrival order. Zone intervals come out nearly sorted and nearly disjoint,
so the sweep is close to linear and the pruning fraction is high.

### 7.4 Left skips and right skips are different operations

- **Right-block skipping** is the obvious one: for a given left block, visit only the
  overlapping right blocks.
- **Left-block skipping** pays only for `INNER` (and semi-) joins: a left block whose key
  range is disjoint from the right side's global envelope produces no output and need not
  be scanned at all. Pure metadata, zero data read.
- For a `LEFT JOIN` a left block can never be skipped — but it can be **fast-pathed**:
  the same disjointness proof says every row in it is null-extended, so emit the whole
  block without touching the right side. Same metadata, different action.

### 7.5 Exact filters beyond min/max

Two block-level facts are **exact**, not approximations, and are stronger than any range
test:

- **`escAbsent`.** If either side's block proves the join attribute absent, every pair's
  `==` is undefined, hence not true: skip the block. Works for strings as well as numbers,
  and needs no zone at all. (Not valid for `=?=`, per §7.2.)
- **Dictionary intersection for string keys.** A block's fold-sorted string dictionary is
  the exact set of distinct values present. Two blocks are skippable for a string equi-join
  **iff their dictionaries do not intersect** — a merge of two sorted lists, O(|Dl|+|Dr|).
  Better: the intersection is the set of values that *can* match, so it can be emitted as
  the `membershipExpr` form that already folds to an `in` probe — a semijoin filter built
  entirely from metadata the blocks already carry.

### 7.6 Group-column zones, and the condition that makes them sound

A group block's zone covers **only the group's member records**. Non-members are not
represented, so the group zone alone cannot prune a base block. But the block records
exactly what the non-members do:

- a record whose `members` bit is clear and which is **not** in `exceptions` provably has
  every member attribute undefined ("a clear bit otherwise PROVES every member undefined");
- `exceptions` lists records holding *some but not all* members — their values live in the
  base cold tail and are **not** covered by the group zone.

So the rule is:

> A base block may be skipped for a join on a group attribute `a` **iff** the group-block
> zone rules it out **and** `exceptions` is empty (or no exception carries `a`).

With `exceptions` empty, every record either has `a` in the group column — covered by the
zone — or provably has `a` undefined, so `==` is undefined and cannot match. Exact. A
group block with no members at all (`blk == nil`) and no exceptions proves the attribute
absent block-wide, which is `escAbsent` for group attributes.

This is what extends block skipping to attributes that are **not in the base schema at
all** — which, given how much of a job ad lives in secondary group schemas, is most of the
interesting join keys.

### 7.7 The cliff, and reporting it

For a **non-monotone** key — a hashed id, `Owner`, anything uncorrelated with insertion
order — every block's range spans nearly the whole domain, overlap is total, and zone
pruning collapses to nothing while still paying for the sweep. This is the same shape as
the columnar GROUP BY cliff, and the same remedy applies: measure, fall back, and *say so*.

`EXPLAIN` must report blocks considered, pairs pruned by each filter (envelope, sweep,
pair test, `escAbsent`, dictionary), and pairs actually visited — the join's version of
`vecSplit`. A join that silently declined every block looks exactly like a working one
that is mysteriously slow.

### 7.8 Where it does not apply

Zone maps and columnar blocks exist for **append-only, sealed** data. Mutable tables have
no zone maps and the active segment is never pruned. So `jobs ⋈ history` prunes history
blocks, not jobs blocks. That is the right asymmetry — the archive is the expensive side —
but the planner must know it and must not promise pruning it cannot deliver.

## 7a. Guard rails: the cartesian case is not hypothetical

A join can generate quadratic work from one line of SQL, so the brake is part of the
design, not an afterthought. Two things are easy to mistake for a brake and are not:

- **A per-side row cap does not bound the pair count.** Two sides at a 500k cap are 2.5e11
  pairs.
- **Hashing does not bound it either.** A join key with one distinct value puts every row
  in one bucket, and that bucket *is* the cross product. Measured on a 300x300 fixture, a
  join on a constant-valued key produced all 90,000 pairs with the hash path "working"
  exactly as designed.

So the budget is counted where pairs are actually examined -- the only place that sees both
sides -- and it covers the nested loop and the degenerate hash with one mechanism. The
refusal names which case it is: no equality term in the ON clause, or an equality whose key
does not separate the rows. A second cap bounds the output rows, because the result is
materialized to be sorted and rendered, so an unbounded one is an out-of-memory rather than
a slow query, and LIMIT does not help: the rows are built before they are limited.

CROSS JOIN is not a special case needing a fallback -- it is the nested loop with no
condition, and it meets the same budget. "Fall back to a cartesian product" is never the
right answer to a planning failure here; refusing is.

## 7b. Towards a cost-based planner

The planner today is rule-based: split the conjuncts, extract the equalities, hash if there
is anything to hash on. That is the right starting point, and these are the decisions that
will actually need costs, in rough order of what they are worth:

1. **Which side builds and which side probes.** Currently fixed (the right side builds).
   It should be the smaller or more selective side.
2. **Which side drives the block sweep** (§7.3), since the two sides prune each other
   asymmetrically.
3. **Whether to push a conjunct at all.** A pushdown that barely filters costs a constraint
   evaluation per row for nothing -- the same judgement `maxPushdownFrac` already makes for
   the match pushdown.
4. **Hash vs indexed nested loop vs block-nested loop**, which is only decidable with an
   estimate of how many right-side rows a left row will probe.

**What exists.** `planFrac`, `estimateCandidates` and `conjunctSelectivity` already give
per-side selectivity, with the independence assumption deliberately damped rather than
multiplied raw. The equi-depth value-index histogram gives distribution. Zone maps give
ranges.

**What is missing is NDV** -- the number of distinct values of a join key. The standard
join-cardinality estimate is `|L| * |R| / max(ndv(L.k), ndv(R.k))`, and nothing in the
store reports NDV today. The equi-depth histogram is the natural place to derive it, and it
is worth noting that the degenerate-hash refusal above is exactly an NDV failure caught at
run time instead of at plan time.

Two things specific to this store are worth building the planner around rather than
retrofitting:

- **The block sweep is itself a statistic.** The number of overlapping block pairs (§7.3)
  is computable from metadata before any data is read, and it estimates join selectivity
  directly -- no histogram, no independence assumption, no NDV. Most planners would love to
  have it. It should be a first-class input, not an afterthought to a textbook estimator.
- **Prefer measuring the first side to estimating it.** The client-side join already fetches
  the left side before deciding whether to key-batch the right, so that decision uses an
  actual count rather than an estimate. Keeping that shape server-side -- decide the second
  side's access path after the first is counted -- avoids the error that compounds worst in
  join planning. An estimate is for choosing which side to read first; after that there is a
  real number.

Estimation error compounds per join, which is the standard argument for keeping N-way joins
out until there is a real estimator, and the reason EXPLAIN must print estimated *and*
actual row counts once estimates exist at all.

## 7c. Sub-selects: later, except for one (see `SETOPS_DESIGN.md`)

Sub-selects do not belong in the first join, but one shape should not wait for the rest.

- **`IN (SELECT ...)` and `EXISTS` are semijoins**, and the machinery for them is already
  written: key batching builds a membership constraint on one table from the values another
  table actually has, which is precisely `WHERE a.k IN (SELECT b.k FROM b WHERE ...)`. Only
  the grammar is missing. MATCH wants semijoin semantics too (does this request have any
  candidate at all?), so this earns its place earlier than general subqueries.
- **Derived tables (`FROM (SELECT ...) x`) and CTEs** need one structural change: a join
  side becomes a row source rather than a table name. That is a small refactor of one struct
  and is not worth doing speculatively -- but it is worth knowing it is the only thing in
  the way, because the composite-ad row model already generalizes (a derived table is just
  another member of the row).
- **Scalar subqueries** are a different execution shape and have no demand behind them yet.

Until then a sub-select in an expression reaches the ClassAd parser and fails with a
confusing message about expression syntax, which should be refused explicitly at the SQL
layer instead.

## 7d. The question that is not a join

"All of one user's live jobs plus their completed ones" reads like a join and is a **set
union**. A job is in the queue or in history, never both, so pairing the two tables finds
nothing to pair: a `LEFT JOIN` of jobs to history returns every live job with an entirely
undefined right side, which looks like a broken feature rather than the wrong question.

`UNION` / `UNION ALL` now answer it; `INTERSECT` and `EXCEPT` do not. See
`SETOPS_DESIGN.md`, which also carries the sub-select design sketched in §7c.

By contrast, "the epochs of each completed job from the last day" is a genuine join:
`history` to `epoch` on `(ClusterId, ProcId)` -- a composite key, one-to-many, with the day
filter pushing into the history scan. It is the shape the design is for.

## 8. MATCH is a ranked join, and most of this applies to it

`MATCH` stays a distinct statement (§9.5), but it is structurally a join: a bilateral
predicate over two tables, ranked, followed by an assignment. Almost everything above
applies to it, and in the places where it differs it is *easier* — so the MATCH work
should land first and de-risk the join.

### 8.1 What MATCH does today

`streamMatchTables` walks the requests matching `reqWhere` and, per request — or per
autocluster signature when `USING` is given — calls
`MatchSortedRankedFiltered(reqAd, targetWhere, 0)`. That list is deliberately
**unlimited**, and the comment says why: "assignment consumes machines, so a later job may
need one ranked past every machine an earlier job claimed." Greedy assignment then takes
each request's best unclaimed resource, marks it claimed, and emits one row per request.

So the current shape is **N independent full scans-and-rankings of the resource table**
(one per autocluster), then a serial assignment.

### 8.2 What MATCH gets from §6.1 and §7 — with less work than the join

- **The vectorized executor, with no new interface** (but see the measurement below for
  what it now has to beat). `rewriteForSlot` maps
  `TARGET.attr` → a self-scoped reference, so `slotMatchExpr`'s residual is entirely
  `NoScope` — exactly what `blockVecSource.LoadColumn` already accepts. MATCH needs no
  `TargetScope` `ColumnSource` and none of §4's alias normalization. It is the simpler
  case of §6.1 and should be built first.
- **Block-zone pruning.** The residual's usual shape (`Memory >= 4096 && Cpus >= 8 &&
  Disk >= 100000`) is precisely the conjunction-of-scalar-comparisons that
  `numPredsOnFields` consumes and `blockMayMatch` prunes with — the degenerate,
  one-side-literal case of §7.2. It needs **no index**, so it covers attributes nobody
  configured a value index for, and it gives a second chance to prune after
  `maxPushdownFrac` rejects the index plan outright.
- **The exact filters of §7.5.** `escAbsent` proves a block carries no candidate at all.
  The per-block string dictionary prunes `State =!= "Claimed"` — the `NOPREEMPT` filter —
  which `zoneMayMatch` cannot touch at all, since it reports `!=` as unprunable.

Two honest qualifications:

- **Slot numerics are not monotone with insertion order.** `Memory`/`Cpus`/`Disk` across a
  heterogeneous pool give heavily overlapping block ranges, so §7.7's cliff bites hardest
  here. The dictionary and absence tests, which are exact and order-independent, are the
  ones that carry; the numeric zones are a bonus.
- **The resource table is mutable**, so it has no segment-level zone map (`ZoneAttrs()` is
  empty for a mutable collection). It does get block zones, because `columnarizeSealed` is
  not gated on append-only — block zones are automatic wherever a columnar block exists.

**Measured, 2026-10-31: that hypothesis is wrong.** The reasoning was that since a record
read out of a columnarized segment is rebuilt from its columns, every index candidate on a
columnarized resource table pays a column→row reassembly only to be rejected. A benchmark
over 20,000 slots (`collections/match_columnar_bench_test.go` in the classad tree) says
otherwise:

| job shape (matches / 20,000) | plain | schema scan only | columnarized |
|---|---|---|---|
| selective (312) | 87 ms | 81 ms | **12 ms** |
| one string attribute (13,333) | 74 ms | 77 ms | **17 ms** |
| accepts everything (20,000) | 73 ms | 116 ms | **44 ms** |

Columnarization makes matchmaking **3–11x faster**, and by the largest margin on the job
that rejects almost everything — the exact opposite of what a per-candidate reassembly tax
would produce. The signature is of a form that is *cheaper to reject from*: the win is
biggest where most candidates are thrown away, and smallest where everything matches and
must be materialized into a ClassAd regardless.

Two methodological notes, because the first attempt got this wrong in both directions.
Columnarizing requires a derived schema installed first, so a two-arm comparison varies two
things at once and credits the schema scan's own effect to columnarization; the middle arm
above shows the schema scan alone is worth nothing here (and costs something when nothing is
rejected). And the arms are run in both orders: absolute numbers move between orderings —
`accept-all/plain` reads 73 ms one way and 223 ms the other — while the ratios hold.

**What this changes.** There is no reassembly tax to remove, so §8.2's vectorized pre-filter
cannot be justified by one. It has to earn its place against the 12 ms the columnar reject
path already achieves on a selective match, which is a much harder target. Re-measure
against that baseline before building it, and treat the ordering spread above as the noise
floor any claimed improvement has to clear.

### 8.3 Matching many requests in one pass

MATCH is already multi-request — but as N independent scans with an autocluster cache. The
join framing inverts the loop: **one pass over the resource blocks, evaluating every
autocluster's baked predicate against the same loaded columns.** The left side of the
block-nested loop is the autocluster set, not individual requests.

That matters because column loading, not predicate evaluation, is the dominant cost. In a
real pool — thousands of autoclusters, one slot table — amortizing the load across every
autocluster turns N scans into one. Memory cost is one heap per autocluster live during the
scan (N × k entries), which is why k wants to stay small (§8.4).

### 8.4 Rank-bounded block skipping

Keep a per-autocluster min-heap of its best k candidates; the heap's minimum is a live
cutoff. **Skip a block when its maximum achievable Rank is at or below the cutoff.**

Bounding Rank over a block is tractable because `rewriteForSlot` has already baked `MY.*`
to constants, leaving a function of slot columns alone. Over a block each column is an
interval — its zone — so for an expression monotone in each variable the maximum over the
box is obtained at a corner: the interval's max for an increasing variable, its min for a
decreasing one. A conservative walk returns increasing / decreasing / **unknown** per
variable, and unknown means no bound, hence no skip. Real Rank expressions
(`TARGET.Memory`, `KFlops`, `-LoadAvg`, `TARGET.Memory - MY.RequestMemory`) are all inside
the tractable class.

Visit blocks in descending order of their Rank upper bound, so the cutoff rises fast — the
Fagin-style threshold shape. `opTopK`'s cutoff scan, with its `stStreamStats` trailer, is
the existing precedent to generalize rather than a new idea.

Note the direction: pruning is strongest at k=1 and weakens as a heap deepens. That is the
right shape, because most requests take a top-few choice.

**This is the one optimization here that can change the answer, and it needs a fallback.**
Zone pruning cannot alter a result — a pruned block provably held no match. Truncating to
top-k can, because it interacts with assignment consumption: if a request's heap is
exhausted during assignment, the unpruned run would have found a deeper candidate and
placed the job. So the heap is **an optimization with a fallback, not a truncation**:
exhaustion must be detected and must trigger a deeper re-probe for that autocluster. With
that detection the result is identical to today's; without it, MATCH silently returns a
worse assignment — the same failure mode §5.3 rule 3 guards against, in a new place.

In practice the re-probe set is small, and even with it this strictly improves on today,
which ranks every matching slot for every autocluster even when the first candidate turns
out to be free.

### 8.5 Does inverting the loop change the answer?

In principle, no — and that is checkable rather than assumed. `candidatesFor(reqAd)` reads
only the request ad and the resource table; it never reads `claimed`, which is applied
*after* the ranked list is produced. Candidate generation is therefore already independent
of assignment state, so inverting its loop changes how (a) is computed, not what it
computes.

In practice four things can change it, and three of them are latent in the code today.
They are preconditions for §8.3, not consequences of it.

**1. Ties are broken by scan order, and that order is already parallel.**
`MatchSortedRanked` sorts with `sort.SliceStable` and a comparator that returns `false`
for equal ranks, so equal-ranked candidates keep the order `collectMatches` produced —
which comes from a parallel shard fan-out. Two equally-ranked slots is the *common* case in
a homogeneous pool, and which one gets claimed cascades: it is then unavailable to a later
request that would have taken it. So MATCH's assignment may already vary run to run.

**Fix this first, before inverting anything:** give candidates a deterministic total order,
`(hasRank, rank desc, resource key asc)`. Inversion then provably cannot move a tie, and
today's nondeterminism goes away as a side effect.

**2. Request iteration order must remain request order.** The inverted pass groups by
autocluster for *computation*; assignment must still walk requests in their original order,
never autocluster order. Today's code keeps that separation (cache keyed by signature, loop
over requests) and the inverted version must preserve it. `limit` also breaks the request
loop early, so the inverted pass needs a request pre-pass to learn which autoclusters are
live — otherwise it computes candidates for requests past the limit.

**3. Snapshot consistency changes, for the better.** Each `candidatesFor` is a separate
query today, so a resource table mutating mid-MATCH means different requests see different
states. One inverted pass sees one snapshot. That is a better answer but a *different* one:
the before/after equivalence test must run against a quiescent table, and the improvement
should be deliberate and documented rather than discovered.

**4. The exhaustion re-probe must continue the same deterministic order** (§8.4, §8.8) — a
cursor with a well-defined continuation point, the `(shard, snapshot, seq, key)` shape
`opQueryRawProjSeq` already uses.

### 8.6 Assignment order is the policy, and today it is unspecified

The request loop is `reqDB.Query(orTrue(reqWhere))` — the store's scan order. Not user
priority, not job priority, not submission order. So "more important users go first" is not
something loop inversion would break; it is something MATCH does not do.

If MATCH is to stand in for negotiation, the assignment order **is** the policy and must be
explicit: a request-side ordering clause (`MATCH … ORDER BY <expr>`, e.g. by user priority)
that assignment follows strictly, with the current scan order remaining the documented
default. Until then the result is arbitrary, and any implementation change — this one
included — will look like a regression to anyone who came to depend on that arbitrary
order.

### 8.7 The same invariant, already enforced next door

`match_reorder.go` reorders a job's Requirements operands so the evaluator short-circuits
sooner, and it rests on exactly the result in §5.3. The rule it enforces is worth repeating
here because the join planner's splitter depends on the same reasoning:

```
error || true  -> error   (lsErr on the left returns error; the right is never consulted)
true  || error -> true    (lsTrue on the left returns true;  the right is never consulted)
```

So `||` is **not** safe to reorder: it can turn a non-match into a match when an operand
coerces to error — a string or list operand, a bad regexp, a division by zero. `&&` is safe,
because `true` there requires both operands true regardless of order. Undefined is harmless
for both.

That package already acts on this: only top-level `&&` conjuncts are reordered, nested
chains and `||` chains keep their written order, the job ad itself is never rewritten (so a
slot's `TARGET.Requirements` always sees the original expression), and two named tests fail
the moment the restriction is loosened. The join splitter inherits the same rule: split and
reorder `&&` spines in predicates only, never `||`, and never in a value position.

### 8.8 Only the assignment serializes

Correct, and that is the decomposition to build to:

- **(a) Candidate generation and ranking** — a join. Parallel, vectorizable,
  block-skippable, one pass over the resource table.
- **(b) Greedy assignment** — serial, consumes resources, one row per request.

For (b) to stay correct without (a) materializing everything, (a) must be **resumable**
rather than merely top-k: a per-autocluster cursor that yields the next-best candidate on
demand. Depth is then paid only where assignment actually consumed it, and the cutoff of
§8.4 becomes dynamic per pull instead of a fixed k. That is the clean form of the
exhaustion fallback.

Worth recording: (b) being *greedy* is a policy choice, not a constraint. Once (a) produces
ranked candidate streams, a different assignment rule is a drop-in — but it would change
results, so it is out of scope here.

### 8.9 The slot side is the half that does not vectorize

MATCH is bilateral: `Symmetry` also requires the slot's own `Requirements` to accept the
job. That direction does **not** vectorize, because a slot's `Requirements` is an
expression-valued attribute — held per record as data, not compiled as a query — and
expression-valued fields in columnar storage are the known hazard that made fast COUNT
paths read them as UNDEFINED. So the vectorized pass is a **pre-filter** and the slot-side
check stays per survivor, which is exactly today's structure: "the index only narrows which
slots are visited — correctness is unchanged from a full scan."

It can largely be recovered. Measured on OSPool, **99.76% of slot ads carry the same
`Requirements`** (`Start && WithinResourceLimits`). Group slots by `Requirements` text — an
autocluster on the *resource* side — and bake one predicate per (request-autocluster ×
slot-requirements-class) pair. At that ratio it is effectively one extra predicate, and it
makes the bilateral check vectorizable for everything but a long tail. Symmetric
autoclustering is `USING (...)` generalized to the other side.

### 8.10 A documentation bug found on the way

`docs/repl.md` states that `LIMIT k` means "best k resources per request (default 1)" and
that MATCH "returns the top-k bilaterally-matching resources by the request's Rank." The
code says otherwise: `limit` bounds the number of **requests** assigned, candidates are
never limited, and exactly one row is emitted per request. The `dbrpc` comment is right and
the repl.md example is right; the prose between them is wrong. Fix it before anyone builds
on the stated semantics.

## 9. Risks

1. **Owner-scope injection is a security hole until fixed.** The webapi/MCP scoping wraps
   *one* constraint (`ownerScope func(string) (string, error)`). A join whose outer `WHERE`
   is scoped but whose second table is not lets a caller read other owners' rows. The
   scoping layer must inject per-table constraints, and **`JOIN` must not be exposed to
   MCP/webapi until it does.**
2. **The private-attribute gate must run per side.** `db.ConstraintRefs` deliberately
   collects scoped references too, but an alias-qualified reference arrives as a
   `SelectExpr` and must be attributed to the right table. The comment on that function
   records that a type-switch walk silently found nothing until it was caught — same class
   of hole, new node shape.
3. **Intern ids are per collection.** Zone maps are keyed by interned id and ids are
   process-local *and* collection-local (`s.c.intern.LookupID(name)`). A join spans two
   collections, so `ClusterId` has two different ids. Resolve the attribute name to each
   side's own id separately and never carry an id across the table boundary. Unstable zone
   ids have already produced silent wrong counts here once.
4. **Composite ads and goroutines.** `NewMatchClassAd` calls `SetTarget`, nesting an ad
   under a parent changes its scope chain, and ClassAd reads are not pure. The hot
   two-table path uses the §4 rewrite plus `MatchClassAd.EvaluateExprLeft` (no per-row
   allocation), with the composite-ad form of §2 as the semantic reference — and a
   differential test asserting the two agree, because that is exactly the fast-path /
   slow-path pair that has diverged here before.
5. **`JOIN` is not `MATCH`, and the docs must say so.** `MATCH` is bilateral and
   ad-specified: each side's `Requirements` must accept the other. `JOIN` is unilateral and
   user-specified. Both stay — §8 makes them share machinery, not meaning.
6. **Rank-bounded pruning can change results; zone pruning cannot.** Keep the two
   categories visibly apart (§8.4), and keep the exhaustion fallback non-optional.

## 10. Phasing

**Phase 1 — client-side, in `repl`, no protocol change.** Alias normalization first (§4),
then parser, resolution, conjunct splitting (§5), hash join and key-batched lookup,
strategy (c) via batched constraints, `EXPLAIN`. Works against every existing server, and
because `hcdb_sql` and the Python DB-API both run statements through `repl.Parse`/`Exec`,
the REPL, the C API, the Python driver, Grafana and MCP all get `JOIN` from one
implementation. `ORDER BY` and `DISTINCT` over a join result run client-side here;
aggregates and `GROUP BY` over a join are refused, because the aggregate argument grammar
takes a bare attribute name that cannot say which table it means — they arrive with the
server-side work in Phase 3. **No vectorization and no block skipping in this phase** — a
client cannot see blocks. Also the repl.md fix of §8.10, which is independent of everything.

**Phase 2 — the columnar machinery, proved out on MATCH.** Block-zone pruning and the
vectorized residual (§8.2) land here and land **first**: same machinery as the join's hot
path, but with no `TargetScope`, no composite ad and no outer-join asymmetry to get wrong.
Measure `RecordsReassembled` before building.

**Phase 3 — `opJoin` (opcode 66; 65 is the current high-water mark).** Two tables, `ON` as
a ClassAd expression, executed inside the store: the join `ColumnSource` of §6.1, the block
sweep and zone skipping of §7, wire-form row assembly, streaming only joined output rows.
Same new-opcode-plus-`ErrXUnsupported`-fallback pattern `opTopK` and
`opArchiveAggregateStats` use, so an older server degrades to Phase 1. Then aggregates over
a join pushed server-side — `SELECT j.Owner, COUNT(*) FROM jobs j JOIN history h ON …` is
what dashboards actually ask, and only the grouped result should cross the wire.

**Phase 4 — MATCH-specific: the ranked join.** The items that have no join analogue, in
dependency order:

1. the deterministic candidate total order and explicit assignment ordering (§8.5, §8.6),
   which are preconditions for everything below;
2. the (a)/(b) split with resumable per-autocluster candidate cursors (§8.8);
3. the single-pass multi-autocluster scan (§8.3);
4. rank-bounded block skipping with the exhaustion fallback (§8.4);
5. resource-side autoclustering by `Requirements` text (§8.9).

(1) must land before any of the rest: without a deterministic tie-break, "same answer as
before" is not even a testable claim. (2) is then the precondition for (4) being sound, so
it precedes it even though (3) is the larger immediate win.

## 11. Testing

- **Differential oracle against SQLite** on the pure-equality subset where SQL and ClassAd
  semantics agree — the approach the existing C++ differential fuzzers use.
- **Property test:** hash join, nested-loop join and the composite-ad reference return
  identical bags for the same `ON`.
- **Zone-skip equivalence:** for every join the planner accepts, the result with block
  skipping enabled equals the result with it forced off. This is the one that catches an
  unsound prune, and it must run over data with escaped values, group exceptions, mixed
  segment schemas and an unsealed active segment.
- **MATCH equivalence:** the full `MATCH` result — every row, including which resource each
  request got — is identical with rank pruning on and off, and with resource-side
  autoclustering on and off.
- **Exhaustion fallback exercised:** a pool constructed so greedy assignment must go deeper
  than k for some request, asserting the same assignment as an unpruned run. Without this
  test the fallback is unfalsifiable, and a truncation would pass every other test here.
- **`Native()` assertion** on every accepted join predicate (§4), and on every rewritten
  slot residual.
- **Both storage kinds**, mutable and archive, since in-memory/persistent divergence has
  bitten twice.
- **Pinned negative tests:** unqualified reference refused; the §5.2 table, both rows;
  `=?=` not zone-pruned; candidate budget refuses rather than hangs.
- Keep the suite inside the 5–10 minute integration budget; the zone-skip matrix runs on
  demand, like the collections suite.
