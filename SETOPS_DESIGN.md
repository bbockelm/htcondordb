# Design: set operations and sub-selects

**Status:** UNION / UNION ALL implemented; the rest is design.
**Scope:** `repl` (parser, planner, executor).

## 1. Why this is a separate document

`JOIN_DESIGN.md` has one spine: a joined row is a composite ClassAd, with each side under
its alias, so every clause stays an expression over one ad. Nothing here shares that spine.
A union produces rows of the *same* kind as its inputs — there is no pairing, no composite
ad, no `ON` clause, and nothing to push down across a boundary. A sub-select is a different
thing again: a row source where a table name was expected.

They belong together in one document because they answer the questions people reach for
*after* a join does not fit, and because they share one implementation idea: a branch is an
ordinary SELECT, so every pushdown a single-table SELECT already has, a branch has.

## 2. UNION: stacking, not pairing

### 2.1 The question it answers

"One user's live jobs plus their completed ones." This reads like a join and is not one. A
job is in the queue **or** in history, never both, so `jobs LEFT JOIN history` returns every
live job with an entirely undefined right side — which looks like a broken feature rather
than the wrong question. A join pairs rows side by side; a union stacks them.

Keeping that distinction sharp in the error messages matters more than the feature itself.
The parser previously reported `unexpected "ALL" after statement`, having swallowed `UNION`
as a table alias.

### 2.2 Rows are ClassAds, which makes this easier than in SQL

SQL requires union-compatible branches: same arity, compatible types. Here a row is a
ClassAd, so `SELECT * FROM jobs UNION ALL SELECT * FROM history` is just the concatenation
of two ad streams, and the existing star rendering already copes with ads that do not carry
the same attributes — the column set is the union of what the branches carry, and a row from
a branch lacking an attribute shows `undefined` rather than shifting the columns. No schema
reconciliation, no type coercion rules.

For named columns the SQL rule applies: branches match by position, the first branch's
headers name the output, and an arity mismatch is refused with both counts. Mixing `SELECT
*` with named columns is refused — there is no arity to match against.

Provenance ("which of these are still running?") is the SQL idiom, a literal column per
branch, and it works:

```sql
SELECT 'live' AS src, ClusterId FROM jobs    WHERE Owner == "alice"
UNION ALL
SELECT 'done' AS src, ClusterId FROM history WHERE Owner == "alice";
```

### 2.3 De-duplication compares rendered rows, not values

`UNION` de-duplicates; `UNION ALL` does not. The comparison is over **rendered rows**, and
that choice is load-bearing: SQL's `DISTINCT` treats two NULLs as alike even though
`NULL = NULL` is unknown, and ClassAd's `undefined == undefined` is likewise undefined.
Comparing with `==` would keep every undefined row and quietly fail to de-duplicate exactly
the rows a user most expects to collapse.

`UNION` and `UNION ALL` may not be mixed in one chain. SQL gives that a precise meaning
through left-associative, per-step de-duplication; rather than pick a reading, the mix is
refused and the two halves run separately.

### 2.4 ORDER BY and LIMIT bind to the operation

`... UNION ALL SELECT ... ORDER BY QDate LIMIT 5` orders and limits the combined rows, not
the last branch. Since each branch is parsed by the ordinary SELECT parser, the last branch
is the one that consumes them, and they are lifted to the operation afterwards; a branch
that is not the last may not carry them, because there is nothing sensible it could mean.

The corresponding execution rule: the operation's `LIMIT` is pushed into a branch **only**
when the output is the branches concatenated in order — no ordering and no de-duplication
afterwards. Otherwise a branch's first k rows are not the operation's first k.

### 2.5 Every clause that captures an expression must stop at a set operator

`WHERE`, `HAVING`, `QUALIFY` and a join's `ON` are captured verbatim and handed to the
ClassAd parser. Each one's stop predicate has to include `UNION` / `INTERSECT` / `EXCEPT`,
or the expression swallows `UNION ALL SELECT ...` and the failure surfaces as a ClassAd
syntax error pointing at the operator. The same applies to the alias slot after a table
name, which otherwise reads `UNION` as the table's alias.

This is the kind of thing that is obvious once seen and invisible beforehand, so it is
written down rather than left to the next person to rediscover.

### 2.6 INTERSECT and EXCEPT

Refused, not designed. Over rendered rows they would be a few lines, but over whole ads
(`SELECT *`) set difference needs a defined row identity, and there is no demand behind
them yet. When there is, the identity question is the thing to settle first.

## 3. Sub-selects

### 3.1 `IN (SELECT ...)` and `EXISTS` come first, because the machinery exists

These are semijoins, and the join already builds one internally: key batching folds the
values one table actually has into a membership constraint on the other table's scan. That
is precisely

```sql
WHERE a.k IN (SELECT b.k FROM b WHERE ...)
```

so the *execution* is already written. MATCH wants semijoin semantics too — "does this
request have any candidate at all?" is `EXISTS`, not a ranked match — which is a second
reason this shape earns its place ahead of general subqueries.

**But "only the grammar is missing" understated it, and the sequencing changed once that was
checked.** There is no `IN` operator at all, list form included: `WHERE Owner IN ('alice',
'bob')` reaches the ClassAd parser verbatim and fails, and `NOT IN` was worse than
unsupported — `NOT` translated to `!` on its own, so `Owner NOT IN ('alice')` became
`Owner ! IN (...)` and surfaced as a syntax error pointing at the `!`. Both are now refused
with the ClassAd spelling, `member(x, {a, b})`, which does work.

The obstacle is architectural rather than a missing case. ClassAd spells membership as a
*function*, so the left operand has to move inside a call — and by the time the expression
capture reaches `IN`, that operand has already been written through to the output verbatim.
`captureExpr` streams source left to right and cannot retract it. So `IN` needs one of:

- a capture that can retract or defer the preceding primary term (a lookbehind over the
  token stream to find the operand's start — an attribute, a qualified path, or a balanced
  parenthesized group);
- a source-to-source pre-pass over the token slice that rewrites `IN` forms before capture,
  which is simple but has to avoid rewriting an attribute that happens to be named `in`;
- or accepting `member()` as the only spelling and not supporting `IN` at all, which is
  defensible given that `WHERE` is documented as a ClassAd expression rather than SQL.

**So the order is: decide the `IN` spelling question first, build the list form, and only
then `IN (SELECT ...)`** — which at that point really is just the grammar plus the value cap
and plain-scan fallback below, because the membership execution exists. Choosing the third
option above makes `x IN (SELECT ...)` unspellable, so that decision is not only about
ergonomics.

Two things to settle when it is built:

- **The budget.** The inner SELECT's result becomes a constraint on the outer scan, and a
  membership constraint with a hundred thousand terms is slower than the scan it replaces.
  The join caps this at a few hundred values and falls back to a plain scan; `IN (SELECT)`
  needs the same cap and the same fallback, not a refusal.
- **`NOT IN` is not the complement.** With an undefined value anywhere in the inner result,
  SQL's `NOT IN` is famously empty. ClassAd's three-valued logic lands in the same place for
  the same reason, so the behaviour is consistent — but it must be documented rather than
  discovered, and `NOT EXISTS` offered as the spelling that does what people mean.

### 3.2 Derived tables and CTEs need one structural change

`FROM (SELECT ...) x` and `WITH x AS (...)` need a join side to become a **row source**
rather than a table name — today `joinSide` holds a `TableRef`. That is one struct and a
handful of call sites, and it is not worth doing speculatively; what is worth recording is
that it is the *only* thing in the way. The composite-ad row model already generalizes: a
derived table is just another member of the row, under its alias, exactly like a table.

Note that a derived table also removes the pushdown story, because its constraint cannot be
handed to a scan. The planner must either inline the sub-select's own WHERE into the outer
constraint or accept that the side is materialized.

### 3.3 Scalar sub-selects

`SELECT (SELECT COUNT(*) FROM h WHERE h.ClusterId == j.ClusterId) FROM j` is a different
execution shape — one evaluation per outer row, or a rewrite into a grouped join — and has
no demand behind it. Listed so the omission is deliberate.

Until any of this exists, a sub-select in an expression position reaches the ClassAd parser
and fails with a message about expression syntax, which says nothing useful. It should be
refused at the SQL layer with a message naming what is missing.

## 4. Where this runs

Entirely client-side, like the join, over query operations that predate it: each branch is
dispatched through the ordinary executor and only the combination happens here. No new
opcode, nothing version-gated, and it works against any server.

The server-side opportunity, when it is worth taking: a union of two constrained scans is
two streams the server could interleave without materializing either, and a de-duplicating
union could use the same hash the join builds. Neither is worth doing before there is a
query that needs it.

## 5. Testing

- The motivating shape: one owner's live rows plus their completed ones, across a mutable
  table and an archive.
- `SELECT *` across two tables of **different shapes**, asserting the column set is the
  union and that a missing attribute reads as undefined rather than shifting the row.
- De-duplication of two undefined values as equal — the rendered-row rule of §2.3, which a
  value comparison would silently fail.
- `ORDER BY` / `LIMIT` spanning the branches, and the parse-level assertion that they were
  lifted off the last branch.
- The pushed-limit path returning the same rows as the unlimited one.
- Every refusal in §2.2–§2.3 and §2.6, each asserting the message explains the rule.
- A branch that is itself a JOIN, since a branch is an ordinary SELECT.
