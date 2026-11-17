# Derived tables and CTEs

**Siblings:** [sub-selects.md](sub-selects.md) (the predicate forms, built; its §9 is the
constraint this takes as given), [arrow-flight.md](arrow-flight.md) (the columnar surface §3.3
borrows its shape from).

`FROM (SELECT ...) d` and `WITH d AS (SELECT ...) SELECT ... FROM d`. This is the construct
[sub-selects.md](sub-selects.md) §9 set aside, and that section's constraint on how rows are
held still stands; what follows is the rest of the design around it.

Out of scope, each for its own reason: a **recursive** `WITH`, which is a fixpoint and not a
relation anyone here has asked for; a **`LATERAL`** or otherwise correlated derived table, which
re-runs per outer row and is the correlated-scalar problem wearing a different hat; and `WITH`
on anything but a `SELECT`.

## 1. Where this starts from

Neither spelling parses today, and neither is refused the way the rest of this language refuses
things:

```
SELECT Owner FROM (SELECT Owner FROM jobs) t   ->  expected an identifier, got "("
WITH t AS (SELECT Owner FROM jobs) SELECT ...  ->  unsupported statement "WITH"
```

`repl.md` says each unsupported construct is "refused with the reason and, where there is one,
the spelling to use instead", and for these two it was not true: both fell out of the generic
parser, and the second read as though `WITH` were a typo. Both now name the construct and point
at the spelling to use instead — inlining the inner statement, or a materialized view when it
groups or is read more than once. Nothing else here depends on that, which is why it went first.

One piece of grammar luck worth recording: a **leading** `WITH` is unclaimed. The `WITH` already
in the language is the trailing option list on `CREATE MATERIALIZED VIEW ... WITH (grace = ...,
retention = ...)`, in a position a CTE never occupies. There is no collision to design around.

## 2. A relation, not a predicate

Every other construct in [sub-selects.md](sub-selects.md) pushes its inner statement *somewhere*: a predicate
lowers to a filter the store evaluates, or to a key set shipped as literals. A derived table is
the first that cannot, because the outer statement reads it as a **relation**.

The important consequence is not that rows must be held. It is that **the inner statement is no
longer a fragment of the outer one's constraint** — it has its own projection, its own grouping,
and its own row shape, and the outer statement refers to that shape by name.

## 3. Three modes, and holding rows is the rarest

§9 reads as though materializing were the default case. It is the fallback. Sorting the work by
what the outer statement actually needs gives three modes, and the first two hold nothing:

| | when | what is held |
|---|---|---|
| **Flatten** | the derived table is a filter + projection | nothing; one statement runs |
| **Stream** | the outer reads each row once, in order | nothing; rows pass through |
| **Materialize** | the outer reads the rows more than once | the whole relation |

Most derived tables people write — `FROM (SELECT a, b FROM t WHERE p) d WHERE q` — are the first
row of that table. Designing for the third and letting the first fall out of it would be
backwards, and would pay the representation's cost on the common case that does not need it.

### 3.1 Flatten

A derived table whose statement is a filter and a projection merges into the outer one: the
outer's references to `d.col` are rewritten to `d`'s expression for that column, and the two
`WHERE`s are conjoined.

Flattening is legal when the derived statement has **no** aggregate, `GROUP BY`, `DISTINCT`,
window function, or `LIMIT`. Each of those makes the derived table's row set depend on its own
boundary, so an outer predicate conjoined into it would filter a different set of rows than the
one the reader wrote. `LIMIT` is the sharpest case: `(SELECT ... LIMIT 10) WHERE q` means "filter
those ten", and flattening would silently make it "the first ten that satisfy q".

A bare `ORDER BY` with no `LIMIT` is the one clause that may simply be **dropped** when
flattening, because a derived table is a relation and the outer statement's own ordering is what
survives. Dropping it is worth doing rather than refusing: it is a common habit, and it is the
only clause here whose removal changes nothing a caller can observe.

**Flattening is what reaches the store.** This is the part that makes the mode worth having
rather than merely tidy. `db.JoinSpec`'s sides already carry a filter and a projection each, so

```sql
SELECT j.Owner FROM jobs j JOIN (SELECT ClusterId FROM epochs WHERE Restarts > 0) d
  ON j.ClusterId == d.ClusterId
```

flattens into exactly the `JoinSpec` the planner would have built for a two-table join with a
`WHERE` on one side. The derived table disappears before placement is considered, and every
store-side path that exists today serves it unchanged. The same is true of a derived table under
a subquery predicate.

### 3.2 Stream

A derived table that cannot flatten — it groups, or it is `DISTINCT`, or it has a `LIMIT` — can
still avoid holding anything when the outer statement reads each of its rows once and in order:
an outer filter, projection, aggregate, or ordering over `d` alone.

This is the mode that covers the shape people actually reach for a CTE to express:

```sql
WITH per_owner AS (SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner)
SELECT Owner FROM per_owner WHERE n > 100
```

The inner statement is already answered server-side by the grouped path, and its reply is one row
per group. The outer `WHERE` then runs over that reply as it arrives. Nothing is materialized,
and the cap that bounds a subquery's key set never enters, because no key set is shipped.

An outer predicate that reads only the derived table's **group columns** — not its aggregates —
should additionally be pushed into the derived statement's own `WHERE`, where the store evaluates
it. That is ordinary predicate pushdown and it is worth stating explicitly, because the grouped
path's cost is dominated by what it scans, not by what it returns.

### 3.3 Materialize

What is left is the case where the outer statement reads the rows more than once:

- the derived table is one side of a **join**, so the pairing reads it per pair; or
- a **CTE is referenced more than once**, and running it twice would be both slower and, for a
  non-deterministic read, potentially inconsistent.

Only here does §9's representation apply. Everything that section says about it holds and is not
restated: a column set derived from the batch, one `_ad` tail for what no column covers, and a
tail holding **wire bytes with a lazy decode** rather than the text `arrowtype` serves to an
Arrow client, because the reader is the evaluator in this process.

Two things to add to it.

**A derived table is always a complete sample.** `arrowtype.Measured.Complete` marks a
measurement taken over the whole table rather than a capped draw, and it is what makes a
non-nullable column sound. A derived table is one finite closed batch by construction, so
`Complete` is always true and the nullability question §9 waves at is simply answered — unlike a
live table, where `flightsvc/columntypes.go` has to move a column's type only once successive
derivations agree, because the table under it is still being written to.

**It needs a bound, and the existing ones do not cover it.** `maxJoinPairs` bounds pairs examined
and `MaxHashRows` bounds a hash side; neither bounds *the rows a derived table produces before
anything consumes them*. A `WITH` over an unfiltered history table would hold the table. The
bound should be stated in **rows and bytes** and refused rather than truncated, the way
`MaxGroups` refuses — a truncated relation is a wrong answer, and a wrong answer is worse here
than an error naming the limit.

## 4. Column names

A derived table's columns are named, and the outer statement refers to them by name. `SelectItem.
Alias` already exists and "defaults to the source text", which is the problem: `SELECT ClusterId
* 2 FROM ...` yields an alias of `ClusterId * 2`, and `d.ClusterId * 2` is not a reference
anything can resolve.

So: **an item that is not a plain column must carry an explicit `AS`**, and the alias must be a
legal ClassAd attribute name. Refusing the unnamed case is better than inventing `_col2`-style
names, because an invented name is one more thing the reader must look up to write the outer
statement, and the error that asks for `AS n` tells them exactly what to type. A plain column
keeps its own name, and `SELECT *` from a derived table carries whatever the inner statement
carried.

## 5. CTEs specifically

`WITH a AS (...), b AS (...) SELECT ...`. Beyond the modes above, three rules:

- **Scope is the statement.** A CTE name is visible to the main `SELECT` and to CTEs defined
  after it, not before. No name may shadow a real table — shadowing is the kind of thing that
  reads fine and means something else six months later, and refusing it costs nothing.
- **The reference count picks the mode**, per §3: referenced once and flattenable, flatten;
  referenced once otherwise, stream; referenced more than once, materialize once and read it
  twice.
- **A CTE that is never referenced is an error**, not a no-op. It is always a mistake — a typo in
  the name, or an edit that removed the only use.

A CTE is otherwise exactly a derived table with its definition moved; the two must share one
implementation, or they will drift into answering the same query differently.

## 6. Where it runs, and what EXPLAIN says

Flattening happens before placement, so a flattened derived table has nothing of its own to
report: the plan is the plan of the statement it merged into. The other two modes do, and the
reader needs to be told **which mode ran and why**, because the three have very different costs
and the difference between them is a property of the query the reader can usually fix.

The set-operation work is the precedent: a plan that described only the first branch was
tolerable while every branch ran alike, and stopped being so the moment branches could differ.
The same trap is here in a sharper form — a CTE referenced twice silently becomes the expensive
mode, and nothing in the query text says so. EXPLAIN should name the mode per derived table, and
for `materialize` say what made it necessary ("referenced 2 times", "joined").

## 7. What should be measured before this is built

In the order the answers would change the design:

1. **How often does flattening apply** in queries people actually write? If it is the
   overwhelming majority, the materialized representation can be built last and crudely. The
   honest version of this is to collect real CTE-shaped queries rather than guess.
2. **What does streaming a grouped CTE cost** against the same query written as a plain grouped
   statement? It should be approximately free. If it is not, the streaming layer is doing
   something the grouped path already did.
3. **Where is the materialization bound**, in rows and bytes, on a realistic ad width? §3.3 asks
   for a limit; the number should come from the same kind of measurement that set `maxJoinPairs`,
   not from a round figure.

Only the third needs the representation to exist, so the first two can be answered early.

## 8. Testing

The differential harness the subquery work uses applies directly, with one lesson carried over
from the set-operation change: **a differential test does not establish that the mode under test
ran.** Two arms that both flatten agree with each other perfectly. Each mode needs its own
assertion that it was the one taken — the op recorder for anything that reaches the store, and
EXPLAIN's mode line for the rest — beside the agreement tests that cover the shapes.

The specific pairings worth standing up early, because each is a place two paths must agree:

- a flattened derived table against the same query written flat by hand;
- a streamed grouped CTE against the same grouped statement written directly;
- a materialized CTE referenced twice against the same statement with the definition pasted in
  twice.

## 9. Staging

1. **Refuse both spellings properly**, naming the construct and the spelling to use instead.
   **Done** — and `repl.md`'s claim that every unsupported construct names a reason and a
   replacement is now true of these two rather than needing to be withdrawn.
2. Parse `FROM (SELECT ...) alias` into a `TableRef` that can carry a statement, and implement
   **flattening only**, refusing everything else with the reason. This alone covers the join and
   subquery-predicate cases, and reaches the store with no new opcode.
3. `WITH`, as sugar over step 2: the same node, the name bound in the statement's scope, single
   reference only.
4. **Streaming**, which makes grouped CTEs work and is where most of the remaining value is.
5. **Materialization** in §9's representation, with the bound from §7.3 — the join-side and
   multiply-referenced cases.

Steps 1–3 are each small and independently useful. Step 4 is where a reader stops having to
rewrite their query by hand. Step 5 is the only one that needs the columnar work, and it should
not be started until §7.1 says how often it is actually reached.
