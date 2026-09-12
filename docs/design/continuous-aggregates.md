# Phase 2 design sketch: continuous aggregates (bucketed views → archive)

**Status:** draft for review. Grounded in `db/view.go`, `db/archive.go`, `db/catalog.go`.
**Depends on:** Phase 1's `GroupCol{Attr, BucketWidth}` shape (shipped in dbrpc v0.13.0).

## 1. Goal

A materialized view is a *current-state gauge* — `renderGroup` overwrites one row per
group in an in-memory `backing` DB (`view.go:320-350`). Phase 2 turns a view **grouped
by a `time_bucket`** into a *time series*: because the bucket key advances with
wall-clock, each interval is a new, permanent group. As buckets age out they're
**sealed** — appended once to an append-only **archive** and evicted from memory — so
history is unbounded and cheap to read, while memory stays bounded to recent buckets.

This is TimescaleDB's continuous aggregate, expressed in primitives we already have:
the view's delta-maintenance loop (COUNT/SUM/AVG add/subtract) + `ArchiveTable.Append`.

## 2. What the code already gives us

- `ViewGroupCol{Attr, Alias}` (`view.go:39`); groupKey is the NUL-join of the group
  columns' rendered values (`contributionOf`, `view.go:213-236`). A bucketed group
  column's value is just the floored timestamp — it drops into the existing keying with
  no special path.
- `renderGroup` (`view.go:320`) already builds the `label_*`/`metric_*` ad we'd append;
  factor its ad-construction into a helper shared by `backing.Put` and `archive.Append`.
- `ArchiveTable` (`db/archive.go`): `Append(ad)`, newest-first `Query`/`QueryLimit`
  (zone-map pruned by a numeric attr in `ZoneAttrs`), `Rotate(now)` for age retention.
  Lives in the same `Catalog` as views (`cat.archives`, `cat.views`), and `CreateView`
  already holds `cat.mu` and references `cat.archives` — wiring a per-view archive there
  is clean. **Requires a persistent catalog** (`cat.dir != ""`).
- MIN/MAX stay rejected (not delta-maintainable — `view.go:24-26`); COUNT/SUM/AVG only.

## 3. The mechanics

- **Bucketed group column.** `ViewGroupCol` gains `BucketWidth int64` (0 = raw, as
  today) — the same shape as dbrpc's `GroupCol`. `contributionOf` floors the attribute
  when `BucketWidth > 0` (epoch-aligned; a non-numeric timestamp drops the row). A view
  with any bucketed group column is a **continuous aggregate**.
- **Live window in `backing`.** Recent buckets stay in the view's in-memory backing and
  are updated in place — so **late-arriving base rows land in the correct bucket** as
  long as it isn't sealed yet. This is exactly today's behavior, just with more groups.
- **Seal & evict.** A bucket is *sealed* once wall-clock passes `bucket_end + grace`.
  On seal: `Append` the bucket's rendered `(time, label_*, metric_*)` ad to the archive,
  then evict its group from `groups`/`contrib`/`backing`, and advance a **watermark**
  (the newest sealed bucket start). Base rows for a sealed bucket that arrive later are
  **dropped and counted** (surfaced in view stats), not silently merged.
- **Retention.** The archive's own `Rotate(now, MaxAgeAttr=time, MaxAge=…)` bounds the
  series history; it needs a caller (see the tick below).

## 4. The seal trigger (the one genuinely hard part)

View maintenance is **purely event-driven** off the base `Watch` stream — there is *no*
timer (`run()` blocks in `for ev := range seq`, `view.go:377`). Two signals:

- **Bucket-advance (event-driven):** when an upsert produces a bucket newer than the
  running max, seal every group whose bucket window has closed. Zero new machinery, but
  a *quiet* table never seals its last buckets.
- **Wall-clock tick (backstop):** a per-view `time.Ticker` (sharing the view's existing
  `context.WithCancel` from `CreateView`, `view.go:511`) that takes `v.mu` and runs the
  same seal routine, so buckets close on time even with no new base events.

Recommend **both**: bucket-advance for promptness, the tick (e.g. every `grace/2`) as
the backstop that also drives `Rotate`.

## 5. Durability / reload (correctness-critical)

Views are documented as "in-memory, rebuilt from base on reload" (`recoverViews`,
`view.go:581`). A gauge rebuilds trivially; **a sealed series does not** — a rebuild from
the base table only re-derives still-live rows, not sealed history, and must never
re-append a bucket the archive already has. So Phase 2 persists, alongside `view.json`:

- the **watermark** (newest sealed bucket start), and
- the durable **watch cursor** (`view.go:174`, already tracked but not yet saved).

On reload: the archive *is* the durable sealed history; replay the base from the saved
cursor to rebuild only the **live (unsealed)** buckets; never seal/append anything at or
below the watermark. (`viewpersist.go` gains these two fields.)

## 6. Read path

Each archived sample already carries `time` + `label_*` + `metric_*`, so reading the
history is a **plain archive query**, newest-first, zone-pruned by `time` — no
re-aggregation. Recent (unsealed) buckets live in the view backing. v1: expose the
archive as the canonical series (Grafana reads it like a table; the bucketing is already
materialized) and document the ≤`grace` horizon lag on the live edge. Unioning the live
backing for a gap-free right edge is a clean follow-up.

## 7. SQL surface — DECISION NEEDED

Two options for how a user declares one:

- **(A) Implicit** — a `CREATE MATERIALIZED VIEW` whose GROUP BY contains a `time_bucket`
  *is* a continuous aggregate. Reuses the whole `viewSpecFromSelect` pipeline; the
  gauge-vs-series distinction is exactly "is there a time bucket." Reads naturally; the
  only subtlety is the implicit overwrite→append behavior switch.
  ```sql
  CREATE MATERIALIZED VIEW jobs_ts AS
    SELECT time_bucket(QDate,'5m') AS time, Owner AS label_owner, COUNT(*) AS metric_jobs
    FROM jobs GROUP BY time_bucket(QDate,'5m'), Owner
  ```
- **(B) Explicit** — `CREATE CONTINUOUS AGGREGATE <name> … WITH (grace='10m', retention='30d')`.
  Self-documenting, a natural home for grace/retention knobs; costs a new DDL statement.

**Recommendation: (A) implicit**, with grace/retention as optional `WITH (...)` options
on the `CREATE MATERIALIZED VIEW` (defaults otherwise). Maximum reuse, minimal new
grammar, and the knobs still have a home.

## 8. Implementation plan (mostly `db/`, then repl, then release)

1. `db`: `ViewGroupCol.BucketWidth`; `contributionOf` floors bucketed columns; `Validate`
   allows it. (Pure gauge behavior unchanged when width 0.)
2. `db`: per-view `*ArchiveTable` (created under the view dir, `ZoneAttrs=[time]`,
   `Retention`) when the spec has a bucket; factor `renderGroup`'s ad build into a helper.
3. `db`: seal-and-evict routine + bucket-advance hook + per-view tick + drop-counter.
4. `db`: persist watermark + cursor (`viewpersist.go`); reload rebuilds only live buckets.
5. `repl`: `viewSpecFromSelect` accepts `time_bucket` (+ optional `WITH` grace/retention);
   read path is a normal query over the view's archive.
6. Release: tag classad (db + deps), bump htcondordb; grafana E2E graphs a continuous
   aggregate accumulating over time.

## 9. Open decisions to confirm before building

1. **SQL surface:** implicit (A, recommended) vs explicit (B)?
2. **Seal trigger:** bucket-advance + wall-clock tick (recommended) vs event-only?
3. **Read edge:** archive-only with a documented `grace` lag for v1 (recommended) vs
   union live+archive now?
4. **Scope:** this is the largest phase (new durable state + a tick in view maintenance).
   OK to land it as its own classad minor release, or should it be split (bucketed-view
   grouping first, seal/evict/persistence second)?
