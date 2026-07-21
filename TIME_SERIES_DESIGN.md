# Design sketch: windowed aggregate over history

**Status:** draft / sketch — for discussion, not yet a spec.
**Problem owner:** the Grafana time-series path.

## 1. The problem

A Grafana graph panel wants a *series*: one aggregate value per time bucket over a
range — e.g. "machines by state, one point every 5 minutes over the last 24 h".

Today the only way to get a past value is `FOR SYSTEM_TIME AS OF <t>`, a snapshot at
**one instant**. To draw a 24 h / 5 min graph the plugin would have to issue **288
separate `AS OF` queries**, each of which resolves a per-shard seq and does a *full
scan at that seq* (`collections.QueryAsOf`, `timeseq.go`). That is O(buckets × rows)
work and O(buckets) round-trips — the cost the plugin should never pay, and the
reason we deliberately did **not** hack an N-query loop into the datasource.

The goal: **one SQL statement, one round trip, sub-linear-in-buckets work**, returning
rows already shaped for Grafana.

## 2. What exists to build on

| Mechanism | What it gives | Limit |
|---|---|---|
| MVCC `AS OF` (`opQueryAsOf=43`) | aggregate at one instant | single instant; bounded by `MaxDistance` retention |
| Materialized views (`db/view.go`) | forward-maintained COUNT/SUM/AVG per group | current state only, no time axis |
| Archive (`db/archive.go`) | append-only event log, newest-first, zone-map pruned | no aggregation, no bucketing |

Key raw material: **every MVCC record already stores `[seq, supersededBySeq)`**
(`collections/segment.go`) — the exact interval of commit-seqs during which that
version was the current one. The time→seq checkpoint index (`shardTimeIndex`,
`timeseq.go`) already maps wall-clock → seq. Those two together are everything a
one-pass historical aggregate needs; only the traversal is missing.

## 3. Two kinds of time series (and why they need different mechanisms)

A subtlety that shapes the whole design: "time series" means two genuinely different
operations, and one construct cannot express both.

- **Event / counter series** — rows each carry a timestamp *attribute*, and you bucket
  by it: *"jobs completed per hour"* (bucket on `CompletionDate`). Each row lands in
  exactly one bucket.
- **Gauge / snapshot series** — a current-state aggregate sampled repeatedly over time:
  *"machines by state every 5 min"*. Here a single row-version must be counted in
  **every** bucket it was alive during — a machine `Claimed` from 10:00–14:00 belongs
  in all 48 five-minute buckets, not just the bucket of its start.

A bucketing *function over a column* handles the first cleanly and **cannot** express
the second (a column value maps a row to one bucket; a gauge needs a row to span a
bucket *range*). So the surface is split accordingly.

## 4. Proposed surface

### 4a. `time_bucket()` — the primary, Postgres-style grouping function

Bucket rows by a unix-epoch timestamp attribute they carry, over any regular table or
archive:

```sql
SELECT time_bucket(CompletionDate, '1h') AS time,
       ExitCode AS label_exit,
       COUNT(*)  AS metric_jobs
FROM history
WHERE CompletionDate >= $__timeFrom() AND CompletionDate <= $__timeTo()
GROUP BY time_bucket(CompletionDate, '1h'), ExitCode
```

`time_bucket(tsAttr, width)` floors a unix-epoch attribute to a bucket
(`floor(ts/width)*width`) — the TimescaleDB name/shape (see naming note below). Output
columns are already the `(time, label_*, metric_*)` shape the Grafana backend infers
into a timeseries frame (`frame.go`) and the Prometheus exporter's convention — **no new
result-plumbing on the consumer side.** COUNT/SUM/AVG/MIN/MAX all work here (it's an
ordinary grouped aggregate; MIN/MAX are fine because we're not incrementally maintaining
it — see §5's restriction, which applies only to the maintained paths).

This is the workhorse: it serves event/counter metrics directly, and — via §5.2 — it
*also* becomes how gauge series are read.

*Naming:* `time_bucket` is not ANSI SQL (no standard has a bucketing function). It's the
TimescaleDB (Postgres-ecosystem) name, chosen for legibility to the Grafana/Prometheus
audience and because its arbitrary-width signature fits the epoch-floor implementation
(unlike Postgres core `date_trunc`, which only truncates to fixed calendar units).

### 4b. `FOR SYSTEM_TIME FROM…TO…` — faithful SQL:2011 range selection

Separately, extend the existing temporal clause to the standard *range* form — return
every row-version whose system-time validity overlaps `[t1, t2)`:

```sql
SELECT Key, State FROM machines
FOR SYSTEM_TIME FROM '-1h' TO 'now'
```

This is the real SQL:2011 semantics (a bag of historical versions), independently useful
for auditing "what changed in this window", and it does **not** overload the keyword with
a non-standard sampling meaning. It composes with the parser's existing `AS OF` clause
(`AS OF` = the point form; `FROM…TO` / `BETWEEN…AND` = the range form). It is *not* the
series mechanism — §5.3 covers turning a range into a gauge series.

## 5. Producing gauge series, and the one-pass engine

### 5.1 The restriction on maintained aggregates

Any *incrementally maintained* aggregate (materialized views, the continuous aggregate of
§5.2, the diff-array engine of §5.3) is limited to **COUNT / SUM / AVG** — MIN/MAX can't
be maintained without a before-image to subtract on delete, the same reason `db/view.go`
rejects them. Ad-hoc `time_bucket` queries (§4a) have no such limit.

### 5.2 Continuous aggregates — the primary path for gauge series

The clean, unbounded way to get *"machines by state every 5 min"* is to **record** it, not
reconstruct it: a materialized-view variant that, instead of overwriting the current value
per group, **appends** a `(timestamp, label_*, metric_*)` sample to an **archive table** on
an interval. This reuses `db/view.go`'s existing incremental maintenance loop (it already
computes the per-group aggregate on every change) wired to `archive.go`'s `Append`.

The payoff: **reads are then just a §4a `time_bucket` query over the archive** — no MVCC,
no retention bound, zone-map pruned by time. Gauge series and event series collapse to the
*same* read surface. This is essentially TimescaleDB's continuous aggregate, expressed in
primitives htcondordb already has.

### 5.3 MVCC snapshot reconstruction — the optional advanced path

For *ad-hoc* look-back over a gauge when no continuous aggregate was pre-declared (bounded
by the MVCC `MaxDistance` retention window), we can reconstruct the series directly from
version history in a single scan. The naive plan is N snapshots; the insight that collapses
it to one scan:

> For a fixed key, a version with interval `[seq, sup)` is the visible version at
> bucket `t_i` iff `seq ≤ s_i < sup`, where `s_i` is the seq that `t_i` resolves to.
> Because buckets are time-ordered and seq is monotonic in time, the buckets a given
> version covers form a **contiguous range** `[i_lo, i_hi)`.

So the engine:

1. Resolve all `N` bucket seqs up front — `N` cheap binary searches in the checkpoint
   index (per shard), **not** `N` scans.
2. Scan the retained version set **once**. For each version with group value `g` and
   interval `[seq, sup)`:
   - `i_lo` = first bucket with `s_i ≥ seq`, `i_hi` = first bucket with `s_i ≥ sup`.
   - Apply a **range update** to a per-group difference array:
     `diff[g][i_lo] += v; diff[g][i_hi] -= v` (v = 1 for COUNT, attr value for SUM;
     AVG carries both a SUM and a COUNT diff array).
3. Prefix-sum each group's diff array → per-bucket totals.

Cost: **O(versions + buckets × groups)** with one scan, versus O(buckets × rows) with
N snapshot queries — the answer to "the cost of time-travel over multiple queries" for
the ad-hoc case. MIN/MAX are excluded here for the §5.1 reason.

Per-shard subtlety: the seq vector is per-shard, so `s_i` differs by shard. Accumulate
the diff arrays **per shard** (buckets→that shard's seqs are still monotonic) and sum
into shared per-group per-bucket totals. Correct and still one pass per shard.

The natural surface for this reconstruction is the §4b range clause plus a sampling
interval — e.g. `FOR SYSTEM_TIME FROM t1 TO t2` with a `time_bucket`-width sample. Since
it's the advanced/optional path, its exact spelling is deferred (see §7).

## 6. Phasing (ordered by value and by what's editable where)

**Phase 0 — `time_bucket()` over regular tables/archives, client-side (ships from `repl/`
alone, no dependency bump). ✅ DONE.** The parser gained a `time_bucket(attr, 'width')`
function-call form in select items and `GROUP BY` (`SelectItem` grew `Bucket`/`BucketWidth`;
`GroupBy` stays `[]string` holding a canonical `time_bucket(attr,secs)` key). The executor
(`execAggregateBucket`) floors the epoch attribute and buckets client-side, honoring an
`AS OF` instant; buckets are epoch-aligned; rows with an undefined bucket attribute drop
out. `frame.go` recognizes a `time` column as the axis. Honest cost: O(rows in the
WHERE-pruned range) to the client — fine for archives (zone-map pruned by a time
predicate). Covered by repl unit tests + an E2E time-series graph.

**Phase 1 — push `time_bucket` grouping into the server aggregate (needs
`collections`/`dbrpc`/`db` bump + release chain).** Extend `opAggregate` to accept a
bucketing group key (floor of an epoch attribute) so bucketing happens server-side and
only grouped rows cross the wire. Same SQL, same E2E test, O(groups×buckets) on the wire.

**Phase 2 — continuous aggregates (the primary gauge-series path; §5.2).** The
view-appends-to-archive recorder. Reuses `view.go` + `archive.go` almost entirely. Once
this lands, gauge series ("machines by state over time") are read with the *same*
`time_bucket` surface from Phase 0/1 — no new query syntax. This is the high-value,
unbounded-history piece; it just needs the maintained-append plumbing.

**Phase 3 — MVCC snapshot reconstruction (optional; §5.3).** The single-scan diff-array
engine for ad-hoc gauge look-back within the retention window, exposed via §4b's range
clause + a sample interval. Needs a new `collections` range-scan (`(seq, sup, ad)` within
a seq range) + an `opAggregateHistory` opcode. Do this only if ad-hoc historical gauge
queries (without a pre-declared continuous aggregate) prove worth the upstream work —
continuous aggregates cover the steady-state dashboards.

## 7. Consumer / Grafana integration

Minimal, because the output shape is already right:
- The datasource backend needs no new plumbing — `frame.go` already turns
  `(time, label_*, metric_*)` string rows into a timeseries frame; `format=="timeseries"`
  already sets the graph visualization.
- The query builder gets a "time series" mode that emits
  `time_bucket(<tsAttr>, $__interval)` in the SELECT + GROUP BY and a
  `WHERE <tsAttr> >= $__timeFrom() AND <tsAttr> <= $__timeTo()` predicate. The existing
  time macros already carry the panel range and interval.

## 8. Edge cases / open questions

- **`time_bucket` argument order/units:** propose `time_bucket(tsAttr, 'width')` (attr
  first, Timescale puts width first — pick one and document; attr-first reads better for
  ClassAd). Width is a duration string (`'5m'`, `'1h'`); values are unix **seconds**
  (HTCondor epoch), so the floor is integer arithmetic.
- **Bucket alignment:** align to the epoch (stable, cacheable across refreshes) rather
  than to the range start. Decide and document.
- **Bucket count cap:** guard against `'1s'` widths over huge ranges; cap the client-side
  bucket/row count (Phase 0) and error clearly, mirroring the view cardinality cap.
- **Null/undefined timestamps:** rows where the bucket attribute is undefined drop out of
  the series (document, don't silently zero-fill).
- **Range exceeds retention (Phase 3 only):** buckets older than `MaxDistance` have no
  versions; return the buckets we can + a `Result.Note` warning so Grafana shows a gap
  rather than erroring the panel. (Phases 0–2 read archives/tables, not MVCC, so no bound.)
- **Range-clause syntax (§4b/§5.3):** `FROM…TO…` faithful to SQL:2011 is settled; the
  sampling spelling for the optional reconstruction path is still open (a trailing
  `SAMPLE 'w'`? a `time_bucket` over the range?) — defer until Phase 3 is actually on deck.

## 9. Recommendation

Land **Phase 0** now — `time_bucket()` over tables/archives in `repl/` alone, plus one E2E
test proving an event/counter Grafana graph. Then **Phase 2** (continuous aggregates) is the
real prize: it turns gauge dashboards into cheap, unbounded `time_bucket` reads with no new
query syntax, reusing the view + archive engines. **Phase 1** (server-side bucket pushdown)
is a straightforward perf follow-up on the same SQL. Treat **Phase 3** (MVCC reconstruction)
as optional — build it only if ad-hoc historical gauge look-back without a pre-declared
continuous aggregate turns out to matter.
