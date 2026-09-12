# REPL reference

`htcondordb-cli` is the SQL-like shell over the database.

```
htcondordb-cli                       # interactive, auto-locate the daemon
htcondordb-cli -addr '<host:port>'   # against a specific daemon
htcondordb-cli -e "SELECT COUNT(*) FROM ads"   # one-shot
```

The database holds one or more **tables**, each an independent ClassAd
collection (no joins) with its own indexes, hot set, and persisted config. The
default table is `ads`; create more with DDL. Each row's primary key lives in a
key attribute (default `Key`): `INSERT` stamps it into the ad so `SELECT` can
show it and `UPDATE`/`DELETE` can recover the key of every matched row.

```sql
CREATE TABLE machines;
CREATE VALUE INDEX ON machines (Cpus);          -- or CATEGORICAL for string eq
DROP INDEX ON machines (Cpus);   DROP TABLE machines;

SELECT * FROM machines WHERE Cpus >= 8 ORDER BY Cpus DESC LIMIT 10;
SELECT DISTINCT Owner FROM jobs ORDER BY Owner;
SELECT COUNT(*), AVG(Cpus), MAX(Memory) FROM machines WHERE State == "Unclaimed";
SELECT Owner, COUNT(*), SUM(RequestCpus) FROM jobs GROUP BY Owner ORDER BY COUNT(*) DESC;
INSERT INTO jobs (Key, Owner, RequestCpus) VALUES ('1.0', 'alice', 4);
UPDATE jobs SET JobStatus = 2 WHERE Owner == "alice";
DELETE FROM jobs WHERE JobStatus == 4;
```

- **Tables**: `CREATE TABLE t` / `DROP TABLE t`; `FROM`/`INTO`/`UPDATE`/`DELETE`
  route to the named table. Each table is isolated on disk under
  `<db>/tables/<name>` and persists across restarts. In the shell, `.tables`
  lists them and `.use <t>` sets the table the diagnostic/management commands act
  on.
- **Indexes as DDL**: `CREATE [VALUE|CATEGORICAL] INDEX ON t (a, b)` /
  `DROP INDEX ON t (a)`. `CREATE INDEX` builds over existing rows immediately and
  persists.
- **`WHERE` is a ClassAd expression**, captured verbatim and evaluated by the
  store's engine — the full ClassAd language is available (`==`, `=?=`, `=!=`,
  `undefined`, `member()`, `regexp()`, `?:`, …), not a SQL dialect. String
  literals use double quotes (`Owner == "alice"`). The right-hand side of an
  `UPDATE … SET` is likewise a ClassAd expression.
- **Aggregates**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, with `GROUP BY` over one or
  more columns. Aggregation runs **server-side** (hash-map grouping): only the
  grouped result crosses the wire, not every matched ad. SUM/AVG/MIN/MAX use the
  ClassAd library's own coercion rules (`classad.Sum`/`Avg`/`Min`/`Max`) — integer
  sums stay exact, an int+real mix promotes to real, booleans coerce to 0/1,
  undefined is skipped, an error propagates, and `MIN`/`MAX` are numeric (a string
  argument yields `error`).
- **`DISTINCT`** over explicit columns is `GROUP BY` those columns (server-side
  de-duplication); `DISTINCT *` de-duplicates whole ads.
- **`ORDER BY`** one or more columns/aggregates, each `ASC` (default) or `DESC`;
  numeric values sort numerically, applied before `LIMIT`.
- `JOIN` and subqueries are rejected with a clear error — cross-table work is
  matchmaking (`MATCH`), not a join.
- **`time_bucket(attr, 'width')`** groups rows by a floored unix-epoch timestamp
  attribute for time-series output. See the [time-series design](design/time-series.md).

## MATCH — matchmaking between two tables

`MATCH` is HTCondor bilateral matchmaking, not a join: a request (e.g. a job) and
a resource (e.g. a machine) match when each one's `Requirements` is satisfied with
the other as `TARGET`, ranked by the request's `Rank`.

```sql
MATCH <requestTable> TO <resourceTable>
  [USING (attr, ...)]             -- significant attrs: autocluster identical requests
  [WHERE <request-filter>]        -- which requests to matchmake
  [WHERE TARGET <resource-filter>] -- filter resources (resource side)
  [LIMIT <k>];                    -- best k resources per request (default 1)

-- single request:
MATCH KEY '<key>' IN <requestTable> TO <resourceTable> [LIMIT k];
```

For each request passing the request-side `WHERE`, it returns the top-`k`
bilaterally-matching resources by the request's `Rank`. Output columns are
`Request`, `Resource`, `Rank`.

Two things make it cheap on real (repetitive) workloads:

- **Requirements pushdown.** The request's `Requirements` prunes candidate
  resources through any covering index (the same index-aware match the negotiator
  uses), so a match visits index candidates, not every resource. The `WHERE
  TARGET` clause (resource side, using `TARGET` — ClassAd's name for "the other
  ad") further filters the ranked matches.
- **Autoclustering (`USING`).** Real pools have thousands of *identical* jobs.
  `USING (attrs)` lists the attributes significant to matchmaking (the request's
  `Requirements`/`Rank` and the request attributes resources examine); requests
  whose significant attributes are textually equal share **one** candidate
  computation — the match runs once per distinct signature and is reused for every
  identical request (HTCondor autocluster semantics, via the store's
  `ProjectionChecksum`). Omit `USING` to match each request individually.

```sql
-- For each of alice's jobs, its best X86_64 machine:
MATCH jobs TO machines WHERE Owner == "alice" WHERE TARGET Arch == "X86_64" LIMIT 1;

-- Request | Resource   | Rank
-- 1.0     | slot1@ep7  | 16
```

It is a dry run: it reports matches, it does not claim/consume resources.

## Formatting and control commands (interactive)

```
.help                 show help
.format <mode>        table (default) | json | classad | classad-new
.output <file>        redirect query output to a file; .output stdout to restore
.quit                 exit
```

The interactive shell has line editing and command history (arrow keys, Ctrl-A/E,
Ctrl-R), persisted to `~/.htcondordb_history`.

`.format json` emits one JSON object per ad (JSONL); `.format classad` /
`classad-new` emit each ad in old / new ClassAd format. In non-table formats a
`SELECT` serializes whole matched ads (projection is a table-mode feature); an
aggregate result serializes its group rows. One-shot mode takes `-format`:

```sh
htcondordb-cli -format json    -e "SELECT * FROM machines WHERE Cpus >= 8"
htcondordb-cli -format classad -e 'SELECT * FROM jobs WHERE Owner == "alice"'
```

## Diagnostics and index tuning

The shell can introspect the store's storage engine — the hot set, indexes, and
query planner — and manage them:

```
.stats                storage stats (ads, segments, arena/live/dead bytes)
.indexes              configured categorical/value indexes + demand suggestions
.hot                  hot attributes (front-loaded in each ad's hot header)
.suggest              index add/drop suggestions from observed query demand
.explain <expr>       how the planner would run a ClassAd constraint

.addindex value|categorical <attr>[, ...]   create an index (needs WRITE)
.dropindex <attr>[, ...]                     drop an index
.reindex                                     rebuild indexes
.addhot <attr>[, ...]                        pin hot attributes
.refreshhot [<sampleMax> <topN>]             recompute the hot set from sampling
```

`.explain` shows the chosen access path (`indexed` / `parallel-scan` /
`serial-scan`), whether evaluation is wire-native (no per-ad ClassAd built), and
per-probe which conjuncts can prune via an index:

```
htcondordb> .addindex value Cpus
value index on Cpus (changed)
htcondordb> .explain Cpus >= 8 && Owner == "alice"
plan:         indexed
wire-native:  true
index-usable: 1 of 2 probe(s)
parallelism:  4 worker(s) over 8 shard(s)
ads:          120000
probes:
  Cpus                 >=   INDEX  (value)  est ~38.0% (~45600 of 120000)
  Owner                ==   scan   (not indexed)
```

For an index-usable probe, `.explain` also shows the planner's **selectivity
estimate** — how many ads the index expects to visit (from the segment indexes'
per-value stats), so you can see which conjunct actually prunes. (It appears once
the relevant segments have built indexes; a brand-new/tiny store may not have
stats yet.)

Index suggestions come from the store's own demand tracker (it records which
attributes queries filter on) and a sample of live ads, so `.suggest` recommends
exactly the indexes your workload would benefit from. The management commands
(and thus `.addindex`/`.dropindex`/`.reindex`/`.addhot`) require WRITE, so a
read-only connection can observe but not retune.

`.addindex` builds the new index over the existing ads immediately (it reindexes),
so it prunes and reports selectivity right away — not only for future writes. The
index configuration and hot set are **persisted** in the database directory
(`indexcfg.json`), so runtime `.addindex`/`.dropindex`/`.addhot` changes survive a
daemon restart (and the indexes are rebuilt over the loaded ads on open).

## Loading ads from a collector or schedd

`INSERT` is impractical for real 50-attribute machine/job ads, so the CLI has a
`load` subcommand that ingests the native `-long` ClassAd stream (ads separated
by blank lines) straight from a pipe. Each ad is keyed by `-key` (default `Name`),
and that value is stamped into the row's `Key` attribute so `SELECT`/`UPDATE`/
`DELETE` can address it.

```sh
# Machine ads from the collector into the "machines" table (keyed by Name):
condor_status -long | htcondordb-cli load -table machines

# Job ads from the schedd into "jobs" (keyed by GlobalJobId):
condor_q -global -long | htcondordb-cli load -table jobs -key GlobalJobId

# A mixed stream, auto-routed to a table per MyType (Machine->machines,
# Job->jobs, Scheduler->schedulers, ...), auto-creating tables:
condor_status -any -long | htcondordb-cli load -auto
```

`-table <name>` sends every ad to one table (created if absent); `-auto` routes
each ad to a table named for its `MyType` (lowercased and pluralized). With
neither, ads go to the default `ads` table. The summary reports the per-table
counts. Then query:

```sh
htcondordb-cli -e "SELECT Name, Cpus, Memory FROM machines WHERE Cpus >= 8"
htcondordb-cli -e 'SELECT COUNT(*), AVG(Memory) FROM machines WHERE Arch == "X86_64"'
```

Ads without the chosen key attribute are skipped (reported in the summary). The
load commits per table in batches; use `-key` to match the ad type (`Name` for
machine/daemon ads, `GlobalJobId` or another unique attribute for jobs).

For mirroring a live schedd automatically (rather than a one-shot load), see
[Schedd sync mode](schedd-sync.md).
