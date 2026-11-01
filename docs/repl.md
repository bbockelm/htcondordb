# REPL reference

`htcondordb-cli` is the SQL-like shell over the database.

```
htcondordb-cli                       # interactive, auto-locate the daemon
htcondordb-cli -addr '<host:port>'   # against a specific daemon
htcondordb-cli -pool cm.example.com -name db.example.com   # locate via a collector
htcondordb-cli -e "SELECT COUNT(*) FROM ads"   # one-shot
```

## Finding the daemon

Three ways name the daemon, in descending order of directness.

- `-addr <host:port>` is the address itself, and consults nothing else.
- `-pool <host:port>` / `-name <name>` ask a collector, the way `condor_status
  -pool ... -name ...` locates any other daemon: the daemon advertises an
  `HTCondorDB` ad carrying its address, and the CLI reads the address out of it.
  `-pool` defaults to `COLLECTOR_HOST`, so `-name` alone works within a pool.
  `-name` takes either the advertised name or the host: unless `HTCONDORDB_NAME`
  is set, a daemon calls itself `htcondordb@<host>`, and `-name <host>` finds it
  anyway. With several databases in the pool and no `-name`, the CLI lists them
  and stops rather than picking one.
- With neither, the daemon is whichever one this host is configured for:
  `HTCONDORDB_ADDRESS_FILE` (by default `$(LOG)/.htcondordb_address`), else
  `HTCONDORDB_HOST`. Both can be set in the environment or the HTCondor
  configuration; the environment wins, and wins as a pair.

`-addr` together with `-pool`/`-name` is an error: they can name different
daemons, and running the query against the wrong database is worse than a
refusal.

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
- **`IN` is not supported**, in either the list or the sub-select form: ClassAd spells
  membership as a function, so write `member(x, {a, b})` (and `!member(...)` for `NOT IN`).
  Both forms are refused with that pointer.
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
- **`JOIN`** reads two tables; see [JOIN](#join--two-tables-one-row). Subqueries are
  still rejected with a clear error.
- **`time_bucket(attr, 'width')`** groups rows by a floored unix-epoch timestamp
  attribute for time-series output. See the [time-series design](design/time-series.md).

## JOIN — two tables, one row

```sql
SELECT <items>
FROM <table> [[AS] alias]
  [INNER | LEFT [OUTER] | CROSS] JOIN <table> [[AS] alias] ON <classad-expr>
[WHERE ...] [ORDER BY ...] [LIMIT n]
```

**The joined row is itself a ClassAd**, with each side under its alias:

```
[ j = [ ClusterId = 42; Owner = "alice"; ... ];
  h = [ ClusterId = 42; ExitCode = 0;    ... ] ]
```

so `j.ClusterId == h.ClusterId` is not new syntax — it is the ClassAd select operator
over a nested ad. **`ON` is a ClassAd expression, exactly as `WHERE` is**, which means a
join condition can be anything the language can say, not just an equality:

```sql
SELECT j.Owner, h.ExitCode
  FROM jobs j JOIN hist h ON j.ClusterId == h.ClusterId
  WHERE j.Owner == "alice";

-- non-equi conditions are first class:
SELECT j.Owner, m.Name FROM jobs j JOIN machines m
  ON j.RequestMemory <= m.Memory && j.RequestCpus <= m.Cpus;
```

Three things follow from the row being a ClassAd:

- **An unmatched `LEFT JOIN` row has no member for the right side**, so every `h.X`
  reads as `undefined` — ClassAd's own spelling of SQL's `NULL`, with no new rule.
- **`==` propagates undefined**, so rows missing the join key match nothing, as in SQL.
  `=?=` is the operator that does equate two undefined values (SQL's
  `IS NOT DISTINCT FROM`).
- **Every attribute must be alias-qualified.** `WHERE ExitCode == 0` is refused, naming
  the aliases, rather than being read as one side and silently returning nothing. An
  alias defaults to the table's own name, so `jobs.ClusterId` works unaliased.

`SELECT *` returns every attribute of both sides, alias-qualified so like-named
attributes stay distinct; `SELECT h.*` returns one side's.

**Where the work happens.** A join runs in the client, over query operations that predate
it, so it works against any server. What makes it affordable is that the `&&` spine of
`ON` and `WHERE` is split: conjuncts naming one table become server-side constraints on
that table's scan, each side fetches only the attributes the statement reads from it, and
an equality on a bare attribute lets the right-hand scan be narrowed to the key values the
left side actually has. `EXPLAIN` reports all of it.

Outer joins push asymmetrically, which is worth knowing because it is the usual source of
wrong join results: for a `LEFT JOIN`, `ON` conjuncts push only to the right (inner) side,
and `WHERE` conjuncts only to the left (preserved) side. An `INNER JOIN` pushes both ways.

**Aggregates and `GROUP BY` work over a join**, with the aggregate's argument qualified like
any other attribute:

```sql
-- every run attempt of each owner's jobs completed in the last day
SELECT h.Owner, COUNT(*) AS attempts,
       COUNT(*) FILTER (WHERE e.ExitCode == 0) AS succeeded
  FROM history h JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
  WHERE h.CompletionDate > CurrentTime - 86400
  GROUP BY h.Owner
  HAVING COUNT(*) > 1
  ORDER BY attempts DESC;
```

The grouping runs **client-side** over the joined rows — the server's aggregate is
single-table and has no notion of a composite row — and `EXPLAIN` says so. Over a single
table an aggregate still takes a bare attribute name, since there is no second table for a
qualified one to refer to.

**Not supported yet**, each refused with the reason: `RIGHT`/`FULL OUTER` (swap the tables
and use `LEFT JOIN`), `NATURAL JOIN` and `USING` (write the condition with `ON`),
comma-separated `FROM` lists, more than two tables, sub-selects, and window functions over
a join.

**Guard rails.** A join refuses rather than runs away when it would examine too many
candidate pairs — either because the `ON` clause has no equality between the tables, so
every row is compared with every row, or because the join key has too few distinct values
to separate the rows. Note that a per-table row count does not bound the pair count, and
neither does hashing: a key with one distinct value puts every row in one bucket, and that
bucket is the cross product. There is a second cap on output rows, because the rows are
built before `LIMIT` applies.

**"Live jobs plus completed jobs" is not a join.** A job is in the queue *or* in history,
never both, so joining `jobs` to `history` finds nothing to pair — a `LEFT JOIN` returns
every live job with an entirely undefined right side. That question is a set union; see
[UNION](#union--stacking-two-results) below. The genuine join over history is to a second
archive, e.g. one row per run attempt:

```sql
-- every run attempt of each job completed in the last day
SELECT h.ClusterId, e.RunInstanceId, e.ExitCode
  FROM history h
  JOIN epoch e ON h.ClusterId == e.ClusterId && h.ProcId == e.ProcId
  WHERE h.CompletionDate > CurrentTime - 86400;
```

`JOIN` is not `MATCH`. A join is unilateral and says its own condition; matchmaking is
bilateral and takes the condition from the ads themselves. Both are below.

## UNION — stacking two results

```sql
SELECT <items> FROM <table> [WHERE ...]
UNION [ALL]
SELECT <items> FROM <table> [WHERE ...]
[ORDER BY ...] [LIMIT n]
```

A union **stacks** rows; a join **pairs** them. "One user's live jobs plus their completed
ones" is a union, because a job is in the queue or in history, never both:

```sql
SELECT 'live' AS src, ClusterId, Owner FROM jobs    WHERE Owner == "alice"
UNION ALL
SELECT 'done' AS src, ClusterId, Owner FROM history WHERE Owner == "alice"
ORDER BY ClusterId;
```

The literal column is the SQL idiom for saying which branch a row came from.

- **Rows are ClassAds**, so `SELECT * FROM jobs UNION ALL SELECT * FROM history` works even
  though the two tables carry different attributes: the column set is the union of what the
  branches carry, and a row from a branch lacking an attribute shows `undefined` rather than
  shifting the columns.
- **Named columns match by position**, and the first branch's headers name the output. A
  branch selecting a different number of columns is refused, as is mixing `SELECT *` with
  named columns.
- **`UNION` de-duplicates, `UNION ALL` does not.** De-duplication compares rendered rows, so
  two `undefined` values count as alike — matching SQL's `DISTINCT`, which treats `NULL`s as
  alike, rather than `==`, under which `undefined == undefined` is undefined. The two forms
  may not be mixed in one chain.
- **`ORDER BY` and `LIMIT` apply to the whole union**, not to the branch they follow, and
  may not appear on an earlier branch.
- Each branch is an ordinary `SELECT` with its own pushdowns, and may itself be a `JOIN`.

`INTERSECT` and `EXCEPT` are not supported.

## MATCH — matchmaking between two tables

`MATCH` is HTCondor bilateral matchmaking, not a join: a request (e.g. a job) and
a resource (e.g. a machine) match when each one's `Requirements` is satisfied with
the other as `TARGET`, ranked by the request's `Rank`.

```sql
MATCH <requestTable> TO <resourceTable>
  [USING (attr, ...)]             -- significant attrs: autocluster identical requests
  [WHERE <request-filter>]        -- which requests to matchmake
  [WHERE TARGET <resource-filter>] -- filter resources (resource side)
  [LIMIT <k>];                    -- assign only the first k requests (default: all)

-- single request:
MATCH KEY '<key>' IN <requestTable> TO <resourceTable> [LIMIT k];
```

It emits **one row per request**: the best-ranked resource that no earlier request
has already been given, or an empty resource when the request could not be placed.
Assignment consumes resources, so two requests never receive the same one, and each
request's candidate list is computed in full rather than truncated — a later request
may need one ranked past every resource an earlier request claimed. `LIMIT k` bounds
how many *requests* are assigned (the first `k` in scan order), not how many
resources a request is offered. Output columns are `Request`, `Resource`, `Rank`.

The order requests are assigned in is the store's scan order, which is **not** a
priority order. If assignment order matters to you, treat the current behaviour as
unspecified rather than as a guarantee.

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
.dialect <d>          classad (default) | ansi -- how WHERE expressions are read
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

### `.dialect` — which language a WHERE clause is in

A `WHERE` clause is a **ClassAd** expression by default, which is why the full ClassAd
language works in one (`=?=`, `undefined`, `member()`, `regexp()`, `?:`). ClassAd compares
strings **case-insensitively**, so this matches `alice`, `ALICE` and `Alice` alike:

```sql
SELECT Owner FROM jobs WHERE Owner == "alice"
```

The Arrow Flight SQL endpoint cannot read it that way. A client writing `WHERE Owner =
'alice'` means SQL's `=`, which does **not** fold case, so the same text selects different
rows over the two doors — each correct for its reader, with nothing in the results to say
which you got. `.dialect ansi` reads a predicate the way that endpoint does:

```
> .dialect ansi
dialect: ansi
  string comparisons are now case-sensitive, and a predicate this dialect cannot lower
  faithfully is refused. `.dialect classad` to go back.

> SELECT Owner FROM jobs WHERE Owner = 'alice'   -- matches "alice" only
> .dialect classad
```

Use it to check what a Flight SQL or ADBC client will get, without needing one to hand.

**It is deliberately less capable.** The ANSI dialect lowers only what it can prove
equivalent and refuses the rest rather than approximating it, because a half-understood
query returns rows that look right. `WHERE Owner > 'R'` works in the ClassAd dialect and is
refused here: ClassAd's fold-order and SQL's byte-order results are neither a subset nor a
superset of one another, and the stored dictionaries are fold-sorted, so no index could
serve a byte-ordered range anyway. Also refused: inequality on a string column, a negated
string equality, a comparison whose literal cannot be what the column holds, a predicate on
an attribute with no scalar form (a policy expression), and any `JOIN` or set operation —
a bare column there could belong to either table.

What it adds over the default: `IN` and `BETWEEN`, neither of which the ClassAd dialect
parses. `IS [NOT] NULL` is **not** an addition — it is already translated in both dialects and
means the same thing in each.

Column types come from the same derivation the Flight SQL endpoint uses, read fresh per
statement, so an attribute that appeared since the session started is usable immediately.
One-shot mode takes `-dialect`:

```sh
htcondordb-cli -dialect ansi -e "SELECT Owner FROM jobs WHERE Owner = 'alice'"
```

## Diagnostics and index tuning

The shell can introspect the store's storage engine — the hot set, indexes, and
query planner — and manage them:

```
.stats                storage stats (ads, segments, arena/live/dead bytes)
.indexes              configured categorical/value indexes + demand suggestions
.hot                  hot attributes (front-loaded in each ad's hot header)
.suggest              index add/drop suggestions from observed query demand
.slow [table] [n]     costliest query shapes, ranked by total time
.slow reset [table]   clear the aggregation window (needs DAEMON)
.explain <expr>       how the planner would run a ClassAd constraint

.addindex value|categorical <attr>[, ...]   create an index (needs WRITE)
.dropindex <attr>[, ...]                     drop an index
.reindex                                     rebuild indexes (an archive: every segment)
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
segments:     6 of 8 answer from the index, 1 ruled out by it, 1 unindexed (scanned in full)
candidates:   ~45600 record(s) from the indexed segments
reason:       index probe on Cpus: 6 of 8 segments answer from postings, ~45600 candidate records; 1 segments ruled out by their index; 1 segments have no index on it and are scanned in full
probes:
  Cpus                 >=   INDEX  (value)  est ~38.0% (~45600 of 120000)
  Owner                ==   scan   (not indexed)
```

For an indexed plan the `segments:` line says how many segments actually carry
the index. An attribute can be listed as indexed while most segments have no
postings for it: an index added to an existing archive is carried back only as
far as the archive's backfill horizon (1 GiB by default), and every older
segment keeps the index it was sealed with. Those segments are counted as
`unindexed` and read in full. `reason:` is the planner's one-line account of the
plan, including why it chose a scan.

For an index-usable probe, `.explain` also shows the planner's **selectivity
estimate** — how many ads the index expects to visit (from the segment indexes'
per-value stats), so you can see which conjunct actually prunes. (It appears once
the relevant segments have built indexes; a brand-new/tiny store may not have
stats yet.)

### `.slow` — which queries cost

`.slow` answers "what are my slowest queries?". The daemon buckets every query
it serves by **shape** — the opcode, the table, a fingerprint of the constraint,
whether a LIMIT was sent, and, for an aggregate, how it groups — and ranks the
buckets by **total** time, because the query worth fixing is usually a cheap one
run constantly rather than one slow outlier.

The fingerprint is the constraint with its literals replaced by `?` and its
attribute names lowercased, taken from the parsed and constant-folded
expression. So `CompletionDate >= 1761300000` and `CompletionDate >= 1729764000`
are one shape, and so are `owner == "a"`, `Owner == "b"` and
`Owner == "a" || Owner == "b" || Owner == "c"`.

Each shape also carries the **slowest actual queries** that matched it. That is
the point of keeping both: in this engine the literal decides the work — a
one-hour window prunes to a couple of sealed segments on their zone maps while a
one-year window reads every one of them, and the two are the same shape. The
bucket says which pattern costs; the exemplar gives you the constraint to paste
into `.explain`.

```
htcondordb> .slow
query shapes (all tables): 18204 queries aggregated over 4h12m, 23 shape(s) tracked
  ordered by TOTAL time -- a cheap query run constantly outranks one slow outlier

#1  QueryRawWire jobs   count 8421   total 12m4s   mean 86ms   max 1.4s   rows 2101340
    where: (completiondate >= ? && owner == ?)
    projection: none -- every attribute of every matched ad crossed the wire
    segments: 101052 scanned, 0 pruned (0% skipped) over 8421 measured instance(s)
    slowest: 1.4s  rows 18420  user alice@example  segments 12/12 scanned  at 09:40:11
      CompletionDate >= 1761300000 && Owner == "alice"
```

Two of those lines are what make the report diagnostic rather than descriptive:
`projection:` says whether the client pulled whole ads instead of projecting
server-side, and `segments:` says how much of the scan was skipped — which
separates "slow because the window is big" from "slow because nothing pruned".

An aggregate (a `GROUP BY`, or any query that reduces rather than returns rows)
is reported with its grouping, because here the grouping decides the work rather
than only the output — a single raw numeric group column is answered from the
columnar blocks, while grouping by an expression, or by a column the accelerator
does not cover, rebuilds every matched record:

```
#2  ArchiveAggregate history   count 288   total 6m2s   mean 1.2s   max 4.1s   groups 11520
    where: (enteredhistorytime >= ?)
    group by: owner, enteredhistorytime/3600   aggregates: count(*), sum(remotewallclocktime)
    segments: 228096 scanned, 0 pruned (0% skipped) over 288 measured instance(s)
    slowest: 4.1s  groups 40  user alice@example  segments 792/792 scanned  tier scan  at 09:41:02
      EnteredHistoryTime >= 1761300000
```

A group column that carries a bucket width prints it after a slash
(`enteredhistorytime/3600` is hourly), and two widths over one attribute are two
shapes: hourly and daily differ by 24x in how many groups come back and often by
which strategy answers. The count beside the duration is **groups**, not rows —
twelve groups can be the product of reading every record in the table — and
`tier` on an exemplar names the strategy that answered it (`index` and
`columnar` read no records; `scan` rebuilt them). There is no `projection:` line,
because an aggregate streams no ads.

Shapes carry no literals and no identities, so any reader may see them.
Exemplars quote the real constraint and name the user who ran it, so they
require DAEMON, as does `.slow reset`.

Aggregation is on by default and writes nothing to the log; set
`HTCONDORDB_QUERY_STATS = false` to turn it off. It is independent of
`HTCONDORDB_LOG_QUERIES`, which logs every query individually.

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

### Tables owned by a writer in the daemon

A table that schedd sync, replication or a managed history importer maintains is
read-only to every session (see [Authorization](authorization.md#tables-owned-by-an-in-process-writer)).
A refused write prints the reason, and — on a DAEMON-authorized session — which
writer owns the table. `.truncate`, `.rotate` and `.retention` on such a table are
sent to the owner over `DBSyncControl` automatically: `.truncate history` wipes
the archive and has schedd sync re-read the history file; a truncate the owner
cannot rebuild from (a job_queue.log table, a replica, an import target) is
refused with what to run instead.

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
