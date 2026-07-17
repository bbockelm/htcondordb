# htcondordb

A full-fledged HTCondor daemon that serves the embedded ClassAd-log database
(transactional key/ad store with constraint queries, matchmaking, ordered
indexes, and change watches) over CEDAR, with HTCondor authorization, optional
high availability, and a SQL-like REPL client.

Built on:

- `github.com/PelicanPlatform/classad/db` — the embedded ClassAd log.
- `github.com/PelicanPlatform/classad/dbrpc` — the client/server RPC over CEDAR.
- `github.com/bbockelm/golang-htcondor` — config, security policy, DaemonCore
  integration (running under `condor_master`, ready/keepalive pings, privilege
  drop, SIGHUP reconfigure).
- `github.com/bbockelm/cedar` — authenticated, encrypted transport.
- `github.com/hashicorp/raft` — consensus for the consistent HA mode, tunneled
  over CEDAR.

## Components

| Path | What |
|------|------|
| `command/` | The CEDAR command integers, from HTCondor's retired `TRANSFERD_BASE` (74000) block — clear of every live command int. |
| `server/` | The DB service: wraps `db.DB` + `dbrpc.Server` behind one command, enforcing READ / WRITE / DAEMON authorization per connection. |
| `ha/leaderfollower/` | Asynchronous commit-stream replication (the "leader-follower" mode). |
| `ha/consistent/` | Quorum-replicated raft state machine, CEDAR raft transport, and leader-routing control protocol (the "consistent" mode). |
| `repl/` | The SQL-like language (parser + executor + formatter). |
| `cmd/htcondordb/` | The daemon. |
| `cmd/htcondordb-cli/` | The interactive shell. |

## Authorization

One authenticated CEDAR connection carries an entire `dbrpc` multiplex. The
access level is decided once, at connect time, from the authenticated identity
(re-evaluated per connection, so a reconfigure takes effect on the next
connection):

- **READ** — read-only, and every returned ad has its private (secret)
  attributes stripped (claim ids, capabilities, transfer keys).
- **WRITE** — full read/write; private attributes visible.
- **DAEMON** — WRITE plus the HA/replication surface (commit stream, raft
  transport, cluster control).

The command is registered at READ; the handler escalates to WRITE/DAEMON by
re-checking the `ALLOW_`/`DENY_` policy on the peer. Private stripping and
read-only enforcement are implemented in `dbrpc` via per-connection
`ServeOptions`.

### Commands (from the retired transferd block)

| Command | Int | Level | Purpose |
|---------|-----|-------|---------|
| `DBSession` | 74000 | READ | The multiplexed DB RPC session. |
| `DBReplicate` | 74001 | DAEMON | (reserved) dedicated commit stream. |
| `DBRaft` | 74002 | DAEMON | Raft transport tunneled over CEDAR. |
| `DBControl` | 74003 | WRITE | Consistent-mode control (leader discovery, peer registration, write-batch apply). |

## HA modes (`HTCONDORDB_HA_MODE`)

### `standalone` (default)
A single read/write daemon.

### `leader-follower`
The leader is an ordinary read/write daemon. Each follower opens a DAEMON-level
session and consumes the leader's commit stream (the store's `Watch` feed),
applying every upsert/delete to its local database and persisting the stream
cursor, so a restart resumes without missing a change. If a follower has fallen
out of the leader's retention, the leader answers with a reset and replays full
state. Followers serve **read-only** queries from local state (offloading reads);
writes go to the leader. No transactional/quorum safety — replication is
asynchronous and best-effort.

Knobs: `HTCONDORDB_ROLE` (`leader`|`follower`), `HTCONDORDB_LEADER` (the leader's
address, for a follower), `HTCONDORDB_CURSOR_FILE`.

### `consistent`
Strong consistency via raft. A write is a `Batch` of mutations proposed to the
raft log; once a quorum durably accepts it, every node's FSM applies the same
batch, so all replicas converge and no acknowledged write is lost while a quorum
survives. **The raft transport is tunneled over CEDAR** (not raw TCP), so
replication inherits HTCondor authentication and encryption. Any node serves
reads; a write sent to a non-leader is answered with a redirect to the leader
(`ControlClient` follows it transparently).

Membership bootstraps from the initial leader, which is either given the peer set
explicitly (`HTCONDORDB_RAFT_PEERS = id1@addr1 id2@addr2 …`) or told the cluster
size `N` (`HTCONDORDB_RAFT_SIZE`) and adopts the first `N` DAEMON-authenticated
peers that register.

Knobs: `HTCONDORDB_RAFT_BOOTSTRAP` (bool, the initial leader),
`HTCONDORDB_RAFT_PEERS`, `HTCONDORDB_RAFT_SIZE`, `HTCONDORDB_NODE_ID`.

> Raft log + stable state are stored durably in boltdb (`<db>/raft/raft.db`) and
> FSM state in FileSnapshotStore snapshots, so a node's membership and committed
> log survive restarts (a restarted node replays its log into the FSM rather than
> re-bootstrapping). The REPL routes writes through the consistent path with
> `-consistent` (via `consistent.ControlClient`, which follows leader redirects).

## Schedd sync mode (`HTCONDORDB_SYNC_SCHEDD`)

A read model of a local `condor_schedd`: htcondordb tails the schedd's on-disk
files and mirrors them into its own tables, so the live queue and the job history
become queryable through the REPL / dbrpc without polling `condor_q`/`condor_history`.
Two independent tailers run, and **both** are active when their source is present
— sync mode covers *both* the live queue and the history archive:

- **Live jobs** — tails `JOB_QUEUE_LOG` (the schedd's transaction log, via the
  `classadlog` reader) and mirrors the active queue into the mutable **`jobs`**
  table, applying each new / modified / deleted job as the log grows and following
  a log rotation.
- **History** — tails the `HISTORY` file and appends each completed job into the
  **`history`** *archive* table (append-only, zone-mapped on `CompletionDate` for
  fast time-range queries). Retention is enforced by the periodic archive rotation
  (`HTCONDORDB_ARCHIVE_ROTATE_INTERVAL`, default hourly).

At least one of the two sources must be configured; enable either or both. Paths
default to HTCondor's standard `JOB_QUEUE_LOG` / `HISTORY` and can be overridden
with `HTCONDORDB_JOB_QUEUE_LOG` / `HTCONDORDB_HISTORY`.

> **Never runs as root.** The schedd's `job_queue.log` / `history` are owned by the
> condor user; following them as root risks reading through an attacker-planted
> symlink to a privileged file. Sync refuses to start while still root — the daemon
> must have dropped to the condor user first (do not combine `HTCONDORDB_SYNC_SCHEDD`
> with `DROP_PRIVILEGES=false`).

## Configuration knobs

| Knob | Default | Meaning |
|------|---------|---------|
| `HTCONDORDB_DIR` | `$(SPOOL)/htcondordb` | On-disk database directory. |
| `HTCONDORDB_ADDRESS_FILE` | `$(LOG)/.htcondordb_address` | Where the command address is published. |
| `HTCONDORDB_HOST` | — | Client fallback when no address file is found. |
| `HTCONDORDB_HA_MODE` | `standalone` | `standalone` / `leader-follower` / `consistent`. |
| `HTCONDORDB_ROLE` | `leader` | Leader-follower role. |
| `HTCONDORDB_LEADER` | — | Leader address (follower). |
| `HTCONDORDB_CURSOR_FILE` | `$(SPOOL)/htcondordb/.replica_cursor` | Follower stream cursor. |
| `HTCONDORDB_RAFT_BOOTSTRAP` | `false` | This node initializes a fresh raft cluster. |
| `HTCONDORDB_RAFT_PEERS` | — | Explicit `id@addr` member list. |
| `HTCONDORDB_RAFT_SIZE` | `0` | Cluster size `N` for first-N-hosts bootstrap. |
| `HTCONDORDB_NODE_ID` | advertised address | This node's stable raft id. |
| `HTCONDORDB_SYNC_SCHEDD` | `false` | Mirror a local schedd: `job_queue.log`→`jobs` table + `history`→`history` archive. |
| `HTCONDORDB_JOB_QUEUE_LOG` | `$(JOB_QUEUE_LOG)` | Schedd job-queue log to tail (live `jobs`). |
| `HTCONDORDB_HISTORY` | `$(HISTORY)` | Schedd history file to tail (`history` archive). |
| `HTCONDORDB_ARCHIVE_ROTATE_INTERVAL` | `3600` | Archive-table retention sweep interval (seconds; `0` disables). |

Standard `SEC_*` and `ALLOW_`/`DENY_` knobs configure security and authorization.

## Security & authorization (getting WRITE)

Two independent things must both hold for a client to write (INSERT/UPDATE/DELETE):

1. **The client is authenticated**, so the daemon has an identity to authorize.
   HTCondor's default `SEC_*_AUTHENTICATION` is `OPTIONAL`, and OPTIONAL on both
   ends negotiates to *no* authentication — leaving the peer anonymous (`user=""`)
   and read-only. htcondordb therefore *prefers* authentication by default (it
   runs whenever a mutually-supported method exists, e.g. `FS` for a local client,
   and still admits a peer with no method as read-only). You normally don't need
   to set anything; to force it, `SEC_DEFAULT_AUTHENTICATION = REQUIRED`.

2. **The identity is authorized for WRITE.** With `ALLOW_WRITE` unset, WRITE is
   fail-closed (denied), even for an authenticated user — so you must grant it.

Quick start for **local development** (anonymous writes, no auth needed):

```
ALLOW_WRITE  = *
ALLOW_DAEMON = *          # only if you use an HA mode
```

**Identity-based** (recommended for real use): let FS/TOKEN/SSL map the user and
authorize that identity:

```
SEC_DEFAULT_AUTHENTICATION = PREFERRED      # (htcondordb already prefers it)
ALLOW_WRITE  = you@your.uid.domain
ALLOW_DAEMON = other-daemon@your.uid.domain
```

The daemon logs each connection's outcome at Info —
`htcondordb session opened … user=<fqu> level=READ|WRITE|DAEMON` — which is the
quickest way to see the identity you mapped to and the level it was granted.

## REPL

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

### MATCH — matchmaking between two tables

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

### Formatting and control commands (interactive)

```
.help                 show help
.format <mode>        table (default) | json | classad | classad-new
.output <file>        redirect query output to a file; .output stdout to restore
.quit                 exit
```

The interactive shell has line editing and command history (arrow keys, Ctrl-A/E,
Ctrl-R), persisted to `~/.htcondordb_history`.

### Diagnostics and index tuning

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

`.format json` emits one JSON object per ad (JSONL); `.format classad` /
`classad-new` emit each ad in old / new ClassAd format. In non-table formats a
`SELECT` serializes whole matched ads (projection is a table-mode feature); an
aggregate result serializes its group rows. One-shot mode takes `-format`:

```sh
htcondordb-cli -format json    -e "SELECT * FROM machines WHERE Cpus >= 8"
htcondordb-cli -format classad -e 'SELECT * FROM jobs WHERE Owner == "alice"'
```

### Loading ads from a collector or schedd

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

## Building

```sh
make build     # -> bin/htcondordb and bin/htcondordb-cli
make test      # run the suite
make vet
```

(The Makefile sets the module-graph environment the sibling `replace` directives
need. To run `go` directly, export the same:
`GOWORK=off GOFLAGS=-mod=mod GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform GOPROXY=direct`.)

The `go.mod` `replace` directives point at sibling checkouts for local
development; resolve them to tagged versions before publishing with CI.
