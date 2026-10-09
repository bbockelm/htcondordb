# Authorization & security

## Access levels

One authenticated CEDAR connection carries an entire `dbrpc` multiplex. The
access level is decided once, at connect time, from the authenticated identity
(re-evaluated per connection, so a reconfigure takes effect on the next
connection):

- **READ** — read-only, and every returned ad has its private (secret)
  attributes stripped (claim ids, capabilities, transfer keys).
- **WRITE** — read/write of ads in tables no in-process writer owns (see
  below). Private attributes are still stripped: a submitter can add and update
  jobs without seeing other principals' secrets.
- **DAEMON** — WRITE plus private attributes, the administrative table actions
  (index/hot/compact/retrain, encryption, truncate, retention, rotate), and the
  HA/replication surface (commit stream, raft transport, cluster control).

The command is registered at READ; the handler escalates to WRITE/DAEMON by
re-checking the `ALLOW_`/`DENY_` policy on the peer. Private stripping and
read-only enforcement are implemented in `dbrpc` via per-connection
`ServeOptions`.

## Tables owned by an in-process writer

Some tables are maintained by a writer inside the daemon, and are read-only to
every client — DAEMON included:

| Owner | Tables | While |
|-------|--------|-------|
| schedd sync | `jobs`, `users`, `jobsets`, `clusters`, `header`, `clusterprivate`, `logmeta` (job_queue.log); `job_metrics` (`HTCONDORDB_JOB_METRICS`); `history`; `epoch_history` | `HTCONDORDB_SYNC_SCHEDD` is on and the source file is configured |
| replication (cedar-sync) | each `HTCONDORDB_REPLICATE_<NAME>_TARGET` | the source is listed in `HTCONDORDB_REPLICATE_SOURCES` |
| history import | each managed `HTCONDORDB_HISTORY_IMPORT_<NAME>_TABLE` | the job is configured and the daemon runs the importer (not with `HTCONDORDB_MANAGE_HISTORY_IMPORT = false` or no `history-import` binary) |

Ownership follows configuration: it is re-evaluated on every reconfigure, and a
table is an ordinary table again once its writer is disabled. Names match
case-insensitively.

Clients may query and `WATCH` an owned table, but every write is refused with
`read-only table "<name>": … not permitted` (`dbrpc.ErrTableReadOnly`, a
deterministic error — do not retry): ad writes and commits, `DELETE`,
`CREATE`/`DROP` of that name, archive appends, convert-to-memory, restore, and
the admin actions that remove data (`truncate`, `rotate`, `retention.set`). The
row-preserving admin actions (index, hot set, compact, rewrite, retrain, schema,
analyze) still work. The consistent-mode `DBControl` write path applies the same
rule. The managed history importer writes its own tables over a session minted
for it; nothing else can.

Operators reach the data-removing actions through the owner, over
`DBSyncControl`; the REPL does this on its own when the direct action is
refused:

- `.truncate history` / `.truncate epoch_history` — schedd sync wipes the
  archive and re-reads the history file from the start, in one step.
  `.truncate job_metrics` wipes the samples (they are not re-derived).
- `.truncate` of a job_queue.log table is refused: run `.resync jobs`, which
  rebuilds them from the current log.
- `.truncate` of a replica or import target is refused, since neither source
  resends what its cursor has passed: remove the source/job from the config and
  `condor_reconfig` first (and delete its cursor file to fill the table again).
- `.rotate` and `.retention` work on any owned archive. For a schedd-sync
  archive the configured size cap (`HTCONDORDB_*_MAX_BYTES`) is re-applied when
  the daemon restarts or a reconfigure changes it.

## Commands (from the retired transferd block)

| Command | Int | Level | Purpose |
|---------|-----|-------|---------|
| `DBSession` | 74000 | READ | The multiplexed DB RPC session. |
| `DBReplicate` | 74001 | DAEMON | (reserved) dedicated commit stream. |
| `DBRaft` | 74002 | DAEMON | Raft transport tunneled over CEDAR. |
| `DBControl` | 74003 | WRITE | Consistent-mode control (leader discovery, peer registration, write-batch apply). |
| `DBSyncControl` | 74004 | DAEMON | Sync control: `resync` a schedd-sync tailer or exporter; `truncate`/`rotate`/`retention.set` an owned table through its owner; `owner` (who owns a table). |
| `DBSyncStatus` | 74005 | READ | Sync health ad. |

## Getting WRITE

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

Standard `SEC_*` and `ALLOW_`/`DENY_` knobs configure security and authorization.
