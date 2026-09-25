# Configuration

htcondordb is configured through the HTCondor config it inherits from
`condor_master` (plus a couple of environment overrides for clients).

## Address resolution

`HTCONDORDB_ADDRESS_FILE` and `HTCONDORDB_HOST` are also read from the
environment, where they take precedence over the configuration (and over each
other in the same order). That is how a client with no command line — the Python
driver's `connect()` — is pointed at a different daemon. They apply as a pair:
setting either in the environment makes the environment the only source of both,
so overriding just the host cannot lose to a configured address file. Every
client in the tree resolves through `locate.Daemon`, and the daemon publishes to
`locate.AddressFilePath`, so a redirect moves both halves together.

## Knobs

| Knob | Default | Meaning |
|------|---------|---------|
| `HTCONDORDB_DIR` | `$(SPOOL)/htcondordb` | On-disk database directory. |
| `HTCONDORDB_ADDRESS_FILE` | `$(LOG)/.htcondordb_address` | Where the command address is published, and where clients look for it. Also read from the environment. |
| `HTCONDORDB_HOST` | — | Client fallback when no address file is readable. Also read from the environment. |
| `HTCONDORDB_HA_MODE` | `standalone` | `standalone` / `leader-follower` / `consistent`. See [HA](ha.md). |
| `HTCONDORDB_ROLE` | `leader` | Leader-follower role. |
| `HTCONDORDB_LEADER` | — | Leader address (follower). |
| `HTCONDORDB_CURSOR_FILE` | `$(SPOOL)/htcondordb/.replica_cursor` | Follower stream cursor. |
| `HTCONDORDB_RAFT_BOOTSTRAP` | `false` | This node initializes a fresh raft cluster. |
| `HTCONDORDB_RAFT_PEERS` | — | Explicit `id@addr` member list. |
| `HTCONDORDB_RAFT_SIZE` | `0` | Cluster size `N` for first-N-hosts bootstrap. |
| `HTCONDORDB_NODE_ID` | advertised address | This node's stable raft id. |
| `HTCONDORDB_SYNC_SCHEDD` | `false` | Mirror a local schedd: `job_queue.log`→`jobs` table + `history`→`history` archive. See [Schedd sync](schedd-sync.md). |
| `HTCONDORDB_JOB_QUEUE_LOG` | `$(JOB_QUEUE_LOG)` | Schedd job-queue log to tail (live `jobs`). |
| `HTCONDORDB_HISTORY` | `$(HISTORY)` | Schedd history file to tail (`history` archive). |
| `HTCONDORDB_ARCHIVE_ROTATE_INTERVAL` | `3600` | Archive-table retention sweep interval (seconds; `0` disables). |
| `HTCONDORDB_ARCHIVE_MAX_BYTES` | — | Default on-disk size cap applied to **both** schedd-sync archives (`history`, `epoch_history`). Oldest whole segments are dropped past the cap on the retention sweep. Accepts a unit suffix (`10 GB`, `500MiB`) or plain bytes; unset/`0` = no limit. |
| `HTCONDORDB_HISTORY_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for the `history` archive; overrides the shared default (set to `0` to uncap this table while the default caps the other). |
| `HTCONDORDB_EPOCH_HISTORY_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for the `epoch_history` archive; overrides the shared default. |
| `HTCONDORDB_JOB_METRICS` | `false` | Sample running jobs' resource usage (memory/CPU/disk/IO/GPU) into the `job_metrics` archive, off the same `job_queue.log` stream. See [Schedd sync](schedd-sync.md#job-resource-metrics). |
| `HTCONDORDB_JOB_METRICS_ATTRS` | — | Additional job attributes copied onto every sample — an `AccountingGroup`, a `ProjectName`, or a metric the job publishes with `condor_chirp` (all already flow through `job_queue.log`). |
| `HTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS` | `Owner` | Categorical (string-equality) indexes on `job_metrics`. An unindexed `GROUP BY` is a full scan, and adding an index later costs a backfill. |
| `HTCONDORDB_JOB_METRICS_MIN_INTERVAL` | `0` | Seconds between samples for one job; a volume backstop. A state change, a new run and the run's endpoint are never throttled. |
| `HTCONDORDB_JOB_METRICS_SEGMENT_SIZE` | library default (8 MiB) | Sealed-segment size for `job_metrics` (create-time only). Leave it alone unless you have measured: bytes per record is **not** monotone in segment size, and 8 MiB measured best of 2/8/32/64 MiB (2 MiB cost 1.5x the storage for a 4% faster recent-range query). |
| `HTCONDORDB_JOB_METRICS_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for `job_metrics`; overrides the shared default (`0` uncaps this table). |
| `HTCONDORDB_JOB_METRICS_MAX_AGE` | — | Age cap in seconds, measured against `SampleTime`. Both caps apply; whichever binds first drops the oldest whole segments. |

Standard `SEC_*` and `ALLOW_`/`DENY_` knobs configure security and authorization
— see [Authorization](authorization.md).
