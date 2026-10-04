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
| `HTCONDORDB_SYNC_SCHEDD` | `false` | Mirror a local schedd: `job_queue.log`→`jobs` table + `history`→`history` archive. The mirrored tables are read-only to clients while sync is on. See [Schedd sync](schedd-sync.md). |
| `HTCONDORDB_JOB_QUEUE_LOG` | `$(JOB_QUEUE_LOG)` | Schedd job-queue log to tail (live `jobs`). |
| `HTCONDORDB_HISTORY` | `$(HISTORY)` | Schedd history file to tail (`history` archive). |
| `HTCONDORDB_MIRRORED_SCHEDD_NAME` | the schedd's own rule | The schedd this spoke mirrors, advertised as `MirroredScheddName` (collector ad, `DBSyncStatus`, `syncstatus` row) when schedd sync runs. Unset follows the schedd's naming: `SCHEDD.SCHEDD_NAME`/`SCHEDD_NAME` used as is when it contains `@`, the full hostname when it names this host, else `name@$(FULL_HOSTNAME)`; with no `SCHEDD_NAME`, `$(FULL_HOSTNAME)` (or `user@$(FULL_HOSTNAME)` for a non-condor user). Set it when the schedd is started with `-name` or a local name. The chosen name and the rule are logged at startup. `MirroredScheddAddress` is the first line of `SCHEDD_ADDRESS_FILE`, omitted when unreadable. |
| `HTCONDORDB_SYNCSTATUS_INTERVAL` | `5` | Seconds between rewrites of the `syncstatus` heartbeat row (key `status`) while schedd sync runs. A federation hub reads per-AP freshness from it. A duration suffix (`5s`, `1m`) is accepted. See [Federation](federation.md#spoke-side). |
| `HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT` | — | Run as a [federation hub](federation.md) over the schedds matching this ScheddAd constraint (needs `COLLECTOR_HOST`). Refused together with `HTCONDORDB_SYNC_SCHEDD`. |
| `HTCONDORDB_FEDERATE_SPOKES` | — | Static hub spokes, by name; each needs `HTCONDORDB_FEDERATE_SPOKE_<NAME>_ADDRESS` and may set `HTCONDORDB_FEDERATE_SPOKE_<NAME>_SCHEDD` (default `<NAME>`). Static spokes skip host validation. Also enables hub mode. |
| `HTCONDORDB_FEDERATE_SPOKE_<NAME>_ADDRESS` | — | A static spoke's command address. |
| `HTCONDORDB_FEDERATE_SPOKE_<NAME>_SCHEDD` | `<NAME>` | The schedd a static spoke mirrors; its rows carry this `ScheddName`. |
| `HTCONDORDB_FEDERATE_TABLES` | `jobs history syncstatus` | Spoke tables a hub fans in (`epoch_history` also supported). |
| `HTCONDORDB_FEDERATE_RETIRE_AFTER` | `7d` | A hub deletes an AP's mutable rows once it has been unseen, or unmatched after a constraint change, this long. Seconds or a suffix (`36h`, `7d`). Archive rows are never deleted by retirement. |
| `HTCONDORDB_FEDERATE_FRESH_SECONDS` | `60` | A hub source at or under this staleness is `fresh`. |
| `HTCONDORDB_FEDERATE_DISCOVER_INTERVAL` | `60` | Seconds between a hub's collector discovery passes. |
| `HTCONDORDB_FEDERATE_STATE_INTERVAL` | `5` | Seconds between a hub's source-state refreshes (`federation_sources`, ad summary, metrics). |
| `HTCONDORDB_ARCHIVE_ROTATE_INTERVAL` | `3600` | Archive-table retention sweep interval (seconds; `0` disables). |
| `HTCONDORDB_ARCHIVE_MAX_BYTES` | — | Default on-disk size cap applied to **both** schedd-sync archives (`history`, `epoch_history`). Oldest whole segments are dropped past the cap on the retention sweep. Accepts a unit suffix (`10 GB`, `500MiB`) or plain bytes; unset/`0` = no limit. |
| `HTCONDORDB_HISTORY_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for the `history` archive; overrides the shared default (set to `0` to uncap this table while the default caps the other). |
| `HTCONDORDB_EPOCH_HISTORY_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for the `epoch_history` archive; overrides the shared default. |
| `HTCONDORDB_JOB_METRICS` | `false` | Sample running jobs' resource usage (memory/CPU/disk/IO/GPU) into the `job_metrics` archive, off the same `job_queue.log` stream. See [Schedd sync](schedd-sync.md#job-resource-metrics). |
| `HTCONDORDB_JOB_METRICS_ATTRS` | — | Additional job attributes copied onto every sample — an `AccountingGroup`, a `ProjectName`, or a metric the job publishes with `condor_chirp` (all already flow through `job_queue.log`). |
| `HTCONDORDB_JOB_METRICS_DERIVED` | — | Names computed columns, each evaluated once per sample from `HTCONDORDB_JOB_METRICS_DERIVED_<NAME>`. Use this for a dimension you will `GROUP BY`: a computed group key is evaluated client-side, so every matching row crosses the wire, while a stored column groups server-side and can carry a categorical index. |
| `HTCONDORDB_JOB_METRICS_DERIVED_<NAME>` | — | The ClassAd expression for one computed column. It sees the whole finished sample, including the derived rates — so `CpuUtil / RequestCpus` works, which no query can express (an aggregate's argument may not contain an expression, and subqueries are unsupported). Undefined results are not stored. |
| `HTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS` | `Owner` | Categorical (string-equality) indexes on `job_metrics`. An unindexed `GROUP BY` is a full scan. Applied to an **existing** archive too: a newly-named attribute is backfilled in the background on the next start or `condor_reconfig`, which decompresses every record once. Add-only — an index created by hand is never dropped. |
| `HTCONDORDB_JOB_METRICS_MIN_INTERVAL` | `0` | Minimum spacing between samples for one job; a volume backstop. A state change, a new run and the run's endpoint are never throttled. A bare number is seconds; a duration suffix (`5m`, `2h`, `1d`) is accepted. |
| `HTCONDORDB_JOB_METRICS_SEGMENT_SIZE` | library default (8 MiB) | Sealed-segment size for `job_metrics` (create-time only). Accepts a unit suffix (`8 MiB`); `0` means the library default. Values below 64 KiB or at/above 4 GiB are refused (logged, default used). Leave it alone unless you have measured: bytes per record is **not** monotone in segment size, and 8 MiB measured best of 2/8/32/64 MiB (2 MiB cost 1.5x the storage for a 4% faster recent-range query). |
| `HTCONDORDB_JOB_METRICS_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for `job_metrics`; overrides the shared default (`0` uncaps this table). |
| `HTCONDORDB_JOB_METRICS_MAX_AGE` | — | Age cap measured against `SampleTime`. A bare number is seconds; a duration suffix (`30d`, `720h`) is accepted. Both caps apply; whichever binds first drops the oldest whole segments. |
| `HTCONDORDB_JOB_METRICS_GROUP_SCHEMAS` | `true` | **Create-time only** — the storage layer has no runtime setter for it, so on an existing archive this is ignored; set it before first enabling or drop the table. Allows secondary (group) columnar schemas for attributes only *some* jobs carry — GPU metrics, a container universe's `NetworkIn`/`NetworkOut`. They sit below the 90% presence a field needs in the base schema, so without grouping they are stored as rows. Not free: measured at +60% bytes/record on a heterogeneous pool, in exchange for the columnar fast path on those attributes. A pool with no GPUs and no containers pays nothing either way. |

Standard `SEC_*` and `ALLOW_`/`DENY_` knobs configure security and authorization
— see [Authorization](authorization.md).

### Unparseable values

Every `HTCONDORDB_JOB_METRICS_*` knob above that takes a size or a duration reports a value it
cannot parse, at `ERROR`, naming the knob — and falls back to the **safe** direction: no cap, no
throttle, the library default. Nothing is guessed at.

This is worth stating because the older integer parser did guess, and silently.
`HTCONDORDB_JOB_METRICS_MAX_AGE = 30d` resolved to **30 seconds**, so the hourly retention sweep
erased the table on every pass; `HTCONDORDB_JOB_METRICS_SEGMENT_SIZE = 8 MiB` resolved to **8
bytes**. If you are reading logs from a deployment older than this change, check for
`max_age_seconds` and `SegmentSize` values far smaller than what the configuration says.
