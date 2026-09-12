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

Standard `SEC_*` and `ALLOW_`/`DENY_` knobs configure security and authorization
— see [Authorization](authorization.md).
