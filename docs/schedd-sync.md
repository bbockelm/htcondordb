# Schedd sync mode (`HTCONDORDB_SYNC_SCHEDD`)

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

## Quickstart

Mirror a local schedd's queue and history into a queryable database. Run
htcondordb on the **same host as the schedd**, under `condor_master`, so it
inherits the condor config and drops to the condor user.

1. **Build** the daemon and shell (see [Install](../README.md#install)):

   ```sh
   make build      # -> bin/htcondordb and bin/htcondordb-cli
   ```

2. **Configure.** Add to the HTCondor config the master reads (e.g. a file in
   `/etc/condor/config.d/`). The `HTCONDORDB_*` file paths default to the
   schedd's own `$(JOB_QUEUE_LOG)` / `$(HISTORY)`, so on the schedd host this is
   the whole minimum:

   ```conf
   HTCONDORDB     = /path/to/bin/htcondordb   # absolute path to the built binary
   DAEMON_LIST    = $(DAEMON_LIST), HTCONDORDB
   DC_DAEMON_LIST = +HTCONDORDB               # register it as a DaemonCore daemon

   HTCONDORDB_SYNC_SCHEDD = true              # tail job_queue.log -> jobs, history -> history
   ```

   `DC_DAEMON_LIST` is required for any non-stock daemon: without it the master
   won't treat htcondordb as a DaemonCore daemon (no address file, no
   ready/keepalive), even though `DAEMON_LIST` starts it. The leading `+`
   appends to the built-in list.

   If the schedd's files aren't at the standard `$(JOB_QUEUE_LOG)` /
   `$(HISTORY)` locations, point the tailers explicitly (they read local files,
   so htcondordb still runs beside the schedd):

   ```conf
   HTCONDORDB_JOB_QUEUE_LOG = /var/lib/condor/spool/job_queue.log
   HTCONDORDB_HISTORY       = /var/lib/condor/spool/history
   ```

3. **Start it.** Restart the master — a `condor_reconfig` is **not** enough,
   because `DC_DAEMON_LIST` is only consulted when daemons start:

   ```sh
   condor_restart -master
   ```

   The daemon publishes its command address to `$(LOG)/.htcondordb_address`.

4. **Query.** The two tables fill as the tailers catch up — the live queue in
   `jobs`, completed jobs in `history`:

   ```sh
   bin/htcondordb-cli -e "SELECT COUNT(*) FROM jobs"
   bin/htcondordb-cli -e "SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner ORDER BY COUNT(*) DESC"
   bin/htcondordb-cli -e "SELECT ClusterId, ProcId, JobStatus FROM jobs WHERE Owner == \"alice\""
   bin/htcondordb-cli -e "SELECT COUNT(*) FROM history WHERE CompletionDate > 1700000000"
   ```

   `htcondordb-cli` with no arguments opens the interactive shell and auto-locates
   the daemon via the address file (see the [REPL reference](repl.md)). Reading
   requires READ authorization; the sync itself writes in-process and needs no
   client credentials.

## Related configuration

| Knob | Default | Meaning |
|------|---------|---------|
| `HTCONDORDB_SYNC_SCHEDD` | `false` | Mirror a local schedd: `job_queue.log`→`jobs` table + `history`→`history` archive. |
| `HTCONDORDB_JOB_QUEUE_LOG` | `$(JOB_QUEUE_LOG)` | Schedd job-queue log to tail (live `jobs`). |
| `HTCONDORDB_HISTORY` | `$(HISTORY)` | Schedd history file to tail (`history` archive). |
| `HTCONDORDB_ARCHIVE_ROTATE_INTERVAL` | `3600` | Archive-table retention sweep interval (seconds; `0` disables). |

See [Configuration](configuration.md) for the full knob list.
