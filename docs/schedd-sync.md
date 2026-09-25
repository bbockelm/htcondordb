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

- **Job resource usage** (optional, `HTCONDORDB_JOB_METRICS`) — samples running jobs'
  resource counters out of the *same* parsed `job_queue.log` stream and appends them to
  the **`job_metrics`** archive: one record per committed transaction that moved a usage
  attribute, with per-interval rates derived at ingest. See
  [Job resource metrics](#job-resource-metrics) below.

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
| `HTCONDORDB_ARCHIVE_MAX_BYTES` | — | On-disk size cap applied to both archives; oldest whole segments drop past the cap on the sweep. Accepts `10 GB` / `500MiB` / plain bytes; unset = no limit. |
| `HTCONDORDB_HISTORY_MAX_BYTES` | inherits default | Per-table size cap for `history` (overrides the shared default; `0` uncaps). |
| `HTCONDORDB_EPOCH_HISTORY_MAX_BYTES` | inherits default | Per-table size cap for `epoch_history`. |
| `HTCONDORDB_JOB_METRICS` | `false` | Sample running jobs' resource usage into the `job_metrics` archive. |
| `HTCONDORDB_JOB_METRICS_ATTRS` | — | Additional job attributes to record on every sample (e.g. `ProjectName`, a chirp-published metric). |
| `HTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS` | `Owner` | Which of them get a categorical index (an unindexed `GROUP BY` is a full scan). |
| `HTCONDORDB_JOB_METRICS_MIN_INTERVAL` | `0` | Seconds; throttles redundant samples per job. Never drops a state change or a run endpoint. |
| `HTCONDORDB_JOB_METRICS_SEGMENT_SIZE` | library default | Segment size (create-time only). Measured best as-is; see [Sizing](#sizing). |
| `HTCONDORDB_JOB_METRICS_MAX_BYTES` | inherits default | Per-table size cap for `job_metrics`. |
| `HTCONDORDB_JOB_METRICS_MAX_AGE` | — | Age cap in seconds, measured against `SampleTime`. |

### Bounding disk usage

The archives grow without bound by default. To cap them from the HTCondor config (so a fleet
manages it via configuration management rather than per-database commands), set a byte ceiling:

```conf
HTCONDORDB_ARCHIVE_MAX_BYTES = 20 GB           # applies to both history and epoch_history
HTCONDORDB_EPOCH_HISTORY_MAX_BYTES = 5 GB      # optional per-table override
```

The periodic retention sweep (`HTCONDORDB_ARCHIVE_ROTATE_INTERVAL`) drops the oldest whole
segments once a table exceeds its cap. The caps are applied on every start and `condor_reconfig`,
so a configuration-management change takes effect without recreating the database.

See [Configuration](configuration.md) for the full knob list.

## Job resource metrics

`HTCONDORDB_JOB_METRICS = true` turns on a resource-usage time series for running jobs:
memory, CPU, disk, block I/O, network, and GPU, plotted over time without standing up a
metrics stack. It costs **no new polling** — the shadow already pushes these counters into
the job queue, and the live-jobs tailer is already parsing every one of those commits.

```conf
HTCONDORDB_SYNC_SCHEDD    = true
HTCONDORDB_JOB_METRICS    = true
HTCONDORDB_JOB_METRICS_MAX_AGE = 2592000       # keep 30 days
HTCONDORDB_JOB_METRICS_ATTRS   = ProjectName   # extra grouping dimension
```

```sql
-- CPU efficiency by project, hourly
SELECT time_bucket(SampleTime, '1h') AS time,
       ProjectName                   AS label_project,
       AVG(CpuUtil / RequestCpus)    AS metric_cpu_efficiency
FROM job_metrics
WHERE SampleTime >= 1700000000
GROUP BY time_bucket(SampleTime, '1h'), ProjectName;

-- Right-sizing: how much of requested memory jobs actually used
SELECT Owner AS label_owner, AVG(MemUtil) AS metric_mem_fraction
FROM job_metrics WHERE RunInstanceID == 0 GROUP BY Owner;
```

### Sizing

Measured on a production-shaped population (20k concurrently running jobs, every job sampled in
the same round, so a segment holds one sample each from thousands of different jobs):

| | bytes/record |
|---|---|
| as appended | ~810 |
| after the archive maintenance pass | **~250** |

At 20k running jobs sampling at the 900s floor that is **~480 MiB/day, ~14 GiB for 30 days**,
before whatever the event-driven triggers add on an eventful pool. Budget with
`HTCONDORDB_JOB_METRICS_MAX_BYTES` / `_MAX_AGE`; the retention sweep drops the oldest whole
segments once either binds.

Three notes that follow from the table:

- **Keep archive maintenance enabled.** The 3x comes from the per-segment columnar build, which
  the maintenance pass does (`HTCONDORDB_ARCHIVE_ROTATE_INTERVAL`, hourly by default). With it
  disabled, samples stay in row form and the table is roughly three times bigger.
- **Do not tune the segment size without measuring.** Bytes per record is not monotone in it —
  8 MiB measured best of 2/8/32/64 MiB, and 2 MiB cost 1.5x the storage for a 4% faster
  recent-range query.
- **Watch the baseline share.** If
  `job_metrics_samples_total{outcome="baseline"} / {outcome="appended"}` exceeds ~10%, the
  derived rate columns stop being stored columnar (a column needs to be present on 90% of
  records to enter the segment schema, and a rate is absent on a run's first samples). A pool of
  very short jobs -- sampled only two or three times each before they finish -- is the case that
  trips it, and the cost is both size and the fast path for the columns dashboards aggregate.
  Lowering `SHADOW_QUEUE_UPDATE_INTERVAL` so short jobs get more samples is the lever.

### When a sample is taken

One per committed transaction that moved a usage attribute on a job that is **executing**
(running, transferring output, or suspended) — plus the run's terminal commit whatever status
it leaves the job in. HTCondor includes its queue-update attribute whitelist in *every* kind
of shadow update, not just the periodic one, so samples arrive on:

| Trigger (`SampleTrigger`) | When |
|---|---|
| `periodic` | the starter's stats clock advanced — `SHADOW_QUEUE_UPDATE_INTERVAL`, default 900s |
| `status` | job state change: began executing, suspended, transfer start/finish, reconnect |
| `checkpoint` | a checkpoint completed |
| `terminal` | the run ended — completed, evicted, held or removed |
| `update` | a usage attribute moved with no other signal |
| `chirp` | only an admin-configured extra attribute moved |

The periodic timer is therefore a **floor**, not a sampling window: an eventful job gets more
points, exactly where a plot wants them. Lowering `SHADOW_QUEUE_UPDATE_INTERVAL` raises the
floor but multiplies the schedd's job-queue write traffic for *every* whitelisted attribute,
so prefer `HTCONDORDB_JOB_METRICS_MIN_INTERVAL` to bound volume rather than raising cadence to
chase resolution.

Because the terminal commit carries the run's final counters, the **last sample of a run is
its endpoint** — a resource plot needs no `epoch_history` lookup to find where a run finished.

### Reading the columns correctly

Two properties of HTCondor's counters will mislead a dashboard built without them:

- **The memory numbers are high-water marks, not gauges.** `MemoryUsage`, `ResidentSetSize`
  and `ImageSize` only ratchet up, so a memory series is a staircase, not a working-set trace.
  Worse, the shadow **seeds them from the previous run**, so for `RunInstanceID > 0` they are
  a whole-*job* maximum wearing this run's timestamp — that run's real peak was never written
  anywhere. **Filter to `RunInstanceID == 0` to ask a per-run memory question.**
- **Rates are derived at ingest, not at query time.** `CpuUtil`, `BlockReadRate`,
  `NetworkInRate`, `GpuUtil` and friends are computed from consecutive observations when the
  sample is written, because the SQL surface has no window functions or `LAG`. They are
  **undefined, never zero**, when there is no usable predecessor — a run's first sample, the
  first after a daemon restart, or an interval spanning a run boundary (counters restart).
  `SampleBaseline = true` marks those, and `time_bucket` drops undefined rows, so a series
  shows a gap rather than a false zero.

`SampleInterval` is elapsed time **net of suspension**, so a suspended interval reports the
rate over the time the job was actually running.

### Watching it

`htcondordb_job_metrics_samples_total{outcome=...}` counts samples by outcome. Two of the
outcomes are bug signals rather than workload signals and should stay at zero:

- `reset` — a rate was suppressed because its counter went backwards, i.e. HTCondor reset a
  counter in a way the sampler does not model.
- `inherited` — a new run was still carrying the previous run's counters when first observed.

`baseline` is expected at a low rate (roughly one or two per run); `throttled` is whatever
`HTCONDORDB_JOB_METRICS_MIN_INTERVAL` is dropping; `deduped` should be nonzero only just after
a restart.

See [the design sketch](design/job-metrics.md) for why each of these rules exists, with
citations into the HTCondor source.
