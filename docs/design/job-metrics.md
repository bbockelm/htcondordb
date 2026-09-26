# Design sketch: `job_metrics` — per-job resource-usage samples

**Status:** IMPLEMENTED (Phase 1). This document is the rationale; `scheddsync/metrics.go` is
the spec. Where the two differ the code is right — the sections below were updated to match what
was actually built, and §10 records where implementation changed the design.
**Problem owner:** "plot memory/CPU usage over time for running jobs", without standing up Prometheus.
**Siblings:** [time-series.md](time-series.md) (the `time_bucket` read surface this reuses),
[continuous-aggregates.md](continuous-aggregates.md) (the *aggregate* recorder; this is the
*per-entity* one).

## 1. What we are building

A second archive table, `job_metrics`, holding one flat record per **(job, run attempt,
observation)** carrying that job's resource counters as of that observation. It is fed as a
side effect of the schedd-sync tailer we already run — **one sample per committed update that
touched a usage attribute**, not on a clock of our own — it is append-only, and it drops
oldest-first when it hits a size cap. Reads are ordinary `time_bucket` queries
(time-series.md §4a), so Grafana graphs and the Prometheus exporter need no new query syntax.

It is *not* a third kind of time series. time-series.md §3 splits series into **event**
(row carries a timestamp, bucket by it) and **gauge** (a row-version must be counted in
every bucket it was alive during). `job_metrics` converts the gauge problem into the event
problem by **recording the observations** — the same trick as a continuous aggregate, one
level down: continuous aggregates record a *grouped aggregate* per bucket, this records a
*raw observation per job*, so the grouping stays open-ended (by Owner, by project, by exit
code, by anything we chose to carry).

## 2. What HTCondor actually gives us (the constraints that shape everything)

### 2.1 The tap point, and the exact menu

The shadow holds the live job ad and pushes dirty attributes into the job queue, but it
pushes only attributes on a **whitelist**: `QmgrJobUpdater::common_job_queue_attrs`
(`condor_schedd.V6/qmgr_job_updater.cpp:80`, enforced at `:418`). That list *is* the menu —
nothing outside it reaches `job_queue.log` while a job runs, so nothing outside it can be
sampled by a `job_queue.log` tailer.

The usage-bearing entries, grouped by what they actually mean:

| Kind | Attributes | Plot directly? |
|---|---|---|
| **High-water mark** (max-so-far, monotone) | `MemoryUsage`, `ResidentSetSize`, `ProportionalSetSize`, `ImageSize`, `DiskUsage`, `ScratchDirFileCount` | Yes, but it is a *ratchet*, and §2.4 says whose ratchet |
| **Cumulative counter** | `RemoteUserCpu`, `RemoteSysCpu`, `CumulativeRemoteUserCpu`, `CumulativeRemoteSysCpu`, `BlockReadBytes`/`BlockWriteBytes`/`BlockReads`/`BlockWrites`, `BlockReadKbytes`/`BlockWriteKbytes`, `NetworkIn`, `NetworkOut`, `JobCpuInstructions`, `BytesSent`, `BytesRecvd`, `CumulativeTransferTime`, `CommittedTime`, `CommittedSlotTime` | No — differentiate first, and read §2.4 on *what* each one accumulates over |
| **Already windowed** (starter's recent-window stats) | `RecentBlockReadBytes`, `RecentBlockWriteBytes`, `RecentBlockReads`, `RecentBlockWrites`, `RecentStatsLifetimeStarter`, `RecentWindowMaxStarter` | Yes (a rate over the starter's own window) |
| **Instantaneous** | `IOWait`, `JobVMCpuUtilization` (VM universe only) | Yes |
| **Sample clock / run identity** | `StatsLastUpdateTimeStarter`, `StatsLifetimeStarter`, `NumJobStarts`, `JobCurrentStartExecutingDate`, `JobStatus`, `LastJobLeaseRenewal`, `NumJobCompletions`, `TotalSuspensions`, `CumulativeSuspensionTime` | n/a — these are the axes |
| **Nested ads** | `TransferInputStats`, `TransferOutputStats`, `TransferCommonStats` | Not without flattening — see §3.3 |

Two attributes are worth calling out because they are *not* on the static list and arrive by
a different route:

- **`CpusUsage`** and **the CMR family (`GPUsUsage`, `GPUsAverageUsage`, `Recent<R>Usage`,
  `Assigned<R>`)** are pulled from the slot's update ad by the starter
  (`condor_starter.V6.1/jic_shadow.cpp:2267-2302`, driven by `STARTD_CRON_<name>_METRICS`),
  forwarded by the shadow through a *suffix* match on `Usage`/`AverageUsage`/`Provisioned`
  and a prefix match on `Assigned` (`condor_shadow.V6.1/remoteresource.cpp:1655-1690`), and
  each one is then added to the whitelist dynamically via `watchJobAttr` →
  `QmgrJobUpdater::watchAttribute`. So **GPU metrics reach `job_queue.log` only on pools
  that configured the GPU monitor** (`use feature: GPUsMonitor`). The sampler must treat
  them as optional-and-discovered, not as a fixed schema.
- **Chirp-set attributes** ride the same `watchJobAttr` path
  (`remoteresource.cpp:1643-1652`). That is the free win behind "let the admin specify
  additional attributes": a user's `condor_chirp set_job_attr TrainingLoss 0.42` is already
  in `job_queue.log`, and an admin-named extra attribute makes it a plottable series with
  no further work.

### 2.2 Cadence: the periodic timer is a floor, not a window

The design records **whatever the schedd commits, when it commits it**. There is no window
and no sampling clock of our own — which means the series self-adjusts to how eventful a job
is, and it means the *last* sample of a run is the run's real endpoint (§4.3). What follows
is therefore a description of how often updates arrive, not a resolution we impose.

The load-bearing fact: **`common_job_queue_attrs` is included in *every* update type.** The
filter at `qmgr_job_updater.cpp:418` is `common_job_queue_attrs.count(name) ||
(job_queue_attrs && job_queue_attrs->count(name))` — the type-specific list *adds to* the
common list, it does not replace it. So every one of these carries the current usage numbers:

| Trigger | When | Source |
|---|---|---|
| `U_PERIODIC` | every `SHADOW_QUEUE_UPDATE_INTERVAL`, **default 900s** | `qmgr_job_updater.cpp:243` |
| `U_STATUS` | execution begins, suspend/resume, transfer start/finish, reconnect, proxy refresh, job-state change | ~8 call sites in `remoteresource.cpp` (1811, 1861, 2045, 2166, 2303, 2802, …) |
| `U_CHECKPOINT` | each successful checkpoint | `remoteresource.cpp:1944` |
| `U_TERMINATE` / `U_EVICT` / `U_HOLD` / `U_REQUEUE` | end of run, whatever the reason | `baseshadow.cpp:700, 761, 787, 948, 1087, 1135` |
| starter-requested `U_PERIODIC` | starter asks for an early flush; resets the timer to fire now | `pseudo_ops.cpp:891` → `resetUpdateTimer` |
| chirp `updateAttr` | **immediately, one commit per attribute** | `qmgr_job_updater.cpp:281-310` (its own ConnectQ/SetAttribute/DisconnectQ) |

So the periodic timer is the *floor* — a quiet job gets a point every 15 minutes — and
everything interesting that happens to a job adds a point for free, exactly where a plot
most wants one. A chirp-instrumented job gets resolution bounded only by how often it chirps.

The starter's own sampling rate (`STARTER_UPDATE_INTERVAL`, default 300s,
`condor_starter.V6.1/job_info_communicator.cpp:950`) bounds how *fresh* the numbers in any
update are: a `U_STATUS` at 14:03 carries whatever the starter last reported, which may be
up to 5 minutes stale. `StatsLastUpdateTimeStarter` is the starter's own timestamp for when
it gathered them (`remoteresource.cpp:1462-1467` renames `StatsLastUpdateTime` precisely so
it does not collide with schedd statistics), so it is the honest observation instant and it
absorbs both the shadow's flush delay and the tailer's lag. **Use it as `SampleTime`.** Two
commits can therefore share a `SampleTime` — the shadow flushed twice between starter
reports — which the identity scheme in §4.2 has to tolerate.

Raising resolution beyond this means lowering `SHADOW_QUEUE_UPDATE_INTERVAL`, which
multiplies job-queue write traffic and `job_queue.log` growth for *every* whitelist
attribute, not just the usage ones. That is a real cost, and §8 has a cheaper upstream
alternative.

### 2.3 The trap: there is no instantaneous memory number, anywhere

`ResidentSetSize` is maxed **twice**: once in the starter, in a function-static that never
decreases (`condor_starter.V6.1/vanilla_proc.cpp:766`), and again in the shadow, which only
ever writes a larger value (`remoteresource.cpp:1386-1391`). `MemoryUsage` is the same story
(`remoteresource.cpp:1359-1380`) and by default is just `((ResidentSetSize+1023)/1024)`.
`ImageSize` and `DiskUsage` likewise only ratchet up.

**Consequence:** a "memory usage over time" plot built from `MemoryUsage` is a monotone
staircase. It answers "how much has this job ever needed" (the right-sizing question, which
is probably what the team actually wants) but it cannot show a job that allocated 100 GB for
ten minutes and then freed it — the curve stays at 100 GB. Nobody should read it as a
working-set trace.

The underlying instantaneous value *exists* — `ProcFamilyUsage.total_resident_set_size` is
re-summed from scratch on every sample (`condor_procd/proc_family_monitor.cpp:471`,
`proc_family.cpp:752`) — it is simply discarded by the two maxes. Publishing it under a new
attribute is a small, self-contained HTCondor change; see §8.

CPU has the mirror-image property in a benign direction: `RemoteUserCpu`/`RemoteSysCpu` are
cumulative seconds, so **differences between consecutive observations give a genuine
per-interval utilization**. Same for block I/O, network, and instructions. GPU is the
awkward one: `GPUsAverageUsage` is a *lifetime* average by construction
(`condor_startd.V6/Resource.cpp:1704-1730` builds it as
`(Uptime - StartOfJobUptime)/(LastUpdate - FirstUpdate)`), so per-interval GPU utilization
has to be de-averaged across observations.

### 2.4 Counter reset semantics: three different policies in one function

**Do not assume anything resets.** `RemoteResource::setJobAd` (`remoteresource.cpp:1153`) is
the function that decides what a new run inherits, and it applies three different policies
in forty lines:

| Attribute(s) | Policy at run start | Where |
|---|---|---|
| `RemoteUserCpu`, `RemoteSysCpu` | **Explicitly zeroed**, with a comment saying they "reflect usage for this execution only" | `remoteresource.cpp:1165-1171` |
| `ImageSize`, `MemoryUsage`, `ResidentSetSize`, `ProportionalSetSize` | **Seeded from the previous run's value** — the high-water mark is carried forward, across runs and across machines | `remoteresource.cpp:1179-1195` |
| `DiskUsage`, `ScratchDirFileCount` | Shadow's *member* reset to 0/-1 so the first starter report wins even if smaller — but the **job-ad attribute is not cleared**, so it carries the old value until that report lands | `remoteresource.cpp:1197-1199`, applied at `:1483-1494` |
| `BytesSent`, `BytesRecvd` | **Never reset** — explicitly seeded from the job ad as `prev_run_bytes_*` and added to this run's transfer | `baseshadow.cpp:145-149`, `:1565-1567` |
| `CumulativeRemoteUserCpu`/`SysCpu`, `CommittedTime`, `CumulativeSuspensionTime`, `NumJobStarts` | Cumulative across runs by design | shadow/schedd |
| block I/O, network, `JobCpuInstructions`, `Recent*`, `IOWait` | Per-*starter*, so per-run in practice; copied straight through, and `CopyAttribute` **deletes** the target when the starter omits it | `remoteresource.cpp:1402-1424` |

Three consequences that the design has to absorb:

1. **The memory high-water mark is per-job, not per-run, and that data is unrecoverable.**
   A job that peaked at 64 GB on its first run and then reruns on a small machine using 2 GB
   will report 64 GB for the whole second run — the shadow's `memory_usage_mb` is seeded to
   the old value and never writes anything smaller. No post-processing recovers run 2's real
   peak, because it was never written. So: **memory series are trustworthy per-run only for
   `RunInstanceID == 0`**, and for later runs they are a whole-job HWM wearing a run's
   timestamp. Carry `RunInstanceID` on every sample and *say this in the dashboard*, or the
   first person to investigate a rerun will draw the wrong conclusion. (This looks
   deliberate — it matches `condor_history` semantics, and the `DiskUsage` code right below
   it has a comment explaining why *it* does the opposite — so the fix is a new per-run
   attribute, not a behavior change. See §8.)

2. **There is an explicit zero at run start, and it does reach the queue.** `baseInit`
   constructs `QmgrJobUpdater` — which calls `ClearAllDirtyFlags` — at `baseshadow.cpp:253`,
   and `setJobAd` runs *after* it (`shadow.cpp:120`). So the `RemoteUserCpu = 0` assignment
   is dirty and gets pushed on the shadow's first update. That is useful: it is a real,
   observable run baseline, and it makes the first CPU delta of a run well-defined. It also
   means a sample can legitimately carry `RemoteUserCpu == 0` mid-stream.

3. **A window exists where a sample carries a new run's identity and the old run's numbers.**
   The schedd bumps `NumShadowStarts` in its own transaction before the shadow starts
   (`condor_schedd.V6/schedd.cpp:13083-13087`); the shadow's zeroing update lands later. A
   commit in between yields a sample with the new `RunInstanceID` and run *N−1*'s
   `DiskUsage`/HWMs. The sampler must not treat that as run *N*'s first observation — see
   §3.4.

One more, within a run: a self-checkpoint restart does **not** reset the starter's counters
(`m_checkpoint_usage += last_usage`, `vanilla_proc.cpp:976`), but a *new* starter resuming
from a checkpoint elsewhere starts at zero. So "counters are monotone within a
`RunInstanceID`" holds, and nothing stronger does.

## 3. The table

### 3.1 Type and key — no key needed

`collections.Archive.Append` passes a **nil key** and stores a zero-length one
(`collections/archive.go:208-211`: *"An append log has no key … Duplicate appends are all
retained"*). So the answer to "will we need to make up a different key?" is **no** — an
archive record has no key to collide, and `job_metrics` needs no synthetic `SampleId`. The
`KeyAttr` stamping in `scheddsync/jobsync.go:1346` is a mutable-table concern (the REPL needs
a key to address rows for `UPDATE`/`DELETE`) and does not apply here.

What *is* needed is a way to recognize a sample the archive already holds, because a restart
re-applies part of the log and appends are not idempotent. §4.2 uses an identity *constraint*,
the same "identity as a query, not as a key" approach the epoch tailer already uses.

### 3.2 Record shape

Flat, scalar-only, same attribute set every time — the ideal shape for the columnar segment
(§3.3). Roughly 50 attributes:

```
# identity
ClusterId, ProcId, GlobalJobId,
RunInstanceID,                           # = NumShadowStarts - 1; joins epoch_history
Owner, User, RemoteHost,
<admin-configured extras>                # AccountingGroup, ProjectName, chirp attrs, ...

# provenance
LogSeq,                                  # the job_queue.log generation (op-107 sequence number)

# time axis
SampleTime,                              # StatsLastUpdateTimeStarter (starter clock)
SampleTimeFromIngest,                    # true when that clock was unavailable and this is ours
SampleInterval,                          # elapsed since the previous sample, net of suspension
SampleTrigger,                           # periodic|status|checkpoint|terminal|update|chirp
SampleBaseline,                          # true => no usable predecessor, so no rates
JobCurrentStartExecutingDate, JobStatus, CumulativeSuspensionTime

# as-sampled: high-water marks (verbatim -- see §2.4 note 1 before plotting these per run)
MemoryUsage, ResidentSetSize, ProportionalSetSize, ImageSize, DiskUsage,
RequestMemory, RequestCpus, RequestDisk, RequestGpus,   # the denominators

# as-sampled: cumulative counters (verbatim, so the series is re-derivable)
RemoteUserCpu, RemoteSysCpu, CumulativeRemoteUserCpu, CumulativeRemoteSysCpu,
BlockReadBytes, BlockWriteBytes, BlockReads, BlockWrites,
NetworkIn, NetworkOut, JobCpuInstructions, BytesSent, BytesRecvd, IOWait,
GPUsAverageUsage, CpusUsage              # when the pool publishes them

# derived at ingest (§3.4)
CpuUtil, MemUtil, DiskUtil, GpuUtil,
BlockReadRate, BlockWriteRate, BlockReadOpRate, BlockWriteOpRate,
NetworkInRate, NetworkOutRate, InstructionRate, BytesSentRate, BytesRecvdRate
```

**Run identity.** `RunInstanceID = NumShadowStarts - 1`, exactly how the epoch writer derives
it (`condor_utils/job_ad_instance_recording.cpp:182-192`). The schedd writes
`NumShadowStarts` to the queue on every spawn (`schedd.cpp:13083-13087`), so it is in
`job_queue.log`. Same spelling as `epoch_history` means a dashboard can cross to that run's
epoch record with no translation.

**`SampleTrigger`** is cheap (we know it from which attributes the commit touched) and worth
carrying: it lets a panel filter to `!= "chirp"` for an even-ish series, and it makes
`terminal` samples findable without a join.

### 3.3 Why this shape is the best case for columnarization

The per-segment PAX accelerator builds a schema from attributes whose *dominant storable
kind* is a scalar literal (`collections/adschema.go:40`, `nodeKind`); computed expressions,
lists and **nested ClassAds** fall back to row form. So:

1. **Keep the record flat.** `TransferInputStats`/`TransferOutputStats` are nested ads and
   would be the only un-columnarized bytes in an otherwise perfect record. Drop them, or
   flatten the handful of scalars worth having into top-level attributes.
2. **Coverage should approach 100%, not the ~54% a real job ad reaches.** The
   columnar-native measurement on real OSPool machine ads got −43% total bytes at ~54%
   schema coverage, limited by the long tail of attributes only some ads carry.
   `job_metrics` has a fixed, small, all-numeric attribute set, most columns monotone per
   job. This table should do dramatically better per record than history does — which is
   what makes 30 days of it affordable.

**MEASURED** (`scheddsync/metrics_scale_test.go`, `HTCONDORDB_SCALE=1`), and the estimate this
paragraph used to carry — 40–80 bytes/record — was wrong by about 4x:

| | bytes/record |
|---|---|
| as appended (row form) | **~810** |
| whole table after maintenance | ~264 |
| **columnarized data only** (the steady state) | **~252** |

Measure the third row, not the second: the active (unsealed) segment is never columnarized, so a
whole-table average is diluted by it — by ~5% at 47 segments and by ~25% at the eight a CI-sized
run produces. The first version of this measurement reported the diluted number.

20k running jobs at the 900s floor is 1.9M samples/day ≈ **480 MiB/day ≈ 14 GiB for 30 days**,
before whatever event-driven sampling adds on an eventful pool. That is four to six times the old
estimate, and it is the number the retention guidance in the user docs is written against.

**Where the bytes go** (50 columnar fields, 210 B of uncompressed fixed slots per record):

| group | fields | slot B/rec | share |
|---|---|---|---|
| derived rates | 10 | 80 | 38% |
| raw counters | 13 | 71 | 34% |
| context (including 6 strings) | 16 | 23 | 11% |
| high-water marks | 4 | 14 | 7% |
| sample metadata | 5 | 14 | 7% |
| gauges | 1 | 8 | 4% |

The derived rates are the single largest contributor, which is the honest price of §3.4: ten
float64 columns of high-entropy values, bought because `rate(counter)` is not expressible as a
query. The raw counters are next, and they are kept (§3.4) so a wrong derivation is fixable by
recomputation. Nothing else is close, and no single attribute is a villain — removing all three
identity strings was measured at roughly 10%, inside the noise of the segment-size effect below.

Two caveats on the number itself:

- **The generator's counters increment by uniform random amounts**, which is the least
  compressible thing they could do. Real block-I/O and instruction counters are smoother, so
  ~252 B/rec should be read as an upper bound.
- **The rate columns only columnarize when the pool's jobs are sampled enough times.** A field
  enters the schema at >=90% presence, and a rate is absent on a run's rate-less samples. A job
  sampled 24 times carries rates on 96% of its records and the columns are taken; a pool whose
  jobs are sampled 4 times sits at 75% and *every* rate column falls out — costing the storage
  and, worse, the columnar fast path for exactly the columns a dashboard aggregates. The sampler
  already exports the signal: `baseline / appended` IS that absence fraction, so an operator can
  see it coming (§5). `TestJobMetricsRecordComposition` asserts the rule rather than a size.

Two things the measurement changed about how to think of this table:

- **The columnar build is not free and not automatic-on-write.** Records land in row form and
  are rewritten by the maintenance pass (`ArchiveSchemaScanHotTopN`, on by default). The 2.7x is
  only realized once that pass has run, so a deployment with archive maintenance disabled pays
  ~780 bytes/record indefinitely.
- **The population has no locality, by construction.** Every running job is sampled in the same
  round, so a segment holds one sample each from thousands of DIFFERENT jobs — a job's
  consecutive samples are tens of thousands of records apart. Dropping the identity strings
  (`GlobalJobId`, `User`, `RemoteHost`) to fight that was measured and is worth ~10%, inside the
  noise of the segment-size effect below. They stay.

### 3.4 Derive the rates at ingest, not at query time

The SQL surface has **no window functions, no `LAG`, no self-join** — `JOIN` and subqueries
are rejected outright (`docs/repl.md`). So `rate(counter)`, the thing Prometheus does for
free, cannot be expressed as a query over stored counters. Either every dashboard
reimplements it (wrongly, around restarts and gaps) or **the sampler does it once at write
time**, where it already holds the previous observation.

Do it at write time:

```
CpuUtil        = Δ(RemoteUserCpu + RemoteSysCpu) / SampleInterval     # cores
MemUtil        = MemoryUsage / RequestMemory                          # fraction (a HWM ratio)
BlockReadRate  = Δ(BlockReadBytes) / SampleInterval                   # B/s
GpuUtil        = (GPUsAverageUsage·t − GPUsAverageUsage_prev·t_prev) / SampleInterval
```

The rules that keep these honest all come out of §2.4:

- **Never differentiate across a `RunInstanceID` boundary** — including the counters §2.4 marks
  cumulative-across-runs (`CumulativeRemote*Cpu`, `BytesSent`/`BytesRecvd`, `CommittedTime`).
  Those were originally exempted, on the reasoning that their inputs do not restart so the delta
  is sound. The delta is; the *denominator* is not. The interval from a run's last sample to the
  next run's first spans however long the job sat idle in between, so a job that transferred 1 MB
  in 100s six hours after its previous run recorded 46 B/s — unmarked, and indistinguishable from
  a genuinely slow transfer. A rate averaged over time the job was not running is not a rate.
- **An interval may not span two different clocks.** `SampleTime` is the starter's when it reports
  stats and the ingest clock otherwise, and the difference between them is AP/EP skew rather than
  elapsed time: with the AP behind it can even go negative and silently drop every rate in the
  window. `prevObservation` records which clock it came from and a mixed interval derives nothing
  (counted as `clock_mix`).
- **Anchor a run on its observed zero.** The shadow writes `RemoteUserCpu = 0` at run start
  and it reaches the queue (§2.4 note 2). Treat the first observation of a `RunInstanceID`
  carrying `RemoteUserCpu == 0` as that run's t0. An observation carrying a *new*
  `RunInstanceID` and an inherited non-zero counter is pre-baseline (§2.4 note 3): record it
  with `SampleTrigger` intact and **no derived rates**, and do not use it as the delta base.
- **Rates are `undefined`, never zero**, when there is no valid predecessor.
  `time_bucket` already drops undefined rows out of a series (time-series.md §8), which
  renders as a gap — the correct picture. `SampleBaseline = true` marks those samples, so a gap
  is explainable from the data rather than being indistinguishable from a lost write.
- **An input missing from *either* observation yields no rate.** Missing from the current one is
  obvious (`CopyAttribute` deletes a starter-sourced attribute when an update omits it, so an
  input genuinely comes and goes mid-run). Missing from the *previous* one is the tempting case:
  reading "absent" as zero looks safe, since HTCondor does zero the per-run counters at run
  start. It is not. That predecessor is usually the run's spawn commit, and the interval from it
  spans input file transfer — time the job was not running — so the rate would be wrong even
  with a correct zero; and for a job first seen mid-run, absent does not mean zero at all and
  the invented baseline shows up as a spike. The cost of refusing is one rate-less sample per
  run. The cost of guessing is a fabricated number in a dashboard.
- **A counter going backwards means a reset we did not predict.** Emit the raw counters,
  leave the rates undefined, and count the occurrence in a metric so we can see whether §2.4
  missed a case. Same for an attribute that *disappears*: `CopyAttribute` deletes the
  target when the starter omits it, so undefined is a state that happens mid-run.
- **`SampleInterval` is elapsed wall time, not run time.** Subtract
  `Δ(CumulativeSuspensionTime)` before dividing, or a suspended interval reports a
  spuriously low rate.
- **Keep the raw counters.** Derived columns are a convenience; storing the inputs means a
  wrong derivation is fixable by recomputation rather than by data loss, and they compress
  to nearly nothing (monotone per job, and a job's observations are adjacent in a segment).

### 3.5 Indexes, zone maps, retention

```go
db.ArchiveConfig{
    SegmentSize:      2 << 20,   // 2 MiB -- deliberately SMALLER than the 8 MiB default; see below
    ZoneAttrs:        []string{"SampleTime", "ClusterId"},
    ValueAttrs:       []string{"ClusterId"},
    CategoricalAttrs: []string{"Owner"},          // + admin-configured extras
    Retention: collections.Retention{
        MaxBytes:   <HTCONDORDB_JOB_METRICS_MAX_BYTES>,
        MaxAgeAttr: "SampleTime",                 // must be a ZoneAttr
        MaxAge:     <HTCONDORDB_JOB_METRICS_MAX_AGE>,
    },
}
```

- **"Drop old segments when space fills" already exists.** `Retention.MaxBytes` /
  `MaxSegments` / `MaxAge` (`collections/archive.go:45-67`) are enforced by the periodic
  rotation pass the daemon already runs for `history` and `epoch_history`
  (`HTCONDORDB_ARCHIVE_ROTATE_INTERVAL`). Wire a per-table cap the same way
  `historyMaxBytes`/`epochMaxBytes` are wired (`cmd/htcondordb/scheddsync_manager.go:190-199`).
- **`SampleTime` must be a `ZoneAttr`** both for `MaxAge` (the doc comment requires it) and
  because it is what makes `WHERE SampleTime >= $__timeFrom()` prune whole segments.
- **Do NOT override the segment size.** This shipped as 2 MiB, on the argument that the active
  segment carries no sidecar and is rescanned in full, so a dashboard reading the newest data
  wants that window short. Measurement killed both halves of the argument. The read cost is a
  wash (last-hour panel: 686ms at 2 MiB, 718ms at 8 MiB, i.e. 4% in favour of the smaller
  segment), and storage is **not monotone** in segment size, with 2 MiB on the wrong side of it:

  | segment | bytes/record |
  |---|---|
  | 2 MiB | 425 |
  | **8 MiB (library default)** | **281** |
  | 32 MiB | 328 |
  | 64 MiB | 424 |

  So a 2 MiB segment bought a 4% faster query for a 1.5x storage bill. The override is gone.
  The U-shape is recorded as a measurement, not explained: it is not the locality story it first
  looked like, since the 64 MiB arm spans four sampling rounds and is as bad as the 2 MiB arm
  that spans an eighth of one. `TestJobMetricsSegmentSizeAB` fails if some other size ever wins,
  so the default gets revisited on evidence rather than drifting.

## 4. Where the sampler lives

### 4.1 Hook: the schedd-sync tailer, at commit

The samples cost **zero new polling**. `JobSync` already parses every `SetAttribute` the
schedd commits (`scheddsync/jobsync.go:1372`) and buffers each on-disk transaction into one
DB transaction. So:

- While applying, note any key whose transaction touched a usage attribute, and what kind of
  update it looked like (→ `SampleTrigger`).
- At `commitAll`, read those rows back from the jobs table — the row is now complete and
  merged with everything the sampler needs but the transaction did not carry (`Owner`,
  `RequestMemory`, `NumShadowStarts`) — build the flat sample ad, and `Append` it.

Reading back post-commit rather than assembling from the transaction is what makes the
sample complete without the sampler holding a second copy of the queue — the same design
choice `JobSync` itself makes (`jobsync.go:7-9`).

### 4.2 Idempotency: an identity constraint, not a synthesized key

A restart re-applies the log from the last durable position, re-producing samples that were
already appended. An archive record has no key to collide on, so the sampler asks the archive
whether it already holds this observation, and stops asking once one turns out to be new — the
self-limiting recovery dedup `HistorySync` uses (`scheddsync/historysync.go:40-45`). In steady
state the check costs nothing.

The identity is **(ClusterId, ProcId, RunInstanceID, SampleTime, SampleTrigger)**.

An earlier draft of this section proposed the exact log coordinate `(LogSeq, LogOffset)` instead,
which is a better key in principle: unique per commit, and invariant under where a read pass
happens to begin. It was dropped because `classadlog.Parser` exposes no per-entry offset —
`GetNextOffset` only advances on `Close`, so mid-pass it reports the offset the pass *started*
at, which shifts when a restart resumes from an earlier checkpoint. Making it exact means adding
an accessor to `golang-htcondor` and a release-chain bump for a self-contained feature. If that
accessor lands for another reason, switching is a small change and strictly better.

The residual weakness of the constraint: two commits of the *same trigger kind* between two
starter reports share an identity, so the second would be dropped on replay. That is bounded to
the few seconds of log a crash re-reads, and dropping a sample is a better error than
double-counting one. `LogSeq` is still recorded, as provenance — it says which log generation a
sample came from — but it is not part of the identity.

**A reconcile reload emits nothing.** After a compaction the tailer replays the whole log, which
would otherwise produce a duplicate sample for every running job. That falls out of structure
rather than a guard: the reconciler writes through its own batched transactions and never calls
`commitAll`, which is the only place samples are taken.

The sampler's steady-state memory is the previous observation per *running* job — bounded by
concurrency, not queue size, dropped at a run's endpoint and when the job leaves the queue, and
reconstructible from the stream. It is a cache, not a record: after a restart the first sample
per job is simply a baseline.

### 4.2a Only executing jobs are sampled

Not in the original sketch, and necessary. A job's attributes move for reasons that are not
observations — a submit writes `RequestMemory` and `Owner`, the schedd rewrites bookkeeping on
idle jobs — and sampling those would put points on a series at times the job was consuming
nothing, plus one useless record per job in a large submit. Two rules together:

- A **context** attribute (identity, axes, denominators) never triggers a sample. Only a usage
  attribute does.
- A sample is only built when the job has run (`NumShadowStarts` present) **and** is executing
  (status Running / Transferring Output / Suspended) — or the commit is terminal, which is the
  run's endpoint whatever status it leaves behind.

### 4.3 End of run comes free — `epoch_history` is a cross-check, not a dependency

Because `common_job_queue_attrs` is included in every update type (§2.2), the run's **final**
usage numbers reach the queue before the schedd destroys the ad. Streaming therefore gives us the
endpoint for free: the last sample of a `RunInstanceID` *is* the end-of-run record, tagged
`SampleTrigger == "terminal"`, and a short job that never survived a full
`SHADOW_QUEUE_UPDATE_INTERVAL` still gets at least one point.

**How the endpoint is actually detected, which is not what this section first assumed.** The
obvious rule — "the commit that sets `ExitCode` is the terminal one" — does not work, and the
integration test against a real schedd is what established that. A completion is spread across
several transactions, and the one carrying the terminal-looking attributes is *not* the one that
changes the status. Observed, in order:

| transaction | sets | `JobStatus` at that point |
|---|---|---|
| A | `ExitCode`, `ExitBySignal`, `MemoryUsage`, `CpusUsage` | still 2 (Running) |
| B, C | transfer timings, `CommittedTime`, `TerminationPending` | still 2 |
| D | `JobStatus 4`, `LastJobStatus 2`, `EnteredCurrentStatus` | 4 |
| E | `LastRemoteWallClockTime`, then DELETES `RemoteHost`, `ClaimId`, … | 4 |
| (loose) | `CurrentHosts 0`, `CompletionDate` | 4 |
| F | `TotalCompletedJobs`, then `DestroyClassAd` | — |

So no single commit is both terminal-looking and terminally-statused, and a rule requiring both
emits no endpoint at all. The reliable signal is the **transition**: the sampler was following the
run (it holds a predecessor for it) and the job is no longer executing. That is the run's last
sample whatever attribute triggered the commit, and by then the row has accumulated the final
numbers transactions A–C wrote. Two consequences worth stating:

- `CompletionDate` is *not* a terminal signal. The schedd writes it as `0` at **submit**, so only
  its value means anything, and treating its presence as an ending misclassifies every job.
- A terminal-looking attribute on a still-executing job (a vacate time from an earlier run, a
  hold reason being cleared) must **not** be labelled terminal, or a consumer filtering to
  terminal samples finds one mid-run and the "one endpoint per run" guarantee is false.

`epoch_history` remains the authoritative per-run record (it carries the disposition, exit
code, and attributes outside the whitelist) and it is keyed by the same
`(ClusterId, ProcId, RunInstanceID)` — but a resource plot no longer has to go get it. It is
worth using as a **consistency check**: if the terminal sample and the epoch ad disagree on
final CPU, something in the sampler is wrong, and that is a cheap test to write.

## 5. Configuration

Following the existing `HTCONDORDB_*` conventions (`cmd/htcondordb/scheddsync_manager.go`),
read from HTCondor config so a fleet manages it with puppet:

| Knob | Meaning | Default |
|---|---|---|
| `HTCONDORDB_JOB_METRICS` | enable the sampler | `false` |
| `HTCONDORDB_JOB_METRICS_MAX_BYTES` | size cap (drops oldest segments) | inherits `HTCONDORDB_ARCHIVE_MAX_BYTES` |
| `HTCONDORDB_JOB_METRICS_MAX_AGE` | age cap, against `SampleTime` | unset |
| `HTCONDORDB_JOB_METRICS_ATTRS` | **extra** attributes to record per sample | empty |
| `HTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS` | which of them get a categorical index | `Owner` |
| `HTCONDORDB_JOB_METRICS_SEGMENT_SIZE` | segment size (create-time only) | 2 MiB |
| `HTCONDORDB_JOB_METRICS_MIN_INTERVAL` | floor on sample spacing per job | 0 |

`_ATTRS` is the "let the admin specify additional attributes" hook, and it is worth more than
it looks because of §2.1: any chirp-set attribute and any CMR metric is already flowing
through `job_queue.log`, so naming it here is the entire cost of turning a user's custom
metric into a plottable series.

`_MIN_INTERVAL` matters more now that sampling is event-driven, because the trigger rate is
no longer bounded by a timer we control: a chirp-happy job can rewrite its row continuously
(`updateAttr` commits per attribute, §2.2). But it must be a **throttle, not a filter**:
a sample must never be suppressed when `JobStatus` or `RunInstanceID` changed, or when
`SampleTrigger == "terminal"`, or the endpoint guarantee in §4.3 evaporates and the state
transitions a plot annotates go missing. Suppress only redundant periodic/chirp samples.

**Indexing the extras matters.** An unindexed `GROUP BY` is a full scan of every segment the
zone maps cannot prune, and adding the index later costs a backfill that decompresses every
record once — the note already carried for the history archive
(`scheddsync_manager.go:93-99`) applies here with more force, because this table has far more
records.

## 6. Reading it — "plots out of the box"

Nothing new. The Phase-0 `time_bucket` path already works over archive tables
(`repl/archive_bucket_test.go`), and the Grafana backend already infers a timeseries frame
from `(time, label_*, metric_*)` columns (`grafana/pkg/plugin/frame.go`).

```sql
-- one user's memory high-water mark, per job, 5-minute buckets
SELECT time_bucket(SampleTime, '5m') AS time,
       ClusterId                     AS label_cluster,
       MAX(MemoryUsage)              AS metric_memory_mb
FROM job_metrics
WHERE Owner == "alice" AND SampleTime >= $__timeFrom() AND SampleTime <= $__timeTo()
GROUP BY time_bucket(SampleTime, '5m'), ClusterId

-- pool-wide CPU efficiency by project (the derived column earns its keep here)
SELECT time_bucket(SampleTime, '1h') AS time,
       ProjectName                   AS label_project,
       AVG(CpuUtil / RequestCpus)    AS metric_cpu_efficiency
FROM job_metrics
WHERE SampleTime >= $__timeFrom() AND RunInstanceID == 0
GROUP BY time_bucket(SampleTime, '1h'), ProjectName

-- right-sizing: how much of requested memory jobs actually used
-- (RunInstanceID == 0 per §2.4 note 1 -- later runs inherit the earlier peak)
SELECT Owner AS label_owner, AVG(MemoryUsage / RequestMemory) AS metric_mem_fraction
FROM job_metrics
WHERE SampleTime >= $__timeFrom() AND RunInstanceID == 0
GROUP BY Owner
```

That last one is the query that pays for the table. "Which users request 64 GB and use 4" is
currently answerable only from completed-job history — a day late, and only for jobs that
finished.

Two follow-ons, not blocking:
- A **materialized view over `job_metrics`** (pool-wide `AVG(CpuUtil)` by project, current
  bucket) exports to Prometheus for free through the existing `label_*`/`metric_*`
  convention (`metrics/metrics.go:29-31`) — the literal "poor man's Prometheus" endpoint.
- A **canned dashboard** shipped with the Grafana plugin, since "out of the box" means a
  dashboard exists, not that a query language exists.

## 7. Phasing

**Phase 0 — measure. ✅ DONE.** Note on where these live: the size, query-pruning and
segment-size measurements are **scale-gated** (`HTCONDORDB_SCALE=1`) and do not run in CI. Not
merely because they are slow — under `-race` they are 13x slower, and the first attempt at
running them in CI blew the job's 15-minute timeout — but because every number they produce
depends on production's shape, and a population small enough for CI stops describing any
deployment. What CI runs instead is the *structural* guard, which is cheap and arguably stronger:
is the record still columnar, is any field escaping to row form, did the rate columns make the
schema, and is a sample still under a byte ceiling. (`scheddsync/metrics_scale_test.go`; `HTCONDORDB_SCALE=1` for the
full size, a production-shaped subset in CI as a regression guard). It built its records through
the real sampler rather than hand-writing them, and it overturned two things this document
asserted: the per-record size (off by ~4x, §3.3) and the segment-size default (§3.5). The one
thing still unmeasured is the *observed trigger rate* — how much event-driven sampling adds over
the periodic floor — because that is a property of a real workload, not of a generator. It is
visible in production from `htcondordb_job_metrics_samples_total` broken down by trigger.

**Phase 1 — the table and the sampler. ✅ DONE.** `scheddsync/metrics.go` (the sampler),
hooks in `jobsync.go` (`applyEntry` notes, `commitAll` collects and appends, `abort` discards),
`MetricsStatus` on `SyncStatus`, `htcondordb_job_metrics_samples_total{outcome}` in `metrics/`,
and the `HTCONDORDB_JOB_METRICS*` knobs plus archive creation and retention in
`cmd/htcondordb/scheddsync_manager.go`. Tests in `scheddsync/metrics_test.go` and
`cmd/htcondordb/scheddsync_jobmetrics_test.go`, each mutation-checked against the rule it
covers.

**Phase 2 — reading.** A canned Grafana dashboard, a builder-mode "job metrics" preset, the
documented queries from §6, and the terminal-sample-vs-`epoch_history` consistency test
(§4.3).

**Phase 3 — the upstream asks** (§8), which improve the data but are not needed for the
table to be useful.

## 8. Upstream HTCondor changes worth asking for

Each is small and self-contained; none blocks the table. The first two are now the
high-value ones, because §2.4 showed the data loss is worse than the resolution loss.

1. **Publish instantaneous RSS. ✅ PATCH WRITTEN** (HTCondor branch `instantaneous-rss`, local,
   unpushed — upstream filing is the maintainer's to do). `ProcFamilyUsage.total_resident_set_size`
   is already re-summed per sample and discarded by the max in `vanilla_proc.cpp:766` and again in
   `remoteresource.cpp:1386-1391`. The patch publishes it as **`CurrentResidentSetSize`** (KiB):
   un-maxed in `VanillaProc::PublishUpdateAd`, forwarded as reported by the shadow, cleared per
   run in `setJobAd` (the `DiskUsage` precedent, not the memory one — so it cannot go stale across
   runs the way §2.4 note 1 describes), added to `common_job_queue_attrs`, and documented against
   `ResidentSetSize` so a reader knows which answers which question.

   The sampler already records it and derives `CurrentMemUtil` from it, so a pool that gains the
   attribute starts plotting real working-set curves with no change here — and on a pool that
   never does, an absent attribute is simply not written. That is the only memory column in the
   table that can go *down*.
2. **Publish a per-run memory high-water mark.** (Not written; §1 covers the more valuable half.) Today the HWM is carried across runs
   (`remoteresource.cpp:1179-1195`), so a rerun's real peak is unrecoverable (§2.4 note 1).
   A `<attr>ThisRun` companion — reset in `setJobAd` the way `DiskUsage` already is, twenty
   lines below — is additive and breaks no existing consumer.
3. **Publish `percent_cpu`.** `ProcFamilyUsage` carries it
   (`condor_procd/proc_family_io.h:85`) and nothing publishes it to the job ad.
4. **Peak-since-last-report variants.** A `Recent`-style *max* (`RecentResidentSetSizeMax`)
   would give a true per-interval peak — strictly better than either the lifetime max or an
   instantaneous spot reading, and it fits the starter's existing `generic_stats`
   recent-window machinery.
5. **Decouple the queue-update cadence from the whitelist.** A separate, shorter timer for
   *usage-only* attributes would let a site get 5-minute resolution without tripling the
   write rate of everything else on the whitelist (§2.2).

Worth a bug report regardless: `static unsigned int max_rss` in
`VanillaProc::PublishUpdateAd` (`vanilla_proc.cpp:766`) is a *function-level* static, so in a
starter running more than one proc the high-water mark is shared across them.

## 9. Open questions

- **Does a shadow *reconnect* bump `NumShadowStarts`?** If it does, one continuous execution
  splits into two `RunInstanceID`s while the starter's counters keep climbing — the seam would
  look like a run boundary where no reset happened, and §3.4's "never differentiate across a run
  boundary" would throw away a valid delta. The `inherited` counter
  (`htcondordb_job_metrics_samples_total{outcome="inherited"}`) is the instrument: it fires on
  exactly this shape, so a deployment answers the question in a day. Nothing to do until it does.
- **Scope beyond one AP.** `job_metrics` is per-AP, like the rest of schedd-sync. A pool-wide
  view is a question for the mirror/aggregation layer.
- **Do we want a delta on the high-water marks too?** "How much the ceiling rose this interval"
  localizes *when* a job grew. Cheap to add; unclear if anyone wants it.
- **`SampleTrigger` as a column vs. an index.** It is a string on every record and a natural
  thing to filter on (`!= "chirp"` for an even series). It is not categorically indexed today;
  whether that is worth an index depends on whether panels actually filter on it.

## 10. Where building it changed the design

Recorded because the reasons generalize, not for completeness:

1. **Idempotency moved from the log coordinate to an identity constraint** (§4.2). The exact
   coordinate would be better, and is unavailable without an upstream accessor. Documented rather
   than quietly downgraded, because the residual weakness is real.
2. **An executing-only gate had to be added** (§4.2a). The original sketch would have emitted a
   record per job on every submit and on every schedd touch of an idle job. The general shape:
   "an attribute changed" is not the same question as "the thing I am measuring changed".
3. **The strict missing-input rule** (§3.4). Writing the test forced the question of what an
   absent predecessor value means, and the answer — refuse, do not assume zero — costs one sample
   per run and prevents a fabricated spike.
4. **`TransferRate` split into `BytesSentRate`/`BytesRecvdRate`,** and later confined to a single
   run like every other rate (§3.4). A rate summed over two inputs
   needs *both* reported; a test that set only one showed the whole rate vanishing. Multi-input
   rates should be reserved for inputs HTCondor genuinely always writes together (CPU user+sys).
5. **The first measurement measured the wrong bytes.** It averaged over the whole table, which
   mixes columnarized segments with the row-form active one — a 25% error at the size CI runs,
   and the kind of mistake that is invisible because the number is merely somewhat too big rather
   than obviously wrong. Reporting coverage (`46/46 sealed`) and per-field escape rates alongside
   the total is what makes "is this actually columnar" answerable instead of assumed.
6. **The two measurements need different populations, and saying so is part of the design.** The
   size number is only meaningful at production locality (many jobs, few samples each, so a
   segment holds one sample from thousands of different jobs). The schema/columnarization number
   is only meaningful with enough samples per job for the rate columns to clear the presence
   threshold. A single population would have made one of the two tests quietly vacuous.
7. **The segment-size default was wrong, and so was the per-record estimate** (§3.5, §3.3). Both
   were argued rather than measured in the original sketch, and both survived code review and CI
   because nothing in either checks a number that only shows up at scale. The generalizable part
   is that the measurement had to reproduce production's *locality* to be worth anything: a
   benchmark that shrinks the job count instead of the sample count puts a job's consecutive
   samples in the same segment, where they compress beautifully and the guard it calibrates is
   useless.
8. **The endpoint rule was wrong, and only a real schedd showed it** (§4.3). Two successive
   versions were wrong in opposite directions: keying on the attribute name put a "terminal"
   sample mid-run on a job that was still going, and then requiring the status to agree in the
   *same commit* removed the endpoint entirely, because HTCondor sets `ExitCode` and `JobStatus`
   in different transactions. No amount of synthetic `job_queue.log` content would have found
   either, because the synthetic content encoded the same assumption the code did. The lesson is
   narrow and reusable: when a rule depends on how another system batches its writes, the test
   has to be that system.
9. **Three of the first tests passed for the wrong reason,** and mutation-checking each rule
   against the test that claimed to cover it is what found them: the run-boundary test was really
   exercising the backwards-counter guard (a seam delta is normally negative — it needs the case
   where the new run's counter has already *exceeded* the old one), the context-attribute test was
   really exercising the executing gate, and the abort test was exercising nothing at all.
