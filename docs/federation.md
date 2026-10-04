# Federation hub (`HTCONDORDB_FEDERATE_*`)

A federation hub is an htcondordb that fans the per-AP mirrors of many access points into one
catalog, so one query -- a dashboard aggregate, a `GROUP BY`, a paged job list -- covers every AP.

```
  AP 1:  schedd ── job_queue.log/history ──▶ htcondordb (spoke) ──┐
  AP 2:  schedd ── job_queue.log/history ──▶ htcondordb (spoke) ──┤  dbrpc Watch over CEDAR
   …                                                              ├───────────────────────▶ htcondordb (hub)
  AP N:  schedd ── job_queue.log/history ──▶ htcondordb (spoke) ──┘
```

- A **spoke** is the htcondordb already running [schedd sync](schedd-sync.md) next to its schedd.
  Its local store is the durable buffer: a hub outage or a WAN blip costs a resume, not a re-read
  of the schedd's files.
- The **hub** is a separate htcondordb (its own `HTCONDORDB_DIR`, normally its own local name). It
  runs one replication stream per (spoke, table) and writes the hub's tables in process.

A daemon is a hub *or* runs schedd sync, never both: both write `jobs`, `history` and
`syncstatus`, under different keys. A daemon configured for both refuses to start (and a reconfig
into both is refused, leaving whichever was running alone).

## Spoke side

Nothing to configure beyond schedd sync itself. With schedd sync on, a spoke:

- advertises `MirroredScheddName` (and `MirroredScheddAddress` when the schedd's address file is
  readable) in its collector ad and `DBSyncStatus` reply -- see
  [naming the mirrored schedd](schedd-sync.md#naming-the-mirrored-schedd);
- rewrites a heartbeat row in its `syncstatus` table every `HTCONDORDB_SYNCSTATUS_INTERVAL`
  seconds (default 5) -- see [the syncstatus heartbeat](schedd-sync.md#the-syncstatus-heartbeat).

The hub must be allowed to read the spoke: grant the hub's identity READ on the spoke
(`ALLOW_READ`). The hub dials an ordinary `DBSession`.

## Configuration

Enable a hub with either or both of:

```conf
# Discover the AP set from the collector (COLLECTOR_HOST).
HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT = regexp("^ap[0-9]+\\.example\\.org$", Name)

# Or name spokes explicitly. Static spokes skip host validation: the admin asserted them.
HTCONDORDB_FEDERATE_SPOKES = ap1 ap2
HTCONDORDB_FEDERATE_SPOKE_AP1_ADDRESS = <10.0.0.1:9620>
HTCONDORDB_FEDERATE_SPOKE_AP1_SCHEDD  = ap1.example.org
HTCONDORDB_FEDERATE_SPOKE_AP2_ADDRESS = <10.0.0.2:9620>
HTCONDORDB_FEDERATE_SPOKE_AP2_SCHEDD  = ap2.example.org
```

| Knob | Default | Meaning |
|------|---------|---------|
| `HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT` | unset | ScheddAd constraint defining the AP set. Requires `COLLECTOR_HOST`. |
| `HTCONDORDB_FEDERATE_SPOKES` | unset | Names of static spokes. Each needs `HTCONDORDB_FEDERATE_SPOKE_<NAME>_ADDRESS` (the spoke's command address) and may set `_SCHEDD` (the schedd it mirrors; default `<NAME>`). |
| `HTCONDORDB_FEDERATE_TABLES` | `jobs history syncstatus` | Spoke tables to fan in. `epoch_history` is also supported. Without `syncstatus` no source can be measured fresh. |
| `HTCONDORDB_FEDERATE_RETIRE_AFTER` | `7d` | How long a source may go unseen, or stay unmatched after the constraint changed, before its rows are deleted. Seconds, or a suffix (`36h`, `7d`). |
| `HTCONDORDB_FEDERATE_FRESH_SECONDS` | `60` | Staleness at or under this is `fresh`. |
| `HTCONDORDB_FEDERATE_DISCOVER_INTERVAL` | `60` | Seconds between collector discovery passes. |
| `HTCONDORDB_FEDERATE_STATE_INTERVAL` | `5` | Seconds between recomputations of source state (`federation_sources`, the collector ad summary, metrics). |

The hub's archives take the same tuning knobs as schedd sync's: `HTCONDORDB_ARCHIVE_MAX_BYTES`,
`HTCONDORDB_HISTORY_MAX_BYTES`, `HTCONDORDB_EPOCH_HISTORY_MAX_BYTES`,
`HTCONDORDB_ARCHIVE_SEGMENT_SIZE`, and extra index attributes in
`HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS` / `HTCONDORDB_ARCHIVE_VALUE_ATTRS`. A hub's history grows
with the sum of every AP's completions; **set a size cap**. A hub needs a persistent database
(`HTCONDORDB_DIR` or `SPOOL`): archives and resume cursors live on disk.

All of it is reapplied on `condor_reconfig`. A configuration change restarts the hub's streams;
each resumes from its cursor.

## Discovery

Every `HTCONDORDB_FEDERATE_DISCOVER_INTERVAL` the hub queries the collector for the ScheddAds
matching the constraint, for all ScheddAds (to tell "no longer matches" from "gone"), and for
`HTCondorDB` ads carrying `MirroredScheddName`. It pairs them by name:

- **Validation.** A spoke's claim is accepted only if the host in `MirroredScheddName` (the part
  after `@`) is the spoke's own host -- its address, its `alias`, or an address that name
  resolves to -- or the spoke shares a host with the schedd's advertised address. A spoke on
  another host claiming an AP is rejected and logged (`rejected_spokes_total{reason="host_mismatch"}`);
  a schedd whose every claimant was rejected is reported `untrusted`. Without this check a
  misconfigured or hostile spoke could publish rows under another AP's name. Static spokes skip it.
- **HA pairs.** Two valid spokes claiming one schedd: the one reporting `Syncing` and caught up
  wins; if both or neither are, the hub declines rather than guess (`reason="ha_tie"`).
- A failed collector query is not an empty AP set: the pass is skipped.

## Tables

| Hub table | Kind | From | Identity | Indexes |
|---|---|---|---|---|
| `jobs` | mutable | spoke `jobs` | (schedd, spoke key) | categorical `ScheddName`, `User`; value `JobStatus` |
| `syncstatus` | mutable | spoke `syncstatus` | schedd | -- |
| `history` | archive | spoke `history` | `GlobalJobId` | categorical `ScheddName`, `Owner`, `GlobalJobId`; value `ClusterId`; zones `CompletionDate`, `EnteredHistoryTime` |
| `epoch_history` | archive | spoke `epoch_history` | `GlobalJobId` + `RunInstanceID` + `EpochAdType` | as history; zones `EpochWriteDate`, `EnteredHistoryTime` |
| `federation_sources` | mutable | computed | schedd name (the key) | -- |

Every replicated row carries `ScheddName`, **overwritten** from the source's validated identity --
a row cannot claim another AP. Select an AP's rows with `ScheddName == "..."` and a job with
`ScheddName`, `ClusterId` and `ProcId`. The storage keys of the hub's mutable tables are an
internal encoding of (schedd, spoke key); it may change between releases (which would mean
rebuilding the hub from its spokes), so never parse or construct one. The spoke's own `Key`
attribute is carried through unchanged and is unique only within its AP.

`clusters`, `jobsets`, `users`, `header` and the other spoke-internal tables are not replicated:
`jobs` rows already carry their cluster's attributes, because the spoke chains them at write time.

## Guarantees

- **No collisions.** Two APs' job `123.0` are two rows; a delete from one AP never touches the
  other's.
- **No phantoms.** When a spoke restarts (its watch epoch changes) or the hub falls out of its
  delete journal, the spoke replays its whole table (a Reset). The hub records which rows the
  replay touched, writes only rows that differ from what it holds, and at the end of the replay
  deletes that AP's rows the replay did not include. An AP's rows never vanish mid-replay (no
  clear-then-replay), and a replay of an unchanged queue writes nothing. A replay cut off before
  it completes deletes nothing; the next one finishes the job.
- **No archive duplicates.** From the start of each session until the spoke says it is caught up
  -- a Reset replay, or the overlap a resume re-delivers -- each history/epoch record is checked
  against the hub's archive by its identity and dropped if present. Small catch-ups are checked
  record by record; a long one loads the AP's identities once and checks in memory. Live records
  are appended without a check. A record with no `GlobalJobId` (or, for epochs, no
  `RunInstanceID`) is appended unchecked and counted in `missing_identity_total`.
- **Bounded replay after a hub restart.** Writes are batched into one transaction per flush (about
  a second) and the resume cursor is committed only after that transaction. A hub crash re-applies
  at most the last flush window, idempotently.

## Freshness

Each spoke heartbeats into `syncstatus`; the hub stamps each new heartbeat with its own clock on
receipt (`HubReceivedTime`; a redelivered heartbeat keeps its first receipt time). Staleness uses
the hub's clock only, so clock skew between hosts never becomes staleness:

```
staleness = (hub_now - HubReceivedTime) + SpokeLagSeconds + HeartbeatIntervalSeconds
```

An idle AP keeps heartbeating and stays fresh; a stopped heartbeat is staleness that grows. No
heartbeat yet, or a spoke that cannot measure its own lag, is `stale` with no
`StalenessSeconds` -- never fresh.

## Membership is sticky

`federation_sources` holds one row per schedd: `State` (`fresh`, `stale`, `absent`, `untrusted`,
`retiring`), `Reason`, `StalenessSeconds`, `LastSeen` (the later of the last collector sighting
and the last live stream), `LastCollectorSeen`, `LastContact`, `LastReset`, `RetiringSince`,
`SpokeAddress`, `SpokeName`, `Static`, and per table `<Table>Connected`, `<Table>LastError`,
`<Table>Rows` (where an index can count them).

- A schedd missing from the collector is **absent**: its rows are kept and its streams keep
  running against the last validated spoke address. A collector restart makes every AP vanish at
  once; nothing is deleted for it.
- The table is persisted, so a hub restarting into an empty collector still knows every member and
  when it was last seen.
- A schedd still advertising but no longer matching a changed constraint, or a static spoke removed
  from the configuration, is **retiring**: its streams stop and its rows stay until
  `HTCONDORDB_FEDERATE_RETIRE_AFTER` has passed. Matching again before then cancels retirement.
- Rows are deleted only by **retirement**: `HTCONDORDB_FEDERATE_RETIRE_AFTER` after `LastSeen`
  (absent) or after `RetiringSince` (retiring), or at once with the admin command
  `.retire <schedd>` in `htcondordb-cli` (DAEMON). Retirement deletes the AP's rows from the
  mutable tables (`jobs`, `syncstatus`, `federation_sources`) and its cursors. **Archive rows are
  not deleted**: archives are append-only and age out with their retention. A retired AP that is
  still in the AP set comes back at the next discovery and is replayed from scratch.

## Monitoring

The hub's collector ad carries a summary only -- `FederationConstraint` (the constraint, with
static spokes OR'd in by name), `SourcesTotal`, `SourcesFresh`, `SourcesStale`, `SourcesAbsent`,
`SourcesRetiring`, `SourcesUntrusted`, `MaxSourceStaleness` (absent until some source has a measured
staleness). Per-source detail is in `federation_sources`, which a consumer can watch.

`/metrics` adds `htcondordb_federate_*`: `events_applied_total`, `identical_skips_total`,
`resets_total`, `dedup_hits_total`, `missing_identity_total`, `phantom_deletes_total` (by table),
`rejected_spokes_total` (by reason), `retired_total`, `source_staleness_seconds` (by schedd), and
`sources` (by state).

## What is not guaranteed

- **Hub tables are writable by WRITE-level clients.** A client write to a federated table would be
  overwritten or swept by the next replay, and could claim any `ScheddName`. A per-table write gate
  in dbrpc is pending; until then restrict `ALLOW_WRITE` on a hub to the hub itself.
- **Spoke chaining gaps are inherited.** The hub has no cluster ads to repair partial job rows from.
- **Replays are correct, not cheap.** Every spoke restart replays its tables (spoke watch epochs are
  per process). The hub writes almost nothing for an unchanged spoke, but it still streams and
  checks the spoke's whole retained history.
- **An OS crash on the hub** can lose archive appends that a committed cursor already covers
  (appends are not fsynced with the cursor); a process crash cannot.
- **Absent sources keep their last state.** `absent` says the collector does not list the AP; the
  rows are as fresh as `StalenessSeconds` says, no fresher.
- **Freshness needs `syncstatus`.** A spoke too old to heartbeat is always `stale`.
- Two hubs fed from the same spokes converge independently; there is no coordination between them.
