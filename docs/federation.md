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
into both is refused, leaving whichever was running alone). A reconfig from one to the other works.

The hub's tables -- each federated table and `federation_sources` -- are
[owned by the hub](authorization.md#tables-owned-by-an-in-process-writer): clients, DAEMON
included, may query and watch them but not write, delete or drop them, so no client can plant a
row under an AP's name or a spoke address for the hub to dial. `.rotate` and `.retention` on the
hub's archives still work (routed through the daemon); `.truncate` is refused -- use
`.retire <schedd>`.

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

# Or name spokes explicitly. Static spokes skip host validation: the admin asserted them. Use them
# for spokes behind CCB or NAT, and on networks where collector ads cannot be trusted.
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
each resumes from its cursor. A change to security settings alone (`SEC_*`) does not restart them:
each spoke session reads them when it connects, so it applies from the next reconnect.

## Discovery

Every `HTCONDORDB_FEDERATE_DISCOVER_INTERVAL` the hub queries the collector for the ScheddAds
matching the constraint, for all ScheddAds (to tell "no longer matches" from "gone"), and for
`HTCondorDB` ads carrying `MirroredScheddName`. It pairs them by name:

- **Validation.** A spoke's claim is accepted only if the host of the spoke's *primary* address
  (`MyAddress`'s host:port, not its `alias=` or `addrs=` parameters) is the schedd's host -- the
  part of its `Name` after `@` -- by name, or is an address the hub's resolver returns for that
  name. A spoke on another host claiming an AP is rejected and logged
  (`rejected_spokes_total{reason="host_mismatch"}`); a schedd whose every claimant was rejected is
  reported `untrusted`. Static spokes skip it.

  This is a guard against misconfiguration -- a spoke copied to another host still claiming its
  old AP -- **not authentication**. Every input is a collector ad, written by whoever advertised
  it: a host that may advertise to the collector can claim to be any spoke. Alias and `addrs`
  parameters are not consulted because the spoke writes them itself, and an address shared with the
  schedd's is not consulted because private addresses repeat across hosts (two APs behind
  different NATs can both be `172.17.0.2`). Consequences:
  - A spoke reached only through CCB or NAT, or whose primary address is not what its AP's name
    resolves to on the hub, fails validation: pair it statically (`HTCONDORDB_FEDERATE_SPOKES`).
  - On a network where advertising cannot be trusted, use static spokes only (no constraint), and
    restrict who may advertise to the collector (`ALLOW_ADVERTISE_*` / `ALLOW_DAEMON` on the
    collector).
- **HA pairs.** Two valid spokes claiming one schedd: the one reporting `Syncing` and caught up
  wins; if both or neither are, the hub declines rather than guess (`reason="ha_tie"`).
- **Validated pairings are sticky.** `untrusted` (every claimant rejected) and a declined HA tie
  apply only to a schedd with no previously validated spoke. A schedd that had one keeps streaming
  from that address -- a rejected impostor or a tie does not stop an established stream; only a new
  *valid* claim moves it. To force a re-pairing, `.retire` the schedd.
- `rejected_spokes_total` counts rejections per discovery pass: one misconfigured spoke adds one
  every `HTCONDORDB_FEDERATE_DISCOVER_INTERVAL`, so read it as a rate, not a count of spokes.
- A failed collector query is not an empty AP set: the constraint's part of the pass is skipped.
  Static spokes are configuration, not collector data: they are federated whether or not the
  collector answers.

## Tables

| Hub table | Kind | From | Identity | Indexes |
|---|---|---|---|---|
| `jobs` | mutable | spoke `jobs` | (schedd, spoke key) | categorical `ScheddName`, `User`; value `JobStatus` |
| `syncstatus` | mutable | spoke `syncstatus` | schedd | -- |
| `history` | archive | spoke `history` | `GlobalJobId` | categorical `ScheddName`, `Owner`, `User`, `GlobalJobId`; value `ClusterId`; zones `CompletionDate`, `EnteredHistoryTime` |
| `epoch_history` | archive | spoke `epoch_history` | `GlobalJobId` + `RunInstanceID` + `EpochAdType` | as history; zones `EpochWriteDate`, `EnteredHistoryTime` |
| `federation_sources` | mutable | computed | schedd name, lowercased (the key) | -- |

Every replicated row carries `ScheddName`, **overwritten** from the source's validated identity --
a row cannot claim another AP. Select an AP's rows with `ScheddName == "..."` and a job with
`ScheddName`, `ClusterId` and `ProcId`. A schedd's identity is case-insensitive, as in HTCondor:
two spellings of one schedd name are one AP -- one source, one `federation_sources` row (keyed by
the lowercased name; `ScheddName` keeps the spelling the hub first saw), one set of cursors.

A hub upgraded from a release that keyed sources by spelling merges the rows of an AP known under
several spellings into one at startup, and drops cursors it kept under a capitalised spelling: such
an AP replays once (correct -- the replay reconciles -- but a full replay).

The storage keys of the hub's mutable tables are an internal encoding of (schedd, spoke key); it
may change between releases (which would mean rebuilding the hub from its spokes), so never parse
or construct one. The spoke's own `Key`
attribute is carried through unchanged and is unique only within its AP.

`clusters`, `jobsets`, `users`, `header` and the other spoke-internal tables are not replicated:
`jobs` rows already carry their cluster's attributes, because the spoke chains them at write time.

## Guarantees

- **No collisions.** Two APs' job `123.0` are two rows; a delete from one AP never touches the
  other's.
- **No phantoms.** When a spoke restarts (a mutable table's watch epoch changes with every spoke
  process) or the hub falls out of its delete journal, the spoke replays its whole table (a
  Reset). The hub records which rows the
  replay touched, writes only rows that differ from what it holds, and at the end of the replay
  deletes that AP's rows the replay did not include. An AP's rows never vanish mid-replay (no
  clear-then-replay), and a replay of an unchanged queue writes nothing. A Reset clears the AP's
  resume cursor before the replay is applied, so a replay cut off before it completes deletes
  nothing and the next session replays in full and finishes the job. A replayed row whose ad
  cannot be decoded counts as delivered: the hub keeps the row it holds (`undecodable_total`).
- **No archive duplicates.** From the start of each session until the spoke says it is caught up
  -- a Reset replay, or the overlap a resume re-delivers -- each history/epoch record is checked against what the hub held before the session, by identity
  and by count. An identity is not unique (a run instance writes a CHECKPOINT epoch record per
  checkpoint and a COMMON one per common-files group), so if the hub held k records of an identity,
  the first k replayed are dropped and the rest appended. A resume's overlap is checked record by
  record; a Reset replay past 256 records loads the AP's identity counts once and checks in
  memory. Live records
  are appended without a check. A record with no `GlobalJobId` (or, for epochs, no
  `RunInstanceID`) is appended unchecked and counted in `missing_identity_total`.
- **Bounded replay after a hub restart, no gap after a crash.** Writes are batched into one
  durable transaction per flush (about a second); archive appends are durable when they return.
  The resume cursor is written (and fsynced) only after the data it covers is durable, so neither a
  process nor an OS crash can leave a committed cursor covering lost data. A crash re-applies at
  most the last flush window, idempotently. A batch that fails to commit is never covered by a
  saved cursor: the stream stops with the error (`<Table>LastError`), backs off, and re-delivers
  the batch on reconnect; a write that keeps failing stalls the stream instead of being skipped.
- **Replays respect the hub's cap.** When a spoke restart replays into a hub archive that is
  size-capped and at its cap (or age-capped), records older than the oldest the hub still holds
  for that AP (by `EnteredHistoryTime`, `EpochWriteDate` for epochs) are skipped
  (`below_retention_total`): the cap already dropped them, and re-appending them would evict other
  APs' recent history.

## Freshness

Each spoke heartbeats into `syncstatus`; the hub stamps each new live heartbeat with its own clock on
receipt (`HubReceivedTime`; a redelivered heartbeat keeps its first receipt time; a heartbeat first
seen during catch-up -- a replay or a resume's overlap -- is not stamped, so the AP is stale until
the next live heartbeat). Staleness uses
the hub's clock only, so clock skew between hosts never becomes staleness:

```
staleness = (hub_now - HubReceivedTime) + SpokeLagSeconds + HeartbeatIntervalSeconds
```

`SpokeLagSeconds` grows while the spoke is behind its schedd (it counts from the last time the
spoke was caught up, not from its last read pass), so a spoke replaying a large backlog is stale
however promptly it heartbeats. An idle AP keeps heartbeating and stays fresh; a stopped heartbeat
is staleness that grows. No heartbeat yet, or a spoke that cannot state its lag (one that restarted
behind and has not caught up), is `stale` with no `StalenessSeconds` -- never fresh. So is a
heartbeat received "in the future" (the hub's clock stepped backwards since) until the clock passes
the receipt time again.

## Membership is sticky

`federation_sources` holds one row per schedd: `State` (`fresh`, `stale`, `absent`, `untrusted`,
`retiring`), `Reason`, `StalenessSeconds`, `LastSeen` (the later of the last collector sighting
and the last live stream), `LastCollectorSeen`, `LastContact`, `LastReset`, `RetiringSince`,
`SpokeAddress`, `SpokeName`, `Static`, and per table `<Table>Connected`, `<Table>LastError`,
`<Table>Rows` (where an index can count them).

A row is rewritten as soon as anything but its clock-driven fields changes -- `State`, `Reason`, a
stream connecting or failing, a row count. `StalenessSeconds`, `LastSeen`, `LastCollectorSeen` and
`LastContact` move on every state pass, so on their own they are refreshed at most once a minute:
a watcher of `federation_sources` sees state changes promptly without an event per source every few
seconds. The collector ad's summary and `/metrics` carry the live values.

- A schedd missing from the collector is **absent**: its rows are kept and its streams keep
  running against the last validated spoke address. A collector restart makes every AP vanish at
  once; nothing is deleted for it.
- The table is persisted, so a hub restarting into an empty collector still knows every member and
  when it was last seen. Nothing is retired before the restarted hub's first discovery pass. A
  restarted hub resumes streaming from a persisted `SpokeAddress` only if it is a configured static
  spoke or passes host validation again (the persisted `Static` flag is not trusted); otherwise the
  source is `untrusted` and streams nothing until discovery pairs it with a spoke again.
- A schedd still advertising but no longer matching a changed constraint, or a static spoke removed
  from the configuration, is **retiring**: its streams stop and its rows stay until
  `HTCONDORDB_FEDERATE_RETIRE_AFTER` has passed. Matching again before then cancels retirement.
- Rows are deleted only by **retirement**: once an absent source has gone unseen for
  `HTCONDORDB_FEDERATE_RETIRE_AFTER` *while the hub was running* (time the hub was down does not
  count, so after a hub restart the delay starts over from the restart; and a spoke the hub's
  streams reach is seen, whatever the collector says), `HTCONDORDB_FEDERATE_RETIRE_AFTER` after
  `RetiringSince` (retiring), or at once with the admin command
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
`resets_total`, `dedup_hits_total`, `missing_identity_total`, `phantom_deletes_total`,
`undecodable_total`, `below_retention_total` (by table), `rejected_spokes_total` (by reason:
`host_mismatch`, `ha_tie`), `retired_total`, `source_staleness_seconds` (by schedd), and `sources`
(by state). The series are registered on every htcondordb, so a daemon can become a hub on
reconfig; on one that is not a hub they stay at zero or absent.

## What is not guaranteed

- **Spoke chaining gaps are inherited.** The hub has no cluster ads to repair partial job rows from.
- **Replays are correct, not cheap.** Every spoke restart replays its mutable tables (`jobs`,
  `syncstatus`: their watch epochs are per process). An archive (`history`, `epoch_history`) keeps
  its epoch across a clean spoke restart and resumes; a spoke that crashed or was killed replays it
  too. The hub writes almost nothing for an unchanged spoke, but a history replay still streams and
  checks the spoke's whole retained history.
- **Archive ingest pays one msync per record.** classad's archive has no non-durable append and no
  batch sync, so the hub cannot defer durability to the once-per-flush point it uses for tables.
- **Absent sources keep their last state.** `absent` says the collector does not list the AP; the
  rows are as fresh as `StalenessSeconds` says, no fresher.
- **Freshness needs `syncstatus`.** A spoke too old to heartbeat is always `stale`.
- Two hubs fed from the same spokes converge independently; there is no coordination between them.
