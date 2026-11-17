# Backup and restore

htcondordb has two kinds of table and only one of them has a built-in backup
mechanism. Read [What is covered](#what-is-covered) before designing a backup
regime: a snapshot that looks like a whole-database backup is not one.

## What is covered

The daemon's snapshot mechanism writes a single `.cadb` file holding the ads of
every **mutable table** — the `jobs` table and its schedd-sync siblings, and any
table you created yourself.

| | In a `.cadb` snapshot | How to back it up |
|---|---|---|
| Mutable table ads (incl. private attributes) | **yes** | snapshot |
| Internal sync idempotency markers | **yes** | snapshot |
| **Archive tables** (`history`, `epoch_history`, any append-only table) | **no** | a continuous export — see [Backing up archives](#backing-up-archives) |
| Per-table index config, hot set, encrypted-attribute policy, time-travel and delta settings | no | file-level copy of the database directory |
| Materialized view definitions | no | file-level copy (view *data* is rebuilt from the base table at startup, so it never needs backing up) |
| Exporter definitions and their resume cursors | no | file-level copy |
| Schedd-sync, CEDAR-sync and history-import cursors | no | file-level copy |
| Raft log and membership (`consistent` mode) | no | file-level copy, or rebuild by rejoining the cluster |
| The pool signing keys that decrypt an encrypted backup | no | back up `SEC_PASSWORD_DIRECTORY` — see [Keys and escrow](#keys-and-escrow) |

### The archive gap

**Archive tables are not in a snapshot at all.** The catalog's snapshot walks the
mutable tables only; archives are a separate registry and are skipped silently —
no warning, no error, no empty section in the file. A `.cadb` taken from a
schedd-sync deployment therefore contains the live queue and *none* of the
completed-job history, which on a typical deployment is the overwhelming majority
of the data and the part that cannot be regenerated.

The same applies to the raft snapshots taken in `consistent` mode: they are built
from the same catalog snapshot, so a node that joins a raft cluster and restores
from a leader snapshot comes up with the mutable tables and an empty history. In
every replicated mode, archive data is node-local and is never replicated.

Archives need their own procedure, and it does not involve downtime: run a
continuous export. See [Backing up archives](#backing-up-archives).

## Configuration

Periodic snapshots are **off by default**. Nothing is backed up until you set
`HTCONDORDB_SNAPSHOT_DIR` and `HTCONDORDB_SNAPSHOT_INTERVAL`.

| Knob | Default | Meaning |
|------|---------|---------|
| `HTCONDORDB_SNAPSHOT_DIR` | — | Directory for periodic snapshots. Unset disables them. Created at startup (mode `0750`) if absent. |
| `HTCONDORDB_SNAPSHOT_INTERVAL` | — | Seconds between snapshots. Unset or `0` disables them, even with a directory set. |
| `HTCONDORDB_SNAPSHOT_KEEP` | `7` | How many of the most recent snapshot files to keep in that directory. `0` or negative is treated as the default; there is no "keep everything" setting. |
| `HTCONDORDB_RESTORE_FILE` | — | One-shot restore trigger, read at startup. See [Restoring](#restoring). |
| `HTCONDORDB_ENCRYPT_AT_REST` | `false` | Encrypt the store, and with it every snapshot, under the pool signing keys. |
| `HTCONDORDB_ENCRYPT_ATTRS` | — | Extra attributes to encrypt beyond the always-encrypted private ones. |

A minimal regime:

```
HTCONDORDB_SNAPSHOT_DIR      = $(SPOOL)/htcondordb-backups
HTCONDORDB_SNAPSHOT_INTERVAL = 3600
HTCONDORDB_SNAPSHOT_KEEP     = 24
```

Snapshots are written as `htcondordb-<UTC timestamp>.cadb`, for example
`htcondordb-20260411T030000Z.cadb`. The stamp is the file's only ordering key:
pruning sorts the names lexically and deletes all but the newest
`HTCONDORDB_SNAPSHOT_KEEP`. **Anything else you leave in that directory matching
`htcondordb-*.cadb` is a pruning candidate**, so keep your long-term copies
somewhere else — the snapshot directory is working space, not an archive.

Each file is written to `<name>.cadb.tmp` and renamed into place, so a reader
never sees a partial file and a crashed snapshot leaves no `.cadb` behind. The
`.tmp` file is removed on failure.

In a replicated deployment every node snapshots its own copy independently; the
setting is node-local, and a follower's snapshot is sealed under that node's own
keys.

## Taking a backup

### Live tables

Set the knobs above and the daemon does it. There is **no command to take a
snapshot on demand** — no CLI subcommand, no REPL dot-command, no signal, and no
administrative RPC that covers the whole catalog. If you need an ad-hoc backup,
your options are:

- Wait for the next periodic snapshot, or temporarily lower
  `HTCONDORDB_SNAPSHOT_INTERVAL` and `condor_reconfig`.
- Stop the daemon and copy the database directory (see below). This is the only
  way to get a genuinely point-in-time copy, and the only way to capture archives
  and per-table configuration at the same moment.

**A snapshot is not a point-in-time image.** It is taken online, and that is
deliberate: it does not block writers, so an hourly backup of a busy queue costs
nothing in availability. The price is that it is not consistent against the
writers either. Within a single table, each internal shard is captured
consistently but the shards are not captured together; across tables, each table
is a separate capture taken as the writer reaches it. A relationship written
across two tables while the snapshot runs can come back half-present, and a
single table can mix pre- and post-write state across shards. Treat a `.cadb` as
a crash-consistent image of a moving database — the same class of guarantee you
get from restoring after a power loss — not as a transactional one. If you need
a true point-in-time copy, take a filesystem snapshot (or, failing that, stop the
daemon).

Copy finished snapshots off the machine as part of the same job that would copy
any other spool data. A snapshot sitting next to the database it came from
protects you against nothing except operator error.

### On demand

`.backup` in `htcondordb-cli` writes a snapshot immediately, instead of waiting
for the next tick of the periodic timer. Take one before an upgrade, before a
configuration change, and after an incident.

```
htcondordb-cli -e '.backup'
wrote /var/lib/condor/spool/htcondordb-backups/htcondordb-20261116T143012Z.cadb (48213504 bytes) in 3.2s
sha256 9f2c...e41a
tables: jobs, users, syncstatus
NOT in this snapshot (archives -- back these up separately): history, epoch_history
```

With no argument it writes into `HTCONDORDB_SNAPSHOT_DIR`, using the same naming
as the periodic snapshots, so it sorts and prunes alongside them. Pass an
absolute path to write somewhere else; that file is outside retention and is
never pruned for you.

Needs `DAEMON`. One backup runs at a time: a second request while one is in
flight is refused rather than queued.

### Verifying a snapshot

Every snapshot written from this version on gets a sidecar manifest beside it
(`<name>.cadb.manifest`) holding its size, a sha256 over the whole file, the
tables it contains and the archives it does not. `.verify` checks a snapshot
against it — size first, which catches a truncated copy immediately, then the
digest, which catches everything else.

```
htcondordb-cli -e '.verify'
ok           htcondordb-20261116T143012Z.cadb (48213504 bytes)
DAMAGED      htcondordb-20261116T130012Z.cadb: checksum mismatch: the file's contents are not what was written
unverifiable htcondordb-20261115T000000Z.cadb: no manifest beside this snapshot
3 checked, 1 ok, 1 damaged, 1 unverifiable
```

With no argument it checks every snapshot in the configured directory; pass an
absolute path to check one.

**Unverifiable is not damaged.** A snapshot written before manifests existed is
intact — it simply cannot be checked this way. Only `DAMAGED` means the bytes on
disk are not the bytes that were written.

The manifest is a sidecar rather than part of the `.cadb` on purpose: the
snapshot format is unchanged in both directions, so a snapshot written here still
restores on an older daemon.

A verify is not a test restore. It proves the file is the file that was written;
it does not prove the daemon can load it. For anything you are relying on, do the
restore into a throwaway daemon described under [Verifying](#verifying).

### Backing up archives

Archives are not in snapshots, so they need their own regime. There are two
kinds of archive backup and they answer different questions:

| | What it is | Downtime | Restores by |
|---|---|---|---|
| **Continuous export** (below) | every record, streamed out as it arrives | none | replaying records |
| **Directory copy** | a point-in-time image of the archive's files | none to full, depending on method | putting the files back |

**Run the continuous export.** It is the only archive backup that needs no
downtime, and the only one that cannot be ruined by damage to the store's own
structure: every record in it is an independent ClassAd in text, with each
attribute written out in full and no reference to a shared compression
dictionary. If a dictionary goes missing and strands a run of segments, a
directory copy taken afterwards faithfully preserves the damage; an exported
record has nothing to lose.

Add a directory copy on top if you want to be able to restore an archive in one
move, with its indexes and configuration intact, rather than paying for an
ingest.

#### Continuous export: the primary path

**1. Let the daemon manage exporters.** It does by default. The binary is found
on `PATH`; point at it explicitly if it is not there, and give it an
unprivileged account to run as:

```
# HTCONDORDB_MANAGE_EXPORTERS = true     # the default; false disables all exporters
# HTCONDORDB_EXPORTER_DROPBOX_BINARY = /usr/libexec/condor/htcondordb-plugin-dropbox
HTCONDORDB_EXPORTER_DROPBOX_USER = condor
```

**2. Create one exporter per archive.** This registers it in the database; the
daemon notices and starts it, with no restart needed:

```
htcondordb-plugin-dropbox create -name history-backup -table history \
    -dir /var/spool/htcondordb-dropbox/history \
    -container frames -max-bytes 8GiB

htcondordb-plugin-dropbox create -name epoch-backup -table epoch_history \
    -dir /var/spool/htcondordb-dropbox/epoch_history \
    -container frames -max-bytes 8GiB
```

`-container frames` is worth setting here even though it is not the default:
nothing but `htcondordb-restore-dropbox` reads these directories, and a framed batch loses
one frame to a bad byte where a tarball loses the rest of the batch. Leave it off
only if something else of yours consumes the dropbox and expects `.tar.gz`.

`-max-bytes` is the backpressure ceiling — see step 4, which is the step people
skip.

**3. Check it is running.**

```
htcondordb-cli -e '.exporters'
htcondordb-cli -e '.exporter history-backup'
```

Batches appear in the dropbox as `batch-<seq>-<time>.cadf` (or `.tar.gz`), each
with a `.manifest` beside it. A batch is rolled every 2000 records or every 10
minutes, whichever comes first, so a record is durable in the dropbox within
about ten minutes of being written. `-roll-jobs` and `-roll-interval` change
that.

**4. Drain the dropbox. This is not optional.**

The dropbox is a staging area, not the backup. Nothing in htcondordb removes
batches from it — that is the consumer's job, and if you do not write one, the
dropbox fills to `-max-bytes`, the exporter stops advancing, and records
eventually age out of the archive's retention **before they are ever exported**.
You get a `loss-*.ad` report saying which window was lost, after the fact.

A cron job moving finished batches to wherever your backups actually live is
enough:

```
# every 15 minutes
rsync -a --remove-source-files --exclude='.*' \
    /var/spool/htcondordb-dropbox/history/ \
    backup-host:/backups/htcondordb/history/
```

`--exclude='.*'` is what matters: a dot-prefixed file is a batch the exporter is
still writing. Finished batches and their manifests are never dot-prefixed, so
this moves whole, complete batches and leaves partial ones alone.

Watch the dropbox size. A dropbox that is growing is a consumer that has stopped:

```
du -sh /var/spool/htcondordb-dropbox/*
ls /var/spool/htcondordb-dropbox/history/loss-*.ad 2>/dev/null   # any file here means lost records
```

**5. Verify the backup.** Each batch carries a manifest naming the records it
holds and a digest of its bytes, so the backup can be checked where it lives,
without a database:

```
htcondordb-restore-dropbox -verify -dir /backups/htcondordb/history
```

It writes nothing and exits non-zero if any batch is damaged or yields fewer
records than it declares, so it belongs in cron next to the drain job.

**6. Restore.**

```
htcondordb-restore-dropbox -dir /backups/htcondordb/history -table history
```

Re-runnable: records dedup on `GlobalJobId`, so the exporter's at-least-once
duplicates and a re-run of a restore that died halfway are both harmless.
`-dry-run` reports what would be written without needing the database reachable
at all. Read the top of its report — `DATA LOSS`, `INCOMPLETE`, `DAMAGED BATCH`
and `SHORT READ` each mean records you do not have, and the totals below them
will not tell you.

**What this gives you, and what it does not.** No downtime, every record, and an
artifact that survives damage to the store. What it is not is an image of the
archive: restoring replays records into a table, so it costs an ingest and it
does not bring back that archive's indexes, hot set, retention bounds or
encrypted-attribute policy. Those live in the database directory — back that up
too, or be ready to re-apply them.

#### Directory copy: a point-in-time image

Archives live under the database directory (`HTCONDORDB_DIR`, by default
`$(SPOOL)/htcondordb`):

```
$(SPOOL)/htcondordb/
├── tables/<name>/            mutable tables (also covered by snapshots)
├── archives/<name>/          archive tables — NOT covered by snapshots
├── views/                    materialized view definitions
├── exporters/                exporter definitions and resume state
├── scheddsync/, cedarsync/   sync cursors
├── replica_cursors/          leader-follower replication cursors
└── raft/                     raft log and membership (consistent mode)
```

Each archive directory is self-contained: its segment data files, their sidecar
indexes, the compression dictionaries, the persisted archive configuration, and
its own wrapped master key. The only thing it needs from outside is the pool
signing keys that unwrap that master key.

**Filesystem snapshot — the way to do this online.** If the database directory
sits on LVM, ZFS, btrfs or anything else that can take an atomic point-in-time
snapshot, take one and copy the archive directories out of it. That is a
crash-consistent image for free, with no downtime and none of the caveats below;
the daemon recovers from it exactly as it recovers from a crash. If you want
directory copies, arrange for this.

**Plain copy of a live directory.** Without a filesystem snapshot, a copy taken
while the daemon runs is still usable, with caveats you need to understand.
Segment data files are immutable once sealed, so the overwhelming majority of an
archive does not change under a copying process. Three things do move:

- *The active segment* is being appended to. A copy catches a partial record at
  its end. Recovery handles this: the tail walk stops at the first record that
  does not verify, so the restored archive ends shortly before the copy did.
  This is the ordinary shape of the loss and it is bounded by one segment.
- *Retention* unlinks whole sealed segments once they fall outside the archive's
  age or size bounds. A copy that misses a segment being dropped is fine — the
  data was going away anyway.
- *Merging* replaces a run of adjacent sealed segments with one larger file, then
  unlinks the sources. This is the only maintenance that rewrites archive data,
  and it is the one that can damage a plain hot copy: between the merged file
  being renamed into place and the sources being unlinked, both exist, and a copy
  that captures both restores an archive with **duplicated records**. The merge
  leaves an intent marker on disk that recovery uses to finish the job, so a copy
  that includes the marker resolves correctly — but whether your copy tool catches
  the marker is luck, not a guarantee.

**There is no configuration that quiesces archive maintenance.** Setting
`HTCONDORDB_ARCHIVE_ROTATE_INTERVAL = 0` stops the dedicated retention sweep but
not the general background maintenance pass, which rotates and merges on its own
cadence and has no knob.

What makes a plain hot copy acceptable in practice is that merging does not start
until an archive holds roughly ten thousand sealed segments. Below that watermark
a merge pass is a segment count and a comparison, and nothing is rewritten — so
the only moving part is the active segment's tail. Check before you rely on it:

```
# interactive shell
.stats history          # look at the "segments:" line

# or from the Prometheus endpoint (HTCONDORDB_METRICS_ADDRESS)
htcondordb_segments{table="history"}
```

A few thousand segments: a plain hot copy is sound. Approaching or past ten
thousand: use a filesystem snapshot.

A plain hot copy guarantees: every record sealed before the copy started, minus a
possibly truncated final segment, provided no merge ran during it. It does not
guarantee a copy consistent with the mutable tables taken at the same time.

**Stopping the daemon.** A cold copy is the only directory copy that is
point-in-time and internally consistent by construction:

```
condor_off -daemon HTCONDORDB
# wait for the process to exit; the daemon logs "store closed" when it is done
tar -C $(SPOOL)/htcondordb -cf - archives/ | ...
condor_on -daemon HTCONDORDB
```

Shutdown waits for any in-flight maintenance pass to reach a safe boundary, so on
a large archive this is not instantaneous — wait for `store closed` in the log
rather than for the command to return, and do not start copying until the process
is gone.

This is here for completeness, not as a recommendation. **Do not build a backup
regime that requires taking the database down.** Use the continuous export for
the data and a filesystem snapshot if you also want an image; reach for a cold
copy only for a one-off, such as cloning a store before an upgrade you intend to
be able to undo.

Restoring from either kind of backup is in
[Restoring archives](#restoring-archives).
### Everything else

The per-table index configuration, hot sets, encrypted-attribute policy,
retention bounds, view and exporter definitions, and sync cursors are files in
the database directory and are in no snapshot. If you only ever restore into a
*running* catalog whose tables already exist, this does not matter — a restore
replaces ads and leaves table configuration alone. If you are rebuilding a host
from nothing, a snapshot alone gives you the data with default indexes, default
hot sets, no views, no exporters, and sync cursors reset. Back up the database
directory as well as the snapshots, or be prepared to re-apply that configuration
by hand.

## Keys and escrow

With `HTCONDORDB_ENCRYPT_AT_REST` off, snapshots are written in the clear.
Protect them with filesystem permissions: a snapshot carries private attributes
— claim ids, capabilities, transfer keys — that a READ-level client never sees.

With it on, every snapshot is encrypted and there is a key hierarchy between you
and your data:

1. **Pool keys** — the HTCondor pool signing keys in `SEC_PASSWORD_DIRECTORY`,
   the same keys used for token signing. Any one of them opens the database.
2. **A per-table master key**, stored wrapped by the pool keys inside each
   table's or archive's own directory.
3. **A per-table backup key**, derived from that master. It unwraps a snapshot's
   key and cannot read the live store.
4. **A per-snapshot key**, minted fresh for each backup, which seals that
   backup's body and nothing else.

Each snapshot embeds the master-key envelope and the wrapped per-snapshot key, so
a backup can be restored by any node holding a pool key — including a different
host, with no access to the original database.

**The operational rule: back up `SEC_PASSWORD_DIRECTORY`, separately from the
snapshots, and keep it.** It is small and it changes rarely. Losing it while your
only copy of the data is an encrypted snapshot means the data is gone; there is
no recovery path and no override. Rotating a pool key in is safe — the new key is
added to each table's envelope at the next daemon start and old backups stay
readable under the keys that were present when they were taken — but *removing*
every key a backup was sealed under makes that backup unrecoverable.

### What escrow buys you

A per-table backup key can be exported in hex and escrowed separately, so a
backup can be decrypted without the pool keys at all. That capability is in the
protocol but **not exposed by any shipped command**: there is no CLI or REPL
command to export it. Reaching it requires a program written against the Go
client library, holding a DAEMON-authorized session.

Two further limits are worth knowing before you design a recovery model around
escrow:

- The key is **per table**. A catalog-wide `.cadb` holds a section per table,
  each sealed under that table's own key, so escrowing one key does not open the
  file.
- The **catalog restore path accepts no escrowed key at all.** It opens every
  section with the restoring daemon's own pool keys. Escrowed backup keys apply
  only to a single-table restore driven through the client library.

In practice, for a deployment administered through configuration and the shipped
tools, **the pool keys are the only key that matters.** Treat
`SEC_PASSWORD_DIRECTORY` as the thing that must survive.

See [Authorization & security](authorization.md) for how DAEMON authorization is
granted.

## Restoring

### Live tables

Restore is a startup operation. Point `HTCONDORDB_RESTORE_FILE` at a snapshot and
restart the daemon:

```
condor_off -daemon HTCONDORDB
cp /backups/htcondordb-20260411T030000Z.cadb $(SPOOL)/htcondordb-restore.cadb
# add to the config: HTCONDORDB_RESTORE_FILE = $(SPOOL)/htcondordb-restore.cadb
condor_on -daemon HTCONDORDB
```

The restore runs before the daemon serves any client. Afterwards the trigger file
is renamed to `<path>.restored-<timestamp>`, so the next restart serves live data
rather than reverting to the snapshot again. A missing file is a silent no-op,
which means the knob is safe to leave in the configuration permanently; it also
means a typo in the path restores nothing and says nothing beyond the absence of
a "restored database from snapshot" log line. Check for that line.

What a restore does, precisely:

- For each table named in the snapshot, it creates the table if absent, then
  **empties it and reloads it**. Writers are blocked for the duration; the empty
  intermediate state is never visible to a client.
- **Tables present in the database but absent from the snapshot are left
  untouched.** A restore is not a revert to the snapshot's state — it is an
  overwrite of the tables the snapshot happens to contain. If you need the
  database to match the backup exactly, drop the extra tables yourself.
- **Archives are not touched.** Whatever history the host has stays exactly as it
  is, neither replaced nor removed.
- **A restore is not atomic across tables.** Tables are restored one at a time.
  A failure part-way through leaves the already-restored tables restored, the
  failing table emptied and partially loaded, and the remaining tables at their
  pre-restore contents. There is no rollback. The daemon fails to start, which at
  least prevents clients from seeing the mixed state — fix the cause and restart,
  since restoring the same file again is idempotent.
- Ads are re-encrypted under the **target's** policy, not the source's. Restoring
  an encrypted backup into a daemon with `HTCONDORDB_ENCRYPT_AT_REST` off writes
  that data unencrypted at rest.
- Every watcher of a restored table — changefeed consumers, exporters,
  replication followers — is told to resync and replays from a full snapshot of
  the new state. Expect a burst of downstream re-ingest after a restore.
- If the snapshot names a table that exists on the target as an *archive*, the
  restore of that section fails.

### Restoring archives

**From a dropbox**, which is the backup you should have, replay the records:

```
htcondordb-restore-dropbox -dir /backups/htcondordb/history -table history
```

The table is created if it does not exist. Records dedup on `GlobalJobId`, so
this is safe to re-run over a dropbox that was already partly ingested, and safe
over the duplicate batches an at-least-once exporter leaves behind after a crash.
Run it with `-dry-run` first to see what the dropbox holds, and read the
`DATA LOSS` / `INCOMPLETE` / `DAMAGED BATCH` / `SHORT READ` lines at the top of
the report rather than the totals.

What a replay does not restore is the archive's own configuration — its indexes,
hot set, retention bounds and encrypted-attribute policy. A fresh table is
created with defaults. Re-apply what you need (`.addindex`, `.retention`), or
restore a directory copy instead if you have one.

**From a directory copy**, put the directory back:

```
condor_off -daemon HTCONDORDB
# wait for "store closed"
rm -rf $(SPOOL)/htcondordb/archives/history
tar -C $(SPOOL)/htcondordb -xf archives-backup.tar archives/history
chown -R condor:condor $(SPOOL)/htcondordb/archives/history
condor_on -daemon HTCONDORDB
```

The catalog discovers archives by enumerating the `archives/` directory at
startup, so a restored directory is picked up with no further configuration. The
directory carries its own persisted configuration — indexes, zone maps, retention
bounds — which is authoritative on reopen, so the archive comes back configured
as it was rather than as the current config file describes.

An archive restored onto a host whose pool keys differ from the ones it was
sealed under will not open. This is the failure that `SEC_PASSWORD_DIRECTORY`
backups exist to prevent. A dropbox has no such dependency: its records are
plain ClassAd text, so a replay works anywhere.

**Rebuilding from source.** For the `history` archive specifically, the records
originate in a schedd's own history file. If that file is intact, a lost archive
can often be rebuilt — either by letting schedd-sync re-read the configured
history file, or by running a history import job against the pool's schedds. Both
are bounded by what the schedds still retain, so neither is a substitute for a
backup of an archive older than the schedds' own retention.

### Replicated deployments

In `leader-follower` and `consistent` modes, restore the **leader** and let
replication re-propagate. Restoring a snapshot onto a single follower or raft
peer puts that node's mutable tables out of agreement with the rest of the
cluster, and nothing detects or repairs the divergence.

Archives are node-local in every mode. Restoring the leader's archive directory
does not give the followers one — each node needs its own archive backup and its
own restore.

See [High availability](ha.md) for the replication modes.

## Verifying

**There is no command that validates a `.cadb` file without restoring it**, and no
checksum or manifest alongside it. The file is a compressed, usually encrypted,
streaming format; `file`, `gzip -t` and friends tell you nothing. A backup you
have not restored is a backup you have not tested.

Test restores against a scratch daemon, not production:

```
# a throwaway daemon on its own directory and port, with the SAME SEC_PASSWORD_DIRECTORY
HTCONDORDB_DIR          = /var/tmp/htcondordb-verify
HTCONDORDB_RESTORE_FILE = /var/tmp/htcondordb-verify-restore.cadb
```

Start it, confirm the `restored database from snapshot` log line, then count the
restored rows and compare against the source:

```
htcondordb-cli -addr <scratch-host>:<port> -e 'SELECT COUNT(*) FROM jobs'
```

Doing this on a schedule is the only way to know that your snapshots, your pool
keys and your off-host copies all still work together.

For a restored **archive**, open an interactive shell against the scratch daemon
and use `.tables` to confirm it was discovered and `.stats <archive>` to read back
its ad count, segment count and byte totals; compare those to the source. (These
are interactive meta-commands — `-e` runs SQL only.) Record contents are checksum-
verified as they are read, so a query that actually scans records surfaces
corruption rather than returning bad data silently; a bare `COUNT(*)` may be
answered from metadata and prove less than it appears to. There is **no offline
archive checker exposed by any shipped command**: the ability to inspect a damaged
archive's files without opening it, and to repair one, exists in the storage
library but has no CLI.

### What you can monitor

The daemon exports its own backup history on the metrics endpoint:

| Metric | Meaning |
|---|---|
| `htcondordb_backup_last_success_timestamp_seconds` | Unix time of the last successful snapshot; 0 if none since start |
| `htcondordb_backup_last_failure_timestamp_seconds` | Unix time of the last failed snapshot; 0 if none |
| `htcondordb_backup_attempts_total` | Snapshots attempted since start |
| `htcondordb_backup_failures_total` | Snapshots that returned an error |
| `htcondordb_backup_last_bytes` | Size of the last successful snapshot |
| `htcondordb_backup_last_duration_seconds` | How long it took |

**Alert on the age of the last success, not on the failure counter.** A backup
regime fails silently: the snapshot that never happens produces no artifact and
at most a log line nobody reads, so a failure count of zero is exactly what both
a healthy daemon and a disabled one report.

```
time() - htcondordb_backup_last_success_timestamp_seconds > 2 * <your interval>
```

A value of 0 means no snapshot has succeeded since the daemon started, which is
also what a daemon with backups unconfigured reports — so pair the alert with the
knowledge that you configured them. `htcondordb_backup_last_bytes` is worth a
second look after a schema or configuration change: a sharp drop means tables
stopped being included.

These cover snapshots only. Nothing here reports on archive directory copies or
on a dropbox, because the daemon does not make them.

There are **no metrics for backups** — nothing reports the age of the last
successful snapshot, and nothing counts failures. The only signal is the daemon
log:

| Log line | Level | Meaning |
|---|---|---|
| `periodic snapshots enabled` | Info | Confirms the schedule is actually on, with directory, interval and keep count. Its absence at startup means no backups are being taken. |
| `wrote periodic snapshot` | Info | One snapshot succeeded, with the file name. |
| `periodic snapshot failed` | Warning | One snapshot failed. The schedule continues; the previous files are untouched. |
| `periodic snapshots disabled: cannot create directory` | Warning | The directory could not be created. **No backups will be taken at all.** |
| `could not prune old snapshot` | Warning | Retention could not delete an old file; the directory will grow. |
| `restored database from snapshot` | Info | A restore-on-startup ran. |

Until there is a metric, alert on the backup directory from outside the daemon —
the age of the newest `htcondordb-*.cadb` is a direct proxy for the thing you
care about:

```
find $(SPOOL)/htcondordb-backups -name 'htcondordb-*.cadb' -mmin -120 | grep -q . \
  || echo "htcondordb: no snapshot in the last 2 hours"
```

For sizing backups and watching archive growth, the daemon's Prometheus endpoint
(opt-in via `HTCONDORDB_METRICS_ADDRESS`) exports `htcondordb_used_bytes`,
`htcondordb_live_bytes` and `htcondordb_segments`, labelled by table, for archives
as well as mutable tables. `htcondordb_store_corrupt_chain_links_total` is a
store-integrity counter; it is zero on a healthy store and any increase is worth
alerting on.

## Limitations and known gaps

Collected in one place, because several of these will surprise an administrator
who assumes the snapshot mechanism is a database backup:

- **Archives are not in snapshots.** On a schedd-sync deployment this is most of
  the data. Back up `archives/` separately, by directory copy.
- **No scheduled backups by default.** Snapshots happen only if you configure
  them.
- **A verify is not a test restore.** `.verify` proves a snapshot is the file
  that was written; only restoring it into a throwaway daemon proves it loads.
- **Snapshots written before manifests existed cannot be verified.** They are
  reported as unverifiable, not as damaged.
- **Nothing verifies archive copies or dropboxes.** The manifest and `.verify`
  cover `.cadb` snapshots only. A `htcondordb-restore-dropbox -dry-run` is the nearest
  equivalent for a dropbox.
- **A snapshot is not point-in-time.** It is taken while writers proceed — it
  does not block them and is not consistent against them. Within a single table
  it is consistent per internal shard, not across the table; across tables it is
  a sequence of independently-taken captures, so a cross-table relationship
  written during the snapshot can appear half-present. The file is a crash-
  consistent image of a moving database, not a transactional one. For a true
  point-in-time copy, stop the daemon and copy the directory.
- **Restore is not atomic across tables** and has no rollback.
- **Restore does not remove tables** absent from the snapshot.
- **Escrowed backup keys are per-table and unreachable from the shipped tools**,
  and the catalog restore path ignores them. Pool keys are what you must retain.
- **No offline archive checker or repair tool** is exposed.
- **A dropbox restore rebuilds records, not an archive.** It replays ads into a
  table; it does not restore that archive's indexes or configuration, which come
  from a directory copy. Expect an ingest, not a file move.
- **The dropbox bounds damage at a whole batch unless you opt into frames.** A
  `.tar.gz` is one gzip stream, so a bad byte costs up to `roll-jobs` records.
  `-container frames` reduces that to one frame, but it is not the default
  because external consumers read tarballs.
- **A dropbox cannot be proven complete at its ends.** A consumer drains a
  dropbox from the front, so a low sequence number being absent is the normal
  case, not a loss, and nothing in the directory records how far the exporter has
  got. Holes *between* batches that are present are detected; batches missing
  from either end are not.
- **Manifests are per batch, not per dropbox.** A batch can be checked against
  its own manifest; there is no directory-level record to check the set against.
- **Retention prunes by filename.** Any file matching `htcondordb-*.cadb` in the
  snapshot directory is eligible for deletion; `HTCONDORDB_SNAPSHOT_KEEP` has no
  "unlimited" setting.
- **Archive maintenance cannot be paused from configuration.** There is no knob
  that stops the background pass from rotating or merging, so a hot copy of an
  archive *directory* cannot be made race-free without a filesystem snapshot or a
  daemon stop. This is a limitation of directory copies, not of archive backup:
  the continuous export is unaffected by maintenance.
- **A dropbox needs a consumer.** Nothing removes batches from it. If yours
  stops, the dropbox fills to its ceiling, the exporter stalls, and records age
  out of retention before being exported.
- In `consistent` mode, the raft snapshot that brings a joining node up to date is
  held whole in memory while it is captured, unlike the file-based snapshot, which
  streams.

See also [Configuration](configuration.md),
[Authorization & security](authorization.md) and
[High availability](ha.md).
