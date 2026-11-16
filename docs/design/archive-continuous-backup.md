# Continuous archive backup

Status: design. The producer described under [What exists today](#what-exists-today)
is shipped; the restore path is not.

## The problem

Archive tables are not in snapshots, so the only backup an administrator can take
of an archive today is a copy of its directory. A directory copy is a copy of the
*storage layout*, and it inherits every coupling the live store has: segment data
files, their sidecar indexes, a shard directory snapshot, and a shared `dicts/`
holding the trained compression dictionaries that the segments reference by id.

That last coupling is the dangerous one. A segment is not readable without the
dictionary it names. The dictionaries live outside the segments, are shared by
many of them, and are rotated by a background retrain. If a crash interrupts a
retrain and leaves segments naming dictionaries that are no longer on disk, every
record in those segments becomes unreadable — and a directory copy taken
afterwards faithfully preserves the damage. The blast radius is large because the
coupling is many-to-one: a handful of lost dictionary files can strand hundreds
of segments and gigabytes of records. It is also quiet, because a record whose
dictionary is missing can still be *counted* without being *decoded*.

A backup must not be able to fail this way. The requirement that follows is a
single sentence:

> **No record in the backup may depend on anything outside itself.**

Everything below is downstream of that.

## The robustness bar

1. **Row-wise.** One record is one unit. No columnar layout, no shared block
   structure, no cross-record encoding.
2. **Inline attributes.** Every attribute name and every value written out in
   full, in every record. A reader needs no schema.
3. **No interning and no shared dictionaries.** Nothing in a record is a
   reference into a table that lives somewhere else. This is the rule that the
   failure above violates.
4. **Self-describing and self-validating per unit.** A unit carries what it is
   and a checksum of itself.
5. **Bounded blast radius.** Damage costs the unit that holds it. It never costs
   the stream, and it never costs a record that is not physically damaged.
6. **Append-only and resumable.** The producer can crash and resume without
   re-reading what it already wrote, and without skipping.

Compression is welcome but it is subordinate to all six. A compression scheme
that shares state across records — which is exactly what a trained dictionary is
— trades the property that makes this a backup for a few percent of space.

## What exists today

The `archivedropbox` module already implements most of this. It is a standalone
module and binary (a `dbrpc` client, registered in the exporter registry with
`Kind = "dropbox"`), so it never enters the core daemon build. The daemon's
exporter manager spawns it.

It watches one archive and writes its records into a directory as
`batch-<seq>-<firstModUnix>.tar.gz`: one **old-ClassAd text file per record**,
batched into a gzip tarball rolled every `RollJobs` records (default 2000) or
every `RollInterval` (default 10m), whichever comes first.

Durability is already right. Each tarball is written to a dot-prefixed temp file,
fsync'd, atomically renamed into place, and the containing directory fsync'd —
and only then does the resume cursor advance. A crash therefore never advances
past data that is not durably on disk. Delivery is at-least-once: a crash
re-exports the last batch as a duplicate tarball.

It also has backpressure (`MaxDropboxBytes`, default 2 GiB: while the dropbox
holds at least that much undelivered data the exporter writes nothing and does
not advance its cursor) and loss reports (if the exporter falls far enough behind
that its cursor is pruned by retention, it drops a ClassAd describing the
estimated lost time range and resumes from the oldest retained record).

Against the bar:

| | Bar | Status |
|---|---|---|
| 1 | Row-wise | **met** — one record, one tar entry |
| 2 | Inline attributes | **met** — old-ClassAd text, every attribute spelled out |
| 3 | No interning or shared dictionaries | **met** — gzip is per-tarball and untrained; nothing references `dicts/` |
| 4 | Self-describing, self-validating per unit | partial — tar carries a per-entry checksum, gzip a per-member CRC; there is no manifest |
| 5 | Bounded blast radius | partial — bounded at one batch, not one record; see gap 3 |
| 6 | Append-only and resumable | **met** |

## Gaps

1. **There is no restore path.** Nothing in the tree reads `batch-*.tar.gz` back.
   The exporter produces a correct, durable, self-contained artifact that no
   shipped code can turn back into an archive. This is the gap that matters: a
   backup you cannot restore is not a backup.
2. **It is presented as a feed, not as a backup.** The backup guide describes it
   under logical export and correctly says nothing re-ingests the tarballs. Once
   gap 1 closes, it should be documented as what it then is: the archive backup
   that survives damage to the store's internal structure.
3. **Damage is contained per batch, not per record.** A tarball is a single gzip
   stream. A truncation or bit flip costs the remainder of that member, so up to
   `RollJobs` records — not the one record that was actually damaged. Bar 5 is
   met only coarsely.
4. **No manifest and no set-level verification.** Nothing records which batch
   sequence numbers should exist, so a dropbox missing `batch-00000047` is
   indistinguishable from one that never had it. Verification today means
   decompressing everything.
5. **Backpressure can become loss.** Backpressure stalls the cursor; retention
   keeps pruning regardless. A consumer that stops draining for long enough turns
   a full dropbox into lost records. This is reported after the fact by a loss
   report, not prevented.

## Proposed work

### A. The restore path

Small, and it makes today's artifact usable:

- A `DropboxSource` implementing the existing `historyimport.Source` interface
  (`History(ctx, schedd, constraint, since, limit, yield func(*classad.ClassAd) error) error`),
  yielding ads parsed from the tar entries in sequence order.
- A `restore-dropbox --dir <dropbox> --table <archive>` command that drives it.

The idempotency this needs is already present: `historyimport.Writer` exposes
`Has(ctx, table, globalJobID)` and dedups on `GlobalJobId`, which makes the
exporter's at-least-once duplicates harmless on replay. Re-running a restore over
a dropbox that was already partly ingested is therefore safe.

Loss-report ClassAds in the dropbox should be surfaced by the restore as warnings
naming the time ranges they cover, so an operator learns at restore time which
windows the backup never held.

### B. Hardening the stream

Raises bar 4 and 5 from partial to met:

- **Per-record framing.** Replace one gzip member per batch with a sequence of
  independently-decompressible frames, each length-prefixed, each carrying a
  magic and a CRC over its own bytes. Damage then costs one frame. A reader that
  hits a bad frame resyncs by scanning forward for the next magic rather than
  giving up on the rest of the file. zstd frames with no trained dictionary are
  the obvious choice and keep rule 3 intact.
- **A per-batch manifest.** A `manifest.ad` carrying the batch sequence, record
  count, first and last record time, and per-frame checksums — so a dropbox can
  be verified, and gaps in it detected, without a database and without
  decompressing the payload.

### C. Operational

- Re-present the mechanism in [backup-restore.md](../backup-restore.md) as a
  backup rather than a one-way feed, once it has a restore path.
- Export metrics: records exported, cursor lag behind the archive head, bytes and
  batches currently undelivered in the dropbox, loss reports written.
- A `--verify` mode over a dropbox directory, checking frames and manifests.

## Ordering

**A before B.** A is a modest amount of code against interfaces that already
exist, and it converts a shipped-but-unusable artifact into a recoverable one.
B raises the floor under a mechanism that only becomes load-bearing once A
exists. C can land alongside either.

## Relationship to the other mechanisms

| | Covers | Point-in-time | Restores by |
|---|---|---|---|
| `.cadb` snapshot | mutable tables only | no | `HTCONDORDB_RESTORE_FILE` |
| Archive directory copy | one archive, whole | only if cold or from a filesystem snapshot | putting it back |
| **Continuous dropbox** | one archive, incrementally | no — it is a stream | replay (proposed) |

The dropbox is not a replacement for the directory copy. A directory copy
restores an archive in one move, with its indexes and configuration intact; a
replay rebuilds one from records and costs an ingest. The dropbox is the copy
that survives damage to the store's internal structure, because it shares none
of it. A deployment that cares about its history should have both.
