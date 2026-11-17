# Continuous archive backup

Status: partly implemented.

- The producer described under [What exists today](#what-exists-today) ships as
  the `archivedropbox` exporter.
- **The restore path now ships too** — `dropboxrestore` and the `restore-dropbox`
  command, gap 1 below. That was the blocking gap: the exporter produced a
  correct artifact nothing could read back.
- **The framed stream format ships as `dropboxframe`** — writer, resyncing
  reader, and the sizing measurements below — **and the exporter emits it** when
  a dropbox is configured with `container = frames`. The default is still
  `targz`; see [Adopting the framed format](#adopting-the-framed-format).

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
| 5 | Bounded blast radius | partial — bounded at one batch (up to 2000 records), not at a frame; see gap 3 |
| 6 | Append-only and resumable | **met** |

## Gaps

1. ~~**There is no restore path.**~~ **Closed.** `dropboxrestore` reads the
   batches back and `restore-dropbox -dir <dropbox> -table <archive>` drives it.
   It dedups on `GlobalJobId` through `historyimport.Writer.Has`, so the
   exporter's at-least-once duplicates and a re-run of a half-finished restore
   are both harmless. A damaged batch does not stop it: the batch is reported,
   its readable records are still written, and the next batch is attempted.
   Sequence gaps and the exporter's loss reports are surfaced at the top of the
   restore report, because a restore that silently succeeds over a gap is how an
   incomplete archive comes to be trusted.
2. **It is presented as a feed, not as a backup.** The backup guide describes it
   under logical export and correctly says nothing re-ingests the tarballs. Once
   gap 1 closes, it should be documented as what it then is: the archive backup
   that survives damage to the store's internal structure.
3. **Damage is contained per batch, not per frame.** A tarball is a single gzip
   stream. A truncation or bit flip costs the remainder of that member, so up to
   `RollJobs` records — far more than the data physically damaged. Bar 5 is met
   only coarsely.
4. **No manifest and no set-level verification.** Nothing records which batch
   sequence numbers should exist, so a dropbox missing `batch-00000047` is
   indistinguishable from one that never had it. Verification today means
   decompressing everything.
5. **Backpressure can become loss.** Backpressure stalls the cursor; retention
   keeps pruning regardless. A consumer that stops draining for long enough turns
   a full dropbox into lost records. This is reported after the fact by a loss
   report, not prevented.

## Proposed work

### A. The restore path — done

Implemented as the `dropboxrestore` package and the `restore-dropbox` command.

It reuses `historyimport.Writer` rather than the schedd-shaped
`historyimport.Source`: the restore needs `Append` and the `Has` dedup, and none
of the per-schedd discovery and cursor machinery around them. `-dry-run` reads
and reports without writing and without needing the database reachable at all,
so an operator can see what a dropbox holds from anywhere the files are.

### B. Hardening the stream — format done, adoption pending

The format ships as `dropboxframe`. Raises bar 4 and 5 from partial to met:

- **Size-bounded framing.** Replace one gzip member per batch with a sequence of
  independently-decompressible frames, each length-prefixed, each carrying a
  magic and a CRC over its own bytes. A reader that hits a bad frame resyncs by
  scanning forward for the next magic rather than giving up on the rest of the
  file. zstd frames with no trained dictionary are the obvious choice and keep
  rule 3 intact.

  A frame holds **as many whole records as fit in a minimum frame size** — it
  accumulates records and closes once they reach that many bytes, so a frame
  never splits a record and a record never spans frames. The trigger is measured
  on the records as handed in rather than on the compressed result, so the bound
  is one an operator can reason about: *this many records is what a damaged frame
  costs*. A record larger than the frame size gets a frame of its own rather than
  being cut, because rule 1 outranks the size target.

  **Default 256 KiB.** Measured over 2000 synthetic job ads with realistic
  variation in owner, command, working directory, resource requests and times
  (823 KB raw, ~412 bytes per record — the exporter's default `RollJobs` batch):

  | Format | Bytes | Ratio | Frames | Records per frame |
  |---|---|---|---|---|
  | `tar.gz` (today) | 135,747 | 6.06x | 1 | 2000 |
  | framed, 4 KiB | 185,584 | 4.43x | 200 | 10 |
  | framed, 64 KiB | 92,662 | 8.88x | 13 | 153 |
  | **framed, 256 KiB** | **86,053** | **9.56x** | **4** | **500** |
  | framed, 1 MiB | 83,343 | 9.88x | 1 | 2000 |

  Two things fall out of this that are worth stating plainly.

  First, **framing is not a compression cost here, it is a compression win** —
  about 37% smaller than the format today at 256 KiB. The comparison is zstd
  against gzip, not framed against unframed, and zstd wins by much more than
  framing gives back. The blast radius improves and the dropbox gets smaller.

  Second, **1 MiB is too large to do anything.** A default batch is only ~823 KB
  of records, so a 1 MiB frame swallows the whole batch and leaves exactly one
  frame per file — the same blast radius as today, for a 3% space saving over
  256 KiB. A frame size only bounds damage if it is meaningfully smaller than a
  batch. 256 KiB gives four frames per batch, 500 records each; 64 KiB gives
  thirteen, 153 records each, and still costs 32% less than today. Sites that
  value recovery over space should set 64 KiB; sites with much larger batches or
  much larger ads can raise the default, since what matters is the ratio of frame
  size to batch size, not the absolute number.

  The floor is 4 KiB and is enforced. Below roughly 64 KiB the compressor has too
  little history to work with, and at 4 KiB the framed stream is *worse* than the
  gzip tarball it replaces — the one row in the table where the idea does not pay.
- **A per-batch manifest.** A `manifest.ad` carrying the batch sequence, record
  count, frame count and frame size, first and last record time, and per-frame
  checksums — so a dropbox can be verified, and gaps in it detected, without a
  database and without decompressing the payload.

#### Adopting the framed format

`archivedropbox` is a separate Go module that deliberately does not depend on the
main module, so that the exporter binary stays out of the core daemon build. The
frame format lives in the main module, where the restore path needs it. Three
ways to bridge that were available: vendor a copy of the format into the
exporter, make `dropboxframe` its own module, or let `archivedropbox` depend on
the main module.

**The dependency won**, via a relative `replace` — the pattern `grafana/go.mod`
already uses in this repo:

```
require github.com/bbockelm/htcondordb v0.0.0
replace github.com/bbockelm/htcondordb => ../
```

Vendoring was rejected because duplicated format code drifts, and the two sides
of a wire format drifting is precisely the failure that costs data. A separate
module was rejected because `dropboxframe` would then be external to the *main*
module too, so every format change would need a tag and two `go.mod` bumps — real
release choreography for a package with exactly two callers in one repo.

The objection to the dependency — that it drags the daemon's tree into the
exporter — turned out not to hold. Measured, building `cmd/archivedropbox`
before and after:

| | before | after |
|---|---|---|
| binary | 14,755,314 B | 14,772,050 B (**+0.11%**) |
| `archivedropbox/go.sum` | 111 lines | 111 lines (**unchanged**) |

Module-graph pruning plus the local `replace` adds no `go.sum` entries, and the
linker pulls in only `dropboxframe` itself, whose one dependency
(`klauspost/compress`) the exporter already had.

Two constraints come with it. The `replace` is relative, so `archivedropbox` is
not independently consumable from outside the repo — already true of `grafana`.
And the dependency runs one way: **nothing in the main module may import
`archivedropbox`**, or the two become a cycle.

**The default stays `targz`.** The dropbox exists for an out-of-band consumer,
and those consumers are external code reading gzip tarballs with standard tools.
`container = frames` is opt-in, writes `.cadf` instead of `.tar.gz` so the two
are never confused, and becomes the default only after consumers have had a
release to follow. The restore path reads both, including a dropbox holding a
mixture — which is what a dropbox looks like across the switch.

### C. Operational

- Re-present the mechanism in [backup-restore.md](../backup-restore.md) as a
  backup rather than a one-way feed, once it has a restore path.
- Export metrics: records exported, cursor lag behind the archive head, bytes and
  batches currently undelivered in the dropbox, loss reports written.
- A `--verify` mode over a dropbox directory, checking frames and manifests.

## Ordering

**A before B**, which is how it went: A converted a shipped-but-unusable artifact
into a recoverable one, and B raised the floor under a mechanism that only became
load-bearing once A existed. What is left of B is the per-batch manifest. C can
land alongside it.

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
