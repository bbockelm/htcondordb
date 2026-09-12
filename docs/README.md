# htcondordb documentation

Start with the [project README](../README.md) for what htcondordb is and a
quickstart. These pages cover the details:

- **[Schedd sync mode](schedd-sync.md)** — mirror a local `condor_schedd`'s live
  queue and history into queryable tables (the primary use case).
- **[REPL reference](repl.md)** — the full SQL-like language: tables, indexes,
  `SELECT`/aggregates, `MATCH` matchmaking, formatting, diagnostics/index tuning,
  and bulk-loading ads.
- **[Configuration](configuration.md)** — every `HTCONDORDB_*` knob and address
  resolution.
- **[Authorization & security](authorization.md)** — READ/WRITE/DAEMON access
  levels, the command set, and how to get WRITE.
- **[High availability](ha.md)** — `standalone`, `leader-follower`, and raft-based
  `consistent` modes.

## Design notes

Draft design sketches (not user documentation):

- [Time-series over history](design/time-series.md)
- [Continuous aggregates](design/continuous-aggregates.md)
