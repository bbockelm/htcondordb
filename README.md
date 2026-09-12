# htcondordb

A full-fledged HTCondor daemon that serves the embedded ClassAd-log database
(transactional key/ad store with constraint queries, matchmaking, ordered
indexes, and change watches) over CEDAR, with HTCondor authorization, optional
high availability, and a SQL-like REPL client.

Its most common use is as a **read model of a local `condor_schedd`**: point it
at the schedd's files and the live queue and job history become queryable with
SQL-like statements — no polling `condor_q` / `condor_history`.

Built on:

- `github.com/PelicanPlatform/classad/db` — the embedded ClassAd log.
- `github.com/PelicanPlatform/classad/dbrpc` — the client/server RPC over CEDAR.
- `github.com/bbockelm/golang-htcondor` — config, security policy, DaemonCore
  integration (running under `condor_master`, ready/keepalive pings, privilege
  drop, SIGHUP reconfigure).
- `github.com/bbockelm/cedar` — authenticated, encrypted transport.
- `github.com/hashicorp/raft` — consensus for the consistent HA mode, tunneled
  over CEDAR.

## Install

```sh
make build     # -> bin/htcondordb (daemon) and bin/htcondordb-cli (shell)
make test      # run the suite
make vet
```

The `go.mod` `replace` directives point at sibling checkouts for local
development. The Makefile sets the module-graph environment those need; to run
`go` directly, export the same:

```sh
GOWORK=off GOFLAGS=-mod=mod GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform GOPROXY=direct
```

Resolve the `replace` directives to tagged versions before publishing with CI.

## Quickstart: mirror a schedd

Run htcondordb on the **same host as the schedd**, under `condor_master`, so it
inherits the condor config and drops to the condor user. Add to the HTCondor
config the master reads (e.g. `/etc/condor/config.d/`):

```conf
HTCONDORDB     = /path/to/bin/htcondordb   # absolute path to the built binary
DAEMON_LIST    = $(DAEMON_LIST), HTCONDORDB
DC_DAEMON_LIST = +HTCONDORDB               # register it as a DaemonCore daemon

HTCONDORDB_SYNC_SCHEDD = true              # tail job_queue.log -> jobs, history -> history
```

Then restart the master (`condor_restart -master` — a `condor_reconfig` is not
enough, because `DC_DAEMON_LIST` is only consulted when daemons start) and query
as the tables fill:

```sh
bin/htcondordb-cli -e "SELECT COUNT(*) FROM jobs"
bin/htcondordb-cli -e "SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner ORDER BY COUNT(*) DESC"
bin/htcondordb-cli -e "SELECT COUNT(*) FROM history WHERE CompletionDate > 1700000000"
```

`htcondordb-cli` with no arguments opens the interactive shell and auto-locates
the daemon. Reading requires READ authorization; the sync writes in-process and
needs no client credentials. See [Schedd sync mode](docs/schedd-sync.md) for the
full walkthrough and file-path overrides.

## Quickstart: load ads directly

Without a schedd, pipe a `-long` ClassAd stream straight in:

```sh
condor_status -long        | bin/htcondordb-cli load -table machines
condor_q -global -long     | bin/htcondordb-cli load -table jobs -key GlobalJobId
bin/htcondordb-cli -e 'SELECT Name, Cpus, Memory FROM machines WHERE Cpus >= 8'
```

## Using the query language

`WHERE` is a full ClassAd expression (not a SQL dialect), aggregation and
`GROUP BY` run server-side, and cross-table work is HTCondor matchmaking
(`MATCH`), not a join:

```sql
SELECT COUNT(*), AVG(Cpus), MAX(Memory) FROM machines WHERE State == "Unclaimed";
SELECT Owner, COUNT(*), SUM(RequestCpus) FROM jobs GROUP BY Owner ORDER BY COUNT(*) DESC;
MATCH jobs TO machines WHERE Owner == "alice" WHERE TARGET Arch == "X86_64" LIMIT 1;
```

The full language — tables, indexes, aggregates, `MATCH`, output formats, and the
`.stats`/`.explain`/`.addindex` diagnostics — is in the
**[REPL reference](docs/repl.md)**.

## Components

| Path | What |
|------|------|
| `command/` | The CEDAR command integers, from HTCondor's retired `TRANSFERD_BASE` (74000) block. |
| `server/` | The DB service: wraps `db.DB` + `dbrpc.Server` behind one command, enforcing READ / WRITE / DAEMON authorization per connection. |
| `ha/leaderfollower/` | Asynchronous commit-stream replication (the "leader-follower" mode). |
| `ha/consistent/` | Quorum-replicated raft state machine, CEDAR raft transport, and leader-routing control protocol (the "consistent" mode). |
| `repl/` | The SQL-like language (parser + executor + formatter). |
| `cmd/htcondordb/` | The daemon. |
| `cmd/htcondordb-cli/` | The interactive shell. |

## Documentation

- [Schedd sync mode](docs/schedd-sync.md) — mirror a local schedd (primary use case).
- [REPL reference](docs/repl.md) — the full query language and shell.
- [Configuration](docs/configuration.md) — every `HTCONDORDB_*` knob.
- [Authorization & security](docs/authorization.md) — access levels and getting WRITE.
- [High availability](docs/ha.md) — leader-follower and raft-consistent modes.
- [Design notes](docs/README.md#design-notes) — time-series / continuous-aggregate sketches.
