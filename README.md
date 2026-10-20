# htcondordb

A full-fledged HTCondor daemon that serves the embedded ClassAd-log database
(transactional key/ad store with constraint queries, matchmaking, ordered
indexes, and change watches) over CEDAR, with HTCondor authorization, optional
high availability, and a SQL-like REPL client.

Its most common use is as a **read model of a local `condor_schedd`**: point it
at the schedd's files and the live queue and job history become queryable with
SQL-like statements — no polling `condor_q` / `condor_history`.

## Install

Each [release](https://github.com/bbockelm/htcondordb/releases/latest) carries
an RPM for x86_64 and aarch64. Install HTCondor first (the package depends on
`condor`), then install the RPM for the release you want:

```sh
VERSION=0.21.3   # the latest version is on the releases page
sudo dnf install https://github.com/bbockelm/htcondordb/releases/download/v${VERSION}/htcondordb-${VERSION}-1.$(uname -m).rpm
```

The package installs:

| Path | What |
|------|------|
| `/usr/sbin/htcondordb` | The daemon, started by `condor_master`. |
| `/usr/bin/htcondordb-cli` | The query shell. |
| `/usr/sbin/kafkasync`, `opensearchsync`, `archivedropbox` | Optional exporters, launched by the daemon when configured. |
| `/etc/condor/config.d/50-htcondordb.conf` | Configuration. Installing the package does **not** start the daemon; the lines that enable it are commented out. |

The same release page has `.tar.gz` archives for Linux and macOS (amd64 and
arm64). Each holds the daemon, the shell, and the exporters; keep them in one
directory (the daemon looks for the exporters beside itself) and set
`HTCONDORDB` to the daemon's absolute path.

## Quickstart: mirror a schedd

Run htcondordb on the **same host as the schedd**, under `condor_master`, so it
inherits the condor config and drops to the condor user. In
`/etc/condor/config.d/50-htcondordb.conf`, uncomment these three lines:

```conf
DAEMON_LIST     = $(DAEMON_LIST), HTCONDORDB
DC_DAEMON_LIST  = $(DC_DAEMON_LIST) +HTCONDORDB
HTCONDORDB_SYNC_SCHEDD = true
```

Then restart the master (`condor_restart -master` — a `condor_reconfig` is not
enough, because `DC_DAEMON_LIST` is only consulted when daemons start) and query
as the tables fill:

```sh
htcondordb-cli -e "SELECT COUNT(*) FROM jobs"
htcondordb-cli -e "SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner ORDER BY COUNT(*) DESC"
htcondordb-cli -e "SELECT COUNT(*) FROM history WHERE CompletionDate > 1700000000"
```

`htcondordb-cli` with no arguments opens the interactive shell and auto-locates
the daemon; `-pool`/`-name` locate one through a collector instead (see
[REPL reference](docs/repl.md#finding-the-daemon)). Reading requires READ authorization; the sync writes in-process and
needs no client credentials. See [Schedd sync mode](docs/schedd-sync.md) for the
full walkthrough and file-path overrides.

## Quickstart: load ads directly

Without a schedd, pipe a `-long` ClassAd stream straight in:

```sh
condor_status -long        | htcondordb-cli load -table machines
condor_q -global -long     | htcondordb-cli load -table jobs -key GlobalJobId
htcondordb-cli -e 'SELECT Name, Cpus, Memory FROM machines WHERE Cpus >= 8'
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

## Documentation

- [Schedd sync mode](docs/schedd-sync.md) — mirror a local schedd (primary use case).
- [REPL reference](docs/repl.md) — the full query language and shell.
- [Configuration](docs/configuration.md) — every `HTCONDORDB_*` knob.
- [Authorization & security](docs/authorization.md) — access levels and getting WRITE.
- [Federation hub](docs/federation.md) — many access points' mirrors in one catalog.
- [High availability](docs/ha.md) — leader-follower and raft-consistent modes.
- [Design notes](docs/README.md#design-notes) — time-series / continuous-aggregate sketches.

## Building from source

Requires Go 1.26 or newer.

```sh
make build     # -> bin/htcondordb (daemon) and bin/htcondordb-cli (shell)
make test      # run the suite
make vet
make rpm       # -> dist/htcondordb-*.rpm (needs nfpm; PKG_ARCH=amd64|arm64)
```

The Makefile sets the module-graph environment the build needs; to run `go`
directly, export the same:

```sh
GOWORK=off GOFLAGS=-mod=mod GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform GOPROXY=direct
```

htcondordb is built on:

- `github.com/PelicanPlatform/classad/db` — the embedded ClassAd log.
- `github.com/PelicanPlatform/classad/dbrpc` — the client/server RPC over CEDAR.
- `github.com/bbockelm/golang-htcondor` — config, security policy, DaemonCore
  integration (running under `condor_master`, ready/keepalive pings, privilege
  drop, SIGHUP reconfigure).
- `github.com/bbockelm/cedar` — authenticated, encrypted transport.
- `github.com/hashicorp/raft` — consensus for the consistent HA mode, tunneled
  over CEDAR.

| Path | What |
|------|------|
| `command/` | The CEDAR command integers, from HTCondor's retired `TRANSFERD_BASE` (74000) block. |
| `server/` | The DB service: wraps `db.DB` + `dbrpc.Server` behind one command, enforcing READ / WRITE / DAEMON authorization per connection. |
| `ha/leaderfollower/` | Asynchronous commit-stream replication (the "leader-follower" mode). |
| `ha/consistent/` | Quorum-replicated raft state machine, CEDAR raft transport, and leader-routing control protocol (the "consistent" mode). |
| `repl/` | The SQL-like language (parser + executor + formatter). |
| `cmd/htcondordb/` | The daemon. |
| `cmd/htcondordb-cli/` | The interactive shell. |
