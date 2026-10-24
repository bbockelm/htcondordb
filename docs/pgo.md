# Profile-guided optimization

The daemon is built with Go's profile-guided optimization (PGO). The profile is
`cmd/htcondordb/default.pgo`; `go build` applies a main package's `default.pgo`
to that package by default (`-pgo=auto`, Go 1.21 and later), so every way the
daemon is built uses it with no extra flags: `make build`/`make daemon`,
`make rpm`, the release workflow's tarballs, `capstone-e2e`, and
`grafana/e2e/Dockerfile.htcondordb`. The other main packages (`htcondordb-cli`,
`history-import`, `changefeed-toysink`) and the C client library (`./capi`)
have no profile and are built as before.

To check a binary, `go version -m bin/htcondordb | grep pgo` prints the
`-pgo=` build setting; a build without PGO has none. To build without it, pass
`-pgo=off` to `go build` (with the environment the
[README](../README.md#building-from-source) lists).

## What it buys

Measured on linux/arm64 against the same commit built with `-pgo=off`
(GOEXPERIMENT=simd; 6 to 8 interleaved rounds; a generated 60,000-record history
and a 191 MB `job_queue.log`; the median of the per-round PGO/off ratios):

| Workload | Metric | `-pgo=off` | PGO | Change |
|---|---|---|---|---|
| Columnar `COUNT`/`GROUP BY` on `jobs` (~2 ms queries) | queries/s | 403 | 494 | +22% |
| | daemon CPU | 1.45 s | 1.19 s | -17% |
| Mixed shapes: time buckets, TopK, `DISTINCT`, projections | p50 latency | 130 ms | 117 ms | -10% |
| | queries/s | 1.98 | 2.02 | +1% |
| Archive counts and `GROUP BY` over `history` | queries/s | 4.43 | 4.42 | +1% |
| Queries not in the training workload | queries/s | 2.61 | 2.54 | within noise |
| Initial history ingest | time | 22.9 s | 22.9 s | within noise |
| Initial `job_queue.log` ingest | time | 22.4 s | 21.6 s | within noise |

Short queries gain the most; queries that spend their time decompressing and
scanning archive segments gain little. Ingest, memory use and startup time do not
change measurably.

A cold build of the daemon alone takes about as long as without PGO (within 5%
on 4 and 12 CPUs). Building `./cmd/htcondordb` together with another main
package in one `go build`, as `make rpm` and the release workflow do, compiles
the shared dependencies twice, once with the profile and once without: that
build took 26% longer on 12 CPUs and 42% longer on 4. Cached rebuilds are
unaffected unless the profile changes.

## The checked-in profile is synthetic

The current `default.pgo` was recorded in a Linux container from a generated
workload, not from a production daemon: the initial ingest of a history file and
`job_queue.log`, tailing of both while they grow, a mix of SQL queries before and
after `.analyze`, `analyze` and `rewrite` on `jobs` and `history`, and
`.resync jobs` / `.resync history` (sampled at a third of its length, so that a
rare reconcile does not dominate). It should be replaced with a profile captured
from production.

## Refreshing the profile

1. On a busy access-point mirror (linux/amd64 is the main deployment target),
   enable the pprof endpoints and restart the daemon:

   ```
   HTCONDORDB_METRICS_ADDRESS = 127.0.0.1:9630
   HTCONDORDB_ENABLE_PPROF = true
   ```

   They serve `/debug/pprof/` next to `/metrics`. Profiling costs a few percent
   of CPU only while a profile is being taken. Keep the address on loopback, or
   behind a firewall: the endpoints are unauthenticated.

2. Capture profiles of the work the daemon actually does and merge them, with
   [`packaging/pgo-capture.sh`](../packaging/pgo-capture.sh) (needs `curl` and a
   Go toolchain). Run it from the repository root, over an SSH tunnel or on the
   host itself:

   ```sh
   # steady-state tailing and normal query load: three 60 s profiles
   packaging/pgo-capture.sh -n 3 -k prof/steady http://127.0.0.1:9630
   # a busy dashboard period
   packaging/pgo-capture.sh -k prof/busy http://127.0.0.1:9630
   # a reconcile: start `.resync jobs` in htcondordb-cli, then capture
   packaging/pgo-capture.sh -k prof/resync http://127.0.0.1:9630
   # merge everything into cmd/htcondordb/default.pgo
   packaging/pgo-capture.sh $(printf -- '-a %s ' prof/*/*.pprof)
   ```

   `make pgo-profile PGO_URL="http://host1:9630 http://host2:9630" PGO_COUNT=3`
   does the capture and the merge in one step. Profiles merge by CPU time, so
   the result weighs each kind of work by how much CPU it actually used.

3. Check the result is small (well under 1 MB; `go tool pprof -proto` keeps only
   samples and stacks), rebuild, compare against a `-pgo=off` build if in doubt,
   and commit `cmd/htcondordb/default.pgo`.

Refresh the profile when a new classad minor version is adopted, or quarterly.
A stale profile is safe: the compiler ignores samples that no longer match the
source, so an old profile only optimizes less, never incorrectly.
