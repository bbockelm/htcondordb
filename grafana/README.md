# htcondordb Grafana datasource

A Grafana datasource plugin that queries HTCondor job, machine, and history data
stored in [htcondordb](../README.md) using its SQL engine. It is a separate Go
module (`github.com/bbockelm/htcondordb/grafana`) living inside the htcondordb
repo so it can reuse the repl SQL parser/executor and the dbrpc client directly.

## How it works

```
Grafana ──gRPC──> plugin backend (Go) ──dbrpc/CEDAR──> htcondordb server
                    │
                    ├─ builder query ─┐
                    │                 ├─> htcondordb SQL ─> repl.Executor ─> rows
                    └─ raw SQL ───────┘                                       │
                                                          repl.Result ─> Grafana data frame
```

- **Backend** (`pkg/`): a standard `grafana-plugin-sdk-go` datasource.
  - `QueryData` turns each query into htcondordb SQL, runs it through
    `repl.Executor`, and maps the result to a data frame with per-column type
    inference (unix-epoch time / number / string). Regular queries share a
    **pooled** dbrpc session (`connManager`) that reconnects on failure, instead
    of redialing per request.
  - `CallResource` exposes `/tables` and `/attributes?table=…` so the builder can
    **discover schema** for its dropdowns.
  - `StreamHandler` implements a **live source** over htcondordb WATCH: a streaming
    query returns a Grafana live channel; `RunStream` tails the table's change
    stream and pushes one frame per change (time / key / kind / selected attrs).
  - `CheckHealth` connects and runs a probe query.
- **Frontend** (`src/`): a query editor with two modes — a **Builder**
  (table / metrics / group-by / filters / time field / limit, with dropdowns
  populated from the discovery endpoints, plus a **Live (WATCH)** toggle) and a
  **SQL** escape hatch for expert users — plus a config editor (address, connect
  timeout, optional IDTOKEN).

### Query model

The builder assembles SQL server-side; both modes share the same backend
`queryModel` (`pkg/plugin/query.go`). Time macros are expanded in raw SQL:

| Macro | Expands to |
| --- | --- |
| `$__timeFilter(col)` | `(col >= <from> && col <= <to>)` |
| `$__timeFrom()` / `$__unixEpochFrom()` | dashboard range start (unix seconds) |
| `$__timeTo()` / `$__unixEpochTo()` | dashboard range end (unix seconds) |

HTCondor stores timestamps (`QDate`, `EnteredCurrentStatus`, …) as unix-epoch
integers, so those columns render as Grafana time fields automatically; the
builder's **Time field** forces any column to a time field.

### Authentication

The connection reuses htcondordb-cli's path: a CLIENT security config for the
`DBSession` command. Provide an HTCondor **IDTOKEN** in the datasource config to
authenticate (mapping to a user, allowing authorized reads); leave it blank for an
anonymous, read-only session. No HTCondor config files are needed on the Grafana
host — the plugin uses golang-htcondor's compiled-in security defaults.

## Building

The frontend build is self-contained (webpack, no `@grafana/create-plugin`
scaffolding required):

```sh
# Frontend -> dist/module.js + plugin.json + img/
npm ci
npm run build

# Backend -> dist/gpx_htcondordb_<os>_<arch> (pure Go, CGO disabled)
CGO_ENABLED=0 go build -o dist/gpx_htcondordb_linux_amd64 ./pkg
# ...repeat per GOOS/GOARCH you need to serve.
```

The backend is **Unix only** (Linux/macOS): htcondordb's server stack uses
Unix-specific syscalls, so it does not build for Windows. `dist/` is then the
loadable plugin (point Grafana's `plugins` path at it, or zip it). Unsigned plugins require
`allow_loading_unsigned_plugins = bbockelm-htcondordb-datasource` in `grafana.ini`
for local use. CI (`.github/workflows/grafana-plugin.yml`) runs exactly these
steps for all platforms and uploads the combined bundle as an artifact.

## End-to-end test

`e2e/` contains a browser-level test (Playwright + `@grafana/plugin-e2e`) that runs
the whole stack in docker-compose — a real htcondordb server preloaded with sample
ads (`e2e/sample-data.sql`) plus Grafana with this plugin — and drives the UI to
**configure the datasource** (and pass its health check) and **build a dashboard
panel** that queries the data.

```sh
npm run e2e        # build dist -> compose up --wait -> playwright test
npm run e2e:down   # stop + remove the stack
```

The htcondordb container runs with a throwaway anonymous-read/write config so the
plugin connects without credentials; Grafana loads the unsigned plugin and a
provisioned datasource. CI runs this as the `E2E (Playwright)` job.

## Limitations / follow-ups

- **No `$__timeGroup` macro.** The htcondordb SQL engine's SELECT/GROUP BY accept
  attribute names only, not computed expressions, so a time-bucketing macro cannot
  be a pure-SQL rewrite. Per-row time series already work (a `TimeField` column
  renders as a Grafana time field); bucketed aggregation-over-time would need
  backend-side bucketing — a planned follow-up.
- Streaming projects attributes as strings (time / key / kind + selected attrs);
  typed streamed fields are a possible enhancement.

## Status

Backend implemented and unit-tested (`GOWORK=off go test ./pkg/...`): pooled
connection, builder/SQL query model + macros, `repl.Result` → frame mapping, WATCH
streaming, and the discovery resources. Frontend builds to `dist/module.js`
(`npm run typecheck && npm run build`).
