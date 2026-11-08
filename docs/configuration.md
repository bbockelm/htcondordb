# Configuration

htcondordb is configured through the HTCondor config it inherits from
`condor_master` (plus a couple of environment overrides for clients).

## Common setups

Each block goes in `/etc/condor/config.d/50-htcondordb.conf` and takes effect on
`condor_restart -master` (or `condor_reconfig` for anything but
`DAEMON_LIST`/`DC_DAEMON_LIST`).

### Mirror the local schedd

The usual deployment, and the whole minimum — the file paths default to the
schedd's own:

```conf
DAEMON_LIST    = $(DAEMON_LIST), HTCONDORDB
DC_DAEMON_LIST = $(DC_DAEMON_LIST) +HTCONDORDB

HTCONDORDB_SYNC_SCHEDD = true
```

### Keep a bounded amount of history

Archives grow without limit until you cap them:

```conf
HTCONDORDB_SYNC_SCHEDD = true

# applies to history and epoch_history
HTCONDORDB_ARCHIVE_MAX_BYTES = 20 GB
# narrower cap for one of them
HTCONDORDB_EPOCH_HISTORY_MAX_BYTES = 5 GB
```

### Record what jobs actually used

Adds a resource-usage time series off the same file the mirror already reads:

```conf
HTCONDORDB_SYNC_SCHEDD = true
HTCONDORDB_JOB_METRICS = true

HTCONDORDB_JOB_METRICS_MAX_AGE = 30d
# extra dimensions to GROUP BY
HTCONDORDB_JOB_METRICS_ATTRS             = ProjectName
HTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS = ProjectName
```

### Group history by a dimension of your own

An unindexed `GROUP BY` is a full scan, so index what you group by:

```conf
HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS = AccountingGroup, ProjectName
```

### Serve Arrow-native clients

For notebooks, pandas, Polars and DuckDB. Read-only, and off until you set an
address:

```conf
HTCONDORDB_FLIGHT_ADDRESS = 127.0.0.1:9095
```

See [Arrow Flight SQL](arrow-flight-sql.md) for the client side.

### Point a client somewhere else

`HTCONDORDB_ADDRESS_FILE` and `HTCONDORDB_HOST` are read from the environment
too, which is how a client with no command line (the Python driver's
`connect()`) reaches a daemon other than the local one:

```sh
export HTCONDORDB_HOST='<db.example.org:9618?sock=htcondordb>'
python -c "import htcondordb; print(htcondordb.connect().execute('SELECT COUNT(*) FROM jobs').fetchone())"
```

## Address resolution

`HTCONDORDB_ADDRESS_FILE` and `HTCONDORDB_HOST` are also read from the
environment, where they take precedence over the configuration (and over each
other in the same order). That is how a client with no command line — the Python
driver's `connect()` — is pointed at a different daemon. They apply as a pair:
setting either in the environment makes the environment the only source of both,
so overriding just the host cannot lose to a configured address file. Every
client in the tree resolves through `locate.Daemon`, and the daemon publishes to
`locate.AddressFilePath`, so a redirect moves both halves together.

### The certificate name in the advertised address

The address the daemon publishes — to the collector, and to its address file —
carries an `alias=<fqdn>` parameter naming the host an SSL client should check
the daemon's certificate against. It matters because behind shared port the
address host is the shared-port server's IP, and HTCondor host certificates
carry DNS names and no IP SANs: without the alias, a client that found the
daemon through the collector has only that IP to verify and SSL authentication
fails the certificate check.

The name is `HOST_ALIAS`, which defaults to `$(FULL_HOSTNAME)` — the same knob
and the same default the C++ daemons use — so an ordinary host needs no
configuration. Set `HOST_ALIAS` where the certificate is issued under some other
name, such as a load balancer or a CNAME clients reach the host by. The
advertised address is logged at startup, so the alias in effect is visible
there.

## Knobs

| Knob | Default | Meaning |
|------|---------|---------|
| `HTCONDORDB_DIR` | `$(SPOOL)/htcondordb` | On-disk database directory. |
| `HTCONDORDB_ADDRESS_FILE` | `$(LOG)/.htcondordb_address` | Where the command address is published, and where clients look for it. Written once the daemon is ready to serve, so a script that waits for the file has a readiness signal. Also read from the environment. |
| `HTCONDORDB_HOST` | — | Client fallback when no address file is readable. Also read from the environment. |
| `HTCONDORDB_QUEUE_POOL_SIZE` | `4` | **Client side** — set in the configuration of the daemon that embeds the C queue client (the schedd answering `condor_q` from a mirror), not this daemon's. How many dbrpc sessions concurrent queries may spread over, so that simultaneous full dumps do not queue behind each other. Raise it on a submit host serving many simultaneous queries; `1` restores a single shared session. Most of the gain is in the first few sessions. |
| `HTCONDORDB_CLIENT_PPROF` | unset | **Client side**, and a diagnostic — read from the environment of the process that embeds the C client, so for the schedd it is set through `SCHEDD_ENVIRONMENT`. An address (`127.0.0.1:9096`) serves Go pprof for the client half, which neither the schedd's own profiler nor the database's can see. Unset, nothing listens. Keep it on loopback — it serves heap contents, goroutine stacks and CPU profiles; a non-loopback address is accepted but warned about. |
| `HTCONDORDB_HA_MODE` | `standalone` | `standalone` / `leader-follower` / `consistent`. See [HA](ha.md). |
| `HTCONDORDB_ROLE` | `leader` | Leader-follower role. |
| `HTCONDORDB_LEADER` | — | Leader address (follower). |
| `HTCONDORDB_CURSOR_FILE` | `$(SPOOL)/htcondordb/.replica_cursor` | Follower stream cursor. |
| `HTCONDORDB_RAFT_BOOTSTRAP` | `false` | This node initializes a fresh raft cluster. |
| `HTCONDORDB_RAFT_PEERS` | — | Explicit `id@addr` member list. |
| `HTCONDORDB_RAFT_SIZE` | `0` | Cluster size `N` for first-N-hosts bootstrap. |
| `HTCONDORDB_NODE_ID` | advertised address | This node's stable raft id. |
| `HTCONDORDB_SYNC_SCHEDD` | `false` | Mirror a local schedd: `job_queue.log`→`jobs` table + `history`→`history` archive. The mirrored tables are read-only to clients while sync is on. See [Schedd sync](schedd-sync.md). |
| `HTCONDORDB_JOB_QUEUE_LOG` | `$(JOB_QUEUE_LOG)` | Schedd job-queue log to tail (live `jobs`). |
| `HTCONDORDB_HISTORY` | `$(HISTORY)` | Schedd history file to tail (`history` archive). |
| `HTCONDORDB_MIRRORED_SCHEDD_NAME` | the schedd's own `Name` | The schedd this spoke mirrors, advertised as `MirroredScheddName` (collector ad, `DBSyncStatus`, `syncstatus` row) when schedd sync runs. Unset uses the `Name` the schedd writes into its address file (HTCondor 25.x+), else follows the schedd's naming: `SCHEDD.SCHEDD_NAME`/`SCHEDD_NAME` used as is when it contains `@`, the full hostname when it names this host, else `name@$(FULL_HOSTNAME)`; with no `SCHEDD_NAME`, `$(FULL_HOSTNAME)` (or `user@$(FULL_HOSTNAME)` for a non-condor user); nothing without `FULL_HOSTNAME`. The chosen name and the rule are logged at startup. See [Naming the mirrored schedd](schedd-sync.md#naming-the-mirrored-schedd). |
| `HTCONDORDB_MIRRORED_SCHEDD_ADDRESS_FILE` | `SCHEDD_ADDRESS_FILE` | The mirrored schedd's address file. `MirroredScheddAddress` is its first line (the schedd's private address when it has one), omitted when unreadable. Set it when mirroring a schedd other than the host's primary one (a local-name `SCHEDD2` writes `SCHEDD2.SCHEDD_ADDRESS_FILE`). |
| `HTCONDORDB_SYNCSTATUS_INTERVAL` | `5` | Seconds between rewrites of the `syncstatus` heartbeat row (key `status`) while schedd sync runs. A federation hub reads per-AP freshness from it. A duration suffix (`5s`, `1m`) is accepted; a sub-second value (`500ms`) is rejected (logged) and the default used. See [Federation](federation.md#spoke-side). |
| `HTCONDORDB_FEDERATE_SCHEDD_CONSTRAINT` | — | Run as a [federation hub](federation.md) over the schedds matching this ScheddAd constraint (needs `COLLECTOR_HOST`). Refused together with `HTCONDORDB_SYNC_SCHEDD`. |
| `HTCONDORDB_FEDERATE_SPOKES` | — | Static hub spokes, by name; each needs `HTCONDORDB_FEDERATE_SPOKE_<NAME>_ADDRESS` and may set `HTCONDORDB_FEDERATE_SPOKE_<NAME>_SCHEDD` (default `<NAME>`). Static spokes skip host validation. Also enables hub mode. |
| `HTCONDORDB_FEDERATE_SPOKE_<NAME>_ADDRESS` | — | A static spoke's command address. |
| `HTCONDORDB_FEDERATE_SPOKE_<NAME>_SCHEDD` | `<NAME>` | The schedd a static spoke mirrors; its rows carry this `ScheddName`. |
| `HTCONDORDB_FEDERATE_TABLES` | `jobs history syncstatus` | Spoke tables a hub fans in (`epoch_history` also supported). |
| `HTCONDORDB_FEDERATE_RETIRE_AFTER` | `7d` | A hub deletes an AP's mutable rows once it has been unseen, or unmatched after a constraint change, this long. Seconds or a suffix (`36h`, `7d`). Archive rows are never deleted by retirement. |
| `HTCONDORDB_FEDERATE_FRESH_SECONDS` | `60` | A hub source at or under this staleness is `fresh`. |
| `HTCONDORDB_FEDERATE_DISCOVER_INTERVAL` | `60` | Seconds between a hub's collector discovery passes. |
| `HTCONDORDB_FEDERATE_STATE_INTERVAL` | `5` | Seconds between a hub's source-state refreshes (`federation_sources`, ad summary, metrics). |
| `HTCONDORDB_ARCHIVE_ROTATE_INTERVAL` | `3600` | Archive-table retention sweep interval (seconds; `0` disables). |
| `HTCONDORDB_ARCHIVE_INDEX_MIN_DEMAND` | `100` | Query count at which the maintenance pass adds an index to an archive table on its own. Demand decays with a 7-day half-life, so the default asks for a sustained read pattern rather than one afternoon of ad-hoc queries. An auto-added archive index is never auto-dropped. A negative value turns archive index auto-tune off. |
| `HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS` | — | Categorical (string-equality) indexes on the `history` and `epoch_history` archives **in addition to** each table's built-in set (see [Schedd sync](schedd-sync.md#built-in-indexes)). Name grouping dimensions such as `AccountingGroup` or `ProjectName`: an unindexed `GROUP BY` is a full scan. Naming an attribute here that the built-in set indexes as a value index makes it categorical. Applied to an existing archive on the next start or `condor_reconfig`, backfilled in the background. Add-only. |
| `HTCONDORDB_ARCHIVE_INDEX_BACKFILL_BYTES` | `256 MiB` | Per-pass budget, in stored segment bytes, for carrying the index set to archive segments sealed before an index was added. Adding an index (by hand or by auto-tune) rebuilds only the newest 1 GiB of the archive at once; the maintenance pass rebuilds the older segments that lack it, newest first, this much per pass, so a large history is covered over several passes without one long stall. Accepts a unit suffix (`512 MiB`) or plain bytes; `0` turns the backfill off. `.indexes <table>` shows what is still pending; `.reindex <table>` covers the whole archive at once. |
| `HTCONDORDB_ARCHIVE_VALUE_ATTRS` | — | Numeric (equality and range) indexes on the `history` and `epoch_history` archives in addition to the built-in set. Same rules as `HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS`. |
| `HTCONDORDB_SCHEMA_REQUIRE_ATTRS` | `ResidentSetSize, ProportionalSetSize, CurrentResidentSetSize, MemoryUsage, ImageSize, DiskUsage, RemoteWallClockTime, CommittedTime, RemoteUserCpu, RemoteSysCpu, BytesSent, BytesRecvd` | Attributes the columnar schema must carry on `jobs`, `history` and `epoch_history` whatever their presence rate — otherwise an attribute needs to appear on ~90% of sampled ads, which on a job table measures how far the jobs got rather than what anyone queries. When unset, `history` additionally requires `CompletionDate` and `epoch_history` additionally requires `VacateReasonCode`; **a configured list replaces the defaults** rather than adding to them, since every required field costs bytes in every record. `none` requires nothing. A change takes effect gradually: the next maintenance pass adopts the new schema and each pass after it rewrites 64 sealed segments onto it, with exact answers throughout. `.schema rebuild <table>` adopts it immediately; `.schema require` shows any attribute the derivation refused. |
| `HTCONDORDB_ARCHIVE_MAX_BYTES` | — | Default on-disk size cap applied to **both** schedd-sync archives (`history`, `epoch_history`). Oldest whole segments are dropped past the cap on the retention sweep. Accepts a unit suffix (`10 GB`, `500MiB`) or plain bytes; unset/`0` = no limit. |
| `HTCONDORDB_HISTORY_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for the `history` archive; overrides the shared default (set to `0` to uncap this table while the default caps the other). |
| `HTCONDORDB_EPOCH_HISTORY_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for the `epoch_history` archive; overrides the shared default. |
| `HTCONDORDB_JOB_METRICS` | `false` | Sample running jobs' resource usage (memory/CPU/disk/IO/GPU) into the `job_metrics` archive, off the same `job_queue.log` stream. See [Schedd sync](schedd-sync.md#job-resource-metrics). |
| `HTCONDORDB_JOB_METRICS_ATTRS` | — | Additional job attributes copied onto every sample — an `AccountingGroup`, a `ProjectName`, or a metric the job publishes with `condor_chirp` (all already flow through `job_queue.log`). |
| `HTCONDORDB_JOB_METRICS_DERIVED` | — | Names computed columns, each evaluated once per sample from `HTCONDORDB_JOB_METRICS_DERIVED_<NAME>`. Use this for a dimension you will `GROUP BY`: a computed group key is evaluated client-side, so every matching row crosses the wire, while a stored column groups server-side and can carry a categorical index. |
| `HTCONDORDB_JOB_METRICS_DERIVED_<NAME>` | — | The ClassAd expression for one computed column. It sees the whole finished sample, including the derived rates — so `CpuUtil / RequestCpus` works, which no query can express (an aggregate's argument may not contain an expression, and subqueries are unsupported). Undefined results are not stored. |
| `HTCONDORDB_JOB_METRICS_CATEGORICAL_ATTRS` | — | Categorical (string-equality) indexes on `job_metrics` **in addition to** the built-in set (see [Schedd sync](schedd-sync.md#built-in-indexes)). An unindexed `GROUP BY` is a full scan. Applied to an **existing** archive too: a newly-named attribute is added on the next start or `condor_reconfig` and backfilled in the background. Add-only — an index created by hand is never dropped. |
| `HTCONDORDB_JOB_METRICS_MIN_INTERVAL` | `0` | Minimum spacing between samples for one job; a volume backstop. A state change, a new run and the run's endpoint are never throttled. A bare number is seconds; a duration suffix (`5m`, `2h`, `1d`) is accepted. |
| `HTCONDORDB_JOB_METRICS_SEGMENT_SIZE` | library default (8 MiB) | Sealed-segment size for `job_metrics` (create-time only). Accepts a unit suffix (`8 MiB`); `0` means the library default. Values below 64 KiB or at/above 4 GiB are refused (logged, default used). Leave it alone unless you have measured: bytes per record is **not** monotone in segment size, and 8 MiB measured best of 2/8/32/64 MiB (2 MiB cost 1.5x the storage for a 4% faster recent-range query). |
| `HTCONDORDB_JOB_METRICS_MAX_BYTES` | `$(HTCONDORDB_ARCHIVE_MAX_BYTES)` | Size cap for `job_metrics`; overrides the shared default (`0` uncaps this table). |
| `HTCONDORDB_JOB_METRICS_MAX_AGE` | — | Age cap measured against `SampleTime`. A bare number is seconds; a duration suffix (`30d`, `720h`) is accepted. Both caps apply; whichever binds first drops the oldest whole segments. |
| `HTCONDORDB_JOB_METRICS_GROUP_SCHEMAS` | `true` | **Create-time only** — on an existing archive this is ignored; set it before first enabling sampling, or drop the table. Gives a columnar fast path to attributes only *some* jobs carry (GPU metrics, a container universe's `NetworkIn`/`NetworkOut`), which otherwise fall below the 90% presence the base schema needs and are stored as rows. Measured at +60% bytes/record on a mixed pool; a pool with no GPUs and no containers pays nothing either way. |
| `HTCONDORDB_FLIGHT_ADDRESS` | — | Serve **Arrow Flight SQL** on this address (`127.0.0.1:9095`, `:9095`). Unset, nothing listens and no gRPC port is opened. Lets Arrow-native clients — anything speaking ADBC, and so pandas, Polars or DuckDB — read tables as Arrow record batches. Read-only: there is no write surface at all. See [Arrow Flight SQL](arrow-flight-sql.md). |
| `HTCONDORDB_FLIGHT_AUTHENTICATION` | `OPTIONAL` | `OPTIONAL` / `SCITOKEN` / `NONE`. `OPTIONAL` follows CEDAR rather than being stricter: a credential is verified when one is offered, otherwise the connection gets the anonymous identity and `ALLOW_READ` decides — so this door exposes exactly what the same caller could already read over CEDAR, under the same policy. `SCITOKEN` requires a bearer token from a trusted issuer. `NONE` serves every caller as anonymous. |
| `HTCONDORDB_FLIGHT_SCITOKEN_ISSUERS` | — | Trusted token issuers, required by `SCITOKEN` and refused without it. |
| `HTCONDORDB_FLIGHT_STREAM_MEMORY` | `0` (unlimited) | Bytes of Arrow memory one result stream may hold. A stream past it is refused with `ResourceExhausted` naming the limit, rather than growing the daemon's heap until something else fails. Accepts a unit suffix. |
| `HTCONDORDB_FLIGHT_MAX_STREAMS` | `0` (unlimited) | Concurrent result streams. Past it a client is refused immediately and told to retry, which costs nothing, instead of queueing behind work the daemon has not got the memory for. |
| `HTCONDORDB_FLIGHT_SCHEMA_TTL` | `30s` | How long a table's derived Arrow schema is reused, so a client looping over a prepared statement does not pay a sampling scan per call. The trade is staleness: a column that appears in the table shows up in the schema only once the entry ages out — set `0` if you are watching one arrive. A bare number is seconds; a suffix (`90s`, `5m`) works too. Entries are scoped to the caller's authorization. |
| `HTCONDORDB_FLIGHT_TIME_ATTRS` | — | Extra attributes holding unix-epoch **seconds**, to be served as Arrow timestamps rather than integers. Nothing can infer this from the values, so it is declared. `QDate`, `CompletionDate`, `JobStartDate` and similar well-known job attributes are already recognized. |
| `HTCONDORDB_FLIGHT_EXCLUDE_ATTRS` | — | Attributes to give no typed column, whatever the measurement says. Use it for an attribute whose **text** is the content — `Requirements` evaluates to a boolean that means nothing without a machine to match against. Excluding an attribute does not hide it; its text still arrives in the `_ad` tail column. |
| `HTCONDORDB_FLIGHT_ATTR_KINDS` | — | Forced column types, as `Attribute:kind` entries. Kinds: `int`, `real`, `bool`, `string`, `timestamp`. For the attribute whose type is known and whose sample happens not to show it. A forced kind the data does not hold is allowed and is not silent: the encoder counts every value it then cannot store. A malformed or unknown entry is logged and ignored rather than failing startup. |
| `HTCONDORDB_FLIGHT_COLUMN_TYPES_FILE` | beside the database | Where the endpoint remembers each table's measured column types, so a column keeps its position and its settled type across a restart and a client's cached catalog stays valid. Not a schema you author — losing it costs a re-measurement, not an answer. A column's position never moves, so a cached schema is always a prefix of the current one. `none` turns persistence off. |
| `HTCONDORDB_FLIGHT_STRICT_MIXED_KINDS` | `false` | Refuse an attribute whose kinds genuinely disagree instead of serving it as a text column. Off by default because a text column is a faithful representation of mixed data; turn it on to hear about the disagreement rather than read around it. |
| `HTCONDORDB_CHANGEFEED_ADDRESS` | — | Serve the HTTP/SSE change feed on this address (`:9620`), from which a non-CEDAR sink pulls this database's tables and archives. Unset, nothing listens. |
| `HTCONDORDB_CHANGEFEED_ADDRESS_FILE` | — | Where the feed's bound address is written (useful with a `:0` port). |
| `HTCONDORDB_CHANGEFEED_TOKEN` | — | Bearer token a subscriber must present. Its subscribers are **unprivileged**: every event is decoded without the data key, so a sealed value (a private attribute, or one named in `HTCONDORDB_ENCRYPT_ATTRS`) arrives as undefined, and a subscription whose constraint names a private attribute is refused (403). Unset together with `HTCONDORDB_CHANGEFEED_PRIVILEGED_TOKEN`, the feed admits anyone, unprivileged, and logs a warning. |
| `HTCONDORDB_CHANGEFEED_PRIVILEGED_TOKEN` | — | A second bearer token whose subscribers are **privileged**: sealed values arrive in plaintext and constraints may name private attributes. Give it only to a consumer trusted with every attribute the store seals. Must differ from `HTCONDORDB_CHANGEFEED_TOKEN`; the daemon refuses to start if they are equal. |
| `HTCONDORDB_CHANGEFEED_AGE_ATTR` | — | Record attribute (unix seconds) stamped on each event as its GC timestamp, e.g. `EnteredHistoryTime`. Set, a served archive's retention keeps records no subscriber has acknowledged; unset, the acknowledged floor is only logged. |
| `HTCONDORDB_CHANGEFEED_MIN_AGE_SECONDS` | — | With `HTCONDORDB_CHANGEFEED_AGE_ATTR`, the minimum age before acknowledged records may be dropped. The archive's size and age caps still win. |
| `HTCONDORDB_CHANGEFEED_LEASE_SECONDS` | `3600` | How long a subscriber that stops acknowledging holds back the GC floor before it is evicted. |

| `HTCONDORDB_WASM_FUNCTIONS` | — | Names of WebAssembly modules to load as ClassAd **functions**, so a query or a constraint can call one. Each needs `HTCONDORDB_WASM_<NAME>_PATH` and `_MIN_ARGS`. See [User functions](user-functions.md). |
| `HTCONDORDB_WASM_AGGREGATES` | — | Names of WebAssembly modules to load as **aggregates**, reduced the way `SUM` is. Each needs `HTCONDORDB_WASM_<NAME>_PATH`. |
| `HTCONDORDB_WASM_<NAME>_PATH` | — | The module file. Required. |
| `HTCONDORDB_WASM_<NAME>_MIN_ARGS` | — | How many arguments the function takes. Required for a function; it is checked before the module runs, so there is no safe default. |
| `HTCONDORDB_WASM_<NAME>_MAX_ARGS` | `_MIN_ARGS` | The most it takes; `-1` is variadic. |
| `HTCONDORDB_WASM_<NAME>_TRUSTED` | `false` | Drop the wall-clock deadline and run the module uninterruptibly, which is most of the per-call cost. The step budget, the memory cap and every validation still apply. A trusted module that loops forever hangs a goroutine with nothing able to stop it, so set this only for a module you built. |
| `HTCONDORDB_WASM_MEMORY_PAGES` | `128` | Linear memory per module, in 64 KiB pages. |
| `HTCONDORDB_WASM_STEPS` | `10000000` | The deterministic step budget a module charges itself against. |
| `HTCONDORDB_WASM_TIMEOUT` | `1` | Seconds of wall clock before a call is abandoned. This is the backstop that stops a module which charges itself nothing. |
| `HTCONDORDB_WASM_MAX_INSTANCES` | `GOMAXPROCS` | Concurrent instances of one module. |
| `HTCONDORDB_SCRIPT_FUNCTIONS` | — | Names of ClassAd Script functions to register, so a query or a constraint can call one. Each needs `HTCONDORDB_SCRIPT_<NAME>_PATH`. The name is the name of the `def` to take out of that file. See [User functions](user-functions.md). |
| `HTCONDORDB_SCRIPT_AGGREGATES` | — | Names of ClassAd Script **aggregates** to register, reduced the way `SUM` is. Each needs `HTCONDORDB_SCRIPT_<NAME>_PATH`. An aggregate is four `def`s, taken out of that file by prefix: `<name>_init`, `<name>_add`, `<name>_merge` and `<name>_result`. |
| `HTCONDORDB_SCRIPT_<NAME>_PATH` | — | The script file. Required. Two names may give the same file and pick a different `def` out of it. |
| `HTCONDORDB_SCRIPT_<NAME>_INIT` | `<name>_init` | The `def` that gives an aggregate's identity, for a file whose definitions are spelled some other way. `_ADD`, `_MERGE` and `_RESULT` name the other three. A `def` named here and absent from the file fails start-up, saying which one it looked for. |

Standard `SEC_*` and `ALLOW_`/`DENY_` knobs configure security and authorization
— see [Authorization](authorization.md).

### Unparseable values

Every knob above that takes a size or a duration logs a value it cannot parse at `ERROR`, naming
the knob, and falls back to the safe direction: no cap, no throttle, the library default.

Versions before this parser existed read `30d` as **30 seconds** and `8 MiB` as **8 bytes**,
silently. If you are reading logs from an older deployment, check for `max_age_seconds` and
`SegmentSize` values far smaller than the configuration asks for.
