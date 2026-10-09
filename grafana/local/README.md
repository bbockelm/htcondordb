# Local Grafana against a remote htcondordb

Runs Grafana OSS in Docker on your laptop with the htcondordb datasource plugin
mounted, pointed at a **remote** htcondordb (e.g. ap40's). Nothing but Grafana
runs locally -- the plugin's Go backend dials the remote daemon over CEDAR.

```
laptop                                        ap40
┌─────────────────────────────┐              ┌──────────────────┐
│ docker: grafana-oss         │              │ condor_shared_port│
│   └─ gpx_htcondordb ────────┼──CEDAR:9618──┼─> htcondordb      │
└─────────────────────────────┘              └──────────────────┘
        localhost:3000
```

## 1. Build the plugin

From `grafana/` (the parent directory):

```sh
npm ci && npm run build                     # -> dist/module.js, plugin.json, img/
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build -o dist/gpx_htcondordb_linux_arm64 ./pkg
```

Use `GOARCH=amd64` on an Intel machine. The arch must match the **container's**,
not the host's: Grafana's image is Linux, so always `GOOS=linux`. The backend is
Unix-only (htcondordb's stack uses Unix syscalls), so there is no Windows build.

## 2. Name the database

The stack names the database through its pool, the way `condor_status -pool ...
-name ...` does, so the plugin asks the collector where it is on every connection.
A daemon restart changes its address; this keeps working anyway.

List what a pool has:

```sh
condor_status -pool cm-1.ospool.osg-htc.org -any \
    -constraint 'MyType=="HTCondorDB"' -af Name MyAddress
```

```
htcondordb@ap40.uw.osg-htc.org          <128.105.68.62:9618?sock=htcondordb_9188_c841>
htcondordb@ospool-ap4043.chtc.wisc.edu  <128.105.68.226:9618?sock=htcondordb_8310_8c1a>
```

Put the collector in `HTCONDORDB_POOL` and the database in `HTCONDORDB_NAME`. The
name matches the advertised name whole, or the host alone, or `name@host`. Leave it
blank only if the pool has one database; with several the connection fails and
lists them rather than guessing.

To pin an address instead, drop `pool`/`name` from
`provisioning/datasources/htcondordb.yaml` and set `address` to the sinful string
(adding `?alias=<hostname>` so TLS has a hostname to validate). Then a restart does
mean editing it.

## 3. Get a token

```sh
ssh ap40.chtc.wisc.edu condor_token_fetch -lifetime 86400
```

Paste the whole line into `HTCONDORDB_TOKEN` in `.env`.

The token must be issued by the **same trust domain as the server** -- for ap40
that is `flock.opensciencegrid.org`, not the hostname
(`condor_config_val TRUST_DOMAIN` on the AP tells you). A token from another
issuer is filtered out client-side and never offered, so the failure reads
"all authentication methods failed" with TOKEN absent from the list rather than
"bad token". **An expired token fails exactly the same way** -- silently absent,
not rejected. If TOKEN is missing from the attempted methods, suspect expiry
first.

Note that the plugin takes the token *inline*; it does not read
`~/.condor/tokens.d`. The Grafana container has no HTCondor config, so there is
no `SEC_TOKEN_DIRECTORY` for it to search.

### If the token stops working after an hour

Daemons running cedar older than v0.7.2 reject a token an hour after it was
issued, whatever `-lifetime` you asked for. Re-fetch the token, or set
`SEC_TOKEN_MAX_AGE = 31536000` on the server. Fixed from cedar v0.7.2.

## 4. Run it

```sh
cp env.example .env     # paste in the address and the token
docker compose up
```

Grafana is on <http://localhost:3000> with anonymous admin -- no login. The
datasource is already provisioned as **htcondordb**; check it under
*Connections -> Data sources*.

## 5. Try it

Explore -> htcondordb -> **SQL** mode:

```sql
SELECT Owner, COUNT(*) AS n FROM jobs GROUP BY Owner ORDER BY n DESC
```

A time series (set the panel format to *Time series*):

```sql
SELECT time_bucket(QDate, '1h') AS time, COUNT(*) AS metric_jobs
FROM jobs
WHERE $__timeFilter(QDate)
GROUP BY time_bucket(QDate, '1h')
ORDER BY time
```

In **Builder** mode, setting Format to *Time series* and picking a Time field
emits the `time_bucket(<field>, $__interval)` for you, so the buckets follow the
panel as you zoom.

The plugin also ships an **HTCondorDB Overview** dashboard -- import it from the
plugin's Dashboards tab.

## Notes

- `.env` holds a credential and is gitignored. Keep it that way.
- **SSL auth to ap40 does not work**: its host certificate
  (`ospool-ap2140.chtc.wisc.edu`) expired 2026-02-17. IDTOKEN is the only way in,
  with no fallback -- which is why a missing or expired token means no connection
  at all rather than a degraded one.
- The plugin is unsigned, hence
  `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS` in the compose file. Grafana logs a
  warning about this at startup; it is expected.
- `docker compose logs -f grafana | grep bbockelm` shows the backend's own log
  lines, which is where connection errors surface in full.
