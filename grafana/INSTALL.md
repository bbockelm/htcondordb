# Installing the htcondordb Grafana datasource

For an administrator adding this datasource to a shared or production Grafana. For
a throwaway demo on a laptop, see [`local/README.md`](local/README.md) instead.

## What it does

The plugin has a Go backend that runs inside Grafana and opens an authenticated
CEDAR connection to an htcondordb daemon, running SQL through htcondordb's own
engine. Grafana never talks to the schedd, and no HTCondor configuration files are
needed on the Grafana host.

```
Grafana ──gRPC──> plugin backend ──CEDAR──> htcondordb ──> jobs / history / views
```

## Requirements

- **Grafana 10.4 or newer** (`grafanaDependency` in `plugin.json`).
- **Linux or macOS** for the Grafana host, amd64 or arm64. The backend uses
  Unix-specific syscalls and has no Windows build.
- Network reach from the Grafana host to the htcondordb daemon, which normally
  means **TCP 9618** (HTCondor's shared port) on the access point.
- An **HTCondor IDTOKEN** for the pool. See [Authentication](#authentication).

## 1. Install the plugin

Each htcondordb release attaches a prebuilt, ready-to-install plugin:
`bbockelm-htcondordb-datasource_<version>.zip`. It bundles the backend for Linux
and macOS on both amd64 and arm64, so there is nothing to select and nothing to
compile.

```sh
VER=v0.20.0
gh release download "$VER" --repo bbockelm/htcondordb \
    -p 'bbockelm-htcondordb-datasource_*.zip' -p SHA256SUMS.txt
shasum -a 256 -c SHA256SUMS.txt --ignore-missing
```

The archive unpacks to a directory already named for the plugin id, which is what
Grafana matches on, so unzip it straight into the plugin path:

```sh
unzip -d /var/lib/grafana/plugins "bbockelm-htcondordb-datasource_${VER}.zip"
chown -R grafana:grafana /var/lib/grafana/plugins/bbockelm-htcondordb-datasource
```

The plugin is **unsigned**, so Grafana refuses to load it until you allow it by id
in `grafana.ini`:

```ini
[plugins]
allow_loading_unsigned_plugins = bbockelm-htcondordb-datasource
```

Or, equivalently, in the environment:

```
GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=bbockelm-htcondordb-datasource
```

Restart Grafana. You should see, in its log:

```
level=warn msg="Plugin is unsigned" id=bbockelm-htcondordb-datasource
level=warn msg="Permitting unsigned plugin. This is not recommended"
level=info msg="Plugin registered" pluginId=bbockelm-htcondordb-datasource
```

The two warnings are expected. If only the first appears, the
`allow_loading_unsigned_plugins` setting did not take effect.

Building from source is only needed for a platform the release does not cover; see
[`README.md`](README.md).

## 2. Find the daemon's address

htcondordb advertises itself to the pool collector with `MyType == "HTCondorDB"`:

```sh
condor_status -pool <collector> -any \
    -constraint 'MyType=="HTCondorDB"' -af Name MyAddress
```

```
htcondordb@ap40.uw.osg-htc.org  <128.105.68.62:9618?sock=htcondordb_9188_c841>
```

Two things to plan around:

- The daemon sits **behind shared port**, so the port is 9618 and `sock=` selects
  the daemon. You do not need a dedicated port opened.
- **`sock=` contains the daemon's pid**, so the address changes every time
  htcondordb restarts. The datasource config accepts only an address -- it cannot
  locate through a collector the way `htcondordb-cli -pool ... -name ...` can -- so
  plan to update it after a restart. **If a working datasource suddenly fails,
  check this first.**

Append `?alias=<hostname>` if the daemon's host certificate is valid and you want
TLS to verify it; a bare IP address has no matching SAN.

## 3. Add the datasource

Either through the UI (*Connections -> Data sources -> Add -> HTCondorDB*), filling
in the address and token, or by provisioning
`/etc/grafana/provisioning/datasources/htcondordb.yaml`:

```yaml
apiVersion: 1
datasources:
  - name: htcondordb
    uid: htcondordb
    type: bbockelm-htcondordb-datasource
    access: proxy
    jsonData:
      address: "<128.105.68.62:9618?alias=ap40.uw.osg-htc.org&sock=htcondordb_9188_c841>"
      connectTimeoutSeconds: 30
    secureJsonData:
      token: "$HTCONDORDB_TOKEN"
```

Grafana expands environment variables in provisioning files, so keep the token in
the unit's environment (a systemd drop-in with `EnvironmentFile=`, say) rather than
in a file in configuration management. `secureJsonData` is encrypted at rest in
Grafana's database.

Click **Save & test**. Success reads `Connected to htcondordb`.

## Authentication

The plugin authenticates with an **HTCondor IDTOKEN**, supplied inline in the
datasource config. It does not read `~/.condor/tokens.d` -- the Grafana host has no
HTCondor configuration, so there is no token directory for it to search.

Mint one on the access point:

```sh
condor_token_fetch -lifetime 31536000
```

The token grants whatever that identity is authorized for. htcondordb reads are
`READ`-level, so a dedicated low-privilege identity is appropriate; there is no
need to hand Grafana an administrator's token. Leaving the token blank yields an
anonymous, read-only session, which works only if the server permits anonymous
`READ`.

One failure mode is worth knowing in advance: the token's issuer must match the
server's `TRUST_DOMAIN` (`condor_config_val TRUST_DOMAIN` on the AP), which is
often *not* the hostname. A token from another issuer is filtered out client-side
and never offered, so the error reads "all authentication methods failed" with
TOKEN absent from the list, rather than "bad token". An expired token looks the
same. Both are easy to mistake for "no token configured".

## Verify

From the Grafana host, confirm the daemon is reachable before blaming the plugin:

```sh
nc -vz <ap-hostname> 9618
```

Then in Grafana, *Explore -> htcondordb -> SQL*:

```sql
SELECT JobStatus, COUNT(*) AS n FROM jobs GROUP BY JobStatus ORDER BY JobStatus
```

For a graph, produce a column named `time`, set the panel's visualization to
**Time series**, and bucket a unix-epoch attribute:

```sql
SELECT time_bucket(QDate, '1h') AS time, COUNT(*) AS n
FROM jobs WHERE $__timeFilter(QDate)
GROUP BY time_bucket(QDate, '1h') ORDER BY time
```

The plugin ships two dashboards, **HTCondorDB Overview** and **HTCondorDB Job
Resource Usage**; import them from the plugin's Dashboards tab once a datasource
exists.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| Plugin absent from the datasource list | Directory not named `bbockelm-htcondordb-datasource`, or `allow_loading_unsigned_plugins` not set |
| `Plugin unavailable` / backend won't start | Unpacked without the execute bit on `gpx_htcondordb_*`, or an unsupported platform |
| `all authentication methods failed`, TOKEN not listed | No token configured, wrong trust domain, or the token expired -- all three look identical |
| `server rejected token (no reason returned by daemon)` | The daemon declined the token; its own log has the reason, the wire does not carry one |
| Worked yesterday, fails today | htcondordb restarted; `sock=` in the address is stale. Re-run the `condor_status` lookup |
| `Data is missing a time field` | The panel is a Time series visualization but the query has no time column. Switch to Table, or alias a bucketed epoch column `AS time` |

The daemon's own log is the authoritative source for any authentication failure --
the CEDAR wire protocol carries no reason. `grafana-server` logs the plugin backend
under `logger=plugin.bbockelm-htcondordb-datasource`.
