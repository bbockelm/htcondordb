# htcondordb — Python DB-API driver

A [PEP 249](https://peps.python.org/pep-0249/) (DB-API 2.0) driver for htcondordb, so
Python code can run SQL against the store the same way it would against SQLite or Postgres.

```python
import htcondordb

with htcondordb.connect("collector.example.edu:9618") as conn:
    for owner, n in conn.execute(
        "SELECT Owner, COUNT(*) AS n FROM jobs WHERE JobStatus = ? GROUP BY Owner", (2,)
    ):
        print(owner, n)
```

Because it is DB-API, pandas works with no adapter:

```python
import pandas as pd
df = pd.read_sql("SELECT Owner, RequestMemory, QDate FROM jobs LIMIT 5000", conn)
```

## How it works

The driver is a thin layer over the Go client in [`../capi`](../capi), loaded through cffi.
That client carries the whole stack — CEDAR transport, HTCondor authentication, the dbrpc
protocol, and the same SQL parser and executor `htcondordb-cli` uses — so this package
reimplements none of it and inherits new SQL features as the daemon gains them.

```
Python  ->  cffi (ABI mode, dlopen)  ->  libhtcondordb_client  ->  CEDAR  ->  htcondordb
```

`hcdb_sql` runs one statement and returns a JSON result document; the driver decodes it into
rows. Cell types are recovered on the Go side from the underlying ClassAd values where they
exist, so a string attribute whose text happens to be `0042` comes back as `"0042"`, not
`42`.

## Installing

Build the shared library first — it is not on PyPI and there are no wheels yet:

```sh
make lib                    # writes bin/libhtcondordb_client.{so,dylib}
pip install ./python
export HTCONDORDB_LIBRARY=$PWD/bin/libhtcondordb_client.so
```

The driver searches, in order: `$HTCONDORDB_LIBRARY`, a copy bundled inside the installed
package (`htcondordb/_lib/`), the repo's `bin/` for an editable checkout, then the dynamic
loader's own search path.

`cffi` is the only hard dependency. HTCondor's `classad2` bindings are optional and needed
only by `Cursor.fetchads()`.

## Authentication

There are no credential arguments to `connect()`. HTCondor's security configuration is
ambient: the library reads `CONDOR_CONFIG` and authenticates exactly as `htcondordb-cli`
does — pool token, SSL, FS, whatever the configuration allows. Point `CONDOR_CONFIG` at a
different file to authenticate differently.

A client that cannot authenticate still connects, but the daemon authorizes it **READ-only**
and strips private attributes. Writes then fail with `ProgrammingError`, carrying the
daemon's own hint about what to check.

## API notes

Standard DB-API, with a few things worth knowing:

| | |
|---|---|
| `paramstyle` | `qmark` — `?` placeholders, positional |
| `threadsafety` | `2` — connections are shareable across threads, cursors are not |
| `commit()` | No-op. Each statement commits as it runs. |
| `rollback()` | Raises `NotSupportedError` — there is no transaction to undo |
| `description` | 7-tuples; only `name` and `type_code` are meaningful (ClassAd is dynamically typed) |

**Always use placeholders.** The daemon has no server-side bind parameters, so the driver
renders literals itself — carefully, and with tests — in
[`_params.py`](htcondordb/_params.py). Formatting values into a statement yourself gives up
that protection.

Types map as follows. `datetime` becomes epoch seconds because that is how HTCondor stores
times (`QDate`, `CompletionDate`); read them back with `TimestampFromTicks`.

| Python | ClassAd |
|---|---|
| `None` | `UNDEFINED` |
| `bool` | `true` / `false` |
| `int` | integer (must fit in int64) |
| `float`, `Decimal` | real |
| `str` | string |
| `datetime`, `date` | epoch seconds (integer) |
| `list`, `tuple` | list literal `{...}` |
| `bytes` | *unsupported* — ClassAd has no binary type |

Beyond PEP 249, `Connection.execute()` is a one-shot cursor shortcut (the `sqlite3` shape),
and `Cursor.execute_with_ads()` plus `Cursor.fetchads()` return whole rows as
`classad2.ClassAd` objects when you want the ad rather than a projected tuple.

A statement's whole result is materialized when `execute()` returns — the daemon's executor
builds it in full before replying — so bound large queries with `LIMIT`.

## Known issue: backslashes and control characters on write

Writing a string containing a backslash, tab, or newline through SQL corrupts it: one
backslash comes back as four.

This is **upstream of the driver** and reproduces identically in `htcondordb-cli`:

```
$ htcondordb-cli -e "INSERT INTO t (Key,V) VALUES ('k','a\b')"
$ htcondordb-cli -e "SELECT V FROM t"
a\\\\b
```

The cause is in the classad library: `classad.ParseOld` unescapes `\"` in an old-format
string literal but not `\\` or `\t`, while the SQL layer's `quoteClassAd` escapes all three.
The mismatch doubles on each pass, and INSERT makes two passes where UPDATE makes one.

Reads are unaffected — a backslash already in the store comes back exactly. Since reporting
clients almost only read, this is rarely hit in practice, but it does mean the driver should
not be used to load data containing Windows paths or regexes until the upstream fix lands.
`test_integration.py::TestTypes::test_backslash_round_trip` is a strict `xfail` and will
start failing (flagging the fix) once it is corrected.

## Testing

```sh
make lib daemon            # the library, and the daemon the integration tests talk to
make python-test           # or: pytest python/tests
```

The integration tests start a private daemon on a free port with a throwaway config and
store, using FS authentication so they get WRITE. They skip themselves when the daemon
binary is not built; the unit tests need nothing.
