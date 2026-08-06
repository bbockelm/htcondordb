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
| `autocommit` | Defaults to `True` — see [Transactions](#transactions) |
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

## Transactions

The connection starts in **autocommit** mode: each statement commits as it runs, `commit()`
is a no-op, and `rollback()` raises. Opt into a transaction with the context manager:

```python
with conn.transaction():
    cur = conn.cursor()
    cur.executemany("INSERT INTO jobs (Key, Owner) VALUES (?, ?)", rows)
# committed here; rolled back instead if the block raised
```

or by setting `conn.autocommit = False` (also available as `connect(..., autocommit=False)`),
after which `commit()` and `rollback()` are real and a fresh transaction opens after each.

Autocommit is the default here, unlike most DB-API drivers, because a transaction carries
two constraints you should opt into knowingly. Neither is a driver limitation — a dbrpc
transaction is scoped to one table, and queries carry no transaction id:

- **Reads do not join the transaction.** A `SELECT` always reads committed state, so it will
  not see the transaction's own uncommitted writes. `UPDATE` and `DELETE` pick their rows
  with a query, so they also act on committed state.
- **A transaction cannot span tables.** It binds to the first table written; a write to a
  second table raises `ProgrammingError` and leaves the transaction open and usable for its
  own table.

DDL (`CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, views) is not transactional and applies
immediately, as in most databases.

The main reason to want a transaction is bulk writes: `executemany` inside one applies
atomically and in a single commit, instead of a begin/commit round trip per row.

## Iterating ClassAds

A column select *evaluates*: `SELECT Requirements` answers `True`/`False`, because that is
what a result cell can hold. When you want the expression itself, iterate ads instead:

```python
for ad in conn.ads("SELECT * FROM machines WHERE Cpus > ?", (4,)):
    print(ad["Name"])                    # "slot1@ep1"
    print(ad["Requirements"])            # (Start && WithinResourceLimits)  -- an ExprTree
    print(ad.eval("Requirements"))       # True                             -- evaluated
```

Rows **stream**: the next ad is fetched when you ask for it, so walking a large table costs
one ad rather than the whole set, and abandoning the iterator stops the query instead of
draining it. Close it explicitly (or use it as a context manager) if you stop early and want
the server released at a known moment.

This is what makes real matchmaking analysis possible from Python, which no arrangement of
result columns can express:

```python
import classad2
job  = classad2.ClassAd({"RequestCpus": 8})
fits = classad2.ExprTree("Cpus >= TARGET.RequestCpus")

eligible = [ad["Name"] for ad in conn.ads("SELECT * FROM machines")
            if fits.eval(scope=ad, target=job)]
```

`ad.lookup("Requirements").simplify(scope=ad)` folds away everything the machine already
settles and leaves what genuinely depends on the job.

Notes:

- **Parameters bind exactly as in `execute()`** — same `?` placeholders, same quoting, same
  rejections. The statement text is built by the same code before it reaches the library.
- **Requires `classad2`.** Without HTCondor's bindings this raises `InterfaceError` rather
  than falling back to text; live expressions are the whole point. `Cursor.ad_text` gives
  the unparsed text if that is what you want.
- **`ORDER BY`, `DISTINCT` and aggregates cannot stream** — none can emit a correct first row
  before seeing the last — so those are computed whole and then iterated.
- **An aggregate has no ads**, so one is synthesized per group row. Attribute names are
  derived from the headers: `COUNT(*)` becomes `Count`, `SUM(RequestMemory)` becomes
  `SumRequestMemory`, and an `AS` alias is used as-is when it is a legal attribute name. Give
  computed columns an alias if you care what they are called.
- Only `SELECT` is accepted; anything else raises immediately rather than on first
  iteration.

## ClassAd value shapes

A ClassAd column is not scalars-only, and is not homogeneous: two rows of the same column
can hold different types. Three things are worth knowing before writing a report.

**Stored expressions are evaluated, not returned.** A ClassAd attribute can hold an
unevaluated expression over its siblings — `Requirements`, `Rank`, `WithinResourceLimits`
and friends all do. A `SELECT` reports the *evaluated result* per row, so
`SELECT Requirements FROM jobs` answers `True`/`False`/`None`, never the expression text.
To see the expression itself, fetch the whole ad with `execute_with_ads()` and read it from
`ad_text` or `fetchads()`.

**Undefined and error both arrive as `None`.** They are distinct to the engine — an
unresolved reference versus a faulting one — but a reporting client treats both as a
missing cell and Python has one spelling for that. The visible consequence: a `GROUP BY`
over a column holding both produces two separate `None` groups.

**Composite values keep their ClassAd text.** Lists and nested ads come back as strings,
because their elements may themselves be expressions and there is no lossless mapping to a
Python list. Binding a Python list as a parameter works in both directions — as a `VALUES`
item and, more usefully, in a `member(Owner, ?)` membership test.

### Values that cannot be stored at all

Old-ClassAd text is newline-separated, and a value ending in a backslash puts that
backslash directly before the closing quote — where it makes the quote part of the value
and runs the string on. Both are refused with a clear error rather than silently mangled;
HTCondor's own old-format writer has the same two limitations.

Everything else round-trips, backslashes and tabs included. Two escaping bugs used to
corrupt them (one in the SQL layer's quoting, one in the store's raw-text rendering); both
are fixed as of classad v0.24.1, and `SELECT` of an expression-valued attribute now carries
the siblings that expression reads, so a narrow `SELECT Requirements` agrees with
`SELECT *`.

## Testing

```sh
make lib daemon            # the library, and the daemon the integration tests talk to
make python-test           # or: pytest python/tests
```

The integration tests start a private daemon on a free port with a throwaway config and
store, using FS authentication so they get WRITE. They skip themselves when the daemon
binary is not built; the unit tests need nothing.
