# htcondordb — Python DB-API driver

A [PEP 249](https://peps.python.org/pep-0249/) (DB-API 2.0) driver for htcondordb, so
Python code can run SQL against the store the same way it would against SQLite or Postgres.

```python
import htcondordb

with htcondordb.connect() as conn:
    for owner, n in conn.execute(
        "SELECT Owner, COUNT(*) AS n FROM jobs WHERE JobStatus = ? GROUP BY Owner", (2,)
    ):
        print(owner, n)
```

`connect()` with no argument finds the daemon the way `htcondordb-cli` does with no
`-addr`, and `conn.address` reports what it reached. Two knobs name the daemon, each
settable in the environment or in the HTCondor configuration:

| | |
|---|---|
| `HTCONDORDB_ADDRESS_FILE` | The file the daemon publishes its address to, by default `$(LOG)/.htcondordb_address`. Preferred, and re-read on every `connect()`, so a restarted daemon is followed. |
| `HTCONDORDB_HOST` | A static `host:port`, for a daemon on another machine. Used when no address file is configured, or when the configured one cannot be read. |

The environment wins over the configuration, and wins as a pair — so pointing a report at
another pool's daemon takes no config file and no code change:

```sh
HTCONDORDB_HOST=db.example.edu:9618 python report.py
```

```python
os.environ["HTCONDORDB_HOST"] = "db.example.edu:9618"   # before connect()
```

Passing an address — `htcondordb.connect("db.example.edu:9618")` — overrides both.

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

`hcdb_sql_stream` runs one statement and returns a header (columns, and for DML the affected
count) plus rows in batches the driver asks for. Cell types are recovered on the Go side from
the underlying ClassAd values, so a string attribute whose text happens to be `0042` comes back
as `"0042"`, not `42`.

## Large results

Rows arrive in batches as they are fetched, so iterating a cursor costs memory proportional to a
batch rather than to the result:

```python
for owner, mem in conn.execute("SELECT Owner, RequestMemory FROM jobs"):
    ...                      # one batch in flight, whatever the table's size
```

`cursor.itersize` (default 1000) sets how many rows a batch asks for. It is separate from
PEP 249's `arraysize`, which defaults to 1 — a batch of one row would spend a call across the
language boundary per row and undo the point.

Three things follow, all of them worth knowing before a large query:

| | |
|---|---|
| `fetchall()` materializes | It hands back everything, by definition. Iterate instead to keep a large result out of memory. |
| `rowcount` counts what you fetched | For a SELECT it is `-1` before the first fetch and grows as rows arrive, because the total is not known until the result is exhausted. PEP 249 allows this and `sqlite3` does the same. For DML it is the affected count, unchanged. |
| Some shapes cannot stream *as tuples* | `SELECT *` needs its column list before row 1, and that list is the union of every matched ad's attributes — see `conn.mappings()` below, which streams it. Aggregates, `GROUP BY` and window functions cannot stream in any row shape: their rows are synthesized from groups rather than read from ads. Those run whole and are served from memory; the rows are identical, only the memory differs. |

**Name your columns.** `SELECT Owner, RequestMemory FROM jobs` streams *and* pushes a
projection to the server, so only those attributes cross the wire. `SELECT *` does neither.

### Streaming `SELECT *`: `conn.mappings()`

A tuple result has to know its columns before the first row. `SELECT *` does not — its column
list is the union of every matched ad's attributes, and ClassAds are schemaless, so row 2 may
carry an attribute row 1 lacks. Keyed rows need no column list at all, so this streams:

```python
for row in conn.mappings("SELECT * FROM jobs WHERE Owner = ?", ("alice",)):
    print(row["Owner"], row.get("RequestMemory"))
```

Each row is a `dict` of exactly that ad's attributes — nothing is padded to a union, and a wide
or ragged result costs one batch rather than the whole table. It takes `itersize`, is a context
manager, and reports `stream.streamed`.

Values are **evaluated**, so an attribute holding an expression arrives as what it evaluates to.
`conn.ads()` is the path that preserves expressions (`ad.lookup("Requirements")`), at the cost of
needing HTCondor's `classad2` bindings; `mappings()` needs nothing beyond the driver.

Aggregates and `GROUP BY` are still computed whole here — that limit is about synthesizing rows,
not about their shape — but they do come back as dicts.

An unfinished statement holds the daemon's per-connection executor lock, so starting another one
on the same connection first drains the open one into memory. Two cursors on one connection stay
correct; interleaving them just gives up the memory win.

## Installing

```sh
pip install htcondordb-*.whl
```

The wheel bundles `libhtcondordb_client`, so that is the whole installation — no Go
toolchain, no `make lib`, no `HTCONDORDB_LIBRARY`. `cffi` is the only dependency.

One wheel serves every Python 3 on a given platform: the driver uses cffi in ABI mode and
opens the library with `dlopen`, so no CPython ABI is linked. Three artifacts cover
everything — `manylinux` x86_64 and aarch64, and one macOS `universal2`.

Linux wheels are built in `manylinux_2_28` (EL8 and newer) and are checked to install and
run on a different distribution from the one they were built in. The macOS wheel carries
both architectures in one binary, and is tagged for the SDK it was built against rather
than an older floor, so it does not claim support for releases nothing has run on.

Building one yourself:

```sh
make wheel           # for this host
make wheel-linux     # manylinux_2_28, in a container (the shipping artifact)
make wheel-validate  # install it in a clean container and run real SQL through it
```

For development, skip the wheel entirely: `make lib` and point `HTCONDORDB_LIBRARY` at
`bin/libhtcondordb_client.{so,dylib}`. The driver searches that first, then a bundled copy,
then the repo's `bin/`, then the loader's own path.

`conn.ads()` additionally needs HTCondor's `classad2` bindings (`pip install htcondor`).
Those have Linux wheels but no macOS distribution, so on a Mac that path needs a local
HTCondor build; everything else works either way.

## Authentication

There are no credential arguments to `connect()`. HTCondor's security configuration is
ambient: the library reads `CONDOR_CONFIG` and authenticates exactly as `htcondordb-cli`
does — pool token, SSL, FS, whatever the configuration allows. Point `CONDOR_CONFIG` at a
different file to authenticate differently. Setting it from Python works too:
`os.environ["CONDOR_CONFIG"] = ...` before `connect()` is honored, as are `_CONDOR_<KNOB>`
overrides.

The same configuration decides where the daemon is, so pointing `CONDOR_CONFIG` at another
pool moves both the credentials and the address `connect()` resolves.

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
| `description` | 7-tuples; only `name` and `type_code` are meaningful (ClassAd is dynamically typed). Reading it fetches the first batch, since a type can only come from data |
| `rowcount` | For a SELECT, the rows fetched so far (`-1` before the first fetch); for DML, the affected count |
| `itersize` | Rows per batch, default 1000 (extension; `arraysize` stays PEP 249's default of 1) |

**A bug in the library raises; it does not crash.** Every entry point in the C client runs
inside a `recover()`, so a panic in the Go stack arrives as `InternalError` — carrying the Go
stack trace, for a bug report — instead of aborting the interpreter with no traceback. Go's
*unrecoverable* runtime failures (out of memory, `concurrent map writes`) are the exception:
those still abort, by design in Go.

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

## Writing ClassAds

The counterpart to `conn.ads()`. Expressions stay expressions in both directions, so a
read-modify-write round trip does not flatten `Requirements` into whatever it evaluated to:

```python
updated = []
for ad in conn.ads("SELECT * FROM machines WHERE Cpus > 4"):
    ad["Checked"] = True
    updated.append(ad)

conn.write_ads("machines", updated, key="Name")
```

It is also how to bulk load. `ads` is an **iterable**, consumed lazily and batched as it
goes, so a generator over a million ads never materializes:

```python
res = conn.write_ads("machines", (ad for ad in parse_slots(stream)))
print(res.written, res.rejects, res.conflicts)
```

One write per batch, against a statement parse and a round trip per row through `execute()`.

**It is an upsert, and it REPLACES.** An existing ad at a key is overwritten whole, so an
attribute the new ad omits is deleted. Writing back ads from a *projected* `SELECT` will
therefore drop everything you did not select — read with `SELECT *` if you intend to write
back, or use `UPDATE` to change a subset of attributes in place.

Notes:

- **Keys** come from the `Name` attribute by default (matching `htcondordb-cli -key`); pass
  `key="Key"` for a table written by SQL `INSERT`, or a callable for anything else.
- **A bad ad does not lose the batch.** Rejects come back by position in your input, with a
  reason, and the rest still apply. `WriteResult` is falsy when anything was rejected or
  conflicted, so `if not conn.write_ads(...)` reads as "something did not land".
- **Atomicity is per batch** outside a transaction, so a failure partway leaves earlier
  batches applied — that is what lets an arbitrarily large load run. Inside a transaction
  every batch stages into it and they land together.
- **Conflicts** are per key: a write that lost an optimistic race comes back in
  `conflicts`, unapplied, for you to re-read and retry. For a read-modify-write to be
  race-free the read has to share the write's transaction — set `autocommit = False`, and
  `conn.ads()` will read through it.
- Two values old-ClassAd text cannot hold are rejected rather than mangled: a string
  containing a newline, or one ending in a backslash.

## Inserting expressions through SQL

A string parameter binds as a string, so `"Start && WithinResourceLimits"` stores those
characters rather than the expression. To bind code, say so:

```python
from htcondordb import Expr

conn.execute("INSERT INTO machines (Key, Requirements) VALUES (?, ?)",
             ("slot1", Expr("Start && WithinResourceLimits")))
```

`classad2.ExprTree` and `classad2.ClassAd` bind directly and need no wrapper;
`classad2.Value.Undefined` and `.Error` bind as `UNDEFINED` and `error`.

`Expr` text is emitted **verbatim** — not escaped, not quoted, not validated. It is the one
place in this driver where a parameter is not automatically safe, so never build one from
untrusted input. Everything else stays quoted.

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
