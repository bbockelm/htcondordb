# Arrow Flight SQL

The daemon can serve its tables over **Arrow Flight SQL**, so Arrow-native tools read them as
record batches instead of parsing text. Anything that speaks ADBC works: the Python
`adbc_driver_flightsql`, and through it pandas and Polars; DuckDB; the JDBC/ODBC Flight SQL
drivers.

It is **off by default** and **read-only** — there is no write surface at all.

## Turning it on

```
HTCONDORDB_FLIGHT_ADDRESS = 127.0.0.1:9095
```

That is the whole minimum. Every other knob has a working default; the full list is in
[Configuration](configuration.md).

## Reading from Python

```sh
pip install adbc_driver_flightsql pyarrow
```

```python
from adbc_driver_flightsql import dbapi

with dbapi.connect("grpc://127.0.0.1:9095") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT Owner, ClusterId, QDate FROM jobs WHERE Owner = ?", ("alice",))
        table = cur.fetch_arrow_table()        # pyarrow.Table
        df = table.to_pandas()
```

### pandas

```python
from adbc_driver_flightsql import dbapi

with dbapi.connect("grpc://127.0.0.1:9095") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT Owner, RequestCpus, RequestMemory, QDate FROM jobs")
        df = cur.fetch_arrow_table().to_pandas()

print(df.groupby("Owner")["RequestCpus"].sum().sort_values(ascending=False).head())
```

`QDate` and the other epoch attributes arrive as Arrow timestamps, so they land as
`datetime64` without a conversion step:

```python
df.set_index("QDate").resample("1h").size()
```

### Polars

```python
import polars as pl
from adbc_driver_flightsql import dbapi

with dbapi.connect("grpc://127.0.0.1:9095") as conn:
    df = pl.read_database("SELECT Owner, JobStatus, RequestMemory FROM jobs", connection=conn)

print(df.group_by("Owner").agg(pl.col("RequestMemory").mean()))
```

### Larger-than-memory results

Fetch the stream in batches rather than materializing a table:

```python
with dbapi.connect("grpc://127.0.0.1:9095") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT Owner, CompletionDate, RemoteWallClockTime FROM history")
        reader = cur.fetch_record_batch()
        total = 0.0
        for batch in reader:
            total += sum(batch.column("RemoteWallClockTime").to_pylist())
```

### TLS and tokens

For TLS use `grpc+tls://`. With `HTCONDORDB_FLIGHT_AUTHENTICATION = SCITOKEN`, pass the bearer
token as a driver option:

```python
conn = dbapi.connect(
    "grpc+tls://db.example.org:9095",
    db_kwargs={"adbc.flight.sql.authorization_header": f"Bearer {token}"},
)
```

## Who may read what

Authentication defaults to `OPTIONAL`, which **follows CEDAR rather than being stricter**: a
credential is verified when one is offered, and otherwise the connection gets the anonymous
identity and `ALLOW_READ` decides. So enabling this endpoint adds no exposure of its own — what
an anonymous caller may read here is what the same caller may already read over CEDAR, decided
by the same policy. An operator who has tightened one has tightened both.

There is one policy, not two: a `READ` peer does not see private attributes such as `ClaimId`;
a `DAEMON` peer does.

`SCITOKEN` is available for a pool that wants this door stricter than CEDAR, and requires
`HTCONDORDB_FLIGHT_SCITOKEN_ISSUERS`. `NONE` serves every caller as anonymous.

## The SQL it accepts

```sql
SELECT <columns | *> FROM <table> [WHERE <predicate>] [LIMIT n]
```

Prepared statements and bound parameters work, which is what a driver uses for
`cursor.execute(sql, params)`. An aggregate, a `JOIN`, an alias, a function call or an
`ORDER BY` is **refused** rather than approximated, and the error names the shape that is
accepted.

Aggregate in the client, or run the aggregate through the REPL, which has the full language:

```python
# fetch the columns, aggregate in Arrow
cur.execute("SELECT Owner, RequestCpus FROM jobs")
cur.fetch_arrow_table().group_by("Owner").aggregate([("RequestCpus", "sum")])
```

```sh
# or aggregate server-side over CEDAR
htcondordb-cli -e "SELECT Owner, SUM(RequestCpus) FROM jobs GROUP BY Owner"
```

### `WHERE` is ANSI SQL here, not ClassAd

Worth reading carefully, because it changes results rather than causing an error.

ClassAd's `==`, `<` and `>` compare strings **case-insensitively**. SQL's `=` does not. So
`WHERE Owner = 'alice'` means different things to the two languages, with plausible results
either way:

| | matches `alice` | matches `ALICE` |
|---|---|---|
| ClassAd (`condor_q`, the REPL's default, `-constraint`) | yes | **yes** |
| SQL (this endpoint) | yes | **no** |

This endpoint is always ANSI. A predicate that cannot be translated faithfully is refused rather
than guessed at, so these are errors:

- **String ordering** — `Owner > 'R'`.
- **String inequality** — `Owner <> 'alice'`, and a negated equality, `NOT Owner = 'alice'`.
- **A type-invalid comparison** — `Cpus = 'four'`, rejected when the statement is prepared.
- **A predicate on an attribute with no scalar form** — a policy expression such as
  `Requirements`. It is still readable as text; it just cannot be compared.

`IN`, `BETWEEN` and SQL's `NULL` semantics all work. `IS NULL` means the attribute is absent or
undefined, and a comparison against `NULL` drops the row exactly as SQL requires.

To see what a client will get without one to hand, the REPL has
[`.dialect ansi`](repl.md#dialect--which-language-a-where-clause-is-in).

## Column types

Arrow is strictly typed and a ClassAd is not, so each column's type is **derived from what the
attribute actually holds** in a sample of the table, then declared in the schema. An integer
attribute becomes `int64`, a real becomes `double`, a boolean `bool`, a string `utf8`.

An attribute holding **both** integers and reals becomes a `double` — but only if every integer
in it is within 2^53, where a float64 loses nothing. If any exceeds that, widening would silently
alter values, so the column falls back to text instead. An attribute whose kinds genuinely
disagree in any other way (say a string and a number) likewise falls back to text, which is a
faithful representation of any ClassAd value. Each column's schema metadata records how its type
was reached and which value classes were seen, so a client reading only the schema can tell a
derived column from a declared one.

An attribute that is a policy **expression** is evaluated and contributes a value of whatever it
yields, rather than counting as a kind of its own.

Attributes holding unix-epoch seconds are served as Arrow timestamps rather than integers.
Nothing can infer that from the values — an integer is an integer — so the well-known job
attributes (`QDate`, `JobStartDate`, `CompletionDate`, `EnteredCurrentStatus` and friends) are
built in, and `HTCONDORDB_FLIGHT_TIME_ATTRS` adds to that list without replacing it.

A column is nullable unless the sample saw the whole table and found the attribute on every
record, because a sampled absence is not evidence of presence.

## Limits

`HTCONDORDB_FLIGHT_STREAM_MEMORY` caps the Arrow memory one result stream may hold and
`HTCONDORDB_FLIGHT_MAX_STREAMS` caps how many run at once. Both are unlimited unless set.

```conf
HTCONDORDB_FLIGHT_STREAM_MEMORY = 512 MiB
HTCONDORDB_FLIGHT_MAX_STREAMS   = 8
```

A stream past its budget is refused with `ResourceExhausted` naming the limit; a client past the
stream count is refused immediately and told to retry. A value either knob cannot parse is logged
at `ERROR` naming the knob, and the endpoint serves with **no** limit.

## The schema cache

`HTCONDORDB_FLIGHT_SCHEMA_TTL` (default `30s`) is how long a table's derived schema is reused, so
that a client looping over a prepared statement does not make the daemon re-sample the table every
call.

The trade is staleness: **a column that appears in the table does not appear in the schema until
the entry ages out.** If you are watching a column arrive — adding an attribute and immediately
querying for it — set the knob to `0`:

```conf
HTCONDORDB_FLIGHT_SCHEMA_TTL = 0
```

Read the schema that arrives with the stream, which is what every Arrow reader does, rather than
caching the one `execute_schema` reported.

## Arrow without a server

For a one-off extract there is no need for the endpoint at all — the CLI writes an Arrow IPC
stream straight to a file, which DuckDB reads with its `nanoarrow` extension:

```sh
htcondordb-cli arrow -table jobs -o jobs.arrows
```

```sql
INSTALL nanoarrow FROM community; LOAD nanoarrow;
SELECT Owner, count(*) FROM read_arrow('jobs.arrows') GROUP BY Owner;
```
