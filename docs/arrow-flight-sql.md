# Arrow Flight SQL

The daemon can serve its tables over **Arrow Flight SQL**, so Arrow-native tools read them as
record batches instead of parsing text. Anything that speaks ADBC works: the Python
`adbc_driver_flightsql`, and through it pandas and Polars; DuckDB; the JDBC/ODBC Flight SQL
drivers.

It is **off by default** and **read-only**. There is no write surface at all — not a disabled
one, none — so the worst a client can do is read what the same client could already read over
CEDAR.

## Turning it on

```
HTCONDORDB_FLIGHT_ADDRESS = 127.0.0.1:9095
```

That is the whole minimum. Every other knob has a working default; the full list is in
[Configuration](configuration.md).

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

For TLS use `grpc+tls://`. With `HTCONDORDB_FLIGHT_AUTHENTICATION = SCITOKEN`, pass the bearer
token as driver option `adbc.flight.sql.authorization_header` with value `Bearer <token>`.

## Who may read what

Authentication defaults to `OPTIONAL`, which **follows CEDAR rather than being stricter**: a
credential is verified when one is offered, and otherwise the connection gets the anonymous
identity and `ALLOW_READ` decides. So enabling this endpoint adds no exposure of its own — what
an anonymous caller may read here is what the same caller may already read over CEDAR, decided
by the same policy. An operator who has tightened one has tightened both.

That is not an accident of implementation: each connection's reads go through the same
authorization path a CEDAR client's do, so there is no second policy to keep in step. A `READ`
peer does not see private attributes such as `ClaimId`; a `DAEMON` peer does.

`SCITOKEN` is available for a pool that wants this door stricter than CEDAR, and requires
`HTCONDORDB_FLIGHT_SCITOKEN_ISSUERS`. `NONE` serves every caller as anonymous.

## The SQL it accepts

```sql
SELECT <columns | *> FROM <table> [WHERE <predicate>] [LIMIT n]
```

Prepared statements and bound parameters work, which is what a driver uses for
`cursor.execute(sql, params)`. An aggregate, a `JOIN`, an alias, a function call or an
`ORDER BY` is **refused**, not approximated.

That refusal is the design, not a gap waiting to be plugged carelessly. Nothing downstream
corrects this endpoint, so a query it half-understood would return rows that look right and are
not. A refusal is a bug report you can act on; a wrong answer is one nobody files. The error
names the shape that *is* accepted.

### `WHERE` is ANSI SQL here, not ClassAd

This is the one thing worth reading carefully, because it changes results rather than causing an
error.

ClassAd's `==`, `<` and `>` compare strings **case-insensitively**. SQL's `=` does not. So
`WHERE Owner = 'alice'` means different things to the two languages, with plausible results
either way:

| | matches `alice` | matches `ALICE` |
|---|---|---|
| ClassAd (`condor_q`, the REPL's default, `-constraint`) | yes | **yes** |
| SQL (this endpoint) | yes | **no** |

This endpoint is always ANSI. A predicate is translated to a ClassAd constraint only where the
two can be proved equivalent, and refused otherwise — so these are errors rather than guesses:

- **String ordering** (`Owner > 'R'`). ClassAd's fold-order and SQL's byte-order results are
  neither a subset nor a superset of one another, and the stored string dictionaries are
  fold-sorted, so no index could serve a byte-ordered range anyway.
- **String inequality** (`Owner <> 'alice'`). Needs a three-valued guard whose index behaviour
  is unverified, and an unindexed inequality over a large table is better introduced
  deliberately than discovered.
- **A negated string equality** (`NOT Owner = 'alice'`), for the same three-valued reason.
- **A type-invalid comparison** (`Cpus = 'four'`), rejected when the statement is prepared, as a
  SQL engine would, rather than lowered into a ClassAd `ERROR` that quietly filters out every
  row.
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

An attribute that is a policy **expression** is not a kind of its own: it is evaluated, and
contributes a value of whatever it yields. Counting it as a competing kind would turn a numeric
column into text on the strength of a single record — which it did, before that was fixed.

Attributes holding unix-epoch seconds are served as Arrow timestamps rather than integers.
Nothing can infer that from the values — an integer is an integer — so the well-known job
attributes (`QDate`, `JobStartDate`, `CompletionDate`, `EnteredCurrentStatus` and friends) are
built in, and `HTCONDORDB_FLIGHT_TIME_ATTRS` adds to that list without replacing it.

A column is nullable unless the sample saw the whole table and found the attribute on every
record, because a sampled absence is not evidence of presence.

## Limits

`HTCONDORDB_FLIGHT_STREAM_MEMORY` caps the Arrow memory one result stream may hold and
`HTCONDORDB_FLIGHT_MAX_STREAMS` caps how many run at once. Both are unlimited unless set.

A stream past its budget is refused with `ResourceExhausted` naming the limit, and a client past
the stream count is refused immediately and told to retry — which costs nothing — rather than
queued behind work the daemon has not got the memory for.

A value either knob cannot parse is reported at `ERROR` naming the knob, and the endpoint serves
with **no** limit. That is the only fallback that cannot wedge startup over a typo, but it is
also the opposite of what you asked for, which is why it is logged rather than silent.

## The schema cache

Deriving a table's schema samples records and classifies them, and every statement needs one
twice: once to answer the result shape before anything runs, and once to encode the rows. A
client that prepares a statement and executes it in a loop would pay that sampling scan on every
call, inside the daemon.

`HTCONDORDB_FLIGHT_SCHEMA_TTL` (default `30s`) is how long a derived schema is reused. `0`
disables it.

The trade is staleness, and it is worth knowing which way: **a column that appears in the table
does not appear in the schema until the entry ages out.** Thirty seconds is short enough that
this is rarely visible and long enough to collapse a burst. If you are watching a column arrive —
adding an attribute and immediately querying for it — set the knob to `0`.

Two things it does not change. Entries are scoped to the caller's authorization, so a reader that
may not see private attributes is never served a schema derived for one that may; the two never
share an entry. And a cached schema is not a *pinned* one: the entry can expire between the call
that reports a result's shape and the call that returns its rows, so a client that reads
`execute_schema` and then trusts it positionally can still, in principle, be handed a different
shape. Read the schema that arrives with the stream — which is what every Arrow reader does — and
the question does not come up.

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
