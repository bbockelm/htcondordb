"""The Flight SQL endpoint, read by a REAL ADBC driver.

WHY THIS EXISTS. Every Go test of the endpoint drives arrow-go's own flightsql.Client against
arrow-go's own flightsql server -- which checks that the two halves of one library agree, not
that the endpoint is conformant. The difference is not theoretical: connecting
adbc_driver_flightsql to this endpoint for the first time failed immediately with "no
information for sql info number 1", because FLIGHT_SQL_SERVER_VERSION was never registered and
arrow-go's client never asks for it. One missing entry fails a client's whole adbc_get_info()
call, so the endpoint looked broken to every ADBC client while every Go test passed.

ADBC is also the path a DuckDB or pandas user actually takes to a Flight SQL server, so this is
the closest thing to testing what a user will experience.

The driver is an optional dependency. Set HTCONDORDB_REQUIRE_ADBC=1 to turn a skip into a
failure, which is what CI does -- a test that silently skips is not a test.
"""

import os

import pytest

REQUIRE = os.environ.get("HTCONDORDB_REQUIRE_ADBC") == "1"


def _driver():
    """The ADBC Flight SQL DB-API module, or a skip (a failure under REQUIRE)."""
    try:
        from adbc_driver_flightsql import dbapi
    except ImportError as exc:  # pragma: no cover - exercised by the absence itself
        msg = f"adbc_driver_flightsql is not installed: {exc}"
        if REQUIRE:
            pytest.fail(msg, pytrace=False)
        pytest.skip(msg)
    return dbapi


@pytest.fixture
def adbc(flight_address):
    """An ADBC connection to the session daemon's Flight SQL endpoint."""
    dbapi = _driver()
    conn = dbapi.connect(flight_address)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def seeded(connection):
    """A table of ads with known values, written over the ordinary CEDAR connection.

    Written through the normal client rather than through Flight, because Flight is
    read-only -- which is also the point: the two doors see one store.
    """
    name = "adbc_" + os.urandom(4).hex()
    connection.execute(f"CREATE TABLE {name}").close()
    try:
        for i in range(8):
            connection.execute(
                f"INSERT INTO {name} (Key, Owner, ClusterId, QDate, Load) "
                f"VALUES ('{i}.0', 'user{i % 4}', {1000 + i}, {1700000000 + i}, {i / 2})"
            ).close()
        yield name
    finally:
        try:
            connection.execute(f"DROP TABLE {name}").close()
        except Exception:  # pragma: no cover - cleanup only
            pass


def test_adbc_get_info(adbc):
    """adbc_get_info() must succeed and name this server.

    This is the call that failed. It asks for several SqlInfo entries at once and an
    unregistered one comes back NOT_FOUND rather than empty, failing the whole call.

    What each half of the assertion pins is worth being exact about, because the two halves
    catch different mutations. The daemon registers the version twice over -- a placeholder in
    the server's own defaults, then its build stamp through Settings -- so removing the
    placeholder alone does NOT fail this test: the stamp still registers entry 1. The
    placeholder matters for a Server nobody configures, and the Go test
    TestSqlInfoAnswersWhatADBCAsks is what pins it, because its harness sets no version. What
    THIS test uniquely catches is the stamp failing to reach the endpoint, which the
    "not unknown" assertion below detects and nothing about the call succeeding would.
    """
    info = adbc.adbc_get_info()
    assert info["vendor_name"] == "htcondordb"
    # A version must be reported and must not be the "I don't know" placeholder: without the
    # daemon passing its build stamp every deployment looks identical from the client side.
    assert info["vendor_version"], "the server reported no version"
    assert info["vendor_version"] != "unknown", (
        "the server reported the placeholder version, so the daemon's build stamp did not "
        "reach the endpoint"
    )
    assert info["vendor_arrow_version"]


def test_adbc_lists_tables(adbc, seeded):
    """A client's catalog browse must find the table, which is how any GUI populates itself."""
    objects = adbc.adbc_get_objects(depth="tables").read_all().to_pylist()
    names = {
        t["table_name"]
        for cat in objects
        for schema in (cat["catalog_db_schemas"] or [])
        for t in (schema["db_schema_tables"] or [])
    }
    assert seeded in names, f"{seeded} missing from {sorted(names)}"


def test_adbc_select_and_types(adbc, seeded):
    """A projection comes back with the declared Arrow types, not everything as text.

    The types are the whole reason the schema contract exists: a numeric attribute typed as
    utf8 round-trips through Arrow perfectly and then fails sum() in the client.
    """
    with adbc.cursor() as cur:
        cur.execute(f"SELECT Owner, ClusterId, QDate, Load FROM {seeded}")
        table = cur.fetch_arrow_table()
    assert table.num_rows == 8
    types = {f.name: str(f.type) for f in table.schema}
    assert types["Owner"] == "string"
    assert types["ClusterId"] == "int64"
    assert types["Load"] == "double"
    # QDate is declared an epoch-seconds attribute, so it must arrive as a timestamp rather
    # than as the integer underneath -- a client should not have to know the encoding.
    assert types["QDate"].startswith("timestamp"), types["QDate"]


def test_adbc_where_is_case_sensitive(adbc, seeded):
    """SQL's `=` does not fold case; ClassAd's `==` does.

    This is the divergence the lowering exists for, checked through a real driver. If the
    endpoint ever stopped lowering, the second query would match and nothing else would say so.
    """
    with adbc.cursor() as cur:
        cur.execute(f"SELECT Owner FROM {seeded} WHERE Owner = 'user1'")
        assert cur.fetch_arrow_table().num_rows == 2
        cur.execute(f"SELECT Owner FROM {seeded} WHERE Owner = 'USER1'")
        assert cur.fetch_arrow_table().num_rows == 0


def test_adbc_bound_parameters(adbc, seeded):
    """`cursor.execute(sql, params)` -- the form nearly every ADBC caller uses.

    The driver prepares, binds the values as an Arrow batch, then fetches. Nothing in the Go
    tests exercises a real driver's binding, only arrow-go's client talking to arrow-go.
    """
    with adbc.cursor() as cur:
        cur.execute(f"SELECT ClusterId FROM {seeded} WHERE ClusterId >= ?", (1004,))
        got = sorted(cur.fetch_arrow_table().column("ClusterId").to_pylist())
        assert got == [1004, 1005, 1006, 1007]

        # A string parameter, which is the case-sensitive lowering reached through a bind.
        cur.execute(f"SELECT Owner FROM {seeded} WHERE Owner = ?", ("user2",))
        assert cur.fetch_arrow_table().num_rows == 2
        cur.execute(f"SELECT Owner FROM {seeded} WHERE Owner = ?", ("USER2",))
        assert cur.fetch_arrow_table().num_rows == 0

        # Rebinding the same statement must answer the new query, not repeat the last one.
        cur.execute(f"SELECT ClusterId FROM {seeded} WHERE ClusterId >= ?", (1007,))
        assert cur.fetch_arrow_table().num_rows == 1


def test_adbc_bound_null_returns_nothing(adbc, seeded):
    """Comparing to NULL admits nothing, so the result is empty.

    The predicate is chosen so that a NULL read as its type's zero value would return EVERY
    row: ClusterId starts at 1000, so `>= 0` matches all eight.
    """
    with adbc.cursor() as cur:
        cur.execute(f"SELECT ClusterId FROM {seeded} WHERE ClusterId >= ?", (None,))
        assert cur.fetch_arrow_table().num_rows == 0


def test_adbc_refuses_what_it_cannot_lower(adbc, seeded):
    """A predicate with no faithful lowering must raise, not return plausible rows.

    Refusing is the endpoint's stated contract, and it only means anything if a real client
    actually sees the error rather than a partial result.
    """
    dbapi = _driver()
    with adbc.cursor() as cur:
        with pytest.raises(Exception) as excinfo:
            cur.execute(f"SELECT Owner FROM {seeded} WHERE Owner > 'R'")
            cur.fetch_arrow_table()
        assert "case-insensitiv" in str(excinfo.value), str(excinfo.value)
    assert dbapi is not None


def test_adbc_is_read_only(adbc, seeded):
    """Flight SQL serves reads only, and a write must be refused rather than half-applied."""
    with adbc.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute(f"DELETE FROM {seeded} WHERE ClusterId = 1000")
            cur.fetch_arrow_table()
    # And the row is still there, which is the part that matters.
    with adbc.cursor() as cur:
        cur.execute(f"SELECT ClusterId FROM {seeded} WHERE ClusterId = 1000")
        assert cur.fetch_arrow_table().num_rows == 1
