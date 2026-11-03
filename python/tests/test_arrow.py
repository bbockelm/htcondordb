"""Arrow IPC straight out of the C API.

This is not a capability nobody had -- pandas already works through the DB-API, and DuckDB is
already reachable by a pandas hop. What changes is that five representations collapse to one
(ad text, a ClassAd parse, JSON, Python objects, pandas inference) and that the column types
stop being guessed at the far end: they are the ones the server declared, the same derivation
the Flight SQL endpoint publishes.
"""

from __future__ import annotations

import pytest

import htcondordb

# Module-scope importorskip would skip THIS WHOLE FILE, including the arrow_bytes tests that
# need no pyarrow at all -- and a file that skips itself reports green while testing nothing.
# So the dependency is required only by the class that actually uses it.
try:  # noqa: SIM105
    import pyarrow as pa  # type: ignore[no-redef]
except ImportError:  # pragma: no cover - exercised by the environment, not the suite
    pa = None

needs_pyarrow = pytest.mark.skipif(pa is None, reason="arrow() needs pyarrow")


@needs_pyarrow
class TestArrowOutput:
    @pytest.fixture
    def typed(self, connection, table):
        for key, owner, cpus, usage in (
            ("a", "alice", 8, 1.5),
            ("b", "bob", 16, 2.5),
            ("c", "alice", 4, 0.5),
        ):
            connection.execute(
                f"INSERT INTO {table} (Key, Owner, Cpus, Usage) VALUES (?, ?, ?, ?)",
                (key, owner, cpus, usage),
            ).close()
        return table

    def test_columns_carry_the_declared_types(self, connection, typed):
        # The point of the whole path: a number arrives as a number, not as text for
        # somebody's loader to re-derive.
        tbl = connection.arrow(f"SELECT Owner, Cpus, Usage FROM {typed} ORDER BY Key")
        assert tbl.num_rows == 3
        assert pa.types.is_string(tbl.schema.field("Owner").type)
        assert pa.types.is_integer(tbl.schema.field("Cpus").type)
        assert pa.types.is_floating(tbl.schema.field("Usage").type)
        assert tbl.column("Owner").to_pylist() == ["alice", "bob", "alice"]
        assert tbl.column("Cpus").to_pylist() == [8, 16, 4]

    def test_an_aggregate_is_typed_from_its_select_list(self, connection, typed):
        # COUNT(*) is an integer no attribute produced, and AVG is a real whatever it
        # averaged -- neither type is the source column's.
        tbl = connection.arrow(
            f"SELECT Owner, COUNT(*), AVG(Cpus) FROM {typed} GROUP BY Owner ORDER BY Owner"
        )
        assert tbl.num_rows == 2
        assert pa.types.is_integer(tbl.schema.field(1).type)
        assert pa.types.is_floating(tbl.schema.field(2).type)
        assert tbl.column(1).to_pylist() == [2, 1]
        # alice's two rows are 8 and 4, so the mean is 6.0 -- a value an integer column
        # could hold, which is why the SECOND assertion is the one that matters.
        assert tbl.column(2).to_pylist() == [6.0, 16.0]

    def test_an_empty_result_is_still_a_stream_with_a_schema(self, connection, typed):
        # Zero bytes would be indistinguishable from a failure, and would lose the column
        # types -- which are what a caller asked for.
        tbl = connection.arrow(f"SELECT Owner, Cpus FROM {typed} WHERE Owner = ?", ("nobody",))
        assert tbl.num_rows == 0
        assert tbl.schema.names == ["Owner", "Cpus"]
        assert pa.types.is_integer(tbl.schema.field("Cpus").type)

    def test_select_star_carries_the_tail(self, connection, typed):
        # SELECT * is the table's columns plus `_ad`, holding whatever no typed column
        # took. Without it an attribute missing from the table and one the schema declined
        # to type look identical.
        tbl = connection.arrow(f"SELECT * FROM {typed}")
        assert "_ad" in tbl.schema.names
        assert tbl.num_rows == 3

    def test_parameters_bind(self, connection, typed):
        tbl = connection.arrow(f"SELECT Owner FROM {typed} WHERE Cpus > ?", (5,))
        assert sorted(tbl.column("Owner").to_pylist()) == ["alice", "bob"]


class TestArrowBytes:
    """The raw stream, which is the primitive and needs no pyarrow."""

    @pytest.fixture
    def rows(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Owner) VALUES ('a', 'alice')"
        ).close()
        return table

    def test_it_is_an_arrow_ipc_stream(self, connection, rows):
        data = connection.arrow_bytes(f"SELECT Owner FROM {rows}")
        # The IPC stream format starts with a continuation marker. Checking the bytes
        # rather than round-tripping through pyarrow is deliberate: this method's contract
        # is the stream itself, for a caller that has no pyarrow to verify it with.
        assert data[:4] == b"\xff\xff\xff\xff"
        assert len(data) > 8

    @needs_pyarrow
    def test_bytes_survive_a_nul(self, connection, rows):
        # The reason this is not a char* API. An IPC stream contains NUL bytes by
        # construction -- every length prefix with a zero octet in it -- so a C-string
        # return would truncate at the first one, silently and at a different offset per
        # query.
        data = connection.arrow_bytes(f"SELECT Owner FROM {rows}")
        assert b"\x00" in data, "this stream has no NUL, so it cannot prove the point"
        with pa.ipc.open_stream(data) as reader:
            assert reader.read_all().num_rows == 1

    def test_a_write_is_refused_before_it_runs(self, connection, rows):
        # Two things, and the second is the one that matters. A write has no Arrow output,
        # so it is refused -- but it must be refused from the PARSE, not after Exec, or a
        # DELETE passed here deletes and then reports that it cannot be rendered. The
        # caller gets an error either way; the difference is whether the rows survive.
        with pytest.raises(htcondordb.ProgrammingError):
            connection.arrow_bytes(f"DELETE FROM {rows}")
        remaining = connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()[0]
        assert remaining == 1, "the DELETE ran before the refusal; the row is gone"

    def test_bad_sql_is_a_programming_error(self, connection, rows):
        with pytest.raises(htcondordb.ProgrammingError):
            connection.arrow_bytes("SELECT FROM WHERE")
