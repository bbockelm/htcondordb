"""Streaming key/value rows -- and the one thing they can do that tuples cannot.

``SELECT *`` cannot be streamed as a table: a tabular result needs its column list before the
first row, and for ``SELECT *`` that list is the union of every matched ad's attributes, which is
not known until the last ad has been seen. Keyed rows need no such list, so this is the path
where a wide or ragged ``SELECT *`` streams.

The tests that matter most here are the ragged ones: a result where row 2 carries an attribute
row 1 does not is exactly what a fixed header cannot represent, and exactly what this shape is
for.
"""

from __future__ import annotations

import pytest

import htcondordb
from tests.test_streaming import DEADLOCK_TIMEOUT, within


@pytest.fixture
def ragged(connection, table):
    """A table whose rows do not share one attribute set."""
    cursor = connection.cursor()
    cursor.executemany(
        f"INSERT INTO {table} (Key, Owner, Mem) VALUES (?, ?, ?)",
        [(f"j{i}", f"user{i % 3}", 100 + i) for i in range(60)],
    )
    cursor.close()
    # One row with an attribute nothing else has, and one missing an attribute everything else
    # has. Between them they rule out both "header from the first row" and "header from a sample".
    connection.execute(
        f"INSERT INTO {table} (Key, Owner, Mem, Extra) VALUES ('odd', 'zed', 1, 'only-here')"
    ).close()
    connection.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('thin', 'zed')").close()
    return table


class TestRaggedResults:
    def test_each_row_carries_its_own_attributes(self, connection, ragged):
        rows = {row["Key"]: row for row in connection.mappings(f"SELECT * FROM {ragged}")}
        assert len(rows) == 62

        # The point of the shape: an attribute one row has does not appear on rows that lack it,
        # and no row is padded to a union.
        assert rows["odd"]["Extra"] == "only-here"
        assert "Extra" not in rows["j0"]
        assert "Mem" not in rows["thin"]
        assert rows["j0"]["Mem"] == 100

    def test_star_streams(self, connection, ragged):
        # The whole reason this API exists. If this regresses to buffering, the memory story for
        # SELECT * goes with it.
        stream = connection.mappings(f"SELECT * FROM {ragged}")
        assert stream.streamed
        stream.close()

    def test_star_through_execute_is_still_buffered(self, connection, ragged):
        # The contrast, and why mappings() is a separate entry point rather than a flag: the
        # tabular path cannot stream this statement, and still must not.
        cursor = connection.execute(f"SELECT * FROM {ragged}")
        assert cursor.streamed is False
        assert len(cursor.fetchall()) == 62

    def test_agrees_with_the_tabular_path(self, connection, ragged):
        # Same data, both shapes: a cell present in the tuple result must match the mapping, and a
        # cell the tuple result padded to null must simply be absent.
        cursor = connection.execute(f"SELECT * FROM {ragged}")
        columns = [name for name, *_ in cursor.description]
        tabular = {
            row[columns.index("Key")]: dict(zip(columns, row)) for row in cursor.fetchall()
        }
        keyed = {row["Key"]: row for row in connection.mappings(f"SELECT * FROM {ragged}")}

        assert set(tabular) == set(keyed)
        for key, padded in tabular.items():
            for column, value in padded.items():
                if value is None:
                    assert column not in keyed[key] or keyed[key][column] is None
                else:
                    assert keyed[key][column] == value


class TestBatching:
    def test_iterates_in_batches(self, connection, ragged):
        stream = connection.mappings(f"SELECT * FROM {ragged}")
        stream.itersize = 5
        assert len([row for row in stream]) == 62

    def test_context_manager_closes(self, connection, ragged):
        with connection.mappings(f"SELECT * FROM {ragged}") as stream:
            assert next(iter(stream))["Key"]
        # Closing mid-stream releases the connection rather than draining it.
        assert (
            connection.execute(f"SELECT COUNT(*) FROM {ragged}").fetchone()[0] == 62
        )

    def test_named_columns_keyed_by_column_name(self, connection, ragged):
        rows = list(
            connection.mappings(
                f"SELECT Owner, Mem * 2 AS doubled FROM {ragged} WHERE Mem = 100"
            )
        )
        assert rows == [{"Owner": "user0", "doubled": 200}]

    def test_types_match_the_tabular_path(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Name, Count, Ratio, Ready, Ticket) "
            "VALUES ('r1', 'alice', 42, 1.5, true, '0042')"
        ).close()
        (row,) = list(connection.mappings(f"SELECT * FROM {table}"))
        assert row["Name"] == "alice"
        assert row["Count"] == 42 and not isinstance(row["Count"], bool)
        assert row["Ratio"] == 1.5
        assert row["Ready"] is True
        assert row["Ticket"] == "0042"  # a numeric-looking string stays a string

    def test_empty_result(self, connection, ragged):
        assert list(connection.mappings(f"SELECT * FROM {ragged} WHERE Mem > 100000")) == []


class TestShapesThatCannotStream:
    """A row the daemon synthesizes cannot be produced one at a time in any row shape."""

    @pytest.mark.parametrize(
        "sql, streamed",
        [
            ("SELECT * FROM {t}", True),
            ("SELECT Owner FROM {t}", True),
            ("SELECT COUNT(*) AS n FROM {t}", False),
            ("SELECT Owner, COUNT(*) AS n FROM {t} GROUP BY Owner", False),
        ],
    )
    def test_streamed_flag(self, connection, ragged, sql, streamed):
        stream = connection.mappings(sql.format(t=ragged))
        assert stream.streamed is streamed
        list(stream)

    def test_aggregates_still_produce_keyed_rows(self, connection, ragged):
        # Buffered or not, the row shape the caller asked for is what it gets -- keyed from the
        # column headers, since an aggregate's rows were never ads.
        rows = {
            row["Owner"]: row["n"]
            for row in connection.mappings(
                f"SELECT Owner, COUNT(*) AS n FROM {ragged} GROUP BY Owner"
            )
        }
        assert rows["zed"] == 2
        assert sum(rows.values()) == 62


class TestConcurrency:
    def test_a_statement_settles_an_open_mapping_stream(self, connection, ragged):
        stream = connection.mappings(f"SELECT * FROM {ragged}")
        stream.itersize = 3
        first = next(iter(stream))

        count = within(
            DEADLOCK_TIMEOUT,
            lambda: connection.execute(f"SELECT COUNT(*) FROM {ragged}").fetchone()[0],
            "a statement while a mapping stream is open",
        )
        assert count == 62
        # The stream resumes from what it had to buffer, minus the row already handed out.
        assert len(list(stream)) == 61
        assert first["Key"]

    def test_two_mapping_streams(self, connection, ragged):
        first = connection.mappings(f"SELECT * FROM {ragged}")
        first.itersize = 2
        next(iter(first))
        second = within(
            DEADLOCK_TIMEOUT,
            lambda: list(connection.mappings(f"SELECT Owner FROM {ragged} WHERE Mem = 100")),
            "a second mapping stream",
        )
        assert second == [{"Owner": "user0"}]
        assert len(list(first)) == 61


class TestErrors:
    def test_bad_sql(self, connection):
        with pytest.raises(htcondordb.ProgrammingError):
            connection.mappings("SELECT FROM")

    def test_closed_connection(self, daemon_address, ragged):
        conn = htcondordb.connect(daemon_address)
        conn.close()
        with pytest.raises(htcondordb.InterfaceError):
            conn.mappings("SELECT * FROM whatever")

    def test_dml_has_no_rows(self, connection, table):
        # Not the intended use, but it must not hang or lie: a statement with no rows yields none.
        assert list(connection.mappings(f"INSERT INTO {table} (Key) VALUES ('x')")) == []
        assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1
