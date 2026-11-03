"""The ``description`` a cursor reports is a schema, not a summary of what arrived.

The driver used to infer each column's ``type_code`` from the first non-null value it had
seen. That is a guess with three distinct failures, and this file pins the replacement for
each of them:

* a column whose first row is an integer and whose second is a string reported INTEGER;
* a column that is null all the way down reported ``None``, which made an absent attribute
  and a typed column with no values in this result indistinguishable;
* reading METADATA forced I/O, because a type could only come from data -- so asking for
  ``description`` on an unfetched cursor had to pull the first batch, which is the opposite
  of what PEP 249 expects.

The types now come from the server, from the same derivation the Arrow and Flight SQL
surfaces publish, so a column means the same thing however it is read.
"""

from __future__ import annotations

import pytest

import htcondordb
from htcondordb.cursor import _declared_type


class TestDeclaredTypeMapping:
    """The kind-name to PEP 249 mapping, which needs no daemon."""

    @pytest.mark.parametrize(
        "kind,expected",
        [
            ("int", htcondordb.NUMBER),
            ("real", htcondordb.NUMBER),
            ("bool", htcondordb.NUMBER),
            ("string", htcondordb.STRING),
            ("timestamp", htcondordb.DATETIME),
        ],
    )
    def test_every_kind_the_server_declares_has_a_type(self, kind, expected):
        assert _declared_type([kind], 0) is expected

    def test_an_unknown_kind_is_none_rather_than_a_guess(self):
        # A newer library naming a kind this driver has never heard of must report "no
        # type", not the nearest one it knows: a wrong type is acted on, a missing one is
        # not.
        assert _declared_type(["interval"], 0) is None

    def test_a_column_the_server_declined_to_type_is_none(self):
        # An empty string is the server saying "this column has no type", which is a
        # different statement from "no types were reported at all" (that is a missing
        # list, handled in Cursor.description).
        assert _declared_type([""], 0) is None

    def test_an_index_past_the_end_is_none(self):
        assert _declared_type(["int"], 5) is None


class TestDescriptionIsASchema:
    """Against a real daemon, which is the only place the server's types come from."""

    @pytest.fixture
    def typed(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Owner, Cpus, Usage, Running, QDate) "
            f"VALUES ('a', 'alice', 8, 1.5, true, 1700000000)"
        ).close()
        connection.execute(
            f"INSERT INTO {table} (Key, Owner, Cpus, Usage, Running, QDate) "
            f"VALUES ('b', 'bob', 16, 2.5, false, 1700000100)"
        ).close()
        return table

    def test_each_column_reports_the_type_the_server_declared(self, connection, typed):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Owner, Cpus, Usage, Running FROM {typed}")
        types = [d[1] for d in cursor.description]
        assert types == [
            htcondordb.STRING,
            htcondordb.NUMBER,
            htcondordb.NUMBER,
            htcondordb.NUMBER,
        ]

    def test_description_fetches_nothing(self, connection, typed):
        # PEP 249 expects the description to be available once execute() returns. It now
        # is: the types arrive with the header, so reading it consumes no rows and the
        # cursor still returns every one of them.
        cursor = connection.cursor()
        cursor.execute(f"SELECT Owner FROM {typed} ORDER BY Key")
        assert [d[0] for d in cursor.description] == ["Owner"]
        assert [row[0] for row in cursor.fetchall()] == ["alice", "bob"]

    def test_an_all_null_column_still_has_a_type(self, connection, typed):
        # The failure that most needed fixing. Under inference this reported None,
        # because there was no value to infer from -- so a caller could not tell a column
        # that is empty in this result from one the table does not have.
        cursor = connection.cursor()
        cursor.execute(f"SELECT Owner FROM {typed} WHERE Owner = ?", ("nobody",))
        assert cursor.fetchall() == []
        assert cursor.description[0][1] is htcondordb.STRING

    def test_a_count_is_a_number_without_reading_a_row(self, connection, typed):
        # An aggregate's type comes from the SELECT list rather than from any attribute,
        # which is the case inference could only answer by running the query.
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {typed}")
        assert cursor.description[0][1] is htcondordb.NUMBER
        assert cursor.fetchone() == (2,)
