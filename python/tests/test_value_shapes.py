"""What a SELECT actually hands back for the awkward ClassAd value shapes.

ClassAd columns are not scalars-only and are not even homogeneous: a stored attribute can
hold an unevaluated expression over its siblings, a list, a nested ad, or something that
evaluates to undefined or error -- and two rows of the same column can differ in type.
These tests pin what the driver does with each, because "it depends" is not something a
report writer can plan around.
"""

from __future__ import annotations

import pytest

import htcondordb


def one(connection, sql, parameters=None):
    """Run a single-cell SELECT and return the cell."""
    cursor = connection.cursor()
    cursor.execute(sql, parameters)
    row = cursor.fetchone()
    assert row is not None and len(row) == 1, f"{sql!r} did not return one cell: {row}"
    return row[0]


class TestExpressionValuedAttributes:
    """A stored expression is EVALUATED by a SELECT, never returned as expression text.

    This is the biggest semantic surprise reading HTCondor data through SQL: asking for
    `Requirements` answers True/False/None, not the expression a person would recognize.
    """

    @pytest.fixture
    def rows(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Memory, Req) VALUES ('big', 2048, Memory > 1024)")
        cursor.execute(f"INSERT INTO {table} (Key, Memory, Req) VALUES ('small', 512, Memory > 1024)")
        return table

    def test_expression_evaluates_per_row(self, connection, rows):
        # Memory is selected alongside Req so the projection carries the sibling the
        # expression reads -- see TestProjectionDropsExpressionDependencies.
        cursor = connection.cursor()
        cursor.execute(f"SELECT Key, Memory, Req FROM {rows} ORDER BY Key")
        assert cursor.fetchall() == [("big", 2048, True), ("small", 512, False)]

    def test_expression_text_is_available_through_the_ad(self, connection, rows):
        # The unevaluated expression is only reachable via the whole ad, which is what
        # execute_with_ads is for. There is no way to get it as a result cell.
        cursor = connection.cursor()
        cursor.execute_with_ads(f"SELECT * FROM {rows}")
        assert any("Memory" in text and "Req" in text for text in cursor.ad_text)

    def test_where_over_an_expression_column_evaluates_it(self, connection, rows):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Key FROM {rows} WHERE Req == true")
        assert cursor.fetchall() == [("big",)]


class TestUndefinedAndError:
    def test_unresolved_reference_is_none(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Req) VALUES ('u', NoSuchAttribute > 1)"
        ).close()
        assert one(connection, f"SELECT Req FROM {table}") is None

    def test_faulting_expression_is_none(self, connection, table):
        connection.execute(f"INSERT INTO {table} (Key, Req) VALUES ('e', 1 / 0)").close()
        assert one(connection, f"SELECT Req FROM {table}") is None

    def test_self_reference_does_not_hang(self, connection, table):
        # A cyclic expression must fault, not loop -- a hung report is worse than a wrong one.
        connection.execute(f"INSERT INTO {table} (Key, Req) VALUES ('c', Req)").close()
        assert one(connection, f"SELECT Req FROM {table}") is None

    def test_undefined_and_error_both_arrive_as_none(self, connection, table):
        """Both collapse to None, so they are indistinguishable in a result.

        Documented rather than fixed: a reporting client treats each as a missing cell,
        and JSON/Python have one spelling for that. The visible consequence is that a
        GROUP BY over a column holding both produces two separate None groups.
        """
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Req) VALUES ('u', NoSuchAttribute > 1)")
        cursor.execute(f"INSERT INTO {table} (Key, Req) VALUES ('e', 1 / 0)")
        cursor.execute(f"SELECT Req, COUNT(*) FROM {table} GROUP BY Req")
        groups = cursor.fetchall()
        assert len(groups) == 2, f"expected two distinct groups, got {groups}"
        assert all(value is None for value, _ in groups)


class TestComposites:
    def test_list_round_trips_through_values(self, connection, table):
        # The commas inside braces are not value separators; this used to fail as an
        # arity mismatch before the SQL parser tracked brace depth.
        connection.execute(
            f"INSERT INTO {table} (Key, Args) VALUES (?, ?)", ("k", [1, 2, 3])
        ).close()
        assert one(connection, f"SELECT size(Args) FROM {table}") == 3

    def test_list_arrives_as_classad_text_not_a_python_list(self, connection, table):
        # Composite values keep their ClassAd rendering: there is no lossless mapping to
        # a Python list (elements may be expressions), so the driver does not invent one.
        connection.execute(
            f"INSERT INTO {table} (Key, Args) VALUES (?, ?)", ("k", [1, 2, 3])
        ).close()
        value = one(connection, f"SELECT Args FROM {table}")
        assert isinstance(value, str)
        assert "1" in value and "3" in value

    def test_list_binds_usefully_in_a_where_clause(self, connection, table):
        # The shape that actually matters for reporting: an IN-style membership test.
        connection.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('k', 'alice')").close()
        cursor = connection.cursor()
        cursor.execute(f"SELECT Key FROM {table} WHERE member(Owner, ?)", (["alice", "bob"],))
        assert cursor.fetchall() == [("k",)]

    def test_function_call_expression_evaluates(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Label) VALUES ('k', strcat('a', 'b'))"
        ).close()
        assert one(connection, f"SELECT Label FROM {table}") == "ab"


class TestHeterogeneousColumns:
    """Two rows of one column can hold different types; description reports one guess."""

    @pytest.fixture
    def mixed(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, V) VALUES ('n', 42)")
        cursor.execute(f"INSERT INTO {table} (Key, V) VALUES ('s', 'text')")
        cursor.execute(f"INSERT INTO {table} (Key, V) VALUES ('b', true)")
        cursor.execute(f"INSERT INTO {table} (Key, V) VALUES ('u', NoSuchAttribute)")
        return table

    def test_each_cell_keeps_its_own_type(self, connection, mixed):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Key, V FROM {mixed} ORDER BY Key")
        by_key = dict(cursor.fetchall())
        assert by_key["n"] == 42 and isinstance(by_key["n"], int)
        assert by_key["s"] == "text"
        assert by_key["b"] is True
        assert by_key["u"] is None

    def test_description_describes_the_column_not_the_first_row(self, connection, mixed):
        # This used to read "a hint from the first non-null row", and pinned type_code as
        # whatever arrived first rather than as a contract. It is a contract now: the
        # server declares the column's type from a sample of the table, and an attribute
        # that genuinely holds both numbers and strings is declared as the one type that
        # can carry either.
        #
        # The assertion stays a membership test on purpose. Which of the two a mixed
        # column lands on is a property of the derivation's widening rules, not of this
        # driver, and pinning it here would make a change over there fail in the wrong
        # file. What this file is for is that the answer no longer depends on row order.
        cursor = connection.cursor()
        first = connection.cursor()
        cursor.execute(f"SELECT V FROM {mixed} ORDER BY Key")
        first.execute(f"SELECT V FROM {mixed} ORDER BY Key DESC")
        assert cursor.description[0][1] in (htcondordb.NUMBER, htcondordb.STRING)
        assert cursor.description[0][1] == first.description[0][1]


class TestProjectionKeepsExpressionDependencies:
    """A projected SELECT agrees with SELECT * for expression-valued attributes.

    The server projection carries the attributes the projected expressions reference, so
    an expression attribute keeps the siblings it reads. It used to drop them and evaluate
    to undefined -- the shape HTCondor data hits constantly, since Requirements, Rank and
    friends are expressions over siblings.
    """

    @pytest.fixture
    def rows(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Memory, Req) VALUES ('big', 2048, Memory > 1024)"
        ).close()
        return table

    def test_select_star_is_correct(self, connection, rows):
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {rows}")
        columns = [d[0] for d in cursor.description]
        row = cursor.fetchone()
        assert row[columns.index("Req")] is True

    def test_projection_including_the_dependency_is_correct(self, connection, rows):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Memory, Req FROM {rows}")
        assert cursor.fetchone() == (2048, True)

    def test_projection_without_the_dependency_agrees(self, connection, rows):
        # The projection carries the attributes the projected expressions reference, so a
        # narrow SELECT evaluates to what SELECT * reports.
        assert one(connection, f"SELECT Req FROM {rows}") is True


class TestAliasedAndComputedColumnTypes:
    """An alias or an expression does not change a value's type.

    A cell's type comes from the value the SELECT evaluated, so a string attribute whose
    text reads as a number stays a string however the column is named. Nothing downstream
    can undo a wrong guess: a report cannot tell a number that was always a number from a
    job id that a display string was parsed into.

    Each case is asserted through both ways a cursor gets its rows -- the streaming one
    ``execute`` uses, and the materialized document behind ``execute_with_ads`` -- because
    they used to disagree, and which one a statement takes is an internal routing decision
    a caller cannot see.
    """

    @pytest.fixture
    def rows(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Owner, Ticket, Version, Flag, Spelled, Memory) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("k", "123", "0042", "3.5", "true", "undefined", 4096),
        ).close()
        return table

    @pytest.mark.parametrize(
        "column,expected",
        [
            ("Owner AS who", "123"),
            ("Ticket AS t", "0042"),
            ("Version AS v", "3.5"),
            ("Flag AS f", "true"),
            ("Spelled AS s", "undefined"),
            ("Memory AS m", 4096),
            ("Memory * 2 AS doubled", 8192),
            ("strcat(Ticket, '9') AS joined", "00429"),
            ("Memory", 4096),
            ("Owner", "123"),
        ],
    )
    def test_alias_keeps_the_value_type(self, connection, rows, column, expected):
        for run in ("execute", "execute_with_ads"):
            cursor = connection.cursor()
            getattr(cursor, run)(f"SELECT {column} FROM {rows}")
            value = cursor.fetchone()[0]
            assert value == expected, f"{run}: {column}"
            assert type(value) is type(expected), (
                f"{run}: {column} gave {value!r} ({type(value).__name__})"
            )

    def test_undefined_alias_is_still_none(self, connection, rows):
        for run in ("execute", "execute_with_ads"):
            cursor = connection.cursor()
            getattr(cursor, run)(f"SELECT NoSuchAttribute AS t FROM {rows}")
            assert cursor.fetchone()[0] is None, run

    def test_erroring_alias_is_none(self, connection, rows):
        for run in ("execute", "execute_with_ads"):
            cursor = connection.cursor()
            getattr(cursor, run)(f"SELECT Owner + 1 AS t FROM {rows}")
            assert cursor.fetchone()[0] is None, run

    def test_aggregate_group_key_keeps_its_type(self, connection, rows):
        """A group key is carried as a value, so a string that reads as a number stays one.

        The group a row belongs to is sent as a typed value beside its label, which is also
        what keeps the string "4" and the number 4 in different groups. A grouped column
        therefore arrives as what it is rather than as whatever its text parses to.

        An aggregate's own output is the part still read as text: those cells travel as
        display strings with no typed counterpart, so a user-defined aggregate returning a
        numeric-looking string arrives as a number. The Go tests pin that one, which needs a
        registered aggregate to reach.
        """
        cursor = connection.cursor()
        cursor.execute(f"SELECT Owner, COUNT(*) AS n FROM {rows} GROUP BY Owner")
        assert cursor.fetchall() == [("123", 1)]
