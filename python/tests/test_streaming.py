"""Rows arrive in batches, and that must not change what a caller sees.

Two properties are load-bearing here and neither is visible in ordinary use, which is why they
get their own file:

* **Parity.** A streamed result and a materialized one must produce the same rows, in the same
  order, with the same Python types. Half the statements in these tests stream and half do not
  (``SELECT *``, aggregates, ``GROUP BY``), and the assertions do not distinguish them.
* **No deadlock.** An unfinished stream holds the daemon's per-connection executor lock, so
  anything else on that connection has to settle it first. A regression there does not fail an
  assertion -- it hangs forever. Every risky call is therefore run through :func:`within`, which
  turns a hang into a failure with a message.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

import pytest

import htcondordb

# Long enough that a slow CI machine does not trip it, short enough to fail rather than hang.
DEADLOCK_TIMEOUT = 60.0


def within(seconds: float, call: Callable[[], Any], what: str) -> Any:
    """Run *call* on a helper thread and fail if it does not finish in time.

    A deadlock in the driver would otherwise hang the whole session with no indication of which
    interaction caused it. The thread is left running on failure -- there is nothing safe to do
    about a wedged call -- but pytest still reports the cause.
    """
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["value"] = call()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the calling thread
            box["error"] = exc

    thread = threading.Thread(target=run, daemon=True, name=f"within:{what}")
    thread.start()
    thread.join(seconds)
    if thread.is_alive():
        pytest.fail(f"{what} did not finish in {seconds}s -- the executor lock is likely held")
    if "error" in box:
        raise box["error"]
    return box.get("value")


@pytest.fixture
def rows(connection, table):
    """A table with enough rows to span several batches at a small itersize.

    Loaded through SQL rather than write_ads so the fixture does not depend on classad2 being
    installed -- these tests are about the row path, not the ad path.
    """
    cursor = connection.cursor()
    cursor.executemany(
        f"INSERT INTO {table} (Key, Owner, RequestMemory, Idx) VALUES (?, ?, ?, ?)",
        [(f"j{i}", f"user{i % 7}", 1024 + i, i) for i in range(250)],
    )
    cursor.close()
    return table


class TestParity:
    """The same data, read every way, comes back the same."""

    def test_iteration_matches_fetchall(self, connection, rows):
        iterated = [row for row in connection.execute(f"SELECT Idx FROM {rows}")]
        fetched = connection.execute(f"SELECT Idx FROM {rows}").fetchall()
        assert sorted(iterated) == sorted(fetched)
        assert len(iterated) == 250

    def test_fetchmany_matches_fetchall(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 17  # deliberately not a divisor of 250
        batched: list[tuple] = []
        while True:
            batch = cursor.fetchmany(10)
            if not batch:
                break
            assert len(batch) <= 10
            batched.extend(batch)
        assert sorted(batched) == sorted(
            connection.execute(f"SELECT Idx FROM {rows}").fetchall()
        )

    def test_fetchone_to_exhaustion(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 13
        seen = []
        while (row := cursor.fetchone()) is not None:
            seen.append(row)
        assert len(seen) == 250
        # Past the end stays None rather than restarting or raising.
        assert cursor.fetchone() is None

    def test_types_survive_batching(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Name, Count, Ratio, Ready, Ticket) "
            "VALUES ('r1', 'alice', 42, 1.5, true, '0042')"
        ).close()
        cursor = connection.execute(
            f"SELECT Name, Count, Ratio, Ready, Ticket FROM {table}"
        )
        cursor.itersize = 1
        name, count, ratio, ready, ticket = cursor.fetchone()
        assert name == "alice" and isinstance(name, str)
        assert count == 42 and isinstance(count, int) and not isinstance(count, bool)
        assert ratio == 1.5 and isinstance(ratio, float)
        assert ready is True
        # The reason cells are typed on the Go side rather than parsed from display text.
        assert ticket == "0042"

    def test_expression_columns_stream(self, connection, rows):
        cursor = connection.execute(
            f"SELECT Idx, RequestMemory * 2 AS doubled FROM {rows} WHERE Idx < 5"
        )
        for idx, doubled in cursor.fetchall():
            assert doubled == (1024 + idx) * 2

    @pytest.mark.parametrize(
        "sql, streamed",
        [
            ("SELECT Idx FROM {t}", True),
            ("SELECT Idx FROM {t} ORDER BY Idx", True),
            ("SELECT * FROM {t}", False),
            ("SELECT COUNT(*) FROM {t}", False),
            ("SELECT Owner, COUNT(*) FROM {t} GROUP BY Owner", False),
        ],
    )
    def test_streamed_flag_reports_what_happened(self, connection, rows, sql, streamed):
        # Advisory, but worth pinning: it is how a caller knows whether its memory is bounded,
        # and the parity tests above are only meaningful if both paths are actually exercised.
        cursor = connection.execute(sql.format(t=rows))
        assert cursor.streamed is streamed
        cursor.fetchall()

    def test_star_still_returns_every_column(self, connection, rows):
        # SELECT * cannot stream -- its header is the union of every ad's attributes -- and must
        # therefore still behave exactly as it did before rows became lazy.
        cursor = connection.execute(f"SELECT * FROM {rows}")
        assert len(cursor.fetchall()) == 250
        assert {name for name, *_ in cursor.description} >= {"Owner", "RequestMemory", "Idx"}


class TestRowcount:
    """rowcount counts what has been fetched, because the total is not known in advance."""

    def test_unknown_before_fetching(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        assert cursor.rowcount == -1
        cursor.fetchall()

    def test_grows_as_rows_are_fetched(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 50
        cursor.fetchone()
        first = cursor.rowcount
        assert 0 < first <= 50
        cursor.fetchall()
        assert cursor.rowcount == 250

    def test_exact_after_fetchall(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows} WHERE Idx < 10")
        cursor.fetchall()
        assert cursor.rowcount == 10

    def test_empty_result(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows} WHERE Idx > 10000")
        assert cursor.fetchall() == []
        assert cursor.rowcount == 0

    def test_dml_still_reports_affected(self, connection, table):
        cursor = connection.execute(
            f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')"
        )
        assert cursor.rowcount == 1
        assert cursor.description is None
        assert cursor.fetchall() == []


class TestMemoryIsBounded:
    """What can actually be asserted from here about memory, and what cannot.

    The driver's own buffer is observable, so "iteration does not accumulate the result in
    Python" is testable directly. Whether the *daemon* is holding the whole result is not
    visible from this side at all -- so that half is pinned by asserting the statement took the
    streaming path, which is the thing that would regress.
    """

    def test_iterating_does_not_accumulate_rows_in_the_driver(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 10
        # Streaming is the half this test cannot see; without it the rows are all in the daemon
        # regardless of how few the driver holds, so assert it rather than implying it.
        assert cursor.streamed, "the statement stopped streaming; rows are materialized"

        high_water = 0
        seen = 0
        for _ in cursor:
            seen += 1
            high_water = max(high_water, len(cursor._rows) - cursor._position)
        assert seen == 250
        assert high_water <= 20, f"buffered {high_water} rows at an itersize of 10"

    def test_fetchall_does_accumulate(self, connection, rows):
        # The contrast that makes the previous test mean something: asking for everything hands
        # back everything, and the docstring says so.
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        assert len(cursor.fetchall()) == 250
        assert len(cursor._rows) == 250


class TestConcurrencyOnOneConnection:
    """An unfinished stream must never wedge the connection it came from."""

    def test_a_second_statement_settles_the_first(self, connection, rows):
        first = connection.execute(f"SELECT Idx FROM {rows}")
        first.itersize = 10
        assert first.fetchone() is not None  # the stream is now live and holding the lock

        second = within(
            DEADLOCK_TIMEOUT,
            lambda: connection.execute(f"SELECT Idx FROM {rows} WHERE Idx < 3").fetchall(),
            "a second statement while a stream is open",
        )
        assert len(second) == 3
        # The first cursor keeps every row it had not yet handed out.
        assert len(first.fetchall()) == 249

    def test_a_write_settles_an_open_stream(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 5
        cursor.fetchone()
        within(
            DEADLOCK_TIMEOUT,
            lambda: connection.execute(
                f"INSERT INTO {rows} (Key, Owner) VALUES ('extra', 'zoe')"
            ).close(),
            "an INSERT while a stream is open",
        )
        assert len(cursor.fetchall()) == 249

    def test_write_ads_settles_an_open_stream(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 5
        cursor.fetchone()
        within(
            DEADLOCK_TIMEOUT,
            # A callable key because these ads are text: pulling an attribute out of text would
            # mean parsing it, which needs classad2.
            lambda: connection.write_ads(
                rows, ['[ Key = "w1"; Owner = "zoe" ]'], key=lambda ad: "w1"
            ),
            "write_ads while a stream is open",
        )
        assert len(cursor.fetchall()) == 249

    def test_an_ad_stream_and_a_statement_coexist(self, connection, rows):
        # This one deadlocked before streams were settled, and did so on the shipped 0.13.0:
        # conn.ads() holds the same lock, and nothing gave it up.
        pytest.importorskip("classad2")
        stream = connection.ads(f"SELECT Idx FROM {rows}")
        first = next(iter(stream))
        assert first is not None

        count = within(
            DEADLOCK_TIMEOUT,
            lambda: connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()[0],
            "a statement while an ad stream is open",
        )
        assert count == 250
        # The ad stream picks up where it left off, from what it had to buffer.
        assert len(list(stream)) == 249

    def test_abandoning_a_cursor_frees_the_connection(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 5
        cursor.fetchone()
        cursor.close()  # closing mid-stream must release the lock, not settle into memory
        assert within(
            DEADLOCK_TIMEOUT,
            lambda: connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()[0],
            "a statement after abandoning a stream",
        ) == 250

    def test_reexecuting_a_cursor_releases_the_previous_stream(self, connection, rows):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Idx FROM {rows}")
        cursor.itersize = 5
        cursor.fetchone()
        # Re-executing on the same cursor abandons the first result; nothing should wedge.
        within(
            DEADLOCK_TIMEOUT,
            lambda: cursor.execute(f"SELECT Idx FROM {rows} WHERE Idx < 4"),
            "re-executing a cursor with an open stream",
        )
        assert len(cursor.fetchall()) == 4


class TestStreamingInTransactions:
    def test_a_stream_inside_a_transaction_sees_its_own_writes(self, connection, table):
        with connection.transaction():
            connection.execute(
                f"INSERT INTO {table} (Key, Owner) VALUES ('t1', 'alice')"
            ).close()
            # Reads inside a transaction join it, and that has to keep working through the
            # streaming path -- which uses a different server op than the buffered one.
            rows = connection.execute(f"SELECT Owner FROM {table}").fetchall()
            assert rows == [("alice",)]

    def test_commit_while_a_stream_is_open(self, connection, table):
        connection.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('a', 'alice')").close()
        connection.autocommit = False
        try:
            connection.execute(f"INSERT INTO {table} (Key, Owner) VALUES ('b', 'bob')").close()
            cursor = connection.execute(f"SELECT Owner FROM {table}")
            cursor.itersize = 1
            cursor.fetchone()
            # COMMIT needs the connection too.
            within(DEADLOCK_TIMEOUT, connection.commit, "COMMIT while a stream is open")
        finally:
            connection.autocommit = True
        assert len(connection.execute(f"SELECT Owner FROM {table}").fetchall()) == 2


class TestErrors:
    def test_bad_sql_is_a_programming_error(self, connection):
        # Classification has to survive the move to the streaming entry point: the code comes
        # back from the library rather than being guessed from the message.
        with pytest.raises(htcondordb.ProgrammingError):
            connection.execute("SELECT FROM")

    def test_unknown_table_reports_at_execute(self, connection):
        with pytest.raises(htcondordb.DatabaseError):
            connection.execute("SELECT Owner FROM no_such_table_here").fetchall()

    def test_fetch_without_execute(self, connection):
        with pytest.raises(htcondordb.ProgrammingError):
            connection.cursor().fetchone()

    def test_use_after_close(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        cursor.close()
        with pytest.raises(htcondordb.InterfaceError):
            cursor.fetchone()
