"""Odds and ends from the bindings review: fork, GC, the settle cap, non-finite floats.

Each of these is a case where the driver's behaviour was worse than its documentation: a hang
instead of an error after fork, a server-side cursor held until something else needed the
connection, an unbounded drain, a whole query lost to one NaN, and an error message that named
neither the problem nor the fix.
"""

from __future__ import annotations

import gc
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import htcondordb
from htcondordb import _library
from tests.test_streaming import DEADLOCK_TIMEOUT, within


@pytest.fixture
def rows(connection, table):
    cursor = connection.cursor()
    cursor.executemany(
        f"INSERT INTO {table} (Key, Idx) VALUES (?, ?)",
        [(f"j{i}", i) for i in range(200)],
    )
    cursor.close()
    return table


class TestForkGuard:
    """After fork() the library is unusable, so it has to say so instead of hanging."""

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork() on this platform")
    def test_a_forked_child_gets_an_exception(self, daemon_address, tmp_path):
        # Run in a subprocess: the guard is a process-level flag, and a fork from pytest itself
        # would leave a child running the test session.
        script = tmp_path / "forked.py"
        script.write_text(
            textwrap.dedent(
                f"""
                import os, sys
                import htcondordb
                conn = htcondordb.connect({daemon_address!r})

                pid = os.fork()
                if pid == 0:
                    # In the child: Go's runtime did not survive, so any use must raise rather
                    # than hang. Exit codes tell the parent which happened.
                    try:
                        conn.execute("SELECT COUNT(*) FROM nowhere_at_all")
                    except htcondordb.InterfaceError as exc:
                        os._exit(0 if "fork" in str(exc) else 3)
                    except Exception:
                        os._exit(4)
                    os._exit(5)
                _, status = os.waitpid(pid, 0)
                sys.exit(os.waitstatus_to_exitcode(status))
                """
            )
        )
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert result.returncode == 0, (
            f"child exit {result.returncode} (3=wrong message, 4=wrong exception type, "
            f"5=no exception at all)\n{result.stderr}"
        )

    def test_the_guard_is_a_plain_check(self, monkeypatch):
        # The flag itself, without forking: worth pinning because the real test can only report a
        # numeric exit code.
        monkeypatch.setattr(_library, "_forked_child", True)
        with pytest.raises(htcondordb.InterfaceError, match="fork"):
            _library.check_usable()

    def test_the_message_says_what_to_do(self, monkeypatch):
        monkeypatch.setattr(_library, "_forked_child", True)
        with pytest.raises(htcondordb.InterfaceError) as excinfo:
            _library.check_usable()
        message = str(excinfo.value)
        assert "spawn" in message and "forkserver" in message


class TestCursorCleanup:
    def test_a_dropped_cursor_releases_its_stream(self, connection, rows):
        # Before this, Connection held a strong reference, so an abandoned mid-stream cursor kept
        # a server-side cursor open until the next statement settled it.
        cursor = connection.cursor()
        cursor.itersize = 5
        cursor.execute(f"SELECT Idx FROM {rows}")
        cursor.fetchone()
        assert connection._live_streams, "the stream was not registered"

        del cursor
        gc.collect()
        assert not connection._live_streams, "a dropped cursor left its stream registered"

        # The assertion that matters: a new statement completes with nothing left to settle. It can
        # only do that if the Go side released the executor lock -- otherwise this blocks forever,
        # so it runs under a timeout rather than hanging the suite.
        count = within(
            DEADLOCK_TIMEOUT,
            lambda: connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()[0],
            "a statement after dropping a mid-stream cursor",
        )
        assert count == 200

    def test_closing_the_connection_still_closes_live_cursors(self, daemon_address, rows):
        # The weak registry must not break the case it exists for.
        conn = htcondordb.connect(daemon_address)
        cursor = conn.cursor()
        cursor.execute(f"SELECT Idx FROM {rows}")
        conn.close()
        with pytest.raises(htcondordb.InterfaceError):
            cursor.fetchone()


class TestSettleLimit:
    def test_unlimited_by_default(self, connection):
        assert connection.settle_limit is None

    def test_rejects_nonsense(self, connection):
        for bad in (0, -5):
            with pytest.raises(ValueError):
                connection.settle_limit = bad

    def test_a_low_limit_fails_the_drain(self, connection, rows):
        connection.settle_limit = 10
        cursor = connection.cursor()
        cursor.itersize = 5
        cursor.execute(f"SELECT Idx FROM {rows}")
        cursor.fetchone()

        # Another statement needs the connection, which means draining this stream -- and 200 rows
        # is past the limit, so it fails instead of quietly materializing them.
        with pytest.raises(htcondordb.OperationalError, match="settle_limit"):
            connection.execute(f"SELECT COUNT(*) FROM {rows}")

    def test_the_connection_is_usable_after_the_failure(self, connection, rows):
        connection.settle_limit = 10
        cursor = connection.cursor()
        cursor.itersize = 5
        cursor.execute(f"SELECT Idx FROM {rows}")
        cursor.fetchone()
        with pytest.raises(htcondordb.OperationalError):
            connection.execute(f"SELECT COUNT(*) FROM {rows}")

        # The failed drain must still have released the stream: otherwise the caller is stuck
        # with a connection nothing can use.
        connection.settle_limit = None
        assert connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()[0] == 200

    def test_a_generous_limit_does_not_interfere(self, connection, rows):
        connection.settle_limit = 10_000
        cursor = connection.cursor()
        cursor.itersize = 5
        cursor.execute(f"SELECT Idx FROM {rows}")
        cursor.fetchone()
        assert connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()[0] == 200
        assert len(cursor.fetchall()) == 199


class TestWriteAdsErrors:
    def test_text_ad_with_an_attribute_key_explains_itself(self, connection, table):
        # Was: "TypeError: string indices must be integers", which names neither the problem nor
        # the fix.
        with pytest.raises(htcondordb.ProgrammingError) as excinfo:
            connection.write_ads(table, ['[ Key = "t1"; Owner = "alice" ]'], key="Key")
        message = str(excinfo.value)
        assert "text" in message and "callable" in message

    def test_a_callable_key_works_on_text_ads(self, connection, table):
        result = connection.write_ads(
            table, ['[ Key = "t1"; Owner = "alice" ]'], key=lambda ad: "t1"
        )
        assert result
        assert connection.execute(f"SELECT Owner FROM {table}").fetchone() == ("alice",)


class TestPublicStreamedFlag:
    def test_cursor_reports_it(self, connection, rows):
        cursor = connection.execute(f"SELECT Idx FROM {rows}")
        assert cursor.streamed is True
        cursor.fetchall()
        star = connection.execute(f"SELECT * FROM {rows}")
        assert star.streamed is False
        star.fetchall()
