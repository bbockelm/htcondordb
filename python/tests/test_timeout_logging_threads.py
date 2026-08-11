"""Query timeouts, the log knob, and sharing a connection between threads.

Three separate cleanups, tested together because they are all about the driver being embeddable:
a report that hangs forever, a library that writes to someone else's stderr, and a
``threadsafety = 2`` claim the driver has to actually honour.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

import htcondordb


def run_script(tmp_path, name, body, timeout=120):
    """Run a script in a fresh interpreter, with the driver importable.

    A subprocess is the only honest way to test the log destination: it is process-wide and set
    once, so a test in this process could neither observe the default nor restore it.
    """
    script = tmp_path / name
    script.write_text(body)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    return subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


@pytest.fixture
def rows(connection, table):
    cursor = connection.cursor()
    cursor.executemany(
        f"INSERT INTO {table} (Key, Owner, Idx) VALUES (?, ?, ?)",
        [(f"j{i}", f"user{i % 5}", i) for i in range(300)],
    )
    cursor.close()
    return table


class TestTimeout:
    def test_a_generous_timeout_does_not_interfere(self, connection, rows):
        connection.timeout = 30
        try:
            assert len(connection.execute(f"SELECT Idx FROM {rows}").fetchall()) == 300
        finally:
            connection.timeout = None

    def test_an_impossible_timeout_fails_the_query(self, connection, rows):
        # A millisecond is not enough for a round trip, so this is deterministic without
        # depending on how slow the machine is.
        connection.timeout = 0.000001
        try:
            with pytest.raises(htcondordb.OperationalError) as excinfo:
                connection.execute(f"SELECT * FROM {rows}").fetchall()
            # The message has to say it was a timeout, and whose: a bare "context deadline
            # exceeded" reads like an internal fault rather than the limit the caller set.
            assert "timeout" in str(excinfo.value).lower()
        finally:
            connection.timeout = None

    def test_a_timeout_applies_to_streamed_queries_too(self, connection, rows):
        connection.timeout = 0.000001
        try:
            with pytest.raises(htcondordb.OperationalError):
                # Named columns stream, so this exercises the deadline on the producer rather
                # than on a materializing Exec.
                connection.execute(f"SELECT Idx FROM {rows}").fetchall()
        finally:
            connection.timeout = None

    def test_a_timeout_applies_to_mappings(self, connection, rows):
        connection.timeout = 0.000001
        try:
            with pytest.raises(htcondordb.OperationalError):
                list(connection.mappings(f"SELECT * FROM {rows}"))
        finally:
            connection.timeout = None

    def test_writes_are_not_bounded(self, connection, table):
        # Deliberate: cancelling a write mid-flight can leave a transaction open on the server.
        # A tiny timeout must therefore not break an INSERT.
        connection.timeout = 0.000001
        try:
            connection.execute(
                f"INSERT INTO {table} (Key, Owner) VALUES ('w1', 'alice')"
            ).close()
        finally:
            connection.timeout = None
        assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1

    def test_the_connection_survives_a_timeout(self, connection, rows):
        # A query that timed out must not poison the session: the next one has to work.
        connection.timeout = 0.000001
        try:
            with pytest.raises(htcondordb.OperationalError):
                connection.execute(f"SELECT Idx FROM {rows}").fetchall()
        finally:
            connection.timeout = None
        assert connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()[0] == 300

    def test_rejects_a_nonsense_value(self, connection):
        for bad in (0, -1):
            with pytest.raises(ValueError):
                connection.timeout = bad

    def test_connect_accepts_a_timeout(self, daemon_address):
        with htcondordb.connect(daemon_address, timeout=30) as conn:
            assert conn.timeout == 30


class TestLogLevel:
    def test_quiet_by_default(self, daemon_address, tmp_path):
        # The library used to print several INFO lines per connect -- session negotiation, key
        # exchange, the security ClassAd -- into the host program's stderr. A subprocess is the
        # only honest way to check: the setting is process-wide and set once.
        result = run_script(
            tmp_path,
            "quiet.py",
            "import htcondordb\n"
            f"conn = htcondordb.connect({daemon_address!r})\n"
            "conn.close()\n",
        )
        assert result.returncode == 0, result.stderr
        assert result.stderr == "", f"the library wrote to stderr:\n{result.stderr}"

    def test_asking_for_info_produces_output(self, daemon_address, tmp_path):
        result = run_script(
            tmp_path,
            "loud.py",
            "import htcondordb\n"
            "htcondordb.set_log_level('info')\n"
            f"conn = htcondordb.connect({daemon_address!r})\n"
            "conn.close()\n",
        )
        assert result.returncode == 0, result.stderr
        assert result.stderr.strip(), "asking for info logging produced nothing"

    def test_the_environment_variable_works_too(self, daemon_address, tmp_path):
        result = run_script(
            tmp_path,
            "env.py",
            "import os\n"
            "os.environ['HTCONDORDB_LOG_LEVEL'] = 'info'\n"
            "import htcondordb\n"
            f"conn = htcondordb.connect({daemon_address!r})\n"
            "conn.close()\n",
        )
        assert result.returncode == 0, result.stderr
        # Set from Python after import, so this also covers the environment crossing into Go.
        assert result.stderr.strip(), "HTCONDORDB_LOG_LEVEL=info produced nothing"

    def test_unknown_level_raises(self, library_available):
        if not library_available:
            pytest.skip("shared library not built; run 'make lib'")
        with pytest.raises(ValueError, match="unknown log level"):
            htcondordb.set_log_level("chatty")
        # And it went quiet rather than leaving the old level in place.
        htcondordb.set_log_level("off")


class TestSharedConnection:
    """threadsafety = 2: threads may share a connection. They queue; they do not corrupt."""

    def test_concurrent_queries_on_one_connection(self, connection, rows):
        errors: list[BaseException] = []
        counts: list[int] = []

        def worker() -> None:
            try:
                for _ in range(6):
                    cursor = connection.cursor()
                    cursor.itersize = 13  # several batches, so settling actually happens
                    counts.append(len(cursor.execute(f"SELECT Idx FROM {rows}").fetchall()))
                    cursor.close()
            except BaseException as exc:  # noqa: BLE001 - reported on the main thread
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=180)
            assert not thread.is_alive(), "a worker did not finish -- the lock is likely held"

        assert not errors, f"{len(errors)} worker(s) failed, first: {errors[0]!r}"
        # Every query must return the whole table. A short count means one thread's settling
        # stole rows from another's buffer, which is the failure this locking prevents.
        assert counts and set(counts) == {300}, f"row counts diverged: {sorted(set(counts))}"

    def test_concurrent_writes_and_reads(self, connection, table):
        errors: list[BaseException] = []

        def writer(start: int) -> None:
            try:
                for i in range(start, start + 15):
                    connection.execute(
                        f"INSERT INTO {table} (Key, Idx) VALUES ('k{i}', {i})"
                    ).close()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def reader() -> None:
            try:
                for _ in range(15):
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=writer, args=(0,)),
            threading.Thread(target=writer, args=(100,)),
            threading.Thread(target=reader),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=180)
            assert not thread.is_alive(), "a worker did not finish"

        assert not errors, f"{len(errors)} worker(s) failed, first: {errors[0]!r}"
        assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 30

    def test_an_interleaved_stream_is_settled_safely(self, connection, rows):
        # The specific race the connection lock exists for: one thread iterating a live stream
        # while another starts a statement that has to settle it.
        errors: list[BaseException] = []
        cursor = connection.cursor()
        cursor.itersize = 5
        cursor.execute(f"SELECT Idx FROM {rows}")

        def other() -> None:
            try:
                for _ in range(10):
                    connection.execute(f"SELECT COUNT(*) FROM {rows}").fetchone()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        thread = threading.Thread(target=other)
        thread.start()
        seen = sum(1 for _ in cursor)
        thread.join(timeout=180)

        assert not thread.is_alive()
        assert not errors, f"the other thread failed: {errors[0]!r}"
        assert seen == 300, f"the iterating cursor saw {seen} of 300 rows"
