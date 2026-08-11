"""End-to-end tests against a live htcondordb daemon.

These exercise the whole path the driver exists for: Python -> cffi -> the Go C client ->
CEDAR (authenticated) -> dbrpc -> the SQL executor -> back as a typed result. Skipped
automatically when the daemon binary is not built (see conftest).
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path

import pytest

import htcondordb
from htcondordb import _library


def variant_config(tmp_path, name, *, address_file=None, host=None):
    """The live daemon's configuration with the address knobs replaced.

    Reusing the daemon's own config keeps security matching (the tests authenticate over FS);
    only where the daemon is said to be changes. ``LOG`` is dropped too, so
    ``$(LOG)/.htcondordb_address`` cannot quietly supply an address the test did not ask for.
    """
    lines = [
        line
        for line in Path(os.environ["CONDOR_CONFIG"]).read_text().splitlines()
        if not line.startswith(("HTCONDORDB_ADDRESS_FILE", "HTCONDORDB_HOST", "LOG "))
    ]
    if address_file is not None:
        lines.append(f"HTCONDORDB_ADDRESS_FILE = {address_file}")
    if host is not None:
        lines.append(f"HTCONDORDB_HOST = {host}")
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n")
    return path


def unreachable_address_file(tmp_path, name="bogus_address"):
    """An address file naming a port nothing listens on."""
    path = tmp_path / name
    path.write_text("127.0.0.1:1\n")
    return path


def round_trip(conn) -> None:
    """Prove a connection is usable, not merely open, on a table of its own."""
    name = "pytest_locate_" + os.urandom(4).hex()
    conn.execute(f"CREATE TABLE {name}").close()
    try:
        conn.execute(f"INSERT INTO {name} (Key, Owner) VALUES ('j1', 'alice')").close()
        assert conn.execute(f"SELECT Owner FROM {name}").fetchone() == ("alice",)
    finally:
        conn.execute(f"DROP TABLE {name}").close()


class TestConnection:
    def test_connect_and_close(self, daemon_address):
        conn = htcondordb.connect(daemon_address)
        assert not conn.closed
        assert conn.address == daemon_address
        conn.close()
        assert conn.closed

    def test_close_is_idempotent(self, daemon_address):
        conn = htcondordb.connect(daemon_address)
        conn.close()
        conn.close()

    def test_use_after_close_raises(self, daemon_address):
        conn = htcondordb.connect(daemon_address)
        conn.close()
        with pytest.raises(htcondordb.InterfaceError, match="closed"):
            conn.cursor()

    def test_context_manager_closes(self, daemon_address):
        with htcondordb.connect(daemon_address) as conn:
            assert not conn.closed
        assert conn.closed

    def test_connect_failure_reports_the_reason(self):
        # Port 1 on localhost refuses connections; the message must say something useful
        # rather than just "failed".
        with pytest.raises(htcondordb.OperationalError) as excinfo:
            htcondordb.connect("127.0.0.1:1")
        assert "127.0.0.1:1" in str(excinfo.value)

    def test_connect_defaults_to_the_address_file(self, daemon_address):
        # The point of the default: a report never has to be told a port. The test config
        # sets HTCONDORDB_ADDRESS_FILE, so an argument-free connect() has to land on the
        # same daemon and be usable, not merely open.
        with htcondordb.connect() as conn:
            assert conn.address == daemon_address
            round_trip(conn)

    def test_connect_falls_back_to_the_host_knob(
        self, daemon_address, tmp_path, monkeypatch
    ):
        # A daemon on another host publishes no local address file, leaving HTCONDORDB_HOST
        # as the only way to find it.
        config = variant_config(tmp_path, "host_only_config", host=daemon_address)
        monkeypatch.setenv("CONDOR_CONFIG", str(config))

        with htcondordb.connect() as conn:
            assert conn.address == daemon_address
            round_trip(conn)

    def test_environment_host_overrides_the_configuration(
        self, daemon_address, tmp_path, monkeypatch
    ):
        # The override that matters: point a report at another pool's daemon with an
        # environment variable, over a configuration that names a different one. The
        # configured address file is deliberately readable and wrong -- if the environment
        # did not win as a pair, this would connect to a dead port instead.
        config = variant_config(
            tmp_path,
            "config_with_other_daemon",
            address_file=unreachable_address_file(tmp_path),
        )
        monkeypatch.setenv("CONDOR_CONFIG", str(config))
        monkeypatch.setenv("HTCONDORDB_HOST", daemon_address)

        with htcondordb.connect() as conn:
            assert conn.address == daemon_address
            round_trip(conn)

    def test_environment_address_file_overrides_the_configuration(
        self, daemon_address, tmp_path, monkeypatch
    ):
        # The same override through the other knob: the environment names the file to read,
        # and the configuration's own address file is ignored rather than preferred.
        real_address_file = tmp_path / "real_address"
        real_address_file.write_text(f"{daemon_address}\n")
        config = variant_config(
            tmp_path,
            "config_with_other_file",
            address_file=unreachable_address_file(tmp_path),
        )
        monkeypatch.setenv("CONDOR_CONFIG", str(config))
        monkeypatch.setenv("HTCONDORDB_ADDRESS_FILE", str(real_address_file))

        with htcondordb.connect() as conn:
            assert conn.address == daemon_address
            round_trip(conn)

    def test_an_environment_override_stops_applying_when_removed(
        self, daemon_address, tmp_path, monkeypatch
    ):
        # An override the process has since dropped must stop being used -- the library
        # keeps its own copy of the environment, so a stale one would silently outlive it.
        monkeypatch.setenv("HTCONDORDB_HOST", "127.0.0.1:1")
        with pytest.raises(htcondordb.OperationalError):
            htcondordb.connect()

        monkeypatch.delenv("HTCONDORDB_HOST")
        with htcondordb.connect() as conn:
            assert conn.address == daemon_address

    def test_unlocatable_daemon_names_the_knobs(
        self, library_available, tmp_path, monkeypatch
    ):
        if not library_available:
            pytest.skip("shared library not built; run 'make lib'")
        # With nothing configured the error has to point at the knobs; a bare connection
        # failure would send the reader hunting for a network problem instead.
        config = tmp_path / "empty_config"
        config.write_text("LOG =\n")
        monkeypatch.setenv("CONDOR_CONFIG", str(config))

        with pytest.raises(htcondordb.OperationalError) as excinfo:
            htcondordb.connect()
        message = str(excinfo.value)
        assert "HTCONDORDB_ADDRESS_FILE" in message
        assert "HTCONDORDB_HOST" in message

    def test_commit_is_a_noop(self, connection):
        connection.commit()  # must not raise: callers commit defensively after writes

    def test_rollback_is_unsupported(self, connection):
        with pytest.raises(htcondordb.NotSupportedError, match="roll back"):
            connection.rollback()

    def test_closing_a_connection_closes_its_cursors(self, daemon_address):
        conn = htcondordb.connect(daemon_address)
        cursor = conn.cursor()
        conn.close()
        with pytest.raises(htcondordb.InterfaceError):
            cursor.execute("SELECT 1")


class TestCrashSafetyWithALiveConnection:
    """Recovering from a panic has to leave the session usable, not merely the process."""

    def test_a_panic_does_not_disturb_the_connection(self, connection, table):
        connection.execute(
            f"INSERT INTO {table} (Key, Owner) VALUES ('j1', 'alice')"
        ).close()

        lib = connection._lib
        out = lib.ffi.new("char **")
        assert lib.lib.hcdb_selftest_panic(out) == _library.RESULT_PANIC
        assert lib.string(out[0]).startswith(_library.PANIC_PREFIX)

        # Same connection, same CEDAR stream, after a recovered panic in the same runtime.
        assert connection.execute(f"SELECT Owner FROM {table}").fetchone() == ("alice",)
        assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (1,)


class TestDDLAndWrites:
    def test_create_insert_select_delete(self, connection, table):
        cursor = connection.cursor()

        cursor.execute(
            f"INSERT INTO {table} (Key, Owner, RequestMemory) VALUES (?, ?, ?)",
            ("job1", "alice", 2048),
        )
        assert cursor.rowcount == 1

        cursor.execute(f"SELECT Owner, RequestMemory FROM {table} WHERE Key = ?", ("job1",))
        assert cursor.fetchall() == [("alice", 2048)]

        cursor.execute(f"DELETE FROM {table} WHERE Key = ?", ("job1",))
        assert cursor.rowcount == 1

        cursor.execute(f"SELECT Owner FROM {table}")
        assert cursor.fetchall() == []

    def test_update(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(
            f"INSERT INTO {table} (Key, Owner, JobStatus) VALUES (?, ?, ?)",
            ("j1", "alice", 1),
        )
        cursor.execute(f"UPDATE {table} SET JobStatus = ? WHERE Owner = ?", (2, "alice"))
        assert cursor.rowcount == 1
        cursor.execute(f"SELECT JobStatus FROM {table} WHERE Key = ?", ("j1",))
        assert cursor.fetchone() == (2,)

    def test_executemany_sums_affected_rows(self, connection, table):
        cursor = connection.cursor()
        cursor.executemany(
            f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)",
            [("j1", "alice"), ("j2", "bob"), ("j3", "carol")],
        )
        assert cursor.rowcount == 3
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        assert cursor.fetchone() == (3,)


class TestTypes:
    """Values must survive the round trip with their ClassAd type intact."""

    def test_scalar_round_trip(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(
            f"INSERT INTO {table} (Key, Name, Count, Ratio, Ready) VALUES (?, ?, ?, ?, ?)",
            ("row1", "alice", 42, 1.5, True),
        )
        cursor.execute(f"SELECT Name, Count, Ratio, Ready FROM {table} WHERE Key = ?", ("row1",))
        name, count, ratio, ready = cursor.fetchone()

        assert name == "alice" and isinstance(name, str)
        assert count == 42 and isinstance(count, int) and not isinstance(count, bool)
        assert ratio == 1.5 and isinstance(ratio, float)
        assert ready is True

    def test_numeric_looking_string_stays_a_string(self, connection, table):
        # The reason the C side reads types off the ad instead of parsing the display
        # string: a string attribute whose text is digits must not come back as an int.
        cursor = connection.cursor()
        cursor.execute(
            f"INSERT INTO {table} (Key, Ticket, Version) VALUES (?, ?, ?)",
            ("row1", "0042", "1.5"),
        )
        cursor.execute(f"SELECT Ticket, Version FROM {table} WHERE Key = ?", ("row1",))
        ticket, version = cursor.fetchone()
        assert ticket == "0042" and isinstance(ticket, str)
        assert version == "1.5" and isinstance(version, str)

    def test_missing_attribute_is_none(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)", ("row1", "alice"))
        cursor.execute(f"SELECT Owner, NoSuchAttribute FROM {table}")
        assert cursor.fetchone() == ("alice", None)

    def test_none_binds_as_undefined(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)", ("row1", None))
        cursor.execute(f"SELECT Owner FROM {table} WHERE Key = ?", ("row1",))
        assert cursor.fetchone() == (None,)

    @pytest.mark.parametrize(
        "value",
        [
            "O'Brien",  # the SQL escape the driver is responsible for
            'say "hi"',  # ClassAd's own quote
            "' OR 1=1 --",  # an injection attempt, stored as data
            "unicode: café 日本語 🎉",
            "trailing space ",
            "",
        ],
    )
    def test_string_round_trips(self, connection, table, value):
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)", ("row1", value))
        cursor.execute(f"SELECT Owner FROM {table} WHERE Key = ?", ("row1",))
        assert cursor.fetchone() == (value,)

    @pytest.mark.parametrize(
        "value",
        ["back\\slash", "tab\there", "C:\\Users\\alice", "^\\d+$", "a\\\\b"],
    )
    def test_backslash_round_trip(self, connection, table, value):
        """Backslashes and tabs survive a write through SQL against a real daemon.

        They used to multiply: the SQL layer quoted INSERT values for new-ClassAd rules
        and wrote them into old-ClassAd text (fixed in #106), and separately the store
        rendered raw text with new-ClassAd string quoting that its old-ClassAd reader
        never unescaped (classad #134).
        """
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)", ("row1", value))
        cursor.execute(f"SELECT Owner FROM {table} WHERE Key = ?", ("row1",))
        assert cursor.fetchone() == (value,)

    @pytest.mark.parametrize("value", ["a\nb", "a\r\nb", "trailing\\"])
    def test_values_old_format_cannot_hold_are_refused(self, connection, table, value):
        """Two shapes old-ClassAd text cannot represent, refused rather than mangled.

        The format is newline-separated, so a newline ends the attribute mid-string; and a
        value ending in a backslash puts that backslash directly before the closing quote,
        where it makes the quote part of the value and runs the string on. HTCondor's own
        old-format writer has both limitations.
        """
        cursor = connection.cursor()
        with pytest.raises(htcondordb.ProgrammingError, match="old-ClassAd format"):
            cursor.execute(
                f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)", ("row1", value)
            )

    def test_reading_a_backslash_written_outside_sql_is_faithful(self, connection, table):
        """The corruption is on the write path only; reads are exact.

        Worth pinning separately, because reporting clients almost only read -- a driver
        that also mangled reads would be unusable against real job ads, which carry
        backslashes in paths and regexes.
        """
        cursor = connection.cursor()
        # A ClassAd expression on the right-hand side skips the SQL string literal path.
        cursor.execute(f"INSERT INTO {table} (Key) VALUES (?)", ("row1",))
        cursor.execute_with_ads(f"SELECT * FROM {table} WHERE Key = ?", ("row1",))
        assert cursor.ad_text and "row1" in cursor.ad_text[0]

    def test_datetime_binds_as_epoch_seconds(self, connection, table):
        moment = datetime.datetime(2026, 1, 2, 3, 4, 5)
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, QDate) VALUES (?, ?)", ("row1", moment))
        cursor.execute(f"SELECT QDate FROM {table} WHERE Key = ?", ("row1",))
        (stored,) = cursor.fetchone()
        assert stored == int(moment.timestamp())
        assert htcondordb.TimestampFromTicks(stored) == moment


class TestQueries:
    @pytest.fixture
    def populated(self, connection, table):
        cursor = connection.cursor()
        cursor.executemany(
            f"INSERT INTO {table} (Key, Owner, JobStatus, RequestMemory) VALUES (?, ?, ?, ?)",
            [
                ("j1", "alice", 2, 2048),
                ("j2", "alice", 2, 4096),
                ("j3", "alice", 1, 1024),
                ("j4", "bob", 2, 8192),
            ],
        )
        return table

    def test_where_and_order_by(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT Key, RequestMemory FROM {populated} WHERE Owner = ? ORDER BY RequestMemory DESC",
            ("alice",),
        )
        assert [row[0] for row in cursor.fetchall()] == ["j2", "j1", "j3"]

    def test_group_by_aggregate(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT Owner, COUNT(*) AS n, SUM(RequestMemory) AS total "
            f"FROM {populated} WHERE JobStatus = ? GROUP BY Owner ORDER BY Owner",
            (2,),
        )
        rows = cursor.fetchall()
        assert rows == [("alice", 2, 6144), ("bob", 1, 8192)]
        # Aggregate values arrive from the server as strings; the C side types them.
        assert all(isinstance(row[1], int) for row in rows)

    def test_count_star(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {populated}")
        assert cursor.fetchone() == (4,)

    def test_limit(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Key FROM {populated} ORDER BY Key LIMIT 2")
        assert len(cursor.fetchall()) == 2

    def test_select_star_describes_every_attribute(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {populated} LIMIT 1")
        names = [d[0] for d in cursor.description]
        assert "Owner" in names and "RequestMemory" in names

    def test_description_types(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Owner, RequestMemory FROM {populated} LIMIT 1")
        assert cursor.description[0][1] == htcondordb.STRING
        assert cursor.description[1][1] == htcondordb.NUMBER
        assert all(len(d) == 7 for d in cursor.description)

    def test_empty_result(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Owner FROM {populated} WHERE Owner = ?", ("nobody",))
        assert cursor.fetchall() == []
        assert cursor.rowcount == 0
        assert cursor.description is not None

    def test_connection_execute_shortcut_iterates(self, connection, populated):
        owners = {row[0] for row in connection.execute(f"SELECT Owner FROM {populated}")}
        assert owners == {"alice", "bob"}

    def test_cursor_is_reusable(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {populated}")
        assert cursor.fetchone() == (4,)
        cursor.execute(f"SELECT COUNT(*) FROM {populated} WHERE Owner = ?", ("bob",))
        assert cursor.fetchone() == (1,)

    def test_execute_resets_previous_results(self, connection, populated):
        cursor = connection.cursor()
        cursor.execute(f"SELECT Key FROM {populated}")
        cursor.execute(f"SELECT Key FROM {populated} WHERE Owner = ?", ("nobody",))
        assert cursor.fetchone() is None


class TestErrors:
    def test_syntax_error_is_a_programming_error(self, connection):
        with pytest.raises(htcondordb.ProgrammingError):
            connection.execute("SELECT FROM WHERE")

    def test_unknown_table_raises(self, connection):
        with pytest.raises(htcondordb.DatabaseError):
            connection.execute("SELECT * FROM no_such_table_xyz")

    def test_a_failed_statement_leaves_the_connection_usable(self, connection, table):
        cursor = connection.cursor()
        with pytest.raises(htcondordb.ProgrammingError):
            cursor.execute("NOT SQL AT ALL")
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        assert cursor.fetchone() == (0,)


class TestAds:
    def test_execute_with_ads_exposes_ad_text(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(
            f"INSERT INTO {table} (Key, Owner, RequestMemory) VALUES (?, ?, ?)",
            ("j1", "alice", 2048),
        )
        cursor.execute_with_ads(f"SELECT Owner FROM {table}")
        assert len(cursor.ad_text) == 1
        assert "Owner" in cursor.ad_text[0]
        # A named-column SELECT pushes a projection to the server, so the ad carries only
        # the columns asked for -- the point of the pushdown. SELECT * brings the row.
        assert "RequestMemory" not in cursor.ad_text[0]
        cursor.execute_with_ads(f"SELECT * FROM {table}")
        assert "RequestMemory" in cursor.ad_text[0]

    def test_ads_absent_without_the_option(self, connection, table):
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {table} (Key, Owner) VALUES (?, ?)", ("j1", "alice"))
        cursor.execute(f"SELECT Owner FROM {table}")
        assert cursor.ad_text == []

    def test_fetchads_returns_classads(self, connection, table):
        classad2 = pytest.importorskip("classad2")
        cursor = connection.cursor()
        cursor.execute(
            f"INSERT INTO {table} (Key, Owner, RequestMemory) VALUES (?, ?, ?)",
            ("j1", "alice", 2048),
        )
        # A projected select carries only what was asked for, so assert the whole ad
        # separately rather than expecting an unprojected attribute to survive it.
        cursor.execute_with_ads(f"SELECT Owner FROM {table}")
        (ad,) = cursor.fetchads()
        assert isinstance(ad, classad2.ClassAd)
        assert ad["Owner"] == "alice"
        assert "RequestMemory" not in ad

        cursor.execute_with_ads(f"SELECT * FROM {table}")
        (whole,) = cursor.fetchads()
        assert whole["Owner"] == "alice"
        assert whole["RequestMemory"] == 2048
