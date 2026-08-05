"""PEP 249 conformance that can be checked without a daemon.

The module interface, the exception hierarchy, and the type objects are all specified by
the PEP, and tooling written against DB-API relies on them being exactly this shape.
"""

from __future__ import annotations

import pytest

import htcondordb


class TestModuleInterface:
    def test_globals(self):
        assert htcondordb.apilevel == "2.0"
        assert htcondordb.threadsafety in (0, 1, 2, 3)
        assert htcondordb.paramstyle in (
            "qmark",
            "numeric",
            "named",
            "format",
            "pyformat",
        )

    def test_connect_exists(self):
        assert callable(htcondordb.connect)

    def test_connect_rejects_credential_arguments(self):
        # Credentials are ambient (CONDOR_CONFIG). Silently ignoring a user= or password=
        # would be worse than refusing it, since the caller would believe it took effect.
        with pytest.raises(htcondordb.InterfaceError, match="CONDOR_CONFIG"):
            htcondordb.connect("localhost:9618", user="alice", password="hunter2")


class TestExceptionHierarchy:
    """PEP 249 mandates these subclass relationships exactly."""

    def test_error_is_an_exception(self):
        assert issubclass(htcondordb.Error, Exception)
        assert issubclass(htcondordb.Warning, Exception)

    def test_interface_and_database_errors(self):
        assert issubclass(htcondordb.InterfaceError, htcondordb.Error)
        assert issubclass(htcondordb.DatabaseError, htcondordb.Error)

    @pytest.mark.parametrize(
        "name",
        [
            "DataError",
            "OperationalError",
            "IntegrityError",
            "InternalError",
            "ProgrammingError",
            "NotSupportedError",
        ],
    )
    def test_database_error_subclasses(self, name):
        assert issubclass(getattr(htcondordb, name), htcondordb.DatabaseError)

    def test_exceptions_reachable_from_connection_class(self):
        # PEP 249 optional extension: `except conn.ProgrammingError` without importing the
        # module. Checked on the class so it needs no live connection.
        for name in ("Error", "InterfaceError", "DatabaseError", "ProgrammingError"):
            assert getattr(htcondordb.Connection, name) is getattr(htcondordb, name)


class TestTypeObjects:
    def test_type_objects_exist(self):
        for name in ("STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"):
            assert hasattr(htcondordb, name)

    def test_type_object_compares_to_its_types(self):
        assert htcondordb.NUMBER == "int"
        assert htcondordb.NUMBER == "real"
        assert htcondordb.STRING == "string"
        assert htcondordb.NUMBER != "string"

    def test_type_object_identity(self):
        assert htcondordb.NUMBER == htcondordb.NUMBER
        assert htcondordb.NUMBER != htcondordb.STRING

    def test_type_objects_are_hashable(self):
        assert len({htcondordb.NUMBER, htcondordb.STRING, htcondordb.NUMBER}) == 2

    def test_constructors(self):
        assert htcondordb.Date(2026, 1, 2).year == 2026
        assert htcondordb.Time(3, 4, 5).hour == 3
        assert htcondordb.Timestamp(2026, 1, 2, 3, 4, 5).minute == 4
        assert htcondordb.Binary("abc") == b"abc"

    def test_timestamp_from_ticks_reads_an_htcondor_time(self):
        # HTCondor stores QDate and friends as epoch integers, so this is the intended
        # path from a result cell to a datetime.
        moment = htcondordb.TimestampFromTicks(1_767_225_845)
        assert moment.year >= 2025


class TestLibraryLoading:
    def test_missing_library_reports_where_it_looked(self, monkeypatch):
        # A "cannot load" error is the first thing a new user hits; it has to say what to
        # build and where the driver searched.
        import htcondordb._library as library

        monkeypatch.setattr(library, "_loaded", None)
        monkeypatch.setenv(library.LIBRARY_ENV, "/nonexistent/libhtcondordb_client.so")
        monkeypatch.setattr(library, "_candidate_paths", lambda: ["/nonexistent/lib.so"])

        with pytest.raises(htcondordb.InterfaceError) as excinfo:
            library.load()
        message = str(excinfo.value)
        assert "make lib" in message
        assert "/nonexistent/lib.so" in message


class TestCursorWithoutExecute:
    """Cursor state before anything has run, checked against a stub connection.

    These paths are pure driver logic, so they are worth covering without a daemon.
    """

    def _cursor(self):
        from htcondordb.cursor import Cursor

        class _StubConnection:
            def _check_open(self):
                pass

            def _forget(self, cursor):
                pass

            def _note_transaction_state(self, in_transaction):
                pass

        return Cursor(_StubConnection())

    def test_initial_state(self):
        cursor = self._cursor()
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.arraysize == 1

    def test_fetch_before_execute_raises(self):
        cursor = self._cursor()
        with pytest.raises(htcondordb.ProgrammingError, match="no statement"):
            cursor.fetchone()
        with pytest.raises(htcondordb.ProgrammingError, match="no statement"):
            cursor.fetchall()

    def test_use_after_close_raises(self):
        cursor = self._cursor()
        cursor.close()
        with pytest.raises(htcondordb.InterfaceError, match="closed"):
            cursor.fetchone()

    def test_close_is_idempotent(self):
        cursor = self._cursor()
        cursor.close()
        cursor.close()

    def test_load_select_populates_description_and_rows(self):
        cursor = self._cursor()
        cursor._load(
            {
                "select": True,
                "columns": ["Owner", "n", "ratio", "nothing"],
                "rows": [["alice", 3, 1.5, None], ["bob", 1, 2.5, None]],
            }
        )
        assert cursor.rowcount == 2
        assert [d[0] for d in cursor.description] == ["Owner", "n", "ratio", "nothing"]
        assert cursor.description[0][1] == htcondordb.STRING
        assert cursor.description[1][1] == htcondordb.NUMBER
        assert cursor.description[2][1] == htcondordb.NUMBER
        # A column that is null all the way down has nothing to infer a type from.
        assert cursor.description[3][1] is None
        assert cursor.fetchall() == [("alice", 3, 1.5, None), ("bob", 1, 2.5, None)]

    def test_load_dml_reports_affected_and_no_description(self):
        cursor = self._cursor()
        cursor._load({"select": False, "affected": 7, "note": "UPDATE 7"})
        assert cursor.rowcount == 7
        assert cursor.description is None

    def test_fetchmany_respects_arraysize_and_exhausts(self):
        cursor = self._cursor()
        cursor._load({"select": True, "columns": ["n"], "rows": [[1], [2], [3]]})
        cursor.arraysize = 2
        assert cursor.fetchmany() == [(1,), (2,)]
        assert cursor.fetchmany() == [(3,)]
        assert cursor.fetchmany() == []
        assert cursor.fetchone() is None

    def test_iteration(self):
        cursor = self._cursor()
        cursor._load({"select": True, "columns": ["n"], "rows": [[1], [2]]})
        assert [row[0] for row in cursor] == [1, 2]

    def test_empty_select_is_not_a_dml_result(self):
        # rowcount 0 with a description says "a SELECT matched nothing", which is a
        # different thing from an UPDATE that wrote nothing.
        cursor = self._cursor()
        cursor._load({"select": True, "columns": ["Owner"], "rows": []})
        assert cursor.rowcount == 0
        assert cursor.description is not None
        assert cursor.fetchall() == []
