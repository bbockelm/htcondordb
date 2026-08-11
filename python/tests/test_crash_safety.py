"""The library must not be able to take the interpreter down with it.

A panic that unwinds out of a cgo-exported function aborts the whole host process: no
traceback, no exception, no chance for a report to log and move on. Every entry point is
wrapped in a recover() for that reason, and these tests hold it to it -- garbage handles at
every entry point, and a deliberate panic through the guard.

Nothing here needs a daemon: the point is what happens *before* any connection exists, which
is exactly where a caller's bug (a stale handle, a double free) lands.

If a guard regresses, these tests do not fail politely -- pytest dies with SIGSEGV or SIGABRT
and reports the whole session as crashed. That is the signal.
"""

from __future__ import annotations

import pytest

from htcondordb import _library

# Not a handle: cgo hands out small sequential integers, so this is stale-or-never-valid no
# matter how many connections a test session has opened.
BOGUS = 999_999


@pytest.fixture(scope="module")
def lib(library_available):
    if not library_available:
        pytest.skip("shared library not built; run 'make lib'")
    return _library.load()


@pytest.fixture(scope="module")
def selftest(lib):
    """The deliberate-panic seam, which is test-only and so not a required symbol."""
    if not hasattr(lib.lib, "hcdb_selftest_panic"):
        pytest.skip("library predates hcdb_selftest_panic; run 'make lib'")
    return lib


def out_param(lib):
    return lib.ffi.new("char **")


class TestBogusHandles:
    """Every entry point that takes a handle must reject one it does not own."""

    def test_sql(self, lib):
        out = out_param(lib)
        code = lib.lib.hcdb_sql(BOGUS, b"SELECT * FROM whatever", 0, out)
        assert code == _library.RESULT_ERR
        assert "invalid connection handle" in lib.string(out[0])

    def test_write_ads(self, lib):
        out = out_param(lib)
        code = lib.lib.hcdb_write_ads(BOGUS, b'{"table":"t","ads":[]}', out)
        assert code == _library.RESULT_ERR
        assert "invalid connection handle" in lib.string(out[0])

    def test_sql_ads_open(self, lib):
        err = out_param(lib)
        assert lib.lib.hcdb_sql_ads(BOGUS, b"SELECT * FROM whatever", err) == 0
        assert "invalid connection handle" in lib.string(err[0])

    def test_sql_ads_next(self, lib):
        out = out_param(lib)
        assert lib.lib.hcdb_sql_ads_next(BOGUS, out) == _library.RESULT_ERR
        assert "invalid cursor handle" in lib.string(out[0])

    def test_sql_ads_free(self, lib):
        lib.lib.hcdb_sql_ads_free(BOGUS)  # nothing to assert but survival

    def test_query(self, lib):
        assert lib.lib.hcdb_query(BOGUS, lib.ffi.NULL, lib.ffi.NULL) == 0

    def test_query_next(self, lib):
        out = out_param(lib)
        assert lib.lib.hcdb_query_next(BOGUS, out) == _library.RESULT_ERR
        assert "invalid cursor handle" in lib.string(out[0])

    def test_query_free_twice(self, lib):
        # This one used to abort the process: cgo.Handle.Delete panics on a handle that was
        # never valid or has already been freed, and freeing twice is an ordinary mistake.
        lib.lib.hcdb_query_free(BOGUS)
        lib.lib.hcdb_query_free(BOGUS)

    def test_close_twice(self, lib):
        lib.lib.hcdb_close(BOGUS)
        lib.lib.hcdb_close(BOGUS)

    def test_address(self, lib):
        out = out_param(lib)
        assert lib.lib.hcdb_address(BOGUS, out) == _library.RESULT_ERR

    def test_setenv_rejects_a_null_name(self, lib):
        assert lib.lib.hcdb_setenv(lib.ffi.NULL, lib.ffi.NULL) == _library.RESULT_ERR


class TestPanicBecomesAnError:
    """A panic inside an entry point has to arrive as a value, not a signal."""

    def test_reports_a_panic_code_and_message(self, selftest, lib):
        out = out_param(lib)
        code = lib.lib.hcdb_selftest_panic(out)
        message = lib.string(out[0])

        assert code == _library.RESULT_PANIC
        assert message.startswith(_library.PANIC_PREFIX)
        assert "deliberate panic" in message
        # The stack trace travels with the message: without it a bug report from a user is
        # "it raised InternalError somewhere", which is not actionable.
        assert "goroutine" in message and "capi" in message

    def test_the_library_still_works_afterwards(self, selftest, lib):
        # Recovering is only useful if it leaves the library usable -- a guard that survives
        # the panic but poisons the runtime would be no better than crashing.
        lib.lib.hcdb_selftest_panic(out_param(lib))
        assert lib.lib.hcdb_setenv(b"HTCONDORDB_CRASH_TEST", b"1") == _library.RESULT_OK
        assert lib.lib.hcdb_setenv(b"HTCONDORDB_CRASH_TEST", lib.ffi.NULL) == _library.RESULT_OK

    def test_classified_as_internal(self):
        # The driver has to raise InternalError for these, not OperationalError: a panic is
        # not a network problem and retrying will not help.
        assert _library.is_internal(_library.PANIC_PREFIX + "hcdb_sql: boom")
        assert not _library.is_internal("connection refused")
        assert not _library.is_internal("")

