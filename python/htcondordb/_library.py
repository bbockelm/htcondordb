"""Loading and calling ``libhtcondordb_client``.

The library is the Go client in ``capi/`` built with ``-buildmode=c-shared``: it carries
the whole CEDAR stack -- transport, authentication (pool tokens, SSL, FS, ...), the dbrpc
protocol, and the SQL parser and executor. This module is the only place that touches it.

cffi is used in ABI mode (``ffi.dlopen``), so installing this package needs no C compiler
and no build step -- just the shared library. cffi releases the GIL around each call, so a
long-running query does not block other Python threads.
"""

from __future__ import annotations

import os
import sys
import threading
from ctypes.util import find_library
from pathlib import Path

from . import _errors
from ._errors import InterfaceError

#: Return codes from the C library. These mirror the ``hcdb*`` constants in capi/.
RESULT_OK = 0
RESULT_ERR = -1
RESULT_MISSING = -2
RESULT_BAD_SQL = -3
RESULT_DENIED = -4
RESULT_PANIC = -5

#: Prefix of a message produced by a recovered panic in the library. Two entry points return a
#: handle and so report failure only as a string, with no code to carry RESULT_PANIC; this is
#: the contract they use instead. It must match panicPrefix in capi/guard.go.
PANIC_PREFIX = "internal error in "


#: Set in a child process after os.fork(). Go's runtime does not survive a fork -- its threads
#: are not recreated in the child -- so the library is unusable there, and a call would hang or
#: crash rather than fail. This turns that into an exception naming the cause.
_forked_child = False


def _mark_forked_child() -> None:  # pragma: no cover - runs only in a forked child
    global _forked_child
    _forked_child = True


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_mark_forked_child)


def check_usable() -> None:
    """Raise if this process cannot use the library at all.

    Only one case so far: a child of os.fork(). The Go runtime the library is built on does not
    survive forking, and that is not something the child can recover from -- opening a fresh
    connection does not help, because the broken runtime is already loaded. multiprocessing's
    "spawn" and "forkserver" start methods are fine (they exec a new interpreter); "fork", the
    default on Linux before CPython 3.14, is not.
    """
    if _forked_child:
        raise InterfaceError(
            "htcondordb cannot be used in a process forked from one that loaded it: Go's "
            "runtime does not survive fork(). Use multiprocessing with the 'spawn' or "
            "'forkserver' start method (multiprocessing.set_start_method('spawn')), or connect "
            "in each worker after it starts by exec rather than inheriting a connection."
        )


def exception_for(code: int, message: str) -> Exception:
    """Map a library result code to the exception the driver raises.

    One table, used by every call site: the C library classifies its own failures, so a new code
    cannot be handled in one place and forgotten in another. See the hcdb* constants in capi/.
    """
    if code in (RESULT_BAD_SQL, RESULT_DENIED):
        return _errors.ProgrammingError(message)
    if code == RESULT_PANIC:
        # A bug in the library, recovered at the boundary rather than taking the interpreter
        # down. InternalError is PEP 249's category for a failure that is not the caller's
        # fault and that retrying will not fix.
        return _errors.InternalError(message)
    if code == RESULT_ERR:
        return _errors.OperationalError(message)
    return _errors.DatabaseError(f"unexpected result code {code}: {message}")


def is_internal(message: str) -> bool:
    """Whether a failure message came from a bug inside the library rather than from the call.

    A panic in the Go stack is recovered at the boundary and reported like any other failure,
    so without this the driver would raise OperationalError -- telling the caller to check
    their network or their credentials for what is really a defect to report.
    """
    return message.startswith(PANIC_PREFIX)

#: ``hcdb_sql`` option bits.
SQL_ADS = 1 << 0

#: ``hcdb_sql_stream`` option bits. Keyed rows need no column list, which is what lets
#: ``SELECT *`` stream -- see hcdbRowsAsObjects in capi/rows.go.
STREAM_ROWS_AS_OBJECTS = 1 << 0

#: Environment variable naming the shared library explicitly.
LIBRARY_ENV = "HTCONDORDB_LIBRARY"

_CDEF = """
uintptr_t hcdb_connect(char *addr);
uintptr_t hcdb_connect_err(char *addr, char **err);
int hcdb_address(uintptr_t h, char **out);
int hcdb_setenv(char *name, char *value);
int hcdb_selftest_panic(char **out);
int hcdb_sql_stream(uintptr_t h, char *sql, int opts, long long timeout_us, uintptr_t *cursor, char **header, char **out);
int hcdb_set_log_level(char *level);
int hcdb_sql_stream_next(uintptr_t ch, int max_rows, char **out);
void hcdb_sql_stream_free(uintptr_t ch);
uintptr_t hcdb_query(uintptr_t h, char *table, char *constraint);
int hcdb_query_next(uintptr_t qh, char **out);
void hcdb_query_free(uintptr_t qh);
int hcdb_sql(uintptr_t h, char *sql, int opts, char **out);
uintptr_t hcdb_sql_ads(uintptr_t h, char *sql, char **err);
int hcdb_sql_ads_next(uintptr_t ch, char **out);
void hcdb_sql_ads_free(uintptr_t ch);
int hcdb_write_ads(uintptr_t h, char *req, char **out);
void hcdb_close(uintptr_t h);
void hcdb_free(char *p);
"""

_lock = threading.Lock()
_loaded: "_Library | None" = None


def _library_names() -> list[str]:
    """Platform-appropriate file names for the shared library."""
    if sys.platform == "darwin":
        return ["libhtcondordb_client.dylib"]
    if sys.platform == "win32":
        return ["htcondordb_client.dll", "libhtcondordb_client.dll"]
    return ["libhtcondordb_client.so"]


def _candidate_paths() -> list[str]:
    """Where to look for the shared library, in priority order.

    An explicit ``HTCONDORDB_LIBRARY`` always wins -- it is how a developer points at a
    freshly built ``bin/`` and how the test suite pins the library under test. Next comes
    a copy bundled inside the installed package (how a wheel ships it), then the repo's
    own ``bin/`` for an editable checkout, then the loader's default search path.
    """
    candidates: list[str] = []

    explicit = os.environ.get(LIBRARY_ENV)
    if explicit:
        candidates.append(explicit)

    here = Path(__file__).resolve().parent
    for name in _library_names():
        candidates.append(str(here / "_lib" / name))  # bundled in a wheel
        candidates.append(str(here.parent.parent / "bin" / name))  # repo checkout

    # Bare names fall through to the dynamic loader's own search (LD_LIBRARY_PATH,
    # DYLD_LIBRARY_PATH, the system paths), then to a full path if ctypes can find one.
    candidates.extend(_library_names())
    for name in _library_names():
        stem = name.removeprefix("lib").rsplit(".", 1)[0]
        found = find_library(stem)
        if found:
            candidates.append(found)

    return candidates


# What the library reads from the environment: the configuration file, the two knobs that
# name the daemon, and the `_CONDOR_<KNOB>` convention for overriding any knob at all.
# Nothing else in the environment is copied into another runtime.
_CONDOR_ENV_NAMES = frozenset(
    {
        "CONDOR_CONFIG",
        "HTCONDORDB_ADDRESS_FILE",
        "HTCONDORDB_HOST",
        # Read by the library when it configures its log destination; see set_log_level.
        "HTCONDORDB_LOG_LEVEL",
    }
)
_CONDOR_ENV_PREFIX = "_CONDOR_"


class _Library:
    """A loaded ``libhtcondordb_client``, with its cffi handle."""

    def __init__(self, ffi, lib, path: str) -> None:
        self.ffi = ffi
        self.lib = lib
        self.path = path
        # Names handed to hcdb_setenv so far, so one that goes away can be unset.
        self._pushed_env: set[str] = set()

    def sync_environment(self) -> None:
        """Push HTCondor environment variables into the library.

        Go answers ``os.Getenv`` from a copy of the environment taken when the library is
        loaded, so a variable this process sets afterwards -- the usual
        ``os.environ["CONDOR_CONFIG"] = ...`` before connecting -- would otherwise be
        invisible and the library would quietly read a different configuration. Pushing the
        current values across at connect time is what makes the documented
        "configuration is ambient" behaviour actually true.

        Variables pushed on an earlier call and since removed are unset, so deleting one
        takes effect too.
        """
        current = {
            name: value
            for name, value in os.environ.items()
            if name in _CONDOR_ENV_NAMES or name.startswith(_CONDOR_ENV_PREFIX)
        }
        for name in self._pushed_env - current.keys():
            self.lib.hcdb_setenv(name.encode("utf-8"), self.ffi.NULL)
        for name, value in current.items():
            self.lib.hcdb_setenv(name.encode("utf-8"), value.encode("utf-8"))
        self._pushed_env = set(current)

    def string(self, ptr) -> str:
        """Copy a C string the library allocated into a ``str``, then free it.

        Every ``char *`` the library writes through an out-parameter is owned by the
        caller and must go back through ``hcdb_free``; doing the copy and the free in one
        place is what keeps that from being forgotten at a call site.
        """
        if ptr == self.ffi.NULL:
            return ""
        try:
            return self.ffi.string(ptr).decode("utf-8", "replace")
        finally:
            self.lib.hcdb_free(ptr)


def load() -> _Library:
    """Load the shared library, once per process.

    Raises:
        InterfaceError: if no candidate path could be opened, or one opened but did not
            export the expected symbols (an out-of-date library alongside a newer driver).
    """
    global _loaded
    with _lock:
        if _loaded is not None:
            return _loaded

        try:
            import cffi
        except ImportError as exc:  # pragma: no cover - packaging guarantees cffi
            raise InterfaceError(
                "the htcondordb driver requires cffi; install it with 'pip install cffi'"
            ) from exc

        ffi = cffi.FFI()
        ffi.cdef(_CDEF)

        attempts: list[str] = []
        for path in _candidate_paths():
            try:
                lib = ffi.dlopen(path)
            except OSError as exc:
                attempts.append(f"  {path}: {exc}")
                continue
            # dlopen succeeds on any shared object; a missing symbol only surfaces on
            # first access. Check now so the failure names the real problem instead of
            # surfacing as an AttributeError from inside a query.
            missing = [
                name
                for name in (
                    "hcdb_connect_err",
                    "hcdb_address",
                    "hcdb_setenv",
                    "hcdb_sql",
                    "hcdb_sql_stream",
                    "hcdb_set_log_level",
                    "hcdb_sql_ads",
                    "hcdb_close",
                    "hcdb_free",
                )
                if not hasattr(lib, name)
            ]
            if missing:
                attempts.append(
                    f"  {path}: loaded but missing {', '.join(missing)}"
                    " (library predates this driver -- rebuild it with 'make lib')"
                )
                continue
            _loaded = _Library(ffi, lib, path)
            return _loaded

        raise InterfaceError(
            "could not load libhtcondordb_client. Build it with 'make lib' and set "
            f"{LIBRARY_ENV} to the result, or install it on the library search path.\n"
            "Tried:\n" + "\n".join(attempts)
        )


def library_path() -> str:
    """Path of the loaded shared library (useful in bug reports)."""
    return load().path
