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

from ._errors import InterfaceError

#: Return codes from the C library. These mirror the ``hcdb*`` constants in capi/.
RESULT_OK = 0
RESULT_ERR = -1
RESULT_MISSING = -2
RESULT_BAD_SQL = -3
RESULT_DENIED = -4

#: ``hcdb_sql`` option bits.
SQL_ADS = 1 << 0

#: Environment variable naming the shared library explicitly.
LIBRARY_ENV = "HTCONDORDB_LIBRARY"

_CDEF = """
uintptr_t hcdb_connect(char *addr);
uintptr_t hcdb_connect_err(char *addr, char **err);
uintptr_t hcdb_query(uintptr_t h, char *table, char *constraint);
int hcdb_query_next(uintptr_t qh, char **out);
void hcdb_query_free(uintptr_t qh);
int hcdb_sql(uintptr_t h, char *sql, int opts, char **out);
uintptr_t hcdb_sql_ads(uintptr_t h, char *sql, char **err);
int hcdb_sql_ads_next(uintptr_t ch, char **out);
void hcdb_sql_ads_free(uintptr_t ch);
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


class _Library:
    """A loaded ``libhtcondordb_client``, with its cffi handle."""

    def __init__(self, ffi, lib, path: str) -> None:
        self.ffi = ffi
        self.lib = lib
        self.path = path

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
                    "hcdb_sql",
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
