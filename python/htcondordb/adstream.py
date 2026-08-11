"""Streaming ClassAd results.

The DB-API path in :mod:`htcondordb.cursor` returns rows of evaluated values, which is what
a report or a pandas frame wants. This is the other half: an iterator of
``classad2.ClassAd``, where an attribute holding an expression stays an expression.

The distinction matters for HTCondor data. ``SELECT Requirements`` answers ``True`` or
``False`` per row, because the column API evaluates. Iterating ads instead lets a caller
reach ``ad.lookup("Requirements")`` for the ``ExprTree`` itself, evaluate it against a
candidate job, or simplify it -- none of which survives a trip through a result cell.

Rows arrive one at a time from the server, so walking a large table costs one ad rather
than the whole set.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Sequence

from . import _library
from ._errors import (
    DatabaseError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)
from ._params import bind

if TYPE_CHECKING:  # pragma: no cover
    from .connection import Connection


def require_classad2():
    """Import classad2, or explain what is missing.

    A hard error rather than a text fallback: the whole point of this path is live
    expressions, and handing back strings would quietly turn it into the thing it exists
    to avoid. ``Cursor.ad_text`` is there when text really is what you want.
    """
    try:
        import classad2
    except ImportError as exc:
        raise InterfaceError(
            "iterating ClassAds needs HTCondor's classad2 bindings (pip install htcondor). "
            "For the unparsed text without them, use "
            "cursor.execute_with_ads(...) and read cursor.ad_text."
        ) from exc
    return classad2


class AdStream:
    """An iterator over a SELECT's rows as ``classad2.ClassAd`` objects.

    Yielded lazily: the next ad is fetched from the server when it is asked for, so an
    abandoned iterator stops the query rather than draining it. Closing is automatic when
    the iterator is exhausted or garbage collected; :meth:`close` is available for a
    deterministic stop, and the object is a context manager.
    """

    def __init__(self, connection: "Connection", statement: str) -> None:
        self._connection = connection
        self._classad2 = require_classad2()
        self._lib = connection._lib
        self._handle = 0
        self._exhausted = False

        connection._check_open()
        ffi, lib = self._lib.ffi, self._lib.lib
        err = ffi.new("char **")
        handle = lib.hcdb_sql_ads(connection._handle, statement.encode("utf-8"), err)
        if handle == 0:
            reason = self._lib.string(err[0]) or "unknown error"
            raise _classify(reason)
        self._handle = handle

    # --- iteration ---

    def __iter__(self) -> "AdStream":
        return self

    def __next__(self):
        if self._exhausted:
            raise StopIteration
        if self._handle == 0:
            raise InterfaceError("the ad stream is closed")

        ffi, lib = self._lib.ffi, self._lib.lib
        out = ffi.new("char **")
        code = lib.hcdb_sql_ads_next(self._handle, out)
        payload = self._lib.string(out[0])

        if code == _library.RESULT_OK:
            return self._classad2.ClassAd(payload)
        # Exhausted or failed: either way the stream is over, so release it here rather
        # than leaving the caller holding a cursor that can only report the same thing.
        self.close()
        if code == _library.RESULT_MISSING:
            raise StopIteration
        message = payload or "the ad stream failed with no message"
        if code == _library.RESULT_PANIC:
            raise InternalError(message)
        raise OperationalError(message)

    # --- lifetime ---

    def close(self) -> None:
        """Stop the stream and release it. Closing twice is not an error."""
        self._exhausted = True
        if self._handle != 0:
            handle, self._handle = self._handle, 0
            self._lib.lib.hcdb_sql_ads_free(handle)

    def __enter__(self) -> "AdStream":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - GC timing
        try:
            self.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        state = "closed" if self._handle == 0 else "open"
        return f"<htcondordb.AdStream [{state}]>"


def ads(
    connection: "Connection", operation: str, parameters: Sequence[Any] | None = None
) -> AdStream:
    """Run a SELECT and return an :class:`AdStream` over its ClassAds.

    Parameters bind exactly as they do for :meth:`~htcondordb.cursor.Cursor.execute` --
    same ``?`` placeholders, same quoting, same rejections -- because the statement text is
    built by the same function before it reaches the library.
    """
    connection._check_open()
    return AdStream(connection, bind(operation, parameters))


def _classify(reason: str) -> DatabaseError:
    """Map an open failure to an exception.

    Opening parses the statement, so the common failures are the caller's: bad SQL, or a
    statement that is not a SELECT and so has no ads behind it. A recovered panic is the
    exception -- that one is the library's own fault.
    """
    if _library.is_internal(reason):
        return InternalError(reason)
    lowered = reason.lower()
    if "only select" in lowered or "unsupported statement" in lowered or "expected" in lowered:
        return ProgrammingError(reason)
    return OperationalError(reason)
