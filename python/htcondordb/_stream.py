"""The chunked row cursor behind :meth:`Cursor.execute`.

The library used to answer a statement with one JSON document holding every row, so a SELECT
over a large table existed three times at once -- as ads in Go, as JSON bytes, and as tuples
here -- and nothing was available until the last row arrived. This walks the same result in
batches instead: a header at open, then rows on demand.

Whether the daemon can actually produce rows incrementally depends on the statement (see
``repl.RowStreamer``); ``header["streamed"]`` says which happened. Nothing else about the rows
differs, so callers should not branch on it -- it is for diagnostics and for knowing whether the
executor lock is being held.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from . import _library

if TYPE_CHECKING:  # pragma: no cover
    from .connection import Connection


class RowStream:
    """One statement's rows, pulled from the library a batch at a time.

    Opening runs the statement: a DML or DDL statement has finished by the time the constructor
    returns, and reports through :attr:`header`. A SELECT may still be running, in which case
    the connection's executor lock is held until this is exhausted or closed -- which is why
    :class:`~htcondordb.connection.Connection` settles live streams before starting anything
    else on the same connection.
    """

    def __init__(
        self, connection: "Connection", statement: str, objects: bool = False
    ) -> None:
        lib = connection._lib
        ffi, c = lib.ffi, lib.lib

        cursor = ffi.new("uintptr_t *")
        header = ffi.new("char **")
        out = ffi.new("char **")
        options = _library.STREAM_ROWS_AS_OBJECTS if objects else 0
        code = c.hcdb_sql_stream(
            connection._handle, statement.encode("utf-8"), options, cursor, header, out
        )
        if code != _library.RESULT_OK:
            message = lib.string(out[0]) or "the statement failed with no message"
            raise _library.exception_for(code, message)

        self._lib = lib
        self._objects = objects
        self._handle = int(cursor[0])
        #: The result header: select, columns, affected, note, star, in_transaction, streamed.
        self.header: dict[str, Any] = json.loads(lib.string(header[0]))
        # A statement that returns no rows is over already; asking for a batch would be a
        # round trip to be told so.
        self._exhausted = not self.header.get("select", False)

    @property
    def streaming(self) -> bool:
        """Whether rows are still arriving from the server, and so the lock is still held."""
        return bool(self.header.get("streamed")) and not self._exhausted

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    def next_batch(self, size: int) -> list:
        """Return up to *size* more rows, or an empty list when the result is exhausted.

        Rows are tuples, or dicts when the stream was opened with ``objects``.

        A short batch does not mean the end -- the library also caps a batch by byte size, so
        only an empty one is terminal.
        """
        if self._exhausted or self._handle == 0:
            return []

        ffi, c = self._lib.ffi, self._lib.lib
        out = ffi.new("char **")
        code = c.hcdb_sql_stream_next(self._handle, size, out)
        payload = self._lib.string(out[0])

        if code == _library.RESULT_OK:
            try:
                rows = json.loads(payload)
                return rows if self._objects else [tuple(row) for row in rows]
            except ValueError as exc:  # a malformed batch is a driver/library mismatch
                self.close()
                raise _library.exception_for(
                    _library.RESULT_ERR,
                    f"could not decode a row batch from {self._lib.path}: {exc}",
                ) from exc

        # Exhausted or failed: either way the result is over, so release the cursor here rather
        # than leaving the caller holding one that can only report the same thing -- and, for a
        # stream, holding the connection's executor lock while doing it.
        self.close()
        if code == _library.RESULT_MISSING:
            return []
        raise _library.exception_for(
            code, payload or "the row stream failed with no message"
        )

    def close(self) -> None:
        """Release the cursor and stop the stream behind it. Closing twice is not an error."""
        self._exhausted = True
        if self._handle == 0:
            return
        handle, self._handle = self._handle, 0
        self._lib.lib.hcdb_sql_stream_free(handle)
