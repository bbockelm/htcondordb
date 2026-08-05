"""The PEP 249 ``Connection`` object."""

from __future__ import annotations

import threading
from typing import Any

from . import _errors, _library
from ._errors import InterfaceError, NotSupportedError, OperationalError
from .cursor import Cursor


class Connection:
    """An authenticated session with an htcondordb daemon.

    Build one with :func:`htcondordb.connect`. The connection owns a single CEDAR stream,
    so statements on it are serialized (in the C library, which holds a lock for the
    duration of each one); cursors on the same connection are safe to use from several
    threads, but they will not run concurrently.

    Transactions: htcondordb commits each statement as it executes and exposes no
    transaction to the SQL layer, so :meth:`commit` is a no-op and :meth:`rollback` raises
    ``NotSupportedError``. The connection is effectively always in autocommit mode.
    """

    # PEP 249 optional extension: the exception classes are reachable from the connection,
    # so `except conn.ProgrammingError` works when the module itself is not in scope.
    Warning = _errors.Warning
    Error = _errors.Error
    InterfaceError = _errors.InterfaceError
    DatabaseError = _errors.DatabaseError
    DataError = _errors.DataError
    OperationalError = _errors.OperationalError
    IntegrityError = _errors.IntegrityError
    InternalError = _errors.InternalError
    ProgrammingError = _errors.ProgrammingError
    NotSupportedError = _errors.NotSupportedError

    def __init__(self, address: str) -> None:
        self._address = address
        self._lib = _library.load()
        self._lock = threading.Lock()
        self._cursors: set[Cursor] = set()

        ffi, lib = self._lib.ffi, self._lib.lib
        err = ffi.new("char **")
        handle = lib.hcdb_connect_err(address.encode("utf-8"), err)
        if handle == 0:
            reason = self._lib.string(err[0]) or "unknown error"
            raise OperationalError(f"connecting to {address}: {reason}")
        self._handle = handle

    # --- PEP 249 interface ---

    def close(self) -> None:
        """Close the connection and every cursor on it.

        Closing twice is not an error. Any later use of the connection or one of its
        cursors raises ``InterfaceError``.
        """
        with self._lock:
            if self._handle == 0:
                return
            handle, self._handle = self._handle, 0
        for cursor in list(self._cursors):
            cursor.close()
        self._cursors.clear()
        self._lib.lib.hcdb_close(handle)

    def commit(self) -> None:
        """No-op: every statement has already committed as it ran.

        PEP 249 requires the method to exist, and requires a driver without transaction
        support to make it a no-op rather than an error -- code that calls it defensively
        after a write must keep working.
        """

    def rollback(self) -> None:
        """Unsupported: there is no open transaction to undo.

        Raises:
            NotSupportedError: always. Raising is deliberate -- silently doing nothing
                would let a caller believe a failed batch had been undone.
        """
        raise NotSupportedError(
            "htcondordb commits each statement as it runs; there is no transaction to roll back"
        )

    def cursor(self) -> Cursor:
        """Return a new :class:`~htcondordb.cursor.Cursor` on this connection."""
        self._check_open()
        cursor = Cursor(self)
        self._cursors.add(cursor)
        return cursor

    # --- extensions ---

    @property
    def address(self) -> str:
        """The daemon address this connection was opened against."""
        return self._address

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` has been called."""
        return self._handle == 0

    def execute(self, operation: str, parameters: Any = None) -> Cursor:
        """Convenience: open a cursor, run one statement on it, and return it.

        Not part of PEP 249, but the shape ``sqlite3`` established and the one a reporting
        script reaches for::

            for row in conn.execute("SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner"):
                ...
        """
        cursor = self.cursor()
        try:
            cursor.execute(operation, parameters)
        except Exception:
            cursor.close()
            raise
        return cursor

    # --- context manager ---

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Unlike a transactional driver, there is nothing to commit or roll back on the
        # way out, so the block simply owns the connection's lifetime.
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        return f"<htcondordb.Connection {self._address!r} [{state}]>"

    # --- internals ---

    def _check_open(self) -> None:
        if self._handle == 0:
            raise InterfaceError("the connection is closed")

    def _forget(self, cursor: Cursor) -> None:
        """Drop a closed cursor's registration so it is not closed twice."""
        self._cursors.discard(cursor)
