"""The PEP 249 ``Connection`` object."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator

from . import _errors, _library
from ._errors import (
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from .cursor import Cursor

if TYPE_CHECKING:  # pragma: no cover
    from .adstream import AdStream


class Connection:
    """An authenticated session with an htcondordb daemon.

    Build one with :func:`htcondordb.connect`. The connection owns a single CEDAR stream,
    so statements on it are serialized (in the C library, which holds a lock for the
    duration of each one); cursors on the same connection are safe to use from several
    threads, but they will not run concurrently.

    **Transactions.** The connection starts in autocommit mode, where each statement
    commits as it runs, :meth:`commit` is a no-op, and :meth:`rollback` raises. Setting
    :attr:`autocommit` to ``False`` -- or using the :meth:`transaction` context manager --
    opens a real transaction that batches writes until :meth:`commit` or :meth:`rollback`.

    Autocommit defaults to ``True``, unlike most DB-API drivers, because a transaction here
    has two constraints worth opting into deliberately rather than inheriting silently:

    * **Reads do not join the transaction.** A ``SELECT`` always reads committed state, so
      it will not see the transaction's own uncommitted writes. ``UPDATE`` and ``DELETE``
      choose their rows with a query, so they too act on committed state.
    * **A transaction cannot span tables.** It binds to the first table written; a write
      to a second table raises ``ProgrammingError``.

    Neither is a driver limitation -- a dbrpc transaction is scoped to one table, and
    queries carry no transaction id.
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

    def __init__(self, address: str, autocommit: bool = True) -> None:
        self._address = address
        self._lib = _library.load()
        self._lock = threading.Lock()
        self._cursors: set[Cursor] = set()
        self._autocommit = True
        #: Whether a transaction is open, tracked from each statement's reported state.
        self._in_transaction = False

        ffi, lib = self._lib.ffi, self._lib.lib
        err = ffi.new("char **")
        handle = lib.hcdb_connect_err(address.encode("utf-8"), err)
        if handle == 0:
            reason = self._lib.string(err[0]) or "unknown error"
            raise OperationalError(f"connecting to {address}: {reason}")
        self._handle = handle

        # Set through the property so a False opens the first transaction, exactly as a
        # later assignment would.
        if not autocommit:
            self.autocommit = False

    # --- PEP 249 interface ---

    def close(self) -> None:
        """Close the connection and every cursor on it.

        An open transaction is rolled back, as PEP 249 requires -- closing without
        committing must not apply the pending writes. (The daemon aborts open
        transactions when the connection drops, so this is belt and braces, but it makes
        the behaviour deterministic rather than dependent on server-side cleanup.)

        Closing twice is not an error. Any later use of the connection or one of its
        cursors raises ``InterfaceError``.
        """
        if self._in_transaction and self._handle != 0:
            self._autocommit = True  # do not reopen a transaction we are about to drop
            try:
                self._end_transaction("ROLLBACK")
            except Exception:
                pass  # closing must succeed regardless

        with self._lock:
            if self._handle == 0:
                return
            handle, self._handle = self._handle, 0
        for cursor in list(self._cursors):
            cursor.close()
        self._cursors.clear()
        self._lib.lib.hcdb_close(handle)

    def commit(self) -> None:
        """Commit the open transaction.

        A no-op in autocommit mode, where each statement has already committed. PEP 249
        requires that: code calling ``commit()`` defensively after a write must keep
        working whether or not a transaction was open.

        Raises:
            DatabaseError: if the commit failed -- notably ``OperationalError`` when a
                write lost a write-write race, whose message names the conflicted keys.
                The transaction is over either way; it is not left open to retry.
        """
        self._check_open()
        self._end_transaction("COMMIT")

    def rollback(self) -> None:
        """Discard the open transaction's writes.

        Raises:
            NotSupportedError: in autocommit mode, where each statement has already
                committed and there is nothing to undo. Raising rather than silently
                doing nothing is deliberate: a caller rolling back after a failed batch
                must not be left believing the batch was undone. Set
                :attr:`autocommit` to ``False`` first, or use :meth:`transaction`.
        """
        self._check_open()
        if not self._in_transaction:
            raise NotSupportedError(
                "no transaction is open, so there is nothing to roll back. This "
                "connection is in autocommit mode: set connection.autocommit = False, "
                "or use 'with connection.transaction():', to batch writes into a "
                "transaction that can be rolled back."
            )
        self._end_transaction("ROLLBACK")

    # --- transactions ---

    @property
    def autocommit(self) -> bool:
        """Whether each statement commits as it runs (the default).

        Setting this to ``False`` opens a transaction immediately, and a fresh one after
        every :meth:`commit` or :meth:`rollback`. Setting it back to ``True`` commits any
        transaction currently open -- matching what other DB-API drivers do, and matching
        what "stop batching, apply my work" means.
        """
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._check_open()
        value = bool(value)
        if value == self._autocommit:
            return
        if value:
            # Flip the mode first: _end_transaction reopens a transaction while the
            # connection is still non-autocommit, which is exactly what we do not want
            # on the way out of transactional mode.
            self._autocommit = True
            # Leaving transactional mode applies what is pending rather than discarding it.
            if self._in_transaction:
                self._end_transaction("COMMIT")
        else:
            self._autocommit = False
            self._begin()

    @property
    def in_transaction(self) -> bool:
        """Whether a transaction is currently open."""
        return self._in_transaction

    @contextmanager
    def transaction(self) -> Iterator["Connection"]:
        """Run a block inside a transaction, committing on success.

        Commits when the block finishes, rolls back if it raises::

            with conn.transaction():
                cur = conn.cursor()
                for row in rows:
                    cur.execute("INSERT INTO jobs (Key, Owner) VALUES (?, ?)", row)

        Restores the connection's previous autocommit setting on the way out, so the
        block composes with a connection already in either mode. Nesting raises
        ``ProgrammingError``: the store has no savepoints to build a nested transaction
        on, and a silently flattened inner block would make an inner rollback a lie.
        """
        self._check_open()
        if self._in_transaction:
            raise ProgrammingError(
                "a transaction is already open; nested transactions are not supported"
            )
        previous = self._autocommit
        self._autocommit = False
        self._begin()
        try:
            yield self
        except BaseException:
            # Restore the mode before ending, so a connection that was in autocommit mode
            # leaves the block in autocommit mode rather than with a fresh transaction
            # reopened under it. A connection that was already transactional does get a
            # fresh one, which is what it wants.
            self._autocommit = previous
            # Best effort: the statement that raised may itself have been a failed COMMIT,
            # which already ended the transaction. Losing a rollback error here would hide
            # the original exception, which is the one worth seeing.
            try:
                if self._in_transaction:
                    self._end_transaction("ROLLBACK")
            except Exception:
                pass
            raise
        self._autocommit = previous
        if self._in_transaction:
            self._end_transaction("COMMIT")

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

    def ads(self, operation: str, parameters: Any = None) -> "AdStream":
        """Run a SELECT and iterate its rows as ``classad2.ClassAd`` objects.

        The counterpart to :meth:`execute` for callers who want ads rather than rows. A
        column select evaluates, so ``SELECT Requirements`` answers True/False; iterating
        ads keeps the expression, reachable as ``ad.lookup("Requirements")``::

            for ad in conn.ads("SELECT * FROM machines WHERE Cpus > ?", (4,)):
                print(ad["Name"], ad.lookup("Requirements"))

        Rows stream: the next ad is fetched when it is asked for, so abandoning the
        iterator stops the query rather than draining it, and a large table costs one ad
        rather than the whole set.

        Parameters bind exactly as in :meth:`execute` -- same ``?`` placeholders, same
        quoting -- because the same code builds the statement text.

        Requires HTCondor's ``classad2`` bindings; without them this raises
        ``InterfaceError`` rather than falling back to text.

        ORDER BY, DISTINCT and aggregates cannot stream (none can emit a correct first row
        before seeing the last), so those are computed whole and then iterated. An
        aggregate has no ads of its own: one is synthesized per group row, with attribute
        names derived from the column headers -- ``COUNT(*)`` becomes ``Count``,
        ``SUM(RequestMemory)`` becomes ``SumRequestMemory``, and an ``AS`` alias is used
        as-is when it is a legal attribute name.
        """
        from .adstream import ads as _ads

        return _ads(self, operation, parameters)

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

    def _control(self, statement: str) -> None:
        """Run a transaction-control statement on a throwaway cursor."""
        cursor = Cursor(self)
        try:
            cursor.execute(statement)
        finally:
            cursor.close()

    def _begin(self) -> None:
        self._control("BEGIN")

    def _end_transaction(self, statement: str) -> None:
        """Run COMMIT or ROLLBACK, then reopen a transaction if still in that mode.

        The flag is cleared *before* the statement runs because the executor ends the
        transaction whether or not it succeeds -- a COMMIT that hits a write-write
        conflict is over, not retryable. Leaving the flag set on failure would strand the
        connection believing in a transaction the daemon has already discarded.
        """
        if not self._in_transaction:
            return
        self._in_transaction = False
        try:
            self._control(statement)
        finally:
            # Reopen even if the statement failed: the caller is still in non-autocommit
            # mode and its next write belongs in a transaction, not silently committed.
            if not self._autocommit and self._handle != 0:
                try:
                    self._begin()
                except Exception:
                    pass

    def _note_transaction_state(self, in_transaction: bool) -> None:
        """Record the transaction state a statement reported (see Cursor._load)."""
        self._in_transaction = in_transaction
