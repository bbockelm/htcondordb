"""The PEP 249 ``Cursor`` object."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterator, Sequence

from . import _library
from ._errors import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from ._params import bind
from ._types import BINARY, DATETIME, NUMBER, STRING, ROWID  # noqa: F401 - re-exported

if TYPE_CHECKING:  # pragma: no cover
    from .connection import Connection


class Cursor:
    """Runs statements and holds their results.

    A statement's whole result is materialized when :meth:`execute` returns -- the daemon's
    SQL executor builds it in full before replying, so there is nothing to gain from
    fetching lazily. Bound the size with ``LIMIT`` when a query could match a lot of rows.
    """

    def __init__(self, connection: "Connection") -> None:
        self._connection = connection
        self._rows: list[tuple] = []
        self._ads: list[str] = []
        self._position = 0
        self._description: tuple | None = None
        self._rowcount = -1
        self._closed = False
        self._include_ads = False
        #: PEP 249: the number of rows :meth:`fetchmany` returns by default.
        self.arraysize = 1

    # --- PEP 249 attributes ---

    @property
    def description(self) -> tuple | None:
        """Column metadata for the last statement, or ``None`` if it returned no rows.

        Each entry is PEP 249's seven-tuple ``(name, type_code, display_size,
        internal_size, precision, scale, null_ok)``. Only ``name`` and ``type_code`` are
        meaningful: ClassAd attributes are dynamically typed and columns have no declared
        width or nullability. ``type_code`` is inferred from the first non-null value in
        the column, and is ``None`` for a column that is null all the way down.
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """Rows produced by the last SELECT, or written by the last INSERT/UPDATE/DELETE.

        ``-1`` before any statement runs, and for DDL.
        """
        return self._rowcount

    @property
    def connection(self) -> "Connection":
        """The connection this cursor belongs to (a PEP 249 optional extension)."""
        return self._connection

    # --- PEP 249 methods ---

    def close(self) -> None:
        """Release the cursor. Closing twice is not an error."""
        if self._closed:
            return
        self._closed = True
        self._reset()
        self._connection._forget(self)

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> "Cursor":
        """Run one SQL statement.

        Args:
            operation: A single statement. ``?`` marks a parameter (the module's
                ``paramstyle`` is ``qmark``).
            parameters: One value per placeholder. See :func:`htcondordb._params.bind`
                for the type mapping.

        Returns:
            The cursor, so ``for row in cur.execute(...)`` reads naturally. PEP 249 leaves
            the return value undefined; returning ``self`` is the common extension.

        Raises:
            ProgrammingError: the statement did not parse, or the daemon refused it (a
                write on a READ-only connection carries the daemon's hint in the message).
            OperationalError: the statement parsed but failed to run.
            InterfaceError: the cursor or its connection is closed.
        """
        self._check_open()
        self._reset()

        statement = bind(operation, parameters)
        document = self._call_sql(statement)
        self._load(document)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> "Cursor":
        """Run *operation* once per parameter sequence.

        Atomic when the connection is in a transaction: every execution stages into it, so
        a failure partway through leaves nothing applied once the caller rolls back. In
        autocommit mode each execution commits on its own and a failure partway through
        leaves the earlier ones applied -- wrap the call in
        :meth:`~htcondordb.connection.Connection.transaction` if that matters.

        The rows of the final execution are what remains fetchable; ``rowcount`` is the
        sum of the affected counts, which is what a caller batching writes wants to see.

        PEP 249 notes that ``executemany`` on a row-returning statement is undefined
        behaviour; this driver allows it but only the last result set survives.
        """
        self._check_open()
        total = 0
        ran = False
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            ran = True
            if self._rowcount > 0:
                total += self._rowcount
        if ran:
            self._rowcount = total
        return self

    def fetchone(self) -> tuple | None:
        """Return the next row, or ``None`` when the result is exhausted."""
        self._check_open()
        self._check_results()
        if self._position >= len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Return up to *size* rows (default :attr:`arraysize`)."""
        self._check_open()
        self._check_results()
        if size is None:
            size = self.arraysize
        if size <= 0:
            return []
        rows = self._rows[self._position : self._position + size]
        self._position += len(rows)
        return rows

    def fetchall(self) -> list[tuple]:
        """Return every remaining row."""
        self._check_open()
        self._check_results()
        rows = self._rows[self._position :]
        self._position = len(self._rows)
        return rows

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """No-op. ClassAd values are dynamically typed and need no predeclared sizes."""

    def setoutputsizes(self, size: int, column: int | None = None) -> None:
        """No-op. There are no large-column buffers to preallocate."""

    # --- extensions ---

    def execute_with_ads(
        self, operation: str, parameters: Sequence[Any] | None = None
    ) -> "Cursor":
        """Run a statement and also keep each row's whole ClassAd (see :meth:`fetchads`).

        Costs an extra copy of every row on the wire, so it is opt-in rather than the
        default.
        """
        self._check_open()
        self._reset()
        self._include_ads = True
        try:
            statement = bind(operation, parameters)
            self._load(self._call_sql(statement))
        finally:
            self._include_ads = False
        return self

    def fetchads(self) -> list:
        """Return the result's whole ClassAds, as ``classad2.ClassAd`` objects.

        Only populated after :meth:`execute_with_ads`, and only for a non-aggregate SELECT
        -- an aggregate computes rows that were never ads.

        Requires HTCondor's ``classad2`` Python bindings; without them the ad text is
        still available from :attr:`ad_text`.

        Raises:
            InterfaceError: if ``classad2`` is not importable.
        """
        try:
            import classad2
        except ImportError as exc:
            raise InterfaceError(
                "fetchads() needs HTCondor's classad2 bindings "
                "(pip install htcondor); cursor.ad_text has the unparsed text"
            ) from exc
        return [classad2.parseOne(text) for text in self._ads]

    @property
    def ad_text(self) -> list[str]:
        """Each row's whole ClassAd as old-format text, after :meth:`execute_with_ads`."""
        return list(self._ads)

    # --- iteration (PEP 249 optional extension) ---

    def __iter__(self) -> Iterator[tuple]:
        return self

    def __next__(self) -> tuple:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else f"{len(self._rows)} row(s)"
        return f"<htcondordb.Cursor [{state}]>"

    # --- internals ---

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("the cursor is closed")
        self._connection._check_open()

    def _check_results(self) -> None:
        """PEP 249 requires a fetch before any execute to raise."""
        if self._description is None and self._rowcount == -1:
            raise ProgrammingError("no statement has been executed on this cursor")

    def _reset(self) -> None:
        self._rows = []
        self._ads = []
        self._position = 0
        self._description = None
        self._rowcount = -1

    def _call_sql(self, statement: str) -> dict:
        """Run *statement* through the C library and decode its JSON result document."""
        lib = self._connection._lib
        ffi, c = lib.ffi, lib.lib

        options = _library.SQL_ADS if self._include_ads else 0
        out = ffi.new("char **")
        code = c.hcdb_sql(
            self._connection._handle, statement.encode("utf-8"), options, out
        )
        payload = lib.string(out[0])

        if code == _library.RESULT_OK:
            try:
                return json.loads(payload)
            except ValueError as exc:  # a malformed document is a driver/library mismatch
                raise InterfaceError(
                    f"could not decode the result document from {lib.path}: {exc}"
                ) from exc

        # The C library classifies its own failures so the mapping here does not depend on
        # matching error text -- see the hcdb* codes in capi/sql.go.
        message = payload or "the statement failed with no message"
        if code == _library.RESULT_BAD_SQL:
            raise ProgrammingError(message)
        if code == _library.RESULT_DENIED:
            raise ProgrammingError(message)
        if code == _library.RESULT_ERR:
            raise OperationalError(message)
        raise DatabaseError(f"unexpected result code {code}: {message}")

    def _load(self, document: dict) -> None:
        """Populate the cursor from a decoded result document."""
        # Every result reports whether a transaction is open afterwards, so the connection
        # tracks it from the server's answer rather than inferring it from what was sent.
        self._connection._note_transaction_state(bool(document.get("in_transaction", False)))

        if not document.get("select", False):
            # DML/DDL: no rows, and rowcount is what was written.
            self._rowcount = int(document.get("affected", 0))
            return

        columns = document.get("columns") or []
        rows = document.get("rows") or []
        self._rows = [tuple(row) for row in rows]
        self._ads = list(document.get("ads") or [])
        self._rowcount = len(self._rows)
        self._description = tuple(
            (name, _column_type(self._rows, index), None, None, None, None, None)
            for index, name in enumerate(columns)
        )


def _column_type(rows: Sequence[Sequence[Any]], index: int) -> Any:
    """Infer a column's PEP 249 type object from its first non-null value.

    ClassAd attributes have no declared type -- two rows of the same column can genuinely
    differ -- so this is a description of what arrived, not a schema. ``None`` for a column
    with no non-null value to judge by.
    """
    for row in rows:
        if index >= len(row):
            continue
        value = row[index]
        if value is None:
            continue
        if isinstance(value, bool):
            return NUMBER  # ClassAd booleans have no PEP 249 type object of their own
        if isinstance(value, (int, float)):
            return NUMBER
        if isinstance(value, str):
            return STRING
        return STRING
    return None
