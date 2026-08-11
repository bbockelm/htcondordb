"""The PEP 249 ``Cursor`` object."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterator, Sequence

from . import _library
from ._errors import (
    InternalError,
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from ._params import bind
from ._stream import RowStream
from ._types import BINARY, DATETIME, NUMBER, STRING, ROWID  # noqa: F401 - re-exported

if TYPE_CHECKING:  # pragma: no cover
    from .connection import Connection


class Cursor:
    """Runs statements and holds their results.

    Rows arrive in batches as they are fetched, so walking a large table costs memory
    proportional to a batch rather than to the result. :attr:`itersize` sets how many rows a
    batch asks for.

    Two consequences worth knowing:

    * ``rowcount`` counts the rows fetched *so far* for a SELECT, and is ``-1`` before the
      first fetch, because the total is not known until the result is exhausted. PEP 249
      allows this and ``sqlite3`` behaves the same way. For DML it is the affected count, as
      before.
    * A SELECT whose rows cannot be produced one at a time -- ``SELECT *``, an aggregate, a
      ``GROUP BY``, a window function -- is still run whole by the daemon and then served
      from memory. Only the memory differs; the rows do not.

    While a streaming statement is unfinished it holds the connection's executor lock, so
    starting another statement on the same connection first drains this one into memory. Two
    cursors on one connection stay correct; interleaving them just gives up the memory win.
    """

    def __init__(self, connection: "Connection") -> None:
        self._connection = connection
        self._rows: list[tuple] = []
        self._ads: list[str] = []
        self._position = 0
        self._columns: list[str] | None = None
        self._rowcount = -1
        self._closed = False
        self._include_ads = False
        self._stream: RowStream | None = None
        self._streamed = False
        #: PEP 249: the number of rows :meth:`fetchmany` returns by default.
        self.arraysize = 1
        #: How many rows a batch asks the library for. Independent of :attr:`arraysize`,
        #: which PEP 249 defaults to 1 -- a batch size of one row would spend a call across
        #: the language boundary per row and undo the point of fetching in batches.
        self.itersize = 1000

    # --- PEP 249 attributes ---

    @property
    def description(self) -> tuple | None:
        """Column metadata for the last statement, or ``None`` if it returned no rows.

        Each entry is PEP 249's seven-tuple ``(name, type_code, display_size,
        internal_size, precision, scale, null_ok)``. Only ``name`` and ``type_code`` are
        meaningful: ClassAd attributes are dynamically typed and columns have no declared
        width or nullability. ``type_code`` is inferred from the first non-null value in
        the column, and is ``None`` for a column that is null all the way down.

        Reading this fetches the first batch of a result nothing has been fetched from yet:
        the column *names* come from the statement, but a type can only come from data, and
        rows now arrive lazily. The rows are buffered, not consumed -- fetching still returns
        them all.
        """
        if self._columns is None:
            return None
        if not self._rows and self._stream is not None:
            self._buffer(1)
        return tuple(
            (name, _column_type(self._rows, index), None, None, None, None, None)
            for index, name in enumerate(self._columns)
        )

    @property
    def streamed(self) -> bool:
        """Whether this statement's rows arrive as the daemon produces them.

        ``False`` for the shapes that have to be computed whole first -- ``SELECT *``, an
        aggregate, a ``GROUP BY``, a window function (see
        :meth:`~htcondordb.connection.Connection.mappings` for streaming a ``SELECT *``).
        Advisory: it says whether memory is bounded, not anything about the rows.
        """
        return self._streamed

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
        with self._connection._lock:
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

        return self._run(bind(operation, parameters), objects=False)

    def _run(self, statement: str, objects: bool) -> "Cursor":
        """Open a stream for an already-bound statement.

        The keyed-row path (``objects``) is how Connection.mappings streams a ``SELECT *``; it
        reuses everything here -- batching, settling, cleanup -- and differs only in the shape
        each row arrives in.
        """
        # Under the connection's lock for the whole operation: settling another cursor mutates
        # that cursor's row buffer, so a concurrent fetch on it would interleave.
        with self._connection._lock:
            # Another unfinished statement on this connection holds the executor lock, so settle
            # it first. Without this, two live cursors on one connection deadlock.
            self._connection._settle_streams()
            stream = RowStream(
                self._connection,
                statement,
                objects=objects,
                timeout=self._connection.timeout,
            )
            self._load_header(stream)
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
        if not self._buffer(1):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Return up to *size* rows (default :attr:`arraysize`).

        Fewer than *size* rows means the result is exhausted: the batching underneath is not
        visible here.
        """
        self._check_open()
        self._check_results()
        if size is None:
            size = self.arraysize
        if size <= 0:
            return []
        with self._connection._lock:
            while self._buffered_rows() < size:
                if not self._fill(size):
                    break
        rows = self._rows[self._position : self._position + size]
        self._position += len(rows)
        return rows

    def fetchall(self) -> list[tuple]:
        """Return every remaining row.

        This materializes what is left, which is what the caller asked for -- iterate the
        cursor instead to keep a large result out of memory.
        """
        self._check_open()
        self._check_results()
        self._drain()
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
        return [classad2.ClassAd(text) for text in self._ads]

    @property
    def ad_text(self) -> list[str]:
        """Each row's whole ClassAd as new-ClassAd (bracketed) text, after
        :meth:`execute_with_ads`.

        The same form :meth:`~htcondordb.connection.Connection.ads` streams, and what
        ``classad2.ClassAd(text)`` parses.
        """
        return list(self._ads)

    # --- iteration (PEP 249 optional extension) ---

    def __iter__(self) -> Iterator[tuple]:
        return self

    def __next__(self) -> tuple:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __del__(self) -> None:  # pragma: no cover - GC timing
        # A dropped cursor with an unfinished stream is holding a server-side cursor and this
        # connection's executor lock. Releasing it here is what makes forgetting to close one a
        # non-event rather than a stall until the next statement.
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        if self._closed:
            state = "closed"
        elif self._stream is not None and not self._stream.exhausted:
            state = f"{len(self._rows)} row(s) fetched, more pending"
        else:
            state = f"{len(self._rows)} row(s)"
        return f"<htcondordb.Cursor [{state}]>"

    # --- internals ---

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("the cursor is closed")
        self._connection._check_open()

    def _check_results(self) -> None:
        """PEP 249 requires a fetch before any execute to raise."""
        if self._columns is None and self._rowcount == -1 and self._stream is None:
            raise ProgrammingError("no statement has been executed on this cursor")

    def _reset(self) -> None:
        # Release any unfinished stream first: it holds the connection's executor lock, and
        # dropping the reference without freeing the handle would strand both.
        if self._stream is not None:
            stream, self._stream = self._stream, None
            self._connection._forget_stream(self)
            stream.close()
        self._rows = []
        self._ads = []
        self._position = 0
        self._columns = None
        self._rowcount = -1
        self._streamed = False

    def _load_header(self, stream: RowStream) -> None:
        """Take the result header, leaving the rows to be fetched."""
        header = stream.header
        self._connection._note_transaction_state(bool(header.get("in_transaction", False)))
        self._streamed = bool(header.get("streamed", False))

        if not header.get("select", False):
            # DML/DDL: nothing to fetch, and rowcount is what was written.
            self._rowcount = int(header.get("affected", 0))
            stream.close()
            return

        self._columns = list(header.get("columns", []))
        self._stream = stream
        if stream.streaming:
            # Registered only while it holds the lock, so the connection knows what to settle.
            self._connection._note_stream(self)

    def _buffered_rows(self) -> int:
        return len(self._rows) - self._position

    def _fill(self, minimum: int = 1) -> bool:
        """Pull one more batch into the buffer; return whether it added any rows.

        Asks for whole batches rather than exactly what was requested: a batch is one call
        across the language boundary and one JSON decode, so a caller taking rows one at a time
        still pays for them a thousand at a time.
        """
        if self._stream is None:
            return False
        with self._connection._lock:
            if self._stream is None:  # settled by another thread while we waited
                return False
            rows = self._stream.next_batch(max(minimum, self.itersize))
            if not rows:
                self._finish_stream()
                return False
            self._rows.extend(rows)
            self._rowcount = max(self._rowcount, 0) + len(rows)
            if self._stream.exhausted:
                self._finish_stream()
            return True

    def _buffer(self, minimum: int) -> bool:
        """Ensure at least one unread row is buffered; return whether one is available.

        Distinct from :meth:`_fill` on purpose: asking "is a row available" and asking "fetch
        more" are different questions, and answering the first with the second is how a fetch
        loop spins forever.

        Check-and-fill runs under the connection's lock because it has to be atomic. Another
        thread starting a statement settles this cursor -- filling its buffer and clearing its
        stream -- so a fill that added nothing can mean "someone else already did it", and
        treating that as "no rows" ends iteration with rows still in hand.
        """
        with self._connection._lock:
            while self._buffered_rows() == 0:
                if not self._fill(minimum):
                    return False
            return True

    def _drain(self, limit: int | None = None) -> None:
        """Fetch everything that is left into the buffer.

        *limit* caps how many rows may be added, raising rather than growing past it; see
        :attr:`Connection.settle_limit <htcondordb.connection.Connection.settle_limit>` for why
        that is worth having.
        """
        added = 0
        while self._fill(self.itersize):
            if limit is None:
                continue
            added = len(self._rows) - self._position
            if added > limit:
                # Stop the stream: it cannot be left half-drained holding the executor lock, and
                # the caller is going to have to change something either way.
                self._finish_stream()
                raise OperationalError(
                    f"draining an unfinished statement to free the connection exceeded "
                    f"settle_limit ({limit} rows). Another statement needed this connection "
                    "while this one was still streaming; finish or close it first, or run the "
                    "long query on its own connection."
                )

    def _finish_stream(self) -> None:
        """The stream is over: stop tracking it, and settle rowcount at the true total."""
        if self._stream is None:
            return
        self._stream.close()
        self._stream = None
        self._connection._forget_stream(self)
        if self._rowcount < 0:
            self._rowcount = 0

    def _settle(self) -> None:
        """Give up the executor lock by pulling the rest of the result into memory.

        Called when another statement needs the connection. The rows are all still here, so the
        caller sees no difference beyond the memory this costs.
        """
        if self._stream is not None:
            self._drain(self._connection.settle_limit)

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
        if code == _library.RESULT_PANIC:
            # A bug in the library, recovered at the boundary rather than taking the
            # interpreter down with it. InternalError is PEP 249's category for exactly this:
            # a failure that is not the caller's fault and that retrying will not fix.
            raise InternalError(message)
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
        self._columns = list(columns)


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
