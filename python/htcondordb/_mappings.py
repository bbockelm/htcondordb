"""Streaming key/value rows -- :meth:`Connection.mappings`.

A tabular result needs its column list before the first row, and for ``SELECT *`` that list is
the union of every matched ad's attributes: not knowable until the last ad has been seen. So the
DB-API path runs ``SELECT *`` whole and serves it from memory.

Keyed rows need no such list. Each ad carries its own attribute names, nothing has to be
reconciled across rows, and the result streams however wide or ragged it is -- which is what
makes this the shape to reach for when walking a large table with ``SELECT *``.

The trade, stated plainly: values are *evaluated*. An attribute holding an expression arrives as
what it evaluates to. :meth:`Connection.ads` is the path that keeps expressions, at the cost of
needing HTCondor's ``classad2`` bindings.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Sequence

from ._params import bind

if TYPE_CHECKING:  # pragma: no cover
    from .connection import Connection


class MappingStream:
    """An iterator over a statement's rows as ``dict`` objects.

    Yielded in batches: the next batch is fetched when the current one runs out, so a large
    result costs memory proportional to a batch. Closing is automatic when the iterator is
    exhausted; :meth:`close` stops it early, and the object is a context manager.

    Built on :class:`~htcondordb.cursor.Cursor`, so it inherits the batching, the settling that
    keeps two statements on one connection from deadlocking, and the cleanup.
    """

    def __init__(
        self,
        connection: "Connection",
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> None:
        self._cursor = connection.cursor()
        try:
            self._cursor._run(bind(operation, parameters), objects=True)
        except Exception:
            self._cursor.close()
            raise
        # Snapshot rather than delegate: closing the cursor resets its flag, so a caller reading
        # this after iterating would otherwise always be told "not streamed".
        self._streamed = self._cursor.streamed

    @property
    def itersize(self) -> int:
        """Rows per batch (default 1000)."""
        return self._cursor.itersize

    @itersize.setter
    def itersize(self, value: int) -> None:
        self._cursor.itersize = value

    @property
    def streamed(self) -> bool:
        """Whether rows arrive as the server produces them.

        False for the shapes that have to be computed whole -- an aggregate, a ``GROUP BY``, a
        window function -- whose rows are synthesized from groups rather than read from ads, and
        so cannot be produced one at a time in any row shape.
        """
        return self._streamed

    def __iter__(self) -> Iterator[dict]:
        return self

    def __next__(self) -> dict:
        row = self._cursor.fetchone()
        if row is None:
            self.close()
            raise StopIteration
        return row

    def close(self) -> None:
        """Stop the stream and release it. Closing twice is not an error."""
        self._cursor.close()

    def __enter__(self) -> "MappingStream":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<htcondordb.MappingStream {self._cursor!r}>"


def mappings(
    connection: "Connection", operation: str, parameters: Sequence[Any] | None = None
) -> MappingStream:
    """Run a statement and return a :class:`MappingStream` over its rows as dicts."""
    connection._check_open()
    return MappingStream(connection, operation, parameters)
