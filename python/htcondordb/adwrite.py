"""Writing whole ClassAds.

The counterpart to :mod:`htcondordb.adstream`. Reading gives back ads with their
expressions intact; this writes them the same way, so a read-modify-write round trip never
flattens an expression to the value it happened to evaluate to.

It is also how a bulk load should go. The SQL path costs a statement parse and a round trip
per row; this batches whole ads into one write per chunk.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Callable, Iterable, NamedTuple

from . import _library
from ._errors import DatabaseError, InterfaceError, OperationalError, ProgrammingError

if TYPE_CHECKING:  # pragma: no cover
    from .connection import Connection

#: Ads per write. Chosen well under the transport's 1 MiB frame so a chunk of ordinary
#: machine or job ads fits without the server having to split it; the byte budget below is
#: what actually bounds it for unusually large ads.
DEFAULT_CHUNK = 500

#: Bytes of ad text per write, kept under the 1 MiB frame with room for framing overhead.
DEFAULT_CHUNK_BYTES = 512 * 1024


class AdReject(NamedTuple):
    """An ad that was not written, identified by its position in the caller's input."""

    index: int
    reason: str


class WriteResult(NamedTuple):
    """What a :meth:`~htcondordb.connection.Connection.write_ads` call did.

    ``conflicts`` holds keys whose write lost an optimistic write-write race with another
    committer; those ads were not applied and the caller re-reads and retries just those.
    It is only populated when the write committed on its own -- inside an open transaction
    the conflict surfaces at that transaction's commit instead.
    """

    written: int
    rejects: list[AdReject]
    conflicts: list[str]

    def __bool__(self) -> bool:
        """True when everything asked for was written."""
        return not self.rejects and not self.conflicts


def write_ads(
    connection: "Connection",
    table: str,
    ads: Iterable[Any],
    key: str | Callable[[Any], str] = "Name",
    chunk: int = DEFAULT_CHUNK,
) -> WriteResult:
    """Upsert an iterable of ClassAds. See ``Connection.write_ads`` for the contract."""
    connection._check_open()
    lib = connection._lib
    ffi, c_lib = lib.ffi, lib.lib

    key_of = key if callable(key) else _attribute_key(key)

    total = WriteResult(0, [], [])
    index = 0  # position in the caller's iterable, carried across chunks
    batch: list[dict] = []
    batch_bytes = 0
    batch_start = 0

    def flush() -> None:
        nonlocal batch, batch_bytes, batch_start, total
        if not batch:
            return
        request = json.dumps({"table": table, "ads": batch})
        out = ffi.new("char **")
        code = c_lib.hcdb_write_ads(connection._handle, request.encode("utf-8"), out)
        payload = lib.string(out[0])

        if code != _library.RESULT_OK:
            if code in (_library.RESULT_BAD_SQL, _library.RESULT_DENIED):
                raise ProgrammingError(payload)
            raise OperationalError(payload or "the write failed with no message")
        try:
            doc = json.loads(payload)
        except ValueError as exc:
            raise InterfaceError(f"could not decode the write result: {exc}") from exc

        # Reject indices are within the chunk; report them against the caller's input.
        rejects = [
            AdReject(batch_start + r["index"], r["reason"]) for r in doc.get("rejects", [])
        ]
        total = WriteResult(
            total.written + doc.get("written", 0),
            total.rejects + rejects,
            total.conflicts + list(doc.get("conflicts", [])),
        )
        batch, batch_bytes, batch_start = [], 0, batch_start + len(batch)

    for ad in ads:
        # An iterable, not a list: a load of a million ads never materializes, and a
        # generator reading a file streams straight through.
        if not batch:
            batch_start = index
        text = _ad_text(ad)
        batch.append({"key": key_of(ad), "ad": text})
        batch_bytes += len(text)
        index += 1
        if len(batch) >= chunk or batch_bytes >= DEFAULT_CHUNK_BYTES:
            flush()
    flush()
    return total


def _attribute_key(name: str) -> Callable[[Any], str]:
    """Build a key function reading one attribute off each ad."""

    def key_of(ad: Any) -> str:
        try:
            value = ad[name]
        except KeyError:
            raise ProgrammingError(
                f"ad has no {name!r} attribute to use as its key; pass key= with another "
                "attribute name or a callable"
            ) from None
        return str(value)

    return key_of


def _ad_text(ad: Any) -> str:
    """Render one ad as new-ClassAd (bracketed) text.

    repr rather than str for a classad2.ClassAd: str pretty-prints across several lines,
    and the text has to survive being embedded in a JSON request and re-parsed.
    """
    if isinstance(ad, str):
        return ad
    if isinstance(ad, dict):
        classad2 = _require_classad2()
        return repr(classad2.ClassAd(ad))
    return repr(ad)


def _require_classad2():
    try:
        import classad2
    except ImportError as exc:  # pragma: no cover - packaging
        raise InterfaceError(
            "writing a dict as a ClassAd needs HTCondor's classad2 bindings "
            "(pip install htcondor); pass a classad2.ClassAd or ClassAd text instead"
        ) from exc
    return classad2


__all__ = ["AdReject", "WriteResult", "write_ads", "DEFAULT_CHUNK"]
