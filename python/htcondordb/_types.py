"""PEP 249 type objects and constructors.

The spec requires these names so that generic tooling (ORMs, ``pandas.read_sql``, test
harnesses) can compare a ``description`` entry's type code against a known object. ClassAd
is dynamically typed, so the codes describe the values that arrived rather than a declared
schema -- see :func:`htcondordb.cursor._column_type`.
"""

from __future__ import annotations

import datetime
import time


class _TypeObject:
    """A type code that compares equal to every type it stands for.

    PEP 249 specifies this behaviour so a driver can group several database types under one
    comparable object.
    """

    def __init__(self, name: str, *values: str) -> None:
        self._name = name
        self._values = frozenset(values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _TypeObject):
            return self is other
        if isinstance(other, str):
            return other.lower() in self._values
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self) -> int:
        return hash(self._name)

    def __repr__(self) -> str:
        return f"<htcondordb.{self._name}>"


#: Text columns.
STRING = _TypeObject("STRING", "string", "str")
#: Binary columns. ClassAd has no binary type, so no column is ever described as one; the
#: object exists because PEP 249 requires the name.
BINARY = _TypeObject("BINARY", "bytes", "binary")
#: Numeric columns -- ClassAd integers, reals, and booleans.
NUMBER = _TypeObject("NUMBER", "int", "integer", "real", "float", "bool", "boolean")
#: Date/time columns. HTCondor stores times as epoch integers, which arrive as NUMBER; a
#: caller converts with :func:`TimestampFromTicks`.
DATETIME = _TypeObject("DATETIME", "datetime", "date", "time", "timestamp")
#: Row-identifier columns -- the store's ``Key`` attribute.
ROWID = _TypeObject("ROWID", "key", "rowid")


# --- PEP 249 constructors ---


def Date(year: int, month: int, day: int) -> datetime.date:
    """A date value."""
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:
    """A time value."""
    return datetime.time(hour, minute, second)


def Timestamp(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> datetime.datetime:
    """A timestamp value."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> datetime.date:
    """A date from Unix epoch seconds."""
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks: float) -> datetime.time:
    """A time from Unix epoch seconds."""
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """A timestamp from Unix epoch seconds.

    The natural way to read an HTCondor time attribute, which is stored as an epoch
    integer::

        cur.execute("SELECT QDate FROM jobs LIMIT 1")
        submitted = TimestampFromTicks(cur.fetchone()[0])
    """
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(string: bytes | str) -> bytes:
    """A binary value.

    Present because PEP 249 requires it. ClassAd has no binary type, so binding the result
    raises ``DataError`` -- encode binary data as text (base64, hex) first.
    """
    if isinstance(string, str):
        return string.encode("utf-8")
    return bytes(string)


__all__ = [
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
]
