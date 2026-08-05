"""Parameter binding for the ``qmark`` paramstyle.

htcondordb's SQL has no server-side bind parameters: the daemon parses the statement text
it is given. Substitution therefore happens here, which makes this module the driver's
whole defense against SQL injection -- so it does two things carefully.

**Finding placeholders.** The scanner skips string literals exactly the way the daemon's
lexer does (``repl/sql.go``): ``'...'`` with ``''`` as the escaped quote, and ``"..."``
with no escape at all (a ``"`` always closes it). A ``?`` inside either is data, not a
placeholder. Getting this wrong in the *other* direction matters too -- the daemon's lexer
passes any unrecognized byte through verbatim, so a stray ``?`` reaches the ClassAd engine
as its ternary operator rather than erroring.

**Rendering values.** Strings become single-quoted SQL literals with ``'`` doubled. Nothing
else needs escaping: the daemon's lexer treats every other byte literally, unquotes the
literal, and re-quotes it for ClassAd itself (``classAdString``), so backslashes, newlines,
and quotes all survive a round trip without this module knowing ClassAd's escaping rules.
"""

from __future__ import annotations

import datetime
import decimal
from typing import Any, Sequence

from ._errors import DataError, ProgrammingError

#: ClassAd integers are 64-bit; a Python int outside this range cannot be represented.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def render_literal(value: Any) -> str:
    """Render a Python value as SQL literal text.

    Type mapping:

    ============================  ==========================================
    Python                        SQL / ClassAd
    ============================  ==========================================
    ``None``                      ``UNDEFINED`` (ClassAd's spelling of NULL)
    ``bool``                      ``true`` / ``false``
    ``int``                       integer literal (must fit in int64)
    ``float``, ``Decimal``        real literal
    ``str``                       single-quoted string
    ``datetime``, ``date``        Unix epoch seconds, as an integer
    ``list``, ``tuple``           ClassAd list literal, ``{...}``
    ============================  ==========================================

    ``datetime`` maps to epoch seconds because that is how HTCondor stores times
    (``QDate``, ``CompletionDate``, ``EnteredCurrentStatus``). A naive ``datetime`` is
    interpreted in the local timezone, which is what ``datetime.timestamp()`` does.

    Raises:
        DataError: for a value ClassAd cannot represent -- ``bytes`` (there is no binary
            type), a non-finite float, or an int wider than 64 bits.
    """
    # bool before int: bool is a subclass of int, and `true` is not `1` to the engine.
    if value is None:
        return "UNDEFINED"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if not _INT64_MIN <= value <= _INT64_MAX:
            raise DataError(
                f"integer {value} does not fit in ClassAd's 64-bit integer type"
            )
        return str(value)
    if isinstance(value, float):
        return _render_float(value)
    if isinstance(value, decimal.Decimal):
        # ClassAd's only numeric types are int64 and double, so a Decimal has to widen
        # to a float. Say so rather than silently losing precision at the far end.
        try:
            return _render_float(float(value))
        except (ValueError, OverflowError) as exc:
            raise DataError(f"cannot represent Decimal {value!r} as a ClassAd real") from exc
    if isinstance(value, str):
        return _render_string(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise DataError(
            "ClassAd has no binary type; encode the value as text before binding it"
        )
    # datetime before date: datetime is a subclass of date.
    if isinstance(value, datetime.datetime):
        return str(int(value.timestamp()))
    if isinstance(value, datetime.date):
        midnight = datetime.datetime(value.year, value.month, value.day)
        return str(int(midnight.timestamp()))
    if isinstance(value, (list, tuple)):
        return "{" + ", ".join(render_literal(v) for v in value) + "}"

    raise DataError(
        f"cannot bind a value of type {type(value).__name__}; "
        "supported types are None, bool, int, float, Decimal, str, datetime, date, "
        "list and tuple"
    )


def _render_float(value: float) -> str:
    """Render a float, rejecting the values ClassAd cannot spell."""
    if value != value or value in (float("inf"), float("-inf")):
        raise DataError(f"ClassAd has no literal for the real value {value}")
    return repr(value)


def _render_string(value: str) -> str:
    """Render a str as a single-quoted SQL literal.

    Only ``'`` is escaped, by doubling. Every other byte is literal to the daemon's lexer,
    which unquotes this literal and re-escapes it for ClassAd on its own side.
    """
    return "'" + value.replace("'", "''") + "'"


def placeholder_count(sql: str) -> int:
    """Count the ``?`` placeholders in *sql*, ignoring any inside string literals."""
    return len(_placeholder_offsets(sql))


def _placeholder_offsets(sql: str) -> list[int]:
    """Byte offsets of the ``?`` placeholders, skipping string literals.

    The literal scanning mirrors the daemon's lexer, including its asymmetry: a
    single-quoted string escapes ``'`` by doubling it, while a double-quoted string has no
    escape mechanism at all.
    """
    offsets: list[int] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "'":
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2  # an escaped quote, still inside the string
                        continue
                    break
                i += 1
            if i >= n:
                raise ProgrammingError("unterminated string literal in statement")
            i += 1  # past the closing quote
        elif c == '"':
            i += 1
            while i < n and sql[i] != '"':
                i += 1
            if i >= n:
                raise ProgrammingError("unterminated string literal in statement")
            i += 1
        elif c == "?":
            offsets.append(i)
            i += 1
        else:
            i += 1
    return offsets


def bind(sql: str, parameters: Sequence[Any] | None) -> str:
    """Substitute *parameters* for the ``?`` placeholders in *sql*.

    Args:
        sql: A statement in the ``qmark`` paramstyle.
        parameters: One value per placeholder, or ``None``/empty for a statement with
            none.

    Returns:
        The statement with every placeholder replaced by a literal.

    Raises:
        ProgrammingError: if the parameter count does not match the placeholder count, or
            a ``dict`` is passed (this driver's paramstyle is positional).
        DataError: if a value has no ClassAd representation.
    """
    if parameters is None:
        parameters = ()
    if isinstance(parameters, dict):
        raise ProgrammingError(
            "this driver's paramstyle is 'qmark' (positional); pass a sequence, not a dict"
        )
    if isinstance(parameters, (str, bytes)):
        raise ProgrammingError(
            "parameters must be a sequence of values, not a string; "
            "wrap a single parameter in a tuple, e.g. (value,)"
        )

    offsets = _placeholder_offsets(sql)
    values = list(parameters)
    if len(offsets) != len(values):
        raise ProgrammingError(
            f"statement has {len(offsets)} placeholder(s) but {len(values)} "
            "parameter(s) were supplied"
        )
    if not offsets:
        return sql

    out: list[str] = []
    prev = 0
    for offset, value in zip(offsets, values):
        out.append(sql[prev:offset])
        out.append(render_literal(value))
        prev = offset + 1
    out.append(sql[prev:])
    return "".join(out)
