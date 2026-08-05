"""PEP 249 exception hierarchy.

The shape is mandated by the spec::

    Exception
     +-- Warning
     +-- Error
          +-- InterfaceError
          +-- DatabaseError
               +-- DataError
               +-- OperationalError
               +-- IntegrityError
               +-- InternalError
               +-- ProgrammingError
               +-- NotSupportedError

Which one a failure becomes is decided by the C library's return code, not by matching on
message text -- see ``_library.RESULT_*`` and ``Cursor.execute``. The daemon owns the
wording of its errors and is free to change it; the codes are a contract.
"""


class Warning(Exception):  # noqa: A001 - the name is PEP 249's
    """Raised for important warnings. This driver does not currently raise it."""


class Error(Exception):
    """Base class for every error this driver raises."""


class InterfaceError(Error):
    """A problem with the driver itself rather than the database.

    Using a closed connection or cursor, or a library that does not export the expected
    symbols, lands here.
    """


class DatabaseError(Error):
    """A problem reported by the database."""


class DataError(DatabaseError):
    """A problem with the processed data (out of range, bad cast, ...)."""


class OperationalError(DatabaseError):
    """A problem outside the programmer's control.

    Connecting failed, the connection dropped mid-statement, or the daemon refused the
    statement for a reason that is not a syntax error.
    """


class IntegrityError(DatabaseError):
    """A relational integrity constraint was violated."""


class InternalError(DatabaseError):
    """The database hit an internal error (an invalid cursor, a dead transaction, ...)."""


class ProgrammingError(DatabaseError):
    """The statement was wrong.

    A SQL parse error, a bad table name, or a wrong parameter count. A write refused
    because the daemon authorized the connection READ-only is also reported here: the
    remedy is in the caller's hands (authenticate, or get WRITE authorization), and the
    daemon's own hint explaining what to check is carried in the message.
    """


class NotSupportedError(DatabaseError):
    """A method or API the database does not support was used.

    ``Connection.rollback`` raises this: htcondordb commits each statement as it runs and
    exposes no transaction to roll back.
    """


__all__ = [
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]
