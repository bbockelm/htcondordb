"""A PEP 249 (DB-API 2.0) driver for htcondordb.

Talks to an htcondordb daemon over CEDAR -- HTCondor's authenticated, encrypted wire
protocol -- by calling the Go client in ``capi/`` through cffi. That client carries the
whole stack: transport, authentication, the dbrpc protocol, and the SQL parser and executor
the ``htcondordb-cli`` shell uses, so this driver gets the full SQL surface without
reimplementing any of it.

Quick start::

    import htcondordb

    with htcondordb.connect("collector.example.edu:9618") as conn:
        cur = conn.execute(
            "SELECT Owner, COUNT(*) AS n FROM jobs WHERE JobStatus = ? GROUP BY Owner",
            (2,),
        )
        for owner, n in cur:
            print(owner, n)

pandas works directly, since it accepts any DB-API connection::

    import pandas as pd
    df = pd.read_sql("SELECT Owner, RequestMemory FROM jobs LIMIT 1000", conn)

Authentication is HTCondor's own and needs no arguments here: the library reads the ambient
configuration through ``CONDOR_CONFIG`` and authenticates exactly as ``htcondordb-cli``
does (pool token, SSL, FS, ...). A client that cannot authenticate still connects, but the
daemon authorizes it READ-only.

Before this driver can load, build the shared library it calls::

    make lib   # writes bin/libhtcondordb_client.{so,dylib}

and either install it on the library search path or point ``HTCONDORDB_LIBRARY`` at it.
"""

from __future__ import annotations

from ._errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from ._library import LIBRARY_ENV, library_path
from ._params import Expr
from ._types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from ._mappings import MappingStream
from .adstream import AdStream
from .adwrite import AdReject, WriteResult
from .connection import Connection
from .cursor import Cursor

def _installed_version() -> str:
    """The installed distribution's version.

    Read from package metadata rather than written here: the version comes from the
    repository's git tag at build time, and a literal in this file would be a second
    number to keep in step -- exactly the drift the dynamic version removes.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("htcondordb")
    except PackageNotFoundError:  # running from a source tree, never installed
        return "0.0.0+unknown"


__version__ = _installed_version()

#: PEP 249: the supported DB-API level.
apilevel = "2.0"

#: PEP 249: threads may share the module and connections, but not cursors.
#:
#: The C library serializes statements per connection, so sharing a connection across
#: threads is safe; cursors hold per-statement result state and are not.
threadsafety = 2

#: PEP 249: parameters are positional ``?`` markers.
#:
#: The daemon has no server-side bind parameters, so the driver substitutes literals
#: itself. Using placeholders is still strongly preferred over formatting values into the
#: statement yourself -- see :mod:`htcondordb._params` for the escaping this depends on.
paramstyle = "qmark"


def connect(
    address: str | None = None, autocommit: bool = True, **kwargs
) -> Connection:
    """Open an authenticated session with an htcondordb daemon.

    Args:
        address: The daemon's address -- ``host:port``, or an HTCondor sinful string
            (``<1.2.3.4:9618?sock=...>``), including a shared-port or CCB one. Omit it to
            locate the daemon, the way ``htcondordb-cli`` does with no ``-addr``: the
            address file named by ``HTCONDORDB_ADDRESS_FILE`` (by default
            ``$(LOG)/.htcondordb_address``), else ``HTCONDORDB_HOST``. Either knob may be
            set in the environment or in the HTCondor configuration, and the environment
            wins -- ``HTCONDORDB_HOST=db.example.edu:9618`` points a report at another
            pool's daemon without editing a config file or the code.
            :attr:`Connection.address <htcondordb.connection.Connection.address>` reports
            what that resolved to.
        autocommit: When true (the default), each statement commits as it runs. Pass
            ``False`` to open a transaction immediately and batch writes until
            :meth:`~htcondordb.connection.Connection.commit`. See
            :class:`~htcondordb.connection.Connection` for the two constraints a
            transaction carries -- reads do not join it, and it cannot span tables.

    Returns:
        A :class:`~htcondordb.connection.Connection`.

    Raises:
        OperationalError: the daemon was unreachable, could not be located, or
            authentication failed; the message carries the reason.
        InterfaceError: the shared library could not be loaded.

    There are deliberately no credential arguments: HTCondor's security configuration is
    ambient (``CONDOR_CONFIG`` and the token/credential locations it names), and the
    library reads it the same way every HTCondor command-line tool does. Point
    ``CONDOR_CONFIG`` at a different configuration to authenticate differently.
    """
    if kwargs:
        unexpected = ", ".join(sorted(kwargs))
        raise InterfaceError(
            f"connect() got unexpected keyword argument(s): {unexpected}. "
            "Credentials come from the HTCondor configuration (CONDOR_CONFIG), not from "
            "connect() arguments."
        )
    return Connection(address, autocommit=autocommit)


__all__ = [
    # PEP 249 module interface
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    # PEP 249 exceptions
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
    # PEP 249 type objects and constructors
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
    # driver objects and helpers
    "Connection",
    "Cursor",
    "Expr",
    "AdReject",
    "WriteResult",
    "AdStream",
    "MappingStream",
    "LIBRARY_ENV",
    "library_path",
    "__version__",
]
