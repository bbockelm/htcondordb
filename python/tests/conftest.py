"""Test fixtures.

The unit tests need nothing but the package. The integration tests need a live daemon, so
``daemon_address`` starts one on a private port over a throwaway config and store, and
skips the tests that ask for it when the pieces are not built.

Build both with::

    make lib          # the shared library this driver calls
    make daemon       # the htcondordb daemon the integration tests talk to
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BIN_DIR = REPO_ROOT / "bin"

# Give the driver the freshly built library unless the caller pinned one, so a plain
# `pytest python/tests` in a checkout tests what `make lib` just produced rather than
# whatever happens to be installed.
if "HTCONDORDB_LIBRARY" not in os.environ:
    for name in ("libhtcondordb_client.dylib", "libhtcondordb_client.so"):
        candidate = BIN_DIR / name
        if candidate.exists():
            os.environ["HTCONDORDB_LIBRARY"] = str(candidate)
            break

sys.path.insert(0, str(REPO_ROOT / "python"))


def _free_port() -> int:
    """Reserve a port by binding and releasing it.

    Racy in principle, but the daemon binds within milliseconds and the alternative --
    letting it pick with :0 and reading back the address file -- is what we do anyway; this
    just makes the config deterministic.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def library_available() -> bool:
    """Whether the shared library loads; most tests are pointless without it."""
    try:
        import htcondordb

        htcondordb.library_path()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def daemon_address(tmp_path_factory) -> str:
    """Start an htcondordb daemon and yield its address.

    The daemon runs over a config written for this test session alone: its own store
    directory, its own log directory, and FS authentication, which on a single host maps
    the connection to the user running the tests so writes are authorized. Without
    authentication the daemon would authorize the connection READ-only and every write
    test would fail for the wrong reason.
    """
    daemon_bin = BIN_DIR / "htcondordb"
    if not daemon_bin.exists():
        pytest.skip(f"{daemon_bin} not built; run 'make daemon'")

    root = tmp_path_factory.mktemp("htcondordb")
    log_dir = root / "log"
    spool_dir = root / "spool"
    db_dir = root / "db"
    for directory in (log_dir, spool_dir, db_dir):
        directory.mkdir()

    port = _free_port()
    address_file = root / "address"
    config_file = root / "condor_config"
    config_file.write_text(
        "\n".join(
            [
                f"LOG = {log_dir}",
                f"SPOOL = {spool_dir}",
                f"HTCONDORDB_DIR = {db_dir}",
                f"HTCONDORDB_ADDRESS_FILE = {address_file}",
                "UID_DOMAIN = localhost",
                # FS authentication maps the connection to the local user, which is what
                # gets these tests WRITE. Channel binding is off because the test client
                # and daemon share a host and a filesystem.
                "SEC_DEFAULT_AUTHENTICATION = PREFERRED",
                "SEC_DEFAULT_AUTHENTICATION_METHODS = FS",
                "SEC_FS_ENFORCE_CHANNEL_BINDING = False",
                "SEC_DEFAULT_ENCRYPTION = OPTIONAL",
                "SEC_DEFAULT_INTEGRITY = OPTIONAL",
                "ALLOW_READ = *",
                "ALLOW_WRITE = *",
                "ALLOW_DAEMON = *",
                "",
            ]
        )
    )

    env = dict(os.environ, CONDOR_CONFIG=str(config_file), _CONDOR_TOOL_DEBUG="D_ALWAYS")
    process = subprocess.Popen(
        [str(daemon_bin), "-listen", f"127.0.0.1:{port}"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    address = ""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            pytest.skip(f"htcondordb daemon exited early:\n{output}")
        if address_file.exists():
            address = address_file.read_text().strip()
            if address:
                break
        time.sleep(0.05)
    else:
        process.terminate()
        pytest.skip("htcondordb daemon did not publish an address within 30s")

    # The client reads the same configuration the daemon was started with, so its security
    # policy matches. Set it for the whole session: connect() takes no credentials by
    # design, exactly so the ambient configuration stays in charge.
    os.environ["CONDOR_CONFIG"] = str(config_file)

    yield address

    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:  # pragma: no cover - only on a wedged daemon
        process.kill()


@pytest.fixture
def connection(daemon_address):
    """An open connection to the test daemon, closed when the test ends."""
    import htcondordb

    conn = htcondordb.connect(daemon_address)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def table(connection):
    """A freshly created table, named per test and dropped afterwards."""
    name = "pytest_" + os.urandom(4).hex()
    connection.execute(f"CREATE TABLE {name}").close()
    try:
        yield name
    finally:
        try:
            connection.execute(f"DROP TABLE {name}").close()
        except Exception:  # pragma: no cover - cleanup is best effort
            pass
