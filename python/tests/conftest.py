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


# The running test daemon, set by daemon_address: the Popen handle and the file its output
# goes to, so any fixture can tell whether it is still there and what it last said.
_daemon: dict[str, object] = {}


def _daemon_output() -> str:
    """What the daemon has written so far, or "" if there is nothing to read."""
    path = _daemon.get("output")
    if not isinstance(path, Path):
        return ""
    try:
        return path.read_text()
    except OSError:  # pragma: no cover - only if the temp dir went away
        return ""


def _daemon_death() -> str:
    """A report of the daemon's death, or "" while it is still running."""
    process = _daemon.get("process")
    if not isinstance(process, subprocess.Popen) or process.poll() is None:
        return ""
    # The first lines only: a Go fatal error dumps every goroutine, and it is the opening
    # of that dump -- the fault and the stack that took it -- that names the bug.
    output = _daemon_output()
    head = "\n".join(output.splitlines()[:40])
    return (
        f"the htcondordb test daemon died during the session (exit status "
        f"{process.returncode}); every test after this one would fail to connect.\n"
        f"Its output began:\n{head}"
    )


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
    # A second listener for the Flight SQL endpoint, on the SAME daemon. Opening it here
    # rather than in a fixture of its own is deliberate: it is how the endpoint actually
    # deploys -- beside the CEDAR one in a single process -- and a separate daemon would
    # test a configuration nobody runs. It costs one idle listener when no test uses it.
    flight_port = _free_port()
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
                f"HTCONDORDB_FLIGHT_ADDRESS = 127.0.0.1:{flight_port}",
                # QDate is epoch seconds, so an Arrow client should read it as a
                # timestamp rather than an integer. Declared here because nothing can
                # infer it from the value.
                "HTCONDORDB_FLIGHT_TIME_ATTRS = QDate",
                "",
            ]
        )
    )

    env = dict(os.environ, CONDOR_CONFIG=str(config_file), _CONDOR_TOOL_DEBUG="D_ALWAYS")
    # The address knobs are read from the environment as well as the configuration, so a
    # developer with either exported would otherwise redirect this daemon (and the clients
    # in these tests) away from the session's own config.
    for knob in ("HTCONDORDB_ADDRESS_FILE", "HTCONDORDB_HOST"):
        env.pop(knob, None)
        os.environ.pop(knob, None)
    # To a file, not a pipe. Nothing reads the daemon's output while the session runs, so a
    # pipe holds it until the test process exits and then throws it away: a daemon that dies
    # mid-session (it writes the reason -- a Go fatal error and its stack -- to stderr as it
    # goes) leaves no trace at all, and every later test reports only "connection refused".
    # A file keeps the reason where _daemon_death can quote it.
    daemon_output = root / "daemon.out"
    output_sink = daemon_output.open("w")
    process = subprocess.Popen(
        [str(daemon_bin), "-listen", f"127.0.0.1:{port}"],
        env=env,
        stdout=output_sink,
        stderr=subprocess.STDOUT,
        text=True,
    )
    _daemon["process"] = process
    _daemon["output"] = daemon_output

    address = ""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.skip(f"htcondordb daemon exited early:\n{_daemon_output()}")
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
    _daemon["flight"] = f"grpc://127.0.0.1:{flight_port}"

    yield address

    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - only on a wedged daemon
            process.kill()
    _daemon.clear()
    output_sink.close()


@pytest.fixture(autouse=True)
def _daemon_is_alive():
    """Stop the run at the test that follows the daemon's death, saying why.

    One daemon serves the whole session, so when it dies every remaining test fails
    identically at connect() with "connection refused" -- 60 reports of the symptom and
    none of the cause, which is in the daemon's own output. Check before each test and
    quote it instead.
    """
    death = _daemon_death()
    if death:
        pytest.fail(death, pytrace=False)
    yield


@pytest.fixture(scope="session")
def flight_address(daemon_address) -> str:
    """The Flight SQL endpoint's URI on the session daemon, as an ADBC driver wants it.

    Depends on daemon_address so the daemon is up before anything dials this; the port is
    the one that fixture put in the daemon's config.
    """
    uri = _daemon.get("flight")
    if not uri:
        pytest.skip("the session daemon published no Flight SQL address")
    return uri


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
