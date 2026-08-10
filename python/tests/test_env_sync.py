"""Unit tests for pushing the HTCondor environment into the library.

Go answers ``os.Getenv`` from a copy of the environment made when the shared library loads,
so a variable this process sets afterwards is invisible to the configuration parser unless
the driver pushes it across. These tests pin that behaviour without needing the library:
they drive :meth:`_Library.sync_environment` against a stand-in.
"""

from __future__ import annotations

import os

import pytest

from htcondordb import _library


class FakeFFI:
    NULL = object()


class FakeLib:
    """Records hcdb_setenv calls the way the real library would receive them."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def hcdb_setenv(self, name, value) -> int:
        decoded = None if value is FakeFFI.NULL else value.decode("utf-8")
        self.calls.append((name.decode("utf-8"), decoded))
        return 0


@pytest.fixture
def library() -> _library._Library:
    return _library._Library(FakeFFI(), FakeLib(), "fake")


@pytest.fixture(autouse=True)
def clean_condor_env(monkeypatch):
    """Start from an environment with no HTCondor variables in it."""
    for name in list(os.environ):
        if name in _library._CONDOR_ENV_NAMES or name.startswith(
            _library._CONDOR_ENV_PREFIX
        ):
            monkeypatch.delenv(name, raising=False)


def pushed(library) -> dict[str, str | None]:
    return dict(library.lib.calls)


def test_pushes_condor_config(library, monkeypatch):
    monkeypatch.setenv("CONDOR_CONFIG", "/etc/condor/condor_config")
    library.sync_environment()
    assert pushed(library) == {"CONDOR_CONFIG": "/etc/condor/condor_config"}


def test_pushes_knob_overrides(library, monkeypatch):
    # _CONDOR_<KNOB> is how HTCondor overrides any knob from the environment, so the
    # library has to see those too -- not just the config file path.
    monkeypatch.setenv("_CONDOR_HTCONDORDB_HOST", "db.example.edu:9618")
    monkeypatch.setenv("_CONDOR_TOOL_DEBUG", "D_ALWAYS")
    library.sync_environment()
    assert pushed(library) == {
        "_CONDOR_HTCONDORDB_HOST": "db.example.edu:9618",
        "_CONDOR_TOOL_DEBUG": "D_ALWAYS",
    }


def test_leaves_unrelated_variables_alone(library, monkeypatch):
    # The process environment is not ours to copy wholesale into another runtime.
    monkeypatch.setenv("PATH", "/nowhere")
    monkeypatch.setenv("CONDORISH", "no")
    library.sync_environment()
    assert pushed(library) == {}


def test_a_changed_value_is_pushed_again(library, monkeypatch):
    monkeypatch.setenv("CONDOR_CONFIG", "/first")
    library.sync_environment()
    monkeypatch.setenv("CONDOR_CONFIG", "/second")
    library.sync_environment()
    assert library.lib.calls == [
        ("CONDOR_CONFIG", "/first"),
        ("CONDOR_CONFIG", "/second"),
    ]


def test_a_removed_variable_is_unset(library, monkeypatch):
    # Deleting a variable has to take effect as well; leaving the old value behind would
    # mean a test (or a program) that clears CONDOR_CONFIG still connects with it.
    monkeypatch.setenv("CONDOR_CONFIG", "/first")
    library.sync_environment()
    monkeypatch.delenv("CONDOR_CONFIG")
    library.sync_environment()
    assert library.lib.calls[-1] == ("CONDOR_CONFIG", None)


def test_sync_is_idempotent_bookkeeping(library, monkeypatch):
    monkeypatch.setenv("CONDOR_CONFIG", "/first")
    library.sync_environment()
    library.sync_environment()
    # Two pushes of the same value, and crucially no spurious unset of a name that is
    # still present.
    assert library.lib.calls == [
        ("CONDOR_CONFIG", "/first"),
        ("CONDOR_CONFIG", "/first"),
    ]
