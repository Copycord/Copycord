"""
Shared fixtures for Copycord tests.
"""
import os
import tempfile
import pytest

# Modules such as copycord.admin.app create the data dir at import time; point it at a
# session temp dir before any test imports them (never /data or C:\data).
_SESSION_DATA_DIR = tempfile.mkdtemp(prefix="copycord-test-data-")
os.environ["DATA_DIR"] = _SESSION_DATA_DIR


@pytest.fixture()
def tmp_db_path(tmp_path):
    """Return a temporary database file path."""
    return str(tmp_path / "test.db")


@pytest.fixture()
def db(tmp_db_path):
    """Create a fresh DBManager with schema initialized."""
    from copycord.common.db import DBManager
    return DBManager(tmp_db_path, init_schema=True)


@pytest.fixture(autouse=True)
def _update_tests_never_touch_the_network(request, monkeypatch):
    """A real urllib call from copycord.update fails the update tests loudly
    instead of reaching GitHub; tests fake _fetch_json or urlopen themselves."""
    if request.module.__name__.rsplit(".", 1)[-1] not in ("test_update", "test_update_channels"):
        return
    import urllib.request

    def blocked(*args, **kwargs):
        raise AssertionError("test made a real network call: fake it")

    monkeypatch.setattr(urllib.request, "urlopen", blocked)


def pytest_sessionfinish(session, exitstatus):
    import shutil

    shutil.rmtree(_SESSION_DATA_DIR, ignore_errors=True)


@pytest.fixture(autouse=True)
def _no_exit_ip_lookup(request, monkeypatch):
    """The exit-IP timezone lookup never reaches ipinfo.io from a test; it
    fails (so the fallback zone applies) unless a test fakes _fetch_json."""
    from copycord.common import exit_timezone

    async def _offline(proxy, timeout):
        raise OSError("network disabled in tests")

    monkeypatch.setattr(exit_timezone, "_fetch_json", _offline)
    exit_timezone.clear_cache()
    yield
    exit_timezone.clear_cache()
