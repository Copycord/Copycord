"""Shared fixtures for Copycord tests."""
import os
import tempfile
import pytest

# Modules such as copycord.admin.app create the data dir at import time; point it at a
# session temp dir before any test imports them (never /data or C:\data).
_SESSION_DATA_DIR = tempfile.mkdtemp(prefix="copycord-test-data-")
os.environ["DATA_DIR"] = _SESSION_DATA_DIR


@pytest.fixture()
def tmp_db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture()
def db(tmp_db_path):
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


@pytest.fixture(autouse=True)
def _fresh_qos_tokens():
    """Each test starts with no stored READY qos_token, kept in memory only."""
    from copycord.selfclient import qos

    qos.set_store(qos.QosTokenStore())
    yield
    qos.set_store(qos.QosTokenStore())


FAKE_PROXY = "http://proxy.test:8080"


class FakeLeasedPool:
    """A proxy pool where every token holds FAKE_PROXY; no address is ever dialled."""

    enabled = True
    total = 1

    def current(self, token):
        return FAKE_PROXY

    async def lease(self, token, *, exclude=None):
        return FAKE_PROXY

    async def release(self, token):
        pass

    async def report_failure(self, token):
        return FAKE_PROXY

    def report_success(self, proxy):
        pass


@pytest.fixture(autouse=True)
def _sender_has_a_proxy(monkeypatch):
    """Proxies are mandatory for user tokens; sender tests run with a leased fake one.
    A test about the no-proxy path patches get_proxy_pool itself."""
    import sys

    for name in ("copycord.server.token_sender", "copycord.server.token_avatar"):
        mod = sys.modules.get(name)
        if mod is not None:
            monkeypatch.setattr(mod, "get_proxy_pool", lambda: FakeLeasedPool())


@pytest.fixture(autouse=True)
def _no_shared_tls_sessions():
    """A Client a test left logged in never hands its session to a later test."""
    from copycord.common import selfbot_headers

    yield
    selfbot_headers._TLS_SHARED.clear()


SPA_INDEX ='<!doctype html><html><head><title>Copycord</title></head><body><div id="root"></div></body></html>'


@pytest.fixture(autouse=True)
def spa_build(tmp_path_factory, monkeypatch):
    """A stand-in SPA build, so page and login routes work without npm."""
    from copycord.admin import spa

    dist = tmp_path_factory.mktemp("spa") / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text(SPA_INDEX, encoding="utf-8")
    (dist / "assets" / "index-abc123.js").write_text("console.log(1)", encoding="utf-8")
    monkeypatch.setattr(spa, "SPA_DIRS", (dist.parent / "missing", dist))
    return dist
