"""One curl_cffi session per main-client token.

The selfclient's HTTPClient owns the session; REST, science and the gateway
use it directly, and get_tls_session hands the same one to the wire probe and
any other caller for that token. Other tokens keep their own cached session.
"""
import logging

import pytest

from copycord.common import selfbot_headers as sh
from copycord.common import wire_probe as wp
from copycord.selfclient import Client
from copycord.selfclient import client as client_mod

TOKEN = "main-token-not-real"
OTHER = "sender-token-not-real"


class FakeSession:
    """curl_cffi AsyncSession stand-in; records every request."""

    def __init__(self, *args, **kwargs):
        self.calls = []
        self.closed = False

    async def get(self, url, **kwargs):
        self.calls.append(url)
        return _Resp()

    async def close(self):
        self.closed = True


class _Resp:
    status_code = 503


class SessionHTTP:
    """HTTPClient stand-in that owns one session, as the real one does."""

    def __init__(self, fail_login=False):
        self.token = None
        self.proxy = None
        self.session = FakeSession()
        self.closed = False
        self.fail_login = fail_login

    async def login(self):
        if self.fail_login:
            raise RuntimeError("HTTP 401")
        return {"id": "100000000000000001", "username": "me", "discriminator": "0", "avatar": None}

    async def close(self):
        self.closed = True

    def set_proxy(self, proxy):
        self.proxy = proxy


class _Pool:
    enabled = False

    def current(self, token):
        return None


@pytest.fixture(autouse=True)
def _registry(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sh, "_TLS_SESSIONS", {})
    monkeypatch.setattr(sh, "_TLS_PRIMED", set())
    monkeypatch.setattr(sh, "_TLS_SESSION_LOCKS", {})
    monkeypatch.setattr(sh, "_TLS_LAST_USED", {})
    monkeypatch.setattr(sh, "_TLS_SHARED", {})
    monkeypatch.setattr(sh, "_CurlAsyncSession", FakeSession)

    async def no_prime(sess, token, proxy=None):
        pass

    monkeypatch.setattr(sh, "_prime_tls_session", no_prime)


async def _logged_in(**kwargs):
    http = SessionHTTP(**kwargs)
    client = Client(http=http, refresh_build=False)
    await client.login(TOKEN)
    return client, http


@pytest.mark.asyncio
async def test_get_tls_session_returns_the_rest_session(caplog):
    caplog.set_level(logging.DEBUG, logger=sh.logger.name)
    client, http = await _logged_in()
    assert await sh.get_tls_session(TOKEN, use_proxy=True) is http.session
    assert await sh.get_tls_session(TOKEN) is http.session
    assert sh._TLS_SESSIONS == {}
    assert not [r for r in caplog.records if "TLS session created" in r.getMessage()]
    await client.close()


@pytest.mark.asyncio
async def test_gateway_wire_probe_and_rest_share_one_session(monkeypatch):
    client, http = await _logged_in()
    dialed = []

    async def fake_open(url, headers, proxy, session=None):
        dialed.append(session)
        return object()

    monkeypatch.setattr(client_mod, "open_websocket", fake_open)
    await client._tracked_connector("wss://gateway.example", {}, None)
    assert dialed == [http.session]

    monkeypatch.setattr(wp, "_get_proxy_pool", lambda: _Pool())
    out = await wp.probe_wire(TOKEN)
    assert out["ok"] is False and out["error"] == "HTTP 503"
    assert http.session.calls == [wp.PROBE_URL]
    assert sh._TLS_SESSIONS == {}
    await client.close()


@pytest.mark.asyncio
async def test_other_tokens_keep_their_own_session():
    client, http = await _logged_in()
    other = await sh.get_tls_session(OTHER)
    assert other is not http.session
    assert sh._TLS_SESSIONS == {OTHER: other}
    await client.close()


@pytest.mark.asyncio
async def test_close_stops_sharing_and_leaves_the_session_to_its_owner():
    client, http = await _logged_in()
    await sh.close_all_tls_sessions()
    assert not http.session.closed
    await client.close()
    assert TOKEN not in sh._TLS_SHARED
    fresh = await sh.get_tls_session(TOKEN)
    assert fresh is not http.session
    assert sh._TLS_SESSIONS == {TOKEN: fresh}


@pytest.mark.asyncio
async def test_failed_login_shares_nothing():
    http = SessionHTTP(fail_login=True)
    client = Client(http=http, refresh_build=False)
    with pytest.raises(RuntimeError):
        await client.login(TOKEN)
    assert TOKEN not in sh._TLS_SHARED


@pytest.mark.asyncio
async def test_owner_without_a_session_falls_back_to_the_cache():
    sh.share_tls_session(TOKEN, lambda: None)
    sess = await sh.get_tls_session(TOKEN)
    assert isinstance(sess, FakeSession)
    assert sh._TLS_SESSIONS == {TOKEN: sess}


def test_unshare_keeps_a_newer_owner():
    def old():
        return None

    def new():
        return None

    sh.share_tls_session(TOKEN, old)
    sh.share_tls_session(TOKEN, new)
    sh.unshare_tls_session(TOKEN, old)
    assert sh._TLS_SHARED[TOKEN] is new
    sh.unshare_tls_session(TOKEN, new)
    assert TOKEN not in sh._TLS_SHARED
