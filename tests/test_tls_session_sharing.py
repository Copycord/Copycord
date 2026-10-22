"""One curl_cffi session per main-client token.

The selfclient's HTTPClient owns the session; REST, science and the gateway
use it directly, and get_tls_session hands the same one to the wire probe and
any other caller for that token. Other tokens keep their own cached session.
"""
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

    def __init__(self):
        self.token = None
        self.proxy = None
        self.session = FakeSession()
        self.closed = False

    async def login(self):
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


async def _logged_in():
    http = SessionHTTP()
    client = Client(http=http, refresh_build=False)
    await client.login(TOKEN)
    return client, http


@pytest.mark.asyncio
async def test_rest_gateway_and_wire_probe_share_one_session_per_token(monkeypatch):
    client, http = await _logged_in()
    assert await sh.get_tls_session(TOKEN, use_proxy=True) is http.session
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

    # Another token keeps its own cached session.
    other = await sh.get_tls_session(OTHER)
    assert other is not http.session
    assert sh._TLS_SESSIONS == {OTHER: other}
    await client.close()
