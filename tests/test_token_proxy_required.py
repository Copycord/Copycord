"""Proxies are mandatory for user tokens: no proxy lease, no request (no Discord)."""

import types

import pytest

from copycord.common import selfbot_headers as sh
from copycord.common import token_proxy
from copycord.common.proxy_pool import ProxyPool
from copycord.common.token_proxy import NoProxyAvailable
from copycord.server import token_sender
from copycord.server.token_sender import SEND_NO_TOKENS, UserTokenSender
from tests.test_token_sender import _FakeRateLimit


class FakeSession:
    """curl_cffi AsyncSession stand-in; records construction and every request."""

    made = []

    def __init__(self, *args, **kwargs):
        self.calls = []
        FakeSession.made.append(self)

    async def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs.get("proxy")))
        return types.SimpleNamespace(status_code=200)

    async def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs.get("proxy")))
        return types.SimpleNamespace(status_code=200, json=lambda: {"id": "1"})

    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs.get("proxy")))
        return types.SimpleNamespace(status_code=200, json=lambda: {})

    async def close(self):
        pass


class _Log:
    def __init__(self):
        self.warnings = []

    def debug(self, *a, **k):
        pass

    def warning(self, msg, *args, **k):
        self.warnings.append(msg % args if args else msg)

    def exception(self, *a, **k):
        pass

    info = debug


class _DB:
    def __init__(self, ids):
        self.tokens = [{"token_id": i, "token_value": f"tok-{i}"} for i in ids]
        self.disabled = []

    def get_enabled_mapping_tokens(self, mapping_id):
        return [dict(t) for t in self.tokens]

    def increment_mapping_token_usage(self, tid):
        return None

    def set_mapping_token_enabled(self, token_id, enabled):
        self.disabled.append((token_id, enabled))


@pytest.fixture
def no_proxies(monkeypatch):
    """A real, empty proxy pool behind every token code path, and a recording curl session."""
    pool = ProxyPool()
    pool.reload([])
    monkeypatch.setattr(sh, "_get_proxy_pool", lambda: pool)
    monkeypatch.setattr(token_sender, "get_proxy_pool", lambda: pool)
    monkeypatch.setattr(sh, "_CurlAsyncSession", FakeSession)
    monkeypatch.setattr(sh, "_TLS_SESSIONS", {})
    monkeypatch.setattr(sh, "_TLS_PRIMED", set())
    monkeypatch.setattr(sh, "_TLS_SESSION_LOCKS", {})
    monkeypatch.setattr(sh, "_TLS_LAST_USED", {})
    monkeypatch.setattr(sh, "_TLS_SHARED", {})
    monkeypatch.setattr(token_proxy, "_last_warned", {})
    FakeSession.made = []
    return pool


def _sender(db, log):
    return UserTokenSender(
        db=db,
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: None,
        logger=log,
    )


def test_flag_is_always_on_whatever_is_stored():
    assert token_proxy.token_use_proxy({"USER_TOKEN_USE_PROXIES": False}) is True
    assert token_proxy.token_use_proxy({}) is True
    assert token_proxy.token_use_proxy(None) is True


@pytest.mark.asyncio
async def test_get_tls_session_without_proxies_makes_no_session(no_proxies):
    with pytest.raises(NoProxyAvailable):
        # the old opt-out value is ignored
        await sh.get_tls_session("tok-a", use_proxy=False)
    assert FakeSession.made == []
    assert sh._TLS_SESSIONS == {}
    assert "tok-a" not in sh._TLS_PRIMED


@pytest.mark.asyncio
async def test_get_tls_session_with_pool_exhausted_raises(no_proxies):
    no_proxies.reload(["http://only.proxy.test:1"])
    assert await no_proxies.lease("someone-else") == "http://only.proxy.test:1"
    with pytest.raises(NoProxyAvailable):
        await sh.get_tls_session("tok-a")
    assert FakeSession.made == []


@pytest.mark.asyncio
async def test_get_tls_session_primes_through_the_lease(no_proxies):
    no_proxies.reload(["http://only.proxy.test:1"])
    sess = await sh.get_tls_session("tok-a")
    assert sess.calls == [("GET", "https://discord.com", "http://only.proxy.test:1")]
    assert no_proxies.current("tok-a") == "http://only.proxy.test:1"


@pytest.mark.asyncio
async def test_cookie_priming_never_goes_direct():
    sess = FakeSession()
    with pytest.raises(NoProxyAvailable):
        await sh._prime_tls_session(sess, "tok-a", proxy=None)
    assert sess.calls == []


@pytest.mark.asyncio
async def test_shared_session_still_needs_a_lease(no_proxies):
    owner = FakeSession()
    sh.share_tls_session("tok-a", lambda: owner)
    with pytest.raises(NoProxyAvailable):
        await sh.get_tls_session("tok-a")


@pytest.mark.asyncio
async def test_send_without_proxy_makes_no_request_and_marks_nothing_dead(no_proxies):
    log = _Log()
    db = _DB(["a", "b"])
    s = _sender(db, log)

    status = await s.send(mapping_id="m", target_channel_id=1, content="hi")

    assert status == SEND_NO_TOKENS
    assert FakeSession.made == []
    assert db.disabled == []
    assert s._dead_tokens == set()
    # one warning per token, by masked id, never the token value
    assert len(log.warnings) == 2
    assert all("tok-a" not in w and "tok-b" not in w for w in log.warnings)


@pytest.mark.asyncio
async def test_no_proxy_warning_is_rate_limited_per_token(no_proxies):
    log = _Log()
    s = _sender(_DB(["a"]), log)
    for _ in range(3):
        status, _ = await s._request_with_token(
            "tok-a", "https://discord.com/api/v9/users/@me", method="GET"
        )
        assert status == SEND_NO_TOKENS
    assert len(log.warnings) == 1


@pytest.mark.asyncio
async def test_lost_lease_after_session_sends_nothing(no_proxies, monkeypatch):
    log = _Log()
    s = _sender(_DB(["a"]), log)
    sess = FakeSession()

    async def fake_tls(token, *, use_proxy=True):
        return sess

    s._tls_session = fake_tls
    status, _ = await s._request_with_token(
        "tok-a", "https://discord.com/api/v9/channels/1/messages", json_body={}
    )
    assert status == SEND_NO_TOKENS
    assert sess.calls == []


@pytest.mark.asyncio
async def test_typing_and_upload_put_skip_without_proxy(no_proxies):
    s = _sender(_DB(["a"]), _Log())
    sess = FakeSession()

    async def fake_tls(token, *, use_proxy=True):
        return sess

    s._tls_session = fake_tls
    await s._type_for(1, "tok-a", 0)
    assert await s._put_upload("tok-a", "https://upload.test/x", b"x", "text/plain", use_proxy=False) is False
    assert sess.calls == []
