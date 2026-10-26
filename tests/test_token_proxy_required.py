"""Proxies for user tokens: on by default, optional per mapping. A proxied token with
no proxy lease makes no request (no Discord); a direct one never asks for a lease."""

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
    monkeypatch.setattr(sh, "_TLS_DIRECT", set())
    monkeypatch.setattr(sh, "_TLS_SESSION_LOCKS", {})
    monkeypatch.setattr(sh, "_TLS_LAST_USED", {})
    monkeypatch.setattr(sh, "_TLS_SHARED", {})
    monkeypatch.setattr(token_proxy, "_last_warned", {})
    monkeypatch.setattr(token_proxy, "_tokens_disabled_noticed", False)
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


def test_flag_honours_the_stored_setting_and_defaults_on():
    assert token_proxy.token_use_proxy({"USER_TOKEN_USE_PROXIES": False}) is False
    assert token_proxy.token_use_proxy({"USER_TOKEN_USE_PROXIES": "false"}) is False
    assert token_proxy.token_use_proxy({"USER_TOKEN_USE_PROXIES": True}) is True
    assert token_proxy.token_use_proxy({"USER_TOKEN_USE_PROXIES": None}) is True
    assert token_proxy.token_use_proxy({"OTHER": 1}) is True
    assert token_proxy.token_use_proxy({}) is True
    assert token_proxy.token_use_proxy(None) is True


def test_a_token_goes_direct_only_when_every_mapping_using_it_is_off(monkeypatch):
    # empty until the server's first plan: every token is proxied
    assert token_proxy.resolve_use_proxy("tok-a", False) is True
    token_proxy.set_direct_tokens({"tok-a"})
    assert token_proxy.resolve_use_proxy("tok-a", False) is False
    assert token_proxy.resolve_use_proxy("tok-a", True) is True
    # not in the plan's direct set (another mapping using it wants proxies)
    assert token_proxy.resolve_use_proxy("tok-b", False) is True


class _RecordingPool(ProxyPool):
    """A real pool that records every lease and current() lookup."""

    def __init__(self):
        super().__init__()
        self.asked = []

    async def lease(self, token, *a, **k):
        self.asked.append(("lease", token))
        return await super().lease(token, *a, **k)

    def current(self, token):
        self.asked.append(("current", token))
        return super().current(token)


@pytest.fixture
def recording_pool(no_proxies, monkeypatch):
    pool = _RecordingPool()
    pool.reload([])
    monkeypatch.setattr(sh, "_get_proxy_pool", lambda: pool)
    monkeypatch.setattr(token_sender, "get_proxy_pool", lambda: pool)
    return pool


@pytest.mark.asyncio
async def test_direct_mapping_sends_with_no_lease_and_no_proxy(recording_pool):
    token_proxy.set_direct_tokens({"tok-a"})
    log = _Log()
    s = _sender(_DB(["a"]), log)

    status = await s.send(mapping_id="m", target_channel_id=1, content="hi", use_proxy=False)

    assert status == token_sender.SEND_OK
    assert recording_pool.asked == []
    (sess,) = FakeSession.made
    # primed and sent from the server's own address: no proxy on any request
    assert sess.calls[0] == ("GET", "https://discord.com", None)
    assert any(c[0] == "POST" and c[2] is None for c in sess.calls[1:])
    # no proxies configured, but nothing needed one: no notice
    assert token_proxy.TOKENS_DISABLED_NOTICE not in log.warnings


@pytest.mark.asyncio
async def test_token_shared_with_a_proxied_mapping_stays_proxied(recording_pool):
    # tok-a is also used by a mapping that wants proxies, so it is not in the direct set
    token_proxy.set_direct_tokens({"tok-other"})
    log = _Log()
    s = _sender(_DB(["a"]), log)

    status = await s.send(mapping_id="m", target_channel_id=1, content="hi", use_proxy=False)

    assert status == SEND_NO_TOKENS
    assert FakeSession.made == []
    assert log.warnings == [token_proxy.TOKENS_DISABLED_NOTICE]


@pytest.mark.asyncio
async def test_switching_a_token_to_direct_primes_again_without_the_proxy(no_proxies):
    no_proxies.reload(["http://only.proxy.test:1"])
    sess = await sh.get_tls_session("tok-a")
    assert sess.calls == [("GET", "https://discord.com", "http://only.proxy.test:1")]
    token_proxy.set_direct_tokens({"tok-a"})
    again = await sh.get_tls_session("tok-a", use_proxy=False)
    assert again is sess
    assert sess.calls[-1] == ("GET", "https://discord.com", None)


@pytest.mark.asyncio
async def test_get_tls_session_without_proxies_makes_no_session(no_proxies):
    with pytest.raises(NoProxyAvailable):
        # use_proxy=False alone is not enough: the server's plan has not let the token go direct
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
    # no proxies at all: the one disabled notice, not a line per token
    assert log.warnings == [token_proxy.TOKENS_DISABLED_NOTICE]


@pytest.mark.asyncio
async def test_zero_proxies_logs_the_notice_once_and_no_per_token_line(no_proxies):
    log = _Log()
    s = _sender(_DB(["a", "b", "c"]), log)
    for _ in range(3):
        assert await s.send(mapping_id="m", target_channel_id=1, content="hi") == SEND_NO_TOKENS
    assert log.warnings == [token_proxy.TOKENS_DISABLED_NOTICE]
    assert not any("No proxy available" in w for w in log.warnings)
    assert FakeSession.made == []


@pytest.mark.asyncio
async def test_no_lease_with_proxies_configured_warns_per_token(no_proxies):
    no_proxies.reload(["http://only.proxy.test:1"])
    assert await no_proxies.lease("someone-else") == "http://only.proxy.test:1"
    log = _Log()
    s = _sender(_DB(["a", "b"]), log)

    assert await s.send(mapping_id="m", target_channel_id=1, content="hi") == SEND_NO_TOKENS

    assert len(log.warnings) == 2
    assert all(w.startswith("[user-send] No proxy available for token ") for w in log.warnings)
    assert all("tok-a" not in w and "tok-b" not in w for w in log.warnings)
    assert token_proxy.TOKENS_DISABLED_NOTICE not in log.warnings
    assert FakeSession.made == []


@pytest.mark.asyncio
async def test_no_proxy_warning_is_rate_limited_per_token(no_proxies):
    no_proxies.reload(["http://only.proxy.test:1"])
    await no_proxies.lease("someone-else")
    log = _Log()
    s = _sender(_DB(["a"]), log)
    for _ in range(3):
        status, _ = await s._request_with_token(
            "tok-a", "https://discord.com/api/v9/users/@me", method="GET"
        )
        assert status == SEND_NO_TOKENS
    assert len(log.warnings) == 1
    assert "No proxy available" in log.warnings[0]


def test_notice_logs_once_per_state_change(monkeypatch, caplog):
    import logging

    monkeypatch.setattr(token_proxy, "_tokens_disabled_noticed", False)
    log = logging.getLogger("test.token_proxy")
    caplog.set_level(logging.INFO, logger="test.token_proxy")
    assert token_proxy.notice_tokens_enabled(log) is False
    assert token_proxy.notice_tokens_disabled(log) is True
    assert token_proxy.notice_tokens_disabled(log) is False
    assert token_proxy.notice_tokens_enabled(log) is True
    assert token_proxy.notice_tokens_enabled(log) is False
    assert token_proxy.notice_tokens_disabled(log) is True
    assert [r.getMessage() for r in caplog.records] == [
        token_proxy.TOKENS_DISABLED_NOTICE,
        token_proxy.TOKENS_ENABLED_NOTICE,
        token_proxy.TOKENS_DISABLED_NOTICE,
    ]
    assert "—" not in token_proxy.TOKENS_DISABLED_NOTICE


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
