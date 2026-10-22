"""StandaloneScraper's transport: REST on the scraper's own curl_cffi session per token with
that token's client headers, the gateway opened and identified the way the selfclient does
it, bios spread over the valid tokens, proxy credentials kept out of the logs, and every
session closed at the end and on a cancel. A fake curl_cffi session and a fake socket stand
in for Discord; nothing calls it."""
import asyncio
import base64
import json
import logging
import zlib

import pytest

from copycord.admin import standalone_scraper as ss
from copycord.admin.standalone_scraper import ScraperConfig, StandaloneScraper
from copycord.common.selfbot_headers import TLS_IMPERSONATE, build_headers, gateway_properties
from copycord.selfclient.gateway import (
    IDLE_VOICE_STATE,
    RESUME_CLOSE_CODE,
    GatewayClient,
    gateway_url,
)

PASSWORD = "s3cretpw"
PROXIES = [f"host{i}.example:800{i}:user{i}:{PASSWORD}" for i in range(3)]
PROXY_URLS = [f"http://user{i}:{PASSWORD}@host{i}.example:800{i}" for i in range(3)]
# Token i is planned onto proxy i: tok-a, tok-bad, tok-b.
TOKENS = ["tok-a", "tok-bad", "tok-b"]
MEMBERS = {
    "a": [{"user": {"id": f"1{i}", "username": f"a{i}"}} for i in range(3)],
    "b": [{"user": {"id": f"2{i}", "username": f"b{i}"}} for i in range(3)],
}


class _Resp:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self.body = body
        self.headers = {}

    def json(self):
        return self.body


class FakeHTTP:
    """A curl_cffi AsyncSession: records how it was made, each GET's url and headers."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        self.closed = False

    async def get(self, url, headers=None, timeout=None, **kwargs):
        self.calls.append((url, dict(headers or {})))
        await asyncio.sleep(0.01)
        token = (headers or {}).get("Authorization")
        if token == "tok-bad":
            # An error that quotes the proxy URL, credentials and all.
            raise RuntimeError(f"connect through {self.kwargs.get('proxy')} failed")
        if url.endswith("/users/@me"):
            return _Resp(200, {"username": token, "id": "1"})
        if "/guilds/" in url:
            return _Resp(200, {"approximate_member_count": 6, "name": "Guild"})
        if url.endswith("/profile"):
            uid = url.rstrip("/").split("/")[-2]
            return _Resp(200, {"user": {"bio": f"bio {uid}"}})
        return _Resp(404)

    async def close(self):
        self.closed = True


class FakeSocket:
    """The gateway over zlib-stream: HELLO, READY after IDENTIFY, a member chunk per op 8.
    Each message is split over two frames, as Discord may."""

    def __init__(self, url, headers, proxy, *, hello=True):
        self.url, self.headers, self.proxy = url, headers, proxy
        self.sent = []
        self.inbox = asyncio.Queue()
        self.closed = False
        self._z = zlib.compressobj()
        if hello:
            self.push({"op": 10, "d": {"heartbeat_interval": 41250}})

    def push(self, payload):
        data = self._z.compress(json.dumps(payload).encode()) + self._z.flush(zlib.Z_SYNC_FLUSH)
        half = len(data) // 2
        self.inbox.put_nowait(data[:half])
        self.inbox.put_nowait(data[half:])

    async def recv(self):
        item = await self.inbox.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def send(self, text):
        msg = json.loads(text)
        self.sent.append(msg)
        if msg["op"] == 2:
            self.push({"op": 0, "t": "READY", "s": 1, "d": {"session_id": "x"}})
        elif msg["op"] == 8:
            d = msg["d"]
            self.push({"op": 0, "t": "GUILD_MEMBERS_CHUNK", "s": 2,
                       "d": {"nonce": d["nonce"], "members": MEMBERS.get(d["query"], [])}})

    async def close(self, code=1000):
        self.closed = True
        self.close_sent = code
        self.inbox.put_nowait(ConnectionError("closed"))

    def identify(self):
        return next(m for m in self.sent if m["op"] == 2)


class _Capture:
    def __init__(self):
        self.sent = []

    async def send(self, text):
        self.sent.append(json.loads(text))


async def _client_identify(token):
    """What the selfclient's GatewayClient sends as IDENTIFY for this token."""
    ref = GatewayClient(token, reconnect=False)
    ref._ws = _Capture()
    await ref._send_identify()
    return ref._ws.sent[0]


@pytest.fixture()
def fakes(monkeypatch):
    sessions, sockets = [], []

    def make_http(**kwargs):
        sess = FakeHTTP(**kwargs)
        sessions.append(sess)
        return sess

    async def open_ws(url, headers, proxy=None, **kwargs):
        sock = FakeSocket(url, headers, proxy, hello=fakes_state["hello"])
        sockets.append(sock)
        return sock

    fakes_state = {"hello": True}
    monkeypatch.setattr(ss, "_CurlAsyncSession", make_http)
    monkeypatch.setattr(ss, "open_websocket", open_ws)
    return sessions, sockets, fakes_state


def _scraper(logs, **kwargs):
    config = ScraperConfig(guild_id=1, tokens=list(TOKENS), proxies=list(PROXIES),
                           log_callback=lambda msg, level="info": logs.append(msg), **kwargs)
    s = StandaloneScraper(config)

    async def pause(seconds):
        await asyncio.sleep(0)
        return s._stop_event.is_set()

    s._pause = pause
    return s


def _by_token(sessions):
    """{token: [sessions whose calls carried it]}"""
    out = {}
    for sess in sessions:
        for token in {h.get("Authorization") for _, h in sess.calls}:
            out.setdefault(token, []).append(sess)
    return out


@pytest.mark.asyncio
async def test_scrape_uses_each_tokens_own_session_headers_and_gateway_identity(fakes, caplog):
    sessions, sockets, _ = fakes
    caplog.set_level(logging.DEBUG)
    logs = []
    s = _scraper(logs, include_bio=True)
    result = await asyncio.wait_for(s.scrape(), timeout=30)

    assert result.success, result.error
    assert result.metadata["method"] == "gateway"
    members = {m["id"]: m for m in result.members}
    assert set(members) == {"10", "11", "12", "20", "21", "22"}
    assert all(m["bio"] == f"bio {uid}" for uid, m in members.items())

    # REST: one curl_cffi session per token, Chrome TLS, the token's own proxy and headers.
    assert all(sess.kwargs["impersonate"] == TLS_IMPERSONATE for sess in sessions)
    by_token = _by_token(sessions)
    assert set(by_token) == set(TOKENS)
    for i, token in enumerate(TOKENS):
        assert len(by_token[token]) == 1
        assert by_token[token][0].kwargs["proxy"] == PROXY_URLS[i]
    for sess in sessions:
        for _url, headers in sess.calls:
            assert headers == build_headers(headers["Authorization"])

    # Bios: both valid tokens fetched some, each through its own session.
    profile_tokens = {h["Authorization"] for sess in sessions
                      for url, h in sess.calls if url.endswith("/profile")}
    assert profile_tokens == {"tok-a", "tok-b"}

    # Gateway: one socket per valid token, zlib-stream, the token's proxy, the client's
    # upgrade headers and the client's IDENTIFY.
    assert sorted(sock.proxy for sock in sockets) == [PROXY_URLS[0], PROXY_URLS[2]]
    for sock in sockets:
        token = sock.identify()["d"]["token"]
        assert sock.proxy == PROXY_URLS[TOKENS.index(token)]
        assert sock.url == gateway_url(zlib_stream=True) and "compress=zlib-stream" in sock.url
        assert sock.headers == GatewayClient(token)._ws_headers()
        assert sock.headers["User-Agent"] == build_headers(token)["User-Agent"]
        identify = sock.identify()
        assert identify == await _client_identify(token)
        # The client's connect order, op 40 heartbeats only, and no op 41.
        ops = [m["op"] for m in sock.sent]
        assert ops[:2] == [2, 4] and 1 not in ops and 41 not in ops
        for beat in (m for m in sock.sent if m["op"] == 40):
            assert beat["d"]["qos"]["active"] is False
        d = identify["d"]
        assert d["properties"] == gateway_properties(token)
        assert d["properties"]["browser"] == "Chrome" and d["properties"]["os"] == "Windows"
        assert "$browser" not in d["properties"] and "large_threshold" not in d
        assert d["compress"] is False and "capabilities" in d and "client_state" in d
        assert "presence" in d
        # REST and gateway carry one fingerprint per token.
        super_props = json.loads(base64.b64decode(build_headers(token)["X-Super-Properties"]))
        assert super_props["client_launch_id"] == d["properties"]["client_launch_id"]
        assert "client_heartbeat_session_id" not in super_props
        assert super_props["client_app_state"] == d["properties"]["client_app_state"] == "unfocused"
    assert sockets[0].identify()["d"]["properties"]["installation_id"] != \
        sockets[1].identify()["d"]["properties"]["installation_id"]

    # Nothing left open.
    assert all(sess.closed for sess in sessions)
    assert all(sock.closed for sock in sockets)


@pytest.mark.asyncio
async def test_no_log_line_carries_a_proxy_password(fakes, caplog):
    caplog.set_level(logging.DEBUG)
    logs = []
    s = _scraper(logs)
    result = await asyncio.wait_for(s.scrape(), timeout=30)
    assert result.success, result.error
    lines = logs + [r.getMessage() for r in caplog.records]
    assert not [line for line in lines if PASSWORD in line]
    # The proxies are still logged, masked, and the failing token's error was logged too.
    assert any("Using proxy: http://***@host0.example:8000" in line for line in lines)
    assert any("Token validation error: connect through http://***@host1.example" in line
               for line in lines)


@pytest.mark.asyncio
async def test_cancel_closes_every_session_and_socket(fakes):
    sessions, sockets, state = fakes
    state["hello"] = False  # the sockets sit waiting for HELLO
    s = _scraper([], include_bio=True)
    task = asyncio.ensure_future(s.scrape())
    for _ in range(300):
        if len(sockets) == 2:
            break
        await asyncio.sleep(0.01)
    assert len(sockets) == 2 and sessions
    s.stop()
    task.cancel()
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result.error == "Cancelled"
    assert all(sess.closed for sess in sessions)
    assert all(sock.closed for sock in sockets)
    assert not s._http_sessions
    assert not [t for t in asyncio.all_tasks()
                if "_bio_worker" in getattr(t.get_coro(), "__qualname__", "")]


@pytest.mark.asyncio
async def test_gateway_reconnect_rotates_to_another_proxy(fakes, monkeypatch):
    _sessions, sockets, _ = fakes
    fake_open = ss.open_websocket
    tried = []

    async def open_ws(url, headers, proxy=None, **kwargs):
        tried.append(proxy)
        if len(tried) == 1:
            # Discord drops the first connection, as it does on a choked proxy.
            raise ConnectionError("1011 keepalive ping timeout")
        return await fake_open(url, headers, proxy, **kwargs)

    monkeypatch.setattr(ss, "open_websocket", open_ws)
    config = ScraperConfig(guild_id=1, tokens=["tok-a"], proxies=list(PROXIES[:2]),
                           sessions_per_token=1)
    s = StandaloneScraper(config)

    async def pause(seconds):
        await asyncio.sleep(0)
        return s._stop_event.is_set()

    s._pause = pause
    result = await asyncio.wait_for(s.scrape(), timeout=30)

    assert result.success, result.error
    assert tried[:2] == [PROXY_URLS[0], PROXY_URLS[1]]
    assert [sock.proxy for sock in sockets] == [PROXY_URLS[1]]
    # The REST side keeps the planned proxy.
    assert s._proxy_for("tok-a", 0) == PROXY_URLS[0]


@pytest.mark.asyncio
async def test_rest_timezone_is_each_tokens_proxy_exit_zone(fakes, monkeypatch):
    from copycord.common import exit_timezone

    sessions, _, _ = fakes
    zones = {PROXY_URLS[0]: "Europe/Berlin", PROXY_URLS[2]: "Asia/Tokyo"}

    async def lookup(proxy, timeout):
        return {"timezone": zones.get(proxy, "America/Chicago")}

    monkeypatch.setattr(exit_timezone, "_fetch_json", lookup)
    result = await asyncio.wait_for(_scraper([]).scrape(), timeout=30)

    assert result.success, result.error
    by_token = _by_token(sessions)
    for token, zone in (("tok-a", "Europe/Berlin"), ("tok-b", "Asia/Tokyo")):
        assert {h["X-Discord-Timezone"] for sess in by_token[token] for _u, h in sess.calls} == {zone}


@pytest.mark.asyncio
@pytest.mark.parametrize("ready,error", [
    (0, "No gateway session connected to Discord (2 tried)"),
    (1, "The Discord gateway returned no members"),
])
async def test_no_members_fails_the_scrape_without_a_rest_fallback(fakes, ready, error):
    sessions, _sockets, _ = fakes
    s = _scraper([])

    async def gateway(**kwargs):
        s._sessions_ready += ready

    s._run_gateway_session = gateway
    result = await asyncio.wait_for(s.scrape(), timeout=10)
    assert not result.success and result.error == error
    assert not hasattr(s, "_scrape_with_rest_api")
    urls = [url for sess in sessions for url, _ in sess.calls]
    assert not [u for u in urls if u.endswith("/members") or "/members?" in u]
    assert all(sess.closed for sess in sessions)


# the debug report: what each token sent, masked, captured where the scraper builds it

LONG_TOKENS = ["tokAAA" + "a" * 30 + "end1", "tokBBB" + "b" * 30 + "end2"]


def _debug_scraper(**kwargs):
    config = ScraperConfig(guild_id=1, tokens=list(LONG_TOKENS), proxies=list(PROXIES[:2]),
                           debug_report=True, **kwargs)
    s = StandaloneScraper(config)

    async def pause(seconds):
        await asyncio.sleep(0)
        return s._stop_event.is_set()

    s._pause = pause
    return s


def _decoded(headers):
    return json.loads(base64.b64decode(headers["X-Super-Properties"]))


@pytest.mark.asyncio
async def test_debug_report_records_each_tokens_headers_identify_and_proxy_masked(fakes):
    _sessions, sockets, _ = fakes
    s = _debug_scraper(include_bio=True)
    result = await asyncio.wait_for(s.scrape(), timeout=30)
    assert result.success, result.error
    report = s.debug_report(status="success")

    text = json.dumps(report)
    assert not [t for t in LONG_TOKENS if t in text]
    assert PASSWORD not in text and "user0:" not in text
    assert report["kind"] == "copycord-scrape-debug-report" and report["status"] == "success"
    assert report["guild_id"] == "1" and report["impersonate"] == TLS_IMPERSONATE

    for i, (token, entry) in enumerate(zip(LONG_TOKENS, report["tokens"])):
        masked = token[:6] + "..." + token[-4:]
        expected = build_headers(token)
        assert entry["index"] == i + 1 and entry["token"] == masked and entry["valid"] is True
        assert entry["user_agent"] == expected["User-Agent"]
        assert entry["super_properties"] == _decoded(expected)
        assert entry["super_properties"]["browser"] == "Chrome"

        # REST: every kind sent, with the exact headers, the session slot, TLS target and proxy.
        kinds = [r["kind"] for r in entry["rest"]]
        assert "token_check" in kinds and "bio" in kinds
        assert ("guild_info" in kinds) == (i == 0)
        for r in entry["rest"]:
            assert r["method"] == "GET" and r["requests"] >= 1
            assert r["headers"] == {**expected, "Authorization": masked}
            assert r["super_properties"] == _decoded(expected)
            assert r["impersonate"] == TLS_IMPERSONATE and r["session_slot"] == 0
            assert r["proxy"] == f"http://***@host{i}.example:800{i}"
            assert r["user_agent"] == expected["User-Agent"]

        # Gateway: the socket's upgrade headers and the IDENTIFY as sent, without its token.
        (gw,) = entry["gateway"]
        sock = next(sock for sock in sockets if sock.identify()["d"]["token"] == token)
        assert gw["session_index"] == i and gw["session_slot"] == 0 and gw["connects"] == 1
        assert gw["url"] == sock.url == gateway_url(zlib_stream=True)
        assert gw["headers"] == sock.headers == GatewayClient(token)._ws_headers()
        assert gw["proxy"] == f"http://***@host{i}.example:800{i}"
        sent = sock.identify()
        assert "token" not in gw["identify"]["d"]
        assert gw["identify"] == {**sent, "d": {k: v for k, v in sent["d"].items() if k != "token"}}
        assert gw["identify"]["d"]["properties"] == gateway_properties(token)


@pytest.mark.asyncio
async def test_debug_report_keeps_what_was_captured_when_the_scrape_fails(fakes):
    s = _debug_scraper()

    async def gateway(**kwargs):
        return None

    s._run_gateway_session = gateway
    result = await asyncio.wait_for(s.scrape(), timeout=10)
    assert not result.success
    report = s.debug_report(status="failed", error=result.error)
    assert report["status"] == "failed" and report["error"] == result.error
    assert [r["kind"] for r in report["tokens"][0]["rest"]] == ["token_check", "guild_info"]
    assert report["tokens"][0]["gateway"] == []


@pytest.mark.asyncio
async def test_no_debug_report_unless_asked(fakes):
    _sessions, sockets, _ = fakes
    s = _scraper([])
    assert s.debug_report() is None
    result = await asyncio.wait_for(s.scrape(), timeout=30)
    assert result.success and s.debug_report() is None
    assert all(isinstance(sock, FakeSocket) for sock in sockets)


def test_debug_recorder_masks_and_folds_reconnects():
    from copycord.admin import scrape_debug as sd

    token = LONG_TOKENS[0]
    assert sd.mask_token(token) == token[:6] + "..." + token[-4:]
    assert sd.mask_token("short") == "***" and sd.mask_token(None) == "***"
    assert sd.masked_proxy(None) is None
    assert sd.masked_proxy(f"user:{PASSWORD}@1.2.3.4:80") == "http://***@1.2.3.4:80"
    assert sd.decode_super_properties("not base64 json") is None

    rec = sd.DebugRecorder([token], [PROXY_URLS[0]])
    headers = {"User-Agent": "UA", "Origin": "https://discord.com"}
    identify = json.dumps({"op": 2, "d": {"token": token, "properties": {"os": "Windows"}}})
    for _ in range(3):
        rec.gateway_connect(token, 0, "wss://gw", headers, proxy=PROXY_URLS[0])
        rec.gateway_sent(token, 0, json.dumps({"op": 40, "d": {"seq": None}}))
        rec.gateway_sent(token, 0, identify)
    rec.gateway_connect(token, 0, "wss://gw", headers, proxy=PROXY_URLS[0])  # never identified
    # A URL or error that quotes a secret is scrubbed too.
    rec.rest(token, "other", "GET", f"https://x/?t={token}", {"Authorization": token}, proxy=PROXY_URLS[0])
    report = rec.snapshot(error=f"proxy {PROXY_URLS[0]} failed")
    text = json.dumps(report)
    assert token not in text and PASSWORD not in text
    gw = report["tokens"][0]["gateway"]
    assert [g["connects"] for g in gw] == [3, 1]
    assert gw[0]["identify"] == {"op": 2, "d": {"properties": {"os": "Windows"}}}
    assert gw[1]["identify"] is None


# A server op 1, and RESUME after a drop, as the selfclient does them.

RESUME_HOST = "wss://gateway-resume.example.gg"


class ScriptedSocket(FakeSocket):
    """A socket that follows ``plan`` once its session is up (READY or RESUMED):
    "reconnect" sends op 7, "op1" sends a server op 1; None just serves member chunks.
    HELLO's interval is long enough that no timed op 40 falls inside a test."""

    def __init__(self, url, headers, proxy, plan):
        super().__init__(url, headers, proxy, hello=False)
        self.plan = plan
        self.seq = 0
        self.close_code = None
        self.close_reason = ""
        self.close_sent = None
        # len(sent) when the server op 1 was handed over.
        self.op1_at = None
        self.push({"op": 10, "d": {"heartbeat_interval": 10_000_000}})

    async def recv(self):
        item = await self.inbox.get()
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, tuple):
            self.op1_at = len(self.sent)
            return item[1]
        return item

    def _session_up(self):
        if self.plan == "reconnect":
            self.push({"op": 7, "d": None})
        elif self.plan == "op1":
            data = self._z.compress(json.dumps({"op": 1, "d": None}).encode())
            self.inbox.put_nowait(("op1", data + self._z.flush(zlib.Z_SYNC_FLUSH)))

    async def send(self, text):
        msg = json.loads(text)
        self.sent.append(msg)
        op, d = msg["op"], msg["d"]
        if op == 2:
            self.seq += 1
            self.push({"op": 0, "t": "READY", "s": self.seq,
                       "d": {"session_id": "sess-1", "resume_gateway_url": RESUME_HOST}})
            self._session_up()
        elif op == 6:
            self.seq = d["seq"] + 1
            self.push({"op": 0, "t": "RESUMED", "s": self.seq, "d": {}})
            self._session_up()
        elif op == 8:
            self.seq += 1
            self.push({"op": 0, "t": "GUILD_MEMBERS_CHUNK", "s": self.seq,
                       "d": {"nonce": d["nonce"], "members": MEMBERS.get(d["query"], [])}})

    async def close(self, code=1000):
        if self.close_sent is None:
            self.close_sent = code
        self.closed = True
        self.inbox.put_nowait(ConnectionError("closed"))


def _scripted_scraper(monkeypatch, plans):
    """One token, no proxies; socket i follows plans[i], later ones serve chunks."""
    sockets = []

    async def open_ws(url, headers, proxy=None, **kwargs):
        plan = plans[len(sockets)] if len(sockets) < len(plans) else None
        sockets.append(ScriptedSocket(url, headers, proxy, plan))
        return sockets[-1]

    monkeypatch.setattr(ss, "open_websocket", open_ws)
    s = StandaloneScraper(ScraperConfig(guild_id=1, tokens=["tok-a"], sessions_per_token=1))

    async def pause(seconds):
        await asyncio.sleep(0)
        return s._stop_event.is_set()

    s._pause = pause
    return s, sockets


def _ops(sock):
    return [m["op"] for m in sock.sent]


@pytest.mark.asyncio
async def test_a_server_op_1_is_answered_at_once_with_the_clients_op_40(fakes, monkeypatch):
    s, sockets = _scripted_scraper(monkeypatch, ["op1"])
    result = await asyncio.wait_for(s.scrape(), timeout=30)
    assert result.success, result.error

    sock = sockets[0]
    assert sock.op1_at is not None
    # The next frame after op 1 arrived is op 40, in the main client's qos shape.
    beat = sock.sent[sock.op1_at]
    assert beat == {"op": 40, "d": {"seq": 1, "qos": {"active": False, "ver": 31, "reasons": []}}}
    assert list(beat["d"]) == ["seq", "qos"]
    assert 1 not in _ops(sock)


@pytest.mark.asyncio
async def test_after_op_7_the_session_resumes_on_the_resume_url(fakes, monkeypatch):
    s, sockets = _scripted_scraper(monkeypatch, ["reconnect"])
    result = await asyncio.wait_for(s.scrape(), timeout=30)
    assert result.success, result.error

    assert len(sockets) == 2
    first, second = sockets
    assert _ops(first)[:2] == [2, 4]
    # A 4000-range close keeps the session resumable.
    assert first.close_sent == RESUME_CLOSE_CODE

    assert second.url == gateway_url(RESUME_HOST, zlib_stream=True)
    ops = _ops(second)
    assert ops[:3] == [6, 3, 4] and 2 not in ops
    resume = second.sent[0]["d"]
    assert list(resume) == ["token", "session_id", "seq"]
    assert resume == {"token": "tok-a", "session_id": "sess-1", "seq": 1}
    assert second.sent[1]["d"] == {"status": "online", "since": 0, "activities": [], "afk": True}
    assert second.sent[2]["d"] == IDLE_VOICE_STATE

    # The finished session ends with 1000.
    assert second.close_sent == 1000
