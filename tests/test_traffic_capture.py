"""common.traffic_capture: the dev-mode events.jsonl in discord-capture's format.

REST runs a real HTTPClient on a fake session and the gateway a real
GatewayClient on fake zlib-stream sockets, as in test_selfclient_http.py,
test_selfclient_gateway.py and test_selfclient_science.py; nothing reaches
Discord.

COPYCORD_WRITE_CAPTURE_FIXTURE=1 makes test_fixture_session also write its
capture to tests/fixtures/copycord_capture/events.jsonl, the synthetic sample
fed to discord-capture's report.mjs.
"""
import asyncio
import base64
import functools
import json
import os
import shutil
import zlib
from pathlib import Path

import pytest

from copycord.common import selfbot_headers
from copycord.common import traffic_capture as tc
from copycord.selfclient import Client
from copycord.selfclient.gateway import GatewayClient
from copycord.selfclient.http import API_BASE, HTTPClient

TOKEN = "capture-token-not-real-0123456789.abcdef.ghijklmnopqrstuvwxyz"
ANALYTICS_TOKEN = "capture-analytics-token-not-real"
USER_ID = "987654321098765432"
GUILD = "100000000000000001"
CHANNEL = "100000000000000002"
GATEWAY = "wss://gateway.discord.gg/"
RESUME = "wss://gateway-us-east1-b.discord.gg"
FIXTURE = Path(__file__).parent / "fixtures" / "copycord_capture" / "events.jsonl"
_CLOSE = object()


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    tc.forget_all()
    yield tmp_path
    tc.forget_all()
    selfbot_headers.set_heartbeat_session_id(TOKEN, None)


@pytest.fixture
def dev(data, monkeypatch):
    monkeypatch.setenv("COPYCORD_ENV", "dev")
    return data


@pytest.fixture
def prod(data, monkeypatch):
    monkeypatch.delenv("COPYCORD_ENV", raising=False)
    return data


def capture_files(root):
    return sorted((root / tc.CAPTURE_DIR).glob(f"*/{tc.EVENTS_FILE}"))


def read_events(root):
    (path,) = capture_files(root)
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def types(events):
    return [e["type"] for e in events]


# fakes


class FakeResponse:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body
        self.text = "" if body is None else json.dumps(body)
        self.content = self.text.encode()
        self.headers = {}

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    """Answers the paths the tests use; records every request."""

    def __init__(self):
        self.calls = []

    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        path = url.split("?", 1)[0]
        if path.endswith("/science"):
            return FakeResponse(204)
        if path.endswith("/users/@me"):
            return FakeResponse(200, {"id": USER_ID, "username": "someone"})
        if path.endswith("/affinities/users"):
            raise OSError("Failed to perform, curl: (7) Could not connect to server")
        if path.endswith(f"/channels/{CHANNEL}/messages"):
            return FakeResponse(200, {"id": "100000000000000003", "channel_id": CHANNEL})
        return FakeResponse(404, {"message": "Unknown", "code": 0})

    def science_posts(self):
        return [c for c in self.calls if c[1].endswith("/science")]

    async def close(self):
        pass


class FakeWS:
    """A zlib-stream socket: frames fed as dicts go out compressed on one shared stream."""

    def __init__(self, frames=(), *, auto_ack=False):
        self.queue = asyncio.Queue()
        self.sent = []
        self.close_code = None
        self.close_reason = ""
        self.closed = False
        self.auto_ack = auto_ack
        self._deflate = zlib.compressobj()
        for frame in frames:
            self.feed(frame)

    def feed(self, frame):
        data = self._deflate.compress(json.dumps(frame).encode()) + self._deflate.flush(zlib.Z_SYNC_FLUSH)
        self.queue.put_nowait(data)

    async def send(self, data):
        msg = json.loads(data)
        self.sent.append(msg)
        if self.auto_ack and msg.get("op") in (1, 40):
            self.feed({"op": 11})

    async def close(self, code=1000, reason=""):
        if self.closed:
            return
        self.closed = True
        self.close_code = code
        self.close_reason = reason
        self.queue.put_nowait(_CLOSE)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.queue.get()
        if item is _CLOSE:
            raise StopAsyncIteration
        return item

    def ops(self):
        return [m.get("op") for m in self.sent]


class Dialer:
    def __init__(self, *sockets):
        self.sockets = list(sockets)
        self.urls = []

    async def __call__(self, url, headers, proxy):
        self.urls.append(url)
        if not self.sockets:
            raise ConnectionRefusedError("no more scripted sockets")
        return self.sockets.pop(0)


async def wait_until(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


def hello(interval_ms=45000):
    return {"op": 10, "d": {"heartbeat_interval": interval_ms, "_trace": ['["gateway-prd-test",{"micros":0}]']}}


def ready(seq=1):
    return {
        "op": 0,
        "t": "READY",
        "s": seq,
        "d": {
            "v": 9,
            "user": {"id": USER_ID, "username": "someone", "email": "someone@example.com"},
            "session_id": "sess-capture-1",
            "resume_gateway_url": RESUME,
            "analytics_token": ANALYTICS_TOKEN,
            "users": [{"id": "1", "username": "a"}],
            "private_channels": [],
            "guilds": [{"id": GUILD, "channels": [{"id": CHANNEL, "type": 0, "name": "general"}]}],
            "_trace": ['["gateway-prd-test",{"micros":475123}]'],
        },
    }


async def gateway_session(**kwargs):
    """HELLO and READY on one socket, then a clean close."""
    ws = FakeWS([hello(), ready()])
    client = GatewayClient(TOKEN, connector=Dialer(ws), zlib_stream=True, **kwargs)
    task = asyncio.create_task(client.run(refresh_build=False))
    await wait_until(lambda: 37 in ws.ops())
    await client.close()
    await asyncio.wait_for(task, timeout=3.0)
    return ws


async def rest_calls():
    http = HTTPClient(TOKEN, session=FakeSession())
    await http.login()
    await http.send_message(CHANNEL, "hello", guild_id=GUILD)


# dev mode off


class TestDevOff:
    def test_no_recorder(self, prod):
        assert tc.recorder_for(TOKEN) is None
        tc.stop(TOKEN)
        assert not (prod / tc.CAPTURE_DIR).exists()

    @pytest.mark.asyncio
    async def test_rest_and_gateway_write_nothing(self, prod):
        await rest_calls()
        await gateway_session()
        assert not (prod / tc.CAPTURE_DIR).exists()
        assert tc._RECORDERS == {}


# dev mode on


class TestRest:
    @pytest.mark.asyncio
    async def test_types_in_order(self, dev):
        await rest_calls()
        events = read_events(dev)
        assert types(events) == [
            "capture_start",
            "request",
            "request_headers",
            "response",
            "request",
            "request_headers",
            "request_body",
            "response",
        ]
        start = events[0]
        assert start["tool"] == "copycord"
        assert start["account"] == selfbot_headers._token_log_id(TOKEN)
        assert start["impersonate"] == selfbot_headers.TLS_IMPERSONATE
        assert start["userAgent"] == selfbot_headers.get_build_info()["browser_user_agent"]
        assert start["python"] and start["version"]
        for e in events:
            assert isinstance(e["ts"], int) and e["time"][10] == "T"

    @pytest.mark.asyncio
    async def test_records_the_headers_handed_to_curl_cffi(self, dev):
        await rest_calls()
        events = read_events(dev)
        get_req, get_wire = events[1], events[2]
        assert get_req["method"] == "GET"
        assert get_req["url"] == f"{API_BASE}/users/@me"
        assert get_wire["requestId"] == get_req["requestId"]
        sent = HTTPClient(TOKEN).headers_for("GET", has_body=False)
        assert list(get_wire["headers"]) == [k for k, v in sent.items() if v is not None]
        assert get_wire["suppressed"] == ["sec-fetch-user", "upgrade-insecure-requests"]
        assert "Origin" not in get_wire["headers"]
        # Base64 on the wire, as discord-capture records it; decoded alongside.
        raw = get_wire["headers"]["X-Super-Properties"]
        assert json.loads(base64.b64decode(raw)) == get_wire["superProperties"]
        assert get_wire["superProperties"]["client_build_number"]
        assert events[3] == {**events[3], "type": "response", "status": 200}

        post_wire, body = events[5], events[6]
        assert post_wire["headers"]["Origin"] == "https://discord.com"
        assert post_wire["headers"]["Content-Type"] == "application/json"
        assert "X-Context-Properties" in post_wire["headers"]
        assert body["json"]["content"] == "hello"
        assert list(body["json"])[:4] == ["mobile_network_type", "content", "nonce", "tts"]

    @pytest.mark.asyncio
    async def test_failed_request(self, dev):
        http = HTTPClient(TOKEN, session=FakeSession())
        with pytest.raises(Exception):
            await http.get("/users/@me/affinities/users")
        events = read_events(dev)
        assert types(events) == ["capture_start", "request", "request_headers", "request_failed"]
        assert "Could not connect" in events[-1]["errorText"]

    @pytest.mark.asyncio
    async def test_other_hosts_are_not_recorded(self, dev):
        rec = tc.recorder_for(TOKEN)
        assert rec.request("GET", "https://example.com/api", {"Authorization": TOKEN}) is None
        assert rec.ws_open("wss://resume.example.gg/?v=9", {}) is None
        assert types(read_events(dev)) == ["capture_start"]


class TestGateway:
    @pytest.mark.asyncio
    async def test_types_in_order(self, dev):
        ws = await gateway_session()
        events = read_events(dev)
        assert types(events) == [
            "capture_start",
            "ws_created",
            "ws_handshake_request",
            "ws_recv",
            "ws_sent",
            "ws_sent",
            "ws_sent",
            "ws_sent",
            "ws_recv",
            "ws_sent",
            "ws_sent",
            "ws_recv",
            "ws_closed",
        ]
        created = events[1]
        assert created["gateway"] is True
        assert created["compression"] == "zlib-stream"
        assert created["url"] == "wss://gateway.discord.gg/?encoding=json&v=9&compress=zlib-stream"
        assert events[2]["headers"]["Origin"] == "https://discord.com"
        assert "Chrome/" in events[2]["headers"]["User-Agent"]
        sent = [e for e in events if e["type"] == "ws_sent"]
        assert [e["op"] for e in sent] == ws.ops() == [2, 4, 41, 40, 3, 37]
        assert sent[0]["d"]["properties"] == ws.sent[0]["d"]["properties"]
        assert list(sent[0]["d"]) == list(ws.sent[0]["d"])
        assert sent[3]["d"] == ws.sent[3]["d"]

    @pytest.mark.asyncio
    async def test_received_frames_keep_what_discord_capture_keeps(self, dev):
        await gateway_session()
        recv = [e for e in read_events(dev) if e["type"] == "ws_recv"]
        hello_rec, ready_rec, close_rec = recv
        assert hello_rec["op"] == 10 and hello_rec["d"]["heartbeat_interval"] == 45000
        assert hello_rec["opcode"] == 2 and hello_rec["decodedSize"] > 0 and hello_rec["size"] > 0
        assert ready_rec["t"] == "READY" and ready_rec["s"] == 1 and ready_rec["dSummarized"] is True
        assert ready_rec["d"] == {
            "v": 9,
            "user": "<object keys=3>",
            "session_id": "sess-capture-1",
            "resume_gateway_url": RESUME,
            "analytics_token": f"<redacted len={len(ANALYTICS_TOKEN)}>",
            "users": "<array len=1>",
            "private_channels": "<array len=0>",
            "guilds": "<array len=1>",
            "_trace": "<array len=1>",
        }
        assert close_rec["opcode"] == 8 and close_rec["closeCode"] == 1000

    @pytest.mark.asyncio
    async def test_dispatch_d_is_dropped_and_ack_d_kept(self, dev):
        rec = tc.recorder_for(TOKEN)
        wid = rec.ws_open(GATEWAY + "?encoding=json&v=9&compress=zlib-stream", {})
        rec.ws_recv(wid, {"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": {"content": "x"}}, size=10, decoded_size=20)
        rec.ws_recv(wid, {"op": 11, "d": None}, size=5, decoded_size=8)
        rec.ws_recv(wid, {"op": 9, "d": False}, size=5, decoded_size=8)
        message, ack, invalid = read_events(dev)[3:]
        assert "d" not in message and message["t"] == "MESSAGE_CREATE" and message["s"] == 2
        assert "d" in ack and ack["d"] is None
        assert invalid["d"] is False

    @pytest.mark.asyncio
    async def test_dial_failure(self, dev):
        client = GatewayClient(TOKEN, connector=Dialer(), reconnect=False)
        await client.run(refresh_build=False)
        assert types(read_events(dev)) == [
            "capture_start", "ws_created", "ws_handshake_request", "ws_error", "ws_closed"
        ]


class TestRedaction:
    @pytest.mark.asyncio
    async def test_no_token_or_authorization_value_on_disk(self, dev):
        await rest_calls()
        await gateway_session()
        text = capture_files(dev)[0].read_text(encoding="utf-8")
        assert TOKEN not in text
        assert ANALYTICS_TOKEN not in text
        assert "someone@example.com" not in text
        events = [json.loads(line) for line in text.splitlines()]
        auth = [e["headers"]["Authorization"] for e in events if e["type"] == "request_headers"]
        assert auth and set(auth) == {f"<redacted len={len(TOKEN)}>"}
        identify = next(e for e in events if e.get("op") == 2 and e["type"] == "ws_sent")
        assert identify["d"]["token"] == f"<redacted len={len(TOKEN)}>"

    def test_resume_and_science_tokens(self, dev):
        rec = tc.recorder_for(TOKEN)
        wid = rec.ws_open(GATEWAY + "?v=9", {})
        rec.ws_sent(wid, {"op": 6, "d": {"token": TOKEN, "session_id": "s", "seq": 3}})
        rec.request("POST", f"{API_BASE}/science", {"Authorization": TOKEN, "Cookie": "a=b"},
                    body={"token": ANALYTICS_TOKEN, "events": []}, has_body=True)
        text = capture_files(dev)[0].read_text(encoding="utf-8")
        assert TOKEN not in text and ANALYTICS_TOKEN not in text and "a=b" not in text
        events = [json.loads(line) for line in text.splitlines()]
        assert events[3]["d"] == {"token": f"<redacted len={len(TOKEN)}>", "session_id": "s", "seq": 3}
        assert events[6]["json"]["token"] == f"<redacted len={len(ANALYTICS_TOKEN)}>"
        assert events[5]["headers"]["Cookie"] == "<redacted len=3>"

    def test_auth_bodies_and_urls(self, dev):
        rec = tc.recorder_for(TOKEN)
        rec.request(
            "POST",
            f"{API_BASE}/auth/mfa/totp",
            {},
            body={"code": "123456", "ticket": "tkt", "login": "a@b.c", "password": "hunter2", "gift": None},
            has_body=True,
        )
        rec.request("GET", "https://discord.com/ra/abc?handoff_token=sekrit&x=1#token=frag", {})
        rec.request("POST", f"{API_BASE}/channels/1/messages", {}, body={"code": "kept"}, has_body=True)
        events = read_events(dev)
        assert events[3]["json"] == {
            "code": "<redacted len=6>",
            "ticket": "<redacted len=3>",
            "login": "<redacted len=5>",
            "password": "<redacted len=7>",
            "gift": None,
        }
        assert events[4]["url"] == "https://discord.com/ra/abc?handoff_token=<redacted len=6>&x=1#token=<redacted len=4>"
        assert events[-1]["json"] == {"code": "kept"}

    def test_long_body_strings_keep_their_length_only(self, dev):
        rec = tc.recorder_for(TOKEN)
        avatar = "data:image/png;base64," + "A" * 10000
        rec.request("PATCH", f"{API_BASE}/users/@me", {}, body={"avatar": avatar}, has_body=True)
        assert read_events(dev)[-1]["json"] == {"avatar": f"<string len={len(avatar)}>"}

    def test_errors_lose_the_token_and_proxy_credentials(self, dev):
        rec = tc.recorder_for(TOKEN)
        rid = rec.request("GET", f"{API_BASE}/users/@me", {})
        rec.failed(rid, OSError(f"proxy http://user:pass@1.2.3.4:8080 refused {TOKEN}"))
        text = read_events(dev)[-1]["errorText"]
        assert "user:pass" not in text and TOKEN not in text and "***@1.2.3.4" in text


class TestRecorder:
    def test_one_file_per_token_named_by_its_hash(self, dev):
        rec = tc.recorder_for(TOKEN)
        assert tc.recorder_for(TOKEN) is rec
        other = tc.recorder_for(TOKEN + "x")
        assert other is not rec
        names = [p.parent.name for p in capture_files(dev)]
        assert len(names) == 2
        assert any(n.endswith("-" + selfbot_headers._token_log_id(TOKEN)) for n in names)
        assert all(len(n.split("-")[0]) == 8 and len(n.split("-")[1]) == 6 for n in names)

    def test_write_failure_does_not_raise(self, dev, monkeypatch):
        rec = tc.recorder_for(TOKEN)
        rec.stop("test")
        monkeypatch.setattr(tc, "open", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")), raising=False)
        rec.ws_sent("W1", {"op": 1, "d": None})
        rec.request("GET", f"{API_BASE}/users/@me", {})

    def test_stop_writes_capture_stop_with_counts(self, dev):
        rec = tc.recorder_for(TOKEN)
        wid = rec.ws_open(GATEWAY + "?v=9", {})
        rec.ws_sent(wid, {"op": 2, "d": {"token": TOKEN}})
        tc.stop(TOKEN, "client closed")
        last = read_events(dev)[-1]
        assert last["type"] == "capture_stop" and last["reason"] == "client closed"
        assert last["counts"]["identify"] == 1 and last["counts"]["wsSent"] == 1


# the synthetic sample for report.mjs


@pytest.mark.asyncio
async def test_fixture_session(dev):
    """Login, IDENTIFY with heartbeats, op 7, RESUME on the resume host, REST, clean close."""
    session = FakeSession()
    first = FakeWS([hello(120)], auto_ack=True)
    second = FakeWS([hello(120), {"op": 0, "t": "RESUMED", "s": 4, "d": {"_trace": ["gw"]}}], auto_ack=True)
    dialer = Dialer(first, second)
    client = Client(
        http=HTTPClient(TOKEN, session=session),
        connector=dialer,
        refresh_build=False,
        gateway_factory=functools.partial(GatewayClient, base_reconnect_delay=0.01, max_reconnect_delay=0.02),
    )
    await client.login(TOKEN)
    task = asyncio.ensure_future(client.connect())
    await wait_until(lambda: 2 in first.ops())
    first.feed(ready())
    await wait_until(lambda: len(session.science_posts()) >= 1)
    first.feed(
        {
            "op": 0,
            "t": "SESSIONS_REPLACE",
            "s": 2,
            "d": [{"session_id": "sess-capture-1", "status": "online", "active": True,
                   "client_info": {"client": "web", "os": "windows", "version": 0}, "activities": []}],
        }
    )
    await wait_until(lambda: first.ops().count(40) >= 3)
    first.feed({"op": 0, "t": "MESSAGE_CREATE", "s": 3, "d": {"id": "1", "channel_id": CHANNEL}})
    await wait_until(lambda: first.ops().count(40) >= 4)
    first.feed({"op": 7, "d": None})
    await wait_until(lambda: 6 in second.ops() and len(session.science_posts()) >= 2)
    await wait_until(lambda: second.ops().count(40) >= 2)
    await client.http.send_message(CHANNEL, "hello from the fixture", guild_id=GUILD)
    with pytest.raises(Exception):
        await client.http.get("/users/@me/affinities/users")
    await client.gateway.close()
    await asyncio.wait_for(task, timeout=3.0)
    await client.close()

    assert dialer.urls[1].startswith(RESUME + "/?")
    events = read_events(dev)
    seen = types(events)
    assert seen[0] == "capture_start" and seen[-1] == "capture_stop"
    for needed in ("request", "request_headers", "request_body", "response", "request_failed",
                   "ws_created", "ws_handshake_request", "ws_sent", "ws_recv", "ws_closed"):
        assert needed in seen
    sent_ops = [e["op"] for e in events if e["type"] == "ws_sent"]
    assert sent_ops[:4] == [2, 4, 41, 40] and 6 in sent_ops and 37 in sent_ops
    text = capture_files(dev)[0].read_text(encoding="utf-8")
    assert TOKEN not in text and ANALYTICS_TOKEN not in text

    if os.environ.get("COPYCORD_WRITE_CAPTURE_FIXTURE") == "1":
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(capture_files(dev)[0], FIXTURE)


def test_committed_fixture_is_clean():
    """The sample in tests/fixtures is a Copycord capture with nothing secret in it."""
    if not FIXTURE.exists():
        pytest.skip("fixture not written yet")
    lines = FIXTURE.read_text(encoding="utf-8").splitlines()
    events = [json.loads(line) for line in lines]
    assert events[0]["type"] == "capture_start" and events[0]["tool"] == "copycord"
    assert events[-1]["type"] == "capture_stop"
    text = "\n".join(lines)
    assert TOKEN not in text and ANALYTICS_TOKEN not in text
    for e in events:
        for name, value in (e.get("headers") or {}).items():
            if name.lower() in tc.SECRET_HEADERS:
                assert value.startswith("<redacted len=")
