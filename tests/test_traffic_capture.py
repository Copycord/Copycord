"""common.traffic_capture: the dev-mode events.jsonl in discord-capture's format.

REST runs a real HTTPClient on a fake session and the gateway a real
GatewayClient on fake zlib-stream sockets, as in test_selfclient_http.py,
test_selfclient_gateway.py and test_selfclient_science.py; nothing reaches
Discord.
"""
import asyncio
import json
import zlib

import pytest

from copycord.common import selfbot_headers
from copycord.common import traffic_capture as tc
from copycord.selfclient.gateway import GatewayClient
from copycord.selfclient.http import API_BASE, HTTPClient

TOKEN = "capture-token-not-real-0123456789.abcdef.ghijklmnopqrstuvwxyz"
ANALYTICS_TOKEN = "capture-analytics-token-not-real"
USER_ID = "987654321098765432"
GUILD = "100000000000000001"
CHANNEL = "100000000000000002"
RESUME = "wss://gateway-us-east1-b.discord.gg"
_CLOSE = object()


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    tc.forget_all()
    yield tmp_path
    tc.forget_all()


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
        if path.endswith("/users/@me"):
            return FakeResponse(200, {"id": USER_ID, "username": "someone"})
        if path.endswith(f"/channels/{CHANNEL}/messages"):
            return FakeResponse(200, {"id": "100000000000000003", "channel_id": CHANNEL})
        return FakeResponse(404, {"message": "Unknown", "code": 0})

    async def close(self):
        pass


class FakeWS:
    """A zlib-stream socket: frames fed as dicts go out compressed on one shared stream."""

    def __init__(self, frames=()):
        self.queue = asyncio.Queue()
        self.sent = []
        self.close_code = None
        self.close_reason = ""
        self.closed = False
        self._deflate = zlib.compressobj()
        for frame in frames:
            self.feed(frame)

    def feed(self, frame):
        data = self._deflate.compress(json.dumps(frame).encode()) + self._deflate.flush(zlib.Z_SYNC_FLUSH)
        self.queue.put_nowait(data)

    async def send(self, data):
        self.sent.append(json.loads(data))

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

    async def __call__(self, url, headers, proxy):
        return self.sockets.pop(0)


async def wait_until(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


def hello():
    return {"op": 10, "d": {"heartbeat_interval": 45000, "_trace": ['["gateway-prd-test",{"micros":0}]']}}


def ready():
    return {
        "op": 0,
        "t": "READY",
        "s": 1,
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


async def gateway_session():
    """HELLO and READY on one socket, then a clean close."""
    ws = FakeWS([hello(), ready()])
    client = GatewayClient(TOKEN, connector=Dialer(ws), zlib_stream=True)
    task = asyncio.create_task(client.run(refresh_build=False))
    await wait_until(lambda: 37 in ws.ops())
    await client.close()
    await asyncio.wait_for(task, timeout=3.0)


async def rest_calls():
    http = HTTPClient(TOKEN, session=FakeSession())
    await http.login()
    await http.send_message(CHANNEL, "hello", guild_id=GUILD)


# dev mode off


class TestDevOff:
    @pytest.mark.asyncio
    async def test_rest_and_gateway_write_nothing(self, prod):
        await rest_calls()
        await gateway_session()
        assert tc.recorder_for(TOKEN) is None
        assert not (prod / tc.CAPTURE_DIR).exists()


# dev mode on


class TestRecords:
    @pytest.mark.asyncio
    async def test_types_in_order(self, dev):
        await rest_calls()
        await gateway_session()
        events = read_events(dev)
        # op 3 waits 90 s after READY, so this short session sends none.
        assert types(events) == [
            "capture_start",
            "request",
            "request_headers",
            "response",
            "request",
            "request_headers",
            "request_body",
            "response",
            "ws_created",
            "ws_handshake_request",
            "ws_recv",
            "ws_sent",
            "ws_sent",
            "ws_recv",
            "ws_sent",
            "ws_recv",
            "ws_closed",
        ]
        start = events[0]
        assert start["tool"] == "copycord"
        assert start["account"] == selfbot_headers._token_log_id(TOKEN)
        assert [e["op"] for e in events if e["type"] == "ws_sent"] == [2, 4, 37]


class TestRedaction:
    @pytest.mark.asyncio
    async def test_no_secret_reaches_the_disk(self, dev):
        await rest_calls()
        await gateway_session()
        rec = tc.recorder_for(TOKEN)
        rec.request("POST", f"{API_BASE}/auth/mfa/totp", {"Cookie": "a=b"},
                    body={"ticket": "tkt-secret", "password": "hunter2"}, has_body=True)
        rid = rec.request("GET", f"{API_BASE}/users/@me", {})
        rec.failed(rid, OSError(f"proxy http://user:pass@1.2.3.4:8080 refused {TOKEN}"))
        text = capture_files(dev)[0].read_text(encoding="utf-8")
        for secret in (TOKEN, ANALYTICS_TOKEN, "someone@example.com", "a=b", "tkt-secret", "hunter2",
                       "user:pass"):
            assert secret not in text, secret
        events = [json.loads(line) for line in text.splitlines()]
        auth = {e["headers"]["Authorization"] for e in events
                if e["type"] == "request_headers" and "Authorization" in e["headers"]}
        assert auth == {f"<redacted len={len(TOKEN)}>"}
        identify = next(e for e in events if e.get("op") == 2 and e["type"] == "ws_sent")
        assert identify["d"]["token"] == f"<redacted len={len(TOKEN)}>"
        assert "***@1.2.3.4" in events[-1]["errorText"]
