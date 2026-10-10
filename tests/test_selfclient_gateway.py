"""
selfclient.gateway: handshake, heartbeat, resume, close codes, op 37, zlib-stream.

Driven against a fake websocket that replays canned frames and records what the
client sends, injected through GatewayClient's ``connector`` so nothing here
touches the network. Heartbeat intervals are tens of milliseconds so the
heartbeat and zombie paths run in real time without slowing the suite.
"""
import asyncio
import json
import zlib
from urllib.parse import parse_qs, urlsplit

import pytest

from selfclient import gateway as sc_gateway
from selfclient.errors import ConnectionClosed, LoginFailure
from selfclient.gateway import (
    CAPABILITIES,
    MAX_SUBSCRIPTION_PAYLOAD,
    RESUME_CLOSE_CODE,
    GatewayClient,
    ZlibStreamInflater,
    build_subscription_frames,
    gateway_url,
)

TOKEN = "test-token-not-real"
_CLOSE = object()


class FakeWS:
    """A websocket that yields queued frames and records sent ones."""

    def __init__(self, frames=(), *, auto_ack=False):
        self.queue = asyncio.Queue()
        self.sent = []
        self.close_code = None
        self.close_reason = ""
        self.closed = False
        self.auto_ack = auto_ack
        for frame in frames:
            self.feed(frame)

    def feed(self, frame):
        if isinstance(frame, dict):
            frame = json.dumps(frame)
        self.queue.put_nowait(frame)

    def server_close(self, code, reason=""):
        """Queue a close from Discord's side, after whatever is already queued."""
        self.queue.put_nowait(("close", code, reason))

    async def send(self, data):
        msg = json.loads(data)
        self.sent.append(msg)
        if self.auto_ack and msg.get("op") == 1:
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
        if isinstance(item, tuple) and item[0] == "close":
            if not self.closed:
                self.closed = True
                self.close_code, self.close_reason = item[1], item[2]
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item

    def ops(self):
        return [m.get("op") for m in self.sent]

    def frames(self, op):
        return [m for m in self.sent if m.get("op") == op]


class Dialer:
    """The ``connector``: hands out scripted sockets, then stops the client."""

    def __init__(self, *sockets):
        self.sockets = list(sockets)
        self.urls = []
        self.headers = []
        self.proxies = []
        self.client = None

    async def __call__(self, url, headers, proxy):
        self.urls.append(url)
        self.headers.append(headers)
        self.proxies.append(proxy)
        if not self.sockets:
            await self.client.close()
            raise ConnectionRefusedError("no more scripted sockets")
        item = self.sockets.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def make_client(*sockets, **kwargs):
    dialer = Dialer(*sockets)
    kwargs.setdefault("base_reconnect_delay", 0.001)
    kwargs.setdefault("max_reconnect_delay", 0.01)
    client = GatewayClient(TOKEN, connector=dialer, **kwargs)
    client._invalid_session_delay = (0.0, 0.0)
    dialer.client = client
    return client, dialer


async def wait_until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


async def stop(client, task):
    await client.close()
    await asyncio.wait_for(task, timeout=3.0)


def hello(interval_ms=45000):
    return {"op": 10, "d": {"heartbeat_interval": interval_ms}}


def ready(session="sess-1", resume="wss://resume.example.gg", guilds=("1", "2"), seq=1):
    return {
        "op": 0,
        "t": "READY",
        "s": seq,
        "d": {
            "session_id": session,
            "resume_gateway_url": resume,
            "user": {"id": "42", "username": "someone"},
            "guilds": [{"id": g} for g in guilds],
        },
    }


@pytest.fixture
def no_jitter(monkeypatch):
    """First heartbeat right away instead of at interval * random()."""
    monkeypatch.setattr(sc_gateway.random, "random", lambda: 0.0)


# ---------------------------------------------------------------------------
# url
# ---------------------------------------------------------------------------


class TestGatewayUrl:
    def test_default_query(self):
        q = parse_qs(urlsplit(gateway_url()).query)
        assert q == {"encoding": ["json"], "v": ["9"]}

    def test_zlib_stream_query(self):
        q = parse_qs(urlsplit(gateway_url(zlib_stream=True)).query)
        assert q["compress"] == ["zlib-stream"]

    def test_resume_url_gets_query_reapplied(self):
        url = gateway_url("wss://gateway-us-east1-b.discord.gg", zlib_stream=True)
        parts = urlsplit(url)
        assert parts.netloc == "gateway-us-east1-b.discord.gg"
        assert parse_qs(parts.query) == {
            "encoding": ["json"],
            "v": ["9"],
            "compress": ["zlib-stream"],
        }

    def test_existing_query_is_replaced(self):
        url = gateway_url("wss://resume.example.gg/?v=6&encoding=etf")
        assert parse_qs(urlsplit(url).query) == {"encoding": ["json"], "v": ["9"]}


# ---------------------------------------------------------------------------
# handshake
# ---------------------------------------------------------------------------


class TestHandshake:
    @pytest.mark.asyncio
    async def test_hello_then_identify(self):
        ws = FakeWS([hello()])
        client, dialer = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(2))
        await stop(client, task)

        assert ws.sent[0]["op"] == 2, "IDENTIFY must be the first frame after HELLO"
        d = ws.sent[0]["d"]
        assert d["token"] == TOKEN
        assert d["capabilities"] == CAPABILITIES == 1734653
        assert d["compress"] is False
        assert d["properties"]["client_build_number"]
        assert d["properties"]["client_launch_id"]
        assert dialer.headers[0]["Origin"] == "https://discord.com"
        assert "Chrome/" in dialer.headers[0]["User-Agent"]
        q = parse_qs(urlsplit(dialer.urls[0]).query)
        assert q == {"encoding": ["json"], "v": ["9"], "compress": ["zlib-stream"]}

    @pytest.mark.asyncio
    async def test_plain_json_fallback_omits_compress(self):
        ws = FakeWS([hello()])
        client, dialer = make_client(ws, zlib_stream=False)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(2))
        await stop(client, task)
        assert "compress" not in parse_qs(urlsplit(dialer.urls[0]).query)

    @pytest.mark.asyncio
    async def test_ready_subscribes_and_sends_time_spent(self):
        ws = FakeWS([hello(), ready(guilds=("1", "2", "3"))])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37) and ws.frames(41))
        await stop(client, task)

        subs = {}
        for frame in ws.frames(37):
            subs.update(frame["d"]["subscriptions"])
        assert set(subs) == {"1", "2", "3"}
        op41 = ws.frames(41)[0]["d"]
        assert op41["session_id"] == client.heartbeat_session_id
        assert op41["client_launch_id"]
        assert client.session_id == "sess-1"


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_sent_on_interval(self, no_jitter):
        ws = FakeWS(
            [hello(20), {"op": 0, "t": "SOMETHING", "s": 5, "d": {}}], auto_ack=True
        )
        client, dialer = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: len(ws.frames(1)) >= 3)
        await stop(client, task)

        assert ws.frames(1)[-1]["d"] == 5, "heartbeat carries the last sequence"
        assert len(dialer.urls) == 1, "an acked connection must not reconnect"
        assert client.latency != float("inf")

    @pytest.mark.asyncio
    async def test_server_heartbeat_request_is_answered(self):
        ws = FakeWS([hello(), {"op": 1, "d": None}])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(1))
        await stop(client, task)

    @pytest.mark.asyncio
    async def test_zombie_reconnects_when_no_ack(self, no_jitter):
        first = FakeWS([hello(20)])  # never acks
        second = FakeWS([hello()])
        client, dialer = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(2))
        await stop(client, task)

        assert first.closed and first.close_code == RESUME_CLOSE_CODE
        assert len(first.frames(1)) >= 2
        assert len(dialer.urls) >= 2


# ---------------------------------------------------------------------------
# resume
# ---------------------------------------------------------------------------


class TestResume:
    @pytest.mark.asyncio
    async def test_resume_uses_resume_url_and_replays_op37(self):
        first = FakeWS([hello(), ready(guilds=("10", "20")), {"op": 7, "d": None}])
        second = FakeWS([hello(), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}])
        client, dialer = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(37))
        await stop(client, task)

        assert first.close_code == RESUME_CLOSE_CODE, "reconnect must close with 4000"
        parts = urlsplit(dialer.urls[1])
        assert parts.netloc == "resume.example.gg"
        assert parse_qs(parts.query)["v"] == ["9"]
        assert parse_qs(parts.query)["encoding"] == ["json"]

        assert second.sent[0]["op"] == 6, "RESUME, not IDENTIFY"
        assert not second.frames(2)
        assert second.sent[0]["d"] == {"token": TOKEN, "session_id": "sess-1", "seq": 1}

        subs = {}
        for frame in second.frames(37):
            subs.update(frame["d"]["subscriptions"])
        assert set(subs) == {"10", "20"}
        for features in subs.values():
            assert features["typing"] is True and features["threads"] is True

    @pytest.mark.asyncio
    async def test_non_resumable_invalid_session_identifies_fresh(self):
        first = FakeWS([hello(), ready(), {"op": 9, "d": False}])
        second = FakeWS([hello()])
        client, dialer = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(2))
        await stop(client, task)

        assert not second.frames(6)
        assert urlsplit(dialer.urls[1]).netloc == "gateway.discord.gg"

    @pytest.mark.asyncio
    async def test_session_timeout_close_starts_fresh(self):
        first = FakeWS([hello(), ready()])
        first.server_close(4009, "Session timed out")
        second = FakeWS([hello()])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(2))
        await stop(client, task)
        assert not second.frames(6)


# ---------------------------------------------------------------------------
# close codes
# ---------------------------------------------------------------------------


class TestCloseCodes:
    @pytest.mark.asyncio
    async def test_4004_is_fatal_no_resume(self):
        first = FakeWS([hello(), ready()])
        first.server_close(4004, "Authentication failed.")
        second = FakeWS([hello()])
        client, dialer = make_client(first, second)

        with pytest.raises(LoginFailure):
            await asyncio.wait_for(client.run(refresh_build=False), timeout=3.0)

        assert client.fatal_close == (4004, "Authentication failed.")
        assert len(dialer.urls) == 1, "nothing reconnects after 4004"
        assert not second.sent
        assert not first.frames(6)

    @pytest.mark.parametrize("code", [4010, 4011, 4012, 4013, 4014])
    @pytest.mark.asyncio
    async def test_other_fatal_codes_raise_connection_closed(self, code):
        first = FakeWS([hello()])
        first.server_close(code, "nope")
        client, dialer = make_client(first, FakeWS([hello()]))

        with pytest.raises(ConnectionClosed) as info:
            await asyncio.wait_for(client.run(refresh_build=False), timeout=3.0)

        assert info.value.code == code
        assert len(dialer.urls) == 1

    @pytest.mark.asyncio
    async def test_4004_via_websockets_exception(self):
        frames = pytest.importorskip("websockets.frames")
        exceptions = pytest.importorskip("websockets.exceptions")
        first = FakeWS([hello()])
        first.feed(
            exceptions.ConnectionClosedError(frames.Close(4004, "Authentication failed."), None)
        )
        client, dialer = make_client(first, FakeWS([hello()]))

        with pytest.raises(LoginFailure):
            await asyncio.wait_for(client.run(refresh_build=False), timeout=3.0)
        assert len(dialer.urls) == 1

    @pytest.mark.asyncio
    async def test_non_fatal_close_resumes(self):
        first = FakeWS([hello(), ready()])
        first.server_close(4000, "Unknown error")
        second = FakeWS([hello()])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.sent)
        await stop(client, task)
        assert second.sent[0]["op"] == 6


# ---------------------------------------------------------------------------
# proxy
# ---------------------------------------------------------------------------


class FakePool:
    enabled = True

    def __init__(self, proxies):
        self.proxies = list(proxies)
        self.failures = 0
        self.successes = []

    async def lease(self, token):
        return self.proxies[0]

    async def report_failure(self, token):
        self.failures += 1
        self.proxies.pop(0)
        return self.proxies[0]

    def report_success(self, proxy):
        self.successes.append(proxy)


class TestProxy:
    @pytest.mark.asyncio
    async def test_dial_failure_rotates_proxy(self):
        pool = FakePool(["http://a:1", "http://b:2"])
        ws = FakeWS([hello(), ready()])
        client, dialer = make_client(OSError("proxy refused"), ws, proxy_pool=pool)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37))
        await stop(client, task)

        assert dialer.proxies[:2] == ["http://a:1", "http://b:2"]
        assert pool.failures == 1
        assert pool.successes == ["http://b:2"]

    @pytest.mark.asyncio
    async def test_discord_close_does_not_rotate(self):
        pool = FakePool(["http://a:1", "http://b:2"])
        first = FakeWS([hello(), ready(), {"op": 7, "d": None}])
        second = FakeWS([hello()])
        client, dialer = make_client(first, second, proxy_pool=pool)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.sent)
        await stop(client, task)
        assert pool.failures == 0
        assert dialer.proxies[:2] == ["http://a:1", "http://a:1"]

    def test_mask_proxy_hides_credentials(self):
        masked = sc_gateway.mask_proxy("http://user:secret@host.example:8080")
        assert "secret" not in masked and "user" not in masked
        assert masked.endswith("host.example:8080")


# ---------------------------------------------------------------------------
# op 37 batching
# ---------------------------------------------------------------------------


def _snowflakes(n):
    return [str(100000000000000000 + i) for i in range(n)]


class TestSubscriptionFrames:
    def test_every_guild_once_with_typing_and_threads_together(self):
        ids = _snowflakes(600)
        frames = build_subscription_frames(ids)
        assert len(frames) > 1, "600 guilds must not fit one frame"

        seen = []
        for frame in frames:
            assert frame["op"] == 37
            for gid, features in frame["d"]["subscriptions"].items():
                seen.append(gid)
                # never split per guild: both flags in the one frame it is in
                assert features["typing"] is True
                assert features["threads"] is True
                assert features["activities"] is True
                assert features["member_updates"] is True
        assert sorted(seen) == sorted(ids)
        assert len(seen) == len(set(seen)), "a guild must appear in exactly one frame"

    def test_batches_stay_under_byte_cap(self):
        for frame in build_subscription_frames(_snowflakes(1000)):
            assert len(json.dumps(frame["d"])) <= MAX_SUBSCRIPTION_PAYLOAD

    def test_batches_are_packed(self):
        frames = build_subscription_frames(_snowflakes(1000))
        # Each frame but the last is full: adding the next guild would overflow.
        for frame, nxt in zip(frames, frames[1:]):
            subs = dict(frame["d"]["subscriptions"])
            extra = next(iter(nxt["d"]["subscriptions"]))
            subs[extra] = dict(sc_gateway.SUBSCRIPTION_FEATURES)
            assert len(json.dumps({"subscriptions": subs})) > MAX_SUBSCRIPTION_PAYLOAD

    def test_dedupes_and_skips_empty(self):
        frames = build_subscription_frames(["1", 1, None, "", "2", "1"])
        assert len(frames) == 1
        assert list(frames[0]["d"]["subscriptions"]) == ["1", "2"]

    def test_empty_input_sends_nothing(self):
        assert build_subscription_frames([]) == []

    @pytest.mark.asyncio
    async def test_guild_create_subscribes_only_new_guilds(self):
        ws = FakeWS(
            [
                hello(),
                ready(guilds=("1",)),
                {"op": 0, "t": "GUILD_CREATE", "s": 2, "d": {"id": "1"}},
                {"op": 0, "t": "GUILD_CREATE", "s": 3, "d": {"id": "2"}},
            ]
        )
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: len(ws.frames(37)) >= 2)
        await asyncio.sleep(0.02)
        await stop(client, task)
        frames = ws.frames(37)
        assert [list(f["d"]["subscriptions"]) for f in frames] == [["1"], ["2"]]


# ---------------------------------------------------------------------------
# zlib-stream
# ---------------------------------------------------------------------------


def _deflate_messages(*payloads):
    """Compress messages on one shared stream, Z_SYNC_FLUSH after each."""
    comp = zlib.compressobj()
    out = []
    for payload in payloads:
        data = comp.compress(json.dumps(payload).encode()) + comp.flush(zlib.Z_SYNC_FLUSH)
        assert data.endswith(sc_gateway.ZLIB_SUFFIX)
        out.append(data)
    return out


class TestZlibStream:
    def test_inflate_canned_frame(self):
        payload = {"op": 10, "d": {"heartbeat_interval": 41250, "_trace": ["gw"]}}
        (frame,) = _deflate_messages(payload)
        assert json.loads(ZlibStreamInflater().feed(frame)) == payload

    def test_message_split_across_frames(self):
        payload = {"op": 0, "t": "MESSAGE_CREATE", "s": 3, "d": {"content": "x" * 5000}}
        (frame,) = _deflate_messages(payload)
        inflater = ZlibStreamInflater()
        mid = len(frame) // 2
        assert inflater.feed(frame[:mid]) is None
        assert json.loads(inflater.feed(frame[mid:])) == payload

    def test_context_is_shared_across_messages(self):
        a = {"op": 10, "d": {"heartbeat_interval": 41250}}
        b = {"op": 11}
        frame_a, frame_b = _deflate_messages(a, b)
        inflater = ZlibStreamInflater()
        assert json.loads(inflater.feed(frame_a)) == a
        assert json.loads(inflater.feed(frame_b)) == b
        # The second message only inflates with the first one's dictionary.
        with pytest.raises(zlib.error):
            ZlibStreamInflater().feed(frame_b)

    @pytest.mark.asyncio
    async def test_client_inflates_compressed_hello(self):
        frames = _deflate_messages(hello())
        ws = FakeWS(frames)
        client, _ = make_client(ws, zlib_stream=True)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(2))
        await stop(client, task)

    @pytest.mark.asyncio
    async def test_corrupt_stream_falls_back_to_plain_json(self):
        bad = FakeWS([b"\x01\x02\x03garbage" + sc_gateway.ZLIB_SUFFIX])
        good = FakeWS([hello()])
        client, dialer = make_client(bad, good, zlib_stream=True)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: good.frames(2))
        await stop(client, task)

        assert client.zlib_stream is False
        assert "compress" not in parse_qs(urlsplit(dialer.urls[1]).query)


# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    @pytest.mark.asyncio
    async def test_handlers_receive_raw_payloads(self):
        msg = {"id": "5", "channel_id": "6", "content": "hi"}
        ws = FakeWS([hello(), ready(), {"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": msg}])
        client, _ = make_client(ws)
        got, every = [], []

        async def on_message(d):
            got.append(d)

        async def on_any(event, d):
            every.append(event)

        client.on("message_create", on_message)
        client.on_dispatch(on_any)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: got)
        await stop(client, task)

        assert got == [msg]
        assert every[:2] == ["READY", "MESSAGE_CREATE"]
        assert client.sequence == 2

    @pytest.mark.asyncio
    async def test_request_guild_members_sends_op8(self):
        ws = FakeWS([hello(), ready()])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: client.session_id)
        await client.request_guild_members("1", query="ab", limit=100, nonce="n1")
        await wait_until(lambda: ws.frames(8))
        await stop(client, task)
        assert ws.frames(8)[0]["d"] == {
            "guild_id": "1",
            "limit": 100,
            "presences": False,
            "query": "ab",
            "nonce": "n1",
        }
