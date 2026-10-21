"""selfclient.gateway: handshake, op 40 heartbeat, resume, close codes, op 37, zlib-stream.

A fake websocket replays canned frames through the connector. Heartbeat
intervals are tens of milliseconds so those paths run in real time.
"""
import asyncio
import base64
import json
import logging
import zlib
from urllib.parse import parse_qs, urlsplit

import pytest

from copycord.selfclient import gateway as sc_gateway
from copycord.selfclient.errors import ConnectionClosed, LoginFailure
from copycord.selfclient.gateway import (
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
        if self.auto_ack and msg.get("op") == 40:
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


def rest_super_props(token=TOKEN):
    """The decoded X-Super-Properties the REST client would send for ``token``."""
    raw = sc_gateway.fingerprint.build_headers(token)["X-Super-Properties"]
    return json.loads(base64.b64decode(raw))


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


# url


class TestGatewayUrl:
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


# handshake


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
    async def test_ready_subscribes_and_sends_no_time_spent(self):
        ws = FakeWS([hello(), ready(guilds=("1", "2", "3"))])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37))
        await stop(client, task)

        subs = {}
        for frame in ws.frames(37):
            subs.update(frame["d"]["subscriptions"])
        assert set(subs) == {"1", "2", "3"}
        assert not ws.frames(41), "an unfocused tab sends no op 41"
        assert client.session_id == "sess-1"

    @pytest.mark.asyncio
    async def test_fresh_connect_sends_the_browser_order(self):
        ws = FakeWS([hello(20)], auto_ack=True)
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: len(ws.frames(40)) >= 2)
        await stop(client, task)

        ops = ws.ops()
        assert ops[:3] == [2, 4, 40], "op 2, op 4, then op 40; no op 41"
        assert set(ops[2:]) == {40}
        op4 = ws.frames(4)[0]["d"]
        assert list(op4.items()) == [
            ("guild_id", None),
            ("channel_id", None),
            ("self_mute", True),
            ("self_deaf", False),
            ("self_video", False),
            ("flags", 2),
        ]

    @pytest.mark.asyncio
    async def test_identify_matches_the_browser(self):
        ws = FakeWS([hello()])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(2))
        await stop(client, task)

        d = ws.frames(2)[0]["d"]
        assert list(d) == [
            "token",
            "capabilities",
            "properties",
            "presence",
            "compress",
            "client_state",
            "qos_token",
        ]
        assert d["client_state"] == {"guild_versions": {}}
        assert d["qos_token"] == "CgIIAQ=="
        assert list(d["presence"].items()) == [
            ("status", "unknown"),
            ("since", 0),
            ("activities", []),
            ("afk", False),
        ]
        props = d["properties"]
        assert list(props) == [
            "os",
            "browser",
            "device",
            "device_model_name",
            "system_locale",
            "has_client_mods",
            "browser_user_agent",
            "browser_version",
            "os_version",
            "referrer",
            "referring_domain",
            "referrer_current",
            "referring_domain_current",
            "release_channel",
            "client_build_number",
            "client_event_source",
            "client_launch_id",
            "launch_signature",
            "client_app_state",
            "is_fast_connect",
            "gateway_connect_reasons",
            "installation_id",
        ]
        assert props["device_model_name"] == ""
        assert props["client_app_state"] == "unfocused"
        assert "client_heartbeat_session_id" not in props
        assert props["is_fast_connect"] is False
        assert props["gateway_connect_reasons"] == "AppSkeleton"
        rest = sc_gateway.fingerprint.selfbot_headers._fingerprint(TOKEN)["super_props"]
        assert props["launch_signature"] == rest["launch_signature"]


# presence


class TestPresence:
    def test_online_after_90_seconds_by_default(self):
        client = GatewayClient(TOKEN)
        assert client.status == "online"
        assert client.presence_delay == sc_gateway.PRESENCE_DELAY == 90.0

    @pytest.mark.asyncio
    async def test_one_op3_a_while_after_ready(self):
        ws = FakeWS([hello(), ready()])
        client, _ = make_client(ws, presence_delay=0.2)
        loop = asyncio.get_running_loop()
        started = loop.time()
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37))
        assert not ws.frames(3), "no op 3 on READY"
        await wait_until(lambda: ws.frames(3))
        assert loop.time() - started >= 0.2
        await asyncio.sleep(0.3)
        await stop(client, task)

        assert len(ws.frames(3)) == 1, "op 3 goes out once"
        assert ws.ops()[:3] == [2, 4, 37]
        assert list(ws.frames(3)[0]["d"].items()) == [
            ("status", "online"),
            ("since", 0),
            ("activities", []),
            ("afk", True),
        ]

    @pytest.mark.asyncio
    async def test_the_configured_status_and_activities_never_idle(self):
        ws = FakeWS([hello(), ready()])
        client, _ = make_client(ws, presence_delay=0.01, status="dnd", custom_status="hi")
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(3))
        await stop(client, task)

        (d,) = (f["d"] for f in ws.frames(3))
        assert d["status"] == "dnd" and d["since"] == 0 and d["afk"] is True
        assert d["activities"] == [
            {"type": 4, "state": "hi", "name": "Custom Status", "id": "custom"}
        ]

    @pytest.mark.asyncio
    async def test_a_socket_gone_before_the_delay_sends_none(self):
        first = FakeWS([hello(), ready(), {"op": 7, "d": None}])
        second = FakeWS([hello(), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}])
        client, _ = make_client(first, second, presence_delay=0.1)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(37) and client.is_connected)
        await asyncio.sleep(0.25)
        await stop(client, task)

        assert not first.frames(3)
        # Only the RESUME's own op 3, right after op 6.
        assert len(second.frames(3)) == 1 and second.ops()[1] == 3

    @pytest.mark.asyncio
    async def test_no_op3_on_a_closed_client(self):
        ws = FakeWS([hello(), ready()])
        client, _ = make_client(ws, presence_delay=0.1)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37))
        await stop(client, task)
        await asyncio.sleep(0.2)
        assert not ws.frames(3)
        assert client._presence_task is None


# heartbeat


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_sent_on_interval(self):
        ws = FakeWS(
            [hello(20), {"op": 0, "t": "SOMETHING", "s": 5, "d": {}}], auto_ack=True
        )
        client, dialer = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: len(ws.frames(40)) >= 3)
        await stop(client, task)

        assert ws.frames(40)[-1]["d"]["seq"] == 5, "heartbeat carries the last sequence"
        assert not ws.frames(1), "the web client sends no op 1"
        assert len(dialer.urls) == 1, "an acked connection must not reconnect"
        assert client.latency != float("inf")

    @pytest.mark.asyncio
    async def test_qos_heartbeat_foregrounded_first_then_backgrounded(self):
        ws = FakeWS([hello(20)], auto_ack=True)
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: len(ws.frames(40)) >= 3)
        await stop(client, task)

        first, *later = ws.frames(40)
        assert first == {
            "op": 40,
            "d": {"seq": None, "qos": {"active": True, "ver": 31, "reasons": ["foregrounded"]}},
        }
        assert list(first["d"]) == ["seq", "qos"]
        assert list(first["d"]["qos"]) == ["active", "ver", "reasons"]
        for beat in later:
            assert beat["d"]["qos"] == {"active": False, "ver": 31, "reasons": []}

    @pytest.mark.asyncio
    async def test_no_heartbeat_session_in_31_minutes(self):
        ws = FakeWS([hello(), ready()], auto_ack=True)
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37))

        # Move the loop clock 31 minutes ahead, one 45 s heartbeat at a time,
        # so every timer due in that span fires.
        loop = asyncio.get_running_loop()
        real_time = loop.time
        offset = 0.0
        loop.time = lambda: real_time() + offset
        try:
            for _ in range(42):
                beats = len(ws.frames(40))
                offset += 45.0
                for _ in range(500):
                    if len(ws.frames(40)) > beats:
                        break
                    await asyncio.sleep(0.001)
        finally:
            del loop.time
        await stop(client, task)

        assert len(ws.frames(40)) >= 42, "31 minutes of heartbeats went by"
        assert not ws.frames(41), "an unfocused tab never sends op 41"
        assert not hasattr(client, "heartbeat_session_id")
        assert "client_heartbeat_session_id" not in rest_super_props()

    @pytest.mark.asyncio
    async def test_a_fresh_identify_sends_no_op41(self):
        first = FakeWS([hello(), ready(), {"op": 9, "d": False}])
        second = FakeWS([hello(20)], auto_ack=True)
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(40))
        await stop(client, task)

        assert second.ops()[:3] == [2, 4, 40]
        assert not first.frames(41) and not second.frames(41)
        assert "client_heartbeat_session_id" not in rest_super_props()

    @pytest.mark.asyncio
    async def test_server_heartbeat_request_is_answered(self):
        ws = FakeWS([hello(), {"op": 1, "d": None}])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(40))
        await stop(client, task)
        assert not ws.frames(1)

    @pytest.mark.asyncio
    async def test_zombie_reconnects_when_no_ack(self):
        first = FakeWS([hello(20)])
        second = FakeWS([hello(20)])
        client, dialer = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(40))
        await stop(client, task)

        assert first.closed and first.close_code == RESUME_CLOSE_CODE
        assert len(first.frames(40)) >= 2
        assert len(dialer.urls) >= 2
        assert second.frames(40)[0]["d"]["qos"]["active"] is True, "each socket starts foregrounded"


# resume


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
    async def test_resume_sends_the_browser_order(self):
        first = FakeWS([hello(), ready(guilds=("10", "20")), {"op": 7, "d": None}])
        second = FakeWS([hello(), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(37) and client.is_connected)
        await asyncio.sleep(0.05)
        await stop(client, task)

        ops = second.ops()
        assert ops[:4] == [6, 3, 4, 37], "op 6, op 3, op 4, op 37; op 40 waits"
        assert set(ops[4:]) <= {40}
        assert list(second.sent[1]["d"].items()) == [
            ("status", "online"),
            ("since", 0),
            ("activities", []),
            ("afk", True),
        ]
        assert list(second.sent[2]["d"].items()) == list(sc_gateway.IDLE_VOICE_STATE.items())
        assert not first.frames(41) and not second.frames(41), "no op 41, fresh or RESUME"

    @pytest.mark.asyncio
    async def test_resume_op37_has_the_full_shape(self):
        first = FakeWS([hello(), ready(guilds=("10", "20", "30")), {"op": 7, "d": None}])
        second = FakeWS([hello(), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(37) and client.is_connected)
        await asyncio.sleep(0.05)
        await stop(client, task)

        (op37,) = second.frames(37)
        subs = op37["d"]["subscriptions"]
        assert list(subs) == ["10", "20", "30"]
        for features in subs.values():
            assert list(features.items()) == [
                ("typing", True),
                ("threads", True),
                ("activities", True),
                ("members", []),
                ("member_updates", False),
                ("channels", {}),
                ("thread_member_lists", []),
            ]
        # Fresh connect keeps the short shape.
        for features in first.frames(37)[0]["d"]["subscriptions"].values():
            assert list(features) == ["typing", "activities", "threads"]

    @pytest.mark.asyncio
    async def test_resume_heartbeats_follow_as_backgrounded(self):
        first = FakeWS([hello(), ready(), {"op": 7, "d": None}])
        second = FakeWS([hello(50), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}], auto_ack=True)
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: len(second.frames(40)) >= 2)
        await stop(client, task)

        ops = second.ops()
        assert ops[:4] == [6, 3, 4, 37]
        assert set(ops[4:]) == {40}
        for beat in second.frames(40):
            assert beat["d"]["qos"] == {"active": False, "ver": 31, "reasons": []}

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


# close codes


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

    @pytest.mark.parametrize("code", [4010, 4014])
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


# proxy


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


# op 37 batching


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
                assert list(features.items()) == [
                    ("typing", True),
                    ("activities", True),
                    ("threads", True),
                ]
        assert sorted(seen) == sorted(ids)
        assert len(seen) == len(set(seen)), "a guild must appear in exactly one frame"

    def test_batches_stay_under_byte_cap(self):
        for frame in build_subscription_frames(_snowflakes(1000)):
            assert len(json.dumps(frame["d"])) <= MAX_SUBSCRIPTION_PAYLOAD
        full = sc_gateway.RESUME_SUBSCRIPTION_FEATURES
        frames = build_subscription_frames(_snowflakes(1000), full)
        assert len(frames) > 1
        for frame in frames:
            assert len(json.dumps(frame["d"])) <= MAX_SUBSCRIPTION_PAYLOAD

    def test_resume_shape_lists_are_not_shared(self):
        (frame,) = build_subscription_frames(["1", "2"], sc_gateway.RESUME_SUBSCRIPTION_FEATURES)
        subs = frame["d"]["subscriptions"]
        assert subs["1"]["members"] is not subs["2"]["members"]
        assert subs["1"]["members"] is not sc_gateway.RESUME_SUBSCRIPTION_FEATURES["members"]

    def test_dedupes_and_skips_empty(self):
        frames = build_subscription_frames(["1", 1, None, "", "2", "1"])
        assert len(frames) == 1
        assert list(frames[0]["d"]["subscriptions"]) == ["1", "2"]

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


# zlib-stream


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


# dispatch


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
        await client.request_guild_members("1", query="a", limit=10, presences=True)
        await client.request_guild_members("1", user_ids=["7", "8"])
        await wait_until(lambda: len(ws.frames(8)) == 2)
        await stop(client, task)
        search, by_ids = (frame["d"] for frame in ws.frames(8))
        # Chrome's two op 8 shapes, key order included, never a nonce.
        assert list(search.items()) == [
            ("guild_id", ["1"]),
            ("query", "a"),
            ("limit", 10),
            ("presences", True),
        ]
        assert list(by_ids.items()) == [
            ("guild_id", ["1"]),
            ("user_ids", ["7", "8"]),
            ("presences", False),
        ]


# debug log of sent frames


def _sent_lines(caplog):
    return [
        r.getMessage()[len("sent "):]
        for r in caplog.records
        if r.name == "selfclient.gateway" and r.getMessage().startswith("sent ")
    ]


class TestSentFrameLog:
    def test_summaries_name_the_op_and_its_fields(self):
        summary = sc_gateway.frame_summary
        assert summary({"op": 3, "d": {"status": "online", "afk": True, "activities": []}}) == (
            "op 3 presence status=online afk=True activities=0"
        )
        assert summary({"op": 4, "d": dict(sc_gateway.IDLE_VOICE_STATE)}) == (
            "op 4 voice guild=None channel=None flags=2"
        )
        assert summary(
            {"op": 8, "d": {"guild_id": ["1"], "query": "ab", "limit": 10, "presences": True}}
        ) == "op 8 members guilds=1 query='ab' limit=10 presences=True"
        assert summary(
            {"op": 8, "d": {"guild_id": ["1"], "user_ids": ["7", "8"], "presences": False}}
        ) == "op 8 members guilds=1 user_ids=2 presences=False"
        assert summary(build_subscription_frames(["1", "2", "3"])[0]) == "op 37 subscribe guilds=3"
        assert summary(
            {"op": 40, "d": {"seq": 7, "qos": {"active": False, "ver": 31, "reasons": []}}}
        ) == "op 40 heartbeat seq=7 active=False"
        assert summary({"op": 99}) == "op 99"

    def test_identify_and_resume_summaries_leave_out_the_token(self):
        identify = {
            "op": 2,
            "d": {
                "token": TOKEN,
                "capabilities": CAPABILITIES,
                "properties": {
                    "os": "Windows",
                    "browser_user_agent": "Mozilla/5.0 secret-ua",
                    "client_build_number": 123,
                    "os_version": "10",
                    "client_app_state": "unfocused",
                },
            },
        }
        line = sc_gateway.frame_summary(identify)
        assert line == "op 2 IDENTIFY build=123 os=10 app_state=unfocused"
        resume = {"op": 6, "d": {"token": TOKEN, "session_id": "s", "seq": 4}}
        assert sc_gateway.frame_summary(resume) == "op 6 RESUME session=s seq=4"
        assert TOKEN not in line + sc_gateway.frame_summary(resume)

    @pytest.mark.asyncio
    async def test_every_sent_frame_logs_once_in_order(self, caplog):
        caplog.set_level(logging.DEBUG, logger="selfclient.gateway")
        ws = FakeWS([hello(20), ready(guilds=("1", "2", "3"))], auto_ack=True)
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37) and ws.frames(40))
        await client.request_guild_members("1", query="a", limit=10)
        await wait_until(lambda: ws.frames(8))
        await stop(client, task)

        lines = _sent_lines(caplog)
        assert [int(line.split()[1]) for line in lines] == ws.ops()
        assert lines == [sc_gateway.frame_summary(m) for m in ws.sent]
        assert ws.ops()[:2] == [2, 4] and 41 not in ws.ops()
        assert lines[0].endswith("app_state=unfocused")
        assert any(line.startswith("op 40 heartbeat seq=") for line in lines)
        assert "op 37 subscribe guilds=3" in lines
        assert "op 8 members guilds=1 query='a' limit=10 presences=False" in lines
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert TOKEN not in text
        assert "browser_user_agent" not in text
        for old in ("heartbeat sent", "sending IDENTIFY", "sending RESUME", "sending OP 41"):
            assert old not in text

    @pytest.mark.asyncio
    async def test_resume_logs_session_and_seq(self, caplog):
        caplog.set_level(logging.DEBUG, logger="selfclient.gateway")
        ws = FakeWS([hello()])
        client, _ = make_client(ws)
        client.session_id, client.sequence = "sess-9", 12
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(6))
        await stop(client, task)
        assert _sent_lines(caplog)[0] == "op 6 RESUME session=sess-9 seq=12"
        assert TOKEN not in caplog.text

    @pytest.mark.asyncio
    async def test_resume_logs_one_line_per_frame(self, caplog):
        caplog.set_level(logging.DEBUG, logger="selfclient.gateway")
        first = FakeWS([hello(), ready(guilds=("1", "2")), {"op": 7, "d": None}])
        second = FakeWS([hello(), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(37) and client.is_connected)
        await stop(client, task)

        lines = _sent_lines(caplog)
        assert lines == [sc_gateway.frame_summary(m) for m in first.sent + second.sent]
        assert lines[len(first.sent):][:4] == [
            "op 6 RESUME session=sess-1 seq=1",
            "op 3 presence status=online afk=True activities=0",
            "op 4 voice guild=None channel=None flags=2",
            "op 37 subscribe guilds=2",
        ]

    @pytest.mark.asyncio
    async def test_a_send_on_a_closed_socket_logs_no_sent_line(self, caplog):
        caplog.set_level(logging.DEBUG, logger="selfclient.gateway")

        class ClosedWS(FakeWS):
            async def send(self, data):
                raise sc_gateway._WSClosed.__new__(sc_gateway._WSClosed)

        client, _ = make_client()
        client._ws = ClosedWS()
        await client._send({"op": 3, "d": {}})
        await client._send_bypass(client._heartbeat_payload())
        assert _sent_lines(caplog) == []
        assert "tried to send on a closed socket" in caplog.text
        assert "tried to send heartbeat on closed socket" in caplog.text
