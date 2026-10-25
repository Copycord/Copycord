"""selfclient.science: the /api/v9/science POST Chrome sends after READY.

The key lists below are the ones in a Chrome 155 capture (2026-10-20, the
post-login batch at 16:55:47). A real GatewayClient runs on a fake socket and
a real HTTPClient on a fake session, so nothing reaches Discord.
"""
import asyncio
import json

import pytest

from copycord.selfclient import Client
from copycord.selfclient import science as sc
from copycord.selfclient.fingerprint import gateway_properties
from copycord.selfclient.http import API_BASE, HTTPClient

USER_ID = "987654321098765432"
ANALYTICS_TOKEN = "analytics-token-not-real"

COMMON_TAIL = [
    "client_performance_memory",
    "accessibility_features",
    "rendered_locale",
    "uptime_app",
    "launch_signature",
    "client_rtc_state",
    "client_app_state",
    "client_viewport_width",
    "client_viewport_height",
]

# Without client_heartbeat_session_id: a tab that loaded unfocused has none.
CAPTURE_KEYS = {
    "gateway_connected": [
        "client_track_timestamp",
        "event_sequence_number",
        "num_failed_connect_attempts",
        "gateway_url",
        "is_reconnect",
        "is_fast_connect",
        "duration_ms_since_first_connect_attempt",
        "duration_ms_since_connect_attempt_start",
        *COMMON_TAIL,
        "client_send_timestamp",
    ],
    "ready_payload_received": [
        "client_track_timestamp",
        "event_sequence_number",
        "compressed_byte_size",
        "uncompressed_byte_size",
        "compression_algorithm",
        "packing_algorithm",
        "unpack_duration_ms",
        "identify_total_server_duration_ms",
        "identify_api_duration_ms",
        "identify_guilds_duration_ms",
        "num_guilds",
        "num_guild_channels",
        "num_guild_category_channels",
        "presences_size",
        "users_size",
        "read_states_size",
        "private_channels_size",
        "user_settings_size",
        "experiments_size",
        "user_guild_settings_size",
        "relationships_size",
        "remaining_data_size",
        "guild_channels_size",
        "guild_members_size",
        "guild_presences_size",
        "guild_roles_size",
        "guild_emojis_size",
        "guild_threads_size",
        "guild_stickers_size",
        "guild_events_size",
        "guild_features_size",
        "guild_remaining_data_size",
        "size_metrics_duration_ms",
        "duration_ms_since_identify_start",
        "duration_ms_since_connection_start",
        "duration_ms_since_emit_start",
        "is_reconnect",
        "is_fast_connect",
        "did_force_clear_guild_hashes",
        "identify_uncompressed_byte_size",
        "identify_compressed_byte_size",
        "had_cache_at_startup",
        "used_cache_at_startup",
        *COMMON_TAIL,
        "client_uuid",
        "client_send_timestamp",
    ],
}

TRACE = json.dumps(
    [
        "gateway-prd-us-east1-b-test",
        {
            "micros": 475123,
            "calls": [
                "id_created", {"micros": 1, "calls": []},
                "discord-sessions-prd-test", {
                    "micros": 470000,
                    "calls": [
                        "start_session", {"micros": 316400, "calls": []},
                        "guilds_connect", {"micros": 56900, "calls": []},
                    ],
                },
            ],
        },
    ]
)


def ready_payload():
    return {
        "v": 9,
        "user": {"id": USER_ID, "username": "someone"},
        "session_id": "sess-1",
        "resume_gateway_url": "wss://gateway-us-east1-b.discord.gg",
        "analytics_token": ANALYTICS_TOKEN,
        "users": [{"id": "1", "username": "a"}],
        "read_state": {"entries": [], "version": 1},
        "private_channels": [],
        "user_settings_proto": "CgIIAQ==",
        "experiments": [[1, 0, 1]],
        "user_guild_settings": {"entries": []},
        "relationships": [],
        "presences": [],
        "guilds": [
            {
                "id": "100",
                "channels": [{"id": "101", "type": 4, "name": "cat"}, {"id": "102", "type": 0, "name": "general"}],
                "members": [],
                "presences": [],
                "roles": [{"id": "100", "name": "@everyone", "permissions": "0", "position": 0}],
                "emojis": [],
                "threads": [],
                "stickers": [],
                "guild_scheduled_events": [],
                "features": ["COMMUNITY"],
                "properties": {"name": "g"},
            },
            {"id": "200", "channels": [{"id": "201", "type": 0, "name": "chat"}], "presences": []},
        ],
        "_trace": [TRACE],
    }


class FakeWS:
    """A websocket that yields queued frames and records sent ones."""

    def __init__(self, frames=()):
        self.queue = asyncio.Queue()
        self.sent = []
        self.closed = False
        self.close_code = None
        self.close_reason = ""
        for frame in frames:
            self.queue.put_nowait(json.dumps(frame))

    async def send(self, data):
        self.sent.append(json.loads(data))

    async def close(self, code=1000, reason=""):
        if not self.closed:
            self.closed = True
            self.close_code = code
            self.queue.put_nowait(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.queue.get()
        if item is None:
            raise StopAsyncIteration
        return item


class FakeResponse:
    def __init__(self, status, body=None):
        self.status_code = status
        self.body = body
        self.text = "" if body is None else json.dumps(body)
        self.headers = {}

    def json(self):
        if self.body is None:
            raise ValueError("no json")
        return self.body


class FakeSession:
    """Answers /users/@me and /science; records every request."""

    def __init__(self, science_error=None):
        self.calls = []
        self.science_error = science_error

    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if url.endswith("/science"):
            if self.science_error is not None:
                raise self.science_error
            return FakeResponse(204)
        if url.endswith("/users/@me"):
            return FakeResponse(200, {"id": USER_ID, "username": "someone"})
        return FakeResponse(404, {"message": "Unknown", "code": 0})

    def science_posts(self):
        return [c for c in self.calls if c[1].endswith("/science")]

    async def close(self):
        pass


async def wait_until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


@pytest.fixture
def token(request):
    return f"science-token-{request.node.name}"


async def run_client(token, session, frames):
    """Log in, connect to one fake socket, and return once READY's POST went out."""
    ws = FakeWS(frames)

    async def connector(url, headers, proxy):
        return ws

    client = Client(
        http=HTTPClient(token, session=session),
        connector=connector,
        refresh_build=False,
    )
    await client.login(token)
    task = asyncio.ensure_future(client.connect(reconnect=False))
    await wait_until(lambda: session.science_posts())
    return client, task, ws


async def stop(client, task):
    await client.close()
    await asyncio.wait_for(task, timeout=3.0)


def hello():
    return {"op": 10, "d": {"heartbeat_interval": 45000}}


def ready_frame():
    return {"op": 0, "t": "READY", "s": 1, "d": ready_payload()}


# the first batch


class TestFirstBatch:
    @pytest.mark.asyncio
    async def test_one_post_with_an_unfocused_tabs_events(self, token):
        session = FakeSession()
        client, task, _ = await run_client(token, session, [hello(), ready_frame()])
        await stop(client, task)

        posts = session.science_posts()
        assert len(posts) == 1
        method, url, kwargs = posts[0]
        assert (method, url) == ("POST", f"{API_BASE}/science")
        body = kwargs["json"]
        assert list(body) == ["token", "events"]
        assert body["token"] == ANALYTICS_TOKEN
        events = body["events"]
        # No client_heartbeat and no session_start_client: an unfocused tab sends neither.
        assert [e["type"] for e in events] == [
            "gateway_connected",
            "ready_payload_received",
        ]
        for event in events:
            assert list(event) == ["type", "properties"]
            assert list(event["properties"]) == CAPTURE_KEYS[event["type"]], event["type"]
        assert [e["properties"]["event_sequence_number"] for e in events] == [1, 2]
        for event in events:
            props = event["properties"]
            assert "client_heartbeat_session_id" not in props
            assert props["launch_signature"] == gateway_properties(token)["launch_signature"]
            assert props["client_app_state"] == "unfocused"
            assert props["rendered_locale"] == "en-US"
            assert props["accessibility_features"] == sc.ACCESSIBILITY_FEATURES
            assert props["client_rtc_state"] == "DISCONNECTED"
            assert props["client_track_timestamp"] <= props["client_send_timestamp"]
        assert len({e["properties"]["client_send_timestamp"] for e in events}) == 1


# resume


class TestResume:
    @pytest.mark.asyncio
    async def test_resume_posts_gateway_connected_and_connection_resumed_once(self, token):
        session = FakeSession()
        first = FakeWS([hello(), ready_frame()])
        second = FakeWS([hello(), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}])
        sockets = [first, second]

        async def connector(url, headers, proxy):
            return sockets.pop(0)

        client = Client(http=HTTPClient(token, session=session), connector=connector, refresh_build=False)
        await client.login(token)
        task = asyncio.ensure_future(client.connect())
        await wait_until(lambda: session.science_posts())
        gateway = client.gateway
        gateway.base_reconnect_delay = 0.001
        gateway.max_reconnect_delay = 0.01
        client.science.flush_delay = 0.01
        # op 7: reconnect and RESUME.
        first.queue.put_nowait(json.dumps({"op": 7, "d": None}))
        await wait_until(lambda: len(session.science_posts()) >= 2)
        await asyncio.sleep(0.1)
        await stop(client, task)

        assert second.sent[0]["op"] == 6
        posts = session.science_posts()
        assert len(posts) == 2, "one POST for the resume"
        body = posts[1][2]["json"]
        assert body["token"] == ANALYTICS_TOKEN
        events = body["events"]
        assert [e["type"] for e in events] == ["gateway_connected", "connection_resumed"]
        assert [e["properties"]["event_sequence_number"] for e in events] == [3, 4]
        connected, resumed = (e["properties"] for e in events)
        assert list(connected) == CAPTURE_KEYS["gateway_connected"][:-1] + [
            "client_uuid", "client_send_timestamp",
        ]
        assert connected["is_reconnect"] is True
        assert connected["gateway_url"] == "wss://gateway-us-east1-b.discord.gg"
        # Inferred: no capture has connection_resumed's keys, so only the common ones.
        assert list(resumed) == [
            "client_track_timestamp",
            "event_sequence_number",
            *COMMON_TAIL,
            "client_uuid",
            "client_send_timestamp",
        ]


# the reporter on its own


class TestReporter:
    def test_client_uuid_layout(self):
        # Words 0-1 user id, 2 prefix, 3-4 creation ms, 5 sequence; a synthetic user id.
        gen = sc.ClientUUID(created_ms=0x1A15A3D4D91, prefix=0xAB99DE11)
        gen.sequence = 8
        assert gen.generate(111111111111111111) == "x3FghPe+igER3pmrkU09WqEBAAAIAAAA"
        assert gen.generate(111111111111111111) == "x3FghPe+igER3pmrkU09WqEBAAAJAAAA"

    @pytest.mark.asyncio
    async def test_a_failing_science_post_does_not_touch_the_gateway(self, token):
        session = FakeSession(science_error=OSError("boom"))
        client, task, ws = await run_client(token, session, [hello(), ready_frame()])
        await client.science.wait_idle()
        assert client.is_ready()
        assert client.gateway.is_connected
        await stop(client, task)


class _FocusGateway:
    """The gateway attributes science reads for focus."""

    def __init__(self):
        self.foreground_enabled = True
        self.foregrounded = False
        self.heartbeat_session_id = None
        self.heartbeat_initialized_ms = None
        self.listeners = []

    def on_foreground(self, handler):
        self.listeners.append(handler)


class TestForeground:
    def test_unfocused_events_carry_no_heartbeat_session(self):
        gw = _FocusGateway()
        rep = sc.ScienceReporter("focus-science-token", None, gw)
        assert gw.listeners == [rep.on_foreground]
        event = rep.track("gateway_connected", {})
        assert "client_heartbeat_session_id" not in event["properties"]
        assert event["properties"]["client_app_state"] == "unfocused"
        assert rep.ad_heartbeat() is None

    def test_focused_events_carry_it_after_the_track_timestamp(self):
        gw = _FocusGateway()
        rep = sc.ScienceReporter("focus-science-token", None, gw)
        gw.foregrounded, gw.heartbeat_session_id, gw.heartbeat_initialized_ms = True, "hb", 1234
        event = rep.ad_heartbeat()
        assert event["type"] == "client_ad_heartbeat"
        assert list(event["properties"])[:6] == [
            "client_track_timestamp",
            "client_heartbeat_session_id",
            "event_sequence_number",
            "client_ad_session_id",
            "client_heartbeat_initialization_timestamp",
            "client_heartbeat_version",
        ]
        props = event["properties"]
        assert props["client_heartbeat_session_id"] == "hb"
        assert props["client_heartbeat_initialization_timestamp"] == 1234
        assert props["client_heartbeat_version"] == 3
        assert props["client_app_state"] == "focused"
        assert rep.ad_heartbeat()["properties"]["client_ad_session_id"] == props["client_ad_session_id"]

    def test_a_client_without_the_option_gets_no_listener(self):
        gw = _FocusGateway()
        gw.foreground_enabled = False
        sc.ScienceReporter("focus-science-token", None, gw)
        assert gw.listeners == []

    @pytest.mark.asyncio
    async def test_ad_heartbeat_loop_runs_while_focused(self, monkeypatch):
        gw = _FocusGateway()
        rep = sc.ScienceReporter("focus-science-token", None, gw)
        waits = []
        real_sleep = asyncio.sleep

        async def fake_sleep(seconds):
            waits.append(seconds)
            if len(waits) == 2:
                gw.foregrounded = False
            await real_sleep(0)

        monkeypatch.setattr(sc.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(sc.random, "uniform", lambda a, b: (a, b))
        gw.foregrounded, gw.heartbeat_session_id = True, "hb"
        rep.on_foreground()
        await asyncio.wait_for(rep._ad_task, timeout=3.0)
        beats = [e for e in rep.queue if e["type"] == "client_ad_heartbeat"]
        assert len(beats) == 2
        assert waits == [sc.AD_HEARTBEAT_INTERVAL, sc.AD_HEARTBEAT_INTERVAL]
        await rep.close()
