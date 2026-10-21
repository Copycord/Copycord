"""selfclient.science: the /api/v9/science POST Chrome sends after READY.

The key lists below are the ones in Mac's Chrome 155 capture (2026-10-20, the
post-login batch at 16:55:47). A real GatewayClient runs on a fake socket and
a real HTTPClient on a fake session, so nothing reaches Discord.
"""
import asyncio
import base64
import json
import logging
import struct

import pytest

from copycord.selfclient import Client
from copycord.selfclient import science as sc
from copycord.selfclient.fingerprint import SUPPRESSED_PROFILE_HEADERS, build_headers, gateway_properties
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
    "session_start_client": [
        "client_track_timestamp",
        "event_sequence_number",
        *COMMON_TAIL,
        "client_send_timestamp",
    ],
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

# client_heartbeat's keys: no event carries them, since there is no client_heartbeat.
HEARTBEAT_KEYS = {
    "client_heartbeat_session_id",
    "client_heartbeat_initialization_timestamp",
    "client_heartbeat_version",
    "is_afk",
    "is_idle",
    "idle_duration_ms",
    "is_system_locked",
    "is_system_suspended",
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

    def __init__(self, science_status=204, science_error=None):
        self.calls = []
        self.science_status = science_status
        self.science_error = science_error

    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if url.endswith("/science"):
            if self.science_error is not None:
                raise self.science_error
            return FakeResponse(self.science_status)
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


def super_props(headers):
    return json.loads(base64.b64decode(headers["X-Super-Properties"]))


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
        assert [e["type"] for e in events] == [
            "session_start_client",
            "gateway_connected",
            "ready_payload_received",
        ]
        for event in events:
            assert list(event) == ["type", "properties"]
            assert list(event["properties"]) == CAPTURE_KEYS[event["type"]], event["type"]
        assert [e["properties"]["event_sequence_number"] for e in events] == [1, 2, 3]
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

    @pytest.mark.asyncio
    async def test_event_values(self, token):
        session = FakeSession()
        client, task, ws = await run_client(token, session, [hello(), ready_frame()])
        await stop(client, task)
        events = {e["type"]: e["properties"] for e in session.science_posts()[0][2]["json"]["events"]}

        assert "client_heartbeat" not in events
        for event_type, props in events.items():
            assert not HEARTBEAT_KEYS & set(props), event_type

        connected = events["gateway_connected"]
        assert connected["gateway_url"] == "wss://gateway.discord.gg"
        assert connected["num_failed_connect_attempts"] == 0
        assert connected["is_reconnect"] is False and connected["is_fast_connect"] is False

        ready = events["ready_payload_received"]
        assert ready["compression_algorithm"] == "zlib-stream"
        assert ready["packing_algorithm"] == "json"
        assert ready["uncompressed_byte_size"] == len(json.dumps(ready_frame()))
        assert (ready["identify_total_server_duration_ms"], ready["identify_api_duration_ms"],
                ready["identify_guilds_duration_ms"]) == (475, 316, 56)
        assert (ready["num_guilds"], ready["num_guild_channels"], ready["num_guild_category_channels"]) == (2, 3, 1)
        assert ready["presences_size"] == 2 and ready["guild_presences_size"] == 2
        assert ready["guild_features_size"] == len('["COMMUNITY"]')
        assert ready["is_reconnect"] is False
        identify = next(m for m in ws.sent if m["op"] == 2)
        assert ready["identify_uncompressed_byte_size"] == len(json.dumps(identify))
        assert 0 < ready["identify_compressed_byte_size"] < ready["identify_uncompressed_byte_size"]
        # Chrome's mixed clocks: identify and connection starts negative, emit start positive.
        assert ready["duration_ms_since_identify_start"] < 0
        assert ready["duration_ms_since_connection_start"] < 0
        assert ready["duration_ms_since_emit_start"] > 0

    @pytest.mark.asyncio
    async def test_client_uuid_only_once_the_user_is_known(self, token):
        session = FakeSession()
        client, task, _ = await run_client(token, session, [hello(), ready_frame()])
        await stop(client, task)
        events = session.science_posts()[0][2]["json"]["events"]

        # As in the capture: the events tracked before READY carry none.
        assert ["client_uuid" in e["properties"] for e in events] == [False, False, True]
        words = struct.unpack("<6I", base64.b64decode(events[2]["properties"]["client_uuid"]))
        assert words[0] | (words[1] << 32) == int(USER_ID)
        assert words[3] | (words[4] << 32) == client.science.started_ms
        assert words[5] == 0

    @pytest.mark.asyncio
    async def test_posts_with_the_rest_post_headers(self, token):
        session = FakeSession()
        client, task, _ = await run_client(token, session, [hello(), ready_frame()])
        await stop(client, task)
        headers = session.science_posts()[0][2]["headers"]

        expected = dict(build_headers(token))
        expected.update(SUPPRESSED_PROFILE_HEADERS)
        assert headers == expected
        assert headers["Origin"] == "https://discord.com"
        assert headers["Content-Type"] == "application/json"
        assert headers["Authorization"] == token
        assert "client_heartbeat_session_id" not in super_props(headers)
        assert list(super_props(headers))[-1] == "client_app_state"
        assert super_props(headers)["client_app_state"] == "unfocused"

    @pytest.mark.asyncio
    async def test_no_heartbeat_session_id_before_or_after_the_gateway(self, token):
        session = FakeSession()
        assert "client_heartbeat_session_id" not in super_props(build_headers(token))
        client, task, _ = await run_client(token, session, [hello(), ready_frame()])
        await stop(client, task)

        login = next(c for c in session.calls if c[1].endswith("/users/@me"))
        assert "Origin" not in login[2]["headers"] and "Content-Type" not in login[2]["headers"]
        for _, _, kwargs in session.calls:
            assert "client_heartbeat_session_id" not in super_props(kwargs["headers"])
        assert "client_heartbeat_session_id" not in super_props(build_headers(token))

    @pytest.mark.asyncio
    async def test_nothing_is_posted_before_ready(self, token):
        session = FakeSession()
        ws = FakeWS([hello()])

        async def connector(url, headers, proxy):
            return ws

        client = Client(http=HTTPClient(token, session=session), connector=connector, refresh_build=False)
        await client.login(token)
        task = asyncio.ensure_future(client.connect(reconnect=False))
        await wait_until(lambda: any(m["op"] == 4 for m in ws.sent))
        science = client.science
        await stop(client, task)

        assert session.science_posts() == []
        assert [e["type"] for e in science.queue] == [
            "session_start_client", "gateway_connected",
        ]


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
        assert [e["properties"]["event_sequence_number"] for e in events] == [4, 5]
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

    @pytest.mark.asyncio
    async def test_a_fresh_connect_does_not_hold_gateway_connected(self):
        science = sc.ScienceReporter("tok-fresh", None, StubGateway())
        science.analytics_token = ANALYTICS_TOKEN
        scheduled = []
        science.flush_soon = lambda delay=None: scheduled.append(delay)
        science.connected("wss://gateway.discord.gg/?v=9")
        assert scheduled == [None]

        gateway = StubGateway()
        gateway.session_id, gateway.sequence = "sess-1", 4
        science = sc.ScienceReporter("tok-resume", None, gateway)
        science.analytics_token = ANALYTICS_TOKEN
        scheduled = []
        science.flush_soon = lambda delay=None: scheduled.append(delay)
        science.connected("wss://gateway-us-east1-b.discord.gg/?v=9")
        assert scheduled == [], "held for connection_resumed"
        await science.on_resumed({})
        assert scheduled == [0.0]
        assert [e["type"] for e in science.queue] == ["gateway_connected", "connection_resumed"]


# the reporter on its own


class StubGateway:
    zlib_stream = True
    last_frame_stats = None
    identify_sizes = None


class StubHTTP:
    def __init__(self, session):
        self.session = session
        self.proxy = None
        self.timeout = 30

    def headers_for(self, method, *, has_body, referer=None, extra=None):
        return {"Origin": "https://discord.com", "Content-Type": "application/json", "M": method}


class TestReporter:
    def test_one_counter_per_client(self):
        a = sc.ScienceReporter("tok-a", None, StubGateway())
        b = sc.ScienceReporter("tok-b", None, StubGateway())
        seq = [a.track("x", {})["properties"]["event_sequence_number"] for _ in range(3)]
        assert seq == [1, 2, 3]
        assert b.track("x", {})["properties"]["event_sequence_number"] == 1

    def test_client_uuid_matches_the_capture(self):
        # Decoded from the capture: words 0-1 user id, 2 prefix, 3-4 creation ms, 5 sequence.
        gen = sc.ClientUUID(created_ms=0x1A15A3D4D91, prefix=0xAB99DE11)
        gen.sequence = 8
        assert gen.generate(0x0DAD9CA3E502101F) == "HxAC5aOcrQ0R3pmrkU09WqEBAAAIAAAA"
        assert gen.generate(0x0DAD9CA3E502101F) == "HxAC5aOcrQ0R3pmrkU09WqEBAAAJAAAA"

    def test_body_needs_the_analytics_token(self):
        r = sc.ScienceReporter("tok", None, StubGateway())
        r.track("x", {})
        assert r.body() is None
        r.analytics_token = "t"
        body = r.body()
        assert body["token"] == "t" and len(body["events"]) == 1
        assert r.queue == []

    def test_the_queue_is_bounded(self):
        r = sc.ScienceReporter("tok", None, StubGateway())
        for _ in range(sc.MAX_QUEUED + 5):
            r.track("x", {})
        assert len(r.queue) == sc.MAX_QUEUED
        assert r.queue[-1]["properties"]["event_sequence_number"] == sc.MAX_QUEUED + 5

    def test_viewport_is_stable_per_token(self):
        assert sc.viewport_for("tok") == sc.viewport_for("tok")
        assert sc.viewport_for("tok") in sc._VIEWPORTS

    def test_gateway_url_root(self):
        assert sc.gateway_url_root("wss://gateway.discord.gg/?encoding=json&v=9") == "wss://gateway.discord.gg"

    def test_unreadable_trace_gives_zeros(self):
        assert sc.identify_timings(["not json"]) == (0, 0, 0)
        assert sc.identify_timings(None) == (0, 0, 0)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("session", [FakeSession(science_status=500),
                                         FakeSession(science_error=OSError("boom"))])
    async def test_failures_stay_at_debug(self, session, caplog):
        caplog.set_level(logging.DEBUG)
        r = sc.ScienceReporter("tok", StubHTTP(session), StubGateway())
        r.track("x", {})
        await r.on_ready({"user": {"id": USER_ID}, "analytics_token": "t"})
        await r.wait_idle()
        assert len(session.science_posts()) == 1
        assert not [rec for rec in caplog.records if rec.levelno > logging.DEBUG]

    @pytest.mark.asyncio
    async def test_a_sent_post_logs_its_event_types_and_status(self, caplog):
        caplog.set_level(logging.DEBUG, logger="selfclient.science")
        session = FakeSession(science_status=204)
        r = sc.ScienceReporter("tok", StubHTTP(session), StubGateway())
        r.track("session_start_client", {})
        await r.on_ready({"user": {"id": USER_ID}, "analytics_token": ANALYTICS_TOKEN})
        await r.wait_idle()
        sent = [rec for rec in caplog.records if rec.name == "selfclient.science"]
        assert [(rec.levelno, rec.getMessage()) for rec in sent] == [
            (
                logging.DEBUG,
                "science POST sent 2 event(s) [session_start_client, ready_payload_received]: HTTP 204",
            )
        ]
        assert ANALYTICS_TOKEN not in caplog.text

    @pytest.mark.asyncio
    async def test_a_refused_post_logs_its_status_at_debug(self, caplog):
        caplog.set_level(logging.DEBUG, logger="selfclient.science")
        r = sc.ScienceReporter("tok", StubHTTP(FakeSession(science_status=429)), StubGateway())
        await r.on_ready({"user": {"id": USER_ID}, "analytics_token": ANALYTICS_TOKEN})
        await r.wait_idle()
        sent = [rec for rec in caplog.records if rec.name == "selfclient.science"]
        assert [(rec.levelno, rec.getMessage()) for rec in sent] == [
            (logging.DEBUG, "science POST refused: HTTP 429")
        ]

    @pytest.mark.asyncio
    async def test_a_failing_science_post_does_not_touch_the_gateway(self, token):
        session = FakeSession(science_error=OSError("boom"))
        client, task, ws = await run_client(token, session, [hello(), ready_frame()])
        await client.science.wait_idle()
        assert client.is_ready()
        assert client.gateway.is_connected
        await stop(client, task)
