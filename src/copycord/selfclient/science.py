# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""POST /science the way Chrome on discord.com does after it connects.

The web client queues events from page load and flushes them once READY gives
it an ``analytics_token``. Copycord sends what a browser tab that loaded
unfocused sends on its own from startup through READY: session_start_client,
gateway_connected and ready_payload_received; and gateway_connected and
connection_resumed together after a RESUME. Such a tab makes no heartbeat
session id, so there is no client_heartbeat and no event carries
client_heartbeat_session_id. Shapes and key orders follow Mac's
Chrome 155 capture (2026-10-20). Events tracked before READY carry no
client_uuid, as in that capture: the browser's generator needs the user id.

Nothing here may break the gateway: every failure is logged at debug.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import random
import secrets
import struct
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from copycord.common import traffic_capture

from . import fingerprint
from .http import API_BASE

log = logging.getLogger("selfclient.science")

SCIENCE_PATH = "/science"

# The constants every post-login event in the capture carries.
ACCESSIBILITY_FEATURES = 524546
CLIENT_RTC_STATE = "DISCONNECTED"
PERFORMANCE_MEMORY = 0

# Events tracked after READY wait this long so a burst goes out as one batch.
FLUSH_DELAY = 3.0

# Events kept while no analytics token is known; the oldest are dropped first.
MAX_QUEUED = 100

# Window sizes Chrome on a Windows desktop commonly reports as innerWidth x innerHeight.
_VIEWPORTS = (
    (1920, 945),
    (1920, 953),
    (1536, 730),
    (1536, 747),
    (1366, 641),
    (1440, 781),
    (1600, 773),
    (1280, 609),
    (2560, 1305),
)

# READY's top-level sections, measured as ready_payload_received reports them.
_READY_SECTIONS = (
    ("presences_size", ("presences",)),
    ("users_size", ("users",)),
    ("read_states_size", ("read_state",)),
    ("private_channels_size", ("private_channels",)),
    ("user_settings_size", ("user_settings_proto", "user_settings")),
    ("experiments_size", ("experiments",)),
    ("user_guild_settings_size", ("user_guild_settings",)),
    ("relationships_size", ("relationships",)),
)

# Per-guild lists, each flattened across every guild before it is measured.
_GUILD_SECTIONS = (
    ("guild_channels_size", "channels"),
    ("guild_members_size", "members"),
    ("guild_presences_size", "presences"),
    ("guild_roles_size", "roles"),
    ("guild_emojis_size", "emojis"),
    ("guild_threads_size", "threads"),
    ("guild_stickers_size", "stickers"),
    ("guild_events_size", "guild_scheduled_events"),
    ("guild_features_size", "features"),
)

_CATEGORY_CHANNEL = 4


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_len(value: Any) -> int:
    """The length JSON.stringify gives ``value``; 0 when READY has no such key."""
    if value is None:
        return 0
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False))


def viewport_for(token: str) -> Tuple[int, int]:
    """A window size that stays the same for a token, like a user's own browser."""
    seed = int(hashlib.sha256(b"viewport:" + token.encode("utf-8")).hexdigest()[:16], 16)
    return random.Random(seed).choice(_VIEWPORTS)


def gateway_url_root(url: str) -> str:
    """``wss://host``: how gateway_connected names the socket it opened."""
    parts = urlsplit(url or "")
    if not parts.scheme or not parts.netloc:
        return url or ""
    return f"{parts.scheme}://{parts.netloc}"


class ClientUUID:
    """The web client's client_uuid: six little-endian 32-bit words, base64.

    user id low, user id high, a random prefix, creation time low, creation
    time high (ms), then a sequence that counts every uuid made.
    """

    def __init__(self, *, created_ms: Optional[int] = None, prefix: Optional[int] = None):
        self.created_ms = _now_ms() if created_ms is None else int(created_ms)
        self.prefix = secrets.randbits(32) if prefix is None else int(prefix) & 0xFFFFFFFF
        self.sequence = 0

    def generate(self, user_id: Any) -> str:
        uid = int(user_id)
        words = (
            uid & 0xFFFFFFFF,
            (uid >> 32) & 0xFFFFFFFF,
            self.prefix,
            self.created_ms & 0xFFFFFFFF,
            (self.created_ms >> 32) & 0xFFFFFFFF,
            self.sequence & 0xFFFFFFFF,
        )
        self.sequence += 1
        return base64.b64encode(struct.pack("<6I", *words)).decode()


def _trace_calls(node: Any, out: Dict[str, float]) -> None:
    """Walk a READY trace (``[name, {micros, calls: [name, {...}, ...]}]``) by name."""
    if not isinstance(node, list):
        return
    for name, info in zip(node[0::2], node[1::2]):
        if isinstance(name, str) and isinstance(info, dict):
            micros = info.get("micros")
            if isinstance(micros, (int, float)) and name not in out:
                out[name] = float(micros)
            _trace_calls(info.get("calls"), out)


def identify_timings(trace: Any) -> Tuple[int, int, int]:
    """(total, api, guilds) server-side IDENTIFY milliseconds from READY's ``_trace``."""
    total = api = guilds = 0
    try:
        root = json.loads(trace[0]) if isinstance(trace, list) and trace else None
        if isinstance(root, list) and len(root) >= 2 and isinstance(root[1], dict):
            total = int(float(root[1].get("micros") or 0) / 1000)
            calls: Dict[str, float] = {}
            _trace_calls(root[1].get("calls"), calls)
            api = int(calls.get("start_session", 0) / 1000)
            guilds = int(calls.get("guilds_connect", 0) / 1000)
    except Exception as exc:
        log.debug("READY trace unreadable: %r", exc)
    return total, api, guilds


def ready_size_metrics(data: Dict[str, Any]) -> Dict[str, int]:
    """The READY sizes and counts ready_payload_received reports, in its key order."""
    started = time.perf_counter()
    guilds = [g for g in (data.get("guilds") or []) if isinstance(g, dict)]
    channels = [c for g in guilds for c in (g.get("channels") or []) if isinstance(c, dict)]

    out: Dict[str, int] = {
        "num_guilds": len(guilds),
        "num_guild_channels": len(channels),
        "num_guild_category_channels": sum(
            1 for c in channels if c.get("type") == _CATEGORY_CHANNEL
        ),
    }
    measured = {"guilds"}
    for metric, keys in _READY_SECTIONS:
        key = next((k for k in keys if k in data), None)
        out[metric] = _json_len(data.get(key)) if key else 0
        measured.update(keys)
    out["remaining_data_size"] = _json_len({k: v for k, v in data.items() if k not in measured})

    for metric, key in _GUILD_SECTIONS:
        flat: List[Any] = []
        for g in guilds:
            value = g.get(key)
            if isinstance(value, list):
                flat.extend(value)
        out[metric] = _json_len(flat)
    guild_keys = {key for _, key in _GUILD_SECTIONS}
    out["guild_remaining_data_size"] = _json_len(
        [{k: v for k, v in g.items() if k not in guild_keys} for g in guilds]
    )
    out["size_metrics_duration_ms"] = int((time.perf_counter() - started) * 1000)
    return out


class ScienceReporter:
    """One per gateway client: one event_sequence_number and one client_uuid scheme.

    ``http`` is the account's HTTPClient; the POST goes through its headers,
    session and proxy. ``gateway`` is read for the session, the IDENTIFY
    sizes and the last frame's stats.
    """

    def __init__(self, token: str, http: Any, gateway: Any, *, flush_delay: float = FLUSH_DELAY):
        self.token = token
        self.http = http
        self.gateway = gateway
        self.flush_delay = flush_delay
        # Page load: uptime_app and the browser's performance.now() count from here.
        self.started_ms = _now_ms()
        self.uuids = ClientUUID(created_ms=self.started_ms)
        self.sequence = 0
        self.analytics_token: Optional[str] = None
        self.user_id: Optional[str] = None
        self.queue: List[Dict[str, Any]] = []
        self.viewport = viewport_for(token)
        self._tasks: set = set()
        self._delayed_pending = False
        self._closed = False
        self._session_started = False
        self._ready_count = 0
        # Socket timing, for gateway_connected and ready_payload_received.
        self._first_attempt_ms: Optional[int] = None
        self._attempt_ms: Optional[int] = None
        self._connected_ms: Optional[int] = None
        self._failed_attempts = 0
        self._connections = 0

    # what the browser tracks

    def start(self) -> None:
        """Page load: the session start."""
        try:
            if not self._session_started:
                self._session_started = True
                self.track("session_start_client", {})
        except Exception as exc:
            log.debug("science start failed: %r", exc)

    def connect_attempt(self) -> None:
        now = _now_ms()
        self._attempt_ms = now
        if self._first_attempt_ms is None:
            self._first_attempt_ms = now

    def connect_failed(self) -> None:
        self._failed_attempts += 1

    def connected(self, url: str) -> None:
        """A socket opened: gateway_connected.

        On a socket that will RESUME it waits for connection_resumed, so the
        two go out in one POST as the browser's do.
        """
        try:
            now = _now_ms()
            attempt = self._attempt_ms or now
            first = self._first_attempt_ms or attempt
            gateway = self.gateway
            resuming = bool(
                getattr(gateway, "session_id", None)
                and getattr(gateway, "sequence", None) is not None
            )
            self.track(
                "gateway_connected",
                {
                    "num_failed_connect_attempts": self._failed_attempts,
                    "gateway_url": gateway_url_root(url),
                    "is_reconnect": self._connections > 0,
                    "is_fast_connect": False,
                    "duration_ms_since_first_connect_attempt": now - first,
                    "duration_ms_since_connect_attempt_start": now - attempt,
                },
                hold=resuming,
            )
            self._connections += 1
            self._connected_ms = now
            self._failed_attempts = 0
            self._first_attempt_ms = None
        except Exception as exc:
            log.debug("science gateway_connected failed: %r", exc)

    async def on_resumed(self, data: Any) -> None:
        """RESUMED: connection_resumed, then flush it with the held gateway_connected."""
        try:
            # Inferred: notes2.har shows the event on RESUME but no capture has
            # its properties, so it carries only the keys every event has.
            self.track("connection_resumed", {}, hold=True)
            self.flush_soon(0.0)
        except Exception as exc:
            log.debug("science RESUMED handling failed: %r", exc)

    async def on_ready(self, data: Any) -> None:
        """READY: ready_payload_received, then flush with its analytics token.

        Runs before the client's own READY handling, so ``data`` is untouched.
        """
        try:
            data = data if isinstance(data, dict) else {}
            user = data.get("user") or {}
            if user.get("id"):
                self.user_id = str(user["id"])
            if data.get("analytics_token"):
                self.analytics_token = str(data["analytics_token"])
            self.track("ready_payload_received", self._ready_properties(data))
            self._ready_count += 1
            self.flush_soon(0.0)
        except Exception as exc:
            log.debug("science READY handling failed: %r", exc)

    def _perf_now(self) -> float:
        """The browser's performance.now(): ms since page load, 0.1 ms steps."""
        return round(time.time() * 1000 - self.started_ms, 1)

    def _ready_properties(self, data: Dict[str, Any]) -> Dict[str, Any]:
        compressed, uncompressed, unpack_ms, received_ms = getattr(
            self.gateway, "last_frame_stats", None
        ) or (0, 0, 0.0, _now_ms())
        total, api, guilds = identify_timings(data.get("_trace"))
        identify_size, identify_compressed = getattr(self.gateway, "identify_sizes", None) or (0, 0)
        connected = self._connected_ms or _now_ms()
        perf_now = self._perf_now()
        # The browser mixes clocks here: its identify and connection starts are
        # wall-clock ms set against performance.now(), and its emit start is a
        # performance.now() set against the wall clock. Chrome sends exactly
        # these offsets, so they are reproduced rather than corrected.
        emit_start_perf = round(received_ms - self.started_ms, 1)
        props: Dict[str, Any] = {
            "compressed_byte_size": int(compressed),
            "uncompressed_byte_size": int(uncompressed),
            "compression_algorithm": (
                "zlib-stream" if getattr(self.gateway, "zlib_stream", True) else "none"
            ),
            "packing_algorithm": "json",
            "unpack_duration_ms": int(unpack_ms),
            "identify_total_server_duration_ms": total,
            "identify_api_duration_ms": api,
            "identify_guilds_duration_ms": guilds,
        }
        props.update(ready_size_metrics(data))
        props.update(
            {
                "duration_ms_since_identify_start": round(perf_now - connected, 1),
                "duration_ms_since_connection_start": round(
                    perf_now - (self._attempt_ms or connected), 1
                ),
                "duration_ms_since_emit_start": round(_now_ms() - emit_start_perf, 1),
                "is_reconnect": self._ready_count > 0,
                "is_fast_connect": False,
                "did_force_clear_guild_hashes": False,
                "identify_uncompressed_byte_size": int(identify_size),
                "identify_compressed_byte_size": int(identify_compressed),
                "had_cache_at_startup": False,
                "used_cache_at_startup": False,
            }
        )
        return props

    # events

    def _common(self) -> Dict[str, Any]:
        """The tail every post-login event in the capture carries, in its order."""
        try:
            props = fingerprint.gateway_properties(self.token)
        except Exception:
            props = {}
        width, height = self.viewport
        return {
            "client_performance_memory": PERFORMANCE_MEMORY,
            "accessibility_features": ACCESSIBILITY_FEATURES,
            "rendered_locale": fingerprint.LOCALE,
            "uptime_app": max(0, (_now_ms() - self.started_ms) // 1000),
            "launch_signature": props.get("launch_signature"),
            "client_rtc_state": CLIENT_RTC_STATE,
            "client_app_state": props.get("client_app_state", "unfocused"),
            "client_viewport_width": width,
            "client_viewport_height": height,
        }

    def track(
        self, event_type: str, properties: Dict[str, Any], *, hold: bool = False
    ) -> Dict[str, Any]:
        """Queue one event; it goes out with the next flush. ``hold`` schedules
        none, for an event that waits for a later one."""
        self.sequence += 1
        props: Dict[str, Any] = {
            "client_track_timestamp": _now_ms(),
            "event_sequence_number": self.sequence,
        }
        props.update(properties)
        props.update(self._common())
        if self.user_id:
            props["client_uuid"] = self.uuids.generate(self.user_id)
        event = {"type": event_type, "properties": props}
        self.queue.append(event)
        if len(self.queue) > MAX_QUEUED:
            del self.queue[: len(self.queue) - MAX_QUEUED]
        if self.analytics_token and not hold and event_type != "ready_payload_received":
            self.flush_soon()
        return event

    def body(self) -> Optional[Dict[str, Any]]:
        """The next POST body, taking the queue; None when nothing can go yet."""
        if not self.queue or not self.analytics_token:
            return None
        events, self.queue = self.queue, []
        sent = _now_ms()
        for event in events:
            event["properties"]["client_send_timestamp"] = sent
        return {"token": self.analytics_token, "events": events}

    # sending

    def flush_soon(self, delay: Optional[float] = None) -> None:
        """Flush in the background. A delayed flush already waiting covers later events;
        an immediate one always runs, so READY's batch is never held back."""
        if self._closed or self.http is None:
            return
        delay = self.flush_delay if delay is None else delay
        if delay > 0 and self._delayed_pending:
            return
        try:
            task = asyncio.ensure_future(self._flush_after(delay))
        except RuntimeError as exc:  # no running loop
            log.debug("science flush not scheduled: %r", exc)
            return
        if delay > 0:
            self._delayed_pending = True
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _flush_after(self, delay: float) -> None:
        if delay > 0:
            try:
                await asyncio.sleep(delay)
            finally:
                self._delayed_pending = False
        await self.flush()

    async def flush(self) -> bool:
        """POST what is queued. True on a 2xx; never raises."""
        try:
            body = self.body()
            if body is None:
                return False
            return await self._post(body)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("science POST failed: %r", exc)
            return False

    async def _post(self, body: Dict[str, Any]) -> bool:
        http = self.http
        # The REST client's header path, for a POST: Origin and Content-Type stay.
        headers = http.headers_for("POST", has_body=True)
        kwargs: Dict[str, Any] = {"json": body, "headers": headers, "timeout": http.timeout}
        if http.proxy:
            kwargs["proxy"] = http.proxy
        url = f"{API_BASE}{SCIENCE_PATH}"
        capture = traffic_capture.recorder_for(self.token)
        rid = capture.request("POST", url, headers, body=body, has_body=True) if capture else None
        # Straight to the session: one attempt, no retry, and a refusal stays at debug.
        try:
            resp = await http.session.request("POST", url, **kwargs)
        except Exception as exc:
            if capture is not None:
                capture.failed(rid, exc)
            raise
        status = int(getattr(resp, "status_code", 0) or 0)
        if capture is not None:
            capture.response(rid, status)
        if 200 <= status < 300:
            log.debug(
                "science POST sent %d event(s) [%s]: HTTP %s",
                len(body["events"]),
                ", ".join(str(e.get("type")) for e in body["events"]),
                status,
            )
            return True
        log.debug("science POST refused: HTTP %s", status)
        return False

    async def wait_idle(self) -> None:
        """Wait for every scheduled flush to finish."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def close(self) -> None:
        """Stop sending; queued events are dropped."""
        self._closed = True
        tasks, self._tasks = list(self._tasks), set()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


__all__ = [
    "ACCESSIBILITY_FEATURES",
    "SCIENCE_PATH",
    "ClientUUID",
    "ScienceReporter",
    "gateway_url_root",
    "identify_timings",
    "ready_size_metrics",
    "viewport_for",
]
