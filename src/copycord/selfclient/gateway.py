# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""User-token gateway over a curl_cffi websocket.

Handlers get raw dispatch payloads. The socket is dialed through curl_cffi with
the same Chrome impersonation as REST, so its TLS handshake is Chrome's, not
Python's. A failed dial rotates the proxy lease; a close from Discord does not.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import random
import time
import zlib
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit

from copycord.common import traffic_capture
from copycord.common.exit_timezone import note_token_proxy
from copycord.common.token_proxy import lease_with_timezone

from . import fingerprint, qos
from .enums import API_VERSION, FATAL_CLOSE_CODES, Capabilities, CloseCode, GatewayOp
from .errors import ConnectionClosed, LoginFailure

try:
    from curl_cffi.const import CurlWsFlag as _CurlWsFlag
    from curl_cffi.curl import CurlError as _CurlError
    from curl_cffi.requests import AsyncSession as _CurlAsyncSession

    _WS_TEXT = int(_CurlWsFlag.TEXT)
    _WS_BINARY = int(_CurlWsFlag.BINARY)
    _WS_CLOSE = int(_CurlWsFlag.CLOSE)
except ImportError:  # the tests drive a fake socket and do not need it
    _CurlAsyncSession = None
    _WS_TEXT, _WS_BINARY, _WS_CLOSE = 1, 2, 8

    class _CurlError(Exception):  # type: ignore[no-redef]
        code = 0


try:
    # A connector may still hand over a socket from the websockets library.
    from websockets.exceptions import ConnectionClosed as _WSClosed
except ImportError:

    class _WSClosed(Exception):  # type: ignore[no-redef]
        rcvd = None
        sent = None


log = logging.getLogger("selfclient.gateway")

GATEWAY_HOST = "wss://gateway.discord.gg/"

# The largest single message curl_cffi will take (its default is 4 MiB). READY
# decoded to 9.5 MB in captures; zlib-stream frames are smaller, but a socket
# that fell back to plain JSON gets READY in one piece.
MAX_FRAME_SIZE = 16 * 1024 * 1024
OPEN_TIMEOUT = 20.0

# Discord ends each zlib-stream message with this Z_SYNC_FLUSH trailer.
ZLIB_SUFFIX = b"\x00\x00\xff\xff"

# Closing with 1000 or 1001 invalidates the session; a 4000-range code keeps it
# resumable.
RESUME_CLOSE_CODE = CloseCode.RESUME

# Close codes after which only a fresh IDENTIFY works.
_SESSION_RESET_CODES = frozenset({CloseCode.INVALID_SEQ, CloseCode.SESSION_TIMED_OUT})

# Without CHANNEL_OBFUSCATION, so READY has real names for channels the account
# cannot view instead of `___hidden___`.
CAPABILITIES = Capabilities.DEFAULT  # 1734653

# The default subscription skips threads the account has not joined; `threads`
# turns them on. A guild's first frame must carry both `typing` and `threads`:
# once subscribed via `typing`, adding `threads` later sends no THREAD_LIST_SYNC.
SUBSCRIPTION_FEATURES = {
    "typing": True,
    "activities": True,
    "threads": True,
}

# What op 37 carries per guild right after a RESUME: the full shape, in the
# browser's key order (notes2.har). Opening a guild uses the short one above.
RESUME_SUBSCRIPTION_FEATURES = {
    "typing": True,
    "threads": True,
    "activities": True,
    "members": [],
    "member_updates": False,
    "channels": {},
    "thread_member_lists": [],
}

# Discord drops oversized op 37 frames, so batches are split to stay under this.
MAX_SUBSCRIPTION_PAYLOAD = 15360

# Dispatches worth a DEBUG line; the rest are content traffic.
SESSION_DISPATCHES = frozenset(
    {"READY", "READY_SUPPLEMENTAL", "RESUMED", "SESSIONS_REPLACE"}
)

# op 40 `qos.ver` the web client sends.
QOS_VERSION = 31

# Seconds after READY on a fresh connect before the one op 3 an unfocused tab
# sends (the overnight capture: IDENTIFY 02:05:19, op 3 02:06:49).
PRESENCE_DELAY = 90.0

# op 4 the web client sends right after IDENTIFY: in no voice channel.
IDLE_VOICE_STATE = {
    "guild_id": None,
    "channel_id": None,
    "self_mute": True,
    "self_deaf": False,
    "self_video": False,
    "flags": 2,
}

EventHandler = Callable[[Any], Awaitable[Any]]
DispatchHandler = Callable[[str, Any], Awaitable[Any]]
Connector = Callable[[str, Dict[str, str], Optional[str]], Awaitable[Any]]


def gateway_url(host: str = GATEWAY_HOST, *, zlib_stream: bool = False) -> str:
    """Return `host` with this client's `encoding`/`v`/`compress` query.

    READY's `resume_gateway_url` has no query, and connecting to it bare
    silently falls back to the default API version and loses thread events.
    """
    parts = urlsplit(host or GATEWAY_HOST)
    params: Dict[str, Any] = {"encoding": "json", "v": API_VERSION}
    if zlib_stream:
        params["compress"] = "zlib-stream"
    path = parts.path or "/"
    return urlunsplit(parts._replace(path=path, query=urlencode(params)))


def build_subscription_frames(
    guild_ids: Iterable[Any], features: Dict[str, Any] = SUBSCRIPTION_FEATURES
) -> List[Dict[str, Any]]:
    """Batch guild ids into as few op 37 frames as the size cap allows."""
    frames: List[Dict[str, Any]] = []
    batch: Dict[str, Dict[str, Any]] = {}
    seen: set = set()

    def flush() -> None:
        if batch:
            frames.append(
                {"op": GatewayOp.BULK_GUILD_SUBSCRIBE, "d": {"subscriptions": dict(batch)}}
            )
            batch.clear()

    for gid in guild_ids:
        if not gid:
            continue
        gid = str(gid)
        if gid in seen:
            continue
        seen.add(gid)
        batch[gid] = copy.deepcopy(features)
        if (
            len(batch) > 1
            and len(json.dumps({"subscriptions": batch})) > MAX_SUBSCRIPTION_PAYLOAD
        ):
            del batch[gid]
            flush()
            batch[gid] = copy.deepcopy(features)
    flush()
    return frames


def frame_summary(payload: Dict[str, Any]) -> str:
    """One line describing an outgoing frame, for debug logs. Never the token."""
    op = payload.get("op")
    d = payload.get("d")
    d = d if isinstance(d, dict) else {}
    if op == GatewayOp.IDENTIFY:
        props = d.get("properties") or {}
        return (
            f"op 2 IDENTIFY build={props.get('client_build_number')}"
            f" os={props.get('os_version')}"
            f" app_state={props.get('client_app_state')}"
        )
    if op == GatewayOp.PRESENCE_UPDATE:
        return (
            f"op 3 presence status={d.get('status')} afk={d.get('afk')}"
            f" activities={len(d.get('activities') or [])}"
        )
    if op == GatewayOp.VOICE_STATE_UPDATE:
        return (
            f"op 4 voice guild={d.get('guild_id')} channel={d.get('channel_id')}"
            f" flags={d.get('flags')}"
        )
    if op == GatewayOp.RESUME:
        return f"op 6 RESUME session={d.get('session_id')} seq={d.get('seq')}"
    if op == GatewayOp.REQUEST_GUILD_MEMBERS:
        guilds = d.get("guild_id")
        count = len(guilds) if isinstance(guilds, list) else int(guilds is not None)
        if "user_ids" in d:
            target = f"user_ids={len(d.get('user_ids') or [])}"
        else:
            target = f"query={str(d.get('query', ''))[:32]!r} limit={d.get('limit')}"
        return f"op 8 members guilds={count} {target} presences={d.get('presences')}"
    if op == GatewayOp.BULK_GUILD_SUBSCRIBE:
        return f"op 37 subscribe guilds={len(d.get('subscriptions') or {})}"
    if op == GatewayOp.QOS_HEARTBEAT:
        state = d.get("qos") or {}
        return f"op 40 heartbeat seq={d.get('seq')} active={state.get('active')}"
    return f"op {op}"


class ZlibStreamInflater:
    """One socket's zlib-stream context; the whole connection is one deflate stream."""

    def __init__(self) -> None:
        self._inflator = zlib.decompressobj()
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> Optional[str]:
        """Add a frame; return the decoded message once one is complete."""
        self._buffer.extend(chunk)
        if len(self._buffer) < 4 or self._buffer[-4:] != ZLIB_SUFFIX:
            return None
        data = self._inflator.decompress(bytes(self._buffer))
        self._buffer.clear()
        return data.decode("utf-8")


class _GatewayRatelimiter:
    """Sliding window of 110 sends per 60 s. Heartbeats bypass it."""

    RATE = 110
    PER = 60.0

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._sends: List[float] = []

    async def block(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._sends = [t for t in self._sends if now - t < self.PER]
                if len(self._sends) < self.RATE:
                    break
                wait = self.PER - (now - self._sends[0]) + 0.01
                log.warning("gateway ratelimit, sleeping %.2fs", wait)
                await asyncio.sleep(wait)
            self._sends.append(time.monotonic())


class GatewayDialError(Exception):
    """The socket never opened: the proxy or the network, not Discord."""


class ProxyTunnelError(GatewayDialError):
    """The proxy refused or broke the tunnel."""


def mask_proxy(proxy: Optional[str]) -> str:
    """A proxy URL with its credentials hidden, for logs."""
    if not proxy:
        return "none"
    parts = urlsplit(proxy)
    if parts.username or parts.password:
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        return urlunsplit(parts._replace(netloc=f"***@{host}"))
    return proxy


def curl_proxy_url(proxy: Optional[str]) -> Optional[str]:
    """``proxy`` as curl should dial it, keeping what the old tunnel did with it.

    ``https://`` proxies were plain CONNECT proxies, so they become ``http://``
    (to curl, https:// means TLS to the proxy itself). ``socks5://`` resolved the
    host at the proxy, which curl calls ``socks5h://``. Raises ProxyTunnelError
    for a scheme neither supports.
    """
    if not proxy:
        return None
    parts = urlsplit(proxy)
    scheme = (parts.scheme or "").lower()
    if not parts.hostname:
        raise ProxyTunnelError(f"unparsable proxy url {mask_proxy(proxy)}")
    renamed = {"https": "http", "socks5": "socks5h"}.get(scheme, scheme)
    if renamed not in ("http", "socks4", "socks4a", "socks5h"):
        raise ProxyTunnelError(f"unsupported proxy scheme {scheme!r}")
    if renamed == scheme:
        return proxy
    return urlunsplit(parts._replace(scheme=renamed))


class SocketClosed(Exception):
    """The gateway socket closed or broke. ``code`` is the close code, if known."""

    rcvd = None

    def __init__(self, message: str = "", code: Optional[int] = None) -> None:
        super().__init__(message)
        self.code = code


# What the receive loop and the senders treat as "this socket is gone".
_CLOSED_ERRORS = (_WSClosed, SocketClosed)


def _consume_result(task: "asyncio.Future[Any]") -> None:
    """Mark a finished receive as retrieved, so an unread error is not logged."""
    if not task.cancelled():
        task.exception()


class CurlWebSocket:
    """A curl_cffi AsyncWebSocket behind the interface the gateway loop uses:
    ``send(text)``, ``recv()``, ``async for``, ``close(code)``, ``close_code``
    and ``close_reason``. curl_cffi's errors come out as SocketClosed.

    A ``recv()`` cancelled by a timeout keeps its read pending, and the next
    ``recv()`` returns it: dropping one frame would break the zlib stream.
    """

    def __init__(self, ws: Any, owned_session: Any = None) -> None:
        self._ws = ws
        # The session open_websocket made for this socket alone; closed with it.
        self._owned_session = owned_session
        self._pending: Optional["asyncio.Future[Any]"] = None
        self._error_code: Optional[int] = None
        self._sent_code: Optional[int] = None

    @property
    def close_code(self) -> Optional[int]:
        """The code Discord closed with, else the one curl_cffi reported, else the one we sent."""
        code = getattr(self._ws, "close_code", None)
        if code is not None:
            return int(code)
        return self._error_code if self._error_code is not None else self._sent_code

    @property
    def close_reason(self) -> str:
        return str(getattr(self._ws, "close_reason", None) or "")

    def _closed(self, exc: BaseException) -> SocketClosed:
        code = getattr(exc, "code", None)
        # curl_cffi puts either a close code or a libcurl error number here.
        if isinstance(code, int) and 1000 <= code < 5000 and self._error_code is None:
            self._error_code = int(code)
        return SocketClosed(str(exc) or type(exc).__name__, self.close_code)

    async def send(self, data: Any) -> None:
        flags = _WS_TEXT if isinstance(data, str) else _WS_BINARY
        try:
            await self._ws.send(data, flags)
        except _CurlError as exc:
            raise self._closed(exc) from exc

    async def recv(self) -> Any:
        """The next message: ``str`` for a text frame, ``bytes`` for binary."""
        task = self._pending
        if task is None:
            task = self._pending = asyncio.ensure_future(self._ws.recv())
            task.add_done_callback(_consume_result)
        try:
            payload, flags = await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                self._pending = None
            raise
        except _CurlError as exc:
            self._pending = None
            raise self._closed(exc) from exc
        self._pending = None
        if flags & _WS_CLOSE:
            raise SocketClosed("close frame", self.close_code)
        if flags & _WS_TEXT:
            return bytes(payload).decode("utf-8", errors="replace")
        return payload

    def __aiter__(self) -> "CurlWebSocket":
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.recv()
        except SocketClosed:
            raise StopAsyncIteration from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._sent_code is None:
            self._sent_code = code
        try:
            await self._ws.close(code, reason)
        except _CurlError:
            pass
        finally:
            session, self._owned_session = self._owned_session, None
            if session is not None:
                try:
                    await session.close()
                except Exception:
                    pass


async def open_websocket(
    url: str,
    headers: Dict[str, str],
    proxy: Optional[str] = None,
    *,
    session: Any = None,
    max_size: int = MAX_FRAME_SIZE,
    open_timeout: float = OPEN_TIMEOUT,
) -> CurlWebSocket:
    """Open the gateway socket through curl_cffi, via ``proxy`` when one is given.

    The TLS handshake impersonates TLS_IMPERSONATE, as REST does. ``session`` is
    the token's REST session, so both share one TLS identity; without one, a
    session is made for this socket and closed with it. Only ``headers`` go on
    the upgrade request, not curl_cffi's navigation defaults.
    """
    dial_proxy = curl_proxy_url(proxy)
    owned = None
    if session is None:
        if _CurlAsyncSession is None:
            raise GatewayDialError("the curl_cffi package is not installed")
        session = owned = _CurlAsyncSession(impersonate=fingerprint.TLS_IMPERSONATE)
    try:
        ws = await session.ws_connect(
            url,
            headers=dict(headers),
            proxy=dial_proxy,
            impersonate=fingerprint.TLS_IMPERSONATE,
            default_headers=False,
            accept_encoding=None,
            timeout=open_timeout,
            max_message_size=max_size,
        )
    except BaseException:
        if owned is not None:
            try:
                await owned.close()
            except Exception:
                pass
        raise
    return CurlWebSocket(ws, owned)


def _close_info(ws: Any, exc: Optional[BaseException]) -> Tuple[Optional[int], str]:
    """The close code and reason Discord sent, from the exception or the socket."""
    rcvd = getattr(exc, "rcvd", None) if exc is not None else None
    if rcvd is not None:
        return getattr(rcvd, "code", None), str(getattr(rcvd, "reason", "") or "")
    code = getattr(ws, "close_code", None)
    reason = getattr(ws, "close_reason", "") or ""
    return code, str(reason)


class GatewayClient:
    """One user-token gateway session, reconnecting and resuming on its own.

    ``run()`` returns after ``close()``, or raises on a fatal close code.
    """

    def __init__(
        self,
        token: str,
        *,
        status: str = "online",
        custom_status: Optional[str] = None,
        activity: Optional[Dict[str, Any]] = None,
        reconnect: bool = True,
        base_reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 60.0,
        zlib_stream: bool = True,
        host: str = GATEWAY_HOST,
        capabilities: int = CAPABILITIES,
        proxy: Optional[str] = None,
        proxy_pool: Any = None,
        connector: Optional[Connector] = None,
        presence_delay: float = PRESENCE_DELAY,
    ) -> None:
        self.token = token
        # Online by default and never idle: Copycord never goes idle on its own.
        self.status = status
        self.custom_status = custom_status
        self.activity = activity
        self.reconnect = reconnect
        self.base_reconnect_delay = base_reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        # Cleared for good if a stream ever fails to inflate.
        self.zlib_stream = zlib_stream
        self.host = host
        self.capabilities = capabilities
        # Used when there is no enabled proxy pool.
        self.proxy = proxy
        self.proxy_pool = proxy_pool
        self._connector = connector
        self.presence_delay = presence_delay

        self._guild_ids: List[str] = []
        self._subscribed_guilds: set = set()
        self._ws: Any = None
        self._proxy_in_use: Optional[str] = None
        self.session_id: Optional[str] = None
        self.resume_url: Optional[str] = None
        # The account's user id, from the last READY.
        self.user_id: Optional[str] = None
        # The properties the last IDENTIFY sent.
        self.identify_properties: Optional[Dict[str, Any]] = None
        # For science's ready_payload_received: (json bytes, zlib bytes) of the last
        # IDENTIFY, and (bytes received, json chars, unpack ms, received at ms) of
        # the last frame read.
        self.identify_sizes: Optional[Tuple[int, int]] = None
        self.last_frame_stats: Optional[Tuple[int, int, float, int]] = None
        # Dev mode's traffic record (traffic_capture.py) and the current socket's id in it.
        self._capture: Any = None
        self._capture_ws: Optional[str] = None
        self.sequence: Optional[int] = None
        self.heartbeat_interval: float = 41.25
        self.latency: float = float("inf")
        self._last_ack: float = time.monotonic()
        self._last_heartbeat: float = 0.0
        self._heartbeat_task: Optional[asyncio.Task] = None
        # The op 3 a fresh connect sends presence_delay after READY.
        self._presence_task: Optional[asyncio.Task] = None
        self._stopped: bool = False
        self._invalid_session_delay: Tuple[float, float] = (1.0, 5.0)
        # (code, reason) of a close no reconnect can fix.
        self.fatal_close: Optional[Tuple[Optional[int], str]] = None
        # None while a session is live; set until the first READY.
        self.disconnected_since: Optional[float] = time.time()
        self.reconnect_count: int = 0
        self.connect_count: int = 0
        self._ratelimiter = _GatewayRatelimiter()

        self._handlers: Dict[str, List[EventHandler]] = {}
        self._dispatch_handlers: List[DispatchHandler] = []

    # Public API

    def on(self, event: str, handler: EventHandler) -> None:
        """Call ``handler(d)`` for every dispatch named ``event``."""
        self._handlers.setdefault(event.upper(), []).append(handler)

    def on_dispatch(self, handler: DispatchHandler) -> None:
        """Call ``handler(event, d)`` for every dispatch."""
        self._dispatch_handlers.append(handler)

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self.disconnected_since is None

    @property
    def guild_ids(self) -> List[str]:
        return list(self._guild_ids)

    async def run(self, *, refresh_build: bool = True) -> None:
        log.debug("starting gateway client")
        if refresh_build:
            # IDENTIFY carries the build number, and a stale one stands out.
            try:
                await fingerprint.refresh_build_info()
            except Exception as exc:
                log.warning("build refresh failed, using cached build: %r", exc)

        delay = self.base_reconnect_delay
        try:
            while not self._stopped:
                try:
                    await self._connect()
                    delay = self.base_reconnect_delay
                except GatewayDialError as exc:
                    # Likely a dead proxy, so rotate.
                    log.warning("gateway dial failed: %s", exc)
                    await self._rotate_proxy()
                except Exception as exc:
                    log.warning("gateway error: %r", exc)

                if not self.reconnect or self._stopped:
                    break

                self.reconnect_count += 1
                wait = min(delay + random.uniform(0, delay), self.max_reconnect_delay)
                log.info("reconnect #%d in %.1fs", self.reconnect_count, wait)
                await asyncio.sleep(wait)
                delay = min(delay * 2, self.max_reconnect_delay)
        finally:
            await self._cancel_task("_heartbeat_task")
            await self._cancel_task("_presence_task")

        if self.fatal_close is not None:
            code, reason = self.fatal_close
            if code == CloseCode.AUTHENTICATION_FAILED:
                raise LoginFailure("Improper token has been passed.")
            raise ConnectionClosed(code, reason)

    async def close(self) -> None:
        """Stop for good, closing with 1000 so the session is not resumable."""
        self._stopped = True
        await self._cancel_task("_heartbeat_task")
        await self._cancel_task("_presence_task")
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    async def send(self, payload: Dict[str, Any]) -> None:
        """Send a gateway frame through the 110/60s rate limiter."""
        await self._send(payload)

    async def subscribe_guilds(self, guild_ids: Iterable[Any], *, force: bool = False) -> None:
        """Opt into thread, typing, activity and member events for these guilds.

        Already-subscribed guilds are skipped unless `force`.
        """
        ids = [str(g) for g in guild_ids if g]
        if not force:
            ids = [g for g in ids if g not in self._subscribed_guilds]
        frames = build_subscription_frames(ids)
        if not frames:
            return
        for frame in frames:
            await self._send(frame)
            self._subscribed_guilds.update(frame["d"]["subscriptions"].keys())

    async def request_guild_members(
        self,
        guild_id: Any,
        *,
        query: str = "",
        limit: int = 0,
        presences: bool = False,
        user_ids: Optional[Iterable[Any]] = None,
    ) -> None:
        """Send op 8 in one of the web client's two shapes, without a nonce.

        The answer arrives as GUILD_MEMBERS_CHUNK; callers match it by guild.
        """
        d: Dict[str, Any] = {"guild_id": [str(guild_id)]}
        if user_ids is not None:
            d["user_ids"] = [str(u) for u in user_ids]
        else:
            d["query"] = query
            d["limit"] = int(limit)
        d["presences"] = bool(presences)
        await self._send({"op": GatewayOp.REQUEST_GUILD_MEMBERS, "d": d})

    async def change_presence(
        self,
        *,
        status: Optional[str] = None,
        custom_status: Optional[str] = None,
        activity: Optional[Dict[str, Any]] = None,
    ) -> None:
        if status is not None:
            self.status = status
        if custom_status is not None:
            self.custom_status = custom_status or None
        if activity is not None:
            self.activity = activity or None
        await self._send_presence_update()

    # Connection lifecycle

    def _ws_headers(self) -> Dict[str, str]:
        """Upgrade-request headers: the browser's UA and Origin."""
        ua = fingerprint.build_headers(self.token).get("User-Agent", "")
        return {"User-Agent": ua, "Origin": "https://discord.com"}

    async def _current_proxy(self) -> Optional[str]:
        pool = self.proxy_pool
        if pool is not None and getattr(pool, "enabled", False):
            # Rotates past proxies whose exit timezone lookup fails; NoProxyAvailable
            # when none resolves, so the socket is not opened (run() backs off).
            return await lease_with_timezone(pool, self.token)
        proxy = self.proxy
        if not await note_token_proxy(self.token, proxy):
            raise GatewayDialError("exit timezone lookup failed for the token's proxy")
        return proxy

    async def _rotate_proxy(self) -> None:
        pool = self.proxy_pool
        if pool is None or not getattr(pool, "enabled", False):
            return
        try:
            new_proxy = await pool.report_failure(self.token)
        except Exception as exc:
            log.warning("proxy rotation failed: %r", exc)
            return
        log.warning("proxy died, rotated to %s", mask_proxy(new_proxy))

    def _proxy_ok(self) -> None:
        pool = self.proxy_pool
        if pool is not None and self._proxy_in_use:
            try:
                pool.report_success(self._proxy_in_use)
            except Exception:
                pass

    async def _open(self, url: str, proxy: Optional[str]) -> Any:
        headers = self._ws_headers()
        capture = self._capture = traffic_capture.recorder_for(self.token)
        self._capture_ws = capture.ws_open(url, headers) if capture is not None else None
        try:
            if self._connector is not None:
                return await self._connector(url, headers, proxy)
            return await open_websocket(url, headers, proxy)
        except Exception as exc:
            if capture is not None:
                capture.ws_closed(self._capture_ws, error=exc)
            if isinstance(exc, GatewayDialError):
                raise
            raise GatewayDialError(f"{type(exc).__name__}: {exc}") from exc

    async def _connect(self) -> None:
        use_zlib = self.zlib_stream
        url = gateway_url(self.resume_url or self.host, zlib_stream=use_zlib)
        resuming = self.can_resume
        proxy = await self._current_proxy()
        log.debug(
            "connecting [%s]%s%s",
            url,
            " (RESUME)" if resuming else " (IDENTIFY)",
            f" via {mask_proxy(proxy)}" if proxy else "",
        )

        self.connect_count += 1
        ws = await self._open(url, proxy)
        self._proxy_in_use = proxy
        self._ws = ws
        self._last_ack = time.monotonic()
        self._last_heartbeat = 0.0
        inflater =ZlibStreamInflater() if use_zlib else None
        closed_exc: Optional[BaseException] = None
        try:
            await self._event_loop(ws, inflater)
        except _CLOSED_ERRORS as exc:
            closed_exc = exc
        finally:
            self._ws = None
            if self.disconnected_since is None:
                self.disconnected_since = time.time()
            await self._cancel_task("_heartbeat_task")
            # A RESUME on the next socket sends op 3 itself.
            await self._cancel_task("_presence_task")
            # Read before our own close below, so a code we sent is not taken for Discord's.
            code, reason = _close_info(ws, closed_exc)
            try:
                await ws.close(code=RESUME_CLOSE_CODE)
            except Exception:
                pass

        if self._capture is not None:
            self._capture.ws_closed(self._capture_ws, code, reason)
        self._handle_close(code, reason)

    def _handle_close(self, code: Optional[int], reason: str) -> None:
        if code == CloseCode.RATE_LIMITED:
            log.warning("gateway rate limited (4008)")
        elif code in _SESSION_RESET_CODES:
            log.warning("session cannot resume (%s), starting fresh", code)
            self._reset_session()
        elif code in FATAL_CLOSE_CODES:
            log.error("fatal gateway close code=%s reason=%s, stopping", code, reason)
            self.fatal_close = (code, reason)
            self._stopped = True
        elif self._stopped and code == 1000:
            log.debug("gateway closed by client (code=%s)", code)
        else:
            log.info("websocket closed (code=%s reason=%r)", code, reason)

    def _reset_session(self) -> None:
        self.session_id = None
        self.resume_url = None
        self.sequence = None

    async def _event_loop(self, ws: Any, inflater: Optional[ZlibStreamInflater]) -> None:
        compressed = 0
        async for raw in ws:
            started = time.perf_counter()
            if isinstance(raw, (bytes, bytearray, memoryview)):
                compressed += len(raw)
                if inflater is None:
                    text: Optional[str] = bytes(raw).decode("utf-8", errors="replace")
                else:
                    try:
                        text = inflater.feed(bytes(raw))
                    except zlib.error as exc:
                        log.warning(
                            "zlib-stream inflate failed (%s), falling back to plain json", exc
                        )
                        self.zlib_stream = False
                        await ws.close(code=RESUME_CLOSE_CODE)
                        return
                    if text is None:
                        continue
            else:
                text = raw
                compressed = len(raw)
            try:
                msg = json.loads(text)
            except (TypeError, ValueError):
                log.warning("non-JSON frame received")
                compressed = 0
                continue
            self.last_frame_stats = (
                compressed,
                len(text),
                (time.perf_counter() - started) * 1000,
                int(time.time() * 1000),
            )
            if self._capture is not None:
                binary = isinstance(raw, (bytes, bytearray, memoryview))
                self._capture.ws_recv(
                    self._capture_ws,
                    msg,
                    size=compressed,
                    decoded_size=len(text.encode("utf-8")) if binary else None,
                    binary=binary,
                )
            compressed = 0
            if not isinstance(msg, dict):
                continue
            if await self._handle_frame(ws, msg):
                return

    async def _handle_frame(self, ws: Any, msg: Dict[str, Any]) -> bool:
        """Act on one decoded frame. True when this socket is finished."""
        op = msg.get("op")
        s = msg.get("s")
        t = msg.get("t")
        d = msg.get("d")

        if s is not None:
            self.sequence = s

        if op == GatewayOp.HELLO:
            await self._handle_hello(d or {})
        elif op == GatewayOp.HEARTBEAT:
            await self._send_heartbeat()
        elif op == GatewayOp.HEARTBEAT_ACK:
            now = time.monotonic()
            if self._last_heartbeat:
                self.latency = now - self._last_heartbeat
            self._last_ack = now
            log.debug("heartbeat ack, latency %.1fms", self.latency * 1000)
        elif op == GatewayOp.DISPATCH:
            await self._handle_dispatch(t, d)
        elif op == GatewayOp.INVALID_SESSION:
            log.warning("invalid session (resumable=%s)", d)
            if not d:
                self._reset_session()
            await asyncio.sleep(random.uniform(*self._invalid_session_delay))
            if d:
                await ws.close(code=RESUME_CLOSE_CODE)
            else:
                await ws.close()
            return True
        elif op == GatewayOp.RECONNECT:
            log.info("server requested reconnect")
            await ws.close(code=RESUME_CLOSE_CODE)
            return True
        return False

    # HELLO / Heartbeat

    async def _handle_hello(self, d: Dict[str, Any]) -> None:
        self.heartbeat_interval = float(d.get("heartbeat_interval", 41250)) / 1000.0
        log.debug("hello, heartbeat interval %.2fs", self.heartbeat_interval)

        await self._cancel_task("_heartbeat_task")

        ws = self._ws
        await self.send_connect_frames()
        if self._ws is ws:
            # The first beat is jittered, as the gateway docs ask.
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(ws, self.heartbeat_interval * random.random())
            )

    @property
    def can_resume(self) -> bool:
        return bool(self.session_id and self.sequence is not None)

    async def send_connect_frames(self) -> bool:
        """What the web client sends on HELLO, before its first op 40: RESUME
        when the session can resume, else IDENTIFY. True when it resumed."""
        # Copycord is a tab that loaded unfocused and stays so. Such a tab never
        # makes a heartbeat session id: no op 41, on connect or after.
        if self.can_resume:
            # The web client's order: op 6, op 3 (the current presence), op 4,
            # then op 40 a while later. op 37 for every subscribed guild goes
            # before op 40, a deliberate difference: the browser resubscribes
            # only the guild it has open, and Copycord needs them all.
            await self._send_resume()
            await self._send_presence_update()
            await self._send_voice_state()
            await self._send_resume_subscriptions()
            return True
        # The web client's order: op 2, op 4, then op 40 a while later.
        await self._send_identify()
        await self._send_voice_state()
        return False

    async def _heartbeat_loop(self, ws: Any, first_delay: float) -> None:
        await asyncio.sleep(first_delay)
        while self._ws is ws:
            await self._send_heartbeat()
            if time.monotonic() - self._last_ack > self.heartbeat_interval * 2:
                log.warning("no heartbeat ack, closing zombied connection")
                if self._ws is ws:
                    await ws.close(code=RESUME_CLOSE_CODE)
                return
            await asyncio.sleep(self.heartbeat_interval)

    def _heartbeat_payload(self) -> Dict[str, Any]:
        """op 40, always inactive: an unfocused tab is never foregrounded (the
        overnight capture had qos.active false on every beat until focus)."""
        return {
            "op": GatewayOp.QOS_HEARTBEAT,
            "d": {
                "seq": self.sequence,
                "qos": {"active": False, "ver": QOS_VERSION, "reasons": []},
            },
        }

    async def _send_heartbeat(self) -> None:
        self._last_heartbeat = time.monotonic()
        await self._send_bypass(self._heartbeat_payload())

    # IDENTIFY / RESUME

    def _gateway_properties(self) -> Dict[str, Any]:
        return dict(fingerprint.gateway_properties(self.token))

    async def _send_identify(self) -> None:
        props = self._gateway_properties()
        self.identify_properties = dict(props)
        payload = {
            "op": GatewayOp.IDENTIFY,
            "d": {
                "token": self.token,
                "capabilities": self.capabilities,
                "properties": props,
                # The web client's; the real status follows in op 3 about 90 s after READY.
                "presence": {
                    "status": "unknown",
                    "since": 0,
                    "activities": [],
                    "afk": False,
                },
                # Payload compression; transport compression is the URL's `compress`.
                "compress": False,
                "client_state": {"guild_versions": {}},
                # Last in `d`: the last READY's qos_token, wrapped (qos.py).
                "qos_token": qos.identify_token(self.token, self.user_id),
            },
        }
        raw = json.dumps(payload).encode("utf-8")
        self.identify_sizes = (len(raw), len(zlib.compress(raw)))
        await self._send(payload)

    async def _send_voice_state(self) -> None:
        await self._send({"op": GatewayOp.VOICE_STATE_UPDATE, "d": dict(IDLE_VOICE_STATE)})

    async def _send_resume(self) -> None:
        await self._send(
            {
                "op": GatewayOp.RESUME,
                "d": {
                    "token": self.token,
                    "session_id": self.session_id,
                    "seq": self.sequence,
                },
            }
        )

    async def _send_resume_subscriptions(self) -> None:
        """op 37 in the full shape for every guild the session was subscribed to."""
        ids = [g for g in self._guild_ids if g in self._subscribed_guilds]
        ids += sorted(self._subscribed_guilds.difference(ids))
        for frame in build_subscription_frames(ids, RESUME_SUBSCRIPTION_FEATURES):
            await self._send(frame)

    # Dispatch

    def _remember_guild(self, gid: Any) -> None:
        gid = str(gid)
        if gid not in self._guild_ids:
            self._guild_ids.append(gid)

    async def _handle_dispatch(self, event: Optional[str], data: Any) -> None:
        if event is None:
            return
        # Only session events: message, typing and member traffic would bury the
        # sent-frame lines DEBUG exists for.
        if event in SESSION_DISPATCHES:
            log.debug("DISPATCH %s", event)
        data_dict = data if isinstance(data, dict) else {}

        if event in ("READY", "RESUMED"):
            self.disconnected_since = None
            self._proxy_ok()

        if event == "READY":
            self.session_id = data_dict.get("session_id")
            self.resume_url = data_dict.get("resume_gateway_url")
            self._guild_ids = []
            for g in data_dict.get("guilds") or []:
                if isinstance(g, dict) and g.get("id"):
                    self._remember_guild(g["id"])
            user = data_dict.get("user") or {}
            if user.get("id"):
                self.user_id = str(user["id"])
            # The next fresh IDENTIFY sends it back.
            qos.remember_ready(self.token, data_dict)
            log.debug(
                "READY user=%s id=%s guilds=%d session=%s resume=%s",
                user.get("username"),
                user.get("id"),
                len(self._guild_ids),
                self.session_id,
                self.resume_url,
            )
            # An unfocused tab's one op 3 comes about 90 s after READY.
            await self._cancel_task("_presence_task")
            self._presence_task = asyncio.create_task(
                self._delayed_presence(self._ws, self.presence_delay)
            )
            # A new session keeps no subscriptions from the old one.
            self._subscribed_guilds.clear()
            await self.subscribe_guilds(self._guild_ids, force=True)

        elif event == "RESUMED":
            # op 37 already went out right after op 6.
            log.info("RESUMED session=%s seq=%s", self.session_id, self.sequence)

        elif event == "GUILD_CREATE":
            gid = data_dict.get("id")
            if gid:
                self._remember_guild(gid)
                await self.subscribe_guilds([gid])

        elif event == "GUILD_DELETE":
            gid = data_dict.get("id")
            # unavailable means an outage, not a guild we left.
            if gid and not data_dict.get("unavailable"):
                gid = str(gid)
                if gid in self._guild_ids:
                    self._guild_ids.remove(gid)
                self._subscribed_guilds.discard(gid)

        for handler in self._handlers.get(event, []):
            try:
                await handler(data)
            except Exception:
                log.exception("error in handler for %s", event)
        for handler in self._dispatch_handlers:
            try:
                await handler(event, data)
            except Exception:
                log.exception("error in dispatch handler for %s", event)

    # Presence update (op 3)

    async def _delayed_presence(self, ws: Any, delay: float) -> None:
        """op 3 once, ``delay`` seconds on, if this socket is still the live one."""
        await asyncio.sleep(delay)
        if self._ws is ws:
            await self._send_presence_update()

    async def _send_presence_update(self) -> None:
        activities: List[Dict[str, Any]] = []
        if self.custom_status:
            activities.append(
                {
                    "type": 4,
                    "state": self.custom_status,
                    "name": "Custom Status",
                    "id": "custom",
                }
            )
        if self.activity:
            activities.append(self.activity)
        # The web client's shape; an unfocused tab reports afk. The status is
        # the configured one (online by default), never a switch to idle.
        await self._send(
            {
                "op": GatewayOp.PRESENCE_UPDATE,
                "d": {
                    "status": self.status,
                    "since": 0,
                    "activities": activities,
                    "afk": True,
                },
            }
        )

    # Helpers

    async def _cancel_task(self, attr: str) -> None:
        task: Optional[asyncio.Task] = getattr(self, attr)
        setattr(self, attr, None)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def _send(self, payload: Dict[str, Any]) -> None:
        if self._ws is None:
            return
        await self._ratelimiter.block()
        ws = self._ws
        if ws is None:
            return
        text = json.dumps(payload)
        try:
            await ws.send(text)
        except _CLOSED_ERRORS:
            log.warning("tried to send on a closed socket")
            return
        self._log_sent(payload, text)

    async def _send_bypass(self, payload: Dict[str, Any]) -> None:
        """Send without the ratelimiter, for heartbeats."""
        ws = self._ws
        if ws is None:
            return
        text = json.dumps(payload)
        try:
            await ws.send(text)
        except _CLOSED_ERRORS:
            log.warning("tried to send heartbeat on closed socket")
            return
        self._log_sent(payload, text)

    def _log_sent(self, payload: Dict[str, Any], text: str) -> None:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("sent %s", frame_summary(payload))
        if self._capture is not None:
            self._capture.ws_sent(self._capture_ws, payload, text)


__all__ = [
    "GATEWAY_HOST",
    "CAPABILITIES",
    "SUBSCRIPTION_FEATURES",
    "RESUME_SUBSCRIPTION_FEATURES",
    "MAX_SUBSCRIPTION_PAYLOAD",
    "PRESENCE_DELAY",
    "RESUME_CLOSE_CODE",
    "ZLIB_SUFFIX",
    "CurlWebSocket",
    "GatewayClient",
    "GatewayDialError",
    "ProxyTunnelError",
    "SocketClosed",
    "ZlibStreamInflater",
    "build_subscription_frames",
    "curl_proxy_url",
    "frame_summary",
    "gateway_url",
    "mask_proxy",
    "open_websocket",
]
