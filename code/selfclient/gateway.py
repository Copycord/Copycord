# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""User-token gateway over raw websockets.

Ported from wizard/core/gateway.py, which has been holding user-token sessions
in production: same handshake, same heartbeat and zombie rule, same op 37
subscription frames, same resume discipline, same close-code handling. What is
added here:

  * zlib-stream transport compression (``compress=zlib-stream``), which is what
    the web client connects with. One ``zlib.decompressobj`` per socket, fed
    every binary frame, inflated whenever the buffer ends in the 4-byte
    Z_SYNC_FLUSH marker. ``zlib_stream=False`` is the plain-json fallback
    wizard runs on, and a socket whose stream fails to inflate falls back to
    it for the reconnects that follow.
  * op 8 REQUEST_GUILD_MEMBERS, which the admin standalone scraper's gateway
    (admin/standalone_scraper.py) drives, so it can run on this gateway instead
    of keeping one of its own.
  * proxy dialing that works on the pinned websockets 11, which has no
    ``proxy=`` argument: the tunnel is opened here (HTTP CONNECT, or SOCKS via
    python-socks when it is installed) and handed to websockets as ``sock``.

The proxy source is common/proxy_pool's per-token lease, the same one the
curl_cffi REST session leases from, so the socket and the REST calls leave from
one exit IP. A dial that fails rotates to a fresh lease; a Discord-side close
does not, because the proxy is evidently fine.

Events go out raw: ``on(event, handler)`` gets the dispatch ``d`` dict, and
``on_dispatch(handler)`` gets every ``(event, d)``. Turning those into models is
selfclient.events; caching them is the Client's job.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import random
import socket
import time
import uuid
import zlib
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlencode, urlsplit, urlunsplit

from . import fingerprint
from .enums import API_VERSION, FATAL_CLOSE_CODES, Capabilities, CloseCode, GatewayOp
from .errors import ConnectionClosed, LoginFailure

try:
    import websockets
    from websockets.exceptions import ConnectionClosed as _WSClosed

    _WS_MAJOR = int(str(getattr(websockets, "__version__", "11")).split(".")[0])
except ImportError:  # the tests drive a fake socket and do not need it
    websockets = None
    _WS_MAJOR = 0

    class _WSClosed(Exception):  # type: ignore[no-redef]
        rcvd = None
        sent = None


try:
    from python_socks.async_.asyncio import Proxy as _SocksProxy
except ImportError:  # only needed for socks4/socks5 proxies
    _SocksProxy = None

log = logging.getLogger(__name__)

GATEWAY_HOST = "wss://gateway.discord.gg/"

# websockets 14 replaced the legacy client and renamed its header argument.
_HEADERS_KWARG = "additional_headers" if _WS_MAJOR >= 14 else "extra_headers"

MAX_FRAME_SIZE = 10 * 1024 * 1024
OPEN_TIMEOUT = 20.0

# The trailer zlib puts on every Z_SYNC_FLUSH. Discord ends each logical
# message with one, so a buffer ending in it is a complete message.
ZLIB_SUFFIX = b"\x00\x00\xff\xff"

# The close code for a socket we mean to RESUME on. Discord invalidates the
# session when the client closes with 1000 or 1001 (discord-api-docs #1472), so
# a reconnect closed with websockets' default 1000 came back to "invalid session
# (resumable=False)" and a fresh IDENTIFY, and every event in the gap was lost.
# Any 4000-range code keeps the session; 1000 is only for a deliberate goodbye.
RESUME_CLOSE_CODE = CloseCode.RESUME

# Close codes after which the session cannot be resumed and a fresh IDENTIFY is
# the only way back.
_SESSION_RESET_CODES = frozenset({CloseCode.INVALID_SEQ, CloseCode.SESSION_TIMED_OUT})

# What the official web client asks for, minus bit 15, CHANNEL_OBFUSCATION.
# With that bit set the gateway replaces the name and topic of every channel the
# account cannot view with the literal string `___hidden___`, and the sitemap
# clones that placeholder onto the destination server. Clearing it is what makes
# READY carry the real names.
CAPABILITIES = Capabilities.DEFAULT  # 1734653

# The gateway auto-subscribes a user account to its guilds on connect, but that
# default tier only covers top-level channels: messages in threads the account
# hasn't joined are never dispatched. Opting into the `threads` feature via op 37
# is what turns them on (and makes the gateway send THREAD_LIST_SYNC).
#
# DO NOT SPLIT `typing` AND `threads` ACROSS FRAMES. Once a client subscribes to
# `typing` for a guild it counts as subscribed to that guild indefinitely, and a
# guild that was already subscribed will not emit THREAD_LIST_SYNC when `threads`
# is added later; recovering from that needs an unsubscribe/resubscribe cycle.
# So every guild's *first* frame must carry both flags, which is why these ship
# as one fixed feature set rather than as caller-selectable options.
SUBSCRIPTION_FEATURES = {
    "typing": True,
    "threads": True,
    "activities": True,
    "member_updates": True,
}

# Discord drops oversized op 37 frames, so batches are split to stay under this.
MAX_SUBSCRIPTION_PAYLOAD = 15360

# op 41 cadence, the web client's.
TIME_SPENT_INTERVAL = 30 * 60

EventHandler = Callable[[Any], Awaitable[Any]]
DispatchHandler = Callable[[str, Any], Awaitable[Any]]
Connector = Callable[[str, Dict[str, str], Optional[str]], Awaitable[Any]]


def gateway_url(host: str = GATEWAY_HOST, *, zlib_stream: bool = False) -> str:
    """Return `host` carrying this client's `encoding`/`v`/`compress` query.

    Whatever query `host` already has is discarded, so this is the single place
    that decides the query string. It matters for resumes: READY's
    `resume_gateway_url` arrives as a bare host with no query at all, and
    reconnecting to it verbatim silently drops to the gateway's default API
    version: no error, no close code, just no more thread events.

    The order is the web client's (`encoding`, `v`, `compress`).
    """
    parts = urlsplit(host or GATEWAY_HOST)
    params: Dict[str, Any] = {"encoding": "json", "v": API_VERSION}
    if zlib_stream:
        params["compress"] = "zlib-stream"
    path = parts.path or "/"
    return urlunsplit(parts._replace(path=path, query=urlencode(params)))


def build_subscription_frames(guild_ids: Iterable[Any]) -> List[Dict[str, Any]]:
    """Batch guild ids into as few op 37 frames as the size cap allows.

    Each guild appears in exactly one frame with the full feature set, so a
    guild's `typing` and `threads` never land in different frames.
    """
    frames: List[Dict[str, Any]] = []
    batch: Dict[str, Dict[str, bool]] = {}
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
        batch[gid] = dict(SUBSCRIPTION_FEATURES)
        # A lone guild that somehow blows the cap still ships: nothing to split.
        if (
            len(batch) > 1
            and len(json.dumps({"subscriptions": batch})) > MAX_SUBSCRIPTION_PAYLOAD
        ):
            del batch[gid]
            flush()
            batch[gid] = dict(SUBSCRIPTION_FEATURES)
    flush()
    return frames


class ZlibStreamInflater:
    """One socket's zlib-stream context.

    The whole connection is a single deflate stream, so the decompressor must
    live as long as the socket and see every frame in order. A message can span
    several frames; it is complete when the buffered bytes end in ZLIB_SUFFIX.
    """

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
    """Sliding window: 110 sends per 60 s. Heartbeats bypass this entirely."""

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


async def _http_connect_tunnel(
    proxy: str, host: str, port: int, *, timeout: float
) -> socket.socket:
    """Open a CONNECT tunnel to host:port through an HTTP proxy.

    ``https://`` proxy URLs are dialed the same way: in proxy lists that scheme
    almost always names a CONNECT-capable HTTP proxy, not TLS to the proxy.
    """
    parts = urlsplit(proxy)
    if not parts.hostname:
        raise ProxyTunnelError(f"unparsable proxy url {mask_proxy(proxy)}")
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(
        parts.hostname, parts.port or 8080, type=socket.SOCK_STREAM
    )
    sock: Optional[socket.socket] = None
    last_exc: Optional[BaseException] = None
    for family, type_, proto, _, addr in infos:
        candidate = socket.socket(family, type_, proto)
        candidate.setblocking(False)
        try:
            await asyncio.wait_for(loop.sock_connect(candidate, addr), timeout)
        except (OSError, asyncio.TimeoutError) as exc:
            candidate.close()
            last_exc = exc
            continue
        sock = candidate
        break
    if sock is None:
        raise ProxyTunnelError(f"could not reach proxy {mask_proxy(proxy)}: {last_exc!r}")

    try:
        lines = [f"CONNECT {host}:{port} HTTP/1.1", f"Host: {host}:{port}"]
        if parts.username is not None:
            creds = f"{unquote(parts.username)}:{unquote(parts.password or '')}"
            token = base64.b64encode(creds.encode()).decode()
            lines.append(f"Proxy-Authorization: Basic {token}")
        request = ("\r\n".join(lines) + "\r\n\r\n").encode()
        await asyncio.wait_for(loop.sock_sendall(sock, request), timeout)

        response = b""
        while b"\r\n\r\n" not in response:
            chunk = await asyncio.wait_for(loop.sock_recv(sock, 4096), timeout)
            if not chunk:
                raise ProxyTunnelError("proxy closed the connection during CONNECT")
            response += chunk
            if len(response) > 65536:
                raise ProxyTunnelError("oversized CONNECT response")
        head, _, rest = response.partition(b"\r\n\r\n")
        status_line = head.split(b"\r\n", 1)[0].decode("latin-1")
        fields = status_line.split()
        if len(fields) < 2 or fields[1] != "200":
            raise ProxyTunnelError(f"proxy CONNECT refused: {status_line!r}")
        # The client speaks first in TLS, so nothing may follow the 200 yet.
        if rest:
            raise ProxyTunnelError("unexpected bytes after CONNECT response")
        return sock
    except BaseException:
        sock.close()
        raise


async def open_proxy_tunnel(
    proxy: str, host: str, port: int, *, timeout: float = OPEN_TIMEOUT
) -> socket.socket:
    """A connected socket to host:port through ``proxy``, ready for TLS."""
    scheme = (urlsplit(proxy).scheme or "http").lower()
    try:
        if scheme in ("http", "https"):
            return await _http_connect_tunnel(proxy, host, port, timeout=timeout)
        if scheme.startswith("socks"):
            if _SocksProxy is None:
                raise ProxyTunnelError("SOCKS proxy requires the python-socks package")
            return await _SocksProxy.from_url(proxy).connect(
                dest_host=host, dest_port=port, timeout=timeout
            )
    except GatewayDialError:
        raise
    except Exception as exc:
        raise ProxyTunnelError(f"proxy {mask_proxy(proxy)} failed: {exc!r}") from exc
    raise ProxyTunnelError(f"unsupported proxy scheme {scheme!r}")


async def open_websocket(
    url: str,
    headers: Dict[str, str],
    proxy: Optional[str] = None,
    *,
    max_size: int = MAX_FRAME_SIZE,
    open_timeout: float = OPEN_TIMEOUT,
) -> Any:
    """Open the gateway socket, through ``proxy`` when one is given."""
    if websockets is None:
        raise GatewayDialError("the websockets package is not installed")
    kwargs: Dict[str, Any] = {
        _HEADERS_KWARG: headers,
        "max_size": max_size,
        "open_timeout": open_timeout,
    }
    if _WS_MAJOR >= 15:
        # 15+ picks a proxy up from the environment on its own; the proxy here
        # is the token's lease and nothing else.
        kwargs["proxy"] = None
    sock = None
    if proxy:
        parts = urlsplit(url)
        secure = parts.scheme == "wss"
        host = parts.hostname or ""
        port = parts.port or (443 if secure else 80)
        sock = await open_proxy_tunnel(proxy, host, port, timeout=open_timeout)
        kwargs["sock"] = sock
        if secure:
            kwargs["ssl"] = True
            kwargs["server_hostname"] = host
    try:
        return await websockets.connect(url, **kwargs)
    except BaseException:
        if sock is not None:
            sock.close()
        raise


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

    ``run()`` connects and keeps the session alive until ``close()``, or until
    Discord closes it with a code no reconnect can fix, in which case ``run()``
    raises: LoginFailure for 4004, ConnectionClosed for the other fatal codes.
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
    ) -> None:
        self.token = token
        self.status = status
        self.custom_status = custom_status
        self.activity = activity
        self.reconnect = reconnect
        self.base_reconnect_delay = base_reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        #: Whether the next socket asks for zlib-stream. Cleared, for good, if a
        #: stream ever fails to inflate.
        self.zlib_stream = zlib_stream
        self.host = host
        self.capabilities = capabilities
        #: A fixed proxy, used when no pool is given (or the pool is disabled).
        self.proxy = proxy
        #: common/proxy_pool.ProxyPool or anything with its lease /
        #: report_failure / report_success / enabled surface.
        self.proxy_pool = proxy_pool
        self._connector = connector

        # Per process, not per socket: a resume is the same client continuing.
        # Registered with the header builder on READY so the REST
        # super-properties quote the same heartbeat session as the socket.
        self.heartbeat_session_id = str(uuid.uuid4())

        self._guild_ids: List[str] = []
        self._subscribed_guilds: set = set()
        self._ws: Any = None
        self._proxy_in_use: Optional[str] = None
        self.session_id: Optional[str] = None
        self.resume_url: Optional[str] = None
        self.sequence: Optional[int] = None
        self.heartbeat_interval: float = 41.25
        self.latency: float = float("inf")
        self._last_ack: float = time.monotonic()
        self._last_heartbeat: float = 0.0
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._time_spent_task: Optional[asyncio.Task] = None
        self._stopped: bool = False
        # Seconds range slept after an INVALID_SESSION, before reconnecting.
        self._invalid_session_delay: Tuple[float, float] = (1.0, 5.0)
        #: (close code, reason) once Discord has closed the session for good: a
        #: bad token, bad intents, a bad shard. Nothing reconnects after one.
        self.fatal_close: Optional[Tuple[Optional[int], str]] = None
        #: Wall-clock time the gateway last stopped being connected, None while a
        #: session is live. Starts set: a client that has not reached READY yet
        #: is down as far as anything reading this is concerned.
        self.disconnected_since: Optional[float] = time.time()
        self.reconnect_count: int = 0
        #: Sockets opened so far, successful or not. Mostly for tests and logs.
        self.connect_count: int = 0
        self._ratelimiter = _GatewayRatelimiter()

        self._handlers: Dict[str, List[EventHandler]] = {}
        self._dispatch_handlers: List[DispatchHandler] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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
            # The build number feeds IDENTIFY; a stale one is a top detection
            # signal, so fetch it before the first socket rather than after.
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
                    # Failed before the handshake finished: most likely the proxy
                    # itself is dead (refused/timed out), so rotate to the next one.
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
            await self._cancel_task("_time_spent_task")

        if self.fatal_close is not None:
            code, reason = self.fatal_close
            if code == CloseCode.AUTHENTICATION_FAILED:
                raise LoginFailure("Improper token has been passed.")
            raise ConnectionClosed(code, reason)

    async def close(self) -> None:
        """Stop for good. Closes with 1000, so the session is not resumable."""
        self._stopped = True
        await self._cancel_task("_heartbeat_task")
        await self._cancel_task("_time_spent_task")
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
        """Opt into thread (and typing/activity/member) events for these guilds.

        Already-subscribed guilds are skipped unless `force`: a resumed session
        replays nothing server-side, so a resume re-sends the whole set.
        """
        ids = [str(g) for g in guild_ids if g]
        if not force:
            ids = [g for g in ids if g not in self._subscribed_guilds]
        frames = build_subscription_frames(ids)
        if not frames:
            return
        subscribed = 0
        for frame in frames:
            await self._send(frame)
            gids = frame["d"]["subscriptions"].keys()
            self._subscribed_guilds.update(gids)
            subscribed += len(gids)
        log.debug("subscribed to %d guild(s) in %d frame(s)", subscribed, len(frames))

    async def request_guild_members(
        self,
        guild_id: Any,
        *,
        query: str = "",
        limit: int = 0,
        presences: bool = False,
        user_ids: Optional[Iterable[Any]] = None,
        nonce: Optional[str] = None,
    ) -> None:
        """op 8: ask for members; the answer arrives as GUILD_MEMBERS_CHUNK."""
        d: Dict[str, Any] = {
            "guild_id": str(guild_id),
            "limit": int(limit),
            "presences": bool(presences),
        }
        if user_ids is not None:
            d["user_ids"] = [str(u) for u in user_ids]
        else:
            d["query"] = query
        if nonce is not None:
            d["nonce"] = nonce
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

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _ws_headers(self) -> Dict[str, str]:
        """Upgrade-request headers: the browser's UA and Origin."""
        ua = fingerprint.build_headers(self.token).get("User-Agent", "")
        return {"User-Agent": ua, "Origin": "https://discord.com"}

    async def _current_proxy(self) -> Optional[str]:
        pool = self.proxy_pool
        if pool is not None and getattr(pool, "enabled", False):
            try:
                return await pool.lease(self.token)
            except Exception as exc:
                log.warning("proxy lease failed: %r", exc)
                return None
        return self.proxy

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
        try:
            if self._connector is not None:
                return await self._connector(url, headers, proxy)
            return await open_websocket(url, headers, proxy)
        except GatewayDialError:
            raise
        except Exception as exc:
            raise GatewayDialError(f"{type(exc).__name__}: {exc}") from exc

    async def _connect(self) -> None:
        use_zlib = self.zlib_stream
        url = gateway_url(self.resume_url or self.host, zlib_stream=use_zlib)
        resuming = bool(self.session_id and self.sequence is not None)
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
        inflater = ZlibStreamInflater() if use_zlib else None
        closed_exc: Optional[BaseException] = None
        try:
            await self._event_loop(ws, inflater)
        except _WSClosed as exc:
            closed_exc = exc
        finally:
            # The socket belongs to this connection, not to the client. Cleared
            # in a finally because a close arrives as an exception at least as
            # often as it arrives as a return.
            self._ws = None
            if self.disconnected_since is None:
                self.disconnected_since = time.time()
            await self._cancel_task("_heartbeat_task")
            try:
                # A no-op on a socket that is already closed; otherwise keep
                # the session resumable.
                await ws.close(code=RESUME_CLOSE_CODE)
            except Exception:
                pass

        code, reason = _close_info(ws, closed_exc)
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
        else:
            log.info("websocket closed (code=%s reason=%r)", code, reason)

    def _reset_session(self) -> None:
        self.session_id = None
        self.resume_url = None
        self.sequence = None

    async def _event_loop(self, ws: Any, inflater: Optional[ZlibStreamInflater]) -> None:
        async for raw in ws:
            if isinstance(raw, (bytes, bytearray, memoryview)):
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
            try:
                msg = json.loads(text)
            except (TypeError, ValueError):
                log.warning("non-JSON frame received")
                continue
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

    # ------------------------------------------------------------------
    # HELLO / Heartbeat
    # ------------------------------------------------------------------

    async def _handle_hello(self, d: Dict[str, Any]) -> None:
        self.heartbeat_interval = float(d.get("heartbeat_interval", 41250)) / 1000.0
        log.debug("hello, heartbeat interval %.2fs", self.heartbeat_interval)

        await self._cancel_task("_heartbeat_task")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(self._ws))

        if self.session_id and self.sequence is not None:
            await self._send_resume()
        else:
            await self._send_identify()

    async def _heartbeat_loop(self, ws: Any) -> None:
        # First beat at interval * jitter, as the gateway docs ask and the web
        # client does: a fleet of clients reconnecting together must not beat
        # in lockstep.
        await asyncio.sleep(self.heartbeat_interval * random.random())
        while self._ws is ws:
            await self._send_heartbeat()
            if time.monotonic() - self._last_ack > self.heartbeat_interval * 2:
                log.warning("no heartbeat ack, closing zombied connection")
                if self._ws is ws:
                    await ws.close(code=RESUME_CLOSE_CODE)
                return
            await asyncio.sleep(self.heartbeat_interval)

    async def _send_heartbeat(self) -> None:
        self._last_heartbeat = time.monotonic()
        log.debug("heartbeat sent (seq=%s)", self.sequence)
        await self._send_bypass({"op": GatewayOp.HEARTBEAT, "d": self.sequence})

    # ------------------------------------------------------------------
    # IDENTIFY / RESUME
    # ------------------------------------------------------------------

    def _gateway_properties(self) -> Dict[str, Any]:
        # client_launch_id is already the REST super-properties' one: both come
        # out of the same cached per-token fingerprint.
        return dict(fingerprint.gateway_properties(self.token))

    async def _send_identify(self) -> None:
        props = self._gateway_properties()
        log.debug(
            "sending IDENTIFY (build=%s, os=%s, heartbeat_session=%s)",
            props.get("client_build_number"),
            props.get("os_version"),
            self.heartbeat_session_id,
        )
        await self._send(
            {
                "op": GatewayOp.IDENTIFY,
                "d": {
                    "token": self.token,
                    "capabilities": self.capabilities,
                    "properties": props,
                    "presence": {
                        "status": self.status,
                        "activities": [self.activity] if self.activity else [],
                        "since": 0,
                        "afk": False,
                    },
                    # Payload compression, which the web client leaves off; the
                    # transport compression is the `compress` URL parameter.
                    "compress": False,
                    "client_state": {
                        "guild_versions": {},
                        "highest_last_message_id": "0",
                        "read_state_version": 0,
                        "user_guild_settings_version": -1,
                        "user_settings_version": -1,
                        "private_channels_version": "0",
                        "api_code_version": 0,
                    },
                },
            }
        )

    async def _send_resume(self) -> None:
        log.debug("sending RESUME (session=%s, seq=%s)", self.session_id, self.sequence)
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

    # ------------------------------------------------------------------
    # OP 41: UPDATE_TIME_SPENT_SESSION_ID
    # ------------------------------------------------------------------

    async def _time_spent_loop(self) -> None:
        await self._send_time_spent()
        while True:
            await asyncio.sleep(TIME_SPENT_INTERVAL)
            await self._send_time_spent()

    async def _send_time_spent(self) -> None:
        log.debug("sending OP 41 (time spent session)")
        await self._send(
            {
                "op": GatewayOp.UPDATE_TIME_SPENT_SESSION_ID,
                "d": {
                    "initialization_timestamp": int(time.time() * 1000),
                    "session_id": self.heartbeat_session_id,
                    "client_launch_id": self._gateway_properties().get("client_launch_id"),
                },
            }
        )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _remember_guild(self, gid: Any) -> None:
        gid = str(gid)
        if gid not in self._guild_ids:
            self._guild_ids.append(gid)

    async def _handle_dispatch(self, event: Optional[str], data: Any) -> None:
        if event is None:
            return
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
            log.debug(
                "READY user=%s id=%s guilds=%d session=%s resume=%s",
                user.get("username"),
                user.get("id"),
                len(self._guild_ids),
                self.session_id,
                self.resume_url,
            )
            try:
                fingerprint.set_heartbeat_session_id(self.token, self.heartbeat_session_id)
            except Exception:
                pass
            await self._send_presence_update()
            # Fresh session: the gateway remembers no subscriptions from the old one.
            self._subscribed_guilds.clear()
            await self.subscribe_guilds(self._guild_ids, force=True)

            await self._cancel_task("_time_spent_task")
            self._time_spent_task = asyncio.create_task(self._time_spent_loop())

        elif event == "RESUMED":
            log.info("RESUMED session=%s seq=%s", self.session_id, self.sequence)
            await self.subscribe_guilds(self._guild_ids, force=True)

        elif event == "GUILD_CREATE":
            gid = data_dict.get("id")
            if gid:
                self._remember_guild(gid)
                await self.subscribe_guilds([gid])

        elif event == "GUILD_DELETE":
            gid = data_dict.get("id")
            # An outage marks a guild unavailable; it is still ours.
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

    # ------------------------------------------------------------------
    # Presence update (op 3)
    # ------------------------------------------------------------------

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
        await self._send(
            {
                "op": GatewayOp.PRESENCE_UPDATE,
                "d": {
                    "status": self.status,
                    "activities": activities,
                    "since": 0,
                    "afk": False,
                },
            }
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
        try:
            await ws.send(json.dumps(payload))
        except _WSClosed:
            log.warning("tried to send on a closed socket")

    async def _send_bypass(self, payload: Dict[str, Any]) -> None:
        """Send without the ratelimiter. Heartbeats only."""
        ws = self._ws
        if ws is None:
            return
        try:
            await ws.send(json.dumps(payload))
        except _WSClosed:
            log.warning("tried to send heartbeat on closed socket")


__all__ = [
    "GATEWAY_HOST",
    "CAPABILITIES",
    "SUBSCRIPTION_FEATURES",
    "MAX_SUBSCRIPTION_PAYLOAD",
    "RESUME_CLOSE_CODE",
    "ZLIB_SUFFIX",
    "GatewayClient",
    "GatewayDialError",
    "ProxyTunnelError",
    "ZlibStreamInflater",
    "build_subscription_frames",
    "gateway_url",
    "mask_proxy",
    "open_proxy_tunnel",
    "open_websocket",
]
