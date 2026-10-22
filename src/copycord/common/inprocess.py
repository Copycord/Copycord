# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
In-process transport used when admin, server and client share one process.

WebsocketManager and AdminBus deliver through this registry instead of
loopback sockets. Each delivery runs on the loop the endpoint registered on,
and messages are JSON round-tripped so no object is shared between threads.
Off unless enable() is called.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from concurrent.futures import Future as ConcurrentFuture
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

Handler = Callable[[dict], Awaitable[Optional[dict]]]
Key = Union[str, Tuple[str, int]]

BUS_PATH = "/bus"
OUT_PATH = "/ws/out"


class EndpointUnavailable(OSError):
    """No live endpoint for the URL; callers treat it like a refused connect."""


def _dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"))
    except Exception as e:
        return f'{{"ok":false,"error":"json-dumps-failed:{e!r}"}}'


def key_for_url(url: Optional[str]) -> Optional[Key]:
    """Admin bus URLs key by path, role servers by port (sender and listener hosts differ)."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        path = (parsed.path or "").rstrip("/")
        if path in (BUS_PATH, OUT_PATH):
            return path
        port = parsed.port
    except Exception:
        return None
    if port is None:
        port = 443 if parsed.scheme in ("wss", "https") else 80
    return ("port", int(port))


@dataclass
class _Endpoint:
    handler: Handler
    loop: asyncio.AbstractEventLoop


@dataclass
class _Feed:

    attach: Callable[[Any], Awaitable[None]]
    detach: Callable[[Any], Awaitable[None]]
    loop: asyncio.AbstractEventLoop


async def serve_one(handler: Handler, raw: str, logger=None) -> str:
    """Handle one message the way WebsocketManager._serve_loop does."""
    try:
        req = json.loads(raw)
    except Exception:
        return _dumps({"ok": False, "error": "bad-json"})
    if not isinstance(req, dict):
        req = {"type": "(none)", "data": req}
    rid = req.get("rid") or str(uuid.uuid4())
    req["rid"] = rid
    try:
        response = await handler(req)
        if response is None:
            response = {"ok": True}
        if isinstance(response, dict):
            response.setdefault("rid", rid)
    except Exception:
        if logger is not None:
            logger.exception(
                "Error in in-process handler type=%s rid=%s", req.get("type"), rid
            )
        response = {"ok": False, "error": "handler-failed", "rid": rid}
    return _dumps(response)


class LoopSink:
    """Socket stand-in for BusHub.ui_sockets that queues text onto the subscriber's loop."""

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
        self._loop = loop
        self._queue = queue
        self.closed = False

    async def send_text(self, text: str) -> None:
        if self.closed:
            raise RuntimeError("in-process subscriber closed")
        # Raises RuntimeError once the subscriber's loop is closed, so the hub drops this sink.
        self._loop.call_soon_threadsafe(self._queue.put_nowait, text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = True


def _wake(fut: asyncio.Future) -> None:
    if not fut.done():
        fut.set_result(None)


class Registry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._endpoints: Dict[Key, _Endpoint] = {}
        self._feed: Optional[_Feed] = None
        self._waiters: Dict[Key, List[Tuple[asyncio.AbstractEventLoop, asyncio.Future]]] = {}

    def register(self, key: Key, handler: Handler) -> _Endpoint:
        ep = _Endpoint(handler, asyncio.get_running_loop())
        with self._lock:
            self._endpoints[key] = ep
            waiters = self._waiters.pop(key, [])
        for loop, fut in waiters:
            try:
                loop.call_soon_threadsafe(_wake, fut)
            except RuntimeError:
                pass  # the waiter's loop is closed
        return ep

    async def wait_registered(self, url: Optional[str], timeout: float) -> bool:
        """Wait up to timeout for url's endpoint to register; True once it is live."""
        key = key_for_url(url)
        if key is None:
            return False
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        entry = (loop, fut)
        with self._lock:
            ep = self._endpoints.get(key)
            if ep is not None and not ep.loop.is_closed():
                return True
            self._waiters.setdefault(key, []).append(entry)
        try:
            await asyncio.wait_for(fut, max(0.0, timeout))
        except asyncio.TimeoutError:
            pass
        finally:
            with self._lock:
                waiting = self._waiters.get(key)
                if waiting is not None:
                    if entry in waiting:
                        waiting.remove(entry)
                    if not waiting:
                        del self._waiters[key]
        return self._live(key) is not None

    def unregister(self, key: Key, ep: Optional[_Endpoint] = None) -> None:
        with self._lock:
            cur = self._endpoints.get(key)
            # A restarted role may already have re-registered on its new loop.
            if cur is not None and (ep is None or cur is ep):
                del self._endpoints[key]

    def register_feed(self, attach, detach) -> _Feed:
        feed = _Feed(attach, detach, asyncio.get_running_loop())
        with self._lock:
            self._feed = feed
        return feed

    def unregister_feed(self, feed: Optional[_Feed] = None) -> None:
        with self._lock:
            if self._feed is not None and (feed is None or self._feed is feed):
                self._feed = None

    def _live(self, key: Optional[Key]) -> Optional[_Endpoint]:
        if key is None:
            return None
        with self._lock:
            ep = self._endpoints.get(key)
            if ep is not None and ep.loop.is_closed():
                del self._endpoints[key]
                ep = None
        return ep

    def has(self, url: Optional[str]) -> bool:
        return self._live(key_for_url(url)) is not None

    def feed(self) -> Optional[_Feed]:
        with self._lock:
            feed = self._feed
            if feed is not None and feed.loop.is_closed():
                self._feed = None
                feed = None
        return feed

    def dispatch(self, url: Optional[str], payload: dict, logger=None) -> ConcurrentFuture:
        """Schedule a message on the endpoint's loop; a Future of the reply text."""
        key = key_for_url(url)
        ep = self._live(key)
        if ep is None:
            raise EndpointUnavailable(f"no in-process endpoint for {url}")
        coro = serve_one(ep.handler, _dumps(payload), logger)
        try:
            return asyncio.run_coroutine_threadsafe(coro, ep.loop)
        except RuntimeError as e:
            coro.close()
            self.unregister(key, ep)
            raise EndpointUnavailable(f"in-process endpoint for {url} is gone: {e}")

    async def request(
        self, url: Optional[str], payload: dict, timeout: Optional[float] = None, logger=None
    ) -> Optional[dict]:
        """Await the reply on the caller's loop, whatever loop the handler runs on."""
        fut = asyncio.wrap_future(self.dispatch(url, payload, logger))
        if timeout is not None:
            # Shielded: a timeout cancels only the waiter, not the handler.
            raw = await asyncio.wait_for(asyncio.shield(fut), timeout)
        else:
            raw = await fut
        return json.loads(raw)


_registry: Optional[Registry] = None
_state_lock = threading.Lock()


def enable() -> Registry:
    """Switch WebsocketManager/AdminBus to in-process delivery for this process."""
    global _registry
    with _state_lock:
        if _registry is None:
            _registry = Registry()
        return _registry


def disable() -> None:
    global _registry
    with _state_lock:
        _registry = None


def active() -> Optional[Registry]:
    return _registry


async def admin_request(url: str, payload: dict, timeout: float) -> Optional[dict]:
    """The in-process reply for url, or None so the caller falls back to a socket."""
    reg = _registry
    if reg is None or not reg.has(url):
        return None
    t0 = time.monotonic()
    try:
        return await reg.request(url, payload, timeout=timeout)
    except EndpointUnavailable:
        return None
    except asyncio.TimeoutError:
        raise TimeoutError(
            f"in-process request timed out after {(time.monotonic() - t0):.2f}s"
        ) from None
