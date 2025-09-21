# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import time
import uuid
from typing import Any, Awaitable, Callable, Optional
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    ProtocolError,
    InvalidStatusCode,
)
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger(__name__)

MessageHandler = Callable[[dict], Awaitable[None]]

def _ptype(p: dict | None) -> str:
    try:
        return (p or {}).get("type") or "(none)"
    except Exception:
        return "(?)"


def _json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"))
    except Exception as e:
        return f'{{"ok":false,"error":"json-dumps-failed:{e!r}"}}'


def _bytes_len(s: str | bytes) -> int:
    if isinstance(s, bytes):
        return len(s)
    try:
        return len(s.encode("utf-8"))
    except Exception:
        return len(s)


class WebsocketManager:
    """
    - Outbound: fire-and-forget `send()` and request/response `request()`
    - Inbound: simple server (`start_server`) with per-message handler
    - Fast shutdown: call `begin_shutdown()` or `await stop()`
      to collapse retries and lower timeouts so the process exits quickly.
    """

    def __init__(
        self,
        send_url: str,
        listen_host: Optional[str] = None,  # optional
        listen_port: Optional[int] = None,  # optional
        logger: Optional[logging.Logger] = None,
    ):
        self.send_url = send_url
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.logger = logger or logging.getLogger("WebsocketManager")
        self._shutting_down = False

    # ---------- lifecycle ----------
    def begin_shutdown(self) -> None:
        """Mark the manager as shutting down; short-circuit retries/timeouts."""
        self._shutting_down = True

    async def stop(self) -> None:
        """Coroutine alias so callers can `await ws.stop()` during teardown."""
        self.begin_shutdown()

    # ---------- inbound server ----------
    async def start_server(
        self,
        handler: Callable[[dict], Awaitable[dict | None]],
    ) -> None:
        """
        Spins up a websockets.server using `handler` for each incoming JSON message.
        Runs until cancelled by the event loop.
        """
        server = await websockets.serve(
            lambda ws, path: self._serve_loop(ws, path, handler),
            self.listen_host,
            self.listen_port,
            max_size=None,
        )
        self.logger.debug("WS server listening on %s:%s", self.listen_host, self.listen_port)
        try:
            await asyncio.Future()  # run forever
        finally:
            self.logger.debug("WS server shutting down…")
            server.close()
            await server.wait_closed()
            self.logger.debug("WS server closed.")

    async def _serve_loop(
        self,
        ws: WebSocketServerProtocol,
        path: str,
        handler: Callable[[dict], Awaitable[dict | None]],
    ):
        """
        Minimal resilient loop:
        - graceful on peer close
        - no traceback spam when peer closes before our send
        - logs unexpected exceptions, but never blocks shutdown
        """
        peer = getattr(ws, "remote_address", None)
        self.logger.debug("[ws≺] connection open path=%s peer=%s", path, peer)
        try:
            while True:
                try:
                    t0 = time.monotonic()
                    raw = await ws.recv()
                    dt = (time.monotonic() - t0) * 1000
                    self.logger.debug("[ws←] recv bytes=%d ms=%.1f", _bytes_len(raw), dt)
                except ConnectionClosedOK:
                    self.logger.debug("[ws] peer closed (OK)")
                    break
                except ConnectionClosedError as e:
                    self.logger.info("[ws] peer closed with error: %s", e)
                    break

                try:
                    req = json.loads(raw)
                except Exception:
                    self.logger.debug("[ws] bad-json; echoing error")
                    if not await self._safe_send(ws, _json({"ok": False, "error": "bad-json"})):
                        break
                    continue

                rid = req.get("rid") or str(uuid.uuid4())
                req["rid"] = rid
                ptype = _ptype(req)
                self.logger.debug("[ws] handle type=%s rid=%s", ptype, rid)

                try:
                    t1 = time.monotonic()
                    response = await handler(req)
                    if response is None:
                        response = {"ok": True}
                    if isinstance(response, dict):
                        response.setdefault("rid", rid)
                    dt = (time.monotonic() - t1) * 1000
                    self.logger.debug(
                        "[ws] handler done type=%s rid=%s ms=%.1f ok=%s",
                        ptype, rid, dt, isinstance(response, dict) and response.get("ok"),
                    )
                except Exception:
                    self.logger.exception("Error in WS handler type=%s rid=%s", ptype, rid)
                    response = {"ok": False, "error": "handler-failed", "rid": rid}

                payload = _json(response)
                ok = await self._safe_send(ws, payload)
                self.logger.debug(
                    "[ws→] reply type=%s rid=%s ok=%s bytes=%d",
                    ptype, rid, ok, _bytes_len(payload),
                )
                if not ok:
                    break
        finally:
            await self._close_quietly(ws)
            self.logger.debug("[ws≻] connection closed path=%s peer=%s", path, peer)

    async def _safe_send(self, ws, payload: str) -> bool:
        if ws.closed:
            self.logger.debug("[ws→] not sending: connection already closed")
            return False
        try:
            await ws.send(payload)
            self.logger.debug("[ws→] sent bytes=%d", _bytes_len(payload))
            return True
        except (ConnectionClosedOK, ConnectionClosedError) as e:
            self.logger.debug("[ws→] peer closed during send: %s", e)
            return False
        except Exception:
            self.logger.debug("[ws→] send failed", exc_info=True)
            return False

    async def _close_quietly(self, ws) -> None:
        """Attempt a graceful close; ignore transport/close-frame issues."""
        with contextlib.suppress(
            ConnectionClosedOK, ConnectionClosedError, ProtocolError, RuntimeError, OSError, Exception
        ):
            await ws.close()

    async def _sleep_backoff(self, attempt: int, base: float, cap: float, jitter: float) -> None:
        """Exponential backoff with jitter. attempt >= 1"""
        delay = min(cap, base * (2 ** (attempt - 1)))
        j = random.random() * (jitter * delay)
        delay += j
        self.logger.debug("[ws⏳] backoff attempt=%d delay=%.2fs (jitter=%.2fs)", attempt, delay, j)
        await asyncio.sleep(delay)

    # ---------- outbound helpers ----------
    async def send_json(self, obj: Any) -> bool:
        """
        Convenience helper: ensure we hand a dict to `send()`.
        If given a JSON string, try to parse; if given a non-dict, wrap it.
        """
        try:
            if isinstance(obj, str):
                try:
                    obj = json.loads(obj)
                except Exception:
                    obj = {"type": "(none)", "data": obj}
            elif not isinstance(obj, dict):
                obj = {"type": "(none)", "data": obj}

            await self.send(obj)
            return True
        except Exception as e:
            self.logger.info("send_json failed: %s", e)
            return False

    async def send(
        self,
        payload: dict | str,
        *,
        max_attempts: int = 5,
        base_backoff: float = 0.5,
        backoff_cap: float = 8.0,
        jitter: float = 0.2,
        connect_timeout: float | None = 5.0,
        send_timeout: float | None = 5.0,
    ) -> None:
        """
        Fire-and-forget: connect, send JSON, close.
        Retries on OSError/Timeout with exponential backoff.
        During shutdown, retries/timeouts collapse to a single quick attempt.
        """
        # normalize payload
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {"type": "(none)", "data": payload}

        rid = payload.get("rid") or str(uuid.uuid4())
        payload = dict(payload)
        payload["rid"] = rid
        ptype = _ptype(payload)

        # collapse retries/timeouts while shutting down
        if self._shutting_down:
            max_attempts = 1
            if connect_timeout is None or connect_timeout > 0.25:
                connect_timeout = 0.25
            if send_timeout is None or send_timeout > 0.25:
                send_timeout = 0.25

        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.debug(
                    "WS send attempt %d/%d → %s type=%s rid=%s",
                    attempt, max_attempts, self.send_url, ptype, rid
                )

                t0 = time.monotonic()
                if connect_timeout is not None:
                    ws = await asyncio.wait_for(
                        websockets.connect(self.send_url, max_size=None, ping_interval=None),
                        connect_timeout,
                    )
                else:
                    ws = await websockets.connect(self.send_url, max_size=None, ping_interval=None)
                tconn = (time.monotonic() - t0) * 1000
                self.logger.debug("WS send connected ms=%.1f rid=%s", tconn, rid)

                try:
                    raw = _json(payload)

                    t1 = time.monotonic()
                    if send_timeout is not None:
                        await asyncio.wait_for(ws.send(raw), send_timeout)
                    else:
                        await ws.send(raw)
                    tsend = (time.monotonic() - t1) * 1000
                    self.logger.debug(
                        "WS send payload bytes=%d ms=%.1f type=%s rid=%s",
                        _bytes_len(raw), tsend, ptype, rid
                    )
                finally:
                    await self._close_quietly(ws)
                    self.logger.debug("WS send closed rid=%s", rid)

                return  # success

            except (asyncio.TimeoutError, OSError) as e:
                # Be quiet(er) during shutdown; no long backoffs
                lvl = self.logger.info if (self._shutting_down or attempt >= max_attempts) else self.logger.warning
                lvl("[WS] send error attempt %d/%d rid=%s type=%s: %s",
                    attempt, max_attempts, rid, ptype, e)

                if self._shutting_down or attempt >= max_attempts:
                    break
                await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)

            except Exception as e:
                if self._shutting_down:
                    self.logger.info("[WS] send aborted during shutdown rid=%s type=%s: %s", rid, ptype, e)
                    break
                self.logger.error("[⛔] WS send unexpected failure rid=%s type=%s: %s", rid, ptype, e)
                break

        self.logger.info("[WS] send give-up rid=%s type=%s", rid, ptype)

    async def request(
        self,
        payload: dict,
        *,
        timeout: float | None = None,
        max_attempts: int = 5,
        base_backoff: float = 0.5,
        backoff_cap: float = 8.0,
        jitter: float = 0.2,
        connect_timeout: float | None = 5.0,
        retry_on_timeout: bool = False,
        retry_on_connect_error: bool = True,
    ) -> dict | None:
        """
        Request/response helper. During shutdown we also collapse retries.
        """
        rid = payload.get("rid") or str(uuid.uuid4())
        payload = dict(payload)
        payload["rid"] = rid
        ptype = _ptype(payload)

        # collapse retries while shutting down
        if self._shutting_down:
            max_attempts = min(max_attempts, 1)
            if connect_timeout is None or connect_timeout > 0.25:
                connect_timeout = 0.25
            if timeout is None or timeout > 0.25:
                timeout = 0.25

        self.logger.debug(
            "WS request starting rid=%s type=%s url=%s timeout=%s attempts=%d",
            rid, ptype, self.send_url, timeout, max_attempts,
        )

        for attempt in range(1, max_attempts + 1):
            try:
                t0 = time.monotonic()
                if connect_timeout is not None:
                    ws = await asyncio.wait_for(
                        websockets.connect(self.send_url, max_size=None, ping_interval=None),
                        connect_timeout,
                    )
                else:
                    ws = await websockets.connect(self.send_url, max_size=None, ping_interval=None)
                tconn = (time.monotonic() - t0) * 1000
                self.logger.debug("WS request connected ms=%.1f rid=%s", tconn, rid)

                try:
                    raw_out = _json(payload)
                    await ws.send(raw_out)
                    self.logger.debug("WS request sent bytes=%d rid=%s type=%s", _bytes_len(raw_out), rid, ptype)

                    t1 = time.monotonic()
                    if timeout is not None:
                        raw_in = await asyncio.wait_for(ws.recv(), timeout)
                    else:
                        raw_in = await ws.recv()
                    trecv = (time.monotonic() - t1) * 1000
                    self.logger.debug("WS request recv bytes=%d ms=%.1f rid=%s", _bytes_len(raw_in), trecv, rid)

                    data = json.loads(raw_in)
                    drid = (data or {}).get("rid")
                    if drid and drid != rid:
                        self.logger.info("WS request rid mismatch sent=%s got=%s type=%s", rid, drid, ptype)
                    return data
                finally:
                    await self._close_quietly(ws)
                    self.logger.debug("WS request closed rid=%s", rid)

            except asyncio.CancelledError:
                self.logger.info("WS request cancelled (shutdown) rid=%s type=%s", rid, ptype)
                return None

            except (asyncio.TimeoutError, OSError, ConnectionClosedError, ProtocolError) as e:
                lvl = self.logger.info if (self._shutting_down or attempt >= max_attempts) else self.logger.warning
                lvl("[WS] request error (attempt %d/%d) rid=%s type=%s: %s",
                    attempt, max_attempts, rid, ptype, e)
                if self._shutting_down or attempt >= max_attempts:
                    return None
                await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)

            except Exception as e:
                if self._shutting_down:
                    self.logger.info("[WS] request aborted during shutdown rid=%s type=%s: %s", rid, ptype, e)
                    return None
                self.logger.error("[⛔] WS request unexpected failure rid=%s type=%s: %s", rid, ptype, e)
                break

        return None


class AdminBus:
    """
    Helper to publish events to Admin's /bus.
    """
    def __init__(
        self,
        role: str,
        logger: Optional[logging.Logger] = None,
        admin_ws_url: Optional[str] = None,   # <— allow override/injection
    ):
        self.role = role
        self.logger = logger or logging.getLogger(f"AdminBus[{role}]")
        self.ws = WebsocketManager(send_url=admin_ws_url, logger=self.logger)

    def begin_shutdown(self) -> None:
        """Propagate shutdown to internal manager so outbound sends don't retry."""
        self.ws.begin_shutdown()

    async def stop(self) -> None:
        await self.ws.stop()

    async def publish(self, kind: str, payload: Any):
        # Coerce anything that's not a dict into a log payload
        if not isinstance(payload, dict):
            payload = {"text": str(payload)}
            kind = kind or "log"
        env = {"kind": kind or "log", "role": self.role, "payload": payload}
        try:
            await self.ws.send_json(env)
        except Exception as e:
            self.logger.info("publish/send_json failed during %s: %s", kind, e)

    async def status(self, **fields):
        await self.publish("status", fields)

    async def log(self, text: str):
        await self.publish("log", {"text": text})
        
    async def subscribe(self, admin_ws_url: str, handler: MessageHandler):
        """
        Connect to admin /ws/out and invoke `handler(event_dict)` per JSON event.
        """
        # NORMALIZE: allow both .../bus and the bare base
        base = (admin_ws_url or "").rstrip("/")
        if base.endswith("/bus"):
            base = base[:-4]  # strip the '/bus'

        ws_out = f"{base}/ws/out"
        attempt = 0
        while True:
            try:
                self.logger.debug("AdminBus subscribe → %s", ws_out)
                async with websockets.connect(ws_out, ping_interval=None, max_size=None) as ws:
                    attempt = 0
                    async for raw in ws:
                        try:
                            ev = json.loads(raw)
                        except Exception:
                            continue
                        if isinstance(ev, dict) and "kind" in ev and "role" in ev:
                            try:
                                await handler(ev)
                            except Exception:
                                self.logger.exception("AdminBus handler failed kind=%s role=%s",
                                                    ev.get("kind"), ev.get("role"))
            except (ConnectionClosedOK, asyncio.CancelledError):
                self.logger.debug("AdminBus subscribe cancelled/closed")
                return
            except (OSError, ConnectionClosedError, InvalidStatusCode) as e:
                attempt += 1
                delay = min(8.0, 0.5 * (2 ** (attempt - 1))) * (1 + random.random() * 0.2)
                self.logger.warning("AdminBus subscribe error: %s (retry in %.2fs)", e, delay)
                await asyncio.sleep(delay)
            except Exception as e:
                self.logger.exception("AdminBus subscribe unexpected error: %s", e)
                await asyncio.sleep(1.0)