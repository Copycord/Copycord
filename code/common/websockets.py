# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Awaitable, Callable, Optional
import websockets
import random
import contextlib
from websockets.server import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ProtocolError

logger = logging.getLogger(__name__)

MessageHandler = Callable[[dict], Awaitable[None]]

class WebsocketManager:
    def __init__(self, send_url: str, listen_host: str, listen_port: int, logger: Optional[logging.Logger] = None):
        self.send_url = send_url
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.logger = logger.getChild(self.__class__.__name__)
        self._shutting_down = False

    async def start_server(
        self,
        handler: Callable[[dict], Awaitable[dict | None]]
    ) -> None:
        """
        Spins up a websockets.server using `handler` for each incoming JSON
        message. Runs until cancelled.
        """
        server = await websockets.serve(
            lambda ws, path: self._serve_loop(ws, path, handler),
            self.listen_host,
            self.listen_port,
            max_size=None,
        )
        self.logger.debug("WS server listening on %s:%s", self.listen_host, self.listen_port)
        try:
            await asyncio.Future()
        finally:
            self.logger.debug("WS server shutting down…")
            server.close()
            await server.wait_closed()
            self.logger.debug("WS server closed.")

    def begin_shutdown(self) -> None:
        """Tell the manager we're shutting down so we don't retry or spam errors."""
        self._shutting_down = True

    async def _close_quietly(self, ws) -> None:
        """Attempt a graceful close; ignore transport/close-frame issues."""
        with contextlib.suppress(
            ConnectionClosedOK, ConnectionClosedError, ProtocolError, RuntimeError, OSError, Exception
        ):
            await ws.close()

    async def _safe_send(self, ws, payload: str) -> bool:
        if ws.closed:
            self.logger.debug("[ws→] not sending: connection already closed")
            return False
        try:
            sz = _bytes_len(payload)
            await ws.send(payload)
            self.logger.debug("[ws→] sent bytes=%d", sz)
            return True
        except (ConnectionClosedOK, ConnectionClosedError) as e:
            self.logger.debug("[ws→] peer closed during send: %s", e)
            return False
        except Exception:
            self.logger.exception("[ws→] send failed")
            return False

    async def _serve_loop(
        self,
        ws: WebSocketServerProtocol,
        path: str,
        handler: Callable[[dict], Awaitable[dict | None]],
    ):
        """
        Minimal resilient loop:
        - graceful on peer close (1000)
        - no traceback spam when peer closes before our send
        - still logs unexpected exceptions
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
                    self.logger.warning("[ws] peer closed with error: %s", e)
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
                    self.logger.debug("[ws] handler done type=%s rid=%s ms=%.1f ok=%s",
                                 ptype, rid, dt, isinstance(response, dict) and response.get("ok"))
                except Exception:
                    self.logger.exception("Error in WS handler type=%s rid=%s", ptype, rid)
                    response = {"ok": False, "error": "handler-failed", "rid": rid}

                payload = _json(response)
                ok = await self._safe_send(ws, payload)
                self.logger.debug("[ws→] reply type=%s rid=%s ok=%s bytes=%d",
                             ptype, rid, ok, _bytes_len(payload))
                if not ok:
                    break
        finally:
            try:
                await self._close_quietly(ws)
            except Exception:
                pass
            self.logger.debug("[ws≻] connection closed path=%s peer=%s", path, peer)

    async def _sleep_backoff(self, attempt: int, base: float, cap: float, jitter: float) -> None:
        """
        Exponential backoff with jitter. attempt >= 1
        """
        delay = min(cap, base * (2 ** (attempt - 1)))
        j = random.random() * (jitter * delay)
        delay += j
        self.logger.debug("[ws⏳] backoff attempt=%d delay=%.2fs (jitter=%.2fs)", attempt, delay, j)
        await asyncio.sleep(delay)

    async def send(
        self,
        payload: dict,
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
        """
        rid = payload.get("rid") or str(uuid.uuid4())
        payload = dict(payload)
        payload["rid"] = rid
        ptype = _ptype(payload)

        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.debug("WS send attempt %d/%d → %s type=%s rid=%s",
                             attempt, max_attempts, self.send_url, ptype, rid)

                t0 = time.monotonic()
                if connect_timeout is not None:
                    ws = await asyncio.wait_for(
                        websockets.connect(self.send_url, max_size=None),
                        connect_timeout
                    )
                else:
                    ws = await websockets.connect(self.send_url, max_size=None)
                tconn = (time.monotonic() - t0) * 1000
                self.logger.debug("WS send connected ms=%.1f rid=%s", tconn, rid)

                try:
                    raw = _json(payload)
                    sz = _bytes_len(raw)

                    t1 = time.monotonic()
                    if send_timeout is not None:
                        await asyncio.wait_for(ws.send(raw), send_timeout)
                    else:
                        await ws.send(raw)
                    tsend = (time.monotonic() - t1) * 1000
                    self.logger.debug("WS send payload bytes=%d ms=%.1f type=%s rid=%s", sz, tsend, ptype, rid)
                finally:
                    await self._close_quietly(ws)
                    self.logger.debug("WS send closed rid=%s", rid)

                return 

            except (asyncio.TimeoutError, OSError) as e:
                level = self.logger.warning if attempt < max_attempts else self.logger.error
                level("[⚠️] WS send error attempt %d/%d rid=%s type=%s: %s",
                      attempt, max_attempts, rid, ptype, e)
                if attempt < max_attempts:
                    await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)

            except Exception as e:
                self.logger.error("[⛔] WS send unexpected failure rid=%s type=%s: %s", rid, ptype, e)
                break

        self.logger.error("[⛔] WS send giving up rid=%s type=%s", rid, ptype)

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
        rid = payload.get("rid") or str(uuid.uuid4())
        payload = dict(payload)
        payload["rid"] = rid
        ptype = _ptype(payload)

        self.logger.debug(
            "WS request starting rid=%s type=%s url=%s timeout=%s attempts=%d",
            rid, ptype, self.send_url, timeout, max_attempts,
        )

        for attempt in range(1, max_attempts + 1):
            stage = "connect"
            try:
                t0 = time.monotonic()
                if connect_timeout is not None:
                    ws = await asyncio.wait_for(websockets.connect(self.send_url, max_size=None), connect_timeout)
                else:
                    ws = await websockets.connect(self.send_url, max_size=None)
                tconn = (time.monotonic() - t0) * 1000
                self.logger.debug("WS request connected ms=%.1f rid=%s", tconn, rid)

                try:
                    raw_out = _json(payload)
                    sz_out = _bytes_len(raw_out)
                    stage = "send"
                    await ws.send(raw_out)
                    self.logger.debug("WS request sent bytes=%d rid=%s type=%s", sz_out, rid, ptype)

                    stage = "recv"
                    t1 = time.monotonic()
                    if timeout is not None:
                        raw_in = await asyncio.wait_for(ws.recv(), timeout)
                    else:
                        raw_in = await ws.recv()
                    trecv = (time.monotonic() - t1) * 1000
                    sz_in = _bytes_len(raw_in)
                    self.logger.debug("WS request recv bytes=%d ms=%.1f rid=%s", sz_in, trecv, rid)

                    data = json.loads(raw_in)
                    ok = isinstance(data, dict) and data.get("ok")
                    drid = (data or {}).get("rid")
                    if drid and drid != rid:
                        self.logger.warning("WS request rid mismatch sent=%s got=%s type=%s", rid, drid, ptype)
                    self.logger.debug("WS request done ok=%s rid=%s type=%s", ok, rid, ptype)
                    return data

                finally:
                    await self._close_quietly(ws)
                    self.logger.debug("WS request closed rid=%s", rid)

            except asyncio.CancelledError:
                self.logger.info("WS request cancelled (shutdown) rid=%s type=%s", rid, ptype)
                return None

            except (asyncio.TimeoutError, OSError, ConnectionClosedError, ProtocolError) as e:
                level = self.logger.warning if (attempt < max_attempts and not self._shutting_down) else self.logger.info
                level("[WS] request error (attempt %d/%d) rid=%s type=%s: %s",
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

# Helpers
def _ptype(p: dict | None) -> str:
    try:
        return (p or {}).get("type") or "(none)"
    except Exception:
        return "(?)"

def _json(obj: Any) -> str:
    try:
        return json.dumps(obj)
    except Exception as e:
        return f'{{"ok":false,"error":"json-dumps-failed:{e!r}"}}'

def _bytes_len(s: str | bytes) -> int:
    if isinstance(s, bytes):
        return len(s)
    try:
        return len(s.encode("utf-8"))
    except Exception:
        return len(s)