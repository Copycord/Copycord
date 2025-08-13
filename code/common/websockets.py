import asyncio
import json
import logging
from typing import Any, Awaitable, Callable
import websockets
import random
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger("websockets")

MessageHandler = Callable[[dict], Awaitable[None]]


class WebsocketManager:
    def __init__(self, send_url: str, listen_host: str, listen_port: int):
        self.send_url = send_url
        self.listen_host = listen_host
        self.listen_port = listen_port

    async def start_server(
        self, handler: Callable[[dict], Awaitable[dict | None]]
    ) -> None:
        """
        Spins up a websockets.server using `handler` for each incoming JSON
        message.  Runs until cancelled.
        """
        server = await websockets.serve(
            lambda ws, path: self._serve_loop(ws, path, handler),
            self.listen_host,
            self.listen_port,
            logger=None,
        )
        logger.debug("WS server listening on %s:%s", self.listen_host, self.listen_port)
        try:
            await asyncio.Future()
        finally:
            server.close()
            await server.wait_closed()

    async def _serve_loop(
        self,
        ws: WebSocketServerProtocol,
        path: str,
        handler: Callable[[dict], Awaitable[dict | None]],
    ):
        """
        Handles the WebSocket server loop, processing incoming messages and sending responses.
        """
        async for raw in ws:
            try:
                msg = json.loads(raw)
                response = await handler(msg)
                if isinstance(response, dict):
                    await ws.send(json.dumps(response))
            except Exception:
                logger.exception("Error in WS handler")

    async def _sleep_backoff(self, attempt: int, base: float, cap: float, jitter: float) -> None:
        """
        Exponential backoff with jitter. attempt >= 1
        """
        delay = min(cap, base * (2 ** (attempt - 1)))
        delay = delay + random.random() * (jitter * delay)
        await asyncio.sleep(delay)
        
    async def send(
        self,
        payload: dict,
        *,
        max_attempts: int = 5,
        base_backoff: float = 0.5,
        backoff_cap: float = 8.0,
        jitter: float = 0.2,
        connect_timeout: float = 5.0,
        send_timeout: float = 5.0,
    ) -> None:
        """
        Fire-and-forget: connect, send JSON, close.
        Retries on OSError/Timeout with exponential backoff.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                logger.debug("WS send attempt %d → %s", attempt, self.send_url)
                ws = await asyncio.wait_for(websockets.connect(self.send_url), connect_timeout)
                try:
                    raw = json.dumps(payload)
                    await asyncio.wait_for(ws.send(raw), send_timeout)
                finally:
                    await ws.close()
                return

            except (asyncio.TimeoutError, OSError) as e:
                level = logger.warning if attempt < max_attempts else logger.error
                level("[⚠️] WS send error attempt %d/%d: %s", attempt, max_attempts, e)
                if attempt < max_attempts:
                    await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)

            except Exception as e:
                logger.exception("[⛔] WS send unexpected failure: %s", e)
                break

        logger.error("[⛔] WS send giving up on payload type=%s", payload.get("type"))

    async def request(
        self,
        payload: dict,
        *,
        timeout: float = 10.0,
        max_attempts: int = 5,
        base_backoff: float = 0.5,
        backoff_cap: float = 8.0,
        jitter: float = 0.2,
        connect_timeout: float = 5.0,
    ) -> dict | None:
        """
        RPC: connect, send payload, await one reply, close.
        Retries on connection/timeout errors with exponential backoff.
        Returns parsed JSON or None on failure after retries.
        """
        logger.debug(
            "WS request starting: payload=%r, url=%s, timeout=%s, attempts=%d",
            payload, self.send_url, timeout, max_attempts,
        )

        for attempt in range(1, max_attempts + 1):
            try:
                ws = await asyncio.wait_for(websockets.connect(self.send_url), connect_timeout)
                try:
                    await ws.send(json.dumps(payload))
                    raw = await asyncio.wait_for(ws.recv(), timeout)
                    data = json.loads(raw)
                    return data
                finally:
                    await ws.close()

            except asyncio.TimeoutError:
                level = logger.warning if attempt < max_attempts else logger.error
                level("[⏳] WS request timeout (attempt %d/%d, wait=%ss)",
                      attempt, max_attempts, timeout)
                if attempt < max_attempts:
                    await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)

            except OSError as e:
                level = logger.warning if attempt < max_attempts else logger.error
                level("[🚫] WS connect/send failed (attempt %d/%d): %s",
                      attempt, max_attempts, e)
                if attempt < max_attempts:
                    await self._sleep_backoff(attempt, base_backoff, backoff_cap, jitter)

            except Exception as e:
                logger.exception("[⛔] WS request unexpected failure: %s", e)
                break

        return None
