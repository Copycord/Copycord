# common/websockets.py
import asyncio
import json
import logging
from typing import Any, Awaitable, Callable
import websockets
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

    async def send(self, payload: dict, max_attempts: int = 5) -> None:
        """
        Connects to `self.send_url`, sends `payload` as JSON, and closes.
        Retries up to `max_attempts` on OSError.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                logger.debug("WS send attempt %d → %s", attempt, self.send_url)
                async with websockets.connect(self.send_url) as ws:
                    await ws.send(json.dumps(payload))
                return
            except OSError as e:
                logger.warning("WS send error on attempt %d: %s", attempt, e)
                await asyncio.sleep(2)
        logger.error("WS send giving up on payload type=%s", payload.get("type"))

    async def request(self, payload: dict, timeout: float = 10.0) -> dict | None:
        """
        Two‑way RPC: connects, sends payload, awaits *one* reply, then closes.
        Returns the parsed JSON reply, or None on error.
        """
        logger.debug(
            "WS request starting: payload=%r, url=%s, timeout=%s",
            payload,
            self.send_url,
            timeout,
        )
        try:
            async with websockets.connect(self.send_url) as ws:
                raw_payload = json.dumps(payload)
                await ws.send(raw_payload)
                raw = await asyncio.wait_for(ws.recv(), timeout)
                data = json.loads(raw)
                return data

        except asyncio.TimeoutError:
            logger.error("WS request timed out after %s seconds", timeout)
            return None
        except Exception as e:
            logger.error("WS request failed: %s", e, exc_info=True)
            return None
