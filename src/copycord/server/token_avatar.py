# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
from __future__ import annotations

import asyncio
import base64
import json
import re
import uuid
import zlib
from typing import Optional

from curl_cffi import CurlWsFlag

from copycord.common.selfbot_headers import (
    _token_log_id,
    build_headers,
    gateway_properties,
    get_tls_session,
    set_heartbeat_session_id,
)

DISCORD_API_BASE = "https://discord.com/api/v9"
GATEWAY_URL = "wss://gateway.discord.gg/?v=9&encoding=json&compress=zlib-stream"


_OP_DISPATCH = 0
_OP_HEARTBEAT = 1
_OP_IDENTIFY = 2
_OP_HELLO = 10
_OP_INVALID_SESSION = 9


_CAPABILITIES = 1767421


_READY_TIMEOUT = 20.0


_AVATAR_HASH_RE = re.compile(r"/avatars/\d+/([a-zA-Z0-9_]+)\.")


MAX_AVATAR_BYTES = 10 * 1024 * 1024


def avatar_hash_from_url(url: Optional[str]) -> Optional[str]:
    """The hash Discord embeds in an avatar URL, without downloading it."""
    if not url:
        return None
    m = _AVATAR_HASH_RE.search(str(url))
    return m.group(1) if m else None


async def fetch_avatar(session, url: str) -> Optional[tuple[bytes, str]]:
    """Download an avatar. Returns (bytes, content type), or None."""
    try:
        async with session.get(url, timeout=30) as resp:
            if resp.status != 200:
                return None
            data = await resp.read()
    except Exception:
        return None

    if not data or len(data) > MAX_AVATAR_BYTES:
        return None
    ctype = "image/gif" if data[:6] in (b"GIF87a", b"GIF89a") else "image/png"
    return data, ctype


def to_data_uri(data: bytes, content_type: str) -> str:
    """The form Discord wants an avatar in: a base64 data URI."""
    return f"data:{content_type};base64,{base64.b64encode(data).decode()}"


# Close codes that mean this token/session will never work: retrying only
# burns the account further. Everything else is worth another attempt.
_FATAL_CLOSE_CODES = {4004, 4010, 4011, 4012, 4013, 4014}

# curl_cffi sets this flag on a fragment that is not the end of a message.
_WS_OFFSET = getattr(CurlWsFlag, "OFFSET", 32)


class _GatewayClosed(Exception):
    def __init__(self, code: int):
        super().__init__(f"gateway closed with code {code}")
        self.code = code



_ZLIB_SUFFIX = b"\x00\x00\xff\xff"


async def _identify_payload(token: str) -> dict:
    """The IDENTIFY payload the web client sends, built from the same fingerprint as the REST headers."""
    props = gateway_properties(token)
    return {
        "op": _OP_IDENTIFY,
        "d": {
            "token": token,
            "capabilities": _CAPABILITIES,
            "properties": props,
            "presence": {
                "status": "unknown",
                "since": 0,
                "activities": [],
                "afk": False,
            },
            "compress": False,
            "client_state": {
                "guild_versions": {},
            },
        },
    }


class GatewaySession:
    """A gateway connection held only as long as one action needs it.

    No RESUME, event handling or reconnection. A lease is exclusive and a live
    socket never returns it, so many of these would exhaust the proxy pool.
    """

    def __init__(self, token: str, logger, *, use_proxy: bool = False):
        self._token = token
        self._log = logger
        self._use_proxy = use_proxy
        self._ws = None
        self._heartbeat: Optional[asyncio.Task] = None
        self._inflator = None
        self._buf = bytearray()
        self.heartbeat_session_id: Optional[str] = None
        self.fatal = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()
        return False

    async def connect(self) -> bool:
        """Open the socket and identify. True once READY has arrived.

        Sets fatal when the close code says this token is finished.
        """
        session = await get_tls_session(self._token, use_proxy=self._use_proxy)
        try:
            self._ws = await session.ws_connect(GATEWAY_URL)
        except Exception as e:
            self._log.debug("[avatar] gateway connect failed: %s", e)
            return False

        self._inflator = zlib.decompressobj()
        self._buf = bytearray()

        try:
            hello = await asyncio.wait_for(self._recv(), timeout=_READY_TIMEOUT)
            if (hello or {}).get("op") != _OP_HELLO:
                self._log.debug(
                    "[avatar] expected HELLO, got op=%s", (hello or {}).get("op")
                )
                return False
            interval = float(hello["d"]["heartbeat_interval"]) / 1000.0

            # The client mints this when it opens the session and then quotes
            # it on every REST call for the session's lifetime.
            self.heartbeat_session_id = str(uuid.uuid4())

            await self._ws.send_json(await _identify_payload(self._token))
            self._heartbeat = asyncio.create_task(self._heartbeat_loop(interval))

            deadline = asyncio.get_running_loop().time() + _READY_TIMEOUT
            while asyncio.get_running_loop().time() < deadline:
                frame = await asyncio.wait_for(self._recv(), timeout=_READY_TIMEOUT)
                if not frame:
                    continue
                op = frame.get("op")
                if op == _OP_DISPATCH and frame.get("t") == "READY":
                    set_heartbeat_session_id(self._token, self.heartbeat_session_id)
                    return True
                if op == _OP_INVALID_SESSION:
                    # d=True means resumable, so the token itself is fine.
                    self.fatal = frame.get("d") is not True
                    self._log.debug(
                        "[avatar] gateway rejected the session (fatal=%s)",
                        self.fatal,
                    )
                    return False
                if op == _OP_HEARTBEAT:
                    await self._ws.send_json({"op": _OP_HEARTBEAT, "d": None})
        except _GatewayClosed as e:
            self.fatal = e.code in _FATAL_CLOSE_CODES
            self._log.debug(
                "[avatar] gateway closed code=%s fatal=%s", e.code, self.fatal
            )
        except asyncio.TimeoutError:
            self._log.debug("[avatar] gateway did not reach READY in time")
        except Exception as e:
            self._log.debug("[avatar] gateway handshake failed: %s", e)
        return False

    async def _recv(self) -> dict | None:
        """One logical gateway frame, reassembled and inflated.

        zlib-stream means frames are fragments of one continuous deflate
        stream: the decompressor is stateful for the whole connection and a
        message only ends at the Z_SYNC_FLUSH suffix. Treating each websocket
        frame as a message loses the tail of anything large, READY included.
        """
        while True:
            chunk, flags = await self._ws.recv()

            if flags & CurlWsFlag.CLOSE:
                raw = chunk if isinstance(chunk, (bytes, bytearray)) else chunk.encode()
                code = int.from_bytes(raw[:2], "big") if len(raw) >= 2 else 0
                raise _GatewayClosed(code)

            self._buf.extend(
                chunk if isinstance(chunk, (bytes, bytearray)) else chunk.encode()
            )
            if flags & _WS_OFFSET:
                continue

            raw = bytes(self._buf)
            self._buf.clear()
            if not raw:
                return None

            if raw[-4:] == _ZLIB_SUFFIX:
                try:
                    text = self._inflator.decompress(raw).decode("utf-8")
                except Exception as e:
                    self._log.debug("[avatar] zlib inflate failed: %s", e)
                    continue
            else:
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    continue

            try:
                return json.loads(text)
            except Exception:
                continue

    async def _heartbeat_loop(self, interval: float) -> None:
        """Keep the session alive while we hold it.

        The first beat is jittered so a fleet reconnecting together does not beat in lockstep.
        """
        try:
            import random

            await asyncio.sleep(interval * random.random())
            while True:
                await self._ws.send_json({"op": _OP_HEARTBEAT, "d": None})
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def close(self) -> None:
        set_heartbeat_session_id(self._token, None)
        self.heartbeat_session_id = None
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            try:
                await self._heartbeat
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None


async def set_avatar(
    token: str,
    avatar_url: str,
    *,
    http_session,
    logger,
    use_proxy: bool = False,
) -> bool:
    """Point one account's profile picture at avatar_url. True only if Discord accepted it."""
    ref = _token_log_id(token)

    fetched = await fetch_avatar(http_session, avatar_url)
    if not fetched:
        logger.debug("[avatar] could not fetch %s for token %s", avatar_url, ref)
        return False
    data, ctype = fetched

    gateway = GatewaySession(token, logger, use_proxy=use_proxy)
    online = await gateway.connect()
    if not online:

        logger.debug("[avatar] no gateway session for token %s; skipping", ref)
        await gateway.close()
        return False

    try:
        session = await get_tls_session(token, use_proxy=use_proxy)
        from copycord.common.proxy_pool import get_pool

        resp = await session.patch(
            f"{DISCORD_API_BASE}/users/@me",
            json={"avatar": to_data_uri(data, ctype)},
            headers=build_headers(token),
            timeout=60,
            proxy=get_pool().current(token),
        )
        if resp.status_code in (200, 204):
            logger.info("[avatar] updated profile picture for token %s", ref)
            return True

        body = ""
        try:
            body = (resp.text or "")[:200]
        except Exception:
            pass

        # A captcha is the common refusal here and is not something we can
        # answer, so name it rather than leaving a bare 400 in the log.
        if "captcha" in body.lower():
            logger.warning(
                "[avatar] Discord demanded a captcha for token %s; "
                "not retrying this session",
                ref,
            )
        else:
            logger.warning(
                "[avatar] Discord refused the change for token %s (HTTP %s) %s",
                ref,
                resp.status_code,
                body,
            )
        return False
    except Exception as e:
        logger.warning("[avatar] update failed for token %s: %s", ref, e)
        return False
    finally:
        await gateway.close()
