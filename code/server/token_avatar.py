# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
"""
Mirror a host author's profile picture onto the account sending as them.

Unlike the nickname and roles, an avatar is not a per-guild attribute: it is
set on the ACCOUNT, so this changes how that token looks everywhere. Tokens
belong to exactly one mapping and, under sticky_author, to one author within
it, so the picture stays put rather than thrashing between authors.

The change is made with a gateway session open. A real client is always
connected when its user edits their profile, and an account that only ever
mutates over REST -- never having identified -- is a shape worth avoiding. The
session is opened for the change and closed straight after, so nothing is held
open per token; see the note in `set_avatar` about why that matters here.
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
from typing import Optional

from common.selfbot_headers import (
    _token_log_id,
    build_headers,
    get_tls_session,
    make_fingerprint,
)

DISCORD_API_BASE = "https://discord.com/api/v10"
GATEWAY_URL = "wss://gateway.discord.gg/?encoding=json&v=10"


_OP_DISPATCH = 0
_OP_HEARTBEAT = 1
_OP_IDENTIFY = 2
_OP_HELLO = 10
_OP_HEARTBEAT_ACK = 11


_CAPABILITIES = 30717


_READY_TIMEOUT = 20.0


_AVATAR_HASH_RE = re.compile(r"/avatars/\d+/([a-zA-Z0-9_]+)\.")


MAX_AVATAR_BYTES = 10 * 1024 * 1024


def avatar_hash_from_url(url: Optional[str]) -> Optional[str]:
    """The hash Discord embeds in an avatar URL, without downloading it.

    This is what makes "has it changed?" free: the hash is already in the URL
    the message arrived with, so an unchanged avatar costs nothing.
    """
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


async def _identify_payload(token: str) -> dict:
    """The IDENTIFY a desktop client sends.

    ``properties`` is the same fingerprint the REST headers carry, decoded --
    the gateway wants the object, not the base64 the header uses. Sending a
    different device here than the one the HTTP requests claim would be a
    contradiction inside a single session.
    """
    fp = make_fingerprint(token)
    props = json.loads(base64.b64decode(fp["super_props_b64"]))
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
    """A gateway connection held only for as long as one action needs it.

    Deliberately not a full client: no RESUME, no event handling, no
    reconnection. It exists so a profile edit happens the way a real one does
    -- with the account actually online -- and then gets out of the way.
    Holding thousands of these open would exhaust the proxy pool, since a
    lease is exclusive and a live socket never returns it.
    """

    def __init__(self, token: str, logger, *, use_proxy: bool = False):
        self._token = token
        self._log = logger
        self._use_proxy = use_proxy
        self._ws = None
        self._heartbeat: Optional[asyncio.Task] = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()
        return False

    async def connect(self) -> bool:
        """Open the socket and identify. True once READY has arrived."""
        session = await get_tls_session(self._token, use_proxy=self._use_proxy)
        try:
            self._ws = await session.ws_connect(GATEWAY_URL)
        except Exception as e:
            self._log.debug("[avatar] gateway connect failed: %s", e)
            return False

        try:
            hello = await asyncio.wait_for(self._ws.recv_json(), timeout=_READY_TIMEOUT)
            if (hello or {}).get("op") != _OP_HELLO:
                self._log.debug(
                    "[avatar] expected HELLO, got op=%s", (hello or {}).get("op")
                )
                return False
            interval = float(hello["d"]["heartbeat_interval"]) / 1000.0

            await self._ws.send_json(await _identify_payload(self._token))
            self._heartbeat = asyncio.create_task(self._heartbeat_loop(interval))

            deadline = asyncio.get_running_loop().time() + _READY_TIMEOUT
            while asyncio.get_running_loop().time() < deadline:
                frame = await asyncio.wait_for(
                    self._ws.recv_json(), timeout=_READY_TIMEOUT
                )
                if not frame:
                    continue
                if frame.get("op") == _OP_DISPATCH and frame.get("t") == "READY":
                    return True
                if frame.get("op") == 9:

                    self._log.debug("[avatar] gateway rejected the session")
                    return False
        except asyncio.TimeoutError:
            self._log.debug("[avatar] gateway did not reach READY in time")
        except Exception as e:
            self._log.debug("[avatar] gateway handshake failed: %s", e)
        return False

    async def _heartbeat_loop(self, interval: float) -> None:
        """Keep the session alive for the short while we hold it.

        The first beat is jittered the way the client does it, so a fleet that
        reconnects together does not then beat in lockstep forever.
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
    """Point one account's profile picture at ``avatar_url``.

    Downloads the image, opens a gateway session, PATCHes /users/@me, then
    closes the session. Returns True only if Discord accepted the change, so
    the caller can decide whether to record the hash as mirrored.
    """
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
        from common.proxy_pool import get_pool

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
