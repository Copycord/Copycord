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
import logging
import socket
import time
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger("admin.notifications")

# Seconds between notifications of the same event type.
_COOLDOWN = 300  # 5 minutes
_last_sent: dict[str, float] = {}


async def send_webhook(
    webhook_url: str,
    title: str,
    description: str,
    color: int = 0xFF6B6B,
) -> bool:
    """Send a Discord webhook embed notification. Returns True on success."""
    if not webhook_url or not webhook_url.startswith("http"):
        return False

    host = urlparse(webhook_url).hostname
    if not host:
        return False

    # Pre-check DNS so a resolution failure doesn't leave orphaned futures
    # inside aiohttp's connector (which spams "Future exception was never
    # retrieved" warnings).
    try:
        loop = asyncio.get_running_loop()
        await loop.getaddrinfo(host, 443)
    except (socket.gaierror, OSError) as exc:
        logger.warning("[webhook] DNS resolution failed for %s: %s", host, exc)
        return False

    payload = {
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "timestamp": __import__("datetime")
                .datetime.now(__import__("datetime").timezone.utc)
                .isoformat(),
            }
        ],
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                ok = 200 <= resp.status < 300
                if not ok:
                    logger.warning(
                        "[webhook] Failed to send notification: %s %s",
                        resp.status,
                        await resp.text(),
                    )
                return ok
    except Exception as e:
        logger.warning("[webhook] Error sending notification: %s", e)
        return False


async def notify(
    db,
    event: str,
    title: str,
    description: str,
    color: int = 0xFF6B6B,
) -> bool:
    """Send a notification if the event type is enabled and not in cooldown."""
    now = time.monotonic()
    last = _last_sent.get(event, 0)
    if now - last < _COOLDOWN:
        return False

    webhook_url = (db.get_config("NOTIFICATION_WEBHOOK_URL", "") or "").strip()
    if not webhook_url:
        return False

    enabled_raw = (db.get_config(f"NOTIFY_{event.upper()}", "") or "").strip().lower()
    if enabled_raw in ("0", "false", "no"):
        return False
    if not enabled_raw:
        default = EVENTS.get(event.upper(), {}).get("default", True)
        if not default:
            return False

    ok = await send_webhook(webhook_url, title, description, color)
    # Update the cooldown on failure too, so an unreachable webhook is not retried in a loop.
    _last_sent[event] = now
    if ok:
        logger.info("[webhook] Sent %s notification", event)
    return ok


# Known events
EVENTS = {
    "CLIENT_OFFLINE": {
        "title": "Client Offline",
        "description": "The Copycord client bot has gone offline.",
        "color": 0xFF6B6B,
        "default": True,
    },
    "SERVER_OFFLINE": {
        "title": "Server Offline",
        "description": "The Copycord server bot has gone offline.",
        "color": 0xFF6B6B,
        "default": True,
    },
    "PROXIES_DEAD": {
        "title": "All Proxies Dead",
        "description": "All configured proxies have failed. The client may be exposed or unable to connect.",
        "color": 0xFF9800,
        "default": True,
    },
    "TOKEN_INVALID": {
        "title": "Token Invalid",
        "description": "A bot token has been revoked or is no longer valid.",
        "color": 0xFF6B6B,
        "default": True,
    },
}
