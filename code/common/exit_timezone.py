# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""IANA timezone of the IP an account exits through.

``X-Discord-Timezone`` should match the network the account really uses, so
the zone is looked up once per exit (each proxy URL, plus one for direct) and
cached. Header builders are synchronous, so they read the cache through
``timezone_for(token)``; the async call sites that already know which proxy a
token uses call ``note_token_proxy`` to resolve and register it.

Any lookup failure falls back to ``FALLBACK_TIMEZONE`` with one warning per
exit. A failed lookup is retried after ``RETRY_AFTER`` seconds, not per request.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp

from common.proxy_pool import _mask_proxy_url

logger = logging.getLogger("common.exit_timezone")

FALLBACK_TIMEZONE = "America/New_York"
LOOKUP_URL = "https://ipinfo.io/json"
LOOKUP_TIMEOUT = 6.0
RETRY_AFTER = 600.0

_SOCKS_SCHEMES = ("socks4://", "socks5://", "socks5h://")

# exit key (proxy URL, or None for direct) -> (zone or None, retry-at monotonic)
_CACHE: Dict[Optional[str], Tuple[Optional[str], float]] = {}
_LOCKS: Dict[Optional[str], asyncio.Lock] = {}
_WARNED: set = set()
_TOKEN_EXIT: Dict[str, Optional[str]] = {}


def _valid_zone(name) -> Optional[str]:
    if not isinstance(name, str) or not name.strip():
        return None
    name = name.strip()
    try:
        ZoneInfo(name)
    except Exception:
        return None
    return name


async def _fetch_json(proxy: Optional[str], timeout: float) -> dict:
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    if proxy and proxy.startswith(_SOCKS_SCHEMES):
        from aiohttp_socks import ProxyConnector

        connector = ProxyConnector.from_url(proxy)
        async with aiohttp.ClientSession(connector=connector) as sess:
            async with sess.get(LOOKUP_URL, timeout=client_timeout) as r:
                r.raise_for_status()
                return await r.json(content_type=None)
    async with aiohttp.ClientSession() as sess:
        async with sess.get(
            LOOKUP_URL, proxy=proxy or None, timeout=client_timeout
        ) as r:
            r.raise_for_status()
            return await r.json(content_type=None)


async def resolve_timezone(
    proxy: Optional[str] = None, timeout: float = LOOKUP_TIMEOUT
) -> Optional[str]:
    """Timezone of the IP ``proxy`` (or the direct connection) exits through.

    Returns None on any failure or when the answer is not a real IANA zone.
    """
    try:
        data = await _fetch_json(proxy, timeout)
        return _valid_zone(data.get("timezone"))
    except Exception:
        return None


async def ensure_timezone(proxy: Optional[str] = None) -> str:
    """Cached zone for this exit, resolving it first if needed."""
    key = proxy or None
    entry = _CACHE.get(key)
    now = time.monotonic()
    if entry is not None and (entry[0] or now < entry[1]):
        return entry[0] or FALLBACK_TIMEZONE

    lock = _LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        entry = _CACHE.get(key)
        now = time.monotonic()
        if entry is not None and (entry[0] or now < entry[1]):
            return entry[0] or FALLBACK_TIMEZONE
        zone = await resolve_timezone(key)
        _CACHE[key] = (zone, time.monotonic() + RETRY_AFTER)
        label = _mask_proxy_url(key) if key else "direct"
        if zone:
            _WARNED.discard(key)
            logger.info("Exit timezone for %s: %s", label, zone)
        elif key not in _WARNED:
            _WARNED.add(key)
            logger.warning(
                "Timezone lookup failed for %s, using %s", label, FALLBACK_TIMEZONE
            )
        return zone or FALLBACK_TIMEZONE


async def note_token_proxy(token: str, proxy: Optional[str]) -> str:
    """Record the exit ``token`` uses now (resolving its zone) and return it.

    Call again whenever the token's proxy changes; a new proxy re-resolves.
    """
    zone = await ensure_timezone(proxy)
    _TOKEN_EXIT[token] = proxy or None
    return zone


def forget_token(token: str) -> None:
    _TOKEN_EXIT.pop(token, None)


def timezone_for(token: str) -> str:
    """Synchronous cache read for header builders; never does I/O."""
    entry = _CACHE.get(_TOKEN_EXIT.get(token))
    return (entry[0] if entry else None) or FALLBACK_TIMEZONE


def clear_cache() -> None:
    _CACHE.clear()
    _LOCKS.clear()
    _WARNED.clear()
    _TOKEN_EXIT.clear()
