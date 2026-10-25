# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
IANA timezone of the IP an account exits through, for X-Discord-Timezone.

Looked up once per exit (each proxy, plus direct) and cached. A failed direct
lookup uses FALLBACK_TIMEZONE and is retried after RETRY_AFTER seconds. A failed
proxy lookup never falls back: it is cached as a miss for NEGATIVE_TTL seconds
and the caller rotates the token to another proxy (token_proxy.lease_with_timezone).
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp

from copycord.common.http_session import shared_session
from copycord.common.proxy_pool import _mask_proxy_url
from copycord.common.token_proxy import NoProxyAvailable

logger = logging.getLogger("common.exit_timezone")

FALLBACK_TIMEZONE = "America/New_York"
LOOKUP_URL = "https://ipinfo.io/json"
LOOKUP_TIMEOUT = 6.0
RETRY_AFTER = 600.0
# how long a failed proxy lookup is remembered, so many tokens do not retry one dead proxy at once
NEGATIVE_TTL = 30.0

_SOCKS_SCHEMES = ("socks4://", "socks5://", "socks5h://")

# exit key (proxy URL, or None for direct) -> (zone or None, retry-at monotonic)
_CACHE: Dict[Optional[str], Tuple[Optional[str], float]] = {}
_LOCKS: Dict[Optional[str], asyncio.Lock] = {}
_WARNED: set = set()
_TOKEN_EXIT: Dict[str, Optional[str]] = {}
# exit key -> why its last lookup failed
_LAST_ERROR: Dict[Optional[str], str] = {}
_CREDENTIALS = re.compile(r"://[^/@\s]+@")


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
    # A SOCKS proxy needs its own connector above; everything else reuses the role's session.
    async with shared_session().get(
        LOOKUP_URL, proxy=proxy or None, timeout=client_timeout
    ) as r:
        r.raise_for_status()
        return await r.json(content_type=None)


def _describe(exc: BaseException, proxy: Optional[str], timeout: float) -> str:
    """Why a lookup failed, for the warning; never carries proxy credentials."""
    if isinstance(exc, asyncio.TimeoutError):
        text = f"no answer within {timeout:g}s"
    elif isinstance(exc, aiohttp.ClientResponseError):
        text = f"HTTP {exc.status} from {LOOKUP_URL}"
    else:
        text = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    if proxy:
        text = text.replace(proxy, _mask_proxy_url(proxy))
    return _CREDENTIALS.sub("://***@", text)


async def resolve_timezone(
    proxy: Optional[str] = None, timeout: float = LOOKUP_TIMEOUT
) -> Optional[str]:
    """Timezone of the exit IP for proxy (or direct); None on any failure, with
    the reason kept in _LAST_ERROR for the warning."""
    key = proxy or None
    try:
        data = await _fetch_json(proxy, timeout)
        zone = _valid_zone(data.get("timezone"))
    except Exception as exc:
        _LAST_ERROR[key] = _describe(exc, proxy, timeout)
        return None
    if zone is None:
        _LAST_ERROR[key] = f"no valid timezone in the answer ({str(data.get('timezone'))[:40]!r})"
    else:
        _LAST_ERROR.pop(key, None)
    return zone


async def resolve_exit_timezone(proxy: Optional[str] = None) -> Optional[str]:
    """Cached zone for this exit, resolving it first if needed; None when the lookup failed.

    A proxy miss is kept for NEGATIVE_TTL seconds (the caller should rotate, not
    wait); a direct miss is kept for RETRY_AFTER seconds.
    """
    key = proxy or None
    entry = _CACHE.get(key)
    now = time.monotonic()
    if entry is not None and (entry[0] or now < entry[1]):
        return entry[0]

    lock = _LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        entry = _CACHE.get(key)
        now = time.monotonic()
        if entry is not None and (entry[0] or now < entry[1]):
            return entry[0]
        zone = await resolve_timezone(key)
        ttl = RETRY_AFTER if key is None else NEGATIVE_TTL
        _CACHE[key] = (zone, time.monotonic() + ttl)
        label = _mask_proxy_url(key) if key else "direct"
        if zone:
            _WARNED.discard(key)
            logger.debug("Exit timezone for %s: %s", label, zone)
        elif key not in _WARNED:
            _WARNED.add(key)
            logger.warning(
                "Timezone lookup failed for %s (%s), %s",
                label,
                _LAST_ERROR.get(key, "unknown error"),
                f"using {FALLBACK_TIMEZONE}" if key is None else "rotating to another proxy",
            )
        return zone


async def ensure_timezone(proxy: Optional[str] = None) -> str:
    """Zone for this exit with FALLBACK_TIMEZONE on a miss; only direct callers should want the fallback."""
    return await resolve_exit_timezone(proxy) or FALLBACK_TIMEZONE


async def note_token_proxy(token: str, proxy: Optional[str]) -> Optional[str]:
    """Record which proxy token uses, resolving its zone; call again when it changes.

    Returns the zone, or None when the proxy's lookup failed: that proxy is bad
    for this token and timezone_for(token) refuses until another one resolves.
    A direct (None) exit still gets FALLBACK_TIMEZONE.
    """
    _TOKEN_EXIT[token] = proxy or None
    if proxy:
        return await resolve_exit_timezone(proxy)
    return await ensure_timezone(None)


def forget_token(token: str) -> None:
    _TOKEN_EXIT.pop(token, None)


class ExitTimezoneUnavailable(NoProxyAvailable):
    """The token's proxy has no resolved exit timezone, so nothing may be sent with it."""


def timezone_for(token: str) -> str:
    """Synchronous cache read for header builders; never does I/O.

    A token on a proxy whose zone is not resolved raises ExitTimezoneUnavailable
    instead of sending the fallback; unknown or direct tokens get FALLBACK_TIMEZONE.
    """
    exit_key = _TOKEN_EXIT.get(token)
    entry = _CACHE.get(exit_key)
    zone = entry[0] if entry else None
    if zone:
        return zone
    if exit_key is not None:
        raise ExitTimezoneUnavailable("the token's proxy has no resolved exit timezone")
    return FALLBACK_TIMEZONE


def clear_cache() -> None:
    _CACHE.clear()
    _LOCKS.clear()
    _WARNED.clear()
    _TOKEN_EXIT.clear()
    _LAST_ERROR.clear()
