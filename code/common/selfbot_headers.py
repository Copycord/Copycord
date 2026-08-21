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
import base64
import hashlib
import json
import logging
import random
import re
import time

import aiohttp

from common.proxy_pool import get_pool as _get_proxy_pool

try:
    from curl_cffi.requests import AsyncSession as _CurlAsyncSession
except ImportError:
    _CurlAsyncSession = None

logger = logging.getLogger(__name__)


def _token_log_id(token: str) -> str:
    """A short, stable, non-reversible identifier for a token, safe to put
    in logs — never the token itself or a prefix of it (Discord tokens are
    partially structured/guessable, so even a truncated raw prefix is a
    partial credential leak)."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:10]


BUILD_INFO_URL = "https://api.macslodge.com/discord/build"
_DISCORD_APP_URL = "https://discord.com/app"
_DISCORD_ASSET_URL = "https://discord.com/assets/{asset}"
_ASSET_RE = re.compile(r'(?:src|href)="/assets/([^"]+\.js)"')
_BUILD_NUMBER_MARKERS = ('buildNumber:"', 'build_number:"')


TLS_IMPERSONATE = "chrome146"


TLS_SESSION_IDLE_TTL = 3600


DEFAULT_BUILD: dict = {
    "release_channel": "stable",
    "client_version": "1.0.9243",
    "browser_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) discord/1.0.9243 Chrome/138.0.7204.251 "
        "Electron/37.6.0 Safari/537.36"
    ),
    "browser_version": "37.6.0",
    "client_build_number": 573410,
    "native_build_number": 84934,
}


_BUILD: dict = dict(DEFAULT_BUILD)


_FINGERPRINT_CACHE: dict[str, dict] = {}


_WINDOWS_BUILDS = [
    ("10.0.19045", "19045"),
    ("10.0.22621", "22621"),
    ("10.0.22631", "22631"),
    ("10.0.26100", "26100"),
    ("10.0.20348", "20348"),
]

_LOCALES = ["en-US", "en-GB", "de", "fr", "es-ES", "nl", "pt-BR"]
_TIMEZONES = [
    "America/New_York",
    "America/Chicago",
    "America/Los_Angeles",
    "Europe/London",
    "Europe/Berlin",
    "Europe/Amsterdam",
    "Asia/Tokyo",
]


def get_build_info() -> dict:
    """Return a copy of the current shared build fingerprint."""
    return dict(_BUILD)


def set_build_info(build: dict) -> None:
    """Replace the shared build fingerprint and invalidate cached per-token
    fingerprints so they rebuild against the new build."""
    global _BUILD
    merged = dict(DEFAULT_BUILD)
    merged.update({k: v for k, v in (build or {}).items() if v is not None})
    _BUILD = merged
    _FINGERPRINT_CACHE.clear()


async def _scrape_build_number_from_web(
    sess: aiohttp.ClientSession,
) -> int | None:
    """Fallback: read the live build number straight from Discord's own web
    assets."""
    try:
        async with sess.get(
            _DISCORD_APP_URL, timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            page = await resp.text()
        assets = _ASSET_RE.findall(page)

        for asset in reversed(assets):
            try:
                async with sess.get(
                    _DISCORD_ASSET_URL.format(asset=asset),
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    js = await resp.text()
            except Exception:
                continue
            for marker in _BUILD_NUMBER_MARKERS:
                if marker in js:
                    try:
                        return int(js.split(marker, 1)[1].split('"', 1)[0])
                    except (IndexError, ValueError):
                        continue
    except Exception as e:
        logger.debug("build-number web scrape failed: %r", e)
    return None


async def refresh_build_info(session: aiohttp.ClientSession | None = None) -> dict:
    """Fetch the current Discord *stable* build fingerprint."""
    owns = session is None
    sess = session or aiohttp.ClientSession()
    try:
        dec: dict = {}
        try:
            async with sess.get(
                BUILD_INFO_URL, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    dec = (
                        ((data or {}).get("clients") or {}).get("Discord") or {}
                    ).get("decoded") or {}
                else:
                    logger.debug("build-info fetch: HTTP %s", resp.status)
        except Exception as e:
            logger.debug("build-info fetch failed: %r", e)

        build_number = int(dec.get("client_build_number") or 0)
        ua = str(dec.get("browser_user_agent") or "")

        if build_number < 400000 or "discord/" not in ua:
            logger.debug(
                "build-info fetch: implausible or missing payload; scraping web assets"
            )
            scraped = await _scrape_build_number_from_web(sess)
            if scraped is not None and scraped >= 400000:
                parsed = dict(DEFAULT_BUILD)
                parsed["client_build_number"] = scraped
                set_build_info(parsed)
                logger.debug(
                    "Discord build fingerprint updated via web scrape: build=%s",
                    scraped,
                )
                return parsed
            return get_build_info()

        parsed = {
            "release_channel": dec.get("release_channel")
            or DEFAULT_BUILD["release_channel"],
            "client_version": dec.get("client_version")
            or DEFAULT_BUILD["client_version"],
            "browser_user_agent": ua,
            "browser_version": dec.get("browser_version")
            or DEFAULT_BUILD["browser_version"],
            "client_build_number": build_number,
            "native_build_number": int(
                dec.get("native_build_number") or DEFAULT_BUILD["native_build_number"]
            ),
        }
        set_build_info(parsed)
        logger.debug(
            "Discord build fingerprint updated: build=%s version=%s",
            parsed["client_build_number"],
            parsed["client_version"],
        )
        return parsed
    except Exception as e:
        logger.debug("build-info fetch failed: %r", e)
        return get_build_info()
    finally:
        if owns:
            try:
                await sess.close()
            except Exception:
                pass


def _stable_uuid(rng: random.Random) -> str:
    """A deterministic UUIDv4-format string from a seeded RNG."""
    b = bytearray(rng.getrandbits(8) for _ in range(16))
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def make_fingerprint(token: str) -> dict:
    """Build a stable, unique device fingerprint for a token."""
    seed = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)

    build = get_build_info()
    os_version, os_sdk_version = rng.choice(_WINDOWS_BUILDS)
    locale = rng.choice(_LOCALES)
    tz = rng.choice(_TIMEZONES)
    app_state = rng.choice(["focused", "unfocused"])
    launch_id = _stable_uuid(rng)
    launch_signature = _stable_uuid(rng)
    heartbeat_session_id = _stable_uuid(rng)

    user_agent = build["browser_user_agent"]

    super_props = {
        "os": "Windows",
        "browser": "Discord Client",
        "release_channel": build["release_channel"],
        "client_version": build["client_version"],
        "os_version": os_version,
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": locale,
        "has_client_mods": False,
        "client_launch_id": launch_id,
        "browser_user_agent": user_agent,
        "browser_version": build["browser_version"],
        "os_sdk_version": os_sdk_version,
        "client_build_number": build["client_build_number"],
        "native_build_number": build["native_build_number"],
        "client_event_source": None,
        "launch_signature": launch_signature,
        "client_heartbeat_session_id": heartbeat_session_id,
        "client_app_state": app_state,
    }
    super_props_b64 = base64.b64encode(
        json.dumps(super_props, separators=(",", ":")).encode()
    ).decode()

    headers = {
        "User-Agent": user_agent,
        "X-Discord-Locale": locale,
        "X-Discord-Timezone": tz,
        "X-Debug-Options": "bugReporterEnabled",
        "Accept": "*/*",
        "Accept-Language": f"{locale},en;q=0.9",
    }
    return {"headers": headers, "super_props_b64": super_props_b64}


def build_headers(token: str) -> dict:
    """Full header dict for a user-token REST request."""
    fp = _FINGERPRINT_CACHE.get(token)
    if fp is None:
        fp = make_fingerprint(token)
        _FINGERPRINT_CACHE[token] = fp
    return {
        **fp["headers"],
        "Authorization": token,
        "X-Super-Properties": fp["super_props_b64"],
    }


def context_properties(location: str = "chat_input") -> str:
    """Base64 ``X-Context-Properties`` value the client sends with actions."""
    return base64.b64encode(
        json.dumps({"location": location}, separators=(",", ":")).encode()
    ).decode()


_TLS_SESSIONS: dict = {}
_TLS_PRIMED: set[str] = set()
_TLS_SESSION_LOCKS: dict[str, asyncio.Lock] = {}
_TLS_LAST_USED: dict[str, float] = {}


def _tls_session_lock(token: str) -> asyncio.Lock:
    lock = _TLS_SESSION_LOCKS.get(token)
    if lock is None:
        lock = asyncio.Lock()
        _TLS_SESSION_LOCKS[token] = lock
    return lock


async def get_tls_session(token: str, *, use_proxy: bool = False):
    """Return a curl_cffi ``AsyncSession`` dedicated to this token.

    When ``use_proxy`` is set (per-mapping ``USER_TOKEN_USE_PROXIES``) and
    the pool has entries, first creation also leases this token a proxy and
    primes the session's cookies *through* that proxy — priming from a
    different network path than real requests will use would hand the token
    cookies tied to the wrong source IP. The session itself never has a
    proxy baked in; callers pass the token's current lease
    (``proxy_pool.get_pool().current(token)``) per request instead, so a
    proxy failure mid-life can swap the lease without tearing down the
    cookie jar/TLS state.

    The proxy decision is made ONCE, at session creation. A token reachable
    from two mappings with different ``USER_TOKEN_USE_PROXIES`` values
    resolves first-session-wins, deliberately: sessions and leases are keyed
    by token value, and switching a live session between proxied and direct
    would use cookies issued to one IP from another — a sharper anomaly than
    either choice made consistently.
    """
    if _CurlAsyncSession is None:
        raise RuntimeError("curl_cffi is not installed")

    async with _tls_session_lock(token):
        sess = _TLS_SESSIONS.get(token)
        is_new = sess is None
        if sess is None:
            try:
                sess = _CurlAsyncSession(impersonate=TLS_IMPERSONATE)
            except Exception as e:
                logger.error(
                    "Failed to create curl_cffi session with impersonate=%r "
                    "(check the installed curl_cffi version supports this "
                    "target): %r",
                    TLS_IMPERSONATE,
                    e,
                )
                raise
            _TLS_SESSIONS[token] = sess

        proxy = None
        if token not in _TLS_PRIMED:
            pool = _get_proxy_pool()
            if use_proxy and pool.enabled:
                proxy = await pool.lease(token)
            await _prime_tls_session(sess, token, proxy=proxy)
            _TLS_PRIMED.add(token)

        _TLS_LAST_USED[token] = time.monotonic()

        if is_new:
            logger.debug(
                "TLS session created: token=%s impersonate=%s proxy=%s live_sessions=%d",
                _token_log_id(token),
                TLS_IMPERSONATE,
                _mask_proxy_for_log(proxy) if proxy else "none",
                len(_TLS_SESSIONS),
            )

        return sess


def _mask_proxy_for_log(proxy: str) -> str:
    if "@" in proxy:
        scheme_rest = proxy.split("://", 1)
        if len(scheme_rest) == 2:
            creds_host = scheme_rest[1].split("@", 1)
            if len(creds_host) == 2:
                return f"{scheme_rest[0]}://***@{creds_host[1]}"
    return proxy


async def _prime_tls_session(sess, token: str, proxy: str | None = None) -> None:
    """Warm the session's cookie jar, through ``proxy`` if this token has
    one leased, so cookies are tied to the same source IP real requests will
    use."""
    try:
        await sess.get("https://discord.com", timeout=12, proxy=proxy)
    except Exception as e:
        logger.debug("TLS session cookie priming failed for token: %r", e)


async def close_tls_session(token: str, reason: str = "unspecified") -> None:
    """Drop a token's cached curl_cffi session (e.g. on token removal/revoke)
    and release its proxy lease, if any, back to the pool.

    ``reason`` is purely for the log line — pass something like
    "revoked-401", "idle-timeout", or "shutdown" so the log tells you *why*
    a given session went away, not just that it did.
    """
    async with _tls_session_lock(token):
        sess = _TLS_SESSIONS.pop(token, None)
        _TLS_PRIMED.discard(token)
        _TLS_LAST_USED.pop(token, None)
        if sess is not None:
            try:
                await sess.close()
            except Exception:
                pass
    _TLS_SESSION_LOCKS.pop(token, None)

    await _get_proxy_pool().release(token)

    if sess is not None:
        logger.debug(
            "TLS session closed: token=%s reason=%s live_sessions=%d",
            _token_log_id(token),
            reason,
            len(_TLS_SESSIONS),
        )


async def close_all_tls_sessions() -> None:
    """Drop every cached curl_cffi session (e.g. on shutdown)."""
    tokens = list(_TLS_SESSIONS.keys())
    for token in tokens:
        await close_tls_session(token, reason="shutdown")


async def reap_idle_tls_sessions(idle_ttl: float = TLS_SESSION_IDLE_TTL) -> int:
    """Close every cached session that hasn't been used in ``idle_ttl``
    seconds.
    """
    now = time.monotonic()
    stale = [
        token
        for token, last in list(_TLS_LAST_USED.items())
        if (now - last) >= idle_ttl
    ]
    for token in stale:
        await close_tls_session(token, reason="idle-timeout")
    if stale:
        logger.info(
            "TLS idle reap: closed %d session(s), %d still live",
            len(stale),
            len(_TLS_SESSIONS),
        )
    return len(stale)


def session_state(token: str) -> dict:
    """Live TLS-session state for one token, for debug output.

    A closed session is not an error: sessions are created on first send and
    reaped after ``TLS_SESSION_IDLE_TTL``, so a token that has not posted
    recently simply has none.
    """
    now = time.monotonic()
    last = _TLS_LAST_USED.get(token)
    return {
        "session_open": token in _TLS_SESSIONS,
        "cookies_primed": token in _TLS_PRIMED,
        "idle_seconds": round(now - last, 1) if last is not None else None,
        "impersonate": TLS_IMPERSONATE,
    }


def live_session_count() -> int:
    """How many per-token TLS sessions are currently open."""
    return len(_TLS_SESSIONS)
