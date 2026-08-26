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
import hashlib
import json
import logging
import random
import re
import time
import uuid

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


_DISCORD_APP_URL = "https://discord.com/app"
_DISCORD_ASSET_URL = "https://discord.com/assets/{asset}"
_ASSET_RE = re.compile(r'(?:src|href)="/assets/([^"]+\.js)"')
_BUILD_NUMBER_MARKERS = (
    'buildNumber:"',
    'build_number:"',
    '"buildNumber":"',
    '"build_number":',
)


TLS_IMPERSONATE = "chrome150"


TLS_SESSION_IDLE_TTL = 3600


DEFAULT_BUILD: dict = {
    "release_channel": "stable",
    "browser_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    ),
    "browser_version": "150.0.0.0",
    "client_build_number": 600590,
}


_BUILD: dict = dict(DEFAULT_BUILD)


_FINGERPRINT_CACHE: dict[str, dict] = {}


_OS_VERSION = "10"

LOCALE = "en-US"
TIMEZONE = "America/New_York"


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
    """Read the live build number out of Discord's web assets."""
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
    """Refresh the shared build number from Discord's live web assets."""
    owns = session is None
    sess = session or aiohttp.ClientSession()
    try:
        scraped = await _scrape_build_number_from_web(sess)
        if not scraped or scraped < 400000:
            logger.debug(
                "build number unresolved; keeping built-in %s",
                DEFAULT_BUILD["client_build_number"],
            )
            return get_build_info()

        logger.info("Fetched latest Discord build: %s", scraped)
        if scraped != get_build_info().get("client_build_number"):
            set_build_info({"client_build_number": scraped})
        return get_build_info()
    except Exception as e:
        logger.debug("build-info fetch failed: %r", e)
        return get_build_info()
    finally:
        if owns:
            try:
                await sess.close()
            except Exception:
                pass


def make_fingerprint(token: str) -> dict:
    """Build a stable device fingerprint for a token."""
    seed = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)

    build = get_build_info()
    install_id = _installation_id(rng)
    launch_id = str(uuid.uuid4())
    launch_signature = str(uuid.uuid4())

    user_agent = build["browser_user_agent"]

    common = {
        "os": "Windows",
        "browser": "Chrome",
        "device": "",
        "system_locale": LOCALE,
        "has_client_mods": False,
        "browser_user_agent": user_agent,
        "browser_version": build["browser_version"],
        "os_version": _OS_VERSION,
        "referrer": "",
        "referring_domain": "",
    }

    super_props = {
        **common,
        "referrer_current": "https://discord.com/",
        "referring_domain_current": "discord.com",
        "release_channel": build["release_channel"],
        "client_build_number": build["client_build_number"],
        "client_event_source": None,
        "client_launch_id": launch_id,
        "launch_signature": launch_signature,
        "client_app_state": "focused",
        "client_heartbeat_session_id": "",
    }

    gateway_props = {
        **common,
        "referrer_current": "",
        "referring_domain_current": "",
        "release_channel": build["release_channel"],
        "client_build_number": build["client_build_number"],
        "client_event_source": None,
        "client_launch_id": launch_id,
        "installation_id": install_id,
        "is_fast_connect": True,
    }

    headers = {
        "User-Agent": user_agent,
        "X-Discord-Locale": LOCALE,
        "X-Discord-Timezone": TIMEZONE,
        "X-Debug-Options": "bugReporterEnabled",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "X-Installation-Id": install_id,
        "Sec-CH-UA": _sec_ch_ua(user_agent),
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Sec-GPC": "1",
        "Origin": "https://discord.com",
        "Priority": "u=1, i",
    }
    return {
        "headers": headers,
        "super_props": super_props,
        "super_props_b64": _b64_props(super_props),
        "gateway_props": gateway_props,
        "install_id": install_id,
    }


def _b64_props(props: dict) -> str:
    return base64.b64encode(json.dumps(props, separators=(",", ":")).encode()).decode()


def gateway_properties(token: str) -> dict:
    """The ``properties`` object for a gateway IDENTIFY."""
    return _fingerprint(token)["gateway_props"]


def _installation_id(rng: random.Random) -> str:
    """A stable per-install id shaped like the client's: <snowflake>.<27 chars>."""
    snowflake = rng.randrange(10**18, 10**19)
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    suffix = "".join(rng.choice(alphabet) for _ in range(27))
    return f"{snowflake}.{suffix}"


def _sec_ch_ua(user_agent: str) -> str:
    """Chrome's Sec-CH-UA brand list for the Chrome major in ``user_agent``."""
    m = re.search(r"Chrome/(\d+)", user_agent or "")
    major = m.group(1) if m else "150"
    return (
        f'"Not=A?Brand";v="99", "Google Chrome";v="{major}", ' f'"Chromium";v="{major}"'
    )


SUPPRESSED_PROFILE_HEADERS = {
    "sec-fetch-user": None,
    "upgrade-insecure-requests": None,
}


def _fingerprint(token: str) -> dict:
    fp = _FINGERPRINT_CACHE.get(token)
    if fp is None:
        fp = make_fingerprint(token)
        _FINGERPRINT_CACHE[token] = fp
    return fp


_HEARTBEAT_SESSIONS: dict[str, str] = {}


def set_heartbeat_session_id(token: str, session_id: str | None) -> None:
    """Record the heartbeat session id a gateway connection just produced."""
    if session_id:
        _HEARTBEAT_SESSIONS[token] = str(session_id)
    else:
        _HEARTBEAT_SESSIONS.pop(token, None)


def build_headers(token: str, *, referer: str | None = None) -> dict:
    """Full header dict for a user-token REST request."""
    fp = _fingerprint(token)

    heartbeat_id = _HEARTBEAT_SESSIONS.get(token)
    if heartbeat_id:
        props = dict(fp["super_props"])
        props["client_heartbeat_session_id"] = heartbeat_id
        super_props_b64 = _b64_props(props)
    else:
        super_props_b64 = fp["super_props_b64"]

    h = fp["headers"]
    return {
        "Accept": h["Accept"],
        "Accept-Language": h["Accept-Language"],
        "Authorization": token,
        "Content-Type": h["Content-Type"],
        "Origin": h["Origin"],
        "Priority": h["Priority"],
        "Referer": referer or "https://discord.com/channels/@me",
        "Sec-CH-UA": h["Sec-CH-UA"],
        "Sec-CH-UA-Mobile": h["Sec-CH-UA-Mobile"],
        "Sec-CH-UA-Platform": h["Sec-CH-UA-Platform"],
        "Sec-Fetch-Dest": h["Sec-Fetch-Dest"],
        "Sec-Fetch-Mode": h["Sec-Fetch-Mode"],
        "Sec-Fetch-Site": h["Sec-Fetch-Site"],
        "Sec-GPC": h["Sec-GPC"],
        "User-Agent": h["User-Agent"],
        "X-Debug-Options": h["X-Debug-Options"],
        "X-Discord-Locale": h["X-Discord-Locale"],
        "X-Discord-Timezone": h["X-Discord-Timezone"],
        "X-Installation-Id": h["X-Installation-Id"],
        "X-Super-Properties": super_props_b64,
    }


def channel_referer(channel_id, guild_id=None) -> str:
    """The URL the client would be sitting on to send into this channel."""
    scope = str(guild_id) if guild_id else "@me"
    return f"https://discord.com/channels/{scope}/{channel_id}"


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
    """Return a curl_cffi ``AsyncSession`` dedicated to this token."""
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
    """
    async with _tls_session_lock(token):
        sess = _TLS_SESSIONS.pop(token, None)
        _TLS_PRIMED.discard(token)
        _TLS_LAST_USED.pop(token, None)
        _HEARTBEAT_SESSIONS.pop(token, None)
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


REAL_CLIENT_HEADERS = {
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua": None,
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "sec-gpc": "1",
    "origin": "https://discord.com",
    "referer": None,
    "x-installation-id": None,
    "x-debug-options": "bugReporterEnabled",
    "x-discord-locale": None,
    "x-discord-timezone": None,
    "x-super-properties": None,
    "x-context-properties": None,
    "authorization": None,
    "user-agent": None,
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "accept-encoding": "gzip, deflate, br, zstd",
    "content-type": "application/json",
    "priority": "u=1, i",
}


REAL_CLIENT_ABSENT = (
    "sec-fetch-user",
    "upgrade-insecure-requests",
)


_WIRE_IGNORE = (
    "host",
    "content-length",
    "cookie",
    "x-forwarded-proto",
    "x-amzn-trace-id",
)


def diff_against_real_client(wire_headers: dict) -> dict:
    """Compare headers actually sent against the real-client reference."""
    got = {k.lower(): v for k, v in (wire_headers or {}).items()}

    missing, mismatched = [], {}
    for name, expected in REAL_CLIENT_HEADERS.items():
        if name not in got:
            missing.append(name)
        elif expected is not None and got[name] != expected:
            mismatched[name] = {"expected": expected, "got": got[name]}

    extra = [n for n in REAL_CLIENT_ABSENT if n in got]
    unexpected = sorted(
        n
        for n in got
        if n not in REAL_CLIENT_HEADERS
        and n not in REAL_CLIENT_ABSENT
        and n not in _WIRE_IGNORE
    )

    return {
        "matches_real_client": not (missing or mismatched or extra),
        "missing": missing,
        "mismatched": mismatched,
        "present_but_real_client_omits": extra,
        "not_in_reference": unexpected,
    }
