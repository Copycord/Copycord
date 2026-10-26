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

from copycord.common.exit_timezone import (
    FALLBACK_TIMEZONE,
    forget_token as _forget_exit_timezone,
    note_token_proxy as _note_token_proxy,
    timezone_for as _timezone_for,
)
from copycord.common.http_session import shared_session
from copycord.common.proxy_pool import get_pool as _get_proxy_pool
from copycord.common.token_proxy import (
    USE_PROXY,
    NoProxyAvailable,
    lease_with_timezone,
    resolve_use_proxy,
)

try:
    from curl_cffi.requests import AsyncSession as _CurlAsyncSession
except ImportError:
    _CurlAsyncSession = None

logger = logging.getLogger("common.selfbot_headers")


def _token_log_id(token: str) -> str:
    """Short non-reversible id for a token, safe to log (even a raw prefix would leak part of the credential)."""
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


try:
    from curl_cffi.requests import impersonate as _impersonate

    TLS_IMPERSONATE = _impersonate.DEFAULT_CHROME
except Exception:
    TLS_IMPERSONATE = "chrome150"

_m = re.search(r"(\d+)", TLS_IMPERSONATE or "")
CHROME_MAJOR = _m.group(1) if _m else "150"


TLS_SESSION_IDLE_TTL = 3600


DEFAULT_BUILD: dict = {
    "release_channel": "stable",
    "browser_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        f"(KHTML, like Gecko) Chrome/{CHROME_MAJOR}.0.0.0 Safari/537.36"
    ),
    "browser_version": f"{CHROME_MAJOR}.0.0.0",
    "client_build_number": 600590,
}


_BUILD: dict = dict(DEFAULT_BUILD)


_FINGERPRINT_CACHE: dict[str, dict] = {}


_OS_VERSION = "10"

LOCALE = "en-US"
# Fallback only; the header uses the zone of the IP the token exits through.
TIMEZONE = FALLBACK_TIMEZONE


def get_build_info() -> dict:
    """Return a copy of the current shared build fingerprint."""
    return dict(_BUILD)


def set_build_info(build: dict) -> None:
    """Replace the shared build fingerprint and invalidate cached per-token fingerprints."""
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


# A scraped number below this is not a real build; a failed scrape keeps the
# current build (DEFAULT_BUILD until a scrape succeeds).
_MIN_BUILD = 400000


async def refresh_build_info(session: aiohttp.ClientSession | None = None) -> dict:
    """Refresh the shared build number from Discord's live web assets."""
    # Without a caller's session, use the role's shared one (closed on role shutdown).
    sess = session or shared_session()
    try:
        scraped = await _scrape_build_number_from_web(sess)
        if not scraped or scraped < _MIN_BUILD:
            logger.debug(
                "build number unresolved; keeping %s",
                get_build_info().get("client_build_number"),
            )
            return get_build_info()

        logger.info("Fetched latest Discord build: %s", scraped)
        if scraped != get_build_info().get("client_build_number"):
            set_build_info({"client_build_number": scraped})
        return get_build_info()
    except Exception as e:
        logger.debug("build-info fetch failed: %r", e)
        return get_build_info()


def _launch_signature() -> str:
    """UUID4 with a fixed scatter of bits zeroed: the shape the Discord web client produces."""
    mask = int(
        "00000000100000000001000000010000000010000001000000001000000000000010"
        "000010000001000000000100000000000001000000000000100000000000",
        2,
    )
    return str(uuid.UUID(int=uuid.uuid4().int & ~mask & ((1 << 128) - 1)))


def make_fingerprint(token: str) -> dict:
    """Build a stable device fingerprint for a token."""
    seed = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)

    build = get_build_info()
    install_id = _installation_id(rng)
    launch_id = str(uuid.uuid4())
    launch_signature = _launch_signature()

    user_agent = build["browser_user_agent"]

    # Key order is the browser's: Discord sees it in the JSON.
    browser = {
        "system_locale": LOCALE,
        "has_client_mods": False,
        "browser_user_agent": user_agent,
        "browser_version": build["browser_version"],
        "os_version": _OS_VERSION,
        "referrer": "",
        "referring_domain": "",
    }

    super_props = {
        "os": "Windows",
        "browser": "Chrome",
        "device": "",
        "device_model_name": "",
        **browser,
        "referrer_current": "https://discord.com/",
        "referring_domain_current": "discord.com",
        "release_channel": build["release_channel"],
        "client_build_number": build["client_build_number"],
        "client_event_source": None,
        "client_launch_id": launch_id,
        "launch_signature": launch_signature,
        # A tab that loaded unfocused. Once its gateway tab is focused (the
        # warm pool), focused_super_props_b64 makes it "focused" and adds
        # client_heartbeat_session_id.
        "client_app_state": "unfocused",
    }

    gateway_props = {
        "os": "Windows",
        "browser": "Chrome",
        "device": "",
        "device_model_name": "",
        **browser,
        "referrer_current": "",
        "referring_domain_current": "",
        "release_channel": build["release_channel"],
        "client_build_number": build["client_build_number"],
        "client_event_source": None,
        "client_launch_id": launch_id,
        "launch_signature": launch_signature,
        "client_app_state": "unfocused",
        "is_fast_connect": False,
        "gateway_connect_reasons": "AppSkeleton",
        "installation_id": install_id,
        # qos_token is not a property: IDENTIFY carries it at the top level.
    }

    headers = {
        "User-Agent": user_agent,
        "X-Discord-Locale": LOCALE,
        "X-Discord-Timezone": _timezone_for(token),
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


# Chromium components/embedder_support/user_agent_utils.cc,
# GetGreasedUserAgentBrandVersion and GetRandomOrder: the filler brand, its
# version and the brand order are all seeded by the Chrome major version.
_GREASE_CHARS = (" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_")
_GREASE_VERSIONS = ("8", "99", "24")
_BRAND_ORDERS = (
    (0, 1, 2),
    (0, 2, 1),
    (1, 0, 2),
    (1, 2, 0),
    (2, 0, 1),
    (2, 1, 0),
)


def _sec_ch_ua(user_agent: str) -> str:
    """Chrome's Sec-CH-UA brand list for the Chrome major in ``user_agent``."""
    m = re.search(r"Chrome/(\d+)", user_agent or "")
    major = int(m.group(1)) if m else 150
    grease = (
        f"Not{_GREASE_CHARS[major % len(_GREASE_CHARS)]}A"
        f"{_GREASE_CHARS[(major + 1) % len(_GREASE_CHARS)]}Brand"
    )
    brands = [
        (grease, _GREASE_VERSIONS[major % len(_GREASE_VERSIONS)]),
        ("Chromium", str(major)),
        ("Google Chrome", str(major)),
    ]
    ordered: list = [None] * len(brands)
    for source, target in enumerate(_BRAND_ORDERS[major % len(_BRAND_ORDERS)]):
        ordered[target] = brands[source]
    return ", ".join(f'"{brand}";v="{version}"' for brand, version in ordered)


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


def focused_super_props_b64(token: str, heartbeat_session_id: str) -> str:
    """X-Super-Properties of a focused tab: client_app_state "focused" and then
    client_heartbeat_session_id, the op 41 session id, last (the 2026-10-24
    login capture's key order)."""
    fp = _fingerprint(token)
    cached = fp.get("focused_super_props")
    if cached is not None and cached[0] == heartbeat_session_id:
        return cached[1]
    props = dict(fp["super_props"])
    props["client_app_state"] = "focused"
    props["client_heartbeat_session_id"] = heartbeat_session_id
    encoded = _b64_props(props)
    fp["focused_super_props"] = (heartbeat_session_id, encoded)
    return encoded


def build_headers(
    token: str, *, referer: str | None = None, heartbeat_session_id: str | None = None
) -> dict:
    """Full header dict for a user-token REST request. ``heartbeat_session_id``
    is set while the token's gateway tab is focused."""
    fp = _fingerprint(token)
    h = fp["headers"]
    super_props = (
        focused_super_props_b64(token, heartbeat_session_id)
        if heartbeat_session_id
        else fp["super_props_b64"]
    )
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
        "User-Agent": h["User-Agent"],
        "X-Debug-Options": h["X-Debug-Options"],
        "X-Discord-Locale": h["X-Discord-Locale"],
        "X-Discord-Timezone": _timezone_for(token),
        "X-Installation-Id": h["X-Installation-Id"],
        "X-Super-Properties": super_props,
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
# Tokens whose session was primed direct (their mapping has proxies off).
_TLS_DIRECT: set[str] = set()
_TLS_SESSION_LOCKS: dict[str, asyncio.Lock] = {}
_TLS_LAST_USED: dict[str, float] = {}
# Tokens whose curl_cffi session has another owner (the selfclient's REST
# client): get_tls_session hands that session out, so the token keeps one TLS
# identity. Never closed, reaped or primed here; the owner does that.
_TLS_SHARED: dict = {}


def share_tls_session(token: str, provider) -> None:
    """Have get_tls_session(token) return provider(), the owner's session."""
    if token:
        _TLS_SHARED[token] = provider


def unshare_tls_session(token: str, provider=None) -> None:
    """Stop sharing token's session; with provider, only while it is still the shared one."""
    cur = _TLS_SHARED.get(token)
    if cur is not None and (provider is None or cur == provider):
        del _TLS_SHARED[token]


def _shared_tls_session(token: str):
    provider = _TLS_SHARED.get(token)
    if provider is None:
        return None
    try:
        return provider()
    except Exception as e:
        logger.debug("shared TLS session unavailable for token=%s: %r", _token_log_id(token), e)
        return None


def _tls_session_lock(token: str) -> asyncio.Lock:
    lock = _TLS_SESSION_LOCKS.get(token)
    if lock is None:
        lock = asyncio.Lock()
        _TLS_SESSION_LOCKS[token] = lock
    return lock


async def require_token_proxy(token: str) -> str:
    """The proxy leased to ``token``, leasing one if needed and rotating past proxies whose
    exit timezone lookup fails; NoProxyAvailable when there is none."""
    return await lease_with_timezone(_get_proxy_pool(), token)


async def get_tls_session(token: str, *, use_proxy: bool = USE_PROXY):
    """Return a curl_cffi ``AsyncSession`` dedicated to this token.

    Proxies are on by default and optional per mapping. With ``use_proxy`` on and
    no proxy lease this raises NoProxyAvailable before any session is made or
    primed, so nothing goes out from the server's own address. With it off the
    proxy pool is never asked for a lease and the session talks to Discord directly.
    """
    use_proxy = resolve_use_proxy(token, use_proxy)
    shared = _shared_tls_session(token)
    if shared is not None:
        if use_proxy:
            await require_token_proxy(token)
        return shared
    if _CurlAsyncSession is None:
        raise RuntimeError("curl_cffi is not installed")

    async with _tls_session_lock(token):
        proxy = await require_token_proxy(token) if use_proxy else None
        if (token in _TLS_DIRECT) != (proxy is None):
            # Switched between direct and proxied: prime again from the new exit.
            _TLS_PRIMED.discard(token)
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

        if token not in _TLS_PRIMED:
            await _note_token_proxy(token, proxy)
            if use_proxy:
                await _prime_tls_session(sess, token, proxy=proxy)
            else:
                await _prime_tls_session(sess, token, direct=True)
            _TLS_PRIMED.add(token)
            if proxy is None:
                _TLS_DIRECT.add(token)
            else:
                _TLS_DIRECT.discard(token)

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


async def _prime_tls_session(
    sess, token: str, proxy: str | None = None, *, direct: bool = False
) -> None:
    """Warm the session's cookie jar from the exit the token sends from, so cookies match the source IP.

    That is the token's leased proxy, or the server's own address only when
    ``direct`` (its mapping has proxies turned off).
    """
    if not proxy and not direct:
        # never prime a proxied token's session from the server's own address
        raise NoProxyAvailable("cookie priming needs the token's proxy")
    try:
        await sess.get("https://discord.com", timeout=12, proxy=proxy)
    except Exception as e:
        logger.debug("TLS session cookie priming failed for token: %r", e)


async def close_tls_session(token: str, reason: str = "unspecified") -> None:
    """Drop a token's cached session and release its proxy lease."""
    async with _tls_session_lock(token):
        sess = _TLS_SESSIONS.pop(token, None)
        _TLS_PRIMED.discard(token)
        _TLS_DIRECT.discard(token)
        _TLS_LAST_USED.pop(token, None)
        _forget_exit_timezone(token)
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
    """Close cached sessions unused for ``idle_ttl`` seconds."""
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
