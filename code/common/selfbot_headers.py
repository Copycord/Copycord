# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
from __future__ import annotations

import base64
import hashlib
import json
import logging
import random

import aiohttp

logger = logging.getLogger(__name__)


BUILD_INFO_URL = "https://api.macslodge.com/discord/build"


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


async def refresh_build_info(session: aiohttp.ClientSession | None = None) -> dict:
    """Fetch the current Discord *stable* build fingerprint."""
    owns = session is None
    sess = session or aiohttp.ClientSession()
    try:
        async with sess.get(
            BUILD_INFO_URL, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                logger.debug("build-info fetch: HTTP %s", resp.status)
                return get_build_info()
            data = await resp.json()

        dec = (((data or {}).get("clients") or {}).get("Discord") or {}).get(
            "decoded"
        ) or {}
        build_number = int(dec.get("client_build_number") or 0)
        ua = str(dec.get("browser_user_agent") or "")

        # so a bad/spoofed response can't degrade the fingerprint below fallback.
        if build_number < 400000 or "discord/" not in ua:
            logger.debug("build-info fetch: implausible payload; keeping current build")
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
        logger.info(
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
    """A deterministic UUIDv4-format string from a seeded RNG (so a token's
    launch ids are stable per account but unique across accounts)."""
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
    """Base64 ``X-Context-Properties`` value the real client sends with actions."""
    return base64.b64encode(
        json.dumps({"location": location}, separators=(",", ":")).encode()
    ).decode()
