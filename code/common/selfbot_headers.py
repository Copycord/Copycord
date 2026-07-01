# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Shared realistic Discord desktop-client headers for user ("self-bot") tokens.

Both the admin process (token *validation*) and the server process (message
*sending*) must present the SAME browser-like headers when talking to Discord's
REST API with a user token. Discord rejects bare requests — an ``Authorization``
header with a python/aiohttp User-Agent and no client fingerprint — with HTTP
401 even for a valid member account. So every user-token request must carry a
realistic User-Agent plus an ``X-Super-Properties`` fingerprint.

Fingerprints are derived deterministically from the token, so one account always
looks like the same machine across requests and processes, while different
accounts never share a fingerprint (a fleet of accounts sending an identical
fingerprint is itself a detection signal).
"""

from __future__ import annotations

import base64
import hashlib
import json
import random

# token -> {"headers": {...}, "super_props_b64": "..."}
_FINGERPRINT_CACHE: dict[str, dict] = {}


def make_fingerprint(token: str) -> dict:
    """Build a stable, unique device fingerprint for a token.

    Returns ``{"headers": {...}, "super_props_b64": "..."}``. The result is
    deterministic per token (seeded by its SHA-256), so the same account always
    presents the same fingerprint.
    """
    # Deterministic per-token RNG → a stable, unique fingerprint per account.
    seed = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)

    chrome_major = rng.choice([128, 130, 132, 134, 136, 139])
    chrome_version = (
        f"{chrome_major}.0.{rng.randint(6000, 7300)}.{rng.randint(30, 220)}"
    )
    client_version = rng.choice(["1.0.9179", "1.0.9163", "1.0.9156", "1.0.9154"])
    electron_version = rng.choice(["32.2.5", "31.3.1", "28.2.10", "22.3.26"])
    os_version = rng.choice(
        ["10.0.19045", "10.0.22621", "10.0.22631", "10.0.26100"]
    )
    build_number = rng.randint(330000, 366000)
    native_build = rng.randint(60000, 64000)
    locale = rng.choice(["en-US", "en-GB", "de", "fr", "es-ES", "nl", "pt-BR"])
    tz = rng.choice(
        [
            "America/New_York",
            "America/Chicago",
            "America/Los_Angeles",
            "Europe/London",
            "Europe/Berlin",
            "Europe/Amsterdam",
            "Asia/Tokyo",
        ]
    )

    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"discord/{client_version} Chrome/{chrome_version} "
        f"Electron/{electron_version} Safari/537.36"
    )

    # Shape matches discord.py-self's super-properties for the desktop app.
    super_props = {
        "os": "Windows",
        "browser": "Discord Client",
        "release_channel": "stable",
        "client_version": client_version,
        "os_version": os_version,
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": locale,
        "browser_user_agent": user_agent,
        "browser_version": chrome_version,
        "client_build_number": build_number,
        "native_build_number": native_build,
        "client_event_source": None,
        "device": "",
        "referrer": "",
        "referring_domain": "",
        "referrer_current": "",
        "referring_domain_current": "",
    }
    super_props_b64 = base64.b64encode(
        json.dumps(super_props, separators=(",", ":")).encode()
    ).decode()

    headers = {
        "User-Agent": user_agent,
        "X-Discord-Locale": locale,
        "X-Discord-Timezone": tz,
        "Accept": "*/*",
        "Accept-Language": f"{locale},en;q=0.9",
        "Origin": "https://discord.com",
        "Referer": "https://discord.com/channels/@me",
        "Sec-CH-UA": (
            f'"Chromium";v="{chrome_major}", '
            f'"Not(A:Brand";v="24", '
            f'"Google Chrome";v="{chrome_major}"'
        ),
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
    }
    return {"headers": headers, "super_props_b64": super_props_b64}


def build_headers(token: str) -> dict:
    """Full header dict for a user-token REST request.

    Includes the realistic browser headers, the ``Authorization`` token, and the
    ``X-Super-Properties`` fingerprint. Fingerprints are cached per token.
    """
    fp = _FINGERPRINT_CACHE.get(token)
    if fp is None:
        fp = make_fingerprint(token)
        _FINGERPRINT_CACHE[token] = fp
    return {
        **fp["headers"],
        "Authorization": token,
        "X-Super-Properties": fp["super_props_b64"],
    }
