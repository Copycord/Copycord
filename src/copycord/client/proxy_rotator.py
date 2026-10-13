# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations

import hashlib
import logging
import os
import random
import re
import asyncio
import time
from pathlib import Path
from typing import List, Optional, Dict

from copycord.common.paths import data_dir

logger = logging.getLogger("client.proxy_rotator")

DATA_DIR = data_dir()
_PROXY_FILE = DATA_DIR / "proxies.txt"


_HP_UP = re.compile(r"^(?P<host>[^:]+):(?P<port>\d+):(?P<user>[^:]+):(?P<pass>.+)$")

_UP_HP = re.compile(r"^(?P<user>[^:@]+):(?P<pass>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)$")


def _normalise_proxy_url(raw: str) -> Optional[str]:
    """Normalise common proxy formats to ``scheme://[user:pass@]host:port``; None if unparsable."""
    raw = raw.strip()
    if not raw:
        return None

    scheme = "http"
    if "://" in raw:
        scheme, _, raw = raw.partition("://")

    m = _HP_UP.match(raw)
    if m:
        return f"{scheme}://{m.group('user')}:{m.group('pass')}@{m.group('host')}:{m.group('port')}"

    m = _UP_HP.match(raw)
    if m:
        return f"{scheme}://{raw}"

    if ":" in raw:
        return f"{scheme}://{raw}"

    return None


class ProxyRotator:
    """Proxy rotator with health tracking and optional timed rotation.

    Sticks with one proxy until it is suspended after repeated failures, or
    rotates every ``rotation_interval`` seconds when that is set.
    """

    MAX_FAILURES = 3

    SUSPEND_SECONDS = 300

    def __init__(self) -> None:
        self._proxies: List[str] = []
        self._lock = asyncio.Lock()
        self._enabled: bool = False

        self._health: Dict[str, Dict] = {}

        # empty favorites means every proxy is acceptable
        self._favorites: set[str] = set()

        # 0 = per-request round-robin
        self._rotation_interval: int = 0
        self._current_proxy: Optional[str] = None
        self._current_proxy_since: float = 0.0

        self.on_all_dead: Optional[callable] = None
        self.on_rotate: Optional[callable] = None

    @property
    def enabled(self) -> bool:
        return self._enabled and len(self._proxies) > 0

    @property
    def count(self) -> int:
        return len(self._proxies)

    @property
    def healthy_count(self) -> int:
        """Number of proxies currently not suspended."""
        now = time.monotonic()
        return sum(1 for p in self._proxies if not self._is_suspended(p, now))

    @property
    def proxies(self) -> List[str]:
        return list(self._proxies)

    @property
    def rotation_interval(self) -> int:
        """Rotation interval in seconds (0 = per-request)."""
        return self._rotation_interval

    def set_rotation_interval(self, seconds: int) -> None:
        """Set the timed rotation interval in seconds; 0 disables it."""
        seconds = max(0, int(seconds))
        if seconds != self._rotation_interval:
            self._rotation_interval = seconds
            # reset so the next call picks fresh
            self._current_proxy = None
            self._current_proxy_since = 0.0
            if seconds:
                logger.debug(
                    "[🔀] Proxy rotation interval set to %d seconds", seconds
                )
            else:
                logger.debug("[🔀] Timed rotation disabled (sticky mode)")

    @property
    def favorites(self) -> List[str]:
        return sorted(self._favorites)

    @property
    def has_favorites(self) -> bool:
        return bool(self._favorites & set(self._proxies))

    def set_favorites(self, raw_lines: Optional[List[str]]) -> int:
        """Mark proxies as preferred for the gateway; raw forms are normalised to match the rotator's URLs."""
        normalised: set[str] = set()
        for line in raw_lines or []:
            url = _normalise_proxy_url(line)
            if url:
                normalised.add(url)

        if normalised != self._favorites:
            self._favorites = normalised
            logger.debug("[⭐] %d favorite prox(ies) configured", len(normalised))
        return len(normalised)

    def is_favorite(self, proxy_url: Optional[str]) -> bool:
        return bool(proxy_url) and proxy_url in self._favorites

    def healthy_favorites(self) -> List[str]:
        """Favorites in the list that are not currently suspended."""
        now = time.monotonic()
        return [
            p
            for p in self._proxies
            if p in self._favorites and not self._is_suspended(p, now)
        ]

    def on_fallback(self) -> bool:
        """True when on a non-favorite while favorites are configured."""
        if not self.has_favorites:
            return False
        return not self.is_favorite(self._current_proxy)

    def clear_suspension(self, proxy_url: str) -> None:
        """Forget a proxy's failure history after it was probed working."""
        info = self._health.get(proxy_url)
        if info:
            info["failures"] = 0
            info["suspended_until"] = 0

    def set_enabled(self, on: bool) -> None:
        prev = self._enabled
        self._enabled = bool(on)

        if on == prev:
            return
        if on and self._proxies:
            logger.debug(
                "[🔀] Client proxy rotation ENABLED (%d proxies)", len(self._proxies)
            )
        elif on:
            logger.warning("[⚠️] Client proxy rotation enabled but NO proxies loaded")
        else:
            logger.debug("[🔀] Client proxy rotation DISABLED")
            self._current_proxy = None
            self._current_proxy_since = 0.0

    def reload(self, proxy_lines: Optional[List[str]] = None) -> int:
        """(Re)load the proxy list from proxy_lines or disk; returns the number loaded."""
        if proxy_lines is None:
            proxy_lines = self._read_file()

        normalised = []
        for line in proxy_lines:
            url = _normalise_proxy_url(line)
            if url:
                normalised.append(url)

        self._proxies = normalised

        self._health = {k: v for k, v in self._health.items() if k in normalised}

        # reset time-based assignment when the list changes
        self._current_proxy = None
        self._current_proxy_since = 0.0

        logger.debug("[🔀] Loaded %d client proxies", len(normalised))
        return len(normalised)

    def next(self, *, exclude: Optional[set] = None) -> Optional[str]:
        """Return the next healthy proxy URL, or None.

        With ``exclude``, a temporary fallback is returned without changing the sticky proxy.
        """
        if not self._proxies:
            return None

        exclude = exclude or set()
        now = time.monotonic()

        # is the current proxy still usable
        if (
            self._current_proxy
            and self._current_proxy not in exclude
            and not self._is_suspended(self._current_proxy, now)
        ):
            # with timed rotation, check whether the interval expired
            if self._rotation_interval > 0:
                if (now - self._current_proxy_since) < self._rotation_interval:
                    return self._current_proxy
                # expired, assign a new one
            else:
                # no rotation, stay on this proxy
                return self._current_proxy

        # retry/fallback: temporary alternative, sticky proxy unchanged
        if exclude:
            return self._next_healthy(exclude, now)

        # rotation or first assignment
        proxy = self._next_healthy(exclude, now)
        if proxy:
            old = self._current_proxy
            self._current_proxy = proxy
            self._current_proxy_since = now
            safe = _mask_proxy_url(proxy)
            if old is None:
                logger.info("[🔀] Using proxy %s", safe)
            elif old != proxy:
                logger.info("[🔀] Switched to proxy %s", safe)
        return proxy

    def rotate_now(self) -> Optional[str]:
        """Rotate immediately to the next healthy proxy; returns it, or None."""
        if not self._proxies or not self._enabled:
            return None

        now = time.monotonic()
        exclude = {self._current_proxy} if self._current_proxy else set()
        proxy = self._next_healthy(exclude, now)

        # if all others are dead, stay on the current one
        if not proxy:
            proxy = self._current_proxy

        if proxy and proxy != self._current_proxy:
            self._current_proxy = proxy
            self._current_proxy_since = now
            logger.info("[🔀] Switched to proxy %s", _mask_proxy_url(proxy))
            if self.on_rotate:
                try:
                    self.on_rotate(proxy)
                except Exception:
                    pass
        elif proxy and self._current_proxy is None:
            self._current_proxy = proxy
            self._current_proxy_since = now

        return proxy

    async def run_rotation_loop(self) -> None:
        """Background task that handles timed proxy rotation."""
        while True:
            if not self._enabled or self._rotation_interval <= 0:
                await asyncio.sleep(5)
                continue

            if self._current_proxy:
                elapsed = time.monotonic() - self._current_proxy_since
                if elapsed >= self._rotation_interval:
                    self.rotate_now()

            await asyncio.sleep(5)

    def _next_healthy(self, exclude: set, now: float) -> Optional[str]:
        """Pick a random healthy, non-excluded proxy; any healthy favorite wins over non-favorites."""
        candidates = [
            url for url in self._proxies
            if url not in exclude and not self._is_suspended(url, now)
        ]
        if not candidates:
            return None

        preferred = [url for url in candidates if url in self._favorites]
        return random.choice(preferred or candidates)

    def report_success(self, proxy_url: str) -> None:
        """Mark a proxy as healthy after a successful request."""
        if proxy_url in self._health:
            self._health[proxy_url]["failures"] = 0
            self._health[proxy_url]["suspended_until"] = 0

    def report_failure(self, proxy_url: str) -> None:
        """Record a failure; suspend if threshold exceeded."""
        if proxy_url not in self._health:
            self._health[proxy_url] = {"failures": 0, "suspended_until": 0}

        info = self._health[proxy_url]

        # already suspended, avoid duplicate log spam

        if info.get("suspended_until", 0) > time.monotonic():
            return

        info["failures"] += 1

        threshold = 1 if len(self._proxies) <= 2 else self.MAX_FAILURES

        if info["failures"] >= threshold:
            info["suspended_until"] = time.monotonic() + self.SUSPEND_SECONDS
            safe = _mask_proxy_url(proxy_url)
            logger.debug(
                "[🔀] Proxy %s suspended for %ds after %d consecutive failure(s)",
                safe,
                self.SUSPEND_SECONDS,
                info["failures"],
            )

            # force rotation if this was the current time-based proxy
            if self._current_proxy == proxy_url:
                self._current_proxy = None
                self._current_proxy_since = 0.0

            if self._enabled and self.healthy_count == 0:
                logger.warning(
                    "[🔀] All %d proxies dead — no healthy proxies available",
                    len(self._proxies),
                )
                if self.on_all_dead:
                    try:
                        self.on_all_dead(self)
                    except Exception:
                        pass

    def _is_suspended(self, proxy_url: str, now: float) -> bool:
        info = self._health.get(proxy_url)
        if not info:
            return False
        until = info.get("suspended_until", 0)
        if until and now < until:
            return True

        if until and now >= until:
            info["suspended_until"] = 0
            info["failures"] = 0
        return False

    @staticmethod
    def _read_file() -> List[str]:
        if not _PROXY_FILE.exists():
            return []
        try:
            text = _PROXY_FILE.read_text(encoding="utf-8").strip()
            return [l.strip() for l in text.splitlines() if l.strip()]
        except Exception as e:
            logger.warning("[⚠️] Failed to read proxy file: %s", e)
            return []


def _mask_proxy_url(url: str) -> str:
    """Mask credentials in a proxy URL, appending a short hash of the whole URL.

    Rotating-proxy pools differ only in the credentials, so without the hash every entry logs identically.
    """
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    try:
        if "@" in url:
            scheme_rest = url.split("://", 1)
            if len(scheme_rest) == 2:
                creds_host = scheme_rest[1].split("@", 1)
                if len(creds_host) == 2:
                    return f"{scheme_rest[0]}://***@{creds_host[1]} [{digest}]"
    except Exception:
        pass
    short = url[:40] + "…" if len(url) > 40 else url
    return f"{short} [{digest}]"
