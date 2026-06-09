# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations

import logging
import os
import random
import re
import asyncio
import time
from pathlib import Path
from typing import List, Optional, Dict

logger = logging.getLogger("client.proxy_rotator")

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
_PROXY_FILE = DATA_DIR / "proxies.txt"


_HP_UP = re.compile(r"^(?P<host>[^:]+):(?P<port>\d+):(?P<user>[^:]+):(?P<pass>.+)$")

_UP_HP = re.compile(r"^(?P<user>[^:@]+):(?P<pass>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)$")


def _normalise_proxy_url(raw: str) -> Optional[str]:
    """
    Accept many common proxy formats and normalise to ``scheme://[user:pass@]host:port``.
    Returns *None* for lines that cannot be parsed.
    """
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
    """
    Proxy rotator with health tracking and optional timed rotation.

    By default one proxy is selected and used for all requests.  It only
    switches when the current proxy is suspended due to failures.

    When ``rotation_interval`` is set (> 0 seconds), the proxy
    automatically rotates to the next healthy one after the interval
    elapses.

    Proxies that fail repeatedly are temporarily suspended and
    automatically re-tested after a cooldown period.

    Usage::

        rotator = ProxyRotator()
        rotator.reload()

        if rotator.enabled:
            proxy_url = rotator.next()
    """

    MAX_FAILURES = 3

    SUSPEND_SECONDS = 300

    def __init__(self) -> None:
        self._proxies: List[str] = []
        self._lock = asyncio.Lock()
        self._enabled: bool = False

        self._health: Dict[str, Dict] = {}

        # Time-based rotation (0 = per-request round-robin)
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
        """Set the time-based rotation interval in seconds.

        ``0`` disables timed rotation (sticky — one proxy used until it
        fails).
        """
        seconds = max(0, int(seconds))
        if seconds != self._rotation_interval:
            self._rotation_interval = seconds
            # Reset current assignment so the next call picks fresh
            self._current_proxy = None
            self._current_proxy_since = 0.0
            if seconds:
                logger.debug(
                    "[🔀] Proxy rotation interval set to %d seconds", seconds
                )
            else:
                logger.debug("[🔀] Timed rotation disabled (sticky mode)")

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
        """
        (Re)load proxy list.  If *proxy_lines* is ``None``, read from disk.
        Returns the number of valid proxies loaded.
        """
        if proxy_lines is None:
            proxy_lines = self._read_file()

        normalised = []
        for line in proxy_lines:
            url = _normalise_proxy_url(line)
            if url:
                normalised.append(url)

        self._proxies = normalised

        self._health = {k: v for k, v in self._health.items() if k in normalised}

        # Reset time-based assignment when proxy list changes
        self._current_proxy = None
        self._current_proxy_since = 0.0

        logger.debug("[🔀] Loaded %d client proxies", len(normalised))
        return len(normalised)

    def next(self, *, exclude: Optional[set] = None) -> Optional[str]:
        """Return the next healthy proxy URL, or *None*.

        Default behaviour (``rotation_interval == 0``): pick one proxy and
        stick with it indefinitely — only switch when it is suspended.

        When ``rotation_interval > 0``: use one proxy for the configured
        duration, then advance to the next healthy proxy.

        Parameters
        ----------
        exclude:
            Set of proxy URLs to skip (e.g. already tried this request).
            When provided, a temporary fallback is returned *without*
            changing the sticky ``_current_proxy`` assignment.
        """
        if not self._proxies:
            return None

        exclude = exclude or set()
        now = time.monotonic()

        # Check if the current proxy is still usable
        if (
            self._current_proxy
            and self._current_proxy not in exclude
            and not self._is_suspended(self._current_proxy, now)
        ):
            # With timed rotation, check if the interval has expired
            if self._rotation_interval > 0:
                if (now - self._current_proxy_since) < self._rotation_interval:
                    return self._current_proxy
                # Interval expired — fall through to assign a new one
            else:
                # No rotation — stick with this proxy forever
                return self._current_proxy

        # ── Retry / fallback (exclude is non-empty) ──
        # Return a temporary alternative without changing the sticky proxy.
        if exclude:
            return self._next_healthy(exclude, now)

        # ── Genuine rotation or first assignment ──
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
        """Force an immediate rotation to the next healthy proxy.

        Returns the new proxy URL, or *None* if no healthy proxy is
        available.  Used by the background rotation timer.
        """
        if not self._proxies or not self._enabled:
            return None

        now = time.monotonic()
        exclude = {self._current_proxy} if self._current_proxy else set()
        proxy = self._next_healthy(exclude, now)

        # If all other proxies are dead, stick with the current one
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
        """Background task that handles timed proxy rotation.

        Runs forever; safe to wrap in ``asyncio.create_task``.
        Health monitoring is handled reactively via ``on_disconnect``
        / ``on_resumed`` events in the client — no polling needed.
        """
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
        """Pick a random healthy proxy that isn't excluded or suspended."""
        candidates = [
            url for url in self._proxies
            if url not in exclude and not self._is_suspended(url, now)
        ]
        if not candidates:
            return None
        return random.choice(candidates)

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

        # If already suspended, don't pile on — avoids duplicate log spam

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

            # If the suspended proxy was the current time-based proxy, force rotation
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
    """Mask credentials in a proxy URL for safe logging."""

    try:
        if "@" in url:
            scheme_rest = url.split("://", 1)
            if len(scheme_rest) == 2:
                creds_host = scheme_rest[1].split("@", 1)
                if len(creds_host) == 2:
                    return f"{scheme_rest[0]}://***@{creds_host[1]}"
    except Exception:
        pass
    return url[:40] + "…" if len(url) > 40 else url
