# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations

import itertools
import logging
import os
import re
import asyncio
import time
from pathlib import Path
from typing import List, Optional, Dict

import aiohttp

try:
    from aiohttp_socks import ProxyConnector
except ImportError:
    ProxyConnector = None

logger = logging.getLogger("server.proxy_rotator")

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
    Thread-safe, round-robin proxy rotator with health tracking.

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
        self._cycle = itertools.cycle([])
        self._lock = asyncio.Lock()
        self._enabled: bool = False
        self._index: int = 0

        self._health: Dict[str, Dict] = {}

        self.on_all_dead: Optional[callable] = None

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

    def set_enabled(self, on: bool) -> None:
        prev = self._enabled
        self._enabled = bool(on)

        if on == prev:
            return
        if on and self._proxies:
            logger.debug(
                "[🔀] Server proxy rotation ENABLED (%d proxies)", len(self._proxies)
            )
        elif on:
            logger.warning("[⚠️] Server proxy rotation enabled but NO proxies loaded")
        else:
            logger.debug("[🔀] Server proxy rotation DISABLED")

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
        self._cycle = itertools.cycle(normalised) if normalised else itertools.cycle([])
        self._index = 0

        self._health = {k: v for k, v in self._health.items() if k in normalised}

        logger.debug("[🔀] Loaded %d server proxies", len(normalised))
        return len(normalised)

    def next(self, *, exclude: Optional[set] = None) -> Optional[str]:
        """Return the next healthy proxy URL (round-robin), or *None*.

        Parameters
        ----------
        exclude:
            Set of proxy URLs to skip (e.g. already tried this request).
        """
        if not self._proxies:
            return None

        exclude = exclude or set()
        now = time.monotonic()

        for _ in range(len(self._proxies)):
            try:
                url = next(self._cycle)
                self._index = (self._index + 1) % len(self._proxies)
            except StopIteration:
                return None

            if url in exclude:
                continue
            if not self._is_suspended(url, now):
                return url

        return None

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

            if self._enabled and self.healthy_count == 0:
                self._enabled = False
                logger.warning(
                    "[🔀] All %d proxies dead — proxy rotation auto-disabled, "
                    "falling back to direct for remainder of sync",
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


def _is_socks(url: str) -> bool:
    return url.lower().startswith(
        ("socks4://", "socks5://", "socks4a://", "socks5h://")
    )


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


_PROXY_ERRORS = (
    aiohttp.ClientProxyConnectionError,
    aiohttp.ClientHttpProxyError,
    aiohttp.ClientConnectorError,
    aiohttp.ClientOSError,
    ConnectionRefusedError,
    ConnectionResetError,
    OSError,
)


def _make_connector_for_proxy(proxy_url: str) -> Optional[aiohttp.BaseConnector]:
    """
    Build a connector suitable for *proxy_url*.
    - SOCKS proxies require ``aiohttp_socks.ProxyConnector``.
    - HTTP proxies just use the ``proxy=`` parameter on each request.
    """
    if _is_socks(proxy_url):
        if ProxyConnector is None:
            logger.error(
                "[⛔] aiohttp_socks is required for SOCKS proxies but not installed"
            )
            return None
        return ProxyConnector.from_url(proxy_url)
    return None  # HTTP proxy – use aiohttp's native proxy= kwarg


_MAX_PROXY_RETRIES = 3


def patch_discord_http(bot, rotator: ProxyRotator) -> None:
    """
    Monkey-patch the py-cord ``HTTPClient.request`` so every outgoing call
    goes through the next proxy in the rotation (when enabled).

    If a proxy connection fails, automatically retries with the next proxy
    up to ``_MAX_PROXY_RETRIES`` times.  If all proxied attempts fail,
    falls back to a direct (no-proxy) request.

    Safe to call multiple times – only patches once.
    """
    http_client = bot.http

    # Idempotency guard: don't double-patch on reconnects
    if getattr(http_client, "_proxy_patched", False):
        logger.debug("[🔀] Discord HTTP client already patched, skipping")
        return

    original_request = http_client.request

    async def _do_socks_request(proxy_url, route, **kwargs):
        """Execute a single request through a SOCKS proxy."""
        connector = ProxyConnector.from_url(proxy_url)
        old_session = http_client._HTTPClient__session
        if old_session and not old_session.closed:
            new_session = aiohttp.ClientSession(
                connector=connector,
                headers=old_session._default_headers,
            )
            http_client._HTTPClient__session = new_session
            try:
                return await original_request(route, **kwargs)
            finally:
                http_client._HTTPClient__session = old_session
                await new_session.close()

        return await original_request(route, **kwargs)

    async def _proxy_request(route, **kwargs):
        if not rotator.enabled:
            return await original_request(route, **kwargs)

        tried: set = set()
        last_exc = None
        max_attempts = min(_MAX_PROXY_RETRIES, rotator.count)

        for attempt in range(max_attempts):
            proxy_url = rotator.next(exclude=tried)
            if not proxy_url:

                break

            tried.add(proxy_url)

            try:
                if _is_socks(proxy_url):
                    if ProxyConnector is not None:
                        result = await _do_socks_request(proxy_url, route, **kwargs)
                        rotator.report_success(proxy_url)
                        return result
                else:
                    kwargs["proxy"] = proxy_url
                    result = await original_request(route, **kwargs)
                    rotator.report_success(proxy_url)
                    return result

            except _PROXY_ERRORS as exc:
                last_exc = exc
                safe = _mask_proxy_url(proxy_url)
                logger.debug(
                    "[🔀] Proxy %s failed (attempt %d/%d): %s",
                    safe,
                    attempt + 1,
                    max_attempts,
                    type(exc).__name__,
                )
                rotator.report_failure(proxy_url)

                if not rotator.enabled:
                    break

                kwargs.pop("proxy", None)
                continue

        if last_exc is not None and rotator._enabled:
            logger.debug(
                "[🔀] %d proxy attempt(s) failed, falling back to direct connection",
                len(tried),
            )
        kwargs.pop("proxy", None)

        return await original_request(route, **kwargs)

    http_client.request = _proxy_request
    http_client._proxy_patched = True
    logger.debug("[🔀] Patched discord HTTP client for proxy rotation")
