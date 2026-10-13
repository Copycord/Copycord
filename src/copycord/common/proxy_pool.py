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
import hashlib
import logging
import random
import re
import time
from typing import Optional

from copycord.common.paths import data_dir

logger = logging.getLogger("common.proxy_pool")

DATA_DIR = data_dir()
_PROXY_FILE = DATA_DIR / "proxies.txt"

MAX_FAILURES = 3
DEFAULT_SUSPEND_SECONDS = 300

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


def _proxy_label(url: str) -> str:
    """Masked URL plus a short hash id, since providers often differ only in the credentials."""
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:8]
    return f"{_mask_proxy_url(url)} id={digest}"


class ProxyPool:
    """Exclusive per-token proxy lease pool.

    A token keeps its proxy until it fails, and the last proxy used is preferred
    when the token returns, but never waited for if another token holds it.
    """

    MAX_FAILURES = MAX_FAILURES

    def __init__(self) -> None:
        self._all: list[str] = []
        self._leases: dict[str, str] = {}
        # survives release() so a returning token can get the same address
        self._preferred: dict[str, str] = {}
        self._in_use: set[str] = set()
        self._health: dict[str, dict] = {}
        self._lock = asyncio.Lock()
        # instance-level so the server can point it at PROXY_SUSPEND_DURATION
        self.SUSPEND_SECONDS = DEFAULT_SUSPEND_SECONDS

    @property
    def enabled(self) -> bool:
        return bool(self._all)

    @property
    def total(self) -> int:
        return len(self._all)

    @property
    def leased_count(self) -> int:
        return len(self._in_use)

    def reload(self, proxy_lines: Optional[list[str]] = None) -> int:
        """(Re)load the proxy list; reads DATA_DIR/proxies.txt when proxy_lines is None. Returns the count."""
        if proxy_lines is None:
            proxy_lines = self._read_file()

        normalised = []
        for line in proxy_lines:
            url = _normalise_proxy_url(line)
            if url:
                normalised.append(url)

        self._all = normalised
        self._health = {k: v for k, v in self._health.items() if k in normalised}
        # drop preferences for proxies no longer in the file
        self._preferred = {
            t: p for t, p in self._preferred.items() if p in normalised
        }
        logger.info("Loaded %d proxies", len(normalised))
        return len(normalised)

    @staticmethod
    def _read_file() -> list[str]:
        if not _PROXY_FILE.exists():
            return []
        try:
            text = _PROXY_FILE.read_text(encoding="utf-8").strip()
            return [l.strip() for l in text.splitlines() if l.strip()]
        except Exception as e:
            logger.warning("Failed to read proxy file: %s", e)
            return []

    def _is_suspended(self, proxy: str, now: float) -> bool:
        info = self._health.get(proxy)
        if not info:
            return False
        until = info.get("suspended_until", 0)
        if until and now < until:
            return True
        if until and now >= until:
            info["suspended_until"] = 0
            info["failures"] = 0
        return False

    async def lease(self, token: str, *, exclude: Optional[str] = None) -> Optional[str]:
        """Return the proxy leased to ``token``, assigning a free healthy one if needed. ``exclude`` skips one proxy."""
        async with self._lock:
            existing = self._leases.get(token)
            if existing:
                return existing
            if not self._all:
                return None

            now = time.monotonic()

            def _free(proxy: str) -> bool:
                return (
                    proxy in self._all
                    and proxy != exclude
                    and proxy not in self._in_use
                    and not self._is_suspended(proxy, now)
                )

            # falling back instead of waiting: blocking would cap concurrency at the pool size
            remembered = self._preferred.get(token)
            if remembered and _free(remembered):
                self._leases[token] = remembered
                self._in_use.add(remembered)
                logger.debug(
                    "Proxy re-leased (sticky): proxy=%s leased=%d/%d",
                    _proxy_label(remembered),
                    len(self._in_use),
                    len(self._all),
                )
                return remembered

            candidates = [p for p in self._all if _free(p)]
            if not candidates:
                logger.warning(
                    "Proxy pool exhausted: %d total, %d leased, none free/healthy",
                    len(self._all),
                    len(self._in_use),
                )
                return None

            chosen = random.choice(candidates)
            self._leases[token] = chosen
            self._in_use.add(chosen)
            # remember what was granted, so a token that missed its old proxy settles on the new one
            self._preferred[token] = chosen
            logger.debug(
                "Proxy leased: proxy=%s leased=%d/%d",
                _proxy_label(chosen),
                len(self._in_use),
                len(self._all),
            )
            return chosen

    async def release(self, token: str) -> None:
        """Return a token's leased proxy to the free pool; the preference is kept."""
        async with self._lock:
            proxy = self._leases.pop(token, None)
            if proxy:
                self._in_use.discard(proxy)
                logger.debug(
                    "Proxy released: proxy=%s leased=%d/%d",
                    _proxy_label(proxy),
                    len(self._in_use),
                    len(self._all),
                )

    async def report_failure(self, token: str) -> Optional[str]:
        """Release a proxy that failed a real connection, suspend it after repeated failures, and lease a replacement."""
        async with self._lock:
            proxy = self._leases.pop(token, None)
            if proxy:
                self._in_use.discard(proxy)
                info = self._health.setdefault(
                    proxy, {"failures": 0, "suspended_until": 0}
                )
                if info.get("suspended_until", 0) <= time.monotonic():
                    info["failures"] += 1
                    if info["failures"] >= self.MAX_FAILURES:
                        info["suspended_until"] = time.monotonic() + self.SUSPEND_SECONDS
                        logger.warning(
                            "Proxy suspended for %ds after %d consecutive failure(s): proxy=%s",
                            self.SUSPEND_SECONDS,
                            info["failures"],
                            _proxy_label(proxy),
                        )
                # suspension only starts after several failures; without this the sticky path returns it
                self._preferred.pop(token, None)
        return await self.lease(token, exclude=proxy)

    def report_success(self, proxy: str) -> None:
        """Clear a proxy's failure count after a successful request."""
        info = self._health.get(proxy)
        if info and (info.get("failures") or info.get("suspended_until")):
            info["failures"] = 0
            info["suspended_until"] = 0

    def current(self, token: str) -> Optional[str]:
        """Read-only lookup of a token's currently leased proxy, if any."""
        return self._leases.get(token)


_POOL = ProxyPool()


def get_pool() -> ProxyPool:
    """The shared, process-wide proxy pool."""
    return _POOL
