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
import hashlib
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
_PROXY_FILE = DATA_DIR / "proxies.txt"

MAX_FAILURES = 3
DEFAULT_SUSPEND_SECONDS = 300

_HP_UP = re.compile(r"^(?P<host>[^:]+):(?P<port>\d+):(?P<user>[^:]+):(?P<pass>.+)$")
_UP_HP = re.compile(r"^(?P<user>[^:@]+):(?P<pass>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)$")


def _normalise_proxy_url(raw: str) -> Optional[str]:
    """Accept many common proxy formats and normalise to
    ``scheme://[user:pass@]host:port``. Returns *None* for unparsable lines.

    Mirrors ``client.proxy_rotator._normalise_proxy_url`` exactly (same
    accepted formats, same output shape) since both read the same
    ``DATA_DIR/proxies.txt`` file, just with different consumption models —
    this module leases a proxy per token exclusively, the client rotator
    picks one proxy for its single account.
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
    """Masked URL plus a short stable id.

    Residential providers commonly put the rotating session identifier in
    the *credentials* and point every entry at one host:port, so masking
    alone renders every proxy in the pool identical in the logs. The id is
    a hash of the full URL, so distinct entries stay distinguishable
    without ever printing the credentials.
    """
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:8]
    return f"{_mask_proxy_url(url)} id={digest}"


class ProxyPool:
    """Exclusive per-token proxy lease pool.

    Unlike ``client.proxy_rotator.ProxyRotator`` (one active caller, picks
    one proxy at a time, rotates on a timer or on failure), this pool serves
    many tokens simultaneously with a hard exclusivity guarantee: no two
    tokens are ever leased the same proxy at the same time. A token's lease
    is assigned once (randomly, from whatever's free and healthy) and stays
    sticky — it is NEVER swapped just because time passed, only because the
    proxy actually failed. Releasing a lease (token's session closed/reaped)
    returns that proxy to the free pool for another token to pick up.

    Stickiness outlives the lease: the proxy a token last used is remembered
    and preferred when it comes back, so an account keeps the same address
    across sessions instead of re-rolling every time its TLS session is
    reaped. The memory is a preference, not a reservation — a remembered
    proxy that another token holds is not waited for, which is what keeps a
    small pool serving far more tokens than it has entries. Bounded by the
    number of distinct tokens the process has seen.
    """

    MAX_FAILURES = MAX_FAILURES

    def __init__(self) -> None:
        self._all: list[str] = []
        self._leases: dict[str, str] = {}
        # token -> the proxy it last held. Survives release(), so a
        # returning token can be given the same address again.
        self._preferred: dict[str, str] = {}
        self._in_use: set[str] = set()
        self._health: dict[str, dict] = {}
        self._lock = asyncio.Lock()
        # Instance-level so the server can point it at the existing
        # PROXY_SUSPEND_DURATION admin setting, the same one the client's
        # ProxyRotator already honours.
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
        """(Re)load the proxy list. If ``proxy_lines`` is ``None``, reads
        from ``DATA_DIR/proxies.txt``. Returns the number loaded.

        Existing leases for proxies that remain in the new list are left
        untouched; leases for proxies dropped from the list are NOT
        force-revoked here (a token mid-session keeps working until its
        session naturally closes) but won't be handed out to anyone else.
        """
        if proxy_lines is None:
            proxy_lines = self._read_file()

        normalised = []
        for line in proxy_lines:
            url = _normalise_proxy_url(line)
            if url:
                normalised.append(url)

        self._all = normalised
        self._health = {k: v for k, v in self._health.items() if k in normalised}
        # A preference for a proxy that is no longer in the file is dead
        # weight, and would otherwise never be cleared.
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
        """Return the proxy leased to ``token``, assigning a fresh random
        healthy-and-free one if it doesn't already have one. ``None`` if the
        pool is disabled/empty, or exhausted (every proxy is either leased
        to another token or currently suspended).

        ``exclude`` skips a specific proxy, used when replacing one that just
        failed so the sticky preference cannot hand the same one straight back.
        """
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

            # Same address as last time, when it is still available. Falling
            # back rather than waiting is deliberate: blocking on a held proxy
            # would stall the send and cap concurrency at the pool size.
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
            # Remember what was actually granted, not what was wanted: a token
            # that could not get its old proxy should settle on the new one
            # rather than drift every time it reconnects.
            self._preferred[token] = chosen
            logger.debug(
                "Proxy leased: proxy=%s leased=%d/%d",
                _proxy_label(chosen),
                len(self._in_use),
                len(self._all),
            )
            return chosen

    async def release(self, token: str) -> None:
        """Return a token's leased proxy to the free pool (e.g. its TLS
        session was closed/reaped).

        The preference is kept, so the same proxy is handed back when this
        token returns and nothing else has taken it.
        """
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
        """The token's current proxy failed a real connection (not just an
        HTTP error status). Release it, suspend it if it's failed
        repeatedly, and lease a replacement. Returns the new proxy, if any.
        """
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
                # Drop the preference too: this proxy just failed, and a
                # suspension only kicks in after several failures, so without
                # this the sticky path would hand it straight back.
                self._preferred.pop(token, None)
        return await self.lease(token, exclude=proxy)

    def report_success(self, proxy: str) -> None:
        """Clear a proxy's failure count after a genuinely successful
        request through it."""
        info = self._health.get(proxy)
        if info and (info.get("failures") or info.get("suspended_until")):
            info["failures"] = 0
            info["suspended_until"] = 0

    def current(self, token: str) -> Optional[str]:
        """Read-only lookup of a token's currently leased proxy, if any."""
        return self._leases.get(token)


_POOL = ProxyPool()


def get_pool() -> ProxyPool:
    """The single process-wide proxy pool."""
    return _POOL
