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
import contextlib
import heapq
import itertools
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlsplit

from copycord.common.selfbot_headers import (
    TLS_IMPERSONATE,
    _mask_proxy_for_log,
    build_headers,
)
from copycord.selfclient.gateway import (
    GatewayClient,
    ZlibStreamInflater,
    gateway_url,
    open_websocket,
)

try:
    from curl_cffi.requests import AsyncSession as _CurlAsyncSession
except ImportError:
    _CurlAsyncSession = None

logger = logging.getLogger("standalone_scraper")

# Tries for a profile whose fetch fails (network, proxy, timeout, a 5xx or another status)
# before the member is saved without a bio. A 429 is not a failure: it waits out retry_after
# and tries again, however often Discord asks.
BIO_ATTEMPTS = 3
# Profiles Discord refuses outright: no retry, the member is saved without a bio.
BIO_REFUSED = (401, 403, 404)


@dataclass
class ScraperConfig:
    """Configuration for the standalone scraper."""

    guild_id: int
    tokens: List[str]
    proxies: List[str] = field(default_factory=list)
    include_username: bool = True
    include_avatar_url: bool = True
    include_bio: bool = False
    include_roles: bool = False
    sessions_per_token: int = 1  # one gateway session per user token keeps the account safe
    max_parallel_per_session: int = 5
    progress_callback: Optional[Callable] = None
    log_callback: Optional[Callable] = None
    # bio_progress_callback(done, total, message): bios fetched or skipped of bios queued.
    bio_progress_callback: Optional[Callable] = None


@dataclass
class ScraperResult:
    """Result from scraping operation."""

    members: List[Dict] = field(default_factory=list)
    total_count: int = 0
    success: bool = False
    error: Optional[str] = None
    elapsed_seconds: float = 0
    metadata: Dict = field(default_factory=dict)


@dataclass(order=True)
class _PQItem:
    priority: float
    prefix: str = field(compare=False)


class QueryPlanner:
    """Planner for search prefixes using unigram and bigram models learned from observed usernames."""

    def __init__(
        self,
        *,
        alphabet: str,
        limit: int = 100,
        max_repeat_run: int = 4,
        allow_char: Optional[Callable[[str], bool]] = None,
    ) -> None:
        self.limit = limit
        self.alphabet_base = list(dict.fromkeys(alphabet))
        self.max_repeat_run = max_repeat_run
        self.allow_char = allow_char

        self.visited: set[str] = set()
        self.dead: set[str] = set()
        self.leaves: set[str] = set()
        self.stats: dict[str, int] = {}
        self.char_freq: dict[str, int] = {}
        self.bi_freq: dict[tuple[str, str], int] = {}

        for c in self.alphabet_base:
            self.char_freq[c] = 1

        for ch, boost in zip(
            "aeiours tnlcmdupfgwybvkxjqz".replace(" ", ""),
            [
                50,
                45,
                42,
                40,
                38,
                36,
                34,
                30,
                28,
                26,
                24,
                22,
                20,
                18,
                16,
                14,
                12,
                10,
                9,
                8,
                7,
                6,
                5,
                4,
                3,
                2,
            ],
        ):
            if ch in self.char_freq:
                self.char_freq[ch] += boost

        self._pq: list[_PQItem] = []
        self._in_queue: set[str] = set()
        self._expansion_k_used: dict[str, int] = {}
        self._saw_digit_lead: bool = False
        self._session_slots: int = 1

    @staticmethod
    def _tail_run_len(s: str) -> int:
        if not s:
            return 0
        last = s[-1]
        n = 0
        for i in range(len(s) - 1, -1, -1):
            if s[i] == last:
                n += 1
            else:
                break
        return n

    def set_session_slots(self, n: int) -> None:
        self._session_slots = max(1, min(20, int(n or 1)))

    def note_digit_lead(self) -> None:
        self._saw_digit_lead = True

    def _sorted_alphabet(self) -> list[str]:
        base = self.alphabet_base
        return sorted(base, key=lambda c: (-self.char_freq.get(c, 1), base.index(c)))

    def _score_prefix(self, prefix: str) -> float:
        s = self.stats.get(prefix, -1)
        L = len(prefix)
        if L == 2 and s < 0:
            base = 2.10
            lead = prefix[0]
            if lead.isalpha():
                bias = 0.20 if self._session_slots <= 2 else 0.0
            elif lead.isdigit():
                bias = (
                    -0.10
                    if self._session_slots <= 2 and not self._saw_digit_lead
                    else 0.0
                )
            else:
                bias = -0.35 if self._session_slots <= 3 else -0.15
            return base + bias
        if s < 0:
            base = 0.50
        elif s == 0:
            base = 0.10
        elif s < self.limit:
            base = 0.90
        else:
            base = 1.40
        length_bonus = 0.10 * (1.0 / (1 + L))
        depth_penalty = 0.05 * max(0, L - 2)
        return base + length_bonus - depth_penalty

    def _push_internal(self, prefix: str) -> None:
        if prefix in self.visited or prefix in self._in_queue or prefix in self.dead:
            return
        if self._tail_run_len(prefix) > self.max_repeat_run:
            return
        heapq.heappush(
            self._pq, _PQItem(priority=-self._score_prefix(prefix), prefix=prefix)
        )
        self._in_queue.add(prefix)

    def seed_top_level(self, top_level: str) -> None:
        for ch in top_level:
            self._push_internal(ch)

    def add_dynamic_lead(self, lead: str) -> None:
        lead = (lead or "").casefold()
        if not lead:
            return
        self._push_internal(lead)

    def mark_observed_username(self, username: str) -> None:
        if not username:
            return
        u = username.casefold()
        for ch in u[:3]:
            if ch not in self.char_freq:
                self.char_freq[ch] = 1
            self.char_freq[ch] += 1
        for i in range(len(u) - 1):
            a, b = u[i], u[i + 1]
            self.bi_freq[(a, b)] = self.bi_freq.get((a, b), 0) + 1
        if u and u[0].isdigit():
            self._saw_digit_lead = True

    def mark_observed_usernames_bulk(self, usernames: list[str]) -> None:
        """Batch-update frequency tables; the caller holds the lock."""
        for username in usernames:
            if not username:
                continue
            u = username.casefold()
            for ch in u[:3]:
                if ch not in self.char_freq:
                    self.char_freq[ch] = 1
                self.char_freq[ch] += 1
            for i in range(len(u) - 1):
                a, b = u[i], u[i + 1]
                self.bi_freq[(a, b)] = self.bi_freq.get((a, b), 0) + 1
            if u and u[0].isdigit():
                self._saw_digit_lead = True

    def on_chunk_result(self, prefix: str, size: int) -> None:
        self.stats[prefix] = size
        self.visited.add(prefix)
        self._in_queue.discard(prefix)
        if size == 0:
            self.dead.add(prefix)
        elif size < self.limit:
            self.leaves.add(prefix)

    def _class_gate(self, prefix: str, ch: str) -> bool:
        L = len(prefix)
        if self.allow_char and not self.allow_char(ch):
            return False
        is_letter = ch.isalpha()
        is_digit = ch.isdigit()
        is_punct = ch in "._-"
        if L < 3 and not (is_letter or is_digit):
            return False
        if L >= 1 and prefix[-1] in "._-" and ch in "._-":
            return False
        if L >= 1 and prefix[-1] in "_-":
            if is_letter:
                return True
            return self.bi_freq.get((prefix[-1], ch), 0) >= 4
        if is_digit:
            if L < 4:
                parent_sat = self.stats.get(prefix, 0) >= self.limit
                prev = prefix[-1] if L else None
                has_bigram = prev is not None and self.bi_freq.get((prev, ch), 0) >= 1
                if not (parent_sat or self._saw_digit_lead or has_bigram):
                    return False
        if is_punct:
            if L < 4:
                return False
            prev = prefix[-1] if L else None
            if not prev or self.bi_freq.get((prev, ch), 0) < 4:
                return False
        return True

    def _score_next_char(self, prev: Optional[str], ch: str) -> float:
        uni = self.char_freq.get(ch, 1)
        uni_norm = uni / (sum(self.char_freq.values()) or 1)
        if not prev:
            return 0.2 * uni_norm
        num = self.bi_freq.get((prev, ch), 0) + 1
        denom = sum(self.bi_freq.get((prev, x), 0) + 1 for x in self.char_freq.keys())
        p_bigram = num / (denom or 1)
        return 0.8 * p_bigram + 0.2 * uni_norm

    def children_for(
        self, prefix: str, top_k: Optional[int] = 12, *, ignore_gate: bool = False
    ) -> list[str]:
        if self.stats.get(prefix, 0) < self.limit:
            return []
        prev = prefix[-1] if prefix else None
        chars = self._sorted_alphabet()
        ranked: list[tuple[float, str]] = []
        for ch in chars:
            if not ignore_gate and not self._class_gate(prefix, ch):
                continue
            ranked.append((self._score_next_char(prev, ch), ch))
        ranked.sort(reverse=True, key=lambda t: t[0])
        if top_k is None:
            top_k = 12
        return [prefix + c for _, c in ranked[:top_k]]

    def enqueue_children(
        self, prefix: str, top_k: Optional[int] = 12, *, ignore_gate: bool = False
    ) -> int:
        cnt = 0
        for child in self.children_for(prefix, top_k=top_k, ignore_gate=ignore_gate):
            self._push_internal(child)
            cnt += 1
        self._expansion_k_used[prefix] = max(
            self._expansion_k_used.get(prefix, 0), top_k or 0
        )
        return cnt

    def ensure_children(
        self,
        prefix: str,
        *,
        step: int = 6,
        force_full: bool = False,
        ignore_gate: bool = False,
    ) -> int:
        if force_full:
            target = len(self.alphabet_base)
        else:
            current = self._expansion_k_used.get(prefix, 0)
            target = min(len(self.alphabet_base), max(6, current + step))
        return self.enqueue_children(prefix, top_k=target, ignore_gate=ignore_gate)

    def next_batch(self, k: int) -> list[str]:
        if k <= 0:
            return []
        out = []
        while self._pq and len(out) < k:
            it = heapq.heappop(self._pq)
            p = it.prefix
            self._in_queue.discard(p)
            if p in self.visited or p in self.dead:
                continue
            out.append(p)
        return out

    def has_work(self) -> bool:

        while self._pq:
            top = self._pq[0]
            if top.prefix in self.visited or top.prefix in self.dead:
                heapq.heappop(self._pq)
                self._in_queue.discard(top.prefix)
                continue
            return True
        return False

    def queue_len(self) -> int:
        return len(self._pq)

    def all_leaves_exhausted(self, *, ignore_gate: bool = True) -> bool:
        if self.has_work():
            return False
        for p, s in self.stats.items():
            if s < self.limit:
                continue
            for ch in self._sorted_alphabet():
                if not ignore_gate and not self._class_gate(p, ch):
                    continue
                c = p + ch
                if (
                    c not in self.visited
                    and c not in self.dead
                    and c not in self._in_queue
                ):
                    return False
        return True

    def requeue(self, prefix: str) -> None:
        self._push_internal(prefix)

    def push(self, prefix: str) -> None:
        self._push_internal(prefix)

    def two_gram_roots(self) -> list[str]:
        return [a + b for a in self.alphabet_base for b in self.alphabet_base]


class SharedPlanner:
    """Async-safe wrapper so all sessions share one queue and frontier."""

    def __init__(self, planner: QueryPlanner):
        self.p = planner
        self._lock = asyncio.Lock()
        self._roots_seeded = False
        self._seeded_roots_set: Optional[set[str]] = None

    async def seed_two_gram_roots_once(self, roots: Optional[List[str]] = None):
        if self._roots_seeded:
            return
        async with self._lock:
            if self._roots_seeded:
                return
            if roots is None:
                roots = self.p.two_gram_roots()
            for r in roots:
                self.p.push(r)
            self._seeded_roots_set = set(roots)
            self._roots_seeded = True

    async def seed_top_level(self, s: str):
        async with self._lock:
            self.p.seed_top_level(s)

    async def add_dynamic_lead(self, lead: str):
        async with self._lock:
            self.p.add_dynamic_lead(lead)
            for ch in self.p._sorted_alphabet()[:8]:
                self.p.push(lead + ch)

    async def mark_observed_username(self, name: str):
        async with self._lock:
            self.p.mark_observed_username(name)

    async def mark_observed_usernames_bulk(self, usernames: list[str]):
        """Batch-update frequency tables under a single lock acquisition."""
        async with self._lock:
            self.p.mark_observed_usernames_bulk(usernames)

    async def note_digit_lead(self):
        async with self._lock:
            self.p.note_digit_lead()

    async def on_chunk_result(self, prefix: str, size: int):
        async with self._lock:
            self.p.on_chunk_result(prefix, size)

    async def on_chunk_result_and_expand(
        self, prefix: str, size: int, *, expand_step: int = 10
    ) -> int:
        """Record a chunk result and expand children if saturated, under a single lock."""
        async with self._lock:
            self.p.on_chunk_result(prefix, size)
            if size >= self.p.limit:
                return self.p.ensure_children(prefix, step=expand_step)
            return 0

    async def next_batch(self, k: int) -> list[str]:
        async with self._lock:
            return self.p.next_batch(k)

    async def ensure_children(
        self,
        prefix: str,
        *,
        step: int = 6,
        force_full: bool = False,
        ignore_gate: bool = False,
    ) -> int:
        async with self._lock:
            return self.p.ensure_children(
                prefix, step=step, force_full=force_full, ignore_gate=ignore_gate
            )

    async def requeue(self, prefix: str):
        async with self._lock:
            self.p.requeue(prefix)

    async def push(self, prefix: str):
        async with self._lock:
            self.p.push(prefix)

    async def has_work(self) -> bool:
        async with self._lock:
            return self.p.has_work()

    async def all_leaves_exhausted(self) -> bool:
        async with self._lock:
            return self.p.all_leaves_exhausted(ignore_gate=True)

    async def queue_len(self) -> int:
        async with self._lock:
            return self.p.queue_len()

    async def sweep_full_children_for_saturated(self) -> int:
        async with self._lock:
            added = 0
            for p, s in list(self.p.stats.items()):
                if s >= self.p.limit:
                    added += self.p.ensure_children(
                        p, force_full=True, ignore_gate=True
                    )
            return added

    async def missing_roots(self) -> list[str]:
        async with self._lock:
            if not self._roots_seeded:
                return []
            assert self._seeded_roots_set is not None
            return [r for r in self._seeded_roots_set if r not in self.p.stats]

    async def refill_if_starving(self, *, threshold: int = 128, step: int = 12) -> int:
        async with self._lock:
            if len(self.p._pq) >= threshold:
                return 0
            added = 0
            for p, s in sorted(self.p.stats.items(), key=lambda kv: -kv[1]):
                if s >= self.p.limit and self.p._expansion_k_used.get(p, 0) < len(
                    self.p.alphabet_base
                ):
                    added += self.p.ensure_children(p, step=step, force_full=False)
                    if len(self.p._pq) >= threshold:
                        break
            return added

    async def snapshot_metrics(self) -> dict:
        async with self._lock:
            return {
                "pq_len": len(self.p._pq),
                "visited": len(self.p.visited),
                "leaves": len(self.p.leaves),
                "saturated": sum(1 for s in self.p.stats.values() if s >= self.p.limit),
            }

    async def set_session_slots(self, n: int):
        async with self._lock:
            self.p.set_session_slots(n)


class StandaloneScraper:
    """Member scraper that uses multiple tokens over the Discord gateway."""

    DISCORD_API = "https://discord.com/api/v9"

    def __init__(self, config: ScraperConfig):
        self.config = config
        self._members: Dict[str, Dict] = {}
        self._members_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._running = False
        self._clock: Callable[[], float] = time.monotonic
        self._bios = {"queued": 0, "fetched": 0, "skipped": 0}
        # Set while scrape() waits on the bios; the worker reports each bio then.
        self._bio_phase = False
        # Gateway sessions that reached READY.
        self._sessions_ready = 0

        self._proxy_list = [
            self._normalize_proxy(p) for p in (config.proxies or []) if p
        ]
        self._proxy_idx = 0
        # (token, session slot) -> its proxy, and the scraper's own curl_cffi session for
        # it. The slot's gateway socket goes through the same proxy.
        self._slot_proxies: Dict[tuple, Optional[str]] = {}
        self._http_sessions: Dict[tuple, Any] = {}
        self._bio_failures: Dict[str, int] = {}

    @staticmethod
    def _normalize_proxy(raw: str) -> str:
        """Accept common proxy formats and return a URL."""
        raw = raw.strip()
        if not raw:
            return raw

        if "://" in raw:
            scheme, rest = raw.split("://", 1)

            if "@" in rest:
                return raw
            parts = rest.split(":")
            if len(parts) == 4:

                host, port, user, passwd = parts
                return f"{scheme}://{user}:{passwd}@{host}:{port}"

            return raw

        if "@" in raw:
            return f"http://{raw}"

        parts = raw.split(":")
        if len(parts) == 2:

            return f"http://{parts[0]}:{parts[1]}"
        if len(parts) == 4:

            host, port, user, passwd = parts
            return f"http://{user}:{passwd}@{host}:{port}"

        return f"http://{raw}"

    def _next_proxy(self) -> Optional[str]:
        """Return next proxy URL in round-robin order, or None if no proxies."""
        if not self._proxy_list:
            return None
        proxy = self._proxy_list[self._proxy_idx % len(self._proxy_list)]
        self._proxy_idx += 1
        return proxy

    def _plan_proxies(self, tokens: List[str]):
        """Give every (token, session slot) its proxy, round-robin in token order."""
        self._slot_proxies = {}
        self._proxy_idx = 0
        for token in tokens:
            for slot in range(max(1, self.config.sessions_per_token)):
                self._slot_proxies[(token, slot)] = self._next_proxy()

    def _proxy_for(self, token: str, slot: int = 0) -> Optional[str]:
        return self._slot_proxies.get((token, slot))

    def _http(self, token: str, slot: int = 0):
        """The scraper's curl_cffi session for this token and slot, made on first use with
        the Chrome TLS fingerprint and the slot's proxy. Not get_tls_session's cache: a
        scrape never rebinds the session or proxy lease the client or sender holds."""
        key = (token, slot)
        sess = self._http_sessions.get(key)
        if sess is None:
            if _CurlAsyncSession is None:
                raise RuntimeError("curl_cffi is not installed")
            sess = _CurlAsyncSession(
                impersonate=TLS_IMPERSONATE, proxy=self._proxy_for(token, slot)
            )
            self._http_sessions[key] = sess
        return sess

    async def _close_http(self):
        sessions = list(self._http_sessions.values())
        self._http_sessions.clear()
        for sess in sessions:
            try:
                await sess.close()
            except Exception:
                pass

    def _redact(self, message: str) -> str:
        """Hide the credentials of any configured proxy, should an error message carry one."""
        for proxy in self._proxy_list:
            creds = urlsplit(proxy).netloc.rpartition("@")[0]
            if creds:
                message = message.replace(f"{creds}@", "***@")
        return message

    def _log(self, message: str, level: str = "info"):
        message = self._redact(message)
        getattr(logger, level if level != "warn" else "warning", logger.info)(message)
        if self.config.log_callback:
            try:
                self.config.log_callback(message, level)
            except Exception:
                pass

    def _debug(self, message: str, level: str = "info"):
        """Diagnostic log to the Python logger only, not the UI."""
        message = self._redact(message)
        getattr(logger, level if level != "warn" else "warning", logger.info)(message)

    def _progress(self, current: int, total: Optional[int] = None, message: str = ""):
        if self.config.progress_callback:
            try:
                self.config.progress_callback(current, total, message)
            except Exception:
                pass

    def _bio_progress(self):
        b = self._bios
        done = b["fetched"] + b["skipped"]
        if self.config.bio_progress_callback:
            try:
                self.config.bio_progress_callback(
                    done, b["queued"], f"Fetched {done:,} of {b['queued']:,} bios"
                )
            except Exception:
                pass

    @staticmethod
    def _build_avatar_url(uid: str, avatar_hash: Optional[str]) -> Optional[str]:
        if not uid or not avatar_hash:
            return None
        ext = "gif" if str(avatar_hash).startswith("a_") else "png"
        return f"https://cdn.discordapp.com/avatars/{uid}/{avatar_hash}.{ext}?size=1024"

    @staticmethod
    def _fmt_dur(seconds: float) -> str:
        s = int(seconds)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        if h:
            return f"{h}h {m}m {s}s"
        if m:
            return f"{m}m {s}s"
        return f"{s}s"

    async def _get(self, token: str, url: str, timeout: float):
        """GET as this token: its own curl_cffi session and its own client headers."""
        return await self._http(token).get(
            url, headers=build_headers(token), timeout=timeout
        )

    async def _validate_token(
        self, token: str
    ) -> tuple[bool, Optional[str], Optional[str]]:
        try:
            resp = await self._get(token, f"{self.DISCORD_API}/users/@me", 5)
            if resp.status_code == 200:
                data = resp.json()
                return True, data.get("username"), str(data.get("id"))
            return False, None, None
        except Exception as e:
            self._log(f"⚠️ Token validation error", "error")
            self._debug(f"Token validation error: {e}")
            return False, None, None

    async def _check_guild_access(self, token: str, guild_id: int) -> bool:
        """Paginate /users/@me/guilds to check membership (handles 200+ guilds and rate limits)."""
        wanted = str(guild_id)
        after = "0"
        try:
            while True:
                url = f"{self.DISCORD_API}/users/@me/guilds?limit=200&after={after}"
                resp = await self._get(token, url, 8)
                if resp.status_code == 429:
                    try:
                        retry_after = float(resp.json().get("retry_after", 2))
                    except Exception:
                        retry_after = 2.0
                    self._debug(
                        f"Rate limited on guild access check, retrying in {retry_after:.1f}s",
                        "warn",
                    )
                    await asyncio.sleep(retry_after + 0.25)
                    continue
                if resp.status_code != 200:
                    return False
                guilds = resp.json()
                if not guilds:
                    return False
                for g in guilds:
                    if str(g.get("id")) == wanted:
                        return True
                after = str(guilds[-1].get("id", "0"))
                if len(guilds) < 200:
                    return False
        except Exception as e:
            self._debug(f"Guild access check error: {e}", "error")
            return False

    async def _get_guild_info(
        self, token: str, guild_id: int
    ) -> tuple[Optional[int], Optional[str]]:
        """Fetch approximate member count and guild name via REST."""
        try:
            resp = await self._get(
                token, f"{self.DISCORD_API}/guilds/{guild_id}?with_counts=true", 8
            )
            if resp.status_code == 200:
                data = resp.json()
                count = (
                    int(
                        data.get("approximate_member_count")
                        or data.get("member_count")
                        or 0
                    )
                    or None
                )
                name = data.get("name")
                return count, name
            return None, None
        except Exception:
            return None, None

    async def _bio_worker(
        self,
        token: str,
        bio_queue: asyncio.Queue,
        stop: asyncio.Event,
    ):
        """Fetch /users/{id}/profile bios as one token until stop is set and the queue is
        empty, or the scrape is stopped. Every valid token runs one, all on the same queue,
        each pacing its own calls. Every id taken off the queue is marked done exactly once
        (the finally below): fetched, skipped (BIO_REFUSED at once, any other failure after
        BIO_ATTEMPTS tries, counted across tokens) or put back first, for a 429 or a
        failure with tries left."""
        next_allowed = 0.0
        failures = self._bio_failures

        while not stop.is_set() or not bio_queue.empty():
            if self._stop_event.is_set():
                while not bio_queue.empty():
                    try:
                        bio_queue.get_nowait()
                        bio_queue.task_done()
                    except Exception:
                        break
                return

            try:
                uid = await asyncio.wait_for(bio_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                wait = next_allowed - self._clock()
                if wait > 0 and await self._pause(wait + 0.05):
                    continue

                outcome, retry_after = await self._fetch_bio(token, uid)
                if outcome == "rate_limited":
                    next_allowed = self._clock() + retry_after + 0.25
                    bio_queue.put_nowait(uid)
                    continue

                next_allowed = self._clock() + 1.25 + random.uniform(0.1, 0.4)
                if outcome == "failed":
                    failures[uid] = failures.get(uid, 0) + 1
                    if failures[uid] < BIO_ATTEMPTS:
                        bio_queue.put_nowait(uid)
                        continue
                    self._debug(
                        f"Bio skipped uid={uid} after {BIO_ATTEMPTS} attempts", "warn"
                    )
                failures.pop(uid, None)
                self._bios["fetched" if outcome == "ok" else "skipped"] += 1
                if self._bio_phase:
                    self._bio_progress()
            finally:
                bio_queue.task_done()

    async def _fetch_bio(self, token: str, uid: str) -> tuple[str, float]:
        """One GET /users/{uid}/profile as this token: ("ok", 0), ("rate_limited",
        retry_after), ("refused", 0) for BIO_REFUSED, or ("failed", 0) for any other
        status, a network, proxy or timeout error, or a body that does not parse."""
        url = f"{self.DISCORD_API}/users/{uid}/profile"
        try:
            resp = await self._get(token, url, 10)
            status = resp.status_code
            if status == 429:
                retry_after = 1.0
                try:
                    retry_after = float(resp.json().get("retry_after", 1.0))
                except Exception:
                    hdr = resp.headers.get("Retry-After")
                    if hdr:
                        try:
                            retry_after = float(hdr)
                        except Exception:
                            pass
                return "rate_limited", max(retry_after, 0.0)

            if status in BIO_REFUSED:
                self._debug(f"Bio refused uid={uid}: HTTP {status}")
                return "refused", 0.0

            if status != 200:
                self._debug(f"Bio fetch failed uid={uid}: HTTP {status}")
                return "failed", 0.0

            data = resp.json()
            bio_val = (data.get("user") or {}).get("bio") or data.get("bio")
        except Exception as exc:
            self._debug(f"Bio fetch error uid={uid}: {exc}")
            return "failed", 0.0

        async with self._members_lock:
            if uid in self._members and bio_val is not None:
                self._members[uid]["bio"] = bio_val
        return "ok", 0.0

    async def _pause(self, seconds: float) -> bool:
        """Sleep that a stop cuts short: True when the scrape was stopped."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return False
        return True

    def _queue_bio(self, bio_queue: asyncio.Queue, uid: str):
        self._bios["queued"] += 1
        bio_queue.put_nowait(uid)

    async def _finish_bios(
        self, bio_queue: asyncio.Queue, bio_tasks: List[asyncio.Task]
    ):
        """Wait until every queued bio is fetched or skipped, or the scrape is stopped; no
        time cap. Reports bios done of queued now, then the workers report each one they
        finish. Workers that all ended without draining the queue (they should not) end
        the wait too, so the scrape still saves."""
        b = self._bios
        left = b["queued"] - b["fetched"] - b["skipped"]
        if left > 0:
            self._log(f"📝 Fetching bios — {left:,} to go", "info")
        self._bio_phase = True
        self._bio_progress()
        join = asyncio.ensure_future(bio_queue.join())
        stopped = asyncio.ensure_future(self._stop_event.wait())
        workers = asyncio.ensure_future(asyncio.wait(bio_tasks))
        try:
            done, _ = await asyncio.wait(
                {join, stopped, workers}, return_when=asyncio.FIRST_COMPLETED
            )
            if done == {workers}:
                self._debug("Bio workers ended before the queue drained", "warn")
        finally:
            join.cancel()
            stopped.cancel()
            workers.cancel()
            self._bio_phase = False
        if b["skipped"]:
            self._log(
                f"⚠️ {b['skipped']:,} bio(s) skipped: the profile did not load", "warn"
            )

    @contextlib.asynccontextmanager
    async def _gateway_socket(self, identity: GatewayClient, proxy: Optional[str]):
        """A gateway socket opened the way the client opens one: the zlib-stream URL, the
        client's upgrade headers for this token, and the proxy's tunnel. ``identity``
        sends on it while it is open."""
        ws = await open_websocket(
            gateway_url(zlib_stream=True), identity._ws_headers(), proxy
        )
        identity._ws = ws
        try:
            yield ws
        finally:
            identity._ws = None
            try:
                await asyncio.wait_for(ws.close(), timeout=0.5)
            except Exception:
                pass

    async def _run_gateway_session(
        self,
        session_index: int,
        token: str,
        guild_id: int,
        planner: SharedPlanner,
        parallel: int,
        bio_queue: Optional[asyncio.Queue],
        member_count_hint: Optional[int],
        proxy_url: Optional[str] = None,
        num_tokens: int = 1,
    ):
        """Run one gateway session: identify, send op-8 queries, collect GUILD_MEMBERS_CHUNK responses."""

        if (member_count_hint or 0) >= 500_000:
            parallel = max(parallel, 8)
        elif (member_count_hint or 0) >= 100_000:
            parallel = max(parallel, 6)

        total_concurrency = num_tokens * parallel
        refill_threshold = max(64, total_concurrency * 6)
        refill_step = min(10 + (num_tokens - 1) * 3, 20)

        in_flight: set[str] = set()
        nonce_to_query: dict[str, str] = {}
        nonce_seq = 0
        queries_total = 0
        last_growth_at = time.time()
        last_progress_push = 0.0
        logged_99_pct = False
        NO_GROWTH_TIMEOUT = 60.0

        def mk_nonce(q: str) -> str:
            nonlocal nonce_seq
            nonce_seq += 1
            return f"s{session_index}-n{nonce_seq}:{q}"

        # The selfclient's gateway client for this token, used only for what it would send
        # on connecting: its upgrade headers and its IDENTIFY. Sessions of the same token
        # share the token's fingerprint, as the client's do.
        identity = GatewayClient(token, reconnect=False)

        max_reconnects = 5
        max_total_reconnects = 999
        attempt = 0
        total_attempts = 0
        while attempt < max_reconnects and total_attempts < max_total_reconnects:
            if self._stop_event.is_set():
                return

            connection_productive = False
            hb_task: Optional[asyncio.Task] = None
            try:
                async with self._gateway_socket(identity, proxy_url) as ws:
                    inflater = ZlibStreamInflater()

                    async def next_frame(timeout: Optional[float]) -> Optional[dict]:
                        """The next gateway payload, or None while a zlib-stream
                        message is still incomplete."""
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        if isinstance(raw, (bytes, bytearray, memoryview)):
                            text = inflater.feed(bytes(raw))
                            if text is None:
                                return None
                        else:
                            text = raw
                        return json.loads(text)

                    async def first_frame() -> dict:
                        while True:
                            frame = await next_frame(None)
                            if frame is not None:
                                return frame

                    hello = await asyncio.wait_for(first_frame(), timeout=10)
                    if hello.get("op") != 10:
                        raise RuntimeError(f"Unexpected hello op={hello.get('op')}")
                    hb_interval = hello["d"]["heartbeat_interval"] / 1000.0

                    hb_stop = asyncio.Event()
                    seq_num = [None]

                    async def heartbeater():
                        await asyncio.sleep(hb_interval * random.uniform(0.1, 0.9))
                        while not hb_stop.is_set():
                            try:
                                await ws.send(json.dumps({"op": 1, "d": seq_num[0]}))
                            except Exception:
                                return
                            try:
                                await asyncio.wait_for(
                                    hb_stop.wait(), timeout=hb_interval
                                )
                            except asyncio.TimeoutError:
                                pass

                    hb_task = asyncio.create_task(heartbeater())

                    # The client's own IDENTIFY: capabilities, this token's web-client
                    # properties, presence, compress and client_state.
                    await identity._send_identify()

                    ready = False

                    async def send_query(q: str):
                        nonlocal queries_total
                        n = mk_nonce(q)
                        await ws.send(
                            json.dumps(
                                {
                                    "op": 8,
                                    "d": {
                                        "guild_id": str(guild_id),
                                        "query": q,
                                        "limit": 100,
                                        "presences": False,
                                        "nonce": n,
                                    },
                                }
                            )
                        )
                        in_flight.add(n)
                        nonce_to_query[n] = q
                        queries_total += 1

                    async def pump_queries():
                        if self._stop_event.is_set():
                            return
                        slots = parallel - len(in_flight)
                        if slots <= 0:
                            return
                        await planner.refill_if_starving(
                            threshold=refill_threshold, step=refill_step
                        )

                        skip_single = (member_count_hint or 0) > 1000

                        max_local_rounds = 200
                        local_rounds = 0
                        while slots > 0 and local_rounds < max_local_rounds:
                            batch = await planner.next_batch(slots)
                            if not batch:
                                break
                            sent_any = False
                            for q in batch:
                                if self._stop_event.is_set():
                                    return
                                if skip_single and q and len(q) < 2:

                                    await planner.on_chunk_result(q, 100)
                                    await planner.ensure_children(
                                        q, force_full=True
                                    )
                                    local_rounds += 1
                                    continue
                                try:
                                    await send_query(q)
                                    sent_any = True
                                    slots -= 1
                                except Exception:
                                    await planner.requeue(q)
                                    return
                            if sent_any:
                                break

                            await planner.refill_if_starving(
                                threshold=refill_threshold, step=refill_step
                            )

                    stall_start = time.time()
                    STALL_TIMEOUT = 120.0
                    MSG_TIMEOUT = 2.0

                    while True:
                        if self._stop_event.is_set():
                            break

                        try:
                            data = await next_frame(MSG_TIMEOUT)
                        except asyncio.TimeoutError:

                            if not in_flight and not await planner.has_work():

                                if await planner.all_leaves_exhausted():
                                    total = len(self._members)
                                    self._debug(
                                        f"[S{session_index}] Planner fully exhausted — finishing (total={total:,})"
                                    )
                                    break

                                if (
                                    time.time() - last_growth_at
                                    >= NO_GROWTH_TIMEOUT
                                ):
                                    total = len(self._members)
                                    self._debug(
                                        f"[S{session_index}] No new members for {NO_GROWTH_TIMEOUT:.0f}s "
                                        f"and planner empty — finishing (total={total:,})"
                                    )
                                    break
                            continue
                        except Exception as exc:
                            # Closed by Discord or the proxy, or a frame that does not decode.
                            self._debug(
                                f"[S{session_index}] WS closed/error: {exc!r}", "warn"
                            )
                            break

                        if isinstance(data, dict):
                            op = data.get("op")
                            t = data.get("t")
                            d = data.get("d")
                            s = data.get("s")
                            if s is not None:
                                seq_num[0] = s

                            if op == 0 and t == "READY":
                                ready = True
                                self._sessions_ready += 1
                                self._log(
                                    f"🟢 Session {session_index + 1} connected",
                                    "info",
                                )
                                await asyncio.sleep(0.3)
                                await pump_queries()

                            elif op == 0 and t == "GUILD_MEMBERS_CHUNK":
                                nonce = (d or {}).get("nonce")
                                got = (d or {}).get("members") or []

                                q_for_nonce = None
                                if nonce and nonce in in_flight:
                                    in_flight.discard(nonce)
                                    q_for_nonce = nonce_to_query.pop(nonce, None)

                                added = 0
                                if got:
                                    async with self._members_lock:
                                        for m in got:
                                            u = (m or {}).get("user") or {}
                                            uid = u.get("id")
                                            if not uid or uid in self._members:
                                                continue
                                            rec: Dict[str, Any] = {
                                                "id": uid,
                                                "bot": bool(u.get("bot", False)),
                                            }
                                            if self.config.include_username:
                                                rec["username"] = u.get("username")
                                            if self.config.include_avatar_url:
                                                rec["avatar_url"] = (
                                                    self._build_avatar_url(
                                                        uid, u.get("avatar")
                                                    )
                                                )
                                            if self.config.include_roles:
                                                rec["roles"] = [
                                                    str(r)
                                                    for r in (m.get("roles") or [])
                                                ]
                                            if self.config.include_bio:
                                                rec["bio"] = None
                                                if bio_queue is not None:
                                                    self._queue_bio(bio_queue, uid)
                                            self._members[uid] = rec
                                            added += 1
                                    stall_start = time.time()
                                    connection_productive = True
                                    if added > 0:
                                        last_growth_at = time.time()

                                usernames = []
                                for m in got or []:
                                    u = (m or {}).get("user") or {}
                                    uname = (u.get("username") or "").casefold()
                                    if uname:
                                        usernames.append(uname)
                                if usernames:
                                    await planner.mark_observed_usernames_bulk(
                                        usernames
                                    )

                                if q_for_nonce is not None:
                                    size = len(got)

                                    expand_step = min(10 + (num_tokens - 1) * 4, 26)
                                    await planner.on_chunk_result_and_expand(
                                        q_for_nonce, size, expand_step=expand_step
                                    )

                                total = len(self._members)
                                now = time.time()
                                if now - last_progress_push >= 0.25:
                                    self._progress(
                                        total,
                                        member_count_hint,
                                        f"Found {total:,} members",
                                    )
                                    last_progress_push = now

                                if not logged_99_pct:
                                    target = (
                                        int(member_count_hint * 0.99)
                                        if member_count_hint
                                        else 0
                                    )
                                    if target and total >= target:
                                        self._log(
                                            f"🎯 Almost done! {total:,}/{member_count_hint:,} members — finishing up...",
                                            "info",
                                        )
                                        logged_99_pct = True

                                await pump_queries()

                                if not in_flight and not await planner.has_work():

                                    swept = (
                                        await planner.sweep_full_children_for_saturated()
                                    )
                                    miss = await planner.missing_roots()
                                    for r in miss:
                                        await planner.requeue(r)
                                    await planner.refill_if_starving(
                                        threshold=refill_threshold, step=refill_step
                                    )
                                    if await planner.has_work():
                                        await pump_queries()

                            elif op == 11:
                                pass
                            elif op == 9:
                                self._debug(
                                    f"[S{session_index}] Invalid session, reconnecting...",
                                    "warn",
                                )
                                self._log(
                                    f"🔄 Session {session_index + 1} reconnecting...",
                                    "warn",
                                )
                                break
                            elif op == 7:
                                self._debug(
                                    f"[S{session_index}] Server requested reconnect",
                                    "warn",
                                )
                                self._log(
                                    f"🔄 Session {session_index + 1} reconnecting...",
                                    "warn",
                                )
                                break

                        if (
                            time.time() - stall_start > STALL_TIMEOUT
                            and not in_flight
                        ):
                            self._debug(
                                f"[S{session_index}] Stall detected, breaking",
                                "warn",
                            )
                            break

                    pq_len = await planner.queue_len()
                    self._debug(
                        f"[S{session_index}] WS loop exited "
                        f"in_flight={len(in_flight)} pq={pq_len} total={len(self._members):,} "
                        f"productive={connection_productive}"
                    )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._debug(
                    f"[S{session_index}] Gateway error (attempt {attempt+1}): {e}",
                    "warn",
                )
                self._log(
                    f"⚠️ Session {session_index + 1} connection error — retrying...",
                    "warn",
                )
                await asyncio.sleep(2 + attempt)
            finally:
                if hb_task is not None:
                    hb_task.cancel()
                    hb_task = None

            total_attempts += 1
            if connection_productive:
                attempt = 0
            else:
                attempt += 1

            for n in list(in_flight):
                q = nonce_to_query.pop(n, None)
                in_flight.discard(n)
                if q:
                    await planner.requeue(q)

            if self._stop_event.is_set():
                return
            if not await planner.has_work():
                await planner.sweep_full_children_for_saturated()
                miss = await planner.missing_roots()
                for r in miss:
                    await planner.requeue(r)
                if not await planner.has_work():

                    await asyncio.sleep(2.0)
                    await planner.refill_if_starving(
                        threshold=refill_threshold, step=refill_step
                    )
                    if (
                        not await planner.has_work()
                        and await planner.all_leaves_exhausted()
                    ):
                        self._debug(
                            f"[S{session_index}] Planner exhausted after reconnect, finishing"
                        )
                        return

        self._debug(
            f"[S{session_index}] Exhausted reconnect attempts "
            f"(consecutive_failures={attempt}, total_cycles={total_attempts}), session ending"
        )

    async def scrape(self) -> ScraperResult:
        """Execute the scraping operation and return results."""
        t0 = time.perf_counter()
        self._running = True
        self._stop_event.clear()
        self._members.clear()
        self._bios = {"queued": 0, "fetched": 0, "skipped": 0}
        self._bio_phase = False
        self._sessions_ready = 0
        self._bio_failures = {}
        bio_tasks: List[asyncio.Task] = []
        self._plan_proxies(self.config.tokens)

        guild_id = self.config.guild_id
        guild_name = None
        member_count = None
        self._log(f"🚀 Starting scraper for guild {guild_id}", "info")
        n_tokens = len(self.config.tokens)
        n_proxies = len(self._proxy_list)
        if n_proxies:
            self._log(
                f"Using {n_tokens} token(s) and {n_proxies} prox{'ies' if n_proxies != 1 else 'y'}",
                "info",
            )
        else:
            self._log(f"Using {n_tokens} token(s)", "info")

        try:

            valid_tokens: list[str] = []
            for i, token in enumerate(self.config.tokens, 1):
                self._log(
                    f"🔑 Validating token {i}/{len(self.config.tokens)}...", "info"
                )
                ok, uname, uid = await self._validate_token(token)
                if not ok:
                    self._log(f"⚠️ Token {i} is invalid — skipping", "warn")
                    continue
                self._log(f"✅ Token {i} valid ({uname})", "info")
                valid_tokens.append(token)

            if not valid_tokens:
                return ScraperResult(
                    success=False, error="No valid tokens with guild access"
                )

            self._log(f"✅ {len(valid_tokens)} token(s) ready", "info")

            member_count, guild_name = await self._get_guild_info(
                valid_tokens[0], guild_id
            )
            if member_count and guild_name:
                self._log(f"🏰 Guild: {guild_name} (~{member_count:,} members)", "info")
            elif member_count:
                self._log(f"🏰 Guild has ~{member_count:,} members", "info")
            elif guild_name:
                self._log(f"🏰 Guild: {guild_name}", "info")

            alphabet = "abcdefghijklmnopqrstuvwxyz0123456789_-."
            qp = QueryPlanner(alphabet=alphabet, limit=100, max_repeat_run=4)
            planner = SharedPlanner(qp)
            await planner.set_session_slots(
                len(valid_tokens) * self.config.sessions_per_token
            )

            letters = "abcdefghijklmnopqrstuvwxyz"
            small_guild = member_count is not None and member_count <= 150

            if small_guild:
                await planner.seed_top_level(alphabet)
            else:
                await planner.seed_top_level(alphabet)
                letter_priority = list("etaoinrshlcmdupfgwybvkxjqz")
                n_sessions = len(valid_tokens) * self.config.sessions_per_token

                if n_sessions <= 2:
                    top = letter_priority[:10]
                elif n_sessions <= 4:
                    top = letter_priority[:16]
                elif n_sessions <= 6:
                    top = letter_priority[:20]
                else:
                    top = letter_priority
                roots = [a + b for a in top for b in top]

                digs = list("0123456789")
                dig_letters = min(6 + n_sessions, len(letter_priority))
                dig_count = min(4 + n_sessions // 2, len(digs))
                roots += [
                    a + b
                    for a in letter_priority[:dig_letters]
                    for b in digs[:dig_count]
                ]
                roots += [
                    a + b
                    for a in digs[:dig_count]
                    for b in letter_priority[:dig_letters]
                ]
                self._debug(
                    f"Seeding {len(roots):,} 2-gram roots ({n_sessions} sessions)"
                )
                await planner.seed_two_gram_roots_once(roots)

            bio_queue: Optional[asyncio.Queue] = None
            bio_stop = asyncio.Event()
            if self.config.include_bio:
                bio_queue = asyncio.Queue()
                bio_tasks = [
                    asyncio.create_task(self._bio_worker(token, bio_queue, bio_stop))
                    for token in valid_tokens
                ]

            progress_stop = asyncio.Event()

            async def progress_reporter():
                spinner = itertools.cycle("|/-\\")
                while not progress_stop.is_set() and not self._stop_event.is_set():
                    total = len(self._members)
                    snap = await planner.snapshot_metrics()
                    pq = snap["pq_len"]
                    visited = snap["visited"]
                    sat = snap["saturated"]
                    if member_count and member_count > 0:
                        pct = (
                            min(total / member_count * 100, 99.9)
                            if total < member_count
                            else 100.0
                        )
                        self._log(
                            f"⛏️ Scraping — {pct:5.1f}% ({total:,}/{member_count:,})",
                            "info",
                        )
                    else:
                        sp = next(spinner)
                        self._log(
                            f"⛏️ Scraping {sp} — found {total:,} members so far",
                            "info",
                        )

                    self._debug(
                        f"[progress] pq={pq} visited={visited} sat={sat} total={total}"
                    )
                    self._progress(total, member_count, f"Found {total:,} members")
                    try:
                        await asyncio.wait_for(progress_stop.wait(), timeout=1.5)
                    except asyncio.TimeoutError:
                        pass

            progress_task = asyncio.create_task(progress_reporter())

            gateway_error: Optional[str] = None
            try:
                n_tok = len(valid_tokens)
                par = self.config.max_parallel_per_session
                spt = self.config.sessions_per_token
                total_sessions = n_tok * spt
                self._log(
                    f"🔌 Connecting {total_sessions} session(s) to Discord gateway...",
                    "info",
                )
                self._debug(
                    f"Parallelism: {n_tok} tokens × {spt} sessions/token × {par} queries/session "
                    f"= {total_sessions * par} concurrent gateway queries"
                )
                tasks = []
                session_idx = 0
                sessions_per = self.config.sessions_per_token
                for token in valid_tokens:
                    for s in range(sessions_per):
                        session_proxy = self._proxy_for(token, s)
                        if session_proxy:
                            self._debug(
                                f"[S{session_idx}] Using proxy: {_mask_proxy_for_log(session_proxy)}"
                            )
                        tasks.append(
                            self._run_gateway_session(
                                session_index=session_idx,
                                token=token,
                                guild_id=guild_id,
                                planner=planner,
                                parallel=self.config.max_parallel_per_session,
                                bio_queue=bio_queue,
                                member_count_hint=member_count,
                                proxy_url=session_proxy,
                                num_tokens=total_sessions,
                            )
                        )
                        session_idx += 1
                await asyncio.gather(*tasks)
            except Exception as e:
                self._debug(f"Gateway scraping failed: {e}")
                gateway_error = f"Gateway scrape failed: {e}"

            progress_stop.set()
            await progress_task

            if (
                gateway_error is None
                and not self._members
                and not self._stop_event.is_set()
            ):
                if self._sessions_ready:
                    gateway_error = "The Discord gateway returned no members"
                else:
                    gateway_error = (
                        f"No gateway session connected to Discord "
                        f"({total_sessions} tried)"
                    )
            if gateway_error is not None:
                self._log(f"❌ {gateway_error}", "error")
                return ScraperResult(
                    members=list(self._members.values()),
                    total_count=len(self._members),
                    success=False,
                    error=gateway_error,
                    elapsed_seconds=time.perf_counter() - t0,
                    metadata={
                        "method": "gateway",
                        "tokens_used": len(valid_tokens),
                        "member_count_hint": member_count,
                        "guild_name": guild_name,
                        "cancelled": False,
                    },
                )

            if bio_queue is not None and bio_tasks and not self._stop_event.is_set():
                self._progress(len(self._members), member_count)
                await self._finish_bios(bio_queue, bio_tasks)
            bio_stop.set()
            await self._close_bios(bio_tasks)
            bio_tasks = []

            elapsed = time.perf_counter() - t0
            members_list = list(self._members.values())
            was_cancelled = self._stop_event.is_set()

            if was_cancelled:
                self._log(
                    f"🛑 Scrape cancelled — {len(members_list):,} members found in {self._fmt_dur(elapsed)}",
                    "info",
                )
            else:
                self._log(
                    f"✅ Scraping completed — {len(members_list):,} members found in {self._fmt_dur(elapsed)}",
                    "info",
                )

            return ScraperResult(
                members=members_list,
                total_count=len(members_list),
                success=True,
                elapsed_seconds=elapsed,
                metadata={
                    "method": "gateway",
                    "tokens_used": len(valid_tokens),
                    "member_count_hint": member_count,
                    "guild_name": guild_name,
                    "cancelled": was_cancelled,
                    **({"bios": dict(self._bios)} if self.config.include_bio else {}),
                },
            )

        except asyncio.CancelledError:
            elapsed = time.perf_counter() - t0
            n = len(self._members)
            self._log(
                f"🛑 Scrape cancelled — {n:,} members found in {self._fmt_dur(elapsed)}",
                "warn",
            )
            return ScraperResult(
                success=False,
                error="Cancelled",
                members=list(self._members.values()),
                total_count=len(self._members),
                elapsed_seconds=elapsed,
                metadata={
                    "guild_name": guild_name or "",
                    **({"bios": dict(self._bios)} if self.config.include_bio else {}),
                },
            )
        except Exception as e:
            elapsed = time.perf_counter() - t0
            self._log(f"❌ Scraping error: {e}", "error")
            return ScraperResult(
                success=False,
                error=str(e),
                elapsed_seconds=elapsed,
            )
        finally:
            # A cancel or an error before the bio phase ended leaves the workers running.
            await self._close_bios(bio_tasks)
            await self._close_http()
            self._running = False

    async def _close_bios(self, bio_tasks: List[asyncio.Task]):
        if not bio_tasks:
            return
        for task in bio_tasks:
            task.cancel()
        try:
            await asyncio.wait(set(bio_tasks), timeout=0.5)
        except BaseException:
            pass
        for task in bio_tasks:
            if task.done() and not task.cancelled():
                task.exception()

    def stop(self):
        """Stop the scraping operation."""
        self._log("🛑 Cancelling scrape...", "info")
        self._stop_event.set()

    @property
    def is_running(self) -> bool:
        return self._running
