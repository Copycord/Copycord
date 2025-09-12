# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
import asyncio
import json
import time
import random
import heapq
import itertools
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable, List
import contextlib
import aiohttp
import discord


@dataclass(order=True)
class _PQItem:
    priority: float
    prefix: str = field(compare=False)


class QueryPlanner:
    """
    Global, bigram-aware planner for search prefixes.
    - Intended to be wrapped by SharedPlanner to make it async-safe and shared across sessions.
    - Learns unigram + bigram models from observed usernames.
    - Class-aware children (letters first, careful with digits/punct).
    - Guarantees completion: when no work AND no saturated unexplored branch.
    """

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
            "aeiours tnlcmpdbhgkuywvfjzxq".replace(" ", ""),
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
        i = len(s) - 1
        n = 0
        while i >= 0 and s[i] == last:
            n += 1
            i -= 1
        return n

    def set_session_slots(self, n: int) -> None:
        self._session_slots = max(1, min(5, int(n or 1)))

    def note_digit_lead(self) -> None:
        self._saw_digit_lead = True

    def _sorted_alphabet(self) -> list[str]:
        """
        Restrict expansion/closure strictly to the declared base alphabet.
        We still use learned frequencies for ranking but never introduce new chars here.
        """
        base = self.alphabet_base
        return sorted(base, key=lambda c: (-self.char_freq.get(c, 1), base.index(c)))

    def _score_prefix(self, prefix: str) -> float:
        """
        Rank prefixes in PQ: unseen 2-grams win over saturated deep branches.
        Session-aware tweaks bias what unseen 2-grams to try first when slots are few.
        """
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

    def on_chunk_result(self, prefix: str, size: int) -> None:
        self.stats[prefix] = size
        self.visited.add(prefix)
        self._in_queue.discard(prefix)
        if size == 0:
            self.dead.add(prefix)
        elif size < self.limit:
            self.leaves.add(prefix)

    def _class_gate(self, prefix: str, ch: str) -> bool:
        """Heuristics to avoid low-signal punctuation/digit trails (tightened)."""
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
                has_bigram = prev is not None and (self.bi_freq.get((prev, ch), 0) >= 1)
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
        """Blend bigram (if prev) with unigram."""
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
        """Escalate k for a saturated prefix, stepwise or force full fanout."""
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
        return bool(self._pq)

    def queue_len(self) -> int:
        return len(self._pq)

    def all_leaves_exhausted(self, *, ignore_gate: bool = True) -> bool:
        """
        Return True only if:
          - there is no queued work, AND
          - every saturated prefix has had ALL single-char extensions
            (over the alphabet) explored/ruled-out.
        By default we **ignore** the class gate for the final check to guarantee closure.
        """
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
                    (c not in self.visited)
                    and (c not in self.dead)
                    and (c not in self._in_queue)
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
    """
    Async-safe shared planner wrapper.
    All sessions pull from the same PQ/frontier.
    """

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

    async def note_digit_lead(self):
        async with self._lock:
            self.p.note_digit_lead()

    async def on_chunk_result(self, prefix: str, size: int):
        async with self._lock:
            self.p.on_chunk_result(prefix, size)

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
        """Force full fan-out for any saturated parent, ignoring class gates (to guarantee closure)."""
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
            miss: list[str] = []
            for r in self._seeded_roots_set:
                if r not in self.p.stats:
                    miss.append(r)
            return miss

    async def refill_if_starving(self, *, threshold: int = 128, step: int = 12) -> int:
        """
        If PQ is small, aggressively fan out saturated parents to refill work.
        Prevents tunnel on a single branch (e.g., 'john*') when frontier shrinks.
        """
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
        """Lightweight metrics for progress logging."""
        async with self._lock:
            return {
                "pq_len": len(self.p._pq),
                "visited": len(self.p.visited),
                "leaves": len(self.p.leaves),
                "saturated": sum(1 for s in self.p.stats.values() if s >= self.p.limit),
                "stats_size": len(self.p.stats),
            }

    async def set_session_slots(self, n: int):
        async with self._lock:
            self.p.set_session_slots(n)


class MemberScraper:
    GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"

    def __init__(
        self, bot: discord.Client, config: Any, logger: Optional[Any] = None
    ) -> None:
        self.bot = bot
        self.config = config
        self.log = (
            (logger or discord.utils.setup_logging()).getChild("MemberScraper")
            if hasattr(logger or (), "getChild")
            else (
                (logger or __import__("logging").getLogger(__name__)).getChild(
                    "MemberScraper"
                )
            )
        )
        self._cancel_event = asyncio.Event()
        self._members_ref: Optional[Dict[str, Dict[str, Any]]] = None
        self._members_lock_ref: Optional[asyncio.Lock] = None
        self._fingerprint: Optional[dict] = None

        self._progress_stop: Optional[asyncio.Event] = None
        self._progress_task: Optional[asyncio.Task] = None

    def request_cancel(self) -> None:
        """Cooperative cancellation signal."""
        self._cancel_event.set()

        ps = getattr(self, "_progress_stop", None)
        if ps is not None:
            ps.set()
        pt = getattr(self, "_progress_task", None)
        if pt is not None:
            pt.cancel()

    async def snapshot_members(self) -> list[dict]:
        """Return a thread-safe snapshot of members collected so far."""
        lock = self._members_lock_ref
        mem = self._members_ref
        if lock and mem is not None:
            async with lock:
                return list(mem.values())
        return []

    def _build_headers(self, token: str) -> dict:
        import base64, json as _json, random as _random

        if self._fingerprint is not None:
            super_props_b64 = self._fingerprint["super_props_b64"]
            return {
                **self._fingerprint["headers"],
                "Authorization": token,
                "X-Super-Properties": super_props_b64,
            }

        client_versions = ["1.0.9163", "1.0.9156", "1.0.9154"]
        chrome_versions = ["108.0.5359.215", "139.0.7258.155"]
        electron_versions = ["22.3.26", "22.3.18"]
        win_builds = ["10.0.22621", "10.0.22631"]

        client_version = _random.choice(client_versions)
        chrome_version = _random.choice(chrome_versions)
        electron_version = _random.choice(electron_versions)
        os_version = _random.choice(win_builds)

        locales = ["en-US", "en-GB", "de", "fr", "es-ES"]
        timezones = [
            "America/New_York",
            "America/Chicago",
            "Europe/Berlin",
            "Asia/Tokyo",
        ]
        locale = _random.choice(locales)
        tz = _random.choice(timezones)

        super_props = {
            "os": "Windows",
            "browser": "Discord Client",
            "release_channel": "stable",
            "client_version": client_version,
            "os_version": os_version,
            "os_arch": "x64",
            "system_locale": locale,
        }
        super_props_b64 = base64.b64encode(
            _json.dumps(super_props, separators=(",", ":")).encode()
        ).decode()

        headers = {
            "User-Agent": (
                f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                f"AppleWebKit/537.36 (KHTML, like Gecko) "
                f"discord/{client_version} "
                f"Chrome/{chrome_version} "
                f"Electron/{electron_version} Safari/537.36"
            ),
            "X-Discord-Locale": locale,
            "X-Discord-Timezone": tz,
            "Accept": "*/*",
            "Accept-Language": f"{locale},en;q=0.9",
            "DNT": "1",
            "Referer": "https://discord.com/channels/@me",
            "Origin": "https://discord.com",
            "Sec-CH-UA": '"Not A(Brand";v="99", "Chromium";v="108", "Google Chrome";v="108"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
        }

        self._fingerprint = {"headers": headers, "super_props_b64": super_props_b64}
        return {
            **headers,
            "Authorization": token,
            "X-Super-Properties": super_props_b64,
        }

    async def scrape(
        self,
        channel_id: int | str | None = None,
        *,
        guild_id: int | str | None = None,
        include_username: bool = False,
        include_avatar_url: bool = False,
        include_bio: bool = False,
        alphabet: str = "abcdefghijklmnopqrstuvwxyz0123456789_-.",
        max_parallel_per_session: int = 3,
        hello_ready_delay: float = 2.5,
        recycle_after_dispatch: int = 2500,
        stall_timeout: float = 120.0,
        num_sessions: int = 1,
        strict_complete: bool = True,
        final_sweep_seconds: float = 15.0,
        final_no_growth_seconds: float = 6.0,
        final_sweep_burst: int = 5,
        final_sweep_pause: float = 0.15,
        refresh_total_once: bool = True,
    ) -> Dict[str, Any]:

        t0 = time.perf_counter()

        def _fmt_dur(seconds: float) -> str:
            s = int(seconds)
            m, s = divmod(s, 60)
            h, m = divmod(m, 60)
            if h:
                return f"{h}h {m}m {s}s"
            if m:
                return f"{m}m {s}s"
            return f"{s}s"

        self._cancel_event = asyncio.Event()
        bio_processed = 0

        gid_in = (
            guild_id
            if guild_id is not None
            else getattr(self.config, "HOST_GUILD_ID", None)
        )
        try:
            gid_int = int(gid_in) if gid_in is not None else None
        except Exception:
            gid_int = None

        if gid_int is None:
            raise RuntimeError("No guild id provided and HOST_GUILD_ID is not set")

        guild = self.bot.get_guild(gid_int)
        if not guild:
            raise RuntimeError(f"Guild {gid_int} not found or not cached")

        gname = getattr(guild, "name", "UNKNOWN")
        self.log.debug(
            f"[guild] Target guild: {gname} ({guild.id}) (source={'caller' if guild_id is not None else 'config'})"
        )
        self.log.debug(f"[guild] Target guild: {gname} ({guild.id})")

        target_count_cached = getattr(guild, "member_count", None)

        max_total_seen = (
            int(target_count_cached)
            if isinstance(target_count_cached, int) and target_count_cached > 0
            else 0
        )

        stop_event = asyncio.Event()

        def ts() -> str:
            return time.strftime("%H:%M:%S", time.localtime())

        def _dedup_chars(s: str) -> str:
            seen = set()
            out: List[str] = []
            for ch in s or "":
                if ch not in seen:
                    seen.add(ch)
                    out.append(ch)
            return "".join(out)

        cfg_alpha = getattr(self.config, "SCRAPER_ALPHABET", None)
        ext = getattr(self.config, "EXTENDED_CHARS", "")
        alphabet_l = _dedup_chars(
            ((cfg_alpha or alphabet).casefold()) + ((ext or "").casefold())
        )
        letters_only = "".join(ch for ch in alphabet_l if ch.isalpha())
        digits_punct = "".join(ch for ch in alphabet_l if not ch.isalpha())
        base_alpha_set = set(alphabet_l)
        dynamic_leads_added_global: set[str] = set()
        allow_empty_queries: bool = False

        small_guild_hint = (
            isinstance(target_count_cached, int) and target_count_cached <= 150
        )

        shared_planner = SharedPlanner(
            QueryPlanner(
                alphabet=alphabet_l,
                limit=100,
                max_repeat_run=4,
            )
        )

        try:
            is_bot = bool(getattr(getattr(self.bot, "user", None), "bot", False))
        except Exception:
            is_bot = False

        identify_d: Dict[str, Any] = {
            "token": self.config.CLIENT_TOKEN,
            "properties": {"$os": "linux", "$browser": "disco", "$device": "disco"},
            "compress": False,
            "large_threshold": 250,
        }
        if is_bot:
            identify_d["intents"] = (1 << 0) | (1 << 1)

        identify_lock = asyncio.Lock()
        identify_next_allowed = 0.0

        async def gated_identify(ws):
            nonlocal identify_next_allowed
            async with identify_lock:
                now = time.time()
                if now < identify_next_allowed:
                    await asyncio.sleep(identify_next_allowed - now)
                await ws.send_json({"op": 2, "d": identify_d})

                identify_next_allowed = time.time() + 5.5 + random.uniform(0.0, 1.0)

        def build_avatar_url(uid: str, avatar_hash: str | None) -> str | None:
            if not uid or not avatar_hash:
                return None
            ext = "gif" if str(avatar_hash).startswith("a_") else "png"
            return f"https://cdn.discordapp.com/avatars/{uid}/{avatar_hash}.{ext}?size=1024"

        members: Dict[str, Dict[str, Any]] = {}
        members_lock = asyncio.Lock()
        self._members_ref = members
        self._members_lock_ref = members_lock

        warmup_lock = asyncio.Lock()
        warmup_done = asyncio.Event()

        bio_queue: asyncio.Queue[str] = asyncio.Queue()
        bio_status: Dict[str, str] = {}
        global_reset_at: float = 0.0

        final_sweep_lock = asyncio.Lock()
        final_sweep_done = asyncio.Event()

        rest_session_handle: Optional[aiohttp.ClientSession] = None

        async def refresh_guild_total() -> Optional[int]:
            """Try to refresh guild total using REST with with_counts=true."""
            s = rest_session_handle
            if s is None:
                return None
            try:
                url = f"https://discord.com/api/v10/guilds/{guild.id}?with_counts=true"
                r = await s.get(url, timeout=10)
                if r.status != 200:
                    return None
                j = await r.json()

                return (
                    int(j.get("member_count") or j.get("approximate_member_count") or 0)
                    or None
                )
            except Exception as e:
                self.log.debug("[refresh_total] failed: %r", e)
                return None

        class PState(IntEnum):
            PENDING = 0
            INFLIGHT = 1
            DONE = 2

        prefix_state_lock = asyncio.Lock()
        prefix_state: dict[str, PState] = {}

        async def claim_prefix(prefix: str) -> bool:
            async with prefix_state_lock:
                st = prefix_state.get(prefix, PState.PENDING)
                if st != PState.PENDING:
                    return False
                prefix_state[prefix] = PState.INFLIGHT
                return True

        async def finalize_prefix(prefix: str) -> None:
            async with prefix_state_lock:
                prefix_state[prefix] = PState.DONE

        async def abort_claim(prefix: str) -> None:
            async with prefix_state_lock:
                if prefix_state.get(prefix) == PState.INFLIGHT:
                    prefix_state[prefix] = PState.PENDING

        def user_cancelled() -> bool:
            return self._cancel_event.is_set()

        def target_reached() -> bool:
            return stop_event.is_set()

        def next_sibling_prefix(q: str, alpha: str) -> Optional[str]:
            if not q:
                return None
            last = q[-1]
            try:
                i = alpha.index(last)
            except ValueError:
                return None
            if i + 1 < len(alpha):
                return q[:-1] + alpha[i + 1]
            return None

        progress_stop = asyncio.Event()
        self._progress_stop = progress_stop
        spinner_cycle = itertools.cycle("|/-\\")
        bar_width = 28
        printed_planner_init = False

        def _bar(p: float, width: int = bar_width) -> str:
            p = max(0.0, min(1.0, p))
            filled = int(round(p * width))
            return "[" + "#" * filled + "-" * (width - filled) + "]"

        async def _progress_reporter():
            nonlocal printed_planner_init, max_total_seen

            try:
                while not progress_stop.is_set():

                    if self._cancel_event.is_set() or stop_event.is_set():
                        break

                    async with members_lock:
                        done = len(members)

                    live_total = max_total_seen
                    total = live_total if live_total > 0 else None

                    snap = await shared_planner.snapshot_metrics()
                    if not printed_planner_init:
                        self.log.debug(
                            "[planner:init] base_alpha=%d charfreq_keys=%d",
                            len(shared_planner.p.alphabet_base),
                            len(shared_planner.p.char_freq),
                        )
                        printed_planner_init = True

                    pq_len = snap["pq_len"]
                    visited = snap["visited"]
                    leaves = snap["leaves"]
                    saturated = snap["saturated"]

                    if total:
                        pct_raw = done / max(1, total)

                        finished_by_count = done >= total
                        pct = pct_raw if finished_by_count else min(pct_raw, 0.999)
                        self.log.info(
                            "[⛏️ Scraping] %s %5.1f%% (%s/%s) | pq=%d visited=%d leaves=%d sat=%d",
                            _bar(pct),
                            pct * 100.0,
                            f"{done:,}",
                            f"{total:,}",
                            pq_len,
                            visited,
                            leaves,
                            saturated,
                        )
                    else:
                        sp = next(spinner_cycle)
                        self.log.info(
                            "[⛏️ Scraping] %s collected=%s | pq=%d visited=%d leaves=%d sat=%d (total unknown)",
                            sp,
                            f"{done:,}",
                            pq_len,
                            visited,
                            leaves,
                            saturated,
                        )

                    await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                return

        bio_semaphore = asyncio.Semaphore(1)
        next_allowed_at: float = 0.0

        async def bio_worker(sess: aiohttp.ClientSession, stop_event: asyncio.Event):
            nonlocal global_reset_at, next_allowed_at, bio_processed
            max_retries_per_uid = 5
            retry_counts: dict[str, int] = {}

            async def rotate_session(
                old: aiohttp.ClientSession,
            ) -> aiohttp.ClientSession:
                self._fingerprint = None
                tok = getattr(self.config, "CLIENT_TOKEN", None)
                headers = self._build_headers(tok)
                try:
                    await old.close()
                except Exception:
                    pass
                return aiohttp.ClientSession(headers=headers)

            current_sess = sess
            try:
                while True:
                    if (
                        stop_event.is_set() or self._cancel_event.is_set()
                    ) and bio_queue.empty():
                        self.log.debug(
                            "[Copycord Scraper] stop_event or cancel_event set → exiting bio session"
                        )
                        break

                    if self._cancel_event.is_set() and not bio_queue.empty():
                        self.log.debug(
                            "[Copycord Scraper] Cancelled bio scrape — purging bio queue"
                        )
                        while not bio_queue.empty():
                            try:
                                bio_queue.get_nowait()
                                bio_queue.task_done()
                            except Exception:
                                break
                        break

                    try:
                        uid = await asyncio.wait_for(bio_queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    if self._cancel_event.is_set():
                        self.log.debug(
                            "[Copycord Scraper] Cancelled bio scrape mid-loop — exiting before next request"
                        )
                        bio_queue.task_done()
                        return

                    url = f"https://discord.com/api/v10/users/{uid}/profile"
                    success = False
                    last_status = None

                    for attempt in range(3):
                        now = time.time()
                        wait_until = max(global_reset_at, next_allowed_at)
                        if now < wait_until:
                            sleep_for = wait_until - now + 0.05
                            self.log.debug(
                                "[Copycord Scraper] Bio scrape rate limit → sleeping %.2fs before next request",
                                sleep_for,
                            )
                            await asyncio.sleep(sleep_for)

                        if self._cancel_event.is_set():
                            self.log.debug(
                                "[Copycord Scraper] Cancelled bio scrape mid-retry — exiting cleanly"
                            )
                            bio_queue.task_done()
                            return

                        try:
                            async with bio_semaphore:
                                r = await current_sess.get(url, timeout=15)
                        except RuntimeError as e:
                            if (
                                "Session is closed" in str(e)
                            ) and self._cancel_event.is_set():
                                self.log.debug(
                                    "[Copycord Scraper] Bio scrape session already closed on cancel — stopping"
                                )
                                bio_queue.task_done()
                                return
                            raise

                        status = r.status
                        last_status = status

                        if status == 429:
                            retry_after = 1.0
                            used_header = False
                            try:
                                body = await r.json()
                                if "retry_after" in body:
                                    retry_after = float(body["retry_after"])
                            except Exception as e:
                                hdr = r.headers.get("Retry-After")
                                if hdr:
                                    try:
                                        retry_after = float(hdr)
                                        used_header = True
                                    except Exception:
                                        pass
                                self.log.debug(
                                    "[Copycord Scraper] uid=%s bio 429 non-JSON (%r) → header Retry-After=%s",
                                    uid,
                                    e,
                                    r.headers.get("Retry-After"),
                                )

                            if used_header and retry_after >= 300:
                                self.log.debug(
                                    "[Copycord Scraper] Severe 429 in bio scrape session for uid=%s → Retry-After=%ss. Starting new session..",
                                    uid,
                                    retry_after,
                                )
                            async with members_lock:
                                bio_status[uid] = "rate_limited"

                                current_sess = await rotate_session(current_sess)
                                sleep_for = 30 + random.uniform(0, 15)
                                self.log.warning(
                                    "[Copycord Scraper] Recycled session, sleeping %.1fs before retry",
                                    sleep_for,
                                )
                                await asyncio.sleep(sleep_for)

                                bio_queue.task_done()
                                success = True
                                break

                            buffer = max(0.25, retry_after * 0.25)
                            global_reset_at = max(
                                global_reset_at, time.time() + retry_after + buffer
                            )
                            next_allowed_at = global_reset_at

                            retry_counts[uid] = retry_counts.get(uid, 0) + 1
                            if retry_counts[uid] > max_retries_per_uid:
                                async with members_lock:
                                    bio_status[uid] = "rate_limited"
                                self.log.warning(
                                    "[Copycord Scraper] uid=%s bio permanently rate-limited after %d retries",
                                    uid,
                                    retry_counts[uid],
                                )
                                success = True
                            else:
                                await bio_queue.put(uid)
                                self.log.warning(
                                    "[Copycord Scraper] uid=%s bio 429 → backoff %.2fs (attempt %d/%d)",
                                    uid,
                                    retry_after,
                                    retry_counts[uid],
                                    max_retries_per_uid,
                                )
                            break

                        if status != 200:
                            self.log.warning(
                                "[Copycord Scraper] bio uid=%s status=%s (attempt %d/3)",
                                uid,
                                status,
                                attempt + 1,
                            )
                            if 500 <= status < 600:
                                await asyncio.sleep(1.0 + attempt)
                                continue
                            break

                        j = await r.json()
                        bio_val = (j.get("user") or {}).get("bio") or j.get("bio")

                        async with members_lock:
                            if uid in members and bio_val is not None:
                                members[uid]["bio"] = bio_val
                                bio_status[uid] = "found" if bio_val else "empty"
                            else:
                                bio_status[uid] = "empty"
                            bio_processed += 1
                            remaining = bio_queue.qsize()

                            if bio_val:
                                self.log.info(
                                    "[Bio Scraper] Found bio for user %s — %d checked %d remaining",
                                    uid,
                                    bio_processed,
                                    remaining,
                                )
                            else:
                                self.log.debug(
                                    "[Bio Scraper] User %s bio empty — %d found %d remaining",
                                    uid,
                                    bio_processed,
                                    remaining,
                                )

                        success = True
                        break

                    if not success and last_status != 429:
                        async with members_lock:
                            bio_status[uid] = f"failed_{last_status}"

                    next_allowed_at = time.time() + 1.25 + random.uniform(0.1, 0.4)
                    bio_queue.task_done()
            finally:
                if not current_sess.closed:
                    self.log.debug("[Copycord Scraper] Closing bio session on exit")
                    await current_sess.close()

        async def run_session(
            session_index: int, session: aiohttp.ClientSession
        ) -> None:
            nonlocal target_count_cached, max_total_seen
            dispatched_since_connect = 0
            last_progress_at = time.time()
            last_empty_sweep_ts = 0.0
            EMPTY_SWEEP_INTERVAL = 1.25
            EMPTY_SWEEP_BURST = 3

            parallel_limit = max_parallel_per_session

            try_total_hint = (
                int(max_total_seen)
                if max_total_seen > 0
                else (
                    int(target_count_cached)
                    if target_count_cached is not None
                    else None
                )
            )
            small_guild = (
                try_total_hint is not None and try_total_hint <= 150
            ) or small_guild_hint

            if num_sessions == 1 and (try_total_hint or 0) > 100:
                parallel_limit = max(4, int(parallel_limit))

            in_flight_nonces: set[str] = set()
            nonce_to_query: Dict[str, str] = {}
            nonce_sent_at: Dict[str, float] = {}
            query_retry_count: Dict[str, int] = {}
            nonce_seq = 0

            recycle_now = asyncio.Event()

            queries_total = 0
            queries_nonzero = 0

            letter_priority = [
                c for c in "etaoinrshlcmdupfgwybvkxjqz" if c in letters_only
            ]
            for c in letters_only:
                if c not in letter_priority:
                    letter_priority.append(c)

            nonletter_priority = [c for c in "0123456789_-." if c in digits_punct]
            for c in digits_punct:
                if c not in nonletter_priority:
                    nonletter_priority.append(c)

            def session_mode(num_sessions: int, total_hint: Optional[int]) -> str:

                huge = (total_hint or 0) >= 200_000
                if num_sessions <= 1:
                    return "lean_plus" if huge else "lean"
                if num_sessions == 2:
                    return "balanced"
                if num_sessions == 3:
                    return "wide" if not huge else "full-lite"
                if num_sessions == 4:
                    return "full-lite"
                return "full"

            def build_roots_for_mode(mode: str) -> list[str]:
                letters = [c for c in letter_priority]
                digits = [c for c in nonletter_priority if c.isdigit()]
                puncts = [c for c in nonletter_priority if not c.isdigit()]

                roots: list[str] = []

                def grid(A: list[str], B: list[str]) -> list[str]:
                    out = []
                    for a in A:
                        for b in B:
                            out.append(a + b)
                    return out

                if mode in ("lean", "lean_plus"):
                    L = 8 if mode == "lean" else 10
                    topL = letters[:L]
                    roots += grid(topL, topL)

                    roots += grid(topL[:6], digits[:4])
                    return roots

                if mode == "balanced":
                    topL = letters[:12]
                    roots += grid(topL, topL)
                    roots += grid(topL[:8], digits[:6])
                    roots += grid(digits[:4], topL[:10])
                    return roots

                if mode == "wide":
                    roots += grid(letters, letters)
                    roots += grid(letters[:12], digits[:8])
                    roots += grid(digits[:8], letters[:12])
                    return roots

                if mode == "full-lite":

                    all_letters = letters
                    roots += grid(all_letters, all_letters)
                    roots += grid(all_letters, digits)
                    roots += grid(digits, all_letters)
                    return roots

                base = [c for c in (letters + digits + puncts)]
                return [a + b for a in base for b in base]

            async def _safe_send_json(ws, payload) -> bool:
                if (
                    getattr(ws, "closed", False)
                    or getattr(ws, "close_code", None) is not None
                ):
                    return False
                try:
                    await ws.send_json(payload)
                    return True
                except Exception as e:
                    if "closing transport" in str(e).lower():
                        return False
                    raise

            def mk_nonce(q: str) -> str:
                nonlocal nonce_seq
                nonce_seq += 1
                return f"s{session_index}-n{nonce_seq}:{q}"

            async def ensure_prefix_seeded():

                if small_guild:
                    await shared_planner.seed_top_level(letters_only + digits_punct)
                    await shared_planner.set_session_slots(num_sessions)
                    return

                await shared_planner.set_session_slots(num_sessions)
                mode = session_mode(num_sessions, try_total_hint)

                await shared_planner.seed_top_level(alphabet_l)

                roots = build_roots_for_mode(mode)
                await shared_planner.seed_two_gram_roots_once(roots)

            async def send_op8(ws, q: str, *, limit: int) -> str:
                if user_cancelled():
                    raise asyncio.CancelledError()
                if target_reached():
                    return ""
                n = mk_nonce(q)
                payload = {
                    "op": 8,
                    "d": {
                        "guild_id": str(guild.id),
                        "query": q,
                        "limit": limit,
                        "presences": False,
                        "nonce": n,
                    },
                }
                ok = await _safe_send_json(ws, payload)
                if not ok:
                    recycle_now.set()
                    raise RuntimeError("ws closing during send")
                in_flight_nonces.add(n)
                nonce_to_query[n] = q
                nonce_sent_at[n] = time.time()
                self.log.debug(
                    f"[DISPATCH][{ts()}] » S{session_index} Query {q if q else '∅'} → limit={limit} nonce={n}"
                )
                return n

            async def pump_more(ws, reason: str):
                nonlocal dispatched_since_connect, queries_total
                if user_cancelled() or target_reached():
                    return

                slots = parallel_limit - len(in_flight_nonces)
                if slots <= 0:
                    return

                started = 0

                if small_guild:

                    for ch in letter_priority:
                        if slots <= 0:
                            break
                        if not await claim_prefix(ch):
                            continue
                        try:
                            await send_op8(ws, ch, limit=100)
                            started += 1
                            dispatched_since_connect += 1
                            queries_total += 1
                            slots -= 1
                        except Exception as e:
                            self.log.warning(
                                f"[S{session_index}:op8] small-guild send failed ch={ch!r}: {e}"
                            )
                            await abort_claim(ch)
                            await shared_planner.requeue(ch)
                            break
                        await asyncio.sleep(0.05)

                    for ch in nonletter_priority:
                        if slots <= 0:
                            break
                        if not await claim_prefix(ch):
                            continue
                        try:
                            await send_op8(ws, ch, limit=100)
                            started += 1
                            dispatched_since_connect += 1
                            queries_total += 1
                            slots -= 1
                        except Exception as e:
                            self.log.warning(
                                f"[S{session_index}:op8] small-guild send failed ch={ch!r}: {e}"
                            )
                            await abort_claim(ch)
                            await shared_planner.requeue(ch)
                            break
                        await asyncio.sleep(0.05)

                    return

                coverage_ratio = {1: 0.20, 2: 0.33, 3: 0.50, 4: 0.67, 5: 0.75}.get(
                    num_sessions, 0.33
                )
                roots = await shared_planner.missing_roots()
                if roots:
                    cov_slots = max(1, min(slots, int(round(slots * coverage_ratio))))
                    for r in roots[:cov_slots]:
                        if not await claim_prefix(r):
                            continue
                        try:
                            await send_op8(ws, r, limit=100)
                            started += 1
                            dispatched_since_connect += 1
                            queries_total += 1
                        except Exception as e:
                            self.log.warning(
                                f"[S{session_index}:op8] coverage send failed r={r!r}: {e}"
                            )
                            await abort_claim(r)
                            await shared_planner.requeue(r)
                            break
                        await asyncio.sleep(0.06)

                remaining = slots - started

                if remaining > 0:

                    refill_threshold = max(48, 48 * (num_sessions**2))
                    refill_step = max(6, 6 + 2 * (num_sessions - 1))
                    await shared_planner.refill_if_starving(
                        threshold=refill_threshold, step=refill_step
                    )

                batch = (
                    await shared_planner.next_batch(remaining) if remaining > 0 else []
                )

                for q in batch:
                    if not await claim_prefix(q):
                        continue

                    if q != "" and len(q) < 2:
                        await shared_planner.on_chunk_result(q, 100)
                        pushed = await shared_planner.ensure_children(
                            q, force_full=True
                        )
                        if pushed:
                            self.log.debug("[expand:1-char] %s → +%d (full)", q, pushed)
                        await finalize_prefix(q)
                        continue

                    try:
                        await send_op8(ws, q, limit=100)
                        started += 1
                        dispatched_since_connect += 1
                        queries_total += 1
                    except Exception as e:
                        self.log.warning(
                            f"[S{session_index}:op8] send failed q={q!r}: {e}"
                        )
                        await abort_claim(q)
                        await shared_planner.requeue(q)
                        break
                    await asyncio.sleep(0.06)

                if started == 0 and not in_flight_nonces:
                    if (
                        strict_complete
                        and not await shared_planner.all_leaves_exhausted()
                    ):
                        missing_children = (
                            await shared_planner.sweep_full_children_for_saturated()
                        )
                        miss = await shared_planner.missing_roots()
                        for r in miss:
                            await shared_planner.requeue(r)
                        if missing_children or miss:
                            self.log.debug(
                                "[pump:last-resort] swept children=%d roots_missing=%d → retry dispatch",
                                missing_children,
                                len(miss),
                            )

                            batch2 = await shared_planner.next_batch(slots)
                            for q in batch2:
                                if not await claim_prefix(q):
                                    continue
                                try:
                                    await send_op8(ws, q, limit=100)
                                    started += 1
                                    dispatched_since_connect += 1
                                    queries_total += 1
                                except Exception:
                                    await abort_claim(q)
                                    await shared_planner.requeue(q)
                                    break
                                await asyncio.sleep(0.06)

            async def final_empty_sweep(ws) -> None:
                """Run a stronger final sweep with empty queries to catch stragglers."""
                start = time.time()
                async with final_sweep_lock:
                    if final_sweep_done.is_set():
                        return
                    self.log.debug("[final-sweep] starting last empty sweep window")
                    last_growth_at = time.time()
                    async with members_lock:
                        last_count = len(members)
                    while (time.time() - start) < final_sweep_seconds:
                        if user_cancelled() or target_reached():
                            break

                        for _ in range(max(1, int(final_sweep_burst))):
                            try:
                                await send_op8(ws, "", limit=100)
                            except Exception:
                                break
                            await asyncio.sleep(final_sweep_pause)

                        await asyncio.sleep(0.4)
                        async with members_lock:
                            cur = len(members)
                        if cur > last_count:
                            last_count = cur
                            last_growth_at = time.time()

                        if (
                            time.time() - last_growth_at
                        ) >= final_no_growth_seconds and (time.time() - start) > 3.0:
                            break
                    final_sweep_done.set()
                    self.log.debug(
                        "[final-sweep] done; duration=%.1fs", time.time() - start
                    )

            async def expiry_scavenger(ws):
                nonlocal last_empty_sweep_ts, target_count_cached, max_total_seen

                while True:
                    try:
                        if user_cancelled() or target_reached():
                            recycle_now.set()
                            return

                        await asyncio.sleep(0.5)
                        now = time.time()

                        idle = (not in_flight_nonces) and (
                            not await shared_planner.has_work()
                        )

                        if not in_flight_nonces and await shared_planner.has_work():
                            await pump_more(ws, reason="scavenge-has-work")
                            continue

                        if idle:

                            try_total = (
                                max_total_seen
                                if max_total_seen > 0
                                else (
                                    int(target_count_cached)
                                    if target_count_cached is not None
                                    else None
                                )
                            )
                            async with members_lock:
                                current = len(members)

                            small = small_guild

                            if (
                                strict_complete
                                and try_total
                                and current < try_total
                                and not small
                            ):
                                if (now - last_empty_sweep_ts) >= EMPTY_SWEEP_INTERVAL:
                                    for _ in range(EMPTY_SWEEP_BURST):
                                        try:
                                            await send_op8(ws, "", limit=100)
                                        except Exception:
                                            break
                                        await asyncio.sleep(0.15)
                                    last_empty_sweep_ts = time.time()

                            if not await shared_planner.all_leaves_exhausted():
                                missing_children = (
                                    await shared_planner.sweep_full_children_for_saturated()
                                )
                                miss = await shared_planner.missing_roots()
                                if not small:
                                    for r in miss:
                                        await shared_planner.requeue(r)
                                if (
                                    missing_children or (miss and not small)
                                ) and await shared_planner.has_work():
                                    await pump_more(ws, reason="scavenge-sweep")
                                    continue

                            if await shared_planner.all_leaves_exhausted():
                                if (try_total is not None) and (current < try_total):

                                    if not final_sweep_done.is_set() and not small:
                                        await final_empty_sweep(ws)

                                    if refresh_total_once:
                                        new_total = await refresh_guild_total()
                                        if new_total:
                                            n = int(new_total)
                                            if n > max_total_seen:
                                                max_total_seen = n
                                            if (target_count_cached is None) or (
                                                n > int(target_count_cached)
                                            ):
                                                target_count_cached = n

                                    async with members_lock:
                                        current = len(members)

                                self.log.debug(
                                    f"[S{session_index}] completion watcher → stopping: done={current} total={try_total} pq=0 inflight=0"
                                )
                                stop_event.set()
                                recycle_now.set()
                                return

                        if (time.time() - last_progress_at) > stall_timeout:
                            self.log.debug(
                                f"[S{session_index}] stall_timeout hit → recycle"
                            )
                            recycle_now.set()
                            return

                    except asyncio.CancelledError:
                        return
                    except Exception as e:

                        self.log.warning(f"[S{session_index}:scavenger] error: {e}")
                        await asyncio.sleep(0.5)
                        continue

            async def heartbeat(ws, interval_ms: int):
                try:
                    while True:
                        await asyncio.sleep((interval_ms or 41250) / 1000)
                        if (
                            getattr(ws, "closed", False)
                            or getattr(ws, "close_code", None) is not None
                        ):
                            self.log.debug("[ws] HEARTBEAT → socket closing; stopping")
                            return
                        try:
                            await ws.send_json({"op": 1, "d": int(time.time() * 1000)})
                            self.log.debug("[ws] HEARTBEAT → sent")
                        except Exception as e:
                            if (
                                getattr(ws, "closed", False)
                                or "closing transport" in str(e).lower()
                            ):
                                self.log.debug(
                                    "[ws] HEARTBEAT] → transport closing; stopping"
                                )
                                return
                            raise
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    self.log.warning(f"[ws] Heartbeat error: {e}")

            while True:
                if user_cancelled():
                    self.log.debug(f"[S{session_index}] user cancel → CancelledError")
                    raise asyncio.CancelledError()
                if target_reached():
                    self.log.debug(f"[S{session_index}] target reached → normal return")
                    return

                await ensure_prefix_seeded()

                heartbeat_task: Optional[asyncio.Task] = None
                scavenger_task: Optional[asyncio.Task] = None

                dispatched_since_connect = 0
                recycle_now.clear()
                last_progress_at = time.time()

                try:

                    self._fingerprint = None

                    async with session.ws_connect(
                        self.GATEWAY_URL,
                        heartbeat=None,
                        max_msg_size=0,
                        autoclose=True,
                        autoping=True,
                    ) as ws:
                        await gated_identify(ws)

                        while True:
                            if dispatched_since_connect >= recycle_after_dispatch:
                                self.log.debug(
                                    f"[S{session_index}] recycle_after_dispatch → reconnect"
                                )
                                recycle_now.set()

                            recv_task = asyncio.create_task(ws.receive())
                            rec_task = asyncio.create_task(recycle_now.wait())
                            stop_task = asyncio.create_task(stop_event.wait())
                            cnl_task = asyncio.create_task(self._cancel_event.wait())

                            done, pending = await asyncio.wait(
                                {recv_task, rec_task, stop_task, cnl_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            for p in pending:
                                p.cancel()

                            if rec_task in done:
                                self.log.debug(
                                    f"[S{session_index}:ws] recycle_now set → reconnect"
                                )
                                try:
                                    await ws.close(code=1000, message=b"recycle_stall")
                                except Exception:
                                    pass
                                break

                            if stop_task in done or target_reached():
                                self.log.debug(
                                    f"[S{session_index}:ws] target reached → close & return"
                                )
                                try:
                                    await ws.close(code=1000, message=b"target_reached")
                                except Exception:
                                    pass
                                return

                            if cnl_task in done or user_cancelled():
                                self.log.debug(
                                    f"[S{session_index}:ws] user cancel → close & CancelledError"
                                )
                                try:
                                    await ws.close(code=1000, message=b"user_cancel")
                                except Exception:
                                    pass
                                raise asyncio.CancelledError()

                            msg = recv_task.result()
                            if msg.type == aiohttp.WSMsgType.CLOSE:
                                code = ws.close_code
                                exc = ws.exception()
                                if code == 1006:
                                    self.log.debug(
                                        f"[S{session_index}:ws] CLOSE 1006 during shutdown; reconnecting"
                                    )
                                else:
                                    self.log.debug(
                                        f"[S{session_index}:ws] CLOSE code={code} exc={exc}"
                                    )
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue

                            raw = msg.data

                            try:
                                data_in = json.loads(raw)
                            except Exception as e:
                                self.log.warning(
                                    f"[S{session_index}:ws] JSON parse error: {e}"
                                )
                                continue

                            op = data_in.get("op")
                            t = data_in.get("t")
                            d = data_in.get("d")

                            if op == 10:
                                hb_ms = int((d or {}).get("heartbeat_interval", 41250))
                                if heartbeat_task:
                                    heartbeat_task.cancel()
                                heartbeat_task = asyncio.create_task(
                                    heartbeat(ws, hb_ms)
                                )
                                last_progress_at = time.time()

                                if scavenger_task:
                                    scavenger_task.cancel()
                                scavenger_task = asyncio.create_task(
                                    expiry_scavenger(ws)
                                )

                                await pump_more(ws, reason="hello")
                                continue

                            if op == 11:
                                await pump_more(ws, reason="ack")
                                continue

                            if op == 9:
                                self.log.warning(
                                    f"[S{session_index}:ws] INVALID_SESSION → backoff & reconnect"
                                )
                                await asyncio.sleep(
                                    1.0
                                    + (session_index * 0.5)
                                    + (random.random() * 1.5)
                                )
                                try:
                                    await ws.close(
                                        code=1000, message=b"invalid_session_reconnect"
                                    )
                                except Exception:
                                    pass
                                break

                            if op == 0:
                                if t == "READY":
                                    if not warmup_done.is_set():
                                        try:
                                            async with warmup_lock:
                                                if not warmup_done.is_set():

                                                    n = (
                                                        getattr(
                                                            guild, "member_count", None
                                                        )
                                                        or 0
                                                    )

                                                    if n and n <= 100:
                                                        await send_op8(
                                                            ws, "", limit=100
                                                        )
                                                        warmup_done.set()
                                                    else:
                                                        desired = (
                                                            4 if num_sessions > 1 else 2
                                                        )
                                                        can_send = max(
                                                            0,
                                                            parallel_limit
                                                            - len(in_flight_nonces),
                                                        )
                                                        burst = max(
                                                            1, min(desired, can_send)
                                                        )
                                                        for _ in range(burst):
                                                            await send_op8(
                                                                ws, "", limit=100
                                                            )
                                                            await asyncio.sleep(0.05)
                                                        warmup_done.set()
                                        except Exception:
                                            pass
                                    await asyncio.sleep(hello_ready_delay)
                                    await pump_more(ws, reason="ready")

                                elif t == "GUILD_CREATE":
                                    await pump_more(ws, reason="guild_create")

                                elif t == "GUILD_MEMBERS_CHUNK":
                                    nonce = (d or {}).get("nonce")
                                    got = (d or {}).get("members") or []

                                    if nonce and nonce in in_flight_nonces:
                                        in_flight_nonces.discard(nonce)
                                        q_for_nonce = nonce_to_query.pop(nonce, None)
                                        nonce_sent_at.pop(nonce, None)
                                    else:
                                        q_for_nonce = None

                                    added_here = 0
                                    if got:
                                        async with members_lock:
                                            for m in got:
                                                u = (m or {}).get("user") or {}
                                                uid = u.get("id")
                                                if not uid or uid in members:
                                                    continue
                                                rec = {
                                                    "id": uid,
                                                    "bot": bool(u.get("bot", False)),
                                                }
                                                if include_username:
                                                    rec["username"] = u.get("username")
                                                if include_avatar_url:
                                                    av = u.get("avatar")
                                                    rec["avatar_url"] = (
                                                        build_avatar_url(uid, av)
                                                    )
                                                if include_bio:
                                                    rec["bio"] = None
                                                    await bio_queue.put(uid)
                                                members[uid] = rec
                                                added_here += 1
                                        if added_here:
                                            last_progress_at = time.time()

                                    if got:
                                        for m in got:
                                            u = (m or {}).get("user") or {}
                                            ln_raw = u.get("username") or ""
                                            ln = ln_raw.casefold()
                                            if ln:
                                                await shared_planner.mark_observed_username(
                                                    ln
                                                )
                                            lead = ln[:1]
                                            if lead and lead.isdigit():
                                                await shared_planner.note_digit_lead()

                                            if lead and (lead not in base_alpha_set):
                                                if small_guild:

                                                    await shared_planner.push(lead)
                                                else:
                                                    await shared_planner.add_dynamic_lead(
                                                        lead
                                                    )
                                                dynamic_leads_added_global.add(lead)

                                    if q_for_nonce is not None:
                                        size_here = len(got)
                                        await shared_planner.on_chunk_result(
                                            q_for_nonce, size_here
                                        )
                                        await finalize_prefix(q_for_nonce)

                                        if size_here >= 100:
                                            expand_step = {
                                                1: 12,
                                                2: 10,
                                                3: 8,
                                                4: 6,
                                                5: 6,
                                            }.get(num_sessions, 8)
                                            pushed_now = (
                                                await shared_planner.ensure_children(
                                                    q_for_nonce, step=expand_step
                                                )
                                            )
                                            if pushed_now:
                                                self.log.debug(
                                                    "[expand] %s → +%d children (step=%d)",
                                                    q_for_nonce,
                                                    pushed_now,
                                                    expand_step,
                                                )

                                        total_now = len(members)
                                        worker = f"worker {session_index + 1}"
                                        self.log.debug(
                                            "[Copycord Scraper] %s » Query %s [+%d] Total=%d",
                                            worker,
                                            q_for_nonce if q_for_nonce else "∅",
                                            added_here,
                                            total_now,
                                        )

                                        if size_here > 0:
                                            queries_nonzero += 1
                                        if queries_total and (queries_total % 50) == 0:
                                            hr = (
                                                100.0
                                                * queries_nonzero
                                                / max(1, queries_total)
                                            )
                                            pq_len = await shared_planner.queue_len()
                                            self.log.debug(
                                                "[planner] hit_rate=%.1f%% total=%d nonzero=%d pq_len=%d",
                                                hr,
                                                queries_total,
                                                queries_nonzero,
                                                pq_len,
                                            )

                                        if (
                                            max_total_seen
                                            and total_now >= max_total_seen
                                        ) and not stop_event.is_set():
                                            self.log.debug(
                                                "[🎯][%s] %s » Count matched: %d/%d — stopping",
                                                ts(),
                                                worker,
                                                total_now,
                                                max_total_seen,
                                            )
                                            stop_event.set()
                                            recycle_now.set()

                                        if (
                                            (not small_guild)
                                            and q_for_nonce != ""
                                            and size_here > 0
                                        ):
                                            sib = next_sibling_prefix(
                                                q_for_nonce, alphabet_l
                                            )
                                            if sib:
                                                await shared_planner.push(sib)

                                    if (
                                        await shared_planner.has_work()
                                        or in_flight_nonces
                                    ):
                                        await pump_more(ws, reason="chunk")
                                    else:
                                        if (
                                            strict_complete
                                            and not await shared_planner.all_leaves_exhausted()
                                        ):

                                            missing_children = (
                                                await shared_planner.sweep_full_children_for_saturated()
                                            )

                                            if not small_guild:
                                                miss = (
                                                    await shared_planner.missing_roots()
                                                )
                                                for r in miss:
                                                    await shared_planner.requeue(r)
                                            else:
                                                miss = []

                                            if missing_children or miss:
                                                self.log.debug(
                                                    "[sweeper] children=%d roots_missing=%d",
                                                    missing_children,
                                                    len(miss),
                                                )
                                                await pump_more(ws, reason="sweeper")
                                                continue
                                        break

                                else:
                                    await pump_more(ws, reason=f"dispatch:{t}")

                except asyncio.CancelledError:
                    self._cancel_event.set()
                    self.log.debug(
                        f"[S{session_index}] session CancelledError propagated"
                    )
                    raise
                except Exception as e:
                    self.log.warning(f"[S{session_index}] WS session error: {e}")

                try:
                    pending_qs = []
                    for n in list(in_flight_nonces):
                        q = nonce_to_query.pop(n, None)
                        in_flight_nonces.discard(n)
                        nonce_sent_at.pop(n, None)
                        if q and q != "":
                            pending_qs.append(q)
                    for q in pending_qs:
                        await abort_claim(q)
                        await shared_planner.requeue(q)
                except Exception:
                    pass

                try:
                    if scavenger_task:
                        scavenger_task.cancel()
                    if heartbeat_task:
                        heartbeat_task.cancel()
                except Exception:
                    pass

                if not await shared_planner.has_work() and not in_flight_nonces:
                    if (
                        strict_complete
                        and not await shared_planner.all_leaves_exhausted()
                    ):

                        missing_children = (
                            await shared_planner.sweep_full_children_for_saturated()
                        )

                        if not small_guild:
                            miss = await shared_planner.missing_roots()
                            for r in miss:
                                await shared_planner.requeue(r)

                        if missing_children:
                            self.log.debug(
                                "[post-ws-sweep] children=%d → reconnect & continue",
                                missing_children,
                            )
                            continue
                    return
                continue

        bio_stop = None
        bio_worker_task = None
        try:
            tok = getattr(self.config, "CLIENT_TOKEN", None)
            if not tok:
                raise RuntimeError("CLIENT_TOKEN must be set for bio lookups")

            headers = self._build_headers(tok)

            self._progress_task = asyncio.create_task(_progress_reporter())

            async with aiohttp.ClientSession(headers=headers) as gw_session:
                async with aiohttp.ClientSession(headers=headers) as rest_session:
                    rest_session_handle = rest_session
                    bio_stop = asyncio.Event()
                    if include_bio:
                        bio_worker_task = asyncio.create_task(
                            bio_worker(rest_session, bio_stop)
                        )

                    await asyncio.gather(
                        *(run_session(i, gw_session) for i in range(num_sessions))
                    )

                    if include_bio:
                        try:
                            await bio_queue.join()
                        finally:
                            bio_stop.set()
                            if bio_worker_task:
                                await bio_worker_task

            if dynamic_leads_added_global:
                self.log.debug(
                    "[discover] %d dynamic leading characters discovered this run: %s",
                    len(dynamic_leads_added_global),
                    " ".join(repr(c) for c in sorted(dynamic_leads_added_global)),
                )

            if include_bio:
                total = len(self._members_ref or {})
                with_bio = sum(
                    1 for m in (self._members_ref or {}).values() if m.get("bio")
                )
                no_bio = total - with_bio
                failed = sum(
                    1
                    for s in bio_status.values()
                    if s.startswith("failed_") or s == "rate_limited"
                )
                self.log.debug(
                    "[Copycord Scraper] 📜 Bio scrape: has_bio=%d no_bio=%d failed=%d",
                    with_bio,
                    no_bio,
                    failed,
                )

            elapsed = time.perf_counter() - t0
            total_known = max_total_seen if max_total_seen > 0 else None
            if total_known:
                denom = max(1, total_known)
                pct_raw = (len(self._members_ref or {}) / denom) * 100.0
                pct = min(
                    pct_raw,
                    99.9 if len(self._members_ref or {}) < total_known else 100.0,
                )
                self.log.info(
                    "[Copycord Scraper] ✅ Finished in %s. Collected %d/%d members in %s (%.1f%%)",
                    _fmt_dur(elapsed),
                    len(self._members_ref or {}),
                    total_known,
                    gname,
                    pct,
                )
            else:
                self.log.info(
                    "[Copycord Scraper] ✅ Finished in %s. Collected %d members in %s",
                    _fmt_dur(elapsed),
                    len(self._members_ref or {}),
                    gname,
                )

            clean_members = []
            for m in (self._members_ref or {}).values():
                allowed = {"id", "bot"}
                if include_username:
                    allowed.add("username")
                if include_avatar_url:
                    allowed.add("avatar_url")
                if include_bio:
                    allowed.add("bio")

                filtered = {k: v for k, v in m.items() if k in allowed}

                if include_bio and "bio" not in filtered:
                    filtered["bio"] = None

                clean_members.append(filtered)

            return {
                "members": clean_members,
                "count": len(clean_members),
                "guild_id": str(guild.id),
                "guild_name": gname,
            }

        except asyncio.CancelledError:
            elapsed = time.perf_counter() - t0
            self.log.info(
                "[🛑] Scrape canceled early in %s after %s — collected: %d members",
                gname,
                _fmt_dur(elapsed),
                len(self._members_ref or {}),
            )
            self._cancel_event.set()
            raise
        finally:

            if self._progress_stop is None:
                self._progress_stop = asyncio.Event()
            self._progress_stop.set()
            if self._progress_task is not None:
                self._progress_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._progress_task
            self._progress_task = None
            self._progress_stop = None

            if bio_stop is not None:
                bio_stop.set()
            if bio_worker_task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await bio_worker_task
