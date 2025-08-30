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
from collections import deque
from typing import Dict, Any, Optional, List, Tuple
import base64
import gzip
import os
import tempfile
import uuid
import aiohttp
import discord


class MemberScraper:
    """
    Version: 0.1.0
    MemberScraper scrapes member information from a Discord guild
    using the Discord Gateway API. It provides functionality to collect and manage member
    data in a thread-safe manner, with support for dynamic discovery of usernames and
    on request cancellation.
    """

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

    # ---------------------------- public API ----------------------------

    def request_cancel(self) -> None:
        """Cooperative cancellation signal."""
        self._cancel_event.set()

    async def snapshot_members(self) -> list[dict]:
        """Return a thread-safe snapshot of members collected so far."""
        lock = self._members_lock_ref
        mem = self._members_ref
        if lock and mem is not None:
            async with lock:
                return list(mem.values())
        return []

    async def scrape(
        self,
        channel_id: int | str | None = None,  # kept for compatibility
        *,
        guild_id: int | str | None = None,
        include_username: bool = False,
        include_avatar_url: bool = False,
        include_bio: bool = False,
        alphabet: str = "abcdefghijklmnopqrstuvwxyz0123456789_- .!@#$%^&*()+={}[]|:;\"'<>,.?/~`",
        max_parallel_per_session: int = 1,
        hello_ready_delay: float = 2.5,
        inflight_timeout: float = 10.0,
        downstream_retries: int = 1,
        recycle_after_dispatch: int = 2000,
        stall_timeout: float = 120.0,
        num_sessions: int = 1,
        strict_complete: bool = False,
    ) -> Dict[str, Any]:
        # reset cancel flag for this run
        self._cancel_event = asyncio.Event()

        # ---------------------------- resolve guild ----------------------------
        gid_in = guild_id if guild_id is not None else getattr(self.config, "HOST_GUILD_ID", None)
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
        self.log.debug(f"[guild] Target guild: {gname} ({guild.id}) (source={'caller' if guild_id is not None else 'config'})")

        gname = getattr(guild, "name", "UNKNOWN")
        self.log.debug(f"[guild] Target guild: {gname} ({guild.id})")

        target_count = getattr(guild, "member_count", None)
        stop_event = asyncio.Event()

        def ts() -> str:
            return time.strftime("%H:%M:%S", time.localtime())

        # ---------------------------- alphabet prep ----------------------------
        def _dedup_chars(s: str) -> str:
            seen = set()
            out = []
            for ch in s or "":
                if ch not in seen:
                    seen.add(ch)
                    out.append(ch)
            return "".join(out)

        cfg_alpha = getattr(self.config, "SCRAPER_ALPHABET", None)
        ext = getattr(self.config, "EXTENDED_CHARS", "")
        alphabet_l = _dedup_chars((cfg_alpha or alphabet) + (ext or ""))
        base_alpha_set = set(alphabet_l)  # what you configured
        dynamic_leads_added_global: set[str] = (
            set()
        )  # discovered this run (not in base)

        # Dynamic alphabet (for discovery of non-ASCII lead chars)
        alpha_dynamic = set(alphabet_l)

        # ---------------------------- identify payload ----------------------------
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
            # GUILDS (1<<0) + GUILD_MEMBERS (1<<1) to make member chunks flow
            identify_d["intents"] = (1 << 0) | (1 << 1)
            
        def build_avatar_url(uid: str, avatar_hash: str | None) -> str | None:
            if not uid or not avatar_hash:
                return None
            ext = "gif" if str(avatar_hash).startswith("a_") else "png"
            return f"https://cdn.discordapp.com/avatars/{uid}/{avatar_hash}.{ext}?size=1024"

        # ---------------------------- shared stores ----------------------------
        members: Dict[str, Dict[str, Any]] = {}
        members_lock = asyncio.Lock()
        self._members_ref = members
        self._members_lock_ref = members_lock

        # Global warm-up guards (only once per whole run)
        warmup_lock = asyncio.Lock()
        warmup_done = asyncio.Event()
                
        bios_needed: set[str] = set() if include_bio else set()

        # ---------------------------- helpers ----------------------------
        def shard_alphabet(alpha: str, k: int, n: int) -> str:
            return "".join(list(alpha)[k::n]) if n > 1 else alpha

        def user_cancelled() -> bool:
            return self._cancel_event.is_set()

        def target_reached() -> bool:
            return stop_event.is_set()

        def next_sibling_prefix(q: str, alpha: str) -> Optional[str]:
            """Find the lexicographic next prefix at the same depth as q."""
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

        # ---------------------------- per-session worker ----------------------------
        async def run_session(session_index: int) -> None:
            # This session's top-level shard
            top_level = shard_alphabet(alphabet_l, session_index, num_sessions)

            # Per-session worklists
            search_queue: deque[str] = deque()
            visited_prefixes = set()
            seeded_top = False

            # Counters / timers
            dispatched_since_connect = 0
            last_progress_at = time.time()

            # In-flight tracking
            in_flight_nonces: set[str] = set()
            nonce_to_query: Dict[str, str] = {}
            nonce_sent_at: Dict[str, float] = {}
            query_retry_count: Dict[str, int] = {}
            nonce_seq = 0

            # Control signals
            recycle_now = asyncio.Event()

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
                """Seed the initial one-character top-level shard."""
                nonlocal seeded_top
                if seeded_top:
                    return
                for ch in top_level:
                    if ch not in visited_prefixes:
                        visited_prefixes.add(ch)
                        search_queue.append(ch)
                seeded_top = True
                self.log.debug(
                    f"[S{session_index}:prefix] seeded {len(search_queue)} top-level prefixes"
                )

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
                """Dispatch more op:8 requests, respecting per-session parallelism."""
                nonlocal dispatched_since_connect
                if user_cancelled() or target_reached():
                    return
                started = 0
                while search_queue and len(in_flight_nonces) < max_parallel_per_session:
                    if user_cancelled() or target_reached():
                        return
                    q = search_queue.popleft()
                    try:
                        await send_op8(ws, q, limit=100)
                        started += 1
                        dispatched_since_connect += 1
                    except Exception as e:
                        self.log.warning(
                            f"[S{session_index}:op8] send failed q={q!r}: {e}"
                        )
                        # put it back and try again after reconnect
                        search_queue.appendleft(q)
                        break
                    # Small pacing to avoid bursty 400s
                    await asyncio.sleep(0.06)
                if started:
                    self.log.debug(
                        f"[S{session_index}:pump] dispatched {started} (reason={reason}); "
                        f"inflight={len(in_flight_nonces)} qlen={len(search_queue)}"
                    )

            async def expiry_scavenger(ws):
                """Requeue long-stuck inflight requests; trigger recycle on stalls."""
                try:
                    while True:
                        if user_cancelled():
                            recycle_now.set()
                            return
                        if target_reached():
                            recycle_now.set()
                            return
                        await asyncio.sleep(0.5)
                        now = time.time()

                        timed_out = [
                            n
                            for n, ts_ in list(nonce_sent_at.items())
                            if now - ts_ > inflight_timeout
                        ]
                        for n in timed_out:
                            q = nonce_to_query.get(n)
                            if q is None:
                                continue
                            # remove from tracking
                            in_flight_nonces.discard(n)
                            nonce_to_query.pop(n, None)
                            nonce_sent_at.pop(n, None)

                            # Do NOT retry the global warm-up
                            if q == "":
                                self.log.debug(
                                    f"[S{session_index}:retry] skipping warm-up retry"
                                )
                                continue

                            rc = query_retry_count.get(q, 0)
                            if rc < downstream_retries + 1:
                                query_retry_count[q] = rc + 1
                                search_queue.appendleft(q)
                                self.log.debug(
                                    f"[S{session_index}:retry] timeout requeue q={q!r}"
                                )

                        if (now - last_progress_at) > stall_timeout:
                            self.log.debug(
                                f"[S{session_index}] stall_timeout hit → recycle"
                            )
                            recycle_now.set()
                            return
                except asyncio.CancelledError:
                    return

            async def heartbeat(ws, interval_ms: int):
                """Sends heartbeats; exits quietly if the socket is closing."""
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
                                    "[ws] HEARTBEAT → transport closing; stopping"
                                )
                                return
                            raise
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    self.log.warning(f"[ws] Heartbeat error: {e}")

            # ---------------------------- main WS loop ----------------------------
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

                headers = {"User-Agent": "DiscordBot"}
                try:
                    async with aiohttp.ClientSession(headers=headers) as session:
                        async with session.ws_connect(
                            self.GATEWAY_URL,
                            heartbeat=None,
                            max_msg_size=0,
                            autoclose=True,
                            autoping=True,
                        ) as ws:
                            # Identify
                            await ws.send_json({"op": 2, "d": identify_d})

                            while True:
                                if dispatched_since_connect >= recycle_after_dispatch:
                                    self.log.debug(
                                        f"[S{session_index}] recycle_after_dispatch → reconnect"
                                    )
                                    recycle_now.set()

                                recv_task = asyncio.create_task(ws.receive())
                                rec_task = asyncio.create_task(recycle_now.wait())
                                stop_task = asyncio.create_task(stop_event.wait())
                                cnl_task = asyncio.create_task(
                                    self._cancel_event.wait()
                                )

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
                                        await ws.close(
                                            code=1000, message=b"recycle_stall"
                                        )
                                    except Exception:
                                        pass
                                    break

                                if stop_task in done or target_reached():
                                    self.log.debug(f"[S{session_index}:ws] target reached → close & return")
                                    try:
                                        await ws.close(code=1000, message=b"target_reached")
                                    except Exception:
                                        pass
                                    return  # normal success exit for this session

                                if cnl_task in done or user_cancelled():
                                    self.log.debug(f"[S{session_index}:ws] user cancel → close & CancelledError")
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
                                        self.log.warning(
                                            f"[S{session_index}:ws] CLOSE code={code} exc={exc}"
                                        )
                                    break
                                if msg.type != aiohttp.WSMsgType.TEXT:
                                    continue

                                raw = msg.data
                                if len(raw) <= 4096:
                                    self.log.debug(f"[S{session_index}:ws] IN: {raw}")
                                else:
                                    self.log.debug(
                                        f"[S{session_index}:ws] IN: <{len(raw)} bytes>"
                                    )

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
                                    hb_ms = int(
                                        (d or {}).get("heartbeat_interval", 41250)
                                    )
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
                                    continue

                                if op == 11:
                                    # Heartbeat ACK — optionally pump, but READY is better
                                    await pump_more(ws, reason="ack")
                                    continue

                                # INVALID_SESSION - Back off with jitter, then re-identify (start a new session)
                                if op == 9:
                                    self.log.warning(
                                        f"[S{session_index}:ws] INVALID_SESSION → backoff & re-identify"
                                    )
                                    await asyncio.sleep(
                                        1.0
                                        + (session_index * 0.5)
                                        + (random.random() * 1.5)
                                    )
                                    try:
                                        await ws.send_json({"op": 2, "d": identify_d})
                                    except Exception:
                                        break  # force reconnect path
                                    continue

                                if op == 0:
                                    if t == "READY":
                                        # Global warm-up sample to discover non-ASCII leads (send once per entire run)
                                        if not warmup_done.is_set():
                                            try:
                                                async with warmup_lock:
                                                    if not warmup_done.is_set():
                                                        await send_op8(
                                                            ws, "", limit=100
                                                        )
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
                                            q_for_nonce = nonce_to_query.pop(
                                                nonce, None
                                            )
                                            nonce_sent_at.pop(nonce, None)
                                        else:
                                            q_for_nonce = None

                                        # ----- collect members -----
                                        added_here = 0
                                        if got:
                                            async with members_lock:
                                                for m in got:
                                                    u = (m or {}).get("user") or {}
                                                    uid = u.get("id")
                                                    if not uid or uid in members:
                                                        continue
                                                    rec = {"id": uid}

                                                    # Optional fields
                                                    if include_username:
                                                        rec["username"] = u.get("username")

                                                    if include_avatar_url:
                                                        rec["avatar_url"] = build_avatar_url(uid, u.get("avatar"))

                                                    # Optional: mark for later bio enrichment
                                                    if include_bio:
                                                        bios_needed.add(uid)

                                                    members[uid] = rec
                                                    added_here += 1
                                            if added_here:
                                                last_progress_at = time.time()

                                        # ----- dynamic lead-char discovery from *any* chunk -----
                                        if got:
                                            newly_seeded = 0
                                            newly_seen_this_chunk: list[str] = []
                                            for m in got:
                                                u = (m or {}).get("user") or {}
                                                ln = u.get("username") or ""
                                                if not ln:
                                                    continue
                                                lead = ln[
                                                    :1
                                                ]  # keep original glyph for seeding/logging

                                                # Session-scope learning to avoid repeated discovery work
                                                if lead not in alpha_dynamic:
                                                    alpha_dynamic.add(lead)

                                                # Only treat as "dynamic" if not in configured base alphabet
                                                if lead and (
                                                    lead not in base_alpha_set
                                                ):
                                                    newly_seen_this_chunk.append(lead)

                                                    # Seed top-level once (session 0 claims it to avoid dupes)
                                                    if (
                                                        session_index == 0
                                                        and lead not in visited_prefixes
                                                    ):
                                                        visited_prefixes.add(lead)
                                                        search_queue.append(lead)
                                                        newly_seeded += 1

                                            if newly_seen_this_chunk:
                                                uniq = sorted(
                                                    set(newly_seen_this_chunk)
                                                )
                                                dynamic_leads_added_global.update(uniq)
                                                self.log.info(
                                                    "[discover] S%d saw dynamic leads: %s",
                                                    session_index,
                                                    " ".join(repr(c) for c in uniq),
                                                )

                                            if newly_seeded:
                                                self.log.debug(
                                                    f"[S{session_index}:discover] +{newly_seeded} dynamic top-level leads"
                                                )

                                        # ----- logging & soft stop on target_count (non-strict) -----
                                        if q_for_nonce is not None:
                                            total_now = len(members)
                                            worker = f"worker {session_index + 1}"
                                            self.log.info(
                                                "[Copycord Scraper Beta ✨] %s » Query %s [+%d] Total=%d",
                                                worker,
                                                q_for_nonce if q_for_nonce else "∅",
                                                added_here,
                                                total_now,
                                            )
                                            if (
                                                not strict_complete
                                                and (target_count is not None)
                                                and (total_now >= int(target_count))
                                                and not stop_event.is_set()
                                            ):
                                                self.log.debug(
                                                    "[🎯][%s] %s » Reached guild.member_count: %d/%d — stopping (non-strict)",
                                                    ts(),
                                                    worker,
                                                    total_now,
                                                    int(target_count),
                                                )
                                                stop_event.set()
                                                recycle_now.set()

                                        # ----- EXPAND-ALL on saturation -----
                                        # NOTE: For the warm-up empty prefix (q == ""), we skip expansion to avoid
                                        # exploding the queue; it is used only for discovery + collection.
                                        if (
                                            q_for_nonce is not None
                                            and q_for_nonce != ""
                                        ):
                                            if len(got) >= 100:
                                                # Enqueue ALL children for this prefix to avoid missing rare branches
                                                for ch in alphabet_l:
                                                    child = q_for_nonce + ch
                                                    if child not in visited_prefixes:
                                                        visited_prefixes.add(child)
                                                        search_queue.append(child)

                                            # Opportunistic sibling walk (same depth)
                                            if len(q_for_nonce) > 0:
                                                sib = next_sibling_prefix(
                                                    q_for_nonce, alphabet_l
                                                )
                                                if sib and sib not in visited_prefixes:
                                                    visited_prefixes.add(sib)
                                                    search_queue.append(sib)

                                        # Continue pumping until both queues are empty
                                        if search_queue or in_flight_nonces:
                                            await pump_more(ws, reason="chunk")
                                        else:
                                            # Frontier exhausted on this connection
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

                # Cleanup background tasks
                try:
                    if scavenger_task:
                        scavenger_task.cancel()
                    if heartbeat_task:
                        heartbeat_task.cancel()
                except Exception:
                    pass

                # If no more work locally, end this session; otherwise reconnect and continue
                if not search_queue and not in_flight_nonces:
                    return

                continue  # reconnect loop

        # ---------------------------- run all sessions ----------------------------
        try:
            self.log.info(
                f"[Copycord Scraper Beta ✨] Starting member scrape in {gname}"
            )
            await asyncio.gather(*(run_session(i) for i in range(num_sessions)))
            if include_bio and bios_needed:
                conc = int(getattr(self.config, "BIO_FETCH_CONCURRENCY", 3))
                limit = int(getattr(self.config, "BIO_FETCH_LIMIT", 500))  # safety cap
                headers = {}
                tok = getattr(self.config, "CLIENT_TOKEN", None)
                if tok:
                    low = str(tok).lower()
                    if low.startswith("bot ") or low.startswith("bearer "):
                        headers["Authorization"] = tok
                    else:
                        # If the running identity is a bot, most tokens are raw; prefix with Bot
                        try:
                            is_bot = bool(getattr(getattr(self.bot, "user", None), "bot", False))
                        except Exception:
                            is_bot = False
                        headers["Authorization"] = f"Bot {tok}" if is_bot else tok

                async def fetch_bio_one(sess, uid: str) -> None:
                    url = f"https://discord.com/api/v10/users/{uid}/profile"
                    try:
                        async with sess.get(url, timeout=15) as r:
                            if r.status != 200:
                                return
                            j = await r.json()
                            bio_val = (j.get("user") or {}).get("bio") or j.get("bio")
                            if bio_val is not None:
                                async with members_lock:
                                    if uid in members:
                                        members[uid]["bio"] = bio_val
                    except Exception:
                        return  # swallow errors; keep best-effort

                try:
                    sem = asyncio.Semaphore(max(1, conc))
                    subset = list(bios_needed)[:max(0, limit)]
                    async with aiohttp.ClientSession(headers=headers) as sess_bio:
                        async def bound(u):
                            async with sem:
                                await fetch_bio_one(sess_bio, u)
                        await asyncio.gather(*(bound(u) for u in subset))
                except Exception:
                    pass
            if dynamic_leads_added_global:
                self.log.info(
                    "[discover] %d dynamic leading characters discovered this run: %s",
                    len(dynamic_leads_added_global),
                    " ".join(repr(c) for c in sorted(dynamic_leads_added_global)),
                )
            self.log.info(f"[✅] Found {len(members)} members in {gname}")
            return {
                "members": list(members.values()),
                "count": len(members),
                "guild_id": str(guild.id),
                "guild_name": gname,
            }
        except asyncio.CancelledError:
            self.log.info(
                f"[🛑] Scrape canceled early in {gname} — collected: {len(members)} members"
            )
            self._cancel_event.set()
            raise
