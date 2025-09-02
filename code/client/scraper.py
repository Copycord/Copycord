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
from typing import Dict, Any, Optional
import aiohttp
import discord


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

    def _build_headers(self, token: str) -> dict:
        import base64, json, random

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

        client_version = random.choice(client_versions)
        chrome_version = random.choice(chrome_versions)
        electron_version = random.choice(electron_versions)
        os_version = random.choice(win_builds)

        locales = ["en-US", "en-GB", "de", "fr", "es-ES"]
        timezones = [
            "America/New_York",
            "America/Chicago",
            "Europe/Berlin",
            "Asia/Tokyo",
        ]
        locale = random.choice(locales)
        tz = random.choice(timezones)

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
            json.dumps(super_props, separators=(",", ":")).encode()
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

        gname = getattr(guild, "name", "UNKNOWN")
        self.log.debug(f"[guild] Target guild: {gname} ({guild.id})")

        target_count = getattr(guild, "member_count", None)
        stop_event = asyncio.Event()

        def ts() -> str:
            return time.strftime("%H:%M:%S", time.localtime())

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
        base_alpha_set = set(alphabet_l)
        dynamic_leads_added_global: set[str] = set()

        alpha_dynamic = set(alphabet_l)

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
        global_reset_at: float = 0.0

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

        bio_semaphore = asyncio.Semaphore(1)
        next_allowed_at: float = 0.0

        async def bio_worker(sess: aiohttp.ClientSession, stop_event: asyncio.Event):
            nonlocal global_reset_at, next_allowed_at, bio_processed
            max_retries_per_uid = 5
            retry_counts: dict[str, int] = {}

            async def rotate_session(
                old: aiohttp.ClientSession,
            ) -> aiohttp.ClientSession:
                """Tear down old session and build a new one with fresh headers/fingerprint."""
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
                                and self._cancel_event.is_set()
                            ):
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
                                    if uid in members:
                                        members[uid]["bio_error"] = "rate_limited"

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
                                    if uid in members:
                                        members[uid]["bio_error"] = "rate_limited"
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

                            nonlocal bio_processed
                            bio_processed += 1
                            remaining = bio_queue.qsize()
                            if bio_val:
                                self.log.info(
                                    "[Copycord Scraper] [📜] Found bio for user %s — %d checked %d remaining",
                                    uid,
                                    bio_processed,
                                    remaining,
                                )
                            else:
                                members[uid]["bio_error"] = "empty_or_hidden"
                                self.log.debug(
                                    "[Copycord Scraper] [📜] User %s bio empty — %d found %d remaining",
                                    uid,
                                    bio_processed,
                                    remaining,
                                )

                        success = True
                        break

                    if not success and last_status != 429:
                        async with members_lock:
                            if uid in members:
                                members[uid][
                                    "bio_error"
                                ] = f"failed_after_retries_status_{last_status}"

                    next_allowed_at = time.time() + 1.25 + random.uniform(0.1, 0.4)
                    bio_queue.task_done()
            finally:
                if not current_sess.closed:
                    self.log.debug("[Copycord Scraper] Closing bio session on exit")
                    await current_sess.close()

        async def run_session(
            session_index: int, session: aiohttp.ClientSession
        ) -> None:
            # This session's top-level shard
            top_level = shard_alphabet(alphabet_l, session_index, num_sessions)

            search_queue: deque[str] = deque()
            visited_prefixes = set()
            seeded_top = False

            dispatched_since_connect = 0
            last_progress_at = time.time()

            in_flight_nonces: set[str] = set()
            nonce_to_query: Dict[str, str] = {}
            nonce_sent_at: Dict[str, float] = {}
            query_retry_count: Dict[str, int] = {}
            nonce_seq = 0

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

                        search_queue.appendleft(q)
                        break

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

                            in_flight_nonces.discard(n)
                            nonce_to_query.pop(n, None)
                            nonce_sent_at.pop(n, None)

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
                    async with session.ws_connect(
                        self.GATEWAY_URL,
                        heartbeat=None,
                        max_msg_size=0,
                        autoclose=True,
                        autoping=True,
                    ) as ws:
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
                                continue

                            if op == 11:

                                await pump_more(ws, reason="ack")
                                continue

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
                                    break
                                continue

                            if op == 0:
                                if t == "READY":

                                    if not warmup_done.is_set():
                                        try:
                                            async with warmup_lock:
                                                if not warmup_done.is_set():
                                                    await send_op8(ws, "", limit=100)
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
                                                rec = {"id": uid}

                                                if include_username:
                                                    rec["username"] = u.get("username")

                                                if include_avatar_url:
                                                    rec["avatar_url"] = (
                                                        build_avatar_url(
                                                            uid, u.get("avatar")
                                                        )
                                                    )

                                                if include_bio:
                                                    await bio_queue.put(uid)

                                                members[uid] = rec
                                                added_here += 1
                                        if added_here:
                                            last_progress_at = time.time()

                                    if got:
                                        newly_seeded = 0
                                        newly_seen_this_chunk: list[str] = []
                                        for m in got:
                                            u = (m or {}).get("user") or {}
                                            ln = u.get("username") or ""
                                            if not ln:
                                                continue
                                            lead = ln[:1]

                                            if lead not in alpha_dynamic:
                                                alpha_dynamic.add(lead)

                                            if lead and (lead not in base_alpha_set):
                                                newly_seen_this_chunk.append(lead)

                                                if (
                                                    session_index == 0
                                                    and lead not in visited_prefixes
                                                ):
                                                    visited_prefixes.add(lead)
                                                    search_queue.append(lead)
                                                    newly_seeded += 1

                                        if newly_seen_this_chunk:
                                            uniq = sorted(set(newly_seen_this_chunk))
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

                                    if q_for_nonce is not None:
                                        total_now = len(members)
                                        worker = f"worker {session_index + 1}"
                                        self.log.info(
                                            "[Copycord Scraper] %s » Query %s [+%d] Total=%d",
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

                                    if q_for_nonce is not None and q_for_nonce != "":
                                        if len(got) >= 100:

                                            for ch in alphabet_l:
                                                child = q_for_nonce + ch
                                                if child not in visited_prefixes:
                                                    visited_prefixes.add(child)
                                                    search_queue.append(child)

                                        if len(q_for_nonce) > 0:
                                            sib = next_sibling_prefix(
                                                q_for_nonce, alphabet_l
                                            )
                                            if sib and sib not in visited_prefixes:
                                                visited_prefixes.add(sib)
                                                search_queue.append(sib)

                                    if search_queue or in_flight_nonces:
                                        await pump_more(ws, reason="chunk")
                                    else:

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
                    if scavenger_task:
                        scavenger_task.cancel()
                    if heartbeat_task:
                        heartbeat_task.cancel()
                except Exception:
                    pass

                if not search_queue and not in_flight_nonces:
                    return

                continue

        try:
            tok = getattr(self.config, "CLIENT_TOKEN", None)
            if not tok:
                raise RuntimeError("CLIENT_TOKEN must be set for bio lookups")

            headers = self._build_headers(tok)

            async with aiohttp.ClientSession(headers=headers) as gw_session:
                async with aiohttp.ClientSession(headers=headers) as rest_session:
                    bio_stop = asyncio.Event()
                    bio_worker_task = None

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
                self.log.info(
                    "[discover] %d dynamic leading characters discovered this run: %s",
                    len(dynamic_leads_added_global),
                    " ".join(repr(c) for c in sorted(dynamic_leads_added_global)),
                )

            if include_bio:
                total = len(members)
                with_bio = sum(1 for m in members.values() if "bio" in m and m["bio"])
                empty = sum(
                    1
                    for m in members.values()
                    if m.get("bio_error") == "empty_or_hidden"
                )
                failed = sum(
                    1
                    for m in members.values()
                    if m.get("bio_error", "").startswith("failed_after_retries")
                )
                self.log.info(
                    "[Copycord Scraper] 📜 Bio scrape: has_bio=%d no_bio=%d failed=%d",
                    with_bio,
                    empty,
                    failed,
                )

            self.log.info(
                f"[Copycord Scraper] ✅ Found {len(members)} members in {gname}"
            )
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
