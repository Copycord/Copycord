from __future__ import annotations
import asyncio
import json
import logging
import time
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
    Copycord server member scraper (prefix-walk via Gateway op:8).
    """

    GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"

    def __init__(
        self, bot: discord.Client, config: Any, logger: Optional[logging.Logger] = None
    ) -> None:
        self.bot = bot
        self.config = config
        self.log = (logger or logging.getLogger(__name__)).getChild("MemberScraper")
        self._cancel_event = asyncio.Event()
        self._members_ref: Optional[Dict[str, Dict[str, Any]]] = None
        self._members_lock_ref: Optional[asyncio.Lock] = None

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
        include_names: bool = True,
        *,
        alphabet: str = "abcdefghijklmnopqrstuvwxyz0123456789_- .!@#$%^&*()+={}[]|:;\"'<>,.?/~`",
        max_parallel_per_session: int = 1,
        hello_ready_delay: float = 2.5,
        inflight_timeout: float = 10.0,
        downstream_retries: int = 1,
        recycle_after_dispatch: int = 2000,
        stall_timeout: float = 120.0,
        num_sessions: int = 1,
    ) -> Dict[str, Any]:
        self._cancel_event = asyncio.Event()

        # resolve target guild
        guild_id = getattr(self.config, "HOST_GUILD_ID", None)
        guild = self.bot.get_guild(int(guild_id)) if guild_id else None
        if not guild:
            raise RuntimeError("No guild available for scrape")

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
            identify_d["intents"] = 1 << 0 | 1 << 1

        # shared result store
        members: Dict[str, Dict[str, Any]] = {}
        members_lock = asyncio.Lock()
        self._members_ref = members
        self._members_lock_ref = members_lock

        def shard_alphabet(alpha: str, k: int, n: int) -> str:
            return "".join(list(alpha)[k::n]) if n > 1 else alpha

        def should_stop() -> bool:
            return stop_event.is_set() or self._cancel_event.is_set()

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

        async def run_session(session_index: int) -> None:
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
                if should_stop():
                    raise asyncio.CancelledError()
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
                    f"[DISPATCH][{ts()}] » S{session_index} Query {q} → limit={limit} nonce={n}"
                )
                return n

            async def pump_more(ws, reason: str):
                nonlocal dispatched_since_connect
                if should_stop():
                    return
                started = 0
                while search_queue and len(in_flight_nonces) < max_parallel_per_session:
                    if should_stop():
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
                """Requeue long-stuck inflight requests; trigger recycle."""
                try:
                    while True:
                        if should_stop():
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

                if should_stop():
                    self.log.debug(
                        f"[S{session_index}] should_stop() at loop top → raise CancelledError"
                    )
                    raise asyncio.CancelledError()
                await ensure_prefix_seeded()

                if should_stop():
                    raise asyncio.CancelledError()
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

                                if (
                                    (stop_task in done)
                                    or (cnl_task in done)
                                    or should_stop()
                                ):
                                    self.log.debug(
                                        f"[S{session_index}:ws] cancel/stop detected → closing and raising CancelledError"
                                    )
                                    try:
                                        await ws.close(
                                            code=1000, message=b"target_or_cancel"
                                        )
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

                                    await pump_more(ws, reason="hello")
                                    continue

                                if op == 11:
                                    await pump_more(ws, reason="ack")
                                    continue

                                if op == 9:
                                    self.log.warning(
                                        f"[S{session_index}:ws] INVALID_SESSION → re-identify"
                                    )
                                    await asyncio.sleep(1.0)
                                    await ws.send_json({"op": 2, "d": identify_d})
                                    continue

                                if op == 0:
                                    if t == "READY":
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
                                                        "bot": bool(
                                                            u.get("bot", False)
                                                        ),
                                                    }
                                                    if include_names:
                                                        rec.update(
                                                            {
                                                                "username": u.get(
                                                                    "username"
                                                                ),
                                                                "discriminator": u.get(
                                                                    "discriminator"
                                                                ),
                                                                "avatar": u.get(
                                                                    "avatar"
                                                                ),
                                                                "joined_at": m.get(
                                                                    "joined_at"
                                                                ),
                                                            }
                                                        )
                                                    members[uid] = rec
                                                    added_here += 1
                                            if added_here:
                                                last_progress_at = time.time()

                                        if q_for_nonce is not None:
                                            total_now = len(members)
                                            worker = f"worker {session_index + 1}"
                                            self.log.info(
                                                "[Copycord Scraper Beta ✨] %s » Query %s [+%d] Total=%d",
                                                worker,
                                                q_for_nonce,
                                                added_here,
                                                total_now,
                                            )
                                            if (
                                                (target_count is not None)
                                                and (total_now >= int(target_count))
                                                and not stop_event.is_set()
                                            ):
                                                self.log.debug(
                                                    "[🎯][%s] %s » Reached guild.member_count: %d/%d — stopping",
                                                    ts(),
                                                    worker,
                                                    total_now,
                                                    int(target_count),
                                                )
                                                stop_event.set()
                                                recycle_now.set()

                                        if q_for_nonce is not None:
                                            if q_for_nonce not in visited_prefixes:
                                                visited_prefixes.add(q_for_nonce)

                                            if len(got) >= 100:
                                                prefix_len = len(q_for_nonce)
                                                next_letters = set()
                                                for m in got:
                                                    u = (m or {}).get("user") or {}
                                                    ln = (
                                                        u.get("username") or ""
                                                    ).lower()
                                                    if (
                                                        ln.startswith(q_for_nonce)
                                                        and len(ln) > prefix_len
                                                    ):
                                                        ch = ln[prefix_len]
                                                        if ch in alphabet_l:
                                                            next_letters.add(ch)
                                                for ch in alphabet_l:
                                                    if ch in next_letters:
                                                        child = q_for_nonce + ch
                                                        if (
                                                            child
                                                            not in visited_prefixes
                                                        ):
                                                            visited_prefixes.add(child)
                                                            search_queue.append(child)

                                            if len(q_for_nonce) > 1:
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
            self.log.info(
                f"[Copycord Scraper Beta ✨] Starting member scrape in {gname}"
            )
            await asyncio.gather(*(run_session(i) for i in range(num_sessions)))
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


class StreamManager:
    """
    Spools large JSON results to a temp .gz file and serves them in fixed-size chunks.
    """

    def __init__(self, logger=None, default_ttl_seconds: int = 3600):
        self._streams: Dict[str, Dict[str, Any]] = {}
        self._ttl = int(default_ttl_seconds)
        self._logger = logger

    def pack_json(
        self,
        result: Any,
        *,
        max_inline_bytes: int = 1_500_000,
        chunk_size: int = 512 * 1024,
        compresslevel: int = 6,
    ) -> Dict[str, Any]:
        """
        Return a response dict:
          {"ok": True, "data": <result>}                                  # small
          {"ok": True, "stream": {"id", "encoding", "size", "chunk_size"}} # large
        """
        try:
            raw = json.dumps(result, separators=(",", ":")).encode("utf-8")
        except Exception as e:
            return {"ok": False, "error": f"json-serialize-failed: {e!r}"}

        if len(raw) <= max_inline_bytes:
            return {"ok": True, "data": result}

        try:
            gz = gzip.compress(raw, compresslevel=compresslevel)
            tf = tempfile.NamedTemporaryFile(delete=False)
            try:
                tf.write(gz)
                tf.flush()
                size = tf.tell()
            finally:
                tf.close()
        except Exception as e:
            return {"ok": False, "error": f"spool-failed: {e!r}"}

        sid = str(uuid.uuid4())
        now = time.time()
        self._streams[sid] = {
            "path": tf.name,
            "size": size,
            "encoding": "json.gz",
            "created": now,
            "expires": now + self._ttl,
            "chunk_size": int(chunk_size),
        }
        if self._logger:
            self._logger.debug(
                "[📦] Spooled stream %s → %s (%d bytes)", sid, tf.name, size
            )

        return {
            "ok": True,
            "stream": {
                "id": sid,
                "encoding": "json.gz",
                "size": size,
                "chunk_size": int(chunk_size),
            },
        }

    def next(
        self, sid: str, offset: int = 0, length: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Return a chunk response dict:
          {"ok": True, "id", "offset", "next", "eof": False, "encoding", "data_b64"}
          {"ok": True, "id", "offset", "eof": True}  # also deletes the file/entry
        Errors:
          {"ok": False, "error": "..."}
        """
        meta = self._streams.get(sid)
        if not meta:
            return {"ok": False, "error": "stream-not-found"}

        if time.time() > meta["expires"]:
            self._cleanup(sid, meta)
            return {"ok": False, "error": "stream-expired"}

        size = int(meta["size"])
        offset = int(max(0, offset))
        if offset >= size:
            self._cleanup(sid, meta)
            return {"ok": True, "id": sid, "offset": offset, "eof": True}

        chunk_size = int(
            length if length is not None else meta.get("chunk_size", 512 * 1024)
        )
        chunk_size = max(1, min(chunk_size, size - offset))

        try:
            with open(meta["path"], "rb") as f:
                f.seek(offset)
                chunk = f.read(chunk_size)
        except Exception as e:
            return {"ok": False, "error": f"read-failed: {e!r}"}

        return {
            "ok": True,
            "id": sid,
            "offset": offset,
            "next": offset + len(chunk),
            "eof": False,
            "encoding": meta["encoding"],
            "data_b64": base64.b64encode(chunk).decode("ascii"),
        }

    def abort(self, sid: str) -> Dict[str, Any]:
        """Abort and cleanup a stream."""
        meta = self._streams.pop(sid, None)
        if not meta:
            return {"ok": True}  # idempotent
        self._unlink_silent(meta.get("path"))
        if self._logger:
            self._logger.debug("[🧹] Aborted stream %s", sid)
        return {"ok": True}

    def gc_expired(self, *, max_delete: int = 50) -> int:
        """
        Delete expired streams (best-effort). Returns number cleaned.
        Call periodically from a task, or just rely on lazy cleanup in .next().
        """
        now = time.time()
        cleaned = 0
        for sid, meta in list(self._streams.items()):
            if cleaned >= max_delete:
                break
            if now > float(meta.get("expires", 0)):
                self._cleanup(sid, meta)
                cleaned += 1
        return cleaned

    def _cleanup(self, sid: str, meta: Dict[str, Any]) -> None:
        self._streams.pop(sid, None)
        self._unlink_silent(meta.get("path"))
        if self._logger:
            self._logger.debug("[🧹] Cleaned stream %s", sid)

    @staticmethod
    def _unlink_silent(path: Optional[str]) -> None:
        if not path:
            return
        try:
            os.unlink(path)
        except Exception:
            pass
