# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import asyncio
import contextlib
from dataclasses import dataclass, field
import logging
from collections import defaultdict
from datetime import datetime, timezone
import re
import time
import unicodedata
import uuid
import discord
from server.rate_limiter import RateLimitManager, ActionType

logger = logging.getLogger("server")


class BackfillManager:
    def __init__(self, receiver):
        self.r = receiver
        self.bot = receiver.bot
        self.ratelimit = RateLimitManager()

        self._flags: set[int] = set()
        self._progress: dict[int, dict] = {}
        self._inflight = defaultdict(int)
        self._by_clone: dict[int, int] = {}
        self._temps_cache: dict[int, list[str]] = {}
        self.semaphores: dict[int, asyncio.Semaphore] = {}
        self._temp_locks: dict[int, asyncio.Lock] = {}
        self._temp_ready: dict[int, asyncio.Event] = {}
        self._global_lock: asyncio.Lock = asyncio.Lock()
        self._rotate_pool: dict[int, list[str]] = {}
        self._rotate_idx: dict[int, int] = {}
        self._rot_locks: dict[int, asyncio.Lock] = {}
        self._attached: dict[int, set[asyncio.Task]] = defaultdict(set)
        self._global_sync: dict | None = None
        self._temp_prefix_canon = "Copycord"
        self._temp_prefix_key = re.sub(
            r"\s+",
            " ",
            unicodedata.normalize("NFKC", self._temp_prefix_canon).casefold(),
        ).strip()
        self.temp_webhook_max = (
            1  # temp webhooks allowed to be used to support primary webhook
        )


    def attach_task(self, original_id: int, task: asyncio.Task) -> None:
        """
        Track a task for cancellation and inflight accounting.
        """
        cid = int(original_id)
        self._inflight[cid] += 1
        self._attached[cid].add(task)

        def _done_cb(t: asyncio.Task, c=cid):
            self._attached[c].discard(t)
            self._on_task_done(c, t)

        task.add_done_callback(_done_cb)

    async def cancel_all_active(self) -> None:
        # collect every tracked task
        all_tasks = [t for tasks in self._attached.values() for t in list(tasks)]
        # cancel them
        for t in all_tasks:
            t.cancel()
        # let callbacks (_on_task_done) run; swallow exceptions (CancelledError, etc.)
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        # now it’s safe to clear trackers
        self._attached.clear()
        self._inflight.clear()

    def get_sent(self, channel_id: int) -> int:
        st = self._progress.get(int(channel_id)) or {}
        return int(st.get("delivered", 0))

    async def on_started(self, original_id: int, *, meta: dict | None = None) -> None:
        cid = int(original_id)
        self._flags.add(cid)

        st = self._progress.get(cid)
        if not st:
            self.register_sink(cid, user_id=None, clone_channel_id=None, msg=None)
            st = self._progress[cid]

        # keep whatever the server passed (e.g., {"range": {...}})
        if meta is not None:
            st["meta"] = meta

        loop = asyncio.get_event_loop()
        st.setdefault("started_at", loop.time())
        st.setdefault("started_dt", datetime.now(timezone.utc))
        st.setdefault("last_count", 0)
        st.setdefault("delivered", 0)
        st.setdefault("last_edit_ts", 0.0)
        st.setdefault("temp_webhook_ids", [])
        st.setdefault("temp_webhook_urls", [])
        st.setdefault("temp_created_ids", []) 

        clone_id = st.get("clone_channel_id")
        if clone_id:
            await self.ensure_temps_ready(int(clone_id))

    def mark_backfill(self, original_id: int) -> None:
        """
        Marks the given original ID as backfilled by adding it to the internal flags set.
        """
        self._flags.add(int(original_id))

    def is_backfilling(self, original_id: int) -> bool:
        """
        Checks if the given original ID is currently being backfilled.
        """
        return int(original_id) in self._flags

    def register_sink(
        self,
        channel_id: int,
        *,
        user_id: int | None,
        clone_channel_id: int | None,
        msg=None,
    ) -> None:
        now = asyncio.get_event_loop().time()
        self._progress[int(channel_id)] = {
            "user_id": user_id,
            "clone_channel_id": clone_channel_id,
            "msg": msg,
            "started_at": now,
            "started_dt": datetime.now(timezone.utc),
            "last_count": 0,
            "last_edit_ts": 0.0,
            "temp_webhook_ids": [],
            "temp_webhook_urls": [],
        }
        if clone_channel_id:
            self._by_clone[int(clone_channel_id)] = int(channel_id)

    async def clear_sink(self, channel_id: int) -> None:
        """
        Clears the progress sink for a specific channel.
        """
        self._progress.pop(int(channel_id), None)


    async def on_progress(self, original_id: int, count: int) -> None:
        st = self._progress.get(int(original_id))
        if not st:
            return

        prev = int(st.get("last_count", 0))
        st["last_count"] = max(prev, int(count))

        if st.get("msg"):
            now = asyncio.get_event_loop().time()
            if (now - st.get("last_edit_ts", 0.0) >= 2.0) or (count - prev >= 100):
                try:
                    elapsed = int(now - st["started_at"])
                    await st["msg"].edit(
                        content=f"📦 Backfilling… **{count}** messages (elapsed: {elapsed}s)"
                    )
                    st["last_edit_ts"] = now
                except Exception:
                    pass

    def note_sent(self, channel_id: int) -> None:
        """Increment server-side delivered count for a backfill message."""
        cid = int(channel_id)
        st = self._progress.get(cid)
        if not st:
            return
        st["delivered"] = int(st.get("delivered", 0)) + 1

    def update_expected_total(self, channel_id: int, total: int) -> None:
        """Set/raise expected total (from client precount)."""
        cid = int(channel_id)
        st = self._progress.get(cid)
        if not st:
            return
        prev = int(st.get("expected_total") or 0)
        st["expected_total"] = max(prev, int(total))
        logger.debug("[bf] set expected_total | channel=%s total=%s (prev=%s)",
                    cid, st["expected_total"], prev)

    def get_progress(self, channel_id: int) -> tuple[int, int | None]:
        """Return (delivered, total_or_None)."""
        st = self._progress.get(int(channel_id)) or {}
        delivered = int(st.get("delivered", 0))
        total = st.get("expected_total")
        try:
            total = int(total) if total is not None else None
        except Exception:
            total = None
        return delivered, total

    async def try_begin_global_sync(
        self, original_id: int, user_id: int
    ) -> tuple[bool, dict | None]:
        """
        Attempt to claim the global sync slot.
        Returns (ok, conflict_info). If ok=False, conflict_info has keys: original_id, user_id, started_at
        """
        async with self._global_lock:
            if self._global_sync is not None:
                return False, dict(self._global_sync)
            self._global_sync = {
                "original_id": int(original_id),
                "user_id": int(user_id),
                "started_at": asyncio.get_event_loop().time(),
            }
            return True, None

    async def end_global_sync(self, original_id: int) -> None:
        """Release the global sync slot if held for this channel."""
        async with self._global_lock:
            if self._global_sync and self._global_sync.get("original_id") == int(
                original_id
            ):
                self._global_sync = None

    def current_global_sync(self) -> dict | None:
        """Readonly peek at the current global sync info, or None."""
        return dict(self._global_sync) if self._global_sync else None

    async def on_done(self, original_id: int) -> None:
        """
        Handles the completion of a backfill operation for a specific channel.
        """
        cid = int(original_id)

        await self._wait_drain(
            cid,
            timeout=5.0 if getattr(self.r, "_shutting_down", False) else None
        )

        self._flags.discard(cid)
        await self.r._flush_channel_buffer(cid)

        st = self._progress.get(cid) or {}
        clone_id = st.get("clone_channel_id")
        delivered = int(st.get("delivered", 0))
        total = delivered
        started_at = st.get("started_at")
        elapsed_s = (
            int(asyncio.get_event_loop().time() - started_at) if started_at else 0
        )
        finished_dt = datetime.now(timezone.utc)

        shutting_down = getattr(self.r, "_shutting_down", False)
        suppress_dm = getattr(self.r, "_suppress_backfill_dm", False)

        uid = st.get("user_id")
        if uid and not (shutting_down or suppress_dm):
            ch_value = f"<#{clone_id}>" if clone_id else f"Original #{cid}"
            embed = discord.Embed(
                title="💬 Message History Clone Complete",
                description="Channel messages have been fully synced.",
                timestamp=finished_dt,
                color=discord.Color.green(),
            )
            embed.add_field(name="Channel", value=ch_value, inline=True)
            embed.add_field(
                name="Messages Cloned", value=f"`{str(total)}`", inline=True
            )
            embed.add_field(
                name="Duration", value=self._fmt_duration(elapsed_s), inline=True
            )
            try:
                user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                await user.send(embed=embed)
                logger.info(
                    "[📨] DM’d backfill summary to user %s for channel %s", uid, cid
                )
            except Exception as e:
                (logger.warning if not shutting_down else logger.debug)(
                    "[⚠️] Could not DM user %s: %s", uid, e
                )
                
        if clone_id and not shutting_down:
            # delete only the temps CREATED during this run in this channel
            try:
                stats = await self.delete_created_temps_for(int(clone_id))
                logger.debug("[🧹] Deleted %d temp webhooks in #%s (created this run)", stats.get("deleted", 0), clone_id)
            except Exception:
                logger.debug("[cleanup] temp deletion failed for #%s", clone_id, exc_info=True)

        if clone_id:
            self._rotate_pool.pop(int(clone_id), None)
            self._rotate_idx.pop(int(clone_id), None)
            self._by_clone.pop(int(clone_id), None)

        await self.clear_sink(cid)
        await self.end_global_sync(cid)
        await self._wait_drain(cid, timeout=5.0 if getattr(self.r, "_shutting_down", False) else None)

        if not shutting_down:
            logger.debug(
                "[📦] Backfill finished for #%s; buffered messages flushed", cid
            )
        else:
            logger.debug(
                "[📦] Backfill finished for #%s during shutdown; skipped DM/cleanup",
                cid,
            )

    async def _clear_sink(self, channel_id: int, *, send_dm: bool, quiet: bool):
        st = self._progress.get(channel_id)
        if not st:
            return

        if send_dm and not getattr(self.r, "_shutting_down", False):
            try:
                user = self.r.bot.get_user(
                    st["user_id"]
                ) or await self.r.bot.fetch_user(st["user_id"])
                if st.get("final_embed"):
                    await user.send(embed=st["final_embed"])
                    logger.info(
                        "[📨] DM’d backfill summary to user %s for channel %s",
                        st["user_id"],
                        channel_id,
                    )
            except Exception as e:
                logger.warning("[⚠️] Could not DM user %s: %s", st.get("user_id"), e)

        self._progress.pop(channel_id, None)

    def _on_task_done(self, cid: int, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            logger.debug("[bf] task for #%s cancelled", cid)
        except Exception as e:
            logger.exception("[bf] task error for #%s: %s", cid, e)
        finally:
            n = self._inflight.get(cid, 0)
            self._inflight[cid] = max(0, n - 1)
            if self._inflight[cid] == 0:
                self._inflight.pop(cid, None)

    async def _wait_drain(self, cid: int, timeout: float | None = 30.0) -> None:
        """
        Waits for the in-flight operations for a given connection ID (cid) to drain
        (i.e., reach zero) within an optional timeout period.
        """
        start = asyncio.get_event_loop().time()
        while self._inflight.get(cid, 0) > 0:
            if (
                timeout is not None
                and (asyncio.get_event_loop().time() - start) > timeout
            ):
                logger.warning(
                    "[📦] Backfill drain timed out for #%s with %d in-flight",
                    cid,
                    self._inflight.get(cid, 0),
                )
                break
            await asyncio.sleep(0.05)

    def unmark_backfill(self, original_id: int) -> None:
        """
        Removes the specified ID from the backfill flags.
        """
        self._flags.discard(int(original_id))

    def choose_url(self, clone_channel_id: int, primary_url: str) -> str:
        """
        Rotate among: [primary webhook] + up to N temp webhooks, where N=self.temp_webhook_max.
        """
        pool = self._rotate_pool.get(clone_channel_id)
        if not pool:
            sink_key = self._by_clone.get(clone_channel_id)
            temps: list[str] = []
            if sink_key is not None:
                st = self._progress.get(sink_key) or {}
                temps = list(st.get("temp_webhook_urls") or [])
            N = max(0, int(self.temp_webhook_max))
            if not temps or N == 0:
                return primary_url
            pool = [primary_url] + temps[:N]
            self._rotate_pool[clone_channel_id] = pool
            self._rotate_idx[clone_channel_id] = -1

        idx = (self._rotate_idx.get(clone_channel_id, -1) + 1) % len(pool)
        self._rotate_idx[clone_channel_id] = idx
        return pool[idx]

    @staticmethod
    def _fmt_duration(seconds: int) -> str:
        """
        Format a duration given in seconds into a human-readable string.
        """
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        if h:
            return f"{h}h {m}m {s}s"
        if m:
            return f"{m}m {s}s"
        return f"{s}s"

    async def _ensure_temp_webhooks(self, clone_channel_id: int) -> tuple[list[int], list[str]]:
        """
        Ensure there are exactly N temp webhooks for rotation in this channel.

        Strategy (efficient):
        - Never rename/edit temp webhooks to mirror the primary.
        - Create temps with the canonical name only (e.g., "Copycord").
        - At send-time, if the PRIMARY webhook is customized, override per-message
        username/avatar_url to match the PRIMARY (no avatar uploads or edits).
        - Track and persist:
            * temp_webhook_ids / temp_webhook_urls (current rotation set)
            * temp_created_ids (only the IDs we created this run)
            * primary_identity {"name","avatar_url"} for send-time override

        Returns:
            (ids, urls) of the temps in ascending webhook-id order (capped at N)
        """
        try:
            # Resolve channel
            ch = self.bot.get_channel(clone_channel_id) or await self.bot.fetch_channel(clone_channel_id)
            if not ch:
                logger.debug("[temps] Channel not found for #%s", clone_channel_id)
                return [], []

            # Map clone -> original, and find the primary webhook URL for exclusion
            orig = self._by_clone.get(clone_channel_id)
            row = self.r.chan_map.get(orig) or {}
            primary_url = row.get("channel_webhook_url") or row.get("webhook_url")

            st = self._progress.get(orig) if orig is not None else None
            N = max(0, int(getattr(self, "temp_webhook_max", 1)))

            # Determine primary identity (for send-time override). No edits/uploads here.
            primary_name = None
            primary_avatar_url = None
            customized = False
            if primary_url and orig is not None:
                try:
                    wid = int(primary_url.rstrip("/").split("/")[-2])
                    whp = await self.bot.fetch_webhook(wid)
                    canonical = self._canonical_temp_name()
                    primary_name = (whp.name or "").strip() or None
                    customized = bool(primary_name and primary_name != canonical)

                    # Try to grab a CDN URL for the avatar if present
                    av_asset = getattr(whp, "avatar", None)
                    if av_asset:
                        try:
                            primary_avatar_url = str(getattr(av_asset, "url", None)) or None
                        except Exception:
                            primary_avatar_url = None
                except Exception as e:
                    logger.debug("[temps] fetch primary webhook failed for %s: %s", primary_url, e)

            if st is not None:
                st.setdefault("primary_identity", {})
                st["primary_identity"] = {"name": primary_name, "avatar_url": primary_avatar_url}

            # Helper: fetch webhook object from URL
            async def _fetch_wh(url: str):
                try:
                    wid = int(url.rstrip("/").split("/")[-2])
                    return await self.bot.fetch_webhook(wid)
                except Exception:
                    return None

            # Build initial set
            seen_urls: set[str] = set()
            pairs: list[tuple[int, str]] = []  # (id, url)

            # Track the ones WE created now (for auto-delete later)
            created_ids: list[int] = []
            if st is not None:
                created_ids = list(st.get("temp_created_ids") or [])

            async def _adopt(url: str):
                if not url:
                    return
                if primary_url and url == primary_url:
                    return
                if url in seen_urls:
                    return
                wh = await _fetch_wh(url)
                if wh is not None:
                    seen_urls.add(url)
                    pairs.append((int(wh.id), url))

            # Seed from cache first
            if st:
                for u in list(st.get("temp_webhook_urls") or []):
                    await _adopt(u)

            # Adopt any existing bot-created webhooks in the channel (name-agnostic), excluding primary
            try:
                hooks = await ch.webhooks()
                for wh in hooks:
                    try:
                        made_by_us = (getattr(wh, "user", None) and getattr(wh.user, "id", None) == self.bot.user.id)
                    except Exception:
                        made_by_us = False
                    if made_by_us and (not primary_url or wh.url != primary_url):
                        await _adopt(wh.url)
            except Exception as e:
                # best-effort; ignore list failures
                logger.debug("[temps] list hooks failed for #%s: %s", clone_channel_id, e)

            # If N == 0, keep no temps (but keep adoption list untouched for visibility)
            if N <= 0:
                ids, urls = [], []
                if st is not None:
                    st["temp_webhook_ids"] = ids
                    st["temp_webhook_urls"] = urls
                    st["temp_created_ids"] = []
                # reset rotation so future calls rebuild from live state
                self._rotate_pool.pop(clone_channel_id, None)
                self._rotate_idx.pop(clone_channel_id, None)
                return ids, urls

            # Create missing temps to reach N; canonical name only; no avatar uploads
            while len(pairs) < N:
                await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)
                wh_new = await ch.create_webhook(
                    name=self._canonical_temp_name(),
                    reason="Backfill rotation",
                )
                pairs.append((int(wh_new.id), wh_new.url))
                seen_urls.add(wh_new.url)
                created_ids.append(int(wh_new.id))

            # Normalize order & cap to N
            pairs.sort(key=lambda t: t[0])
            pairs = pairs[:N]

            ids = [p[0] for p in pairs]
            urls = [p[1] for p in pairs]

            # Persist into sink for future calls
            if st is not None:
                st["temp_webhook_ids"] = list(ids)
                st["temp_webhook_urls"] = list(urls)
                # keep only the created ids that are still in use
                st["temp_created_ids"] = [i for i in created_ids if i in ids]

            # Reset rotation pool so next pick rebuilds from these
            self._rotate_pool.pop(clone_channel_id, None)
            self._rotate_idx.pop(clone_channel_id, None)
            return ids, urls

        except Exception as e:
            logger.warning("[⚠️] Could not ensure temp webhooks in #%s: %s", clone_channel_id, e)
            return [], []

    async def _list_temp_webhook_urls(self, clone_channel_id: int) -> list[str]:
        """
        Return temp webhook URLs for rotation. Prefer the backfill sink cache.
        If cache is empty, fall back to: all webhooks in channel created by THIS bot and != primary.
        (No name checks – temps may mirror the primary's name.)
        """
        try:
            sink_key = self._by_clone.get(clone_channel_id)
            if sink_key is not None:
                st = self._progress.get(sink_key) or {}
                urls = list(st.get("temp_webhook_urls") or [])
                if urls:
                    pairs = [(int(u.rstrip("/").split("/")[-2]), u) for u in urls]
                    pairs.sort(key=lambda t: t[0])
                    return [u for (_id, u) in pairs]

            # fallback: scan channel
            ch = self.bot.get_channel(clone_channel_id) or await self.bot.fetch_channel(clone_channel_id)
            hooks = await ch.webhooks()

            # primary url for exclusion
            primary_url = None
            orig = self._by_clone.get(clone_channel_id)
            if orig is not None:
                row = self.r.chan_map.get(orig) or {}
                primary_url = row.get("channel_webhook_url")

            pairs = []
            for wh in hooks:
                try:
                    # prefer "created by this bot" as a reliable temp signal
                    made_by_us = (getattr(wh, "user", None) and getattr(wh.user, "id", None) == self.bot.user.id)
                except Exception:
                    made_by_us = False
                if made_by_us and (not primary_url or wh.url != primary_url):
                    pairs.append((int(wh.id), wh.url))

            pairs.sort(key=lambda t: t[0])
            return [u for (_id, u) in pairs]
        except Exception as e:
            logger.debug("[temps] list failed for #%s: %s", clone_channel_id, e)
            return []


    async def pick_url_for_send(
        self, clone_channel_id: int, primary_url: str, create_missing: bool
    ):
        lock = self._rot_locks.setdefault(clone_channel_id, asyncio.Lock())
        async with lock:
            pool = self._rotate_pool.get(clone_channel_id)
            if pool is None:
                temps = (
                    (await self._ensure_temp_webhooks(clone_channel_id))[1]
                    if create_missing
                    else await self._list_temp_webhook_urls(clone_channel_id)
                )

                temps = [u for u in temps if u != primary_url]

                if not temps:
                    return primary_url, False

                N = max(0, int(self.temp_webhook_max))
                pool = [primary_url] + temps[:N]
                self._rotate_pool[clone_channel_id] = pool
                self._rotate_idx.setdefault(clone_channel_id, -1)

            idx = (self._rotate_idx.get(clone_channel_id, -1) + 1) % len(pool)
            self._rotate_idx[clone_channel_id] = idx
            return pool[idx], True

    async def ensure_temps_ready(self, clone_id: int):
        ev = self._temp_ready.setdefault(clone_id, asyncio.Event())
        if ev.is_set():
            return
        lock = self._temp_locks.setdefault(clone_id, asyncio.Lock())
        async with lock:
            if ev.is_set():
                return
            ids, urls = await self._ensure_temp_webhooks(clone_id)
            sink_key = self._by_clone.get(clone_id)
            if sink_key is not None:
                st = self._progress.get(sink_key) or {}
                st["temp_webhook_ids"] = ids
                st["temp_webhook_urls"] = urls
            ev.set()

    def _is_temp_name(self, name: str | None) -> bool:
        if not name:
            return False
        key = re.sub(
            r"\s+", " ", unicodedata.normalize("NFKC", name).casefold()
        ).strip()
        return key == self._temp_prefix_key

    def _canonical_temp_name(self) -> str:
        return self._temp_prefix_canon

    def invalidate_rotation(self, clone_channel_id: int) -> None:
        """Drop any cached rotation info so the next send rebuilds from live webhooks."""
        self._rotate_pool.pop(int(clone_channel_id), None)
        self._rotate_idx.pop(int(clone_channel_id), None)
        self._temps_cache.pop(int(clone_channel_id), None)
        if hasattr(self, "_temp_ready"):
            self._temp_ready.pop(int(clone_channel_id), None)
        logger.debug("[rotate] invalidated pool for #%s", clone_channel_id)
        
    async def delete_created_temps_for(self, clone_channel_id: int, *, dry_run: bool = False) -> dict:
        """
        Delete ONLY temp webhooks that were CREATED during the current/last backfill run
        for this clone channel. Safe: never touches the primary or adopted hooks.

        Returns: {"deleted": int}
        """
        stats = {"deleted": 0}
        try:
            # find original id → sink
            orig = self._by_clone.get(int(clone_channel_id))
            st = self._progress.get(int(orig)) if orig is not None else None
            if not st:
                return stats

            row = self.r.chan_map.get(int(orig)) or {}
            primary_url = row.get("channel_webhook_url") or row.get("webhook_url")
            primary_id = None
            if primary_url:
                with contextlib.suppress(Exception):
                    primary_id = int(primary_url.rstrip("/").split("/")[-2])

            created_ids = list(st.get("temp_created_ids") or [])

            for wid in created_ids:
                if primary_id and int(wid) == int(primary_id):
                    continue
                try:
                    await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)  # reuse bucket
                    if dry_run:
                        logger.info("[🧹 DRY RUN] Would delete temp webhook %s in #%s", wid, clone_channel_id)
                    else:
                        wh = await self.bot.fetch_webhook(int(wid))
                        await wh.delete(reason="Backfill complete: remove temp webhook")
                        stats["deleted"] += 1
                        logger.debug("[🧹] Deleted temp webhook %s in #%s", wid, clone_channel_id)
                except Exception as e:
                    logger.debug("[cleanup] Could not delete webhook %s in #%s: %s", wid, clone_channel_id, e)

            # clear temp fields so we don't try again
            st["temp_created_ids"] = []
            st["temp_webhook_ids"] = []
            st["temp_webhook_urls"] = []
            # drop rotation cache for safety
            self.invalidate_rotation(int(clone_channel_id))
        except Exception:
            logger.debug("[cleanup] delete_created_temps_for failed for #%s", clone_channel_id, exc_info=True)
        return stats

    async def cleanup_non_primary_webhooks(
        self,
        *,
        channel_ids: list[int] | None = None,
        only_ours: bool = True,
        dry_run: bool = False,
    ) -> dict:
        """
        Delete any webhooks in clone channels that are NOT the mapped primary webhook.
        - only_ours=True: only delete webhooks created by this bot (safer default)
        - channel_ids: limit to specific clone channel ids; default = all clones in chan_map
        - dry_run=True: log what would be deleted, but don't delete

        Returns: {"channels_checked": int, "deleted": int, "skipped_no_primary": int, "skipped_missing_channel": int}
        """
        stats = {
            "channels_checked": 0,
            "deleted": 0,
            "skipped_no_primary": 0,
            "skipped_missing_channel": 0,
        }
        try:
            if getattr(self.r, "_shutting_down", False):
                logger.debug("[cleanup] Skipping cleanup: shutting down")
                return stats

            # ensure mappings exist
            with contextlib.suppress(Exception):
                self.r._load_mappings()

            # build clone_channel_id -> primary_webhook_id
            def _wid_from_url(u: str | None) -> int | None:
                if not u:
                    return None
                try:
                    return int(u.rstrip("/").split("/")[-2])
                except Exception:
                    return None

            clone_to_primary: dict[int, int] = {}
            for row in (self.r.chan_map or {}).values():
                try:
                    clone_id = int(row.get("cloned_channel_id") or 0)
                except Exception:
                    clone_id = 0
                if not clone_id:
                    continue
                wid = _wid_from_url(row.get("channel_webhook_url") or row.get("webhook_url"))
                if wid:
                    clone_to_primary[clone_id] = wid

            targets = list(clone_to_primary.keys())
            if channel_ids:
                targets = [int(x) for x in channel_ids if int(x) in clone_to_primary]

            if not targets:
                logger.debug("[cleanup] No clone channels to check; nothing to do")
                return stats

            # Iterate each target channel; skip cleanly if channel is missing/inaccessible
            for clone_id in targets:
                if getattr(self.r, "_shutting_down", False):
                    break

                ch = self.bot.get_channel(int(clone_id))
                if not ch:
                    # Fetch can raise when the channel doesn't exist or is in another guild / no perms
                    try:
                        ch = await self.bot.fetch_channel(int(clone_id))
                    except Exception as e:
                        stats["skipped_missing_channel"] += 1
                        logger.debug(
                            "[cleanup] Clone channel #%s not found/inaccessible (%s); skipping",
                            clone_id, e,
                        )
                        continue

                if not ch:
                    stats["skipped_missing_channel"] += 1
                    logger.debug("[cleanup] Clone channel #%s not found; skipping", clone_id)
                    continue

                stats["channels_checked"] += 1
                primary_id = int(clone_to_primary.get(clone_id) or 0)
                if not primary_id:
                    stats["skipped_no_primary"] += 1
                    logger.debug("[cleanup] #%s has no primary webhook id; skipping", clone_id)
                    continue

                try:
                    hooks = await ch.webhooks()
                except Exception as e:
                    # If guild has zero channels or we lack perms, listing can fail — skip this channel only
                    stats["skipped_missing_channel"] += 1
                    logger.debug("[cleanup] Could not list webhooks for #%s: %s", clone_id, e)
                    continue

                for wh in hooks:
                    try:
                        if int(wh.id) == primary_id:
                            continue
                    except Exception:
                        continue

                    if only_ours:
                        try:
                            made_by_us = (
                                getattr(wh, "user", None)
                                and getattr(wh.user, "id", None) == self.bot.user.id
                            )
                        except Exception:
                            made_by_us = False
                        if not made_by_us:
                            continue

                    if dry_run:
                        logger.info(
                            "[🧹 DRY RUN] Would delete non-primary webhook %s in #%s (name=%r)",
                            wh.id, clone_id, wh.name
                        )
                        continue

                    try:
                        await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)
                        await wh.delete(reason="Cleanup: remove non-primary webhook in clone channel")
                        stats["deleted"] += 1
                        logger.info(
                            "[🧹] Deleted non-primary webhook %s in #%s (name=%r)",
                            wh.id, clone_id, wh.name
                        )
                    except Exception as e:
                        logger.warning(
                            "[⚠️] Failed to delete webhook %s in #%s: %s", wh.id, clone_id, e
                        )

                await asyncio.sleep(0)  # be nice to the loop

            logger.debug(
                "[🧹] Cleanup complete: checked=%d deleted=%d skipped_no_primary=%d skipped_missing_channel=%d",
                stats["channels_checked"], stats["deleted"], stats["skipped_no_primary"], stats["skipped_missing_channel"]
            )
        except Exception:
            logger.exception("[cleanup] Unexpected error while deleting non-primary webhooks")
        return stats



@dataclass
class BackfillTask:
    id: str
    channel_id: str
    started_at: float = field(default_factory=time.time)
    processed: int = 0        # messages the SERVER has actually applied
    in_flight: int = 0        # batches accepted for apply but not completed yet
    client_done: bool = False # client finished sending stream

class BackfillTracker:
    def __init__(self, bus):
        self._bus = bus
        self._by_channel: dict[str, BackfillTask] = {}
        self._lock = asyncio.Lock()

    async def start(self, channel_id: str, meta: dict) -> BackfillTask | None:
        async with self._lock:
            if channel_id in self._by_channel:
                # already running – tell the UI via ACK (handled in caller)
                return None
            t = BackfillTask(id=str(uuid.uuid4()), channel_id=channel_id)
            self._by_channel[channel_id] = t
        # Let the UI know the server accepted and actually started
        await self._bus.publish("client", "server", {
            "type": "backfill_started",
            "channel_id": channel_id,
            "task_id": t.id,
            **(meta or {}),
        })
        return t

    async def mark_client_stream_end(self, channel_id: str):
        # Client finished sending; we will only declare DONE when in_flight==0
        done_payload = None
        async with self._lock:
            t = self._by_channel.get(channel_id)
            if not t:
                return
            t.client_done = True
            if t.in_flight <= 0:
                done_payload = {
                    "type": "backfill_done",
                    "channel_id": channel_id,
                    "task_id": t.id,
                    "processed": t.processed,
                    "started_at": t.started_at,
                    "finished_at": time.time(),
                }
                self._by_channel.pop(channel_id, None)
        if done_payload:
            await self._bus.publish("client", "server", done_payload)

    async def note_batch_submitted(self, channel_id: str, size: int):
        # call when the SERVER queues a batch for apply
        async with self._lock:
            t = self._by_channel.get(channel_id)
            if t:
                t.in_flight += size

    async def note_batch_applied(self, channel_id: str, size: int):
        # call after the SERVER finishes applying a batch to storage
        done_payload = prog_payload = None
        async with self._lock:
            t = self._by_channel.get(channel_id)
            if not t:
                return
            t.in_flight -= size
            t.processed += size
            prog_payload = {
                "type": "backfill_progress",
                "channel_id": channel_id,
                "task_id": t.id,
                "applied": t.processed
            }
            if t.client_done and t.in_flight <= 0:
                done_payload = {
                    "type": "backfill_done",
                    "channel_id": channel_id,
                    "task_id": t.id,
                    "processed": t.processed,
                    "started_at": t.started_at,
                    "finished_at": time.time(),
                }
                self._by_channel.pop(channel_id, None)
        # publish outside the lock
        if prog_payload:
            await self._bus.publish("client", prog_payload)
        if done_payload:
            await self._bus.publish("client", done_payload)

    async def is_running(self, channel_id: str) -> bool:
        async with self._lock:
            return channel_id in self._by_channel