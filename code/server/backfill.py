# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
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
import time
import uuid
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
        self._created_cache: dict[int, list[int]] = defaultdict(list)
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
        self.temp_webhook_max = 1

    def snapshot_in_progress(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for cid_int, st in self._progress.items():
            delivered = int(st.get("delivered", 0))
            total = st.get("expected_total")
            inflight = int(self._inflight.get(cid_int, 0))
            if total is not None and delivered >= int(total) and inflight == 0:
                continue
            out[str(int(cid_int))] = {
                "delivered": delivered,
                "expected_total": (int(total) if total is not None else None),
                "started_at": (st.get("started_dt").isoformat() if st.get("started_dt") else None),
                "clone_channel_id": st.get("clone_channel_id"),
                "in_flight": inflight,
                "run_id": st.get("run_id"),
                "last_orig_message_id": st.get("last_orig_id"),
                "last_orig_timestamp": st.get("last_ts"),
            }

        return out

    def attach_task(self, original_id: int, task: asyncio.Task) -> None:
        cid = int(original_id)
        if task in self._attached[cid]:
            return
        self._attached[cid].add(task)
        self._inflight[cid] = self._inflight.get(cid, 0) + 1

        def _done_cb(t: asyncio.Task, c=cid):
            self._attached[c].discard(t)
            self._on_task_done(c, t)

        task.add_done_callback(_done_cb)

    async def cancel_all_active(self) -> None:

        all_tasks = [t for tasks in self._attached.values() for t in list(tasks)]

        for t in all_tasks:
            t.cancel()

        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._attached.clear()
        self._inflight.clear()

    async def on_started(self, original_id: int, *, meta: dict | None = None) -> None:
        cid = int(original_id)
        self._flags.add(cid)
        

        st = self._progress.get(cid)
        if not st:
            self.register_sink(cid, user_id=None, clone_channel_id=None, msg=None)
            st = self._progress[cid]
            
        try:
            rng = (st.get("meta") or {}).get("range")  # whatever you pass down (mode/params)
            st["run_id"] = self.r.db.backfill_create_run(
                int(cid),
                (rng or {}).get("mode") if isinstance(rng, dict) else None,
                rng or {},
            )
        except Exception:
            logger.exception("[bf] failed to create backfill run for #%s", cid)
            

        if meta:
            st["meta"] = meta
            if meta.get("clone_channel_id") is not None:
                st["clone_channel_id"] = int(meta["clone_channel_id"])

        if st.get("clone_channel_id") is None:
            row = self.r.chan_map.get(cid) or {}
            clone = row.get("cloned_channel_id") or row.get("clone_channel_id")
            if clone:
                st["clone_channel_id"] = int(clone)

        loop = asyncio.get_event_loop()
        st["started_at"] = loop.time()
        st["started_dt"] = datetime.now(timezone.utc)
        st["last_count"] = 0
        st["delivered"] = 0
        st["last_edit_ts"] = 0.0
        st["expected_total"] = None
        st["temp_webhook_ids"] = []
        st["temp_webhook_urls"] = []
        st["temp_created_ids"] = []

        self._inflight[cid] = 0
        for t in list(self._attached.get(cid, ())):
            self._attached[cid].discard(t)

        clone_id = st.get("clone_channel_id")
        run_id = st.get("run_id")
        if run_id and st.get("clone_channel_id") is not None:
            with contextlib.suppress(Exception):
                self.r.db.backfill_set_clone(run_id, int(st["clone_channel_id"]))
        if clone_id is not None:
            clone_id = int(clone_id)
            self._by_clone[clone_id] = cid

            self.semaphores.setdefault(clone_id, asyncio.Semaphore(1))
            self._rot_locks.setdefault(clone_id, asyncio.Lock())
            self._temp_locks.setdefault(clone_id, asyncio.Lock())
            self._temp_ready.pop(clone_id, None)

            self.invalidate_rotation(clone_id)
            await self.ensure_temps_ready(clone_id)

        if hasattr(self, "tracker"):
            await self.tracker.start(str(cid), st.get("meta") or {})

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
            "clone_channel_id": clone_channel_id,
            "msg": msg,
            "started_at": now,
            "started_dt": datetime.now(timezone.utc),
            "last_count": 0,
            "last_edit_ts": 0.0,
            "temp_webhook_ids": [],
            "temp_webhook_urls": [],
        }
        
        orig = int(channel_id)
        st = self._progress.get(orig) or {}
        run_id = st.get("run_id")
        if run_id and clone_channel_id:
            with contextlib.suppress(Exception):
                self.r.db.backfill_set_clone(run_id, int(clone_channel_id))
        if clone_channel_id:
            self._by_clone[int(clone_channel_id)] = int(channel_id)

    async def cancel(self, channel_id: str):
        async with self._lock:
            self._by_channel.pop(channel_id, None)
        self._stop_pump(channel_id)

    async def clear_sink(self, channel_id: int) -> None:
        cid = int(channel_id)
        self._progress.pop(cid, None)
        self._flags.discard(cid)
        if hasattr(self, "tracker"):
            with contextlib.suppress(Exception):
                await self.tracker.cancel(str(cid))

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

    def note_sent(self, channel_id: int, original_message_id: int | None = None) -> None:
        st = self._progress.get(int(channel_id))
        if not st:
            return
        sent_ids = st.setdefault("sent_ids", set())
        if original_message_id:
            oid = str(original_message_id)
            if oid in sent_ids:
                return
            sent_ids.add(oid)
            st["last_orig_id"] = oid
        st["delivered"] = int(st.get("delivered", 0)) + 1

    def note_checkpoint(self, channel_id, original_message_id=None, original_timestamp_iso=None):
        st = self._progress.get(int(channel_id))
        if not st:
            return
        if original_message_id is not None:
            st["last_orig_id"] = str(original_message_id)
        if original_timestamp_iso:
            st["last_ts"] = str(original_timestamp_iso)
        if run_id := st.get("run_id"):
            self.r.db.backfill_update_checkpoint(
                run_id,
                delivered=int(st.get("delivered", 0)),
                expected_total=st.get("expected_total"),
                last_orig_message_id=st.get("last_orig_id"),
                last_orig_timestamp=st.get("last_ts"),
            )
            
    def update_expected_total(self, channel_id: int, total: int) -> None:
        """Set/raise expected total (from client precount)."""
        cid = int(channel_id)
        st = self._progress.get(cid)
        if not st:
            return
        prev = int(st.get("expected_total") or 0)
        st["expected_total"] = max(prev, int(total))
        run_id = (st or {}).get("run_id")
        if run_id:
            with contextlib.suppress(Exception):
                self.r.db.backfill_update_checkpoint(run_id, expected_total=st["expected_total"])
        logger.debug(
            "[bf] set expected_total | channel=%s total=%s (prev=%s)",
            cid,
            st["expected_total"],
            prev,
        )

    def add_expected_total(self, channel_id: int, delta: int = 1) -> None:
        """Increment expected_total by delta (used for synthetic units like text-thread creations)."""
        cid = int(channel_id)
        st = self._progress.get(cid)
        if not st:
            return
        curr = int(st.get("expected_total") or 0)
        st["expected_total"] = curr + int(delta)

    def get_progress(self, channel_id: int) -> tuple[int | None, int | None]:
        cid = int(channel_id)
        if cid not in self._progress or cid not in self._flags:
            return None, None

        st = self._progress.get(cid) or {}
        delivered = int(st.get("delivered", 0))
        total = st.get("expected_total")
        try:
            total = int(total) if total is not None else None
        except Exception:
            total = None
        return delivered, total

    async def end_global_sync(self, original_id: int) -> None:
        """Release the global sync slot if held for this channel."""
        async with self._global_lock:
            if self._global_sync and self._global_sync.get("original_id") == int(
                original_id
            ):
                self._global_sync = None

    async def on_done(self, original_id: int, *, wait_cleanup: bool = False) -> None:
        cid = int(original_id)

        await self._wait_drain(cid, no_progress_timeout=120.0)

        st = self._progress.get(cid) or {}
        delivered = int(st.get("delivered", 0))
        total = st.get("expected_total")
        try:
            total = int(total) if total is not None else None
        except Exception:
            total = None
        if isinstance(total, int) and delivered < total:
            delivered = total

        if hasattr(self, "tracker"):
            with contextlib.suppress(Exception):
                await self.tracker.publish_progress(
                    str(cid), delivered=delivered, total=total
                )

        run_id = (self._progress.get(cid) or {}).get("run_id")
        if run_id:
            with contextlib.suppress(Exception):
                self.r.db.backfill_mark_done(run_id)
        
        try:
            await self.r.bus.publish(
                "client",
                {
                    "type": "backfill_done",
                    "data": {
                        "channel_id": str(cid),
                        "sent": delivered,
                        "total": total,
                    },
                },
            )
        except Exception:
            logger.debug(
                "[bf] failed to publish backfill_done for #%s", cid, exc_info=True
            )

        self._flags.discard(cid)
        await self.r._flush_channel_buffer(cid)

        st = self._progress.get(cid) or {}
        clone_id = st.get("clone_channel_id")

        if clone_id:
            with contextlib.suppress(Exception):
                self.r._clear_bf_throttle(int(clone_id))

        shutting_down = getattr(self.r, "_shutting_down", False)
        if shutting_down or not clone_id:
            if clone_id:
                self._rotate_pool.pop(int(clone_id), None)
                self._rotate_idx.pop(int(clone_id), None)
                self._by_clone.pop(int(clone_id), None)
            await self.clear_sink(cid)
            await self.end_global_sync(cid)
            await self._wait_drain(cid, no_progress_timeout=120.0)
            return

        async def _cleanup_and_teardown(orig: int, clone: int):
            try:
                await self.r.bus.publish(
                    "client",
                    {
                        "type": "backfill_cleanup",
                        "data": {"channel_id": str(orig), "state": "starting"},
                    },
                )
            except Exception:
                pass

            try:
                try:
                    stats = await self.delete_created_temps_for(int(clone))
                    logger.debug(
                        "[🧹] Deleted %d temp webhooks in #%s (created this run)",
                        stats.get("deleted", 0),
                        clone,
                    )
                except Exception:
                    logger.debug(
                        "[cleanup] temp deletion failed for #%s", clone, exc_info=True
                    )
            finally:
                try:
                    await self.r.bus.publish(
                        "client",
                        {
                            "type": "backfill_cleanup",
                            "data": {
                                "channel_id": str(orig),
                                "state": "finished",
                                "deleted": (
                                    int(stats.get("deleted", 0))
                                    if isinstance(stats, dict)
                                    else None
                                ),
                            },
                        },
                    )
                except Exception:
                    pass

                self._rotate_pool.pop(int(clone), None)
                self._rotate_idx.pop(int(clone), None)
                self._by_clone.pop(int(clone), None)
                self.semaphores.pop(int(clone), None)
                self._rot_locks.pop(int(clone), None)
                self._temp_locks.pop(int(clone), None)
                self._temp_ready.pop(int(clone), None)
                await self.clear_sink(orig)
                await self.end_global_sync(orig)
                await self._wait_drain(orig, no_progress_timeout=120.0)

        if wait_cleanup:
            await _cleanup_and_teardown(cid, int(clone_id))
        else:
            asyncio.create_task(_cleanup_and_teardown(cid, int(clone_id)))

    def _on_task_done(self, cid: int, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            logger.debug("[bf] task for #%s cancelled", cid)
        except Exception as e:
            logger.exception("[bf] task error for #%s: %s", cid, e)
            st = self._progress.get(int(cid)) or {}
            run_id = st.get("run_id")
            if run_id:
                with contextlib.suppress(Exception):
                    self.r.db.backfill_mark_failed(run_id, str(e))
        finally:
            n = self._inflight.get(cid, 0)
            self._inflight[cid] = max(0, n - 1)
            if self._inflight[cid] == 0:
                self._inflight.pop(cid, None)

    async def _wait_drain(
        self, cid: int, no_progress_timeout: float = 120.0, log_every: float = 5.0
    ) -> None:
        """
        Wait until in-flight == 0. Only cancel if there's been *no* progress
        (in-flight down or delivered up) for `no_progress_timeout` seconds.
        """
        loop = asyncio.get_event_loop()
        last_change_ts = loop.time()
        last_inflight = self._inflight.get(cid, 0)
        last_delivered = (self._progress.get(cid) or {}).get("delivered", 0)
        last_log_ts = 0.0

        while self._inflight.get(cid, 0) > 0:
            now = loop.time()
            inflight = self._inflight.get(cid, 0)
            delivered = (self._progress.get(cid) or {}).get("delivered", 0)

            if inflight < last_inflight or delivered > last_delivered:
                last_change_ts = now
                last_inflight = inflight
                last_delivered = delivered

            if now - last_log_ts >= log_every:
                logger.debug(
                    "[📦] draining #%s | inflight=%d delivered=%d",
                    cid,
                    inflight,
                    delivered,
                )
                last_log_ts = now

            if (now - last_change_ts) >= no_progress_timeout:
                stuck = list(self._attached.get(cid, ()))
                logger.warning(
                    "[📦] Drain stalled for #%s (no progress %ss, inflight=%d); cancelling %d task(s)",
                    cid,
                    int(no_progress_timeout),
                    inflight,
                    len(stuck),
                )
                for t in stuck:
                    t.cancel()
                if stuck:
                    await asyncio.gather(*stuck, return_exceptions=True)
                self._attached.pop(cid, None)
                self._inflight.pop(cid, None)
                break

            await asyncio.sleep(0.05)

    async def _ensure_temp_webhooks(
        self, clone_channel_id: int
    ) -> tuple[list[int], list[str]]:
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

            ch = self.bot.get_channel(clone_channel_id) or await self.bot.fetch_channel(
                clone_channel_id
            )
            if not ch:
                logger.debug("[temps] Channel not found for #%s", clone_channel_id)
                return [], []

            orig = self._by_clone.get(clone_channel_id)
            row = self.r.chan_map.get(orig) or {}
            primary_url = row.get("channel_webhook_url") or row.get("webhook_url")

            st = self._progress.get(orig) if orig is not None else None
            N = max(0, int(getattr(self, "temp_webhook_max", 1)))

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

                    av_asset = getattr(whp, "avatar", None)
                    if av_asset:
                        try:
                            primary_avatar_url = (
                                str(getattr(av_asset, "url", None)) or None
                            )
                        except Exception:
                            primary_avatar_url = None
                except Exception as e:
                    logger.debug(
                        "[temps] fetch primary webhook failed for %s: %s",
                        primary_url,
                        e,
                    )

            if st is not None:
                st.setdefault("primary_identity", {})
                st["primary_identity"] = {
                    "name": primary_name,
                    "avatar_url": primary_avatar_url,
                }

            async def _fetch_wh(url: str):
                try:
                    wid = int(url.rstrip("/").split("/")[-2])
                    return await self.bot.fetch_webhook(wid)
                except Exception:
                    return None

            seen_urls: set[str] = set()
            pairs: list[tuple[int, str]] = []

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

            if st:
                for u in list(st.get("temp_webhook_urls") or []):
                    await _adopt(u)

            try:
                hooks = await ch.webhooks()
                for wh in hooks:
                    try:
                        made_by_us = (
                            getattr(wh, "user", None)
                            and getattr(wh.user, "id", None) == self.bot.user.id
                        )
                    except Exception:
                        made_by_us = False
                    if made_by_us and (not primary_url or wh.url != primary_url):
                        await _adopt(wh.url)
            except Exception as e:

                logger.debug(
                    "[temps] list hooks failed for #%s: %s", clone_channel_id, e
                )

            if N <= 0:
                ids, urls = [], []
                if st is not None:
                    st["temp_webhook_ids"] = ids
                    st["temp_webhook_urls"] = urls
                    st["temp_created_ids"] = []

                self._rotate_pool.pop(clone_channel_id, None)
                self._rotate_idx.pop(clone_channel_id, None)
                return ids, urls

            while len(pairs) < N:

                logger.info(
                    "[🧹] Creating temp webhook for rotation in #%s...",
                    clone_channel_id,
                )
                await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)
                wh_new = await ch.create_webhook(
                    name=self._canonical_temp_name(),
                    reason="Backfill rotation",
                )
                pairs.append((int(wh_new.id), wh_new.url))
                seen_urls.add(wh_new.url)
                created_ids.append(int(wh_new.id))

            pairs.sort(key=lambda t: t[0])
            pairs = pairs[:N]

            ids = [p[0] for p in pairs]
            urls = [p[1] for p in pairs]

            if st is not None:
                st["temp_webhook_ids"] = list(ids)
                st["temp_webhook_urls"] = list(urls)

                st["temp_created_ids"] = [i for i in created_ids if i in ids]
            else:
                kept = [i for i in created_ids if i in ids]
                if kept:
                    self._created_cache[int(clone_channel_id)].extend(kept)

            self._rotate_pool.pop(clone_channel_id, None)
            self._rotate_idx.pop(clone_channel_id, None)
            return ids, urls

        except Exception as e:
            logger.warning(
                "[⚠️] Could not ensure temp webhooks in #%s: %s", clone_channel_id, e
            )
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

            ch = self.bot.get_channel(clone_channel_id) or await self.bot.fetch_channel(
                clone_channel_id
            )
            hooks = await ch.webhooks()

            primary_url = None
            orig = self._by_clone.get(clone_channel_id)
            if orig is not None:
                row = self.r.chan_map.get(orig) or {}
                primary_url = row.get("channel_webhook_url")

            pairs = []
            for wh in hooks:
                try:

                    made_by_us = (
                        getattr(wh, "user", None)
                        and getattr(wh.user, "id", None) == self.bot.user.id
                    )
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
        lock = self._rot_locks.get(clone_channel_id)
        if lock is None:
            lock = self._rot_locks[clone_channel_id] = asyncio.Lock()
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

    def _canonical_temp_name(self) -> str:
        return self._temp_prefix_canon

    def invalidate_rotation(self, clone_channel_id: int) -> None:
        cid = int(clone_channel_id)
        self._rotate_pool.pop(cid, None)
        self._rotate_idx.pop(cid, None)
        self._temps_cache.pop(cid, None)

        if hasattr(self, "_temp_ready"):
            self._temp_ready.pop(cid, None)
        self._temp_locks.pop(cid, None)
        self._rot_locks.pop(cid, None)

        logger.debug("[rotate] invalidated pool for #%s", clone_channel_id)

    async def delete_created_temps_for(
        self, clone_channel_id: int, *, dry_run: bool = False
    ) -> dict:
        """
        Delete ONLY temp webhooks that were CREATED during the current/last backfill run
        for this clone channel. Safe: never touches the primary or adopted hooks.

        Returns: {"deleted": int}
        """
        stats = {"deleted": 0}
        try:

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

            created_ids = []
            if st:
                created_ids.extend(list(st.get("temp_created_ids") or []))
            created_ids.extend(self._created_cache.get(int(clone_channel_id), []))

            created_ids = list({int(x) for x in created_ids})

            if not created_ids:
                return stats

            for wid in created_ids:
                if primary_id and int(wid) == int(primary_id):
                    continue
                try:
                    logger.info("[🧹] Deleting msg-sync temp webhook, please wait...")

                    if dry_run:
                        logger.info(
                            "[🧹 DRY RUN] Would delete temp webhook %s in #%s",
                            wid,
                            clone_channel_id,
                        )
                    else:
                        wh = await self.bot.fetch_webhook(int(wid))
                        await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)
                        await wh.delete(reason="Backfill complete: remove temp webhook")
                        stats["deleted"] += 1
                        logger.info(
                            "[🧹] Sync completed, deleted temp webhook %s in #%s",
                            wid,
                            clone_channel_id,
                        )
                except Exception as e:
                    logger.debug(
                        "[cleanup] Could not delete webhook %s in #%s: %s",
                        wid,
                        clone_channel_id,
                        e,
                    )

            # clear temp fields so we don't try again
            st["temp_created_ids"] = []
            st["temp_webhook_ids"] = []
            st["temp_webhook_urls"] = []
            self._created_cache.pop(int(clone_channel_id), None)
            self.invalidate_rotation(int(clone_channel_id))
        except Exception:
            logger.debug(
                "[cleanup] delete_created_temps_for failed for #%s",
                clone_channel_id,
                exc_info=True,
            )
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

            with contextlib.suppress(Exception):
                self.r._load_mappings()

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
                wid = _wid_from_url(
                    row.get("channel_webhook_url") or row.get("webhook_url")
                )
                if wid:
                    clone_to_primary[clone_id] = wid

            targets = list(clone_to_primary.keys())
            if channel_ids:
                targets = [int(x) for x in channel_ids if int(x) in clone_to_primary]

            if not targets:
                logger.debug("[cleanup] No clone channels to check; nothing to do")
                return stats

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
                            clone_id,
                            e,
                        )
                        continue

                if not ch:
                    stats["skipped_missing_channel"] += 1
                    logger.debug(
                        "[cleanup] Clone channel #%s not found; skipping", clone_id
                    )
                    continue

                stats["channels_checked"] += 1
                primary_id = int(clone_to_primary.get(clone_id) or 0)
                if not primary_id:
                    stats["skipped_no_primary"] += 1
                    logger.debug(
                        "[cleanup] #%s has no primary webhook id; skipping", clone_id
                    )
                    continue

                try:
                    hooks = await ch.webhooks()
                except Exception as e:

                    stats["skipped_missing_channel"] += 1
                    logger.debug(
                        "[cleanup] Could not list webhooks for #%s: %s", clone_id, e
                    )
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
                            wh.id,
                            clone_id,
                            wh.name,
                        )
                        continue

                    try:

                        await wh.delete(
                            reason="Cleanup: remove non-primary webhook in clone channel"
                        )
                        stats["deleted"] += 1
                        logger.debug(
                            "[🧹] Deleted extra webhook %s in #%s (name=%r)",
                            wh.id,
                            clone_id,
                            wh.name,
                        )
                    except Exception as e:
                        logger.warning(
                            "[⚠️] Failed to delete webhook %s in #%s: %s",
                            wh.id,
                            clone_id,
                            e,
                        )

                await asyncio.sleep(0)

            logger.debug(
                "[🧹] Cleanup complete: checked=%d deleted=%d skipped_no_primary=%d skipped_missing_channel=%d",
                stats["channels_checked"],
                stats["deleted"],
                stats["skipped_no_primary"],
                stats["skipped_missing_channel"],
            )
        except Exception:
            logger.exception(
                "[cleanup] Unexpected error while deleting non-primary webhooks"
            )
        return stats


@dataclass
class BackfillTask:
    id: str
    channel_id: str
    started_at: float = field(default_factory=time.time)
    processed: int = 0
    in_flight: int = 0
    client_done: bool = False


class BackfillTracker:
    def __init__(self, bus, on_done_cb=None, progress_provider=None):
        self._bus = bus
        self._by_channel: dict[str, BackfillTask] = {}
        self._lock = asyncio.Lock()
        self._on_done_cb = on_done_cb
        self._progress_provider = progress_provider
        self._pumps: dict[str, asyncio.Task] = {}

    async def start(self, channel_id: str, meta: dict) -> BackfillTask | None:
        async with self._lock:
            if channel_id in self._by_channel:

                return None
            t = BackfillTask(id=str(uuid.uuid4()), channel_id=channel_id)
            self._by_channel[channel_id] = t
        await self._bus.publish(
            "client",
            {
                "type": "backfill_started",
                "channel_id": channel_id,
                "task_id": t.id,
                **(meta or {}),
            },
        )
        if self._progress_provider and channel_id not in self._pumps:
            self._pumps[channel_id] = asyncio.create_task(
                self._progress_pump(channel_id, t.id)
            )
        return t

    async def publish_progress(
        self, channel_id: str, *, delivered: int | None, total: int | None
    ):
        async with self._lock:
            t = self._by_channel.get(channel_id)
            task_id = t.id if t else str(uuid.uuid4())
        payload = {
            "type": "backfill_progress",
            "channel_id": channel_id,
            "task_id": task_id,
        }
        if delivered is not None:
            payload["delivered"] = delivered
        if total is not None:
            payload["expected_total"] = total
        await self._bus.publish("client", payload)

    async def _progress_pump(self, channel_id: str, task_id: str):
        last_delivered = None
        idle_ticks = 0
        try:
            while True:
                async with self._lock:
                    t = self._by_channel.get(channel_id)
                    if not t:
                        break

                    in_flight = t.in_flight
                    client_done = t.client_done

                delivered = total = None
                if self._progress_provider:
                    try:
                        delivered, total = self._progress_provider(int(channel_id))
                    except Exception:
                        delivered, total = None, None

                changed = delivered is not None and delivered != last_delivered

                heartbeat = in_flight > 0

                if changed or (heartbeat and idle_ticks >= 8):
                    payload = {
                        "type": "backfill_progress",
                        "channel_id": channel_id,
                        "task_id": task_id,
                    }
                    if delivered is not None:
                        payload["delivered"] = delivered
                    if total is not None:
                        payload["expected_total"] = total

                    if (
                        ("delivered" in payload)
                        or ("expected_total" in payload)
                        or heartbeat
                    ):
                        await self._bus.publish("client", payload)

                    if changed:
                        last_delivered = delivered
                    idle_ticks = 0
                else:
                    idle_ticks += 1

                await asyncio.sleep(0.25)
        finally:
            self._pumps.pop(channel_id, None)

    def _stop_pump(self, channel_id: str):
        t = self._pumps.pop(channel_id, None)
        if t:
            t.cancel()
