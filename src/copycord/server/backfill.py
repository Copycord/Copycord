# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import asyncio
import contextlib
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
import time
import uuid
import os
from copycord.server.backfill_webhooks import BackfillWebhookPool

logger = logging.getLogger("server")


class BackfillManager(BackfillWebhookPool):
    def __init__(self, receiver, ratelimit=None):
        self.r = receiver
        self.bot = receiver.bot
        self.ratelimit = ratelimit or receiver.ratelimit
        self._cleanup_task_ids: dict[int, str] = {}
        self._finalized_by_channel: dict[int, str] = {}
        self._flags: set[int] = set()
        self._progress: dict[int, dict] = {}
        self._inflight = defaultdict(int)
        self._by_clone: dict[int, int] = {}
        self._temps_cache: dict[int, list[str]] = {}
        self._created_cache: dict[int, list[int]] = defaultdict(list)
        self.semaphores: dict[int, asyncio.Semaphore] = {}
        self._attach_count: dict[int, int] = defaultdict(int)
        self._last_attach_ts: dict[int, float] = defaultdict(float)
        self._temp_locks: dict[int, asyncio.Lock] = {}
        self._temp_ready: dict[int, asyncio.Event] = {}
        self._global_lock: asyncio.Lock = asyncio.Lock()
        self._rotate_pool: dict[int, list[str]] = {}
        self._rotate_idx: dict[int, int] = {}
        self._rot_locks: dict[int, asyncio.Lock] = {}
        self._attached: dict[int, set[asyncio.Task]] = defaultdict(set)
        self._cleanup_in_progress: set[int] = set()
        self._cleanup_events: dict[int, asyncio.Event] = {}
        self._start_locks: dict[int, asyncio.Lock] = {}
        self._global_sync: dict | None = None
        self._temp_prefix_canon = "Copycord"
        self.temp_webhook_max = 0

    def _ensure_cleanup_event(self, cid: int) -> asyncio.Event:
        ev = self._cleanup_events.get(cid)
        if ev is None:
            ev = asyncio.Event()
            ev.set()
            self._cleanup_events[cid] = ev
        return ev

    async def _wait_cleanup_if_needed(self, cid: int) -> None:
        ev = self._ensure_cleanup_event(cid)
        if not ev.is_set():
            logger.info("[cleanup] waiting for webhook cleanup to finish for #%s", cid)
            await ev.wait()

    def _mark_cleanup_start(self, cid: int) -> None:
        ev = self._ensure_cleanup_event(cid)
        ev.clear()
        self._cleanup_in_progress.add(cid)

    def _mark_cleanup_end(self, cid: int) -> None:
        ev = self._ensure_cleanup_event(cid)
        ev.set()
        self._cleanup_in_progress.discard(cid)

    def is_cleanup_in_progress(self, cid: int) -> bool:
        return not self._ensure_cleanup_event(cid).is_set()

    async def on_started(self, original_id: int, *, meta: dict | None = None) -> None:
        cid = int(original_id)
        await self._wait_cleanup_if_needed(cid)
        self._flags.add(cid)

        st = self._progress.get(cid)
        if not st:
            self.register_sink(cid, user_id=None, clone_channel_id=None, msg=None)
            st = self._progress[cid]

        if meta:
            st["meta"] = meta
            if meta.get("clone_channel_id") is not None:
                st["clone_channel_id"] = int(meta["clone_channel_id"])

            if meta.get("cloned_guild_id") is not None:
                st["cloned_guild_id"] = int(meta["cloned_guild_id"])
            if meta.get("original_guild_id") is not None:
                st["original_guild_id"] = int(meta["original_guild_id"])
            if meta.get("mapping_id"):
                st["mapping_id"] = str(meta["mapping_id"])

        is_resume = bool((meta or {}).get("resume"))
        reused = False


        clone_gid = st.get("cloned_guild_id")
        job_id = (meta or {}).get("job_id")

        if not is_resume:
            # Spare the row the admin just inserted for this job (run_id = job_id).
            keep = str(job_id) if job_id else None
            try:
                if clone_gid is not None:

                    self.r.db.backfill_abort_running_for_channel_in_clone(
                        cid,
                        int(clone_gid),
                        reason="user-declined-resume",
                        except_run_id=keep,
                    )
                else:

                    self.r.db.backfill_abort_running_for_channel(
                        cid, reason="user-declined-resume", except_run_id=keep
                    )
            except Exception:
                logger.exception(
                    "[bf] failed to abort previous running run for #%s", cid
                )

        if is_resume:
            try:
                if clone_gid is not None:
                    row = self.r.db.backfill_get_incomplete_for_channel_in_clone(
                        cid, int(clone_gid)
                    )
                else:
                    row = self.r.db.backfill_get_incomplete_for_channel(cid)

                if row and row.get("run_id"):
                    st["run_id"] = row["run_id"]
                    st["delivered"] = int(row.get("delivered") or 0)
                    st["expected_total"] = (
                        int(row["expected_total"])
                        if row.get("expected_total") is not None
                        else None
                    )
                    st["last_orig_id"] = row.get("last_orig_message_id") or None
                    st["last_ts"] = row.get("last_orig_timestamp") or None

                    if st.get("clone_channel_id") is None and row.get(
                        "clone_channel_id"
                    ):
                        st["clone_channel_id"] = int(row["clone_channel_id"])

                    if st.get("original_guild_id") is None and row.get(
                        "original_guild_id"
                    ):
                        st["original_guild_id"] = int(row["original_guild_id"])
                    if st.get("cloned_guild_id") is None and row.get("cloned_guild_id"):
                        st["cloned_guild_id"] = int(row["cloned_guild_id"])

                    reused = True
            except Exception:
                logger.exception("[bf] failed to reuse existing run for #%s", cid)


        if st.get("clone_channel_id") is None:
            row = self.r.chan_map.get(cid) or {}
            clone = row.get("cloned_channel_id") or row.get("clone_channel_id")
            if clone:
                st["clone_channel_id"] = int(clone)

        # The admin's job id names the run; it already inserted the backfill_runs row.
        desired_id = st.get("run_id") if (is_resume and reused) else None
        st["task_id"] = str(desired_id or job_id or uuid.uuid4())
        if not reused:
            st["run_id"] = st["task_id"]
        self._finalized_by_channel.pop(cid, None)

        if not reused and not job_id:
            try:
                m = st.get("meta") or {}
                rng = (m.get("range") or {}) if isinstance(m.get("range"), dict) else {}
                st["run_id"] = self.r.db.backfill_create_run(
                    cid,
                    rng or {},
                    run_id=st.get("task_id"),
                    original_guild_id=st.get("original_guild_id"),
                    cloned_guild_id=st.get("cloned_guild_id"),
                )
            except Exception:
                logger.exception("[bf] failed to create backfill run for #%s", cid)

        clone_id = st.get("clone_channel_id")
        run_id = st.get("run_id")
        if run_id and clone_id is not None:
            with contextlib.suppress(Exception):
                self.r.db.backfill_set_clone(run_id, int(clone_id))

        loop = asyncio.get_running_loop()
        st["started_at"] = loop.time()
        st["started_dt"] = datetime.now(timezone.utc)
        st.setdefault("last_count", 0)
        st.setdefault("delivered", 0)
        # A resumed run starts from the checkpoint's count; the job reports what it delivers itself.
        st["delivered_base"] = int(st.get("delivered") or 0)
        st.setdefault("expected_total", None)
        st["last_edit_ts"] = 0.0
        st.setdefault("temp_webhook_ids", [])
        st.setdefault("temp_webhook_urls", [])
        st.setdefault("temp_created_ids", [])

        self._inflight[cid] = 0
        for tsk in list(self._attached.get(cid, ())):
            self._attached[cid].discard(tsk)

        if clone_id is not None:
            clone_id = int(clone_id)
            self._by_clone[clone_id] = cid
            self.semaphores.setdefault(clone_id, asyncio.Semaphore(1))
            self._rot_locks.setdefault(clone_id, asyncio.Lock())
            self._temp_locks.setdefault(clone_id, asyncio.Lock())
            self._temp_ready.pop(clone_id, None)
            self.invalidate_rotation(clone_id)
            st["temps_deferred"] = True

    def is_backfilling(self, original_id: int) -> bool:
        """True if the given original ID is currently being backfilled."""
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

    async def clear_sink(
        self, channel_id: int, expected_run_id: str | None = None
    ) -> None:
        cid = int(channel_id)
        st = self._progress.get(cid)

        if expected_run_id and st and st.get("run_id") != expected_run_id:
            logger.debug(
                "[bf] clear_sink skipped for #%s; run mismatch (%s != %s)",
                cid,
                st.get("run_id"),
                expected_run_id,
            )
            return

        self._progress.pop(cid, None)
        self._flags.discard(cid)

    async def on_progress(self, original_id: int, count: int) -> None:
        st = self._progress.get(int(original_id))
        if not st:
            return

        prev = int(st.get("last_count", 0))
        st["last_count"] = max(prev, int(count))

        if st.get("msg"):
            now = asyncio.get_running_loop().time()
            if (now - st.get("last_edit_ts", 0.0) >= 2.0) or (count - prev >= 100):
                try:
                    elapsed = int(now - st["started_at"])
                    await st["msg"].edit(
                        content=f"📦 Backfilling… **{count}** messages (elapsed: {elapsed}s)"
                    )
                    st["last_edit_ts"] = now
                except Exception:
                    pass

    def note_sent(
        self, channel_id: int, original_message_id: int | None = None
    ) -> None:
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
        hook = getattr(self.r, "_bf_note_delivered", None)
        if hook is not None:
            try:
                hook(int(channel_id))
            except Exception:
                logger.debug("[bf] delivered hook failed for #%s", channel_id, exc_info=True)

    def job_counts(self, channel_id: int) -> tuple[int | None, int | None]:
        """(delivered, expected total) for the channel's current job, counted from its start."""
        st = self._progress.get(int(channel_id))
        if not st:
            return None, None
        base = int(st.get("delivered_base") or 0)
        delivered = max(0, int(st.get("delivered") or 0) - base)
        total = st.get("expected_total")
        try:
            total = max(delivered, int(total) - base) if total is not None else None
        except (TypeError, ValueError):
            total = None
        return delivered, total

    async def wait_delivered(self, original_id: int) -> None:
        """Wait for every message task attached to the channel (messages, threads and stickers) to finish."""
        stall = float(os.getenv("BACKFILL_DRAIN_NO_PROGRESS_SECS", "600"))
        await self._wait_drain(int(original_id), no_progress_timeout=stall)

    def note_checkpoint(
        self, channel_id, original_message_id=None, original_timestamp_iso=None
    ):
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
        """Set/raise expected total (from client precount) and short-circuit if zero."""
        cid = int(channel_id)
        st = self._progress.get(cid)
        if not st:
            return

        try:
            t = int(total)
        except Exception:
            return

        if t == 0:
            st["expected_total"] = 0
            st["no_work"] = True
            run_id = st.get("run_id")
            if run_id:
                with contextlib.suppress(Exception):
                    self.r.db.backfill_update_checkpoint(run_id, expected_total=0)

            logger.debug("[bf] set expected_total=0 | channel=%s — ending early", cid)

            if cid in self._flags:
                asyncio.create_task(
                    self.on_done(
                        cid,
                        wait_cleanup=True,
                        expected_task_id=(st or {}).get("task_id"),
                    )
                )
            return

        prev = int(st.get("expected_total") or 0)
        st["expected_total"] = max(prev, t)
        run_id = st.get("run_id")
        if run_id:
            with contextlib.suppress(Exception):
                self.r.db.backfill_update_checkpoint(
                    run_id, expected_total=st["expected_total"]
                )

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

    async def on_done(
        self,
        original_id: int,
        *,
        wait_cleanup: bool = False,
        expected_task_id: str | None = None,
    ) -> int | None:
        """Drain, mark the run done, start temp webhook cleanup; returns messages written since on_started."""
        cid = int(original_id)

        st = self._progress.get(cid) or {}
        live_tid = (st or {}).get("task_id")

        if expected_task_id and live_tid and expected_task_id != live_tid:
            logger.debug(
                "[bf] on_done ignored for #%s; task mismatch (%s != %s)",
                cid,
                expected_task_id,
                live_tid,
            )
            return

        final_tid = expected_task_id or live_tid
        if final_tid and self._finalized_by_channel.get(cid) == final_tid:
            logger.debug(
                "[bf] on_done already finalized for #%s (task=%s)", cid, final_tid
            )
            return

        if final_tid:
            self._finalized_by_channel[cid] = final_tid
        cid = int(original_id)

        stall = float(os.getenv("BACKFILL_DRAIN_NO_PROGRESS_SECS", "600"))
        await self._wait_drain(cid, no_progress_timeout=stall)

        st = self._progress.get(cid) or {}
        delivered = int(st.get("delivered", 0))
        written = max(0, delivered - int(st.get("delivered_base") or 0))
        total = st.get("expected_total")
        try:
            total = int(total) if total is not None else None
        except Exception:
            total = None
        if isinstance(total, int) and delivered < total:
            delivered = total

        shutting_down = getattr(self.r, "_shutting_down", False)

        run_id = st.get("run_id")
        if run_id and not shutting_down:
            with contextlib.suppress(Exception):
                self.r.db.backfill_mark_done(run_id)

        self._flags.discard(cid)
        await self.r._flush_channel_buffer(cid)

        clone_id = st.get("clone_channel_id")
        if clone_id:
            with contextlib.suppress(Exception):
                self.r._clear_bf_throttle(int(clone_id))

        if shutting_down or not clone_id:
            if clone_id:
                self._rotate_pool.pop(int(clone_id), None)
                self._rotate_idx.pop(int(clone_id), None)
                self._by_clone.pop(int(clone_id), None)
            await self.clear_sink(cid)
            await self.end_global_sync(cid)
            await self._wait_drain(cid, no_progress_timeout=stall)
            return written

        created_ids_for_run = list(st.get("temp_created_ids") or [])

        async def _cleanup_and_teardown(
            orig: int, clone: int, created_ids: list[int], task_id: str | None
        ):
            try:
                try:
                    stats = await self.delete_created_temps_for(
                        int(clone), created_ids=created_ids
                    )
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
                self._cleanup_task_ids.pop(orig, None)
                self._mark_cleanup_end(orig)
                self._rotate_pool.pop(int(clone), None)
                self._rotate_idx.pop(int(clone), None)
                self._by_clone.pop(int(clone), None)
                self.semaphores.pop(int(clone), None)
                self._rot_locks.pop(int(clone), None)
                self._temp_locks.pop(int(clone), None)
                self._temp_ready.pop(int(clone), None)
                await self.clear_sink(orig, expected_run_id=task_id)
                await self.end_global_sync(orig)
                await self._wait_drain(orig, no_progress_timeout=120.0)

        task_id_for_run = (st or {}).get("task_id")

        self._mark_cleanup_start(cid)
        if task_id_for_run:
            self._cleanup_task_ids[cid] = task_id_for_run

        if wait_cleanup:
            await _cleanup_and_teardown(
                cid, int(clone_id), created_ids_for_run, task_id_for_run
            )
        else:
            asyncio.create_task(
                _cleanup_and_teardown(
                    cid, int(clone_id), created_ids_for_run, task_id_for_run
                )
            )
        return written

    def attach_task(self, original_id: int, task: asyncio.Task) -> None:
        """Track a message task for the channel; on_done drains these before it reads the delivered count."""
        cid = int(original_id)
        if task in self._attached[cid]:
            return
        self._attached[cid].add(task)
        self._inflight[cid] = self._inflight.get(cid, 0) + 1
        self._attach_count[cid] = self._attach_count.get(cid, 0) + 1
        self._last_attach_ts[cid] = asyncio.get_running_loop().time()

        def _done_cb(t: asyncio.Task, c=cid):
            self._attached[c].discard(t)
            self._on_task_done(c, t)

        task.add_done_callback(_done_cb)

    async def cancel_all_active(self) -> None:
        """Shutdown: cancel every tracked message task and wait for them to unwind."""
        all_tasks = [t for tasks in self._attached.values() for t in list(tasks)]

        for t in all_tasks:
            t.cancel()

        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._attached.clear()
        self._inflight.clear()

    def _on_task_done(self, cid: int, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            logger.debug("[bf] task for #%s cancelled", cid)
            if not getattr(self.r, "_shutting_down", False):
                st = self._progress.get(int(cid)) or {}
                run_id = st.get("run_id")
                if run_id:
                    with contextlib.suppress(Exception):
                        self.r.db.backfill_mark_aborted(run_id, "cancelled-by-server")
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
        self,
        cid: int,
        no_progress_timeout: float = 1800.0,
        log_every: float = 5.0,
    ) -> None:
        loop = asyncio.get_running_loop()
        first_seen = loop.time()
        last_change_ts = first_seen
        last_inflight = self._inflight.get(cid, 0)
        last_delivered = (self._progress.get(cid) or {}).get("delivered", 0)
        last_attach_count = self._attach_count.get(cid, 0)
        last_log_ts = 0.0

        hard_cap = float(os.getenv("BACKFILL_DRAIN_HARD_CAP_SECS", "0")) or None

        while True:
            now = loop.time()
            inflight = self._inflight.get(cid, 0)
            delivered = (self._progress.get(cid) or {}).get("delivered", 0)
            attach_count = self._attach_count.get(cid, 0)

            if inflight == 0:
                break

            if (
                (inflight < last_inflight)
                or (delivered > last_delivered)
                or (attach_count > last_attach_count)
            ):
                last_change_ts = now
                last_inflight = inflight
                last_delivered = delivered
                last_attach_count = attach_count

            if now - last_log_ts >= log_every:
                logger.debug(
                    "[📦] draining #%s | inflight=%d delivered=%d",
                    cid,
                    inflight,
                    delivered,
                )
                last_log_ts = now

            if (now - last_change_ts) >= no_progress_timeout and inflight > 0:
                logger.debug(
                    "[📦] soft-stall but inflight=%d; keeping tasks alive for #%s",
                    inflight,
                    cid,
                )
                last_change_ts = now

            if hard_cap and (now - first_seen) >= hard_cap:
                stuck = list(self._attached.get(cid, ()))
                logger.warning(
                    "[📦] Hard-cap cancelling %d task(s) for #%s after %ds",
                    len(stuck),
                    cid,
                    int(hard_cap),
                )
                for t in stuck:
                    t.cancel()
                if stuck:
                    await asyncio.gather(*stuck, return_exceptions=True)
                self._attached.pop(cid, None)
                self._inflight.pop(cid, None)
                break

            await asyncio.sleep(0.05)
