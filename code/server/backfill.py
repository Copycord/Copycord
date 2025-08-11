import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from aiohttp import ClientConnectionError, ServerDisconnectedError
import ssl
import discord
from server.rate_limiter import RateLimitManager, ActionType


class BackfillManager:
    def __init__(self, receiver):
        self.r = receiver
        self.bot = receiver.bot
        self.log = logging.getLogger("server")
        self.ratelimit = RateLimitManager()

        self._flags: set[int] = set()              
        self._progress: dict[int, dict] = {}       
        self._inflight = defaultdict(int)         
        self._by_clone: dict[int, int] = {}         
        self._rotate_pool: dict[int, list[str]] = {} 
        self._rotate_idx: dict[int, int] = {}     
        self.semaphores: dict[int, asyncio.Semaphore] = {}
        self._global_lock: asyncio.Lock = asyncio.Lock()
        self._global_sync: dict | None = None
        
    async def on_started(self, original_id: int) -> None:
        """
        Initialize server-side state for a channel backfill:
        - mark channel as backfilling (buffers live traffic)
        - ensure a progress sink exists
        - initialize progress counters
        - create a temp webhook for rotation (if we know the clone channel)
        """
        cid = int(original_id)
        self._flags.add(cid)

        # Ensure we have a sink; if not, create a minimal one
        st = self._progress.get(cid)
        if not st:
            self.register_sink(
                cid,
                user_id=None,
                clone_channel_id=None,
                msg=None,
            )
            st = self._progress[cid]

        # Initialize/ensure progress fields
        loop = asyncio.get_event_loop()
        st.setdefault("started_at", loop.time())
        st.setdefault("started_dt", datetime.now(timezone.utc))
        st.setdefault("last_count", 0)     # client-reported count
        st.setdefault("delivered", 0)      # server-side successful sends
        st.setdefault("last_edit_ts", 0.0)
        st.setdefault("temp_webhook_id", None)
        st.setdefault("temp_webhook_url", None)

        # Create a temp webhook for rotation if we know the cloned channel
        clone_id = st.get("clone_channel_id")
        if clone_id:
            temp_id, temp_url = await self._ensure_temp_webhook(int(clone_id))
            st["temp_webhook_id"] = temp_id
            st["temp_webhook_url"] = temp_url

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

    def register_sink(self, channel_id: int, *, user_id: int | None, clone_channel_id: int | None, msg=None) -> None:
        """
        Registers a sink for a given channel, storing metadata and progress tracking information.
        """
        now = asyncio.get_event_loop().time()
        self._progress[int(channel_id)] = {
            "user_id": user_id,
            "clone_channel_id": clone_channel_id,
            "msg": msg,
            "started_at": now,
            "started_dt": datetime.now(timezone.utc),
            "last_count": 0,
            "last_edit_ts": 0.0,
            "temp_webhook_id": None,
            "temp_webhook_url": None,
        }
        if clone_channel_id:
            self._by_clone[int(clone_channel_id)] = int(channel_id)

    async def clear_sink(self, channel_id: int) -> None:
        """
        Clears the progress sink for a specific channel.
        """
        self._progress.pop(int(channel_id), None)

    def attach_task(self, original_id: int, task: asyncio.Task) -> None:
        """
        Attaches a task to the inflight tracking system and sets up a callback 
        to handle its completion.
        """
        cid = int(original_id)
        self._inflight[cid] += 1
        task.add_done_callback(lambda t, c=cid: self._on_task_done(c, t))

    async def on_progress(self, original_id: int, count: int) -> None:
        st = self._progress.get(int(original_id))
        if not st:
            return

        # Always record the latest seen counts
        prev = int(st.get("last_count", 0))
        st["last_count"] = max(prev, int(count))
        st["expected_total"] = max(int(st.get("expected_total", 0)), int(count))

        # Only try to edit if we actually have a sink message
        if st.get("msg"):
            now = asyncio.get_event_loop().time()
            if (now - st.get("last_edit_ts", 0.0) >= 2.0) or (count - prev >= 100):
                try:
                    elapsed = int(now - st["started_at"])
                    await st["msg"].edit(content=f"📦 Backfilling… **{count}** messages (elapsed: {elapsed}s)")
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
        """Optionally set/raise an expected total (from a meta/progress message)."""
        cid = int(channel_id)
        st = self._progress.get(cid)
        if not st:
            return
        st["expected_total"] = max(int(st.get("expected_total", 0)), int(total))
        
    def get_progress(self, channel_id: int) -> tuple[int, int]:
        """
        Return (delivered, total). If total is unknown, it may be 0.
        We prefer 'expected_total' (from meta), otherwise fall back to last_count.
        """
        cid = int(channel_id)
        st = self._progress.get(cid) or {}
        delivered = int(st.get("delivered", 0))
        total = int(st.get("expected_total") or st.get("last_count") or 0)
        if total < delivered:
            total = delivered
        return delivered, total
            
    async def shutdown(self):
        self._flags.clear()
        await asyncio.sleep(0.1)
        for cid in list(self._progress.keys()):
            try:
                await self._clear_sink(cid, send_dm=False, delete_temp=True, quiet=True)
            except Exception:
                pass
        self._progress.clear()
        
    async def try_begin_global_sync(self, original_id: int, user_id: int) -> tuple[bool, dict | None]:
        """
        Attempt to claim the global sync slot.
        Returns (ok, conflict_info). If ok=False, conflict_info has keys: original_id, user_id, started_at
        """
        async with self._global_lock:
            if self._global_sync is not None:
                # someone else is already running a sync
                return False, dict(self._global_sync)
            # claim it
            self._global_sync = {
                "original_id": int(original_id),
                "user_id": int(user_id),
                "started_at": asyncio.get_event_loop().time(),
            }
            return True, None

    async def end_global_sync(self, original_id: int) -> None:
        """Release the global sync slot if held for this channel."""
        async with self._global_lock:
            if self._global_sync and self._global_sync.get("original_id") == int(original_id):
                self._global_sync = None

    def current_global_sync(self) -> dict | None:
        """Readonly peek at the current global sync info, or None."""
        return dict(self._global_sync) if self._global_sync else None

    async def on_done(self, original_id: int) -> None:
        """
        Handles the completion of a backfill operation for a specific channel.
        Shutdown-safe and uses server-side delivered count to avoid 0 in summary.
        """
        cid = int(original_id)

        # 1) wait until server finished forwarding all backfill messages
        await self._wait_drain(cid, timeout=None)

        # 2) clear flag; flush any buffered live messages
        self._flags.discard(cid)
        await self.r._flush_channel_buffer(cid)

        st          = self._progress.get(cid) or {}
        clone_id    = st.get("clone_channel_id")
        temp_id     = st.get("temp_webhook_id")
        delivered   = int(st.get("delivered", 0))
        last_count  = int(st.get("last_count", 0))
        total       = delivered
        started_at  = st.get("started_at")
        elapsed_s   = int(asyncio.get_event_loop().time() - started_at) if started_at else 0
        finished_dt = datetime.now(timezone.utc)

        shutting_down = getattr(self.r, "_shutting_down", False)
        suppress_dm   = getattr(self.r, "_suppress_backfill_dm", False)

        # 3) DM summary (skip during shutdown)
        uid = st.get("user_id")
        if uid and not (shutting_down or suppress_dm):
            ch_value = f"<#{clone_id}>" if clone_id else f"Original #{cid}"
            embed = discord.Embed(
                title="💬 Backfill Complete",
                description="Channel messages have been fully synced.",
                timestamp=finished_dt,
                color=discord.Color.green(),
            )
            embed.add_field(name="Channel", value=ch_value, inline=True)
            embed.add_field(name="Messages Cloned", value=str(total), inline=True)
            embed.add_field(name="Duration", value=self._fmt_duration(elapsed_s), inline=True)
            try:
                user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                await user.send(embed=embed)
                self.log.info("[📨] DM’d backfill summary to user %s for channel %s", uid, cid)
            except Exception as e:
                (self.log.warning if not shutting_down else self.log.debug)(
                    "[⚠️] Could not DM user %s: %s", uid, e
                )

        # 4) temp webhook cleanup (skip during shutdown)
        if temp_id and not shutting_down:
            try:
                if self.r.session and not self.r.session.closed:
                    wh = await self.bot.fetch_webhook(temp_id)
                    await wh.delete(reason="Backfill rotation cleanup")
                else:
                    self.log.debug("[shutdown] Skipping temp webhook delete for %s (session closed)", temp_id)
            except Exception as e:
                self.log.warning("[⚠️] Could not delete temp webhook %s: %s", temp_id, e)

        # 5) clear rotation pools
        if clone_id:
            self._rotate_pool.pop(int(clone_id), None)
            self._rotate_idx.pop(int(clone_id), None)
            self._by_clone.pop(int(clone_id), None)

        # 6) clear sink
        await self.clear_sink(cid)
        await self.end_global_sync(cid)

        # 7) final log
        if not shutting_down:
            self.log.info("[📦] Backfill finished for #%s; buffered messages flushed", cid)
        else:
            self.log.debug("[📦] Backfill finished for #%s during shutdown; skipped DM/cleanup", cid)
        
    async def _clear_sink(self, channel_id: int, *, send_dm: bool, delete_temp: bool, quiet: bool):
        st = self._progress.get(channel_id)
        if not st:
            return

        # 1) DM summary (only if not shutting down)
        if send_dm and not getattr(self.r, "_shutting_down", False):
            try:
                user = self.r.bot.get_user(st["user_id"]) or await self.r.bot.fetch_user(st["user_id"])
                if st.get("final_embed"):
                    await user.send(embed=st["final_embed"])
                    self.r.logger.info("[📨] DM’d backfill summary to user %s for channel %s",
                                       st["user_id"], channel_id)
            except Exception as e:
                self.log.warning("[⚠️] Could not DM user %s: %s", st.get("user_id"), e)

        # 2) Delete temp webhook (skip if session is closing)
        if delete_temp:
            url = st.get("temp_webhook_url")
            if url:
                try:
                    if self.r.session and not self.r.session.closed:
                        wh = discord.Webhook.from_url(url, session=self.r.session)
                        await wh.delete()
                    else:
                        # if our session is gone, do not attempt network I/O during shutdown
                        return
                except (ClientConnectionError, ServerDisconnectedError, ssl.SSLError) as e:
                    # demote to DEBUG while shutting down
                    lvl = self.log.debug if getattr(self.r, "_shutting_down", False) or quiet else self.log.warning
                    lvl("[⚠️] Could not delete temp webhook %s: %s", url, e)
                except Exception as e:
                    lvl = self.log.debug if quiet else self.log.warning

        self._progress.pop(channel_id, None)

    def _on_task_done(self, cid: int, task: asyncio.Task) -> None:
        """
        Handles the completion of an asynchronous task associated with a specific ID.
        """
        try:
            task.result()
        except Exception:
            pass
        else:
            st = self._progress.get(cid)
            if st:
                st["last_count"] = (st.get("last_count") or 0) + 1
        finally:
            n = self._inflight.get(cid, 0)
            if n <= 1:
                self._inflight.pop(cid, None)
            else:
                self._inflight[cid] = n - 1

    async def _wait_drain(self, cid: int, timeout: float | None = 30.0) -> None:
        """
        Waits for the in-flight operations for a given connection ID (cid) to drain 
        (i.e., reach zero) within an optional timeout period.
        """
        start = asyncio.get_event_loop().time()
        while self._inflight.get(cid, 0) > 0:
            if timeout is not None and (asyncio.get_event_loop().time() - start) > timeout:
                self.log.warning("[📦] Backfill drain timed out for #%s with %d in-flight",
                                cid, self._inflight.get(cid, 0))
                break
            await asyncio.sleep(0.05)
            
    def unmark_backfill(self, original_id: int) -> None:
        """
        Removes the specified ID from the backfill flags.
        """
        self._flags.discard(int(original_id))
        
    async def _ensure_temp_webhook(self, clone_channel_id: int) -> tuple[int | None, str | None]:
        """Create a temporary webhook in the cloned channel; return (id, url) or (None, None)."""
        try:
            ch = self.bot.get_channel(clone_channel_id) or await self.bot.fetch_channel(clone_channel_id)
            wh = await ch.create_webhook(name="Copycord Temp", reason="Backfill rotation")
            return wh.id, wh.url
        except Exception as e:
            self.log.warning("[⚠️] Could not create temp webhook in #%s: %s", clone_channel_id, e)
            return None, None
        
    def choose_url(self, clone_channel_id: int, primary_url: str) -> str:
        """
        Selects a URL to use for a given clone channel ID, rotating between the primary URL
        and a temporary URL if available.
        """
        pool = self._rotate_pool.get(clone_channel_id)
        if not pool:
            sink_key = self._by_clone.get(clone_channel_id)
            temp_url = None
            if sink_key is not None:
                st = self._progress.get(sink_key) or {}
                temp_url = st.get("temp_webhook_url")
            if not temp_url:
                return primary_url
            self._rotate_pool[clone_channel_id] = pool = [primary_url, temp_url]
            self._rotate_idx[clone_channel_id] = 1

        idx = self._rotate_idx.get(clone_channel_id, 0)
        idx ^= 1
        self._rotate_idx[clone_channel_id] = idx
        return pool[idx]
    
    async def cleanup_orphan_temp_webhooks(self) -> None:
        """
        On startup, remove any temporary webhooks we created previously but didn't delete.
        Looks for webhooks named 'Copycord Temp' in all cloned channels.
        Safe to run even if there are none; logs what it finds.
        """
        try:
            if getattr(self.r, "_shutting_down", False):
                return

            # Make sure mappings are loaded
            self.r._load_mappings()

            guild = self.bot.get_guild(self.r.clone_guild_id)
            if not guild:
                self.log.warning("[cleanup] Clone guild %s not available; skipping temp webhook cleanup",
                                 self.r.clone_guild_id)
                return

            # unique set of cloned channel IDs from the current mapping
            clone_ids = {row["cloned_channel_id"] for row in self.r.chan_map.values() if row.get("cloned_channel_id")}
            if not clone_ids:
                self.log.debug("[cleanup] No cloned channels in mapping; nothing to clean")
                return

            deleted = 0
            checked = 0

            for cid in clone_ids:
                if getattr(self.r, "_shutting_down", False):
                    break

                ch = guild.get_channel(int(cid))
                if not ch:
                    try:
                        ch = await self.bot.fetch_channel(int(cid))
                    except Exception:
                        ch = None
                if not ch:
                    continue

                try:
                    hooks = await ch.webhooks()
                except Exception as e:
                    self.log.debug("[cleanup] Could not list webhooks for #%s: %s", cid, e)
                    continue

                for wh in hooks:
                    # Heuristic: name match, and (if available) created by this bot
                    is_temp_name = (wh.name or "").strip().lower() == "copycord temp"
                    made_by_us = False
                    try:
                        # wh.user may be None if not expanded; guard it
                        if getattr(wh, "user", None) and getattr(wh.user, "id", None):
                            made_by_us = (wh.user.id == self.bot.user.id)
                    except Exception:
                        pass

                    if is_temp_name and (made_by_us or True):
                        try:
                            await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)
                            await wh.delete(reason="Startup cleanup of orphan temp webhook")
                            deleted += 1
                            self.log.info("[🧹] Deleted orphan temp webhook %s in #%s", wh.id, ch.name)
                        except Exception as e:
                            self.log.warning("[⚠️] Failed to delete temp webhook %s in #%s: %s", wh.id, ch.name, e)

                checked += 1
                await asyncio.sleep(0)  # yield to loop

            self.log.debug("[🧹] Temp webhook cleanup complete: checked %d channels, deleted %d webhooks",
                          checked, deleted)

        except Exception:
            self.log.exception("[cleanup] Unexpected error while cleaning temp webhooks")


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
