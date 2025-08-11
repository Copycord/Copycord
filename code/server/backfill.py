import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

import discord


class BackfillManager:
    def __init__(self, receiver):
        self.r = receiver
        self.bot = receiver.bot
        self.log = logging.getLogger("server")

        self._flags: set[int] = set()              
        self._progress: dict[int, dict] = {}       
        self._inflight = defaultdict(int)         
        self._by_clone: dict[int, int] = {}         
        self._rotate_pool: dict[int, list[str]] = {} 
        self._rotate_idx: dict[int, int] = {}       

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

    async def on_started(self, original_id: int) -> None:
        """
        Handles the initialization process when an operation starts.
        """
        cid = int(original_id)
        self._flags.add(cid)
        if cid not in self._progress:
            self.register_sink(cid, user_id=None, clone_channel_id=None, msg=None)

        st = self._progress.get(cid) or {}
        clone_id = st.get("clone_channel_id")
        if clone_id:
            temp_id, temp_url = await self._ensure_temp_webhook(clone_id)
            st["temp_webhook_id"] = temp_id
            st["temp_webhook_url"] = temp_url


    async def on_progress(self, original_id: int, count: int) -> None:
        """
        Handles progress updates for a backfilling operation.
        """
        st = self._progress.get(int(original_id))
        if not st or not st.get("msg"):
            return
        now = asyncio.get_event_loop().time()
        if (now - st["last_edit_ts"] >= 2.0) or (count - st["last_count"] >= 100):
            try:
                elapsed = int(now - st["started_at"])
                await st["msg"].edit(content=f"📦 Backfilling… **{count}** messages (elapsed: {elapsed}s)")
                st["last_edit_ts"] = now
                st["last_count"] = count
            except Exception:
                pass

    async def on_done(self, original_id: int) -> None:
        """
        Handles the completion of a backfill operation for a specific channel.
        """
        cid = int(original_id)
        await self._wait_drain(cid, timeout=None) 
        self._flags.discard(cid)
        await self.r._flush_channel_buffer(cid)

        st = self._progress.get(cid) or {}
        clone_id = st.get("clone_channel_id")
        temp_id = st.get("temp_webhook_id")
        total = st["last_count"] if st else 0
        elapsed_s = int(asyncio.get_event_loop().time() - st["started_at"]) if st else 0
        finished_dt = datetime.now(timezone.utc)

        # DM summary if we know who invoked
        uid = st.get("user_id") if st else None
        if uid:
            clone_id = st.get("clone_channel_id")
            ch_value = f"<#{clone_id}>" if clone_id else f"Original #{cid}"

            embed = discord.Embed(
                title="💬 Backfill Complete",
                description="Your channel messages have been cloned.",
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
                self.log.warning("[⚠️] Could not DM user %s: %s", uid, e)
                
        if temp_id:
            try:
                wh = await self.bot.fetch_webhook(temp_id)
                await wh.delete(reason="Backfill rotation cleanup")
            except Exception as e:
                self.log.warning("[⚠️] Could not delete temp webhook %s: %s", temp_id, e)

        if clone_id:
            self._rotate_pool.pop(int(clone_id), None)
            self._rotate_idx.pop(int(clone_id), None)
            self._by_clone.pop(int(clone_id), None)


        await self.clear_sink(cid)
        self.log.info("[📦] Backfill finished for #%s; buffered messages flushed", cid)

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
