# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
from typing import Tuple, List, Optional
import asyncio, io
import aiohttp, discord, logging
from PIL import Image, ImageSequence
from server.rate_limiter import ActionType
from server import logctx

logger = logging.getLogger("server.emojis")


class EmojiManager:
    def __init__(
        self,
        bot,
        db,
        guild_resolver,
        ratelimit,
        clone_guild_id: int | None = None,
        session=None,
    ):
        self.bot = bot
        self.db = db
        self.ratelimit = ratelimit
        self.clone_guild_id = int(clone_guild_id or 0)
        self.session = session
        self.guild_resolver = guild_resolver

        self._tasks: dict[int, asyncio.Task] = {}

        self._locks: dict[int, asyncio.Lock] = {}

    def _log(self, level: str, msg: str, *args):
        """
        Emit a log line with the current sync context prefix injected.
        Example final line:
        [✉️][CC-Testing-Client] Sync task khp6n [😊] [CloneGuild] Created emoji party_parrot
        """
        prefix = logctx.format_prefix()
        full = prefix + msg

        if level == "debug":
            logger.debug(full, *args)
        elif level == "info":
            logger.info(full, *args)
        elif level == "warning":
            logger.warning(full, *args)
        elif level == "error":
            logger.error(full, *args)
        elif level == "exception":
            logger.exception(full, *args)
        else:
            logger.log(logging.INFO, full, *args)

    def set_session(self, session: aiohttp.ClientSession | None):
        self.session = session

    def _get_lock_for_clone(self, clone_gid: int) -> asyncio.Lock:
        """
        Return (and cache) the lock for this clone guild.
        We serialize emoji writes per clone guild, but different clone guilds
        can sync in parallel.
        """
        if clone_gid not in self._locks:
            self._locks[clone_gid] = asyncio.Lock()
        return self._locks[clone_gid]

    def kickoff_sync(
        self,
        emojis: list[dict],
        host_guild_id: int | None,
        target_clone_guild_id: int,
        *,
        validate_mapping: bool = True,
    ) -> None:
        host_id = int(host_guild_id) if host_guild_id else None
        clone_gid = int(target_clone_guild_id)

        existing = self._tasks.get(clone_gid)
        if existing and not existing.done():
            self._log(
                "debug",
                "[emoji] Sync already running; skip kickoff.",
            )
            return

        if validate_mapping and host_id is not None:
            try:
                clones = set(self.guild_resolver.clones_for_host(host_id))
                if clones and clone_gid not in clones:
                    self._log(
                        "warning",
                        "[emoji] host %s is not mapped to clone %s; proceeding anyway",
                        host_id,
                        clone_gid,
                    )
            except Exception:
                self._log(
                    "exception",
                    "[emoji] mapping validation threw, continuing anyway",
                )

        guild = self.bot.get_guild(clone_gid)
        if not guild:
            self._log(
                "debug",
                "[emoji] Clone guild unavailable; aborting sync.",
            )
            return

        async def _run_one():
            await self._run_sync_for_guild(
                guild=guild,
                emoji_data=emojis or [],
                host_guild_id=host_id,
                clone_gid=clone_gid,
            )

        self._log("debug", "[😊] Emoji sync scheduled for clone=%s", clone_gid)
        task = asyncio.create_task(_run_one())
        self._tasks[clone_gid] = task

    async def _run_sync_for_guild(
        self,
        guild: discord.Guild,
        emoji_data: list[dict],
        host_guild_id: Optional[int],
        clone_gid: int,
    ):

        return await self._run_sync(
            guild=guild,
            emoji_data=emoji_data,
            host_guild_id=host_guild_id,
            clone_gid=clone_gid,
        )

    async def _run_sync(
        self,
        guild: discord.Guild,
        emoji_data: list[dict],
        host_guild_id: Optional[int],
        clone_gid: int,
    ) -> None:
        lock = self._get_lock_for_clone(clone_gid)

        async with lock:
            try:
                d, r, c = await self._sync(guild, emoji_data, host_guild_id)

                changes = []
                if d:
                    changes.append(f"Deleted {d} emojis")
                if r:
                    changes.append(f"Renamed {r} emojis")
                if c:
                    changes.append(f"Created {c} emojis")

                if changes:
                    self._log(
                        "info",
                        "[😊] Emoji sync complete: %s",
                        "; ".join(changes),
                    )
                else:
                    self._log(
                        "info",
                        "[😊] Emoji sync complete: no changes needed"
                    )

            except asyncio.CancelledError:
                self._log(
                    "debug",
                    "[😊] Emoji sync task was canceled before completion."
                )
            except Exception as e:
                self._log(
                    "error",
                    "[😊] Emoji sync failed: %s", e
                )
            finally:
                task = self._tasks.get(clone_gid)
                if task and task.done():
                    self._tasks.pop(clone_gid, None)

    async def _sync(
        self, guild, emojis, host_guild_id: Optional[int]
    ) -> tuple[int, int, int]:
        """
        Mirror host custom emojis → clone guild, handling deletions, renames, and creations
        with static/animated limits and size shrinking.
        """
        deleted = renamed = created = 0
        skipped_limit_static = skipped_limit_animated = size_failed = 0

        static_count = sum(1 for e in guild.emojis if not e.animated)
        animated_count = sum(1 for e in guild.emojis if e.animated)
        limit = guild.emoji_limit

        rows = self.db.get_all_emoji_mappings()
        current: dict[int, dict] = {}
        for r in rows:
            row = dict(r)
            if (
                host_guild_id is None
                or int(row.get("original_guild_id") or 0) == int(host_guild_id)
            ) and int(row.get("cloned_guild_id") or 0) == int(guild.id):
                current[int(row["original_emoji_id"])] = row

        incoming = {e["id"]: e for e in emojis}

        for orig_id in set(current) - set(incoming):
            row = current[orig_id]
            cloned = discord.utils.get(guild.emojis, id=row["cloned_emoji_id"])
            if cloned:
                try:
                    await self.ratelimit.acquire_for_guild(ActionType.EMOJI, guild.id)
                    await cloned.delete()
                    deleted += 1
                    self._log(
                        "info",
                        "[😊] Deleted emoji %s",
                        row["cloned_emoji_name"],
                    )
                except discord.Forbidden:
                    self._log(
                        "warning",
                        "[⚠️] No permission to delete emoji %s",
                        getattr(cloned, "name", orig_id),
                    )
                except discord.HTTPException as e:
                    self._log(
                        "error",
                        "[⛔] Error deleting emoji: %s",
                        e,
                    )
            self.db.delete_emoji_mapping_for_clone(orig_id, cloned_guild_id=guild.id)

        for orig_id, info in incoming.items():
            name = info["name"]
            url = info["url"]
            is_animated = info.get("animated", False)
            mapping = current.get(orig_id)
            cloned = mapping and discord.utils.get(
                guild.emojis, id=mapping["cloned_emoji_id"]
            )

            if mapping and not cloned:
                self._log(
                    "warning",
                    "[⚠️] Emoji %s missing in clone; will recreate",
                    mapping["original_emoji_name"],
                )
                self.db.delete_emoji_mapping_for_clone(orig_id, cloned_guild_id=guild.id)
                mapping = cloned = None

            if mapping and cloned and cloned.name != name:
                try:
                    await self.ratelimit.acquire_for_guild(ActionType.EMOJI, guild.id)
                    await cloned.edit(name=name)
                    renamed += 1
                    self._log(
                        "info",
                        "[😊] Restored emoji %s → %s",
                        cloned.name,
                        name,
                    )
                    self.db.upsert_emoji_mapping(
                        orig_id,
                        name,
                        cloned.id,
                        name,
                        original_guild_id=host_guild_id,
                        cloned_guild_id=guild.id,
                    )
                except discord.HTTPException as e:
                    self._log(
                        "error",
                        "[⛔] Failed restoring emoji %s: %s",
                        getattr(cloned, "name", "?"),
                        e,
                    )
                continue

            if mapping and cloned and mapping["original_emoji_name"] != name:
                try:
                    await self.ratelimit.acquire_for_guild(ActionType.EMOJI, guild.id)
                    await cloned.edit(name=name)
                    renamed += 1
                    self._log(
                        "info",
                        "[😊] Renamed emoji %s → %s",
                        mapping["original_emoji_name"],
                        name,
                    )
                    self.db.upsert_emoji_mapping(
                        orig_id,
                        name,
                        cloned.id,
                        cloned.name,
                        original_guild_id=host_guild_id,
                        cloned_guild_id=guild.id,
                    )
                except discord.HTTPException as e:
                    self._log(
                        "error",
                        "[⛔] Failed renaming emoji %s: %s",
                        getattr(cloned, "name", "?"),
                        e,
                    )
                continue

            if mapping:
                continue

            if is_animated and animated_count >= limit:
                skipped_limit_animated += 1
                continue
            if not is_animated and static_count >= limit:
                skipped_limit_static += 1
                continue

            try:
                if self.session is None or self.session.closed:
                    self.session = aiohttp.ClientSession()
                async with self.session.get(url) as resp:
                    raw = await resp.read()
            except Exception as e:
                self._log(
                    "error",
                    "[⛔] Failed fetching emoji %s: %s",
                    name,
                    e,
                )
                continue

            try:
                if is_animated:
                    raw = await self._shrink_animated(raw, max_bytes=262_144)
                else:
                    raw = await self._shrink_static(raw, max_bytes=262_144)
            except Exception as e:
                self._log(
                    "error",
                    "[⛔] Error shrinking emoji %s: %s",
                    name,
                    e,
                )

            try:
                await self.ratelimit.acquire_for_guild(ActionType.EMOJI, guild.id)
                created_emo = await guild.create_custom_emoji(name=name, image=raw)
                created += 1
                self._log(
                    "info",
                    "[😊] Created emoji %s",
                    name,
                )
                self.db.upsert_emoji_mapping(
                    orig_id,
                    name,
                    created_emo.id,
                    created_emo.name,
                    original_guild_id=host_guild_id,
                    cloned_guild_id=guild.id,
                )
                if created_emo.animated:
                    animated_count += 1
                else:
                    static_count += 1
            except discord.HTTPException as e:
                if "50138" in str(e):
                    size_failed += 1
                else:
                    self._log(
                        "error",
                        "[⛔] Failed creating %s: %s",
                        name,
                        e,
                    )

        if skipped_limit_static or skipped_limit_animated:
            self._log(
                "info",
                "[😊] Skipped %d static and %d animated due to limit (%d). Consider boosting.",
                skipped_limit_static,
                skipped_limit_animated,
                limit,
            )
        if size_failed:
            self._log(
                "info",
                "[😊] Skipped some emojis because they still exceed 256 KiB after conversion."
            )
        return deleted, renamed, created

    async def _shrink_static(self, data: bytes, max_bytes: int) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._sync_shrink_static, data, max_bytes
        )

    def _sync_shrink_static(self, data: bytes, max_bytes: int) -> bytes:
        img = Image.open(io.BytesIO(data)).convert("RGBA")
        img.thumbnail((128, 128), Image.LANCZOS)

        out = io.BytesIO()
        img.save(out, format="PNG", optimize=True)
        result = out.getvalue()
        if len(result) <= max_bytes:
            return result

        out = io.BytesIO()
        img.convert("P", palette=Image.ADAPTIVE).save(out, format="PNG", optimize=True)
        result = out.getvalue()
        return result if len(result) <= max_bytes else data

    async def _shrink_animated(self, data: bytes, max_bytes: int) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._sync_shrink_animated, data, max_bytes
        )

    def _sync_shrink_animated(self, data: bytes, max_bytes: int) -> bytes:
        buf = io.BytesIO(data)
        img = Image.open(buf)
        frames, durations = [], []
        for frame in ImageSequence.Iterator(img):
            f = frame.convert("RGBA")
            f.thumbnail((128, 128), Image.LANCZOS)
            frames.append(f)
            durations.append(frame.info.get("duration", 100))

        out = io.BytesIO()
        frames[0].save(
            out,
            format="GIF",
            save_all=True,
            append_images=frames[1:],
            duration=durations,
            loop=0,
            optimize=True,
        )
        result = out.getvalue()
        return result if len(result) <= max_bytes else data
