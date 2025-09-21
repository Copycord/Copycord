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
from server.rate_limiter import RateLimitManager, ActionType

logger = logging.getLogger("server.emojis")

class EmojiManager:
    def __init__(
        self,
        bot: discord.Bot,
        db,
        ratelimit: RateLimitManager,
        clone_guild_id: int,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.bot = bot
        self.db = db
        self.ratelimit = ratelimit
        self.clone_guild_id = clone_guild_id
        self.session = session

        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    # ---------- lifecycle ----------
    def set_session(self, session: aiohttp.ClientSession | None):
        self.session = session

    def kickoff_sync(self, emojis: list[dict]) -> None:
        """Schedule a background emoji sync if not running."""
        if self._task and not self._task.done():
            logger.debug("Emoji sync already running; skip kickoff.")
            return
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.debug("kickoff_sync: clone guild not available yet; skip.")
            return
        logger.debug("[😊] Emoji sync task scheduled.")
        self._task = asyncio.create_task(self._run_sync(guild, emojis or []))

    async def _run_sync(self, guild: discord.Guild, emoji_data: list[dict]) -> None:
        async with self._lock:
            try:
                d, r, c = await self._sync(guild, emoji_data)
                changes = []
                if d: changes.append(f"Deleted {d} emojis")
                if r: changes.append(f"Renamed {r} emojis")
                if c: changes.append(f"Created {c} emojis")
                if changes:
                    logger.info("[😊] Emoji sync changes: " + "; ".join(changes))
                else:
                    logger.debug("[😊] Emoji sync: no changes needed")
            except asyncio.CancelledError:
                logger.debug("[😊] Emoji sync task was canceled before completion.")
            except Exception:
                logger.exception("[😊] Emoji sync failed")
            finally:
                self._task = None

    # ---------- core sync ----------
    async def _sync(self, guild: discord.Guild, emojis: list[dict]) -> Tuple[int, int, int]:
        """
        Mirror host custom emojis → clone guild, handling deletions, renames, and creations
        with static/animated limits and size shrinking.
        """
        deleted = renamed = created = 0
        skipped_limit_static = skipped_limit_animated = size_failed = 0

        # Count existing static vs animated
        static_count = sum(1 for e in guild.emojis if not e.animated)
        animated_count = sum(1 for e in guild.emojis if e.animated)
        limit = guild.emoji_limit

        # Build lookup tables
        current = {r["original_emoji_id"]: r for r in self.db.get_all_emoji_mappings()}
        incoming = {e["id"]: e for e in emojis}

        # Deletions
        for orig_id in set(current) - set(incoming):
            row = current[orig_id]
            cloned = discord.utils.get(guild.emojis, id=row["cloned_emoji_id"])
            if cloned:
                try:
                    await self.ratelimit.acquire(ActionType.EMOJI)
                    await cloned.delete()
                    deleted += 1
                    logger.info(f"[😊] Deleted emoji {row['cloned_emoji_name']}")
                except discord.Forbidden:
                    logger.warning(f"[⚠️] No permission to delete emoji {getattr(cloned,'name',orig_id)}")
                except discord.HTTPException as e:
                    logger.error(f"[⛔] Error deleting emoji: {e}")
            self.db.delete_emoji_mapping(orig_id)

        # Upserts
        for orig_id, info in incoming.items():
            name = info["name"]
            url = info["url"]
            is_animated = info.get("animated", False)
            mapping = current.get(orig_id)
            cloned = mapping and discord.utils.get(guild.emojis, id=mapping["cloned_emoji_id"])

            # Orphaned mapping (deleted manually)
            if mapping and not cloned:
                logger.warning(f"[⚠️] Emoji {mapping['original_emoji_name']} missing in clone; will recreate")
                self.db.delete_emoji_mapping(orig_id)
                mapping = cloned = None

            # Repair manual rename in clone
            if mapping and cloned and cloned.name != name:
                try:
                    await self.ratelimit.acquire(ActionType.EMOJI)
                    await cloned.edit(name=name)
                    renamed += 1
                    logger.info(f"[😊] Restored emoji {cloned.name} → {name}")
                    self.db.upsert_emoji_mapping(orig_id, name, cloned.id, name)
                except discord.HTTPException as e:
                    logger.error(f"[⛔] Failed restoring emoji {getattr(cloned,'name','?')}: {e}")
                continue

            # Upstream rename
            if mapping and cloned and mapping["original_emoji_name"] != name:
                try:
                    await self.ratelimit.acquire(ActionType.EMOJI)
                    await cloned.edit(name=name)
                    renamed += 1
                    logger.info(f"[😊] Renamed emoji {mapping['original_emoji_name']} → {name}")
                    self.db.upsert_emoji_mapping(orig_id, name, cloned.id, cloned.name)
                except discord.HTTPException as e:
                    logger.error(f"[⛔] Failed renaming emoji {getattr(cloned,'name','?')}: {e}")
                continue

            # Already up-to-date
            if mapping:
                continue

            # Enforce limits
            if is_animated and animated_count >= limit:
                skipped_limit_animated += 1
                continue
            if not is_animated and static_count >= limit:
                skipped_limit_static += 1
                continue

            # Fetch bytes
            try:
                if self.session is None or self.session.closed:
                    self.session = aiohttp.ClientSession()
                async with self.session.get(url) as resp:
                    raw = await resp.read()
            except Exception as e:
                logger.error(f"[⛔] Failed fetching {url}: {e}")
                continue

            # Shrink to ≤256 KiB
            try:
                if is_animated:
                    raw = await self._shrink_animated(raw, max_bytes=262_144)
                else:
                    raw = await self._shrink_static(raw, max_bytes=262_144)
            except Exception as e:
                logger.error(f"[⛔] Error shrinking emoji {name}: {e}")

            # Create
            try:
                await self.ratelimit.acquire(ActionType.EMOJI)
                created_emo = await guild.create_custom_emoji(name=name, image=raw)
                created += 1
                logger.info(f"[😊] Created emoji {name}")
                self.db.upsert_emoji_mapping(orig_id, name, created_emo.id, created_emo.name)
                if created_emo.animated:
                    animated_count += 1
                else:
                    static_count += 1
            except discord.HTTPException as e:
                if "50138" in str(e):
                    size_failed += 1
                else:
                    logger.error(f"[⛔] Failed creating {name}: {e}")

        if skipped_limit_static or skipped_limit_animated:
            logger.info(
                f"[😊] Skipped {skipped_limit_static} static and {skipped_limit_animated} animated emojis "
                f"due to guild limit ({limit}). Guild needs boosting to increase this limit."
            )
        if size_failed:
            logger.info("[😊] Skipped some emojis because they still exceed 256 KiB after conversion.")
        return deleted, renamed, created

    # ---------- helpers (moved from server.py) ----------
    async def _shrink_static(self, data: bytes, max_bytes: int) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync_shrink_static, data, max_bytes)

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
        return await loop.run_in_executor(None, self._sync_shrink_animated, data, max_bytes)

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
            out, format="GIF", save_all=True, append_images=frames[1:],
            duration=durations, loop=0, optimize=True
        )
        result = out.getvalue()
        return result if len(result) <= max_bytes else data