import signal
import asyncio
import logging
from typing import List, Optional, Tuple, Dict, Union
import aiohttp
import discord
import re
from discord import (
    ForumChannel,
    NotFound,
    Webhook,
    ChannelType,
    Embed,
    Guild,
    TextChannel,
    CategoryChannel,
)
from discord.errors import HTTPException, Forbidden
import os
import sys
import io
from PIL import Image, ImageSequence
from datetime import datetime, timezone
from asyncio import Queue
from common.config import Config
from common.websockets import WebsocketManager
from common.db import DBManager
from common.rate_limiter import RateLimitManager, ActionType

LOG_DIR = "/data"
os.makedirs(LOG_DIR, exist_ok=True)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

root = logging.getLogger()
root.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
root.addHandler(ch)

log_file = os.path.join(LOG_DIR, "server.log")
fh = logging.FileHandler(log_file, encoding="utf-8")
fh.setFormatter(formatter)
root.addHandler(fh)

for name in ("websockets.server", "websockets.protocol"):
    logging.getLogger(name).setLevel(logging.WARNING)
for lib in (
    "discord",
    "discord.client",
    "discord.gateway",
    "discord.state",
    "discord.http",
):
    logging.getLogger(lib).setLevel(logging.WARNING)

logger = logging.getLogger("server")


class ServerReceiver:
    def __init__(self):
        self.config = Config()
        self.bot = discord.Bot(intents=discord.Intents.all())
        self.ws = WebsocketManager(
            send_url=self.config.CLIENT_WS_URL,
            listen_host=self.config.SERVER_WS_HOST,
            listen_port=self.config.SERVER_WS_PORT,
        )
        self.clone_guild_id = int(self.config.CLONE_GUILD_ID)
        self.bot.ws_manager = self.ws
        self.db = DBManager(self.config.DB_PATH)
        self.session: aiohttp.ClientSession = None
        self.sitemap_queue: Queue = Queue()
        self._processor_started = False
        self._sitemap_task_counter = 0
        self._sync_lock = asyncio.Lock()
        self._thread_locks: dict[int, asyncio.Lock] = {}
        self.max_threads = 950
        self.bot.event(self.on_ready)
        self._ws_task: asyncio.Task | None = None
        self._sitemap_task: asyncio.Task | None = None
        self._pending_msgs: dict[int, list[dict]] = {}
        self._pending_thread_msgs: List[Dict] = []
        orig_on_connect = self.bot.on_connect
        self.ratelimit = RateLimitManager()
        # Discord guild/channel limits
        self.MAX_GUILD_CHANNELS = 500
        self.MAX_CATEGORIES = 50
        self.MAX_CHANNELS_PER_CATEGORY = 50
        self._EMOJI_RE = re.compile(r'<(a?):(?P<name>[^:]+):(?P<id>\d+)>')

        async def _command_sync():
            try:
                await orig_on_connect()
            except Forbidden as e:
                logger.warning(
                    "Can't sync slash commands, make sure the bot is in the server: %s",
                    e,
                )

        self.bot.on_connect = _command_sync
        self.bot.load_extension("commands.commands")

    async def on_ready(self):
        self.config.setup_release_watcher(self)
        self.session = aiohttp.ClientSession()
        # Ensure we're in the clone guild
        clone_guild = self.bot.get_guild(self.clone_guild_id)
        if clone_guild is None:
            logger.error(
                "Bot (ID %s) is not a member of the guild %s; shutting down.",
                self.bot.user.id,
                self.clone_guild_id,
            )
            await self.bot.close()
            sys.exit(1)

        logger.info(
            "Logged in as %s and monitoring guild %s ", self.bot.user, clone_guild.name
        )

        if not self._processor_started:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))
            self._sitemap_task = asyncio.create_task(self.process_sitemap_queue())
            self._processor_started = True

    async def _on_ws(self, msg: dict):
        """
        JSON-decoded msg dict sent by the client.
        """
        typ = msg.get("type")
        data = msg.get("data", {})
        if typ == "sitemap":
            self._sitemap_task_counter += 1
            task_id = self._sitemap_task_counter
            self.sitemap_queue.put_nowait((task_id, data))
            logger.info("Sync task #%d received", task_id)
            logger.debug(
                "Sync task #%d (queue size now: %d)",
                task_id,
                self.sitemap_queue.qsize(),
            )

        elif typ == "message":
            asyncio.create_task(self.forward_message(data))

        elif typ == "thread_message":
            asyncio.create_task(self.handle_thread_message(data))

        elif typ == "thread_delete":
            asyncio.create_task(self.handle_thread_delete(data))

        elif typ == "thread_rename":
            asyncio.create_task(self.handle_thread_rename(data))

        else:
            logger.warning("Unknown WS type '%s'", typ)

    async def process_sitemap_queue(self):
        """Continuously process only the newest sitemap, discarding any others."""
        first = True
        while True:
            if not first:
                logger.debug("Waiting 5s before processing next sitemap…")
                await asyncio.sleep(5)
            first = False

            task_id, sitemap = await self.sitemap_queue.get()

            qsize = self.sitemap_queue.qsize()
            if qsize:
                logger.debug(
                    "Dropping %d outdated sitemap(s), will process only the newest (task #%d).",
                    qsize,
                    task_id,
                )
            while True:
                try:
                    old_id, old_map = self.sitemap_queue.get_nowait()
                    self.sitemap_queue.task_done()
                    task_id, sitemap = old_id, old_map
                except asyncio.QueueEmpty:
                    break

            logger.debug(
                "Starting sync task #%d (queue size then: %d)",
                task_id,
                self.sitemap_queue.qsize(),
            )

            try:
                summary = await self.sync_structure(task_id, sitemap)
            except Exception:
                logger.exception("Error processing sitemap %d", task_id)
            else:
                logger.info("Sync task #%d completed: %s", task_id, summary)
            finally:
                self.sitemap_queue.task_done()

    async def _sync_emojis(
        self, guild: discord.Guild, emojis: list[dict]
    ) -> tuple[int, int, int]:
        """
        Mirror the host guild’s custom emojis into the clone guild,
        handling static vs animated limits, deletions, renames, and creations.
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

        # 1) Delete emojis removed upstream
        for orig_id in set(current) - set(incoming):
            row = current[orig_id]
            cloned = discord.utils.get(guild.emojis, id=row["cloned_emoji_id"])
            if cloned:
                try:
                    await self.ratelimit.acquire(ActionType.EMOJI)
                    await cloned.delete()
                    deleted += 1
                    logger.info(f"Deleted emoji {row['cloned_emoji_name']}")
                except discord.Forbidden:
                    logger.warning(f"No permission to delete emoji {cloned.name}")
                except discord.HTTPException as e:
                    logger.error(f"Error deleting emoji {cloned.name}: {e}")
            self.db.delete_emoji_mapping(orig_id)

        # 2) Process all incoming (create / rename / repair)
        for orig_id, info in incoming.items():
            name = info["name"]
            url = info["url"]
            is_animated = info.get("animated", False)
            mapping = current.get(orig_id)
            cloned = mapping and discord.utils.get(
                guild.emojis, id=mapping["cloned_emoji_id"]
            )

            # 2a) Orphaned mapping? (deleted manually in clone)
            if mapping and not cloned:
                logger.warning(
                    f"Emoji {mapping['original_emoji_name']} stored in mapping, but missing; will recreate"
                )
                self.db.delete_emoji_mapping(orig_id)
                mapping = cloned = None

            # 2b) Repair manual rename in clone
            if mapping and cloned.name != name:
                try:
                    await self.ratelimit.acquire(ActionType.EMOJI)
                    await cloned.edit(name=name)
                    renamed += 1
                    logger.info(f"Restored emoji {cloned.name} → {name}")
                    self.db.upsert_emoji_mapping(orig_id, name, cloned.id, name)
                except discord.HTTPException as e:
                    logger.error(f"Failed restoring emoji {cloned.name}: {e}")
                continue

            # 2c) Upstream rename
            if mapping and mapping["original_emoji_name"] != name:
                try:
                    await self.ratelimit.acquire(ActionType.EMOJI)
                    await cloned.edit(name=name)
                    renamed += 1
                    logger.info(
                        f"Renamed emoji {mapping['original_emoji_name']} → {name}"
                    )
                    self.db.upsert_emoji_mapping(orig_id, name, cloned.id, cloned.name)
                except discord.HTTPException as e:
                    logger.error(f"Failed renaming emoji {cloned.name}: {e}")
                continue

            # skip up‐to‐date
            if mapping:
                continue

            # enforce limits
            if is_animated and animated_count >= limit:
                skipped_limit_animated += 1
                continue
            if not is_animated and static_count >= limit:
                skipped_limit_static += 1
                continue

            # fetch raw bytes
            try:
                async with self.session.get(url) as resp:
                    raw = await resp.read()
            except Exception as e:
                logger.error(f"Failed fetching {url}: {e}")
                continue

            # attempt to shrink to ≤256 KiB
            try:
                if is_animated:
                    raw = await self._shrink_animated(raw, max_bytes=262_144)
                else:
                    raw = await self._shrink_static(raw, max_bytes=262_144)
            except Exception as e:
                logger.error(f"Error shrinking emoji {name}: {e}")

            # create emoji
            try:
                await self.ratelimit.acquire(ActionType.EMOJI)
                created_emo = await guild.create_custom_emoji(name=name, image=raw)
                created += 1
                logger.info(f"Created emoji {name}")
                self.db.upsert_emoji_mapping(
                    orig_id, name, created_emo.id, created_emo.name
                )
                if created_emo.animated:
                    animated_count += 1
                else:
                    static_count += 1
            except discord.HTTPException as e:
                if "50138" in str(e):
                    size_failed += 1
                else:
                    logger.error(f"Failed creating {name}: {e}")

        # summary
        if skipped_limit_static or skipped_limit_animated:
            logger.info(
                f"Skipped {skipped_limit_static} static and "
                f"{skipped_limit_animated} animated emojis due to guild limit ({limit}). Guild needs boosting to increase this limit."
            )
        if size_failed:
            logger.info(
                f"Skipped {size_failed} emojis because they still exceed 256 KiB after conversion attempt."
            )

        return deleted, renamed, created

    # ─── emoji shrinking helpers ────────────────────────────────────────

    async def _shrink_static(self, data: bytes, max_bytes: int) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._sync_shrink_static, data, max_bytes
        )

    def _sync_shrink_static(self, data: bytes, max_bytes: int) -> bytes:
        buf = io.BytesIO(data)
        img = Image.open(buf).convert("RGBA")
        img.thumbnail((128, 128), Image.LANCZOS)

        out = io.BytesIO()
        img.save(out, format="PNG", optimize=True)
        result = out.getvalue()
        if len(result) <= max_bytes:
            return result

        out = io.BytesIO()
        quant = img.convert("P", palette=Image.ADAPTIVE)
        quant.save(out, format="PNG", optimize=True)
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
        frames = []
        durations = []

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


    async def sync_structure(self, task_id: int, sitemap: Dict) -> str:
        logging.debug(f"Received sitemap {sitemap}")
        async with self._sync_lock:
            self._backoff_delay = 1
            guild = self.bot.get_guild(self.clone_guild_id)
            if not guild:
                logger.error("Clone guild %s not found", self.clone_guild_id)
                return

            comm = sitemap.get("community", {})
            rules_id = comm.get("rules_channel_id")
            updates_id = comm.get("public_updates_channel_id")

            # ─── COMMUNITY SUPPORT CHECK ────────────────────────────────
            # Does the sitemap ask for community mode?
            want_comm = bool(comm.get("enabled"))
            # Does this guild actually support Community features?
            supports_comm = "COMMUNITY" in guild.features
            if want_comm and not supports_comm:
                logger.warning(
                    "Guild %s doesn’t support Community, some features will be disabled; Please enable it manually in the guild settings.",
                    guild.name,
                )
                want_comm = False

            logger.debug(
                "Sync #%d: sitemap has %d categories and %d standalone channels",
                task_id,
                len(sitemap.get("categories", [])),
                len(sitemap.get("standalone_channels", [])),
            )

            if not want_comm and supports_comm:
                try:
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await guild.edit(community=False)
                    logger.info("Disabled Community mode on clone guild %s", guild.id)
                except Forbidden as e:
                    logger.warning(
                        "Cannot disable Community mode on guild %s (missing permission): %s",
                        guild.id,
                        e,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to disable Community on clone guild %s: %s", guild.id, e
                    )

            if want_comm and rules_id and updates_id:
                for orig_chan_id in (rules_id, updates_id):
                    item = next(
                        (
                            i
                            for i in self._parse_sitemap(sitemap)
                            if i["id"] == orig_chan_id
                        ),
                        None,
                    )
                    if item:
                        await self._ensure_channel_and_webhook(
                            guild,
                            item["id"],
                            item["name"],
                            item["parent_id"],
                            item["parent_name"],
                            item["type"],
                        )

            if want_comm:
                rules_map = next(
                    (
                        r
                        for r in self.db.get_all_channel_mappings()
                        if r["original_channel_id"] == rules_id
                    ),
                    None,
                )
                updates_map = next(
                    (
                        r
                        for r in self.db.get_all_channel_mappings()
                        if r["original_channel_id"] == updates_id
                    ),
                    None,
                )
                if rules_map and updates_map:
                    rules_chan = guild.get_channel(rules_map["cloned_channel_id"])
                    updates_chan = guild.get_channel(updates_map["cloned_channel_id"])
                    if rules_chan and updates_chan:
                        need_enable = "COMMUNITY" not in guild.features
                        current_rules = (
                            guild.rules_channel.id if guild.rules_channel else None
                        )
                        current_updates = (
                            guild.public_updates_channel.id
                            if guild.public_updates_channel
                            else None
                        )
                        need_update = (
                            current_rules != rules_chan.id
                            or current_updates != updates_chan.id
                        )

                        if need_enable or need_update:
                            try:
                                await self.ratelimit.acquire(ActionType.EDIT)
                                await guild.edit(
                                    community=True,
                                    rules_channel=rules_chan,
                                    public_updates_channel=updates_chan,
                                )
                                logger.info(
                                    "%s Community mode on clone guild %s, set rules=%s updates=%s",
                                    "Enabled" if need_enable else "Updated",
                                    guild.id,
                                    rules_chan.id,
                                    updates_chan.id,
                                )
                            except Forbidden as e:
                                logger.warning(
                                    "Cannot enable/update Community mode on guild %s (missing permission): %s",
                                    guild.id,
                                    e,
                                )
                            except Exception as e:
                                logger.error(
                                    "Failed to enable/update Community mode on clone guild %s: %s",
                                    guild.id,
                                    e,
                                )

            if want_comm and rules_id and updates_id:
                for orig_chan_id in (rules_id, updates_id):
                    item = next(
                        (
                            i
                            for i in sitemap.get("standalone_channels", [])
                            + [
                                ch
                                for cat in sitemap.get("categories", [])
                                for ch in cat["channels"]
                            ]
                            if i["id"] == orig_chan_id
                        ),
                        None,
                    )
                    mapping = next(
                        (
                            r
                            for r in self.db.get_all_channel_mappings()
                            if r["original_channel_id"] == orig_chan_id
                        ),
                        None,
                    )
                    if not item or not mapping:
                        continue

                    clone_chan = guild.get_channel(mapping["cloned_channel_id"])
                    if clone_chan and clone_chan.name != item["name"]:
                        old = clone_chan.name
                        await self.ratelimit.acquire(ActionType.EDIT)
                        await clone_chan.edit(name=item["name"])
                        logger.info(
                            "Renamed channel '%s' → '%s' (ID %d)",
                            old,
                            item["name"],
                            clone_chan.id,
                        )
                        self.db.upsert_channel_mapping(
                            orig_chan_id,
                            item["name"],
                            mapping["cloned_channel_id"],
                            mapping["channel_webhook_url"],
                            mapping["original_parent_category_id"],
                            mapping["cloned_parent_category_id"],
                        )

            removed_chan = await self._handle_removed_channels(
                guild, self._parse_sitemap(sitemap)
            )
            removed_cat = await self._handle_removed_categories(guild, sitemap)
            renamed_cat = await self._handle_renamed_categories(guild, sitemap)


            old_map = {
                r["original_channel_id"]: r["original_parent_category_id"]
                for r in self.db.get_all_channel_mappings()
            }

            self._sync_db_with_sitemap(sitemap)

            created_cat = 0
            for cat in sitemap.get("categories", []):
                orig_cat_id = cat["id"]
                mapping = next(
                    (
                        r
                        for r in self.db.get_all_category_mappings()
                        if r["original_category_id"] == orig_cat_id
                    ),
                    None,
                )
                old_clone_id = mapping and mapping["cloned_category_id"]
                already_there = bool(old_clone_id and guild.get_channel(old_clone_id))

                clone_cat = await self._ensure_category(guild, orig_cat_id, cat["name"])
                if clone_cat is None:
                    continue
                if not already_there:
                    created_cat += 1
                    for row in self.db.get_all_channel_mappings():
                        if row["original_parent_category_id"] != orig_cat_id:
                            continue
                        ch = guild.get_channel(row["cloned_channel_id"])
                        if not ch or ch.category_id == clone_cat.id:
                            continue

                        if not self._can_create_in_category(guild, clone_cat):
                            logger.warning(
                                "Cannot move channel %d into full category '%s'; leaving it standalone",
                                ch.id,
                                clone_cat.name,
                            )
                            await self.ratelimit.acquire(ActionType.EDIT)
                            await ch.edit(category=None)
                        else:
                            await self.ratelimit.acquire(ActionType.EDIT)
                            await ch.edit(category=clone_cat)
                            logger.info(
                                "Reparented channel ID %d → category '%s' (ID %d)",
                                ch.id,
                                clone_cat.name,
                                clone_cat.id,
                            )

                        self.db.upsert_channel_mapping(
                            row["original_channel_id"],
                            row["original_channel_name"],
                            row["cloned_channel_id"],
                            row["channel_webhook_url"],
                            row["original_parent_category_id"],
                            clone_cat.id,
                        )
            created_forums = 0
            for forum in sitemap.get("forums", []):
                orig_forum_id = forum["id"]
                fm = next(
                    (
                        r
                        for r in self.db.get_all_channel_mappings()
                        if r["original_channel_id"] == orig_forum_id
                    ),
                    None,
                )

                parent = None
                if forum.get("category_id") is not None:
                    cat_map = next(
                        (
                            c
                            for c in self.db.get_all_category_mappings()
                            if c["original_category_id"] == forum["category_id"]
                        ),
                        None,
                    )
                    parent = (
                        guild.get_channel(cat_map["cloned_category_id"])
                        if cat_map
                        else None
                    )

                existed = bool(fm and guild.get_channel(fm["cloned_channel_id"]))
                if existed:
                    clone_forum = guild.get_channel(fm["cloned_channel_id"])
                else:
                    clone_forum = await self._create_channel(
                        guild, "forum", forum["name"], parent
                    )

                    created_forums += 1

                self.db.upsert_channel_mapping(
                    orig_forum_id,
                    forum["name"],
                    clone_forum.id,
                    fm["channel_webhook_url"] if existed else None,
                    forum["category_id"],
                    parent.id if parent else None,
                )
                if not existed:
                    new_url = await self._recreate_webhook(orig_forum_id)
                    if not new_url:
                        logger.error(
                            "Failed to create webhook for forum %s", orig_forum_id
                        )

            incoming = self._parse_sitemap(sitemap)
            created_chan = renamed_chan = 0
            for item in incoming:
                if comm.get("enabled") and item["id"] in (rules_id, updates_id):
                    continue
                mapping = next(
                    (
                        r
                        for r in self.db.get_all_channel_mappings()
                        if r["original_channel_id"] == item["id"]
                    ),
                    None,
                )
                real_clone = None
                if mapping and mapping["cloned_channel_id"] is not None:
                    real_clone = guild.get_channel(mapping["cloned_channel_id"])
                is_new = mapping is None or real_clone is None

                orig_id, clone_id, _ = await self._ensure_channel_and_webhook(
                    guild,
                    item["id"],
                    item["name"],
                    item["parent_id"],
                    item["parent_name"],
                    item["type"],
                )
                if is_new:
                    created_chan += 1

                ch = guild.get_channel(clone_id)
                if ch and ch.name != item["name"]:
                    old = ch.name
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await ch.edit(name=item["name"])
                    renamed_chan += 1
                    logger.info(
                        "Renamed channel %s → %s #%d",
                        old,
                        item["name"],
                        clone_id,
                    )

            moved_master = await self._handle_master_channel_moves(
                guild, incoming, old_map
            )

            delete_remote = getattr(self.config, "DELETE_CLONED_THREADS", True)
            valid_threads = {rec["id"] for rec in sitemap.get("threads", [])}
            deleted_threads = 0

            for row in self.db.get_all_threads():
                orig_id = row["original_thread_id"]
                clone_id = row["cloned_thread_id"]

                if orig_id not in valid_threads:
                    if delete_remote:
                        guild = self.bot.get_guild(self.clone_guild_id)
                        if guild:
                            ch = guild.get_channel(clone_id)
                            if not ch:
                                try:
                                    ch = await self.bot.fetch_channel(clone_id)
                                except discord.NotFound:
                                    ch = None
                            if ch:
                                if self.config.DELETE_THREADS:
                                    try:
                                        await self.ratelimit.acquire(ActionType.DELETE)
                                        await ch.delete()
                                        logger.info(
                                            "Deleted cloned thread %s for original %s",
                                            clone_id,
                                            orig_id,
                                        )
                                    except Exception as e:
                                        logger.error(
                                            "Failed deleting cloned thread %s: %s",
                                            clone_id,
                                            e,
                                        )

                    self.db.delete_forum_thread_mapping(orig_id)
                    deleted_threads += 1

            renamed_threads = 0
            removed_thread_mappings = 0
            for src in sitemap.get("threads", []):
                orig_tid = src["id"]
                new_name = src["name"]
                mapping = next(
                    (
                        r
                        for r in self.db.get_all_threads()
                        if r["original_thread_id"] == orig_tid
                    ),
                    None,
                )
                if not mapping:
                    continue

                clone_tid = mapping["cloned_thread_id"]
                ch = guild.get_channel(clone_tid)
                if not ch:
                    try:
                        ch = await self.bot.fetch_channel(clone_tid)
                    except discord.NotFound:
                        logger.info(
                            "Cloned thread %d no longer exists; removing its mapping",
                            orig_tid,
                        )
                        self.db.delete_forum_thread_mapping(orig_tid)
                        removed_thread_mappings += 1
                        continue

                if ch.name != new_name:
                    old = ch.name
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await ch.edit(name=new_name)
                    logger.info(
                        "Renamed cloned thread %s → %s",
                        old, new_name
                    )
                    self.db.upsert_forum_thread_mapping(
                        orig_tid,
                        new_name,
                        ch.id,
                        mapping["forum_original_id"],
                        mapping["cloned_thread_id"],
                    )
                    renamed_threads += 1
            # ─── EMOJI SYNC ────────────────────────────────────────
            if self.config.CLONE_EMOJI:
                emoji_deleted, emoji_renamed, emoji_created = await self._sync_emojis(
                    guild, sitemap.get("emojis", [])
                )
            else:
                logger.info("Emoji cloning is disabled, skipping sync.")
                emoji_deleted, emoji_renamed, emoji_created = 0, 0, 0
                
            parts = []
            if removed_cat:
                parts.append(f"Deleted {removed_cat} stale categories")
            if removed_chan:
                parts.append(f"Deleted {removed_chan} stale channels")
            if renamed_cat:
                parts.append(f"Renamed {renamed_cat} categories")
            if created_cat:
                parts.append(f"Created {created_cat} categories")
            if created_forums:
                parts.append(f"Created {created_forums} forum channels")
            if created_chan:
                parts.append(f"Created {created_chan} channels")
            if renamed_chan:
                parts.append(f"Renamed {renamed_chan} channels")
            if moved_master:
                parts.append(f"Reparented {moved_master} channels")
            if deleted_threads:
                parts.append(f"Deleted {deleted_threads} threads")
            if removed_thread_mappings:
                parts.append(f"Deleted {removed_thread_mappings} thread mappings")
            if renamed_threads:
                parts.append(f"Renamed {renamed_threads} threads")
            if emoji_deleted:
                parts.append(f"Deleted {emoji_deleted} emojis")
            if emoji_renamed:
                parts.append(f"Renamed {emoji_renamed} emojis")
            if emoji_created:
                parts.append(f"Created {emoji_created} emojis")
            if not parts:
                parts.append("No changes needed")

        # Flush regular-message buffer
        for source_id, msgs in list(self._pending_msgs.items()):
            for msg in msgs:
                await self.forward_message(msg)
            self._pending_msgs.pop(source_id, None)

        # Flush thread-message buffer
        for data in list(self._pending_thread_msgs):
            await self.handle_thread_message(data)
        self._pending_thread_msgs.clear()

        return "; ".join(parts)

    def _sync_db_with_sitemap(self, sitemap: Dict):
        incoming_cat = {cat["id"]: cat["name"] for cat in sitemap.get("categories", [])}
        incoming_ch = {item["id"]: item for item in self._parse_sitemap(sitemap)}

        existing_cats = {
            r["original_category_id"]: r for r in self.db.get_all_category_mappings()
        }
        for orig_id in list(existing_cats):
            if orig_id not in incoming_cat:
                self.db.delete_category_mapping(orig_id)
                existing_cats.pop(orig_id)
        for orig_id, name in incoming_cat.items():
            row = existing_cats.get(orig_id)
            if row:
                if row["original_category_name"] != name:
                    self.db.upsert_category_mapping(
                        orig_id,
                        name,
                        row["cloned_category_id"],
                        row["cloned_category_name"],
                    )
            else:
                self.db.upsert_category_mapping(orig_id, name, None, None)

        existing_ch = {
            r["original_channel_id"]: r for r in self.db.get_all_channel_mappings()
        }
        for orig_id in list(existing_ch):
            if orig_id not in incoming_ch:
                self.db.delete_channel_mapping(orig_id)
                existing_ch.pop(orig_id)
        for orig_id, item in incoming_ch.items():
            parent = item["parent_id"]
            parent_row = existing_cats.get(parent)
            clone_parent = parent_row["cloned_category_id"] if parent_row else None

            row = existing_ch.get(orig_id)
            if row:
                if (
                    row["original_channel_name"] != item["name"]
                    or row["original_parent_category_id"] != parent
                ):
                    self.db.upsert_channel_mapping(
                        orig_id,
                        item["name"],
                        row["cloned_channel_id"],
                        row["channel_webhook_url"],
                        parent,
                        clone_parent,
                    )
            else:
                self.db.upsert_channel_mapping(
                    orig_id,
                    item["name"],
                    None,
                    None,
                    parent,
                    clone_parent,
                )
        logger.debug(
            "DB sync: now %d category mappings, %d channel mappings",
            len(self.db.get_all_category_mappings()),
            len(self.db.get_all_channel_mappings()),
        )

    def _parse_sitemap(self, sitemap: Dict) -> List[Dict]:
        items: List[Dict] = []
        for cat in sitemap.get("categories", []):
            for ch in cat.get("channels", []):
                items.append(
                    {
                        "id": ch["id"],
                        "name": ch["name"],
                        "parent_id": cat["id"],
                        "parent_name": cat["name"],
                        "type": ch.get("type", 0),
                    }
                )
        for ch in sitemap.get("standalone_channels", []):
            items.append(
                {
                    "id": ch["id"],
                    "name": ch["name"],
                    "parent_id": None,
                    "parent_name": None,
                    "type": ch.get("type", 0),
                }
            )
        for forum in sitemap.get("forums", []):
            items.append(
                {
                    "id": forum["id"],
                    "name": forum["name"],
                    "parent_id": forum.get("category_id"),
                    "parent_name": None,
                    "type": ChannelType.forum.value,
                }
            )
        return items

    def _can_create_category(self, guild: discord.Guild) -> bool:
        return (
            len(guild.categories) < self.MAX_CATEGORIES
            and len(guild.channels) < self.MAX_GUILD_CHANNELS
        )

    def _can_create_in_category(
        self, guild: discord.Guild, category: Optional[discord.CategoryChannel]
    ) -> bool:
        if category is None:
            return len(guild.channels) < self.MAX_GUILD_CHANNELS
        return (
            len(category.channels) < self.MAX_CHANNELS_PER_CATEGORY
            and len(guild.channels) < self.MAX_GUILD_CHANNELS
        )

    async def _create_channel(
        self, guild: Guild, kind: str, name: str, category: CategoryChannel | None
    ) -> Union[TextChannel, ForumChannel]:
        """
        Create a channel of `kind` ('text'|'news'|'forum') named `name` under
        `category`.  If the category or guild is at capacity, it falls back to
        standalone (category=None).  Returns the created channel object.
        """
        if not self._can_create_in_category(guild, category):
            cat_label = category.name if category else "<root>"
            logger.warning(
                "Category %s full (or guild at cap); creating '%s' as standalone",
                cat_label,
                name,
            )
            category = None

        if kind == "forum":
            await self.ratelimit.acquire(ActionType.CREATE)
            ch = await guild.create_forum_channel(name=name, category=category)
        else:
            await self.ratelimit.acquire(ActionType.CREATE)
            ch = await guild.create_text_channel(name=name, category=category)

        logger.info("Created %s channel '%s' #%s", kind, name, ch.id)

        if kind == "news":
            if "NEWS" in guild.features:
                try:
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await ch.edit(type=ChannelType.news)
                    logger.info("Converted '%s' #%d to Announcement", name, ch.id)
                except HTTPException as e:
                    logger.warning(
                        "Could not convert '%s' to Announcement: %s; left as text",
                        name,
                        e,
                    )
            else:
                logger.warning(
                    "Guild %s doesn’t support NEWS; '%s' left as text", guild.id, name
                )
        return ch

    async def _handle_removed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        valid_ids = {c["id"] for c in sitemap.get("categories", [])}
        removed = 0
        for row in self.db.get_all_category_mappings():
            if row["original_category_id"] not in valid_ids:
                c = guild.get_channel(row["cloned_category_id"])
                if c:
                    if self.config.DELETE_CHANNELS:
                        await self.ratelimit.acquire(ActionType.DELETE)
                        await c.delete()
                        logger.info(
                            "Deleted category %s",
                            c.name,
                        )
                self.db.delete_category_mapping(row["original_category_id"])
                removed += 1
        return removed

    async def _handle_removed_channels(
        self, guild: discord.Guild, incoming: List[Dict]
    ) -> int:
        valid_ids = {c["id"] for c in incoming}
        removed = 0
        for row in self.db.get_all_channel_mappings():
            orig_id = row["original_channel_id"]
            clone_id = row["cloned_channel_id"]

            if orig_id not in valid_ids:
                ch = guild.get_channel(clone_id)
                if ch:
                    if self.config.DELETE_CHANNELS:
                        await self.ratelimit.acquire(ActionType.DELETE)
                        await ch.delete()
                        logger.info(
                            "Deleted channel %s #%d",
                            ch.name,
                            ch.id,
                        )
                else:
                    logger.info(
                        "Cloned channel #%d not found; removing mapping",
                        clone_id,
                    )

                self.db.delete_channel_mapping(orig_id)
                removed += 1
        return removed

    async def _handle_renamed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        renamed = 0
        mappings = {
            r["original_category_id"]: r for r in self.db.get_all_category_mappings()
        }
        for cat in sitemap.get("categories", []):
            orig_id, new_name = cat["id"], cat["name"]
            row = mappings.get(orig_id)
            if row:
                clone_cat = guild.get_channel(row["cloned_category_id"])
                if clone_cat and clone_cat.name != new_name:
                    old = clone_cat.name
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await clone_cat.edit(name=new_name)
                    logger.info("Renamed category %s → %s", old, new_name)
                    self.db.upsert_category_mapping(
                        orig_id, new_name, row["cloned_category_id"], new_name
                    )
                    renamed += 1
        return renamed

    async def _ensure_category(
        self, guild: discord.Guild, original_id: int, original_name: str
    ) -> Optional[CategoryChannel]:
        for row in self.db.get_all_category_mappings():
            if row["original_category_id"] == original_id:
                c = guild.get_channel(row["cloned_category_id"])
                if c:
                    logger.debug("Reusing category mapping %d → %d", original_id, c.id)
                    return c
        if not self._can_create_category(guild):
            logger.warning(
                "Cannot create new category '%s': guild has %d/%d channels or %d/%d categories",
                original_name,
                len(guild.channels),
                self.MAX_GUILD_CHANNELS,
                len(guild.categories),
                self.MAX_CATEGORIES,
            )
            return None
        await self.ratelimit.acquire(ActionType.CREATE)
        clone = await guild.create_category(original_name)
        logger.info(
            "Created category %s",
            original_name,
        )
        self.db.upsert_category_mapping(
            original_id, original_name, clone.id, original_name
        )
        return clone

    async def _ensure_channel_and_webhook(
        self,
        guild: discord.Guild,
        original_id: int,
        original_name: str,
        parent_id: Optional[int],
        parent_name: Optional[str],
        channel_type: int,
    ) -> Tuple[int, int, str]:
        """
        Re‑creates channel or webhook as needed.
        """
        for row in self.db.get_all_channel_mappings():
            if row["original_channel_id"] != original_id:
                continue

            clone_id = row["cloned_channel_id"]
            wh_url = row["channel_webhook_url"]

            if clone_id is not None:
                ch = guild.get_channel(clone_id)
                if ch:
                    if wh_url:
                        return original_id, clone_id, wh_url
                    # recreate missing webhook
                    await self.ratelimit.acquire(ActionType.NEW_WEBHOOK)
                    wh = await ch.create_webhook(name="Copycord")
                    url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"
                    self.db.upsert_channel_mapping(
                        original_id,
                        row["original_channel_name"],
                        clone_id,
                        url,
                        row["original_parent_category_id"],
                        row["cloned_parent_category_id"],
                    )
                    logger.info(
                        "Re-created webhook for %s #%d", original_name, original_id
                    )
                    return original_id, clone_id, url
            break

        category = None
        if parent_id is not None:
            category = await self._ensure_category(guild, parent_id, parent_name)

        kind = "news" if channel_type == ChannelType.news.value else "text"
        ch = await self._create_channel(guild, kind, original_name, category)

        await self.ratelimit.acquire(ActionType.NEW_WEBHOOK)
        wh = await ch.create_webhook(name="Clonecord")
        url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"
        logger.debug("Created webhook in %s", original_name)

        self.db.upsert_channel_mapping(
            original_id,
            original_name,
            ch.id,
            url,
            parent_id,
            category.id if category else None,
        )
        return original_id, ch.id, url

    async def _handle_master_channel_moves(
        self,
        guild: discord.Guild,
        incoming: List[Dict],
        old_map: Dict[int, Optional[int]],
    ) -> int:
        """
        Re-parent cloned channels whenever the expected parent (from DB) doesn't
        match what’s actually on Discord.
        """
        moved = 0

        for item in incoming:
            orig_id = item["id"]
            row = next(
                r
                for r in self.db.get_all_channel_mappings()
                if r["original_channel_id"] == orig_id
            )
            clone_id = row["cloned_channel_id"]
            expected_clone = row["cloned_parent_category_id"]

            ch = guild.get_channel(clone_id)
            if not ch:
                continue

            actual_clone = ch.category.id if ch.category else None
            if actual_clone == expected_clone:
                continue

            if expected_clone is None:
                if actual_clone is not None:
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await ch.edit(category=None)
                    logger.info("Reparented channel #%d → standalone", clone_id)
                    moved += 1
                continue

            cat_clone = guild.get_channel(expected_clone)
            if not isinstance(cat_clone, discord.CategoryChannel):
                logger.warning(
                    "Target category ID %d not found; leaving channel #%d standalone",
                    expected_clone,
                    clone_id,
                )
                if actual_clone is not None:
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await ch.edit(category=None)
                continue

            if not self._can_create_in_category(guild, cat_clone):
                logger.warning(
                    "Category %s full; leaving channel #%d standalone",
                    cat_clone.name,
                    clone_id,
                )
                if actual_clone is not None:
                    await self.ratelimit.acquire(ActionType.EDIT)
                    await ch.edit(category=None)
                continue
            await self.ratelimit.acquire(ActionType.EDIT)
            await ch.edit(category=cat_clone)
            logger.info(
                "Reparented channel #%d → category %s (ID %d)",
                clone_id,
                cat_clone.name,
                cat_clone.id,
            )
            moved += 1

        return moved

    async def _recreate_webhook(self, original_id: int) -> Optional[str]:
        """
        Look up the existing cloned channel, create a new webhook on it,
        update the DB, and return its URL.
        """
        row = next(
            (
                r
                for r in self.db.get_all_channel_mappings()
                if r["original_channel_id"] == original_id
            ),
            None,
        )
        if not row:
            logger.error(
                "No DB row for #%s; cannot recreate webhook, are we fully synced?",
                original_id,
            )
            return None

        cloned_id = row["cloned_channel_id"]
        if not cloned_id:
            logger.error(
                "No mapping found for #%s; cannot recreate webhook",
                original_id,
            )
            return None

        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.error(
                "Clone guild %s not found; cannot recreate webhook for #%s",
                self.clone_guild_id,
                original_id,
            )
            return None

        ch = guild.get_channel(cloned_id)
        if not ch:
            logger.error(
                "Clone channel %s not found in guild %s; cannot recreate webhook for #%s",
                cloned_id,
                self.clone_guild_id,
                original_id,
            )
            return None

        try:
            await self.ratelimit.acquire(ActionType.NEW_WEBHOOK)
            wh = await ch.create_webhook(name="Clonecord")
            url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

            self.db.upsert_channel_mapping(
                original_id,
                row["original_channel_name"],
                cloned_id,
                url,
                row["original_parent_category_id"],
                row["cloned_parent_category_id"],
            )

            logger.debug("Recreated webhook for #%s", original_id)
            return url

        except Exception:
            logger.exception("Failed to recreate webhook for #%s", original_id)
            return None

    async def handle_thread_message(self, data: dict):
        """
        Handle an incoming forum-thread message: buffer if unmapped, create or forward to thread.
        """
        # 1) Buffer all messages during sync
        if self._sync_lock.locked():
            logger.info(
                "Sync in progress; buffering thread message for '%s' from %s",
                data.get("thread_name"),
                data.get("author"),
            )
            self._pending_thread_msgs.append(data)
            return

        # 2) Ensure guild exists
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.error("Clone guild %s not available", self.clone_guild_id)
            return

        # 3) Load forum-channel mapping
        thread_map = next(
            (
                r for r in self.db.get_all_channel_mappings()
                if r["original_channel_id"] == data["forum_id"]
            ),
            None,
        )
        if not thread_map:
            logger.info(
                "No forum mapping for '%s'; buffering until synced",
                data.get("thread_name"),
            )
            self._pending_thread_msgs.append(data)
            return

        # 4) Check cloned forum channel
        cloned_forum = guild.get_channel(thread_map["cloned_channel_id"])
        if not cloned_forum:
            logger.info(
                "Mapped forum channel %d not found; buffering until exists",
                thread_map["cloned_channel_id"],
            )
            self._pending_thread_msgs.append(data)
            return

        # 5) Build payload and prepare webhook
        payload = self._build_webhook_payload(data)
        session = aiohttp.ClientSession()
        webhook_url = thread_map.get("channel_webhook_url")
        try:
            # 6) Instantiate the forum webhook
            thread_webhook = Webhook.from_url(
                thread_map["channel_webhook_url"], session=session
            )

            # 7) Ensure only one creator per original thread
            orig_tid = data["thread_id"]
            lock = self._thread_locks.setdefault(orig_tid, asyncio.Lock())
            created = False

            async with lock:
                # re-fetch the thread mapping inside the lock
                thread_map = next(
                    (
                        r for r in self.db.get_all_threads()
                        if r["original_thread_id"] == orig_tid
                    ),
                    None,
                )

                if thread_map is None:
                    cloned_chan = guild.get_channel(thread_map["cloned_channel_id"])
                    chan_name = cloned_chan.name if cloned_chan else "<unknown>"

                    logger.info(
                        "Creating thread '%s' in channel #%s by %s",
                        data["thread_name"],
                        chan_name,
                        data["author"],
                    )

                    if isinstance(cloned_forum, discord.ForumChannel):
                        # Webhook-based creation (includes initial message)
                        base = thread_map["channel_webhook_url"]
                        sep  = "&" if "?" in base else "?"
                        url  = f"{base}{sep}wait=true"
                        await self.ratelimit.acquire(ActionType.THREAD)
                        async with session.post(
                            url, json={**payload, "thread_name": data["thread_name"]}
                        ) as resp:
                            resp.raise_for_status()
                            new_id = int((await resp.json())["channel_id"])
                            created = True
                    else:
                        # Direct thread creation on a TextChannel
                        await self.ratelimit.acquire(ActionType.THREAD)
                        new_thread = await cloned_forum.create_thread(
                            name=data["thread_name"],
                            type=discord.ChannelType.public_thread,
                            auto_archive_duration=1440,
                        )
                        new_id = new_thread.id
                        # send the initial post
                        await self.ratelimit.acquire(ActionType.WEBHOOK, key=webhook_url)
                        await thread_webhook.send(**payload, thread=new_thread, wait=True)
                        created = True
                        

                    # enforce forum thread limit
                    try:
                        await self._enforce_thread_limit(guild)
                    except Exception as e:
                        logger.warning(
                            "Failed to enforce thread limit on forum %s: %s",
                            cloned_forum.id, e
                        )

                    # persist the new mapping
                    self.db.upsert_forum_thread_mapping(
                        orig_tid,
                        data["thread_name"],
                        new_id,
                        data["forum_id"],
                        thread_map["cloned_channel_id"],
                    )
                else:
                    # thread already created by another coroutine
                    new_id = thread_map["cloned_thread_id"]

                # fetch the actual Thread object
                clone_thread = (
                    guild.get_channel(new_id)
                    or await self.bot.fetch_channel(new_id)
                )

            # Outside the lock, forward message only if not initial create
            if not created:
                logger.info(
                    "Forwarding message to existing thread '%s' from %s",
                    data["thread_name"], data["author"]
                )
                await self.ratelimit.acquire(ActionType.WEBHOOK, key=webhook_url)
                await thread_webhook.send(**payload, thread=clone_thread, wait=True)
        finally:
            await session.close()



    async def handle_thread_delete(self, data: dict):
        """When a source thread is deleted, optionally delete its clone and always drop the DB mapping."""
        orig_thread_id = data["thread_id"]
        delete_remote = getattr(self.config, "DELETE_CLONED_THREADS", True)

        row = next(
            (
                r
                for r in self.db.get_all_threads()
                if r["original_thread_id"] == orig_thread_id
            ),
            None,
        )
        if not row:
            logger.info(
                "No mapping for deleted thread %s; nothing to do", orig_thread_id
            )
            return

        cloned_id = row["cloned_thread_id"]
        cloned_thread_name = row["original_thread_name"]
        cloned_thread_chnl = row["forum_cloned_id"]

        if delete_remote:
            guild = self.bot.get_guild(self.clone_guild_id)
            ch = None
            if guild:
                ch = guild.get_channel(cloned_id)
                if not ch:
                    try:
                        ch = await self.bot.fetch_channel(cloned_id)
                    except NotFound:
                        ch = None

            if ch:
                if self.config.DELETE_THREADS:
                    try:
                        await self.ratelimit.acquire(ActionType.DELETE)
                        await ch.delete()
                        logger.info(
                            "Deleted thread '%s' in #%s",
                            cloned_thread_name,
                            cloned_thread_chnl
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to delete cloned thread %s: %s", cloned_id, e
                        )
            else:
                logger.warning(
                    "Cloned thread %s not found in guild or via fetch", cloned_id
                )

        self.db.delete_forum_thread_mapping(orig_thread_id)

    async def handle_thread_rename(self, data: dict):
        """Rename a cloned thread when the source thread was renamed."""
        orig_thread_id = data["thread_id"]
        new_name = data["new_name"]

        row = next(
            (
                r
                for r in self.db.get_all_threads()
                if r["original_thread_id"] == orig_thread_id
            ),
            None,
        )
        if not row:
            logger.warning(f"No mapping for renamed thread {orig_thread_id}; skipping")
            return

        cloned_id = row["cloned_thread_id"]
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.error("Clone guild not available for renames")
            return

        ch = guild.get_channel(cloned_id)
        if not ch:
            try:
                ch = await self.bot.fetch_channel(cloned_id)
            except NotFound:
                logger.error(f"Cloned thread {cloned_id} not found; cannot rename")
                return

        try:
            await self.ratelimit.acquire(ActionType.EDIT)
            await ch.edit(name=new_name)
            logger.info(f"Renamed cloned thread {cloned_id} → {new_name!r}")
        except Exception as e:
            logger.error(f"Failed to rename cloned thread {cloned_id}: {e}")

        self.db.upsert_forum_thread_mapping(
            orig_thread_id,
            new_name,
            cloned_id,
            row["original_thread_id"],
            row["cloned_thread_id"],
        )

    async def _enforce_thread_limit(self, guild: discord.Guild):
        """
        Global thread‐limit enforcement: archive oldest active threads
        across the entire guild (both TextChannel threads and ForumChannel threads)
        so you never have more than self.max_threads active.
        """
        # Gather all active (non‐archived) threads in the guild
        # Guild.threads includes public threads from text channels and forum channels
        active = [t for t in guild.threads if not getattr(t, "archived", False)]
        logger.debug(
            "Guild %d has %d active threads: %s",
            guild.id,
            len(active),
            [t.id for t in active],
        )

        # If we're at or under the limit, nothing to do
        if len(active) <= self.max_threads:
            return

        # Sort oldest → newest
        active.sort(
            key=lambda t: t.created_at
                        or datetime.datetime.min.replace(tzinfo=timezone.utc)
        )

        # Pick exactly how many to archive
        num_to_archive = len(active) - self.max_threads
        to_archive = active[:num_to_archive]

        # Archive them
        for thread in to_archive:
            try:
                await self.ratelimit.acquire(ActionType.EDIT)
                await thread.edit(archived=True)
                parent = thread.parent
                parent_name = parent.name if parent else "Unknown"
                logger.info(
                    "Auto-archived thread '%s' in #%s to respect thread limits",
                    thread.name,
                    parent_name,
                )
            except Exception as e:
                logger.warning("Failed to auto-archive thread %d: %s", thread.id, e)
                
    def _replace_emoji_ids(self, content: str) -> str:
        """
        Replace any <:Name:orig_id> or <a:Name:orig_id> with
        the corresponding cloned emoji ID in this guild.
        """
        def _repl(match: re.Match) -> str:
            animated_flag = match.group(1) or ""
            name = match.group("name")
            orig_id = int(match.group("id"))

            row = self.db.get_emoji_mapping(orig_id)
            if not row:
                # we never cloned this one → leave it untouched
                return match.group(0)

            new_id = row["cloned_emoji_id"]
            prefix = "a" if animated_flag == "a" else ""
            return f"<{prefix}:{name}:{new_id}>"

        return self._EMOJI_RE.sub(_repl, content)

    def _build_webhook_payload(self, msg: Dict) -> dict:
        """
        Turn a raw incoming msg dict into the final webhook payload.
        - Appends any attachment URLs to the content.
        - Pulls out any GIF/video/embed URLs and tacks them onto content too.
        - Carries forward “real” embeds (fields, thumbnails, etc) as Embed objects.
        - If total length >2000, wraps _all_ text+URLs in a new embed.
        """
        # 1) Build up the text blob
        text = msg.get("content", "")
        text = self._replace_emoji_ids(text) # Replace emoji IDs with cloned ones
        for att in msg.get("attachments", []):
            if att["url"] not in text:
                text += f"\n{att['url']}"

        raw_embeds = msg.get("embeds", [])
        embeds: list[Embed] = []

        for raw in raw_embeds:
            if isinstance(raw, dict):
                e_type = raw.get("type")
                page_url = raw.get("url")
                if e_type in ("gifv", "video", "image") and page_url:
                    if page_url not in text:
                        text += f"\n{page_url}"
                    continue

                try:
                    embeds.append(Embed.from_dict(raw))
                except Exception as e:
                    logger.warning("Could not convert embed dict to Embed: %s", e)

            elif isinstance(raw, Embed):
                embeds.append(raw)

        base = {
            "username": msg["author"],
            "avatar_url": msg.get("avatar_url"),
        }

        if len(text) > 2000:
            logger.debug("Payload too long (%d chars), wrapping in embed", len(text))
            long_embed = Embed(description=text[:4096])
            # turn *all* embeds into dicts
            all_embeds = [long_embed] + embeds
            return {**base, "embeds": [e.to_dict() for e in all_embeds]}

        payload = {**base, "content": text or None}
        if embeds:
            # convert all Embed instances to dicts
            payload["embeds"] = [e.to_dict() for e in embeds]
        return payload

    async def forward_message(self, msg: Dict):
        source_id = msg["channel_id"]

        # Lookup mapping
        mapping = next(
            (
                r
                for r in self.db.get_all_channel_mappings()
                if r["original_channel_id"] == source_id
            ),
            None,
        )
        url = mapping["channel_webhook_url"] if mapping else None

        # Buffer if sync in progress or no mapping yet
        if mapping is None:
            logger.info(
                "No mapping yet for channel #%s; buffering message from %s until synced",
                msg["channel_name"],
                msg["author"],
            )
            self._pending_msgs.setdefault(source_id, []).append(msg)
            return

        # 2) We have a mapping but no webhook URL → either buffer (mid-sync) or recreate
        if not url:
            if self._sync_lock.locked():
                logger.info(
                    "Sync in progress; message in #%s from %s will be forwarded after sync",
                    msg["channel_name"],
                    msg["author"],
                )
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return

            logger.warning(
                "Mapped channel %s has no webhook; attempting to recreate", msg["channel_name"]
            )
            url = await self._recreate_webhook(source_id)
            if not url:
                # recreate failed (e.g. channel not yet created) → buffer and retry post-sync
                logger.info(
                    "Could not recreate webhook for #%s; buffering message from %s",
                    msg["channel_name"],
                    msg["author"],
                )
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return

        # Build payload
        payload = self._build_webhook_payload(msg)
        if payload is None:
            logger.info(
                "No webhook payload built for #%s; skipping", msg["channel_name"]
            )
            return

        if not payload.get("content") and not payload.get("embeds"):
            logger.info("Skipping empty message for #%s", msg["channel_name"])
            return

        # Ensure serializable
        try:
            import json

            json.dumps(payload)
        except (TypeError, ValueError) as e:
            logger.error(
                "Skipping message from #%s: payload not JSON serializable: %s; payload=%r",
                msg["channel_name"],
                e,
                payload,
            )
            return

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                await self.ratelimit.acquire(ActionType.WEBHOOK, key=url)
                async with self.session.post(url, json=payload) as resp:
                    # Success
                    if resp.status in (200, 204):
                        logger.info(
                            "Forwarded message to #%s from %s",
                            msg["channel_name"], msg["author"],
                        )
                        return

                    # Rate limited: retry after delay
                    if resp.status == 429 and attempt < max_attempts:
                        retry_after = resp.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after else 1.0
                        logger.warning(
                            "Rate limited on attempt %d/%d for #%s; sleeping %.1fs then retrying",
                            attempt, max_attempts, msg["channel_name"], wait,
                        )
                        await asyncio.sleep(wait)
                        continue

                    # Webhook gone: try recreate on first 404
                    if resp.status == 404 and attempt == 1:
                        logger.debug("Webhook %s 404; attempting to recreate...", url)
                        mapping = next(
                            (
                                r
                                for r in self.db.get_all_channel_mappings()
                                if r["original_channel_id"] == source_id
                            ),
                            None,
                        )
                        if not mapping or not mapping["channel_webhook_url"]:
                            logger.warning(
                                "No mapping for channel %s; buffering msg from %s",
                                msg["channel_name"], msg["author"],
                            )
                            self._pending_msgs.setdefault(source_id, []).append(msg)
                            return

                        url = await self._recreate_webhook(source_id)
                        if not url:
                            return
                        continue

                    # Other failures
                    body = await resp.text()
                    logger.error("Failed to send (status %s): %s", resp.status, body)
                    return

            except Exception:
                logger.exception("Error forwarding message to #%s", msg["channel_name"])
                return

        logger.error(
            "Exhausted %d attempts sending to #%s; dropping message from %s",
            max_attempts, msg["channel_name"], msg["author"],
        )

    async def _shutdown(self):
        logger.info("Shutting down server...")
        if self._ws_task is not None:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        if self._sitemap_task is not None:
            self._sitemap_task.cancel()
            try:
                await self._sitemap_task
            except asyncio.CancelledError:
                pass
        if self.session:
            await self.session.close()
        await self.bot.close()
        logger.info("Shutdown complete.")

    def run(self):
        logger.info("Starting Copycord %s", self.config.CURRENT_VERSION)
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.bot.close()))

        try:
            loop.run_until_complete(self.bot.start(self.config.SERVER_TOKEN))
        finally:
            loop.run_until_complete(self._shutdown())

            pending = asyncio.all_tasks(loop=loop)
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))


if __name__ == "__main__":
    ServerReceiver().run()
