# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import signal
import asyncio
import logging
from logging.handlers import RotatingFileHandler
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
from datetime import datetime, timezone
from asyncio import Queue
from common.config import Config
from common.websockets import WebsocketManager
from common.db import DBManager
from server.rate_limiter import RateLimitManager, ActionType
from server.discord_hooks import install_discord_rl_probe
from server.emojis import EmojiManager
from server.stickers import StickerManager
from server.backfill import BackfillManager
from server.helpers import OnJoinService

LOG_DIR = "/data"
os.makedirs(LOG_DIR, exist_ok=True)

LEVEL_NAME = os.getenv("LOG_LEVEL", "INFO").upper()
LEVEL = getattr(logging, LEVEL_NAME, logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

root = logging.getLogger()
root.setLevel(LEVEL)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(LEVEL)
root.addHandler(ch)

log_file = os.path.join(LOG_DIR, "server.log")
fh = RotatingFileHandler(
    log_file,
    maxBytes=10 * 1024 * 1024,
    backupCount=1,
    encoding="utf-8",
)
fh.setFormatter(formatter)
fh.setLevel(LEVEL)
root.addHandler(fh)

# keep library noise down
for name in ("websockets.server", "websockets.protocol"):
    logging.getLogger(name).setLevel(logging.WARNING)
for lib in ("discord","discord.client","discord.gateway","discord.state","discord.http"):
    logging.getLogger(lib).setLevel(logging.WARNING)
logging.getLogger("discord.client").setLevel(logging.ERROR)

logger = logging.getLogger("server")
logger.setLevel(LEVEL)

class ServerReceiver:
    def __init__(self):
        self.config = Config(logger=logger)
        self.bot = discord.Bot(intents=discord.Intents.all())
        self.bot.server = self
        self.ws = WebsocketManager(
            send_url=self.config.CLIENT_WS_URL,
            listen_host=self.config.SERVER_WS_HOST,
            listen_port=self.config.SERVER_WS_PORT,
            logger=logger,
        )
        self.clone_guild_id = int(self.config.CLONE_GUILD_ID)
        self.bot.ws_manager = self.ws
        self.db = DBManager(self.config.DB_PATH)
        self.backfill = BackfillManager(self)
        self.session: aiohttp.ClientSession = None
        self.sitemap_queue: Queue = Queue()
        self._processor_started = False
        self._sitemap_task_counter = 0
        self._sync_lock = asyncio.Lock()
        self._thread_locks: dict[int, asyncio.Lock] = {}
        self.max_threads = 950
        self._m_ch = re.compile(r"<#(\d+)>")
        self.bot.event(self.on_ready)
        self.bot.event(self.on_webhooks_update)
        self.bot.event(self.on_guild_channel_delete)
        self._default_avatar_bytes: Optional[bytes] = None
        self._ws_task: asyncio.Task | None = None
        self._sitemap_task: asyncio.Task | None = None
        self._pending_msgs: dict[int, list[dict]] = {}
        self._pending_thread_msgs: List[Dict] = []
        self._flush_bg_task: asyncio.Task | None = None
        self._flush_full_flag: bool = False
        self._flush_targets: set[int] = set()  # original channel IDs
        self._flush_thread_targets: set[int] = set()  # thread parent IDs
        self._webhook_locks: Dict[int, asyncio.Lock] = {}
        self._new_webhook_gate = asyncio.Lock()
        self.sticker_map: dict[int, dict] = {}
        self.cat_map: dict[int, dict] = {}
        self.chan_map: dict[int, dict] = {}
        self._unmapped_warned: set[int] = set()
        self._unmapped_threads_warned: set[int] = set()
        self._webhooks: dict[str, Webhook] = {}
        self._warn_lock = asyncio.Lock()
        self._shutting_down = False
        orig_on_connect = self.bot.on_connect
        self.ratelimit = RateLimitManager()
        self.emojis = EmojiManager(
            bot=self.bot,
            db=self.db,
            ratelimit=self.ratelimit,
            clone_guild_id=int(self.config.CLONE_GUILD_ID),
            session=self.session,
        )
        self.stickers = StickerManager(
            bot=self.bot,
            db=self.db,
            ratelimit=self.ratelimit,
            clone_guild_id=int(self.config.CLONE_GUILD_ID),
            session=self.session,
        )
        self.onjoin = OnJoinService(self.bot, self.db, logger.getChild("OnJoin"))
        install_discord_rl_probe(self.ratelimit)
        # Discord guild/channel limits
        self.MAX_GUILD_CHANNELS = 500
        self.MAX_CATEGORIES = 50
        self.MAX_CHANNELS_PER_CATEGORY = 50
        self._EMOJI_RE = re.compile(r"<(a?):(?P<name>[^:]+):(?P<id>\d+)>")

        async def _command_sync():
            try:
                await orig_on_connect()
            except Forbidden as e:
                logger.warning(
                    "[⚠️] Can't sync slash commands, make sure the bot is in the server: %s",
                    e,
                )

        self.bot.on_connect = _command_sync
        self.bot.load_extension("server.commands")

    async def update_status(self, message: str):
        """Update the bot's Discord status."""
        try:
            await self.bot.change_presence(activity=discord.Game(name=message))
            self._last_status = getattr(self, "_last_status", None)
            if self._last_status == message:
                return
            self._last_status = message
            logger.debug("[🟢] Bot status updated to: %s", message)
        except Exception as e:
            logger.debug("[⚠️] Failed to update bot status: %s", e)

    async def on_ready(self):
        """
        Event handler that is called when the bot is ready.
        """
        await self.update_status(f"{self.config.CURRENT_VERSION}")
        asyncio.create_task(self.config.setup_release_watcher(self))
        self.session = aiohttp.ClientSession()
        # Ensure we're in the clone guild
        clone_guild = self.bot.get_guild(self.clone_guild_id)
        if clone_guild is None:
            logger.error(
                "[⛔] Bot (ID %s) is not a member of the guild %s; shutting down.",
                self.bot.user.id,
                self.clone_guild_id,
            )
            await self.bot.close()
            sys.exit(1)
        self._load_mappings()
        self.emojis.set_session(self.session)
        self.stickers.set_session(self.session)
        await self.stickers.refresh_cache()
        await self._backfill_channel_types()

        logger.info(
            "[🤖] Logged in as %s and monitoring guild %s ",
            self.bot.user,
            clone_guild.name,
        )
        if not self.config.ENABLE_CLONING:
            logger.info("[🔕] Server cloning is disabled...")

        if not self._processor_started:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))
            self._sitemap_task = asyncio.create_task(self.process_sitemap_queue())
            self._processor_started = True

    async def on_webhooks_update(self, channel: discord.abc.GuildChannel):
        """
        Handles the event triggered when webhooks are updated in a guild channel.
        """
        if self.config.ENABLE_CLONING:
            if self._shutting_down:
                return
            try:
                if channel.guild.id != self.clone_guild_id:
                    return
            except AttributeError:
                return
            # Zap the cached pool so the next send rebuilds and includes any new “Copycord” hooks
            self.backfill.invalidate_rotation(int(channel.id))
            logger.debug(
                "[rotate] Webhooks changed in #%s — webhook rotation pool invalidated",
                channel.id,
            )

    async def on_guild_channel_delete(self, channel):
        """When a cloned channel/category is deleted, request a sitemap"""
        if self.config.ENABLE_CLONING:
            try:
                if channel.guild.id != self.clone_guild_id:
                    return
            except AttributeError:
                return

            # Skip if sync is in progress
            if getattr(self, "_sync_lock", None) and self._sync_lock.locked():
                logger.debug(
                    "[🛑] Sync in progress — ignoring sitemap request for deleted channel %s",
                    channel.id,
                )
                return

            # Is this a category?
            is_category = (
                isinstance(channel, discord.CategoryChannel)
                or getattr(channel, "type", None) == discord.ChannelType.category
            )

            if is_category:
                # Look up the original category whose clone was deleted
                hit_src_cat_id = None
                for orig_cat_id, row in list(self.cat_map.items()):
                    if int(row.get("cloned_category_id") or 0) == int(channel.id):
                        hit_src_cat_id = int(orig_cat_id)
                        break

                if hit_src_cat_id is None:
                    return  # not one of ours

                # Mark the category mapping stale in memory
                self.cat_map.pop(hit_src_cat_id, None)

                logger.warning(
                    "[🧹] Cloned category deleted: id=%s name=%s (src_cat=%s). Requesting sitemap.",
                    channel.id,
                    getattr(channel, "name", "?"),
                    hit_src_cat_id,
                )

                # Ask the client to send over the sitemap
                await self.bot.ws_manager.send({"type": "sitemap_request"})
                return

            # ----- Not a category: handle channel deletion -----
            hit_src_id = None
            for src_id, row in list(self.chan_map.items()):
                if int(row.get("cloned_channel_id") or 0) == int(channel.id):
                    hit_src_id = int(src_id)
                    break

            if hit_src_id is None:
                return  # not one of ours

            # Invalidate rotation/webhook caches for this clone channel
            try:
                self.backfill.invalidate_rotation(int(channel.id))
            except Exception:
                pass

            # Mark the channel mapping stale in memory
            self.chan_map.pop(hit_src_id, None)

            logger.warning(
                "[🧹] Cloned channel deleted: id=%s name=%s (src=%s). Requesting sitemap.",
                channel.id,
                getattr(channel, "name", "?"),
                hit_src_id,
            )

            # Ask the client to send over the sitemap
            await self.bot.ws_manager.send({"type": "sitemap_request"})

    async def _on_ws(self, msg: dict):
        """
        Handles incoming WebSocket messages and dispatches them based on their type.
        """
        if self._shutting_down:
            return
        typ = msg.get("type")
        data = msg.get("data", {})
        if typ == "sitemap":
            self._sitemap_task_counter += 1
            task_id = self._sitemap_task_counter
            self.sitemap_queue.put_nowait((task_id, data))
            logger.info("[📩] Sync task #%d received", task_id)
            logger.debug(
                "Sync task #%d (queue size now: %d)",
                task_id,
                self.sitemap_queue.qsize(),
            )

        elif typ == "message":
            ct = data.get("channel_type")
            if ct in (ChannelType.voice.value, ChannelType.stage_voice.value):
                logger.debug(
                    "[🔇] Dropping message for voice/stage_voice channel (type: %s, data: %s)",
                    ct,
                    data,
                )
                return
            if data.get("__backfill__"):
                t = asyncio.create_task(self.forward_message(data))
                self.backfill.attach_task(int(data.get("channel_id")), t)
            else:
                asyncio.create_task(self.forward_message(data))

        elif typ == "thread_message":
            asyncio.create_task(self.handle_thread_message(data))

        elif typ == "thread_delete":
            asyncio.create_task(self.handle_thread_delete(data))

        elif typ == "thread_rename":
            asyncio.create_task(self.handle_thread_rename(data))

        elif typ == "announce":
            asyncio.create_task(self.handle_announce(data))

        elif typ == "backfill_started":
            await self.backfill.on_started(int(data.get("channel_id")))

        elif typ == "backfill_progress":
            await self.backfill.on_progress(
                int(data.get("channel_id")), int(data.get("count") or 0)
            )

        elif typ == "backfill_done":
            await self.backfill.on_done(int(data.get("channel_id")))
            
        elif typ == "member_joined":
            asyncio.create_task(self.onjoin.handle_member_joined(data))

        else:
            logger.warning("[⚠️] Unknown WS type '%s'", typ)

    async def process_sitemap_queue(self):
        """Continuously process only the newest sitemap, discarding any others."""
        if self._shutting_down:
            return

        first = True
        while not self._shutting_down:
            if not first:
                logger.debug("Waiting 5s before processing next sitemap…")
                await asyncio.sleep(5)
                if self._shutting_down:
                    break
            first = False

            # Wait for at least one sitemap
            task_id, sitemap = await self.sitemap_queue.get()

            # Drop all but the newest
            qsize = self.sitemap_queue.qsize()
            if qsize:
                logger.debug(
                    "Dropping %d outdated sitemap(s), will process only the newest (task #%d).",
                    qsize,
                    task_id,
                )

            # Drain queue properly (latest item wins)
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
                logger.info("[💾] Sync task #%d completed: %s", task_id, summary)
            finally:
                self.sitemap_queue.task_done()

        # Optional: drain remaining items on shutdown without processing
        try:
            while True:
                self.sitemap_queue.get_nowait()
                self.sitemap_queue.task_done()
        except asyncio.QueueEmpty:
            pass

    async def _backfill_channel_types(self) -> None:
        """Populate channel_mappings.channel_type for old rows."""
        try:
            guild = self.bot.get_guild(self.clone_guild_id)
            if not guild:
                return

            rows = [dict(r) for r in self.db.get_all_channel_mappings()]

            # Early exit if every row already has a type
            if not any(r.get("channel_type") in (None, 0) for r in rows):
                return

            changed = 0
            for row in rows:
                if row.get("channel_type") not in (None, 0):
                    continue

                clone_id = row.get("cloned_channel_id")
                if not clone_id:
                    continue

                ch = guild.get_channel(int(clone_id))
                if not ch:
                    continue

                ctype = int(ch.type.value)

                self.db.upsert_channel_mapping(
                    int(row["original_channel_id"]),
                    row["original_channel_name"],
                    int(row["cloned_channel_id"]) if row["cloned_channel_id"] else None,
                    row["channel_webhook_url"],
                    (
                        int(row["original_parent_category_id"])
                        if row["original_parent_category_id"]
                        else None
                    ),
                    (
                        int(row["cloned_parent_category_id"])
                        if row["cloned_parent_category_id"]
                        else None
                    ),
                    ctype,
                )
                changed += 1

            if changed:
                self._load_mappings()
                logger.debug("[🧭] Backfilled channel_type for %d channels", changed)

        except Exception:
            logger.exception("Backfill of channel_type failed")

    async def handle_announce(self, data: dict):
        """
        Handles the announcement process by sending direct messages (DMs) to users
        subscribed to specific keywords in the announcement content.
        """
        if self._shutting_down:
            return
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.error("[⛔] Clone guild not available for announcements")
            return
        try:
            raw_kw = data["keyword"]
            content = data["content"]
            author = data["author"]
            orig_chan_id = data.get("channel_id")
            timestamp = data["timestamp"]

            all_sub_keys = self.db.get_announcement_keywords()
            matching_keys = [
                sub_kw
                for sub_kw in all_sub_keys
                if sub_kw == "*"
                or re.search(rf"\b{re.escape(sub_kw)}\b", content, re.IGNORECASE)
            ]

            user_ids = set()
            for mk in matching_keys:
                user_ids.update(self.db.get_announcement_users(mk))

            if not user_ids:
                return

            self._load_mappings()
            mapping = self.chan_map.get(orig_chan_id)
            clone_chan_id = mapping["cloned_channel_id"] if mapping else orig_chan_id
            channel_mention = f"<#{clone_chan_id}>"

            def _truncate(text: str, limit: int) -> str:
                return text if len(text) <= limit else text[: limit - 3] + "..."

            MAX_DESC = 4096
            MAX_FIELD = 1024

            desc = _truncate(content, MAX_DESC)
            kw_value = _truncate(", ".join(matching_keys) or raw_kw, MAX_FIELD)

            embed = discord.Embed(
                title="📢 Announcement",
                description=desc,
                timestamp=datetime.fromisoformat(timestamp),
            )
            embed.set_author(name=author)
            embed.add_field(name="Channel", value=channel_mention, inline=True)
            embed.add_field(name="Keyword", value=kw_value, inline=True)

            # 6) send DMs
            for uid in user_ids:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    await user.send(embed=embed)
                    logger.info(f"[🔔] Sent announcement {matching_keys} to {user}")
                except Exception as e:
                    logger.warning(f"[⚠️] Failed to DM {uid} for {matching_keys}: {e}")
        except Exception as e:
            logger.exception("Unexpected error in handle_announce: %s", e)

    def _load_mappings(self):
        """
        Loads category and channel mappings from the database into in-memory dictionaries.
        """
        self.cat_map = {
            r["original_category_id"]: dict(r)
            for r in self.db.get_all_category_mappings()
        }
        self.chan_map = {
            r["original_channel_id"]: dict(r)
            for r in self.db.get_all_channel_mappings()
        }
        try:
            self.sticker_map = {
                r["original_sticker_id"]: dict(r)
                for r in self.db.get_all_sticker_mappings()
            }
        except Exception:
            self.sticker_map = {}

    def _purge_stale_mappings(self, guild: discord.Guild):
        """
        Removes stale category and channel mappings from the internal mappings and database.
        This method iterates through the category and channel mappings stored in `self.cat_map`
        and `self.chan_map`, respectively. If a mapped category or channel no longer exists
        in the provided Discord guild, the mapping is considered stale and is removed from
        both the internal mappings and the database.
        """
        # Categories
        for orig, row in list(self.cat_map.items()):
            if not guild.get_channel(row["cloned_category_id"]):
                logger.info("[🗑️] Purging category mapping %d", orig)
                self.db.delete_category_mapping(orig)
                self.cat_map.pop(orig)

        # Channels
        for orig, row in list(self.chan_map.items()):
            if not guild.get_channel(row["cloned_channel_id"]):
                logger.info("[🗑️] Purging channel mapping %d", orig)
                self.db.delete_channel_mapping(orig)
                self.chan_map.pop(orig)

    async def sync_structure(self, task_id: int, sitemap: Dict) -> str:
        """
        Synchronizes the structure of a Discord guild based on the provided sitemap.
        """
        logger.debug(f"Sync Task #{task_id}: Processing sitemap {sitemap}")
        async with self._sync_lock:
            guild = self.bot.get_guild(self.clone_guild_id)
            if not guild:
                logger.error("[⛔] Clone guild %s not found", self.clone_guild_id)
                return "Error: clone guild missing"
            self._load_mappings()
            self.stickers.set_last_sitemap(sitemap.get("stickers"))

            # --- Emoji sync in background ---
            if self.config.CLONE_EMOJI:
                self.emojis.kickoff_sync(sitemap.get("emojis", []))

            # --- Sticker sync in background ---
            if self.config.CLONE_STICKER:
                self.stickers.kickoff_sync()

            cat_created, ch_reparented = await self._repair_deleted_categories(
                guild, sitemap
            )
            self._purge_stale_mappings(guild)

            parts: List[str] = []
            if cat_created:
                parts.append(f"Created {cat_created} categories")
            if ch_reparented:
                parts.append(f"Reparented {ch_reparented} channels")

            parts += await self._sync_community(guild, sitemap)
            parts += await self._sync_categories(guild, sitemap)
            parts += await self._sync_forums(guild, sitemap)
            parts += await self._sync_channels(guild, sitemap)

            moved = await self._handle_master_channel_moves(
                guild, self._parse_sitemap(sitemap)
            )
            if moved:
                parts.append(f"Reparented {moved} channels")

            parts += await self._sync_threads(guild, sitemap)

        self._schedule_flush()
        return "; ".join(parts) if parts else "No changes needed"

    async def _sync_community(self, guild: Guild, sitemap: Dict) -> List[str]:
        """
        Enable/disable Community mode and set rules/updates channels only when they differ.
        """
        comm = sitemap.get("community", {})
        want = bool(comm.get("enabled"))
        parts: List[str] = []

        curr_enabled = "COMMUNITY" in guild.features
        curr_rules = guild.rules_channel
        curr_updates = guild.public_updates_channel

        rules_id = comm.get("rules_channel_id")
        updates_id = comm.get("public_updates_channel_id")

        if want == curr_enabled:
            if want:
                rm = self.chan_map.get(rules_id)
                um = self.chan_map.get(updates_id)
                if rm and um:
                    rc = guild.get_channel(rm["cloned_channel_id"])
                    uc = guild.get_channel(um["cloned_channel_id"])
                    if curr_rules == rc and curr_updates == uc:
                        return parts
            else:
                return parts

        if curr_enabled and not want:
            try:
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await guild.edit(community=False)
                parts.append("[⚙️] Disabled Community mode")
                logger.info("[⚙️] Community mode disabled.")
            except Exception as e:
                logger.warning("[⚠️] Failed disabling Community mode: %s", e)
            return parts

        if want and rules_id and updates_id:
            rm = self.chan_map.get(rules_id)
            um = self.chan_map.get(updates_id)
            if rm and um:
                rc = guild.get_channel(rm["cloned_channel_id"])
                uc = guild.get_channel(um["cloned_channel_id"])
                # prepare kwargs
                edit_kwargs = {
                    "community": True,
                    "rules_channel": rc,
                    "public_updates_channel": uc,
                }
                changes = []
                if not curr_enabled:
                    changes.append("enabled")
                if curr_rules != rc:
                    changes.append(
                        f"rules {curr_rules.id if curr_rules else 'None'}→{rc.id}"
                    )
                if curr_updates != uc:
                    changes.append(
                        f"updates {curr_updates.id if curr_updates else 'None'}→{uc.id}"
                    )

                try:
                    await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                    await guild.edit(**edit_kwargs)
                    parts.append("Updated Community mode")
                    logger.info(
                        "[⚙️] Community settings changed: %s", ", ".join(changes)
                    )
                except discord.Forbidden as e:
                    if (
                        "150011" in getattr(e, "text", "")
                        or getattr(e, "code", None) == 150011
                    ):
                        logger.warning(
                            "[⚠️] Cannot enable Community mode automatically: "
                            "please enable it once manually in the server settings."
                        )
                    else:
                        logger.warning(
                            "[⚠️] Failed enabling/updating Community mode: %s", e
                        )
                except Exception as e:
                    logger.warning("[⚠️] Failed enabling/updating Community mode: %s", e)

        return parts

    async def _repair_deleted_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> Tuple[int, int]:
        """
        Repairs deleted categories in a the clone guild by recreating missing categories
        and reparenting channels to the newly created categories.
        """
        created = 0
        reparented = 0

        wanted = {c["id"] for c in sitemap.get("categories", [])}
        name_for = {c["id"]: c["name"] for c in sitemap.get("categories", [])}

        for cat_row in self.db.get_all_category_mappings():
            orig_cat_id = cat_row["original_category_id"]
            if orig_cat_id not in wanted:
                continue

            if not guild.get_channel(cat_row["cloned_category_id"]):
                new_cat, did_create = await self._ensure_category(
                    guild, orig_cat_id, name_for[orig_cat_id]
                )
                if did_create:
                    created += 1

                self.db.upsert_category_mapping(
                    orig_cat_id,
                    name_for[orig_cat_id],
                    new_cat.id,
                    new_cat.name,
                )

                for ch_orig_id, ch_row in self.chan_map.items():
                    if ch_row["original_parent_category_id"] != orig_cat_id:
                        continue

                    ch = guild.get_channel(ch_row["cloned_channel_id"])
                    ctype = ch.type.value if ch else None

                    self.db.upsert_channel_mapping(
                        ch_orig_id,
                        ch_row["original_channel_name"],
                        ch_row["cloned_channel_id"],
                        ch_row["channel_webhook_url"],
                        ch_row["original_parent_category_id"],
                        new_cat.id,
                        ctype,
                    )

                    self.chan_map[ch_orig_id]["cloned_parent_category_id"] = new_cat.id

                    ch = guild.get_channel(ch_row["cloned_channel_id"])
                    if ch:
                        await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                        await ch.edit(category=new_cat)
                        logger.info(
                            "[✏️] Reparented channel '%s' (ID %d) → category '%s' (ID %d)",
                            ch.name,
                            ch.id,
                            new_cat.name,
                            new_cat.id,
                        )
                        reparented += 1

        return created, reparented

    async def _sync_categories(self, guild: Guild, sitemap: Dict) -> List[str]:
        """
        Synchronize the categories of a guild with the provided sitemap.
        """
        parts: List[str] = []

        rem = await self._handle_removed_categories(guild, sitemap)
        if rem:
            parts.append(f"Deleted {rem} categories")
        ren = await self._handle_renamed_categories(guild, sitemap)
        if ren:
            parts.append(f"Renamed {ren} categories")
        created = 0
        for cat in sitemap.get("categories", []):
            _, did_create = await self._ensure_category(guild, cat["id"], cat["name"])
            if did_create:
                created += 1
        if created:
            parts.append(f"Created {created} categories")

        return parts

    async def _sync_forums(self, guild: Guild, sitemap: Dict) -> List[str]:
        """
        Synchronize forums for a given guild based on the provided sitemap.
        This method creates new forum channels and their associated webhooks
        in the specified guild. It ensures that forums are created only if
        they do not already exist, and their mappings are persisted in the
        database for future reference.
        """
        parts: List[str] = []
        created = 0

        for forum in sitemap.get("forums", []):
            orig = forum["id"]
            fmap = self.chan_map.get(orig)

            # If it already exists and has a valid channel, skip
            if fmap and guild.get_channel(fmap["cloned_channel_id"]):
                continue

            # Determine parent category (if any)
            parent = None
            if forum.get("category_id") is not None:
                cat_row = self.cat_map.get(forum["category_id"])
                parent = (
                    guild.get_channel(cat_row["cloned_category_id"])
                    if cat_row
                    else None
                )

            # 1) Create the forum channel
            ch = await self._create_channel(guild, "forum", forum["name"], parent)
            created += 1

            # 2) Immediately create its webhook
            wh = await self._create_webhook_safely(
                ch, "Copycord", await self._get_default_avatar_bytes()
            )
            url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

            # 3) Persist the mapping
            self.db.upsert_channel_mapping(
                orig,
                forum["name"],
                ch.id,
                url,
                forum.get("category_id"),
                parent.id if parent else None,
                ChannelType.forum.value,
            )
            self.chan_map[orig] = {
                "original_channel_id": orig,
                "original_channel_name": forum["name"],
                "cloned_channel_id": ch.id,
                "channel_webhook_url": url,
                "original_parent_category_id": forum.get("category_id"),
                "cloned_parent_category_id": parent.id if parent else None,
                "channel_type": ChannelType.forum.value,
            }

        if created:
            parts.append(f"Created {created} forum channel{'s' if created>1 else ''}")
        return parts

    async def _sync_channels(self, guild: Guild, sitemap: Dict) -> List[str]:
        """
        Synchronizes the channels of a guild with the provided sitemap.
        This method handles the following operations:
        1. Deletes stale channels that are no longer present in the sitemap.
        2. Creates new channels based on the sitemap if they do not already exist.
        3. Converts channels to Announcement type if required.
        4. Renames channels to match the names specified in the sitemap.
        """
        parts: List[str] = []
        incoming = self._parse_sitemap(sitemap)

        # 1) deleted, renamed, created logic…
        rem = await self._handle_removed_channels(guild, incoming)
        if rem:
            parts.append(f"Deleted {rem} channels")

        created = renamed = converted = 0

        for item in incoming:
            orig, name, pid, pname, ctype = (
                item["id"],
                item["name"],
                item["parent_id"],
                item["parent_name"],
                item["type"],
            )
            mapping = self.chan_map.get(orig)
            is_new = mapping is None or not guild.get_channel(
                mapping["cloned_channel_id"]
            )
            _, clone_id, _ = await self._ensure_channel_and_webhook(
                guild, orig, name, pid, pname, ctype
            )
            if is_new:
                created += 1

            ch = guild.get_channel(clone_id)
            if not ch:
                continue

            # 2) Convert to Announcement if needed
            if ctype == ChannelType.news.value:
                # guild now supports NEWS?
                if "NEWS" in guild.features and ch.type != ChannelType.news:
                    await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                    await ch.edit(type=ChannelType.news)
                    converted += 1
                    logger.info(
                        "[✏️] Converted channel '%s' #%d → Announcement", ch.name, ch.id
                    )
                    # persist channel_type change
                    row = self.chan_map.get(orig, {})
                    self.db.upsert_channel_mapping(
                        orig,
                        row.get("original_channel_name", name),
                        ch.id,
                        row.get("channel_webhook_url"),
                        row.get("original_parent_category_id"),
                        row.get("cloned_parent_category_id"),
                        ChannelType.news.value,
                    )
                    if orig in self.chan_map:
                        self.chan_map[orig]["channel_type"] = ChannelType.news.value

            # 3) Rename if needed
            if ch.name != name:
                old = ch.name
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await ch.edit(name=name)
                renamed += 1
                logger.info("[✏️] Renamed channel #%d: %r → %r", ch.id, old, name)

        if created:
            parts.append(f"Created {created} channels")
        if converted:
            parts.append(f"Converted {converted} channels to Announcement")
        if renamed:
            parts.append(f"Renamed {renamed} channels")

        return parts

    async def _sync_threads(self, guild: Guild, sitemap: Dict) -> List[str]:
        """
        Delete stale thread mappings (when the original thread is gone upstream),
        or when the cloned thread truly doesn’t exist in Discord anymore;
        then rename any remaining threads.
        """
        parts: List[str] = []
        valid_upstream_ids = {t["id"] for t in sitemap.get("threads", [])}
        deleted = 0

        for row in self.db.get_all_threads():
            orig_id = row["original_thread_id"]
            clone_id = row["cloned_thread_id"]
            thread_name = row["original_thread_name"]

            # Try to get the cloned thread; fall back to fetch if not in cache
            try:
                clone_ch = guild.get_channel(clone_id) or await self.bot.fetch_channel(
                    clone_id
                )
            except (NotFound, HTTPException):
                clone_ch = None

            # 1) If the original thread no longer exists upstream, clear mapping (and delete clone if desired)
            if orig_id not in valid_upstream_ids:
                logger.info(
                    "[🗑️] Thread %s no longer present in the host server; clearing mapping (clone=%s)",
                    thread_name,
                    clone_id,
                )
                if clone_ch and self.config.DELETE_THREADS:
                    await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                    await clone_ch.delete()
                    logger.info("[🗑️] Deleted cloned thread %s", clone_id)
                self.db.delete_forum_thread_mapping(orig_id)
                deleted += 1
                continue

            # 2) If the clone truly doesn’t exist (neither cache nor fetch), clear mapping
            if clone_ch is None:
                logger.info(
                    "[🗑️] Cloned thread %s missing in guild; clearing mapping from DB",
                    thread_name,
                )
                self.db.delete_forum_thread_mapping(orig_id)
                deleted += 1
                continue

        if deleted:
            parts.append(f"Deleted {deleted} threads")

        # 3) Rename any surviving threads whose names have changed
        renamed = 0
        for src in sitemap.get("threads", []):
            mapping = next(
                (
                    r
                    for r in self.db.get_all_threads()
                    if r["original_thread_id"] == src["id"]
                ),
                None,
            )
            if not mapping:
                continue

            ch = guild.get_channel(mapping["cloned_thread_id"])
            if ch and ch.name != src["name"]:
                old = ch.name
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await ch.edit(name=src["name"])
                # Update the mapping with the new name too
                self.db.upsert_forum_thread_mapping(
                    src["id"],
                    src["name"],
                    ch.id,
                    mapping["forum_original_id"],
                    mapping["forum_cloned_id"],
                )
                logger.info("[✏️] Renamed thread %s: %r → %r", ch.id, old, src["name"])
                renamed += 1

        if renamed:
            parts.append(f"Renamed {renamed} threads")

        return parts

    async def _flush_buffers(
        self,
        target_chans: set[int] | None = None,
        target_thread_parents: set[int] | None = None,
    ) -> None:
        """
        If targets provided: drain only those; otherwise drain all buffers.
        """
        # channels
        if target_chans:
            for cid in list(target_chans):
                await self._flush_channel_buffer(cid)
        else:
            for cid in list(self._pending_msgs.keys()):
                await self._flush_channel_buffer(cid)

        # thread parents
        if target_thread_parents:
            for pid in list(target_thread_parents):
                await self._flush_thread_parent_buffer(pid)
        else:
            # drain all thread buffers (whatever your current 'all' logic is)
            parents = {
                d.get("thread_parent_id")
                for d in self._pending_thread_msgs
                if d.get("thread_parent_id") is not None
            }
            for pid in list(parents):
                await self._flush_thread_parent_buffer(pid)

    async def _flush_channel_buffer(self, original_id: int) -> None:
        """Flush just the buffered messages for a single source channel."""
        if self._shutting_down:
            return

        msgs = self._pending_msgs.pop(original_id, [])
        for i, m in enumerate(list(msgs)):
            if self._shutting_down:
                remaining = msgs[i:]
                if remaining:
                    self._pending_msgs.setdefault(original_id, []).extend(remaining)
                return
            try:
                m["__buffered__"] = True
                await self.forward_message(m)
            except Exception:
                # Requeue on error
                self._pending_msgs.setdefault(original_id, []).append(m)
                logger.exception(
                    "[⚠️] Error forwarding buffered msg for #%s; requeued", original_id
                )

    async def _flush_thread_parent_buffer(self, parent_original_id: int) -> None:
        """Flush queued thread messages whose parent is now available."""
        if self._shutting_down or not self._pending_thread_msgs:
            return

        # Split first, then mutate the queue once
        to_send: list[dict] = []
        remaining: list[dict] = []
        for data in list(self._pending_thread_msgs):
            if data.get("thread_parent_id") == parent_original_id:
                to_send.append(data)
            else:
                remaining.append(data)

        # Commit the new queue before doing any awaits
        self._pending_thread_msgs = remaining

        # Now deliver the matched items
        for data in to_send:
            if self._shutting_down:
                return
            try:
                data["__buffered__"] = True
                await self.handle_thread_message(data)
            except Exception:
                logger.exception("[⚠️] Failed forwarding queued thread msg; requeuing")
                # Optional: requeue so it isn't lost
                self._pending_thread_msgs.append(data)

    def _flush_done_cb(self, task: asyncio.Task) -> None:
        """Log any exception raised by the background flush."""
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[flush] Background flush task failed")

    def _schedule_flush(
        self,
        chan_ids: set[int] | None = None,
        thread_parent_ids: set[int] | None = None,
    ) -> None:
        """
        - No args  -> request a full flush.
        - With args -> request a targeted flush (coalesces with other requests).
        If a task is already running, we just enqueue flags/targets and let it pick them up.
        """
        if getattr(self, "_shutting_down", False):
            return

        # record the request
        if not chan_ids and not thread_parent_ids:
            self._flush_full_flag = True  # upgrade everything
        else:
            if chan_ids:
                self._flush_targets |= set(chan_ids)
            if thread_parent_ids:
                self._flush_thread_targets |= set(thread_parent_ids)

        # if a worker is already running, it will notice these flags/sets
        if self._flush_bg_task and not self._flush_bg_task.done():
            return

        async def _runner():
            try:
                # Keep draining until no more work was queued during the run
                while True:
                    full = self._flush_full_flag
                    chans = self._flush_targets.copy()
                    threads = self._flush_thread_targets.copy()

                    # reset for new arrivals during this iteration
                    self._flush_full_flag = False
                    self._flush_targets.clear()
                    self._flush_thread_targets.clear()

                    if full:
                        await self._flush_buffers()  # global drain
                    else:
                        await self._flush_buffers(
                            target_chans=(chans or None),
                            target_thread_parents=(threads or None),
                        )

                    # nothing new queued while we were flushing -> we’re done
                    if (
                        not self._flush_full_flag
                        and not self._flush_targets
                        and not self._flush_thread_targets
                    ):
                        break

                    await asyncio.sleep(0)  # yield to event loop
            except asyncio.CancelledError:
                pass

        self._flush_bg_task = asyncio.create_task(_runner())
        self._flush_bg_task.add_done_callback(self._flush_done_cb)

    def _parse_sitemap(self, sitemap: Dict) -> List[Dict]:
        """
        Parses a sitemap dictionary and extracts channel and thread information into a list of dictionaries.
        """
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
        """
        Determines whether a new category can be created in the cloned guild.
        """
        return (
            len(guild.categories) < self.MAX_CATEGORIES
            and len(guild.channels) < self.MAX_GUILD_CHANNELS
        )

    def _can_create_in_category(
        self, guild: discord.Guild, category: Optional[discord.CategoryChannel]
    ) -> bool:
        """
        Determines whether a new channel can be created in the specified category
        within the clone guild, based on the maximum allowed channels per category and
        the maximum allowed channels in the guild.
        """
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
        if self._shutting_down:
            return
        if not self._can_create_in_category(guild, category):
            cat_label = category.name if category else "<root>"
            logger.warning(
                "[⚠️] Category %s full (or guild at cap); creating '%s' as standalone",
                cat_label,
                name,
            )
            category = None

        if kind == "forum":
            await self.ratelimit.acquire(ActionType.CREATE_CHANNEL)
            ch = await guild.create_forum_channel(name=name, category=category)
        else:
            await self.ratelimit.acquire(ActionType.CREATE_CHANNEL)
            ch = await guild.create_text_channel(name=name, category=category)

        logger.info("[➕] Created %s channel '%s' #%s", kind, name, ch.id)

        if kind == "news":
            if "NEWS" in guild.features:
                try:
                    await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                    await ch.edit(type=ChannelType.news)
                    logger.info("[✏️] Converted '%s' #%d to Announcement", name, ch.id)
                except HTTPException as e:
                    logger.warning(
                        "[⚠️] Could not convert '%s' to Announcement: %s; left as text",
                        name,
                        e,
                    )
            else:
                logger.warning(
                    "[⚠️] Guild %s doesn’t support NEWS; '%s' left as text",
                    guild.id,
                    name,
                )
        return ch

    async def _handle_removed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        """
        Handles the removal of categories that are no longer present in the sitemap.
        """
        valid_ids = {c["id"] for c in sitemap.get("categories", [])}
        removed = 0

        # Iterate over a copy so we can pop from self.cat_map
        for orig_id, row in list(self.cat_map.items()):
            if orig_id not in valid_ids:
                # delete the Discord category if configured
                ch = guild.get_channel(row["cloned_category_id"])
                if ch and self.config.DELETE_CHANNELS:
                    await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                    await ch.delete()
                    logger.info("[🗑️] Deleted category %s", ch.name)

                # remove from DB and in‐memory map
                self.db.delete_category_mapping(orig_id)
                self.cat_map.pop(orig_id, None)
                removed += 1

        return removed
    
    def _protected_channel_ids(self, guild: discord.Guild) -> set[int]:
        ids = set()
        for attr in ("rules_channel", "public_updates_channel", "system_channel"):
            ch = getattr(guild, attr, None)
            if ch:
                ids.add(ch.id)
        return ids

    async def _handle_removed_channels(
        self, guild: discord.Guild, incoming: List[Dict]
    ) -> int:
        """
        Deletes cloned channels that are not present in 'incoming', except channels
        that are protected by community/server settings. Always removes mappings.
        """
        valid_ids = {int(c["id"]) for c in incoming}
        removed = 0
        protected = self._protected_channel_ids(guild)

        for orig_id, row in list(self.chan_map.items()):
            if int(orig_id) in valid_ids:
                continue

            clone_id = int(row["cloned_channel_id"])
            ch = guild.get_channel(clone_id)

            if ch and self.config.DELETE_CHANNELS:
                # 1) Skip if protected
                if ch.id in protected:
                    logger.info(
                        "[🛡️] Skipping deletion of protected channel #%s (%d) (community/system assignment).",
                        ch.name, ch.id
                    )
                else:
                    # 2) Try delete
                    await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                    try:
                        await ch.delete()
                        logger.info("[🗑️] Deleted channel #%s (%d)", ch.name, ch.id)
                    except discord.HTTPException as e:
                        # 50074 = cannot delete a channel required for community servers
                        if getattr(e, "code", None) == 50074 or "required for community" in str(e):
                            logger.info(
                                "[🛡️] API blocked deletion of #%s (%d): protected. Will skip and drop mapping.",
                                getattr(ch, "name", "?"), ch.id
                            )
                        else:
                            logger.warning("[⚠️] Failed to delete channel #%d: %s", ch.id, e)

            elif not ch:
                logger.info("[🗑️] Cloned channel #%d not found; removing mapping", clone_id)

            # Always drop the mapping, even if we couldn't delete the channel.
            self.db.delete_channel_mapping(orig_id)
            self.chan_map.pop(orig_id, None)
            removed += 1

        return removed

    async def _handle_renamed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        """
        Handles renaming of cloned categories in the clone guild based on the sitemap.
        This method compares the current names of cloned categories in the guild
        with the desired names specified in the sitemap. If a mismatch is found,
        the category is renamed, and the changes are persisted in the database
        and the in-memory mapping.
        """
        renamed = 0
        # Build a quick lookup of desired names
        desired = {c["id"]: c["name"] for c in sitemap.get("categories", [])}

        for orig_id, row in self.cat_map.items():
            new_name = desired.get(orig_id)
            if not new_name:
                continue  # no such category in sitemap

            clone_cat = guild.get_channel(row["cloned_category_id"])
            if clone_cat and clone_cat.name != new_name:
                old_name = clone_cat.name
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await clone_cat.edit(name=new_name)
                logger.info("[✏️] Renamed category %s → %s", old_name, new_name)

                # persist change to DB
                self.db.upsert_category_mapping(
                    orig_id,
                    new_name,
                    clone_cat.id,
                    new_name,
                )
                # keep in-memory map up to date
                row["cloned_category_name"] = new_name

                renamed += 1

        return renamed

    async def _ensure_category(
        self, guild: discord.Guild, original_id: int, name: str
    ) -> Tuple[discord.CategoryChannel, bool]:
        """
        Ensure that a mapping exists for original_id → a cloned category.
        Returns (category_obj, did_create) where did_create is True if we had to create it.
        """
        row = self.cat_map.get(original_id)
        if row:
            cat = guild.get_channel(row["cloned_category_id"])
            if cat:
                return cat, False
            # stale mapping fell through to creation

        # create new category
        await self.ratelimit.acquire(ActionType.CREATE_CHANNEL)
        cat = await guild.create_category(name)
        logger.info(
            "[➕] Created category %r (orig ID %d) → clone ID %d",
            name,
            original_id,
            cat.id,
        )
        # persist in DB
        self.db.upsert_category_mapping(
            original_id,
            name,
            cat.id,
            cat.name,
        )
        # update in-memory map
        self.cat_map[original_id] = {
            "original_category_id": original_id,
            "cloned_category_id": cat.id,
            "original_category_name": name,
            "cloned_category_name": cat.name,
        }
        return cat, True

    async def _create_webhook_safely(self, ch, name, avatar_bytes):
        if self._shutting_down:
            return
        async with self._new_webhook_gate:
            rem = self.ratelimit.remaining(ActionType.WEBHOOK_CREATE)
            logger.debug("NEW_WEBHOOK pre-acquire cooldown remaining: %.2fs", rem)
            await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)
            webhook = await ch.create_webhook(name=name, avatar=avatar_bytes)
            logger.info("[➕] Created a webhook in channel %s", ch.name)
            return webhook

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
        Ensures that a channel and its corresponding webhook exist in the clone guild.
        If a mapping already exists and is valid, it returns the existing channel and webhook.
        Otherwise, it creates a new channel and webhook, updates the database, and returns the new mapping.
        """
        if self._shutting_down:
            return
        category = None
        if parent_id is not None:
            category, _ = await self._ensure_category(guild, parent_id, parent_name)

        # 1) existing-mapping / missing-webhook path
        for orig_id, row in list(self.chan_map.items()):
            if orig_id != original_id:
                continue

            clone_id = row["cloned_channel_id"]
            wh_url = row["channel_webhook_url"]
            if clone_id is not None:
                ch = guild.get_channel(clone_id)
                if ch:
                    if wh_url:
                        return original_id, clone_id, wh_url

                    # re-create the webhook
                    wh = await self._create_webhook_safely(
                        ch, "Copycord", await self._get_default_avatar_bytes()
                    )
                    url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"
                    self.db.upsert_channel_mapping(
                        original_id,
                        row["original_channel_name"],
                        clone_id,
                        url,
                        parent_id,
                        category.id if category else None,
                        channel_type,
                    )
                    # update in-memory as well
                    self.chan_map[original_id] = {
                        "original_channel_id": original_id,
                        "original_channel_name": original_name,
                        "cloned_channel_id": clone_id,
                        "channel_webhook_url": url,
                        "original_parent_category_id": parent_id,
                        "cloned_parent_category_id": category.id if category else None,
                        "channel_type": channel_type,
                    }
                    self._schedule_flush(
                        chan_ids={original_id},
                        thread_parent_ids={original_id},
                    )
                    self._unmapped_warned.discard(original_id)
                    return original_id, clone_id, url

                # stale mapping—purge and fall through
                self.db.delete_channel_mapping(original_id)
                break

        # 2) brand-new channel + webhook
        kind = "news" if channel_type == ChannelType.news.value else "text"
        ch = await self._create_channel(guild, kind, original_name, category)
        wh = await self._create_webhook_safely(
            ch, "Copycord", await self._get_default_avatar_bytes()
        )
        url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

        self.db.upsert_channel_mapping(
            original_id,
            original_name,
            ch.id,
            url,
            parent_id,
            category.id if category else None,
            channel_type,
        )
        self.chan_map[original_id] = {
            "original_channel_id": original_id,
            "original_channel_name": original_name,
            "cloned_channel_id": ch.id,
            "channel_webhook_url": url,
            "original_parent_category_id": parent_id,
            "cloned_parent_category_id": category.id if category else None,
            "channel_type": channel_type,
        }
        self._schedule_flush(
            chan_ids={original_id},
            thread_parent_ids={original_id},
        )
        return original_id, ch.id, url

    async def _handle_master_channel_moves(
        self,
        guild: discord.Guild,
        incoming: List[Dict],
    ) -> int:
        """
        Re-parent cloned channels whenever the upstream parent (from sitemap) differs
        from what’s live in Discord. Updates DB mapping so future syncs keep the new parent.
        """
        moved = 0

        for item in incoming:
            orig_id = item["id"]
            row = self.chan_map.get(orig_id)
            if not row:
                continue

            clone_id = row["cloned_channel_id"]
            ch = guild.get_channel(clone_id)
            if not ch:
                continue

            # 1) Determine desired parent from sitemap
            upstream_parent = item["parent_id"]  # None for standalone
            if upstream_parent is None:
                desired_parent = None
                desired_parent_clone_id = None
            else:
                cat_row = self.cat_map.get(upstream_parent)
                desired_parent_clone_id = (
                    cat_row["cloned_category_id"] if cat_row else None
                )
                desired_parent = (
                    guild.get_channel(desired_parent_clone_id)
                    if desired_parent_clone_id
                    else None
                )

            # 2) Check actual parent
            actual_parent = ch.category
            actual_parent_id = actual_parent.id if actual_parent else None

            # 3) If it already matches, skip
            if actual_parent_id == desired_parent_clone_id:
                continue

            # 4) Perform the move
            try:
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await ch.edit(category=desired_parent)
                moved += 1
                old_name = actual_parent.name if actual_parent else "standalone"
                new_name = desired_parent.name if desired_parent else "standalone"
                logger.info(
                    "[✏️] Reparented channel '%s' (ID %d) from '%s' → '%s'",
                    ch.name,
                    clone_id,
                    old_name,
                    new_name,
                )
            except Exception as e:
                logger.warning(
                    "[⚠️] Failed to reparent channel '%s' (ID %d): %s",
                    ch.name,
                    clone_id,
                    e,
                )
                continue
            ctype = ch.type.value if ch else None
            # 5) Persist the new parent in both DB and in-memory map
            self.db.upsert_channel_mapping(
                orig_id,
                row["original_channel_name"],
                clone_id,
                row["channel_webhook_url"],
                upstream_parent,
                desired_parent_clone_id,
                ctype,
            )
            self.chan_map[orig_id][
                "cloned_parent_category_id"
            ] = desired_parent_clone_id

        return moved

    async def _get_default_avatar_bytes(self) -> Optional[bytes]:
        """Fetch (and cache) the default webhook avatar."""
        if self._shutting_down:
            return
        if self._default_avatar_bytes is None:
            url = self.config.DEFAULT_WEBHOOK_AVATAR_URL
            if not url:
                return None

            try:
                if self.session is None or self.session.closed:
                    self.session = aiohttp.ClientSession()
                async with self.session.get(url) as resp:
                    if resp.status == 200:
                        self._default_avatar_bytes = await resp.read()
                    else:
                        logger.warning(
                            "[⚠️] Avatar download failed %s (HTTP %s)", url, resp.status
                        )
            except Exception as e:
                logger.warning("[⚠️] Error downloading avatar %s: %s", url, e)
        return self._default_avatar_bytes

    async def _recreate_webhook(self, original_id: int) -> Optional[str]:
        """
        Recreates a webhook for a given channel if it is missing or invalid.
        This method attempts to retrieve the webhook URL for a channel from the internal
        channel mapping. If the webhook is missing or invalid, it creates a new webhook
        for the corresponding cloned channel and updates the database and internal mapping.
        """
        if self._shutting_down:
            return
        # 1) lookup the DB row
        row = self.chan_map.get(original_id)
        if not row:
            logger.error(
                "[⛔] No DB row for #%s; cannot recreate webhook.", original_id
            )
            return None

        # 2) get the lock for this channel
        lock = self._webhook_locks.setdefault(original_id, asyncio.Lock())

        async with lock:
            # re-fetch the row
            fresh = self.chan_map.get(original_id)
            if not fresh:
                logger.error("[⛔] Mapping disappeared for #%s!", original_id)
                return None

            # use direct indexing instead of .get()
            url = fresh["channel_webhook_url"]  # will be None or str
            if url:
                try:
                    webhook_id = int(url.split("/")[-2])
                    await self.bot.fetch_webhook(webhook_id)
                    return url
                except (NotFound, HTTPException):
                    logger.debug(
                        "Stored webhook #%s for channel #%s missing on Discord; will recreate.",
                        webhook_id,
                        original_id,
                    )

            # fall through to actual creation…
            cloned_id = fresh["cloned_channel_id"]
            guild = self.bot.get_guild(self.clone_guild_id)
            ch = guild.get_channel(cloned_id) if guild else None
            if not ch:
                logger.debug(
                    "[⛔] Cloned channel %s not found for #%s; cannot recreate webhook.",
                    cloned_id,
                    original_id,
                )
                return None
            ctype = ch.type.value
            try:
                wh = await self._create_webhook_safely(
                    ch, "Copycord", await self._get_default_avatar_bytes()
                )
                new_url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

                # persist via direct indexing too
                self.db.upsert_channel_mapping(
                    original_id,
                    fresh["original_channel_name"],
                    cloned_id,
                    new_url,
                    fresh["original_parent_category_id"],
                    fresh["cloned_parent_category_id"],
                    ctype,
                )

                logger.info(
                    "[➕] Recreated missing webhook for channel `%s` #%s",
                    fresh["original_channel_name"],
                    original_id,
                )
                self.chan_map[original_id]["channel_webhook_url"] = new_url
                self._schedule_flush(
                    chan_ids={original_id},
                    thread_parent_ids={original_id},
                )
                return new_url

            except Exception:
                logger.exception("Failed to recreate webhook for #%s", original_id)
                return None

    async def handle_thread_message(self, data: dict):
        """
        Handles the forwarding of thread messages from the original guild to the cloned guild.
        This method ensures that the thread messages are properly forwarded to the corresponding
        cloned thread in the cloned guild. If the cloned thread or its parent channel does not
        exist yet, the message is queued for later processing.
        """
        if self._shutting_down:
            return
        # ensure clone guild
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.error("[⛔] Clone guild %s not available", self.clone_guild_id)
            return

        # parent channel mapping
        self._load_mappings()
        orig_tid = int(data["thread_id"])
        parent_id = int(data["thread_parent_id"])
        tag = self._log_tag(data)

        chan_map = self.chan_map.get(parent_id)
        if not chan_map:
            async with self._warn_lock:
                if orig_tid not in self._unmapped_threads_warned:
                    logger.info(
                        "[⌛] No mapping yet for thread '%s' (thread_id=%s, parent=%s); msg from %s queued until after sync",
                        data.get("thread_name", "<unnamed>"),
                        orig_tid,
                        data.get("thread_parent_name")
                        or data.get("channel_name")
                        or parent_id,
                        data.get("author", "<unknown>"),
                    )
                    self._unmapped_threads_warned.add(orig_tid)
            self._pending_thread_msgs.append(data)
            return

        cloned_parent = guild.get_channel(chan_map["cloned_channel_id"])
        cloned_id = chan_map["cloned_channel_id"]
        if cloned_id is None:
            logger.warning(
                "[⚠️] Channel %s not cloned yet; queueing message until it’s created",
                data["channel_name"],
            )
            self._pending_thread_msgs.append(data)
            return

        if not cloned_parent:
            logger.info(
                "[⌛] Channel %s not cloned yet; queueing message until it’s created",
                cloned_id,
            )
            self._pending_thread_msgs.append(data)
            return

        # build payload
        payload = self._build_webhook_payload(data)
        if not payload or (not payload.get("content") and not payload.get("embeds")):
            logger.info("[⚠️] Skipping empty payload for '%s'", data["thread_name"])
            return

        # prepare webhook & session
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        webhook_url = chan_map["channel_webhook_url"]
        thread_webhook = Webhook.from_url(webhook_url, session=self.session)

        orig_tid = data["thread_id"]
        lock = self._thread_locks.setdefault(orig_tid, asyncio.Lock())
        created = False

        try:
            async with lock:
                # lookup existing thread mapping
                thr_map = next(
                    (
                        r
                        for r in self.db.get_all_threads()
                        if r["original_thread_id"] == orig_tid
                    ),
                    None,
                )

                clone_thread = None

                # Helper to delete stale mapping
                def drop_mapping():
                    self.db.delete_forum_thread_mapping(orig_tid)
                    return None

                # Attempt to fetch existing mapped thread
                if thr_map:
                    try:
                        clone_thread = guild.get_channel(
                            thr_map["cloned_thread_id"]
                        ) or await self.bot.fetch_channel(thr_map["cloned_thread_id"])
                    except HTTPException as e:
                        if e.status == 404:
                            drop_mapping()
                            thr_map = None
                            clone_thread = None
                        else:
                            logger.warning(
                                "[⌛] Error fetching thread %s; adding to queue, waiting for next sync",
                                thr_map["cloned_thread_id"],
                            )
                            self._pending_thread_msgs.append(data)
                            return

                # If no mapping (first use or after deletion), create new
                if thr_map is None:
                    logger.info(
                        "[🧵]%s Creating thread '%s' in #%s by %s (%s)",
                        tag,
                        data["thread_name"],
                        cloned_parent.name,
                        data["author"],
                        data["author_id"],
                    )
                    await self.ratelimit.acquire(ActionType.THREAD)

                    if isinstance(cloned_parent, ForumChannel):
                        # forum: create + post in one step
                        resp_msg = await thread_webhook.send(
                            content=payload.get("content"),
                            embeds=payload.get("embeds"),
                            username=payload.get("username"),
                            avatar_url=payload.get("avatar_url"),
                            thread_name=data["thread_name"],
                            wait=True,
                        )
                        # locate via cache or fetch_active_threads
                        clone_thread = (
                            next(
                                (
                                    t
                                    for t in cloned_parent.threads
                                    if t.name == data["thread_name"]
                                ),
                                None,
                            )
                            or (await cloned_parent.fetch_active_threads()).threads[0]
                        )
                        new_id = clone_thread.id
                        await clone_thread.edit(auto_archive_duration=60)

                    else:
                        # text channel: create then initial post
                        new_thread = await cloned_parent.create_thread(
                            name=data["thread_name"],
                            type=ChannelType.public_thread,
                            auto_archive_duration=60,
                        )
                        new_id = new_thread.id
                        clone_thread = new_thread
                        # initial post
                        await self.ratelimit.acquire(
                            ActionType.WEBHOOK_MESSAGE, key=webhook_url
                        )
                        await thread_webhook.send(
                            content=payload.get("content"),
                            embeds=payload.get("embeds"),
                            username=payload.get("username"),
                            avatar_url=payload.get("avatar_url"),
                            thread=clone_thread,
                            wait=True,
                        )

                    created = True
                    # persist mapping
                    self.db.upsert_forum_thread_mapping(
                        orig_thread_id=orig_tid,
                        orig_thread_name=data["thread_name"],
                        clone_thread_id=new_id,
                        forum_orig_id=data["thread_id"],
                        forum_clone_id=chan_map["cloned_channel_id"],
                    )

            # subsequent messages only
            if not created:
                logger.info(
                    "[💬]%s Forwarding message to thread '%s' in #%s from %s (%s)",
                    tag,
                    data["thread_name"],
                    data["thread_parent_name"],
                    data["author"],
                    data["author_id"],
                )
                await self.ratelimit.acquire(
                    ActionType.WEBHOOK_MESSAGE, key=webhook_url
                )
                await thread_webhook.send(
                    content=payload.get("content"),
                    embeds=payload.get("embeds"),
                    username=payload.get("username"),
                    avatar_url=payload.get("avatar_url"),
                    thread=clone_thread,
                    wait=True,
                )

        finally:
            try:
                await self._enforce_thread_limit(guild)
            except Exception:
                logger.exception("Error enforcing thread limit.")

    async def handle_thread_delete(self, data: dict):
        """
        Handles the deletion of a thread in the host server and optionally deletes
        the corresponding cloned thread in the cloned server.
        """
        if self._shutting_down:
            return
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
            logger.debug(
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
                        await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                        await ch.delete()
                        logger.info(
                            "[🗑️] Deleted thread '%s' in #%s",
                            cloned_thread_name,
                            ch.parent.name,
                        )
                    except Exception as e:
                        logger.error(
                            "[⛔] Failed to delete cloned thread %s: %s", cloned_id, e
                        )
            else:
                logger.warning(
                    "[⚠️] Cloned thread %s not found in guild", cloned_thread_name
                )

        self.db.delete_forum_thread_mapping(orig_thread_id)
        logger.info(
            "[🗑️] Thread '%s' deleted in host server; removed mapping in DB",
            cloned_thread_name,
        )

    async def handle_thread_rename(self, data: dict):
        """
        Handles the renaming of a thread in the cloned guild.
        This method is triggered when a thread is renamed in the host guild. It ensures
        that the corresponding thread in the cloned guild is renamed to match the new name.
        """
        if self._shutting_down:
            return
        orig_thread_id = data["thread_id"]
        new_name = data["new_name"]
        old_name = data["old_name"]
        parent_name = data["parent_name"]
        parent_id = data["parent_id"]

        row = next(
            (
                r
                for r in self.db.get_all_threads()
                if r["original_thread_id"] == orig_thread_id
            ),
            None,
        )
        if not row:
            logger.warning(
                f"[⚠️] Thread renamed in #{parent_name}: {old_name} → {new_name}; does not exist in cloned guild, skipping"
            )
            return

        cloned_id = row["cloned_thread_id"]
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.error("[⛔] Clone guild not available for thread renames")
            return

        ch = guild.get_channel(cloned_id)
        if not ch:
            try:
                ch = await self.bot.fetch_channel(cloned_id)
            except NotFound:
                logger.warning(
                    f"[⚠️] Thread renamed in #{parent_name}: {old_name} → {new_name}; not found in cloned server, cannot rename"
                )
                return

        try:
            await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
            await ch.edit(name=new_name)
            logger.info(
                f"[✏️] Renamed thread in #{ch.parent.name}: {old_name} → {new_name}"
            )
        except Exception as e:
            logger.error(f"[⛔] Failed to rename thread {old_name} in #{ch.name}: {e}")

        self.db.upsert_forum_thread_mapping(
            orig_thread_id,
            new_name,
            cloned_id,
            row["original_thread_id"],
            row["cloned_thread_id"],
        )

    async def _enforce_thread_limit(self, guild: discord.Guild):
        """
        Enforces the thread limit for the clone guild by archiving the oldest active threads
        if the number of active threads exceeds the configured maximum.
        """
        # 1) Build the set of cloned_thread_ids that we still track in the DB
        valid_clone_ids = {r["cloned_thread_id"] for r in self.db.get_all_threads()}

        # 2) Gather all active (non‐archived) threads for which we have a mapping
        active = [
            t
            for t in guild.threads
            if not getattr(t, "archived", False) and t.id in valid_clone_ids
        ]
        logger.debug(
            "Guild %d has %d active, mapped threads: %s",
            guild.id,
            len(active),
            [t.id for t in active],
        )

        # 3) If at or under the limit, nothing to do
        if len(active) <= self.max_threads:
            return

        # 4) Sort oldest → newest and pick how many to archive
        active.sort(
            key=lambda t: t.created_at or datetime.min.replace(tzinfo=timezone.utc)
        )
        num_to_archive = len(active) - self.max_threads
        to_archive = active[:num_to_archive]

        # 5) Archive them
        for thread in to_archive:
            try:
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await thread.edit(archived=True)
                parent = thread.parent
                parent_name = parent.name if parent else "Unknown"
                logger.info(
                    "[✏️] Auto-archived thread '%s' in #%s to respect thread limits",
                    thread.name,
                    parent_name,
                )
            except HTTPException as e:
                if e.status == 404:
                    # Thread truly gone — remove its mapping so we won't retry
                    logger.warning(
                        "[⚠️] Thread %s not found; clearing mapping and skipping future attempts",
                        thread.id,
                    )
                    self.db.delete_forum_thread_mapping(thread.id)
                else:
                    logger.warning(
                        "[⚠️] Failed to auto-archive thread %s: %s", thread.id, e
                    )

    def _replace_emoji_ids(self, content: str) -> str:
        """
        Replaces emoji IDs in the given content string with their corresponding cloned emoji IDs
        based on the database mapping.
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

    def _remap_channel_mentions(self, content: str) -> str:
        """Map host channel mentions to cloned channel mentions using chan_map."""
        if not content:
            return content

        def repl(match: re.Match) -> str:
            orig = int(match.group(1))
            row = self.chan_map.get(orig)
            # row keys come from DB: cloned_channel_id
            if row and row.get("cloned_channel_id"):
                return f"<#{row['cloned_channel_id']}>"
            return match.group(0)

        return self._m_ch.sub(repl, content)

    def _sanitize_inline(self, s: str | None) -> str | None:
        if not s:
            return s
        s = self._replace_emoji_ids(s)
        s = self._remap_channel_mentions(s)
        return s

    def _build_webhook_payload(self, msg: Dict) -> dict:
        """
        Constructs a webhook payload from a given message dictionary.
        Processes text, attachments, embeds, channel mentions, and stickers (as image embeds).
        Also replaces custom emoji IDs in text and embed fields.
        """
        # 1) Build up the text blob
        text = self._sanitize_inline(msg.get("content", "") or "")

        for att in msg.get("attachments", []) or []:
            url = att.get("url")
            if url and url not in text:
                text += f"\n{url}"

        raw_embeds = msg.get("embeds", []) or []
        embeds: list[Embed] = []

        # Convert raw embeds; push heavy media URLs into text if needed
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
                    logger.warning("[⚠️] Could not convert embed dict to Embed: %s", e)
            elif isinstance(raw, Embed):
                embeds.append(raw)

        # sanitize embeds with clone guild replacements
        for e in embeds:
            # top-level text
            if getattr(e, "description", None):
                e.description = self._sanitize_inline(e.description)
            if getattr(e, "title", None):
                e.title = self._sanitize_inline(e.title)

            # footer
            if getattr(e, "footer", None) and getattr(e.footer, "text", None):
                e.footer.text = self._sanitize_inline(e.footer.text)

            # author
            if getattr(e, "author", None) and getattr(e.author, "name", None):
                e.author.name = self._sanitize_inline(e.author.name)

            # fields
            for f in getattr(e, "fields", []) or []:
                if getattr(f, "name", None):
                    f.name = self._sanitize_inline(f.name)
                if getattr(f, "value", None):
                    f.value = self._sanitize_inline(f.value)

        base = {
            "username": msg.get("author") or "Unknown",
            "avatar_url": msg.get("avatar_url"),
        }

        # If content too long, move it into an embed
        if len(text) > 2000:
            long_embed = Embed(description=text[:4096])
            return {**base, "content": None, "embeds": [long_embed] + embeds}

        # Normal payload
        payload = {**base, "content": (text or None), "embeds": embeds}
        return payload

    def _log_tag(self, data: dict) -> str:
        """Return a short tag like ' [sync & buffered]' for backfill/buffered messages."""
        parts = []
        if data.get("__backfill__"):
            parts.append("msg-sync")
        if data.get("__buffered__"):
            parts.append("buffered")
        return f" [{' & '.join(parts)}]" if parts else ""

    async def forward_message(self, msg: Dict):
        """
        Forwards a message to the appropriate channel webhook based on the channel mapping.
        Queues when mapping/sync unavailable, validates payload, and handles RL/retries.
        """
        if self._shutting_down:
            return

        tag = self._log_tag(msg)
        source_id = msg["channel_id"]
        is_backfill = bool(msg.get("__backfill__"))

        # Buffer live messages while backfill is running for this source channel
        if self.backfill.is_backfilling(source_id) and not is_backfill:
            msg["__buffered__"] = True
            self._pending_msgs.setdefault(source_id, []).append(msg)
            logger.debug(
                "[⏳] Buffered live message during backfill for #%s", source_id
            )
            return

        # ----- Lookup mapping early; bail if missing BEFORE any rotation logic -----
        mapping = self.chan_map.get(source_id)
        if mapping is None:
            self._load_mappings()
            mapping = self.chan_map.get(source_id)
        if mapping is None:
            async with self._warn_lock:
                if source_id not in self._unmapped_warned:
                    logger.info(
                        "[⌛] No mapping yet for channel %s (%s); msg from %s is queued and will be sent after sync",
                        msg["channel_name"],
                        msg["channel_id"],
                        msg["author"],
                    )
                    self._unmapped_warned.add(source_id)
            msg["__buffered__"] = True
            self._pending_msgs.setdefault(source_id, []).append(msg)
            return

        url = mapping.get("channel_webhook_url")
        clone_id = mapping.get("cloned_channel_id") or mapping.get("clone_channel_id")

        # ----- Stickers path: try cloned -> standard -> webhook image-embed fallback -----
        stickers = msg.get("stickers") or []
        if stickers:
            guild = self.bot.get_guild(self.clone_guild_id)
            ch = (
                guild.get_channel(mapping["cloned_channel_id"])
                if (guild and mapping)
                else None
            )

            handled = await self.stickers.send_with_fallback(
                receiver=self,
                ch=ch,
                stickers=stickers,
                mapping=mapping,
                msg=msg,
                source_id=source_id,
            )
            if handled:
                if is_backfill:
                    self.backfill.note_sent(source_id)
                    d, t = self.backfill.get_progress(source_id)
                    suffix = f" [{d}/{t}]" if t else f" [{d}]"
                    logger.info(
                        "[💬]%s Forwarded (stickers) to #%s from %s (%s)%s",
                        tag,
                        msg["channel_name"],
                        msg["author"],
                        msg["author_id"],
                        suffix,
                    )
                return

        # ----- Recreate missing webhook if needed -----
        if mapping and not url:
            if self._sync_lock.locked():
                logger.info(
                    "[⌛] Sync in progress; message in #%s from %s is queued and will be sent after sync",
                    msg["channel_name"],
                    msg["author"],
                )
                msg["__buffered__"] = True
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return

            logger.warning(
                "[⚠️] Mapped channel %s has no webhook; attempting to recreate",
                msg["channel_name"],
            )
            url = await self._recreate_webhook(source_id)
            if not url:
                logger.info(
                    "[⌛] Could not recreate webhook for #%s; queued message from %s",
                    msg["channel_name"],
                    msg["author"],
                )
                msg["__buffered__"] = True
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return

        # ----- Build and validate payload -----
        payload = self._build_webhook_payload(msg)
        if payload is None:
            logger.debug(
                "No webhook payload built for #%s; skipping", msg["channel_name"]
            )
            return

        if not payload.get("content") and not payload.get("embeds"):
            logger.info(
                "[⚠️]%s Skipping empty message in #%s (attachments=%d stickers=%d)",
                tag,
                msg["channel_name"],
                len(msg.get("attachments") or []),
                len(msg.get("stickers") or []),
            )
            return

        if payload.get("content"):
            try:
                import json

                json.dumps({"content": payload["content"]})
            except (TypeError, ValueError) as e:
                logger.error(
                    "[⛔] Skipping message from #%s: content not JSON serializable: %s; content=%r",
                    msg["channel_name"],
                    e,
                    payload["content"],
                )
                return

        # ----------------------
        # Sending helpers
        # ----------------------
        if not hasattr(self, "_webhooks"):
            self._webhooks = {}  # url -> Webhook

        async def _do_send(url_to_use: str, rl_key: str):
            if self._shutting_down:
                return
            from aiohttp import ClientError
            import aiohttp, asyncio  # ensure aiohttp is available here

            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()

            webhook = self._webhooks.get(url_to_use)
            if webhook is None or webhook.session is None or webhook.session.closed:
                webhook = Webhook.from_url(url_to_use, session=self.session)
                self._webhooks[url_to_use] = webhook

            await self.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=rl_key)

            if self._shutting_down:
                return

            try:
                await webhook.send(
                    content=payload.get("content"),
                    embeds=payload.get("embeds"),
                    username=payload.get("username"),
                    avatar_url=payload.get("avatar_url"),
                    wait=True,
                )

                if is_backfill:
                    self.backfill.note_sent(source_id)
                    sent, total = self.backfill.get_progress(source_id)
                    left = max(total - sent, 0)
                    logger.info(
                        "[💬] [msg-sync] Forwarded message to #%s from %s (%s) [%d left]",
                        msg["channel_name"],
                        msg["author"],
                        msg["author_id"],
                        left,
                    )
                    # gentle relax after success
                    self.ratelimit.relax(ActionType.WEBHOOK_MESSAGE, key=rl_key)
                else:
                    logger.info(
                        "[💬] Forwarded message to #%s from %s (%s)",
                        msg["channel_name"],
                        msg["author"],
                        msg["author_id"],
                    )

            except HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, "retry_after", None)
                    if retry_after is None:
                        try:
                            retry_after = float(
                                getattr(e, "response", None).headers.get(
                                    "X-RateLimit-Reset-After", 0
                                )
                            )
                        except Exception:
                            retry_after = 2.0
                    delay = max(0.0, float(retry_after))
                    logger.warning(
                        "[⏱️]%s 429 for #%s — sleeping %.2fs then retrying",
                        tag,
                        msg["channel_name"],
                        delay,
                    )

                    self.ratelimit.penalize(ActionType.WEBHOOK_MESSAGE, key=rl_key)

                    await self.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=rl_key)
                    webhook2 = self._webhooks.get(url_to_use) or Webhook.from_url(
                        url_to_use, session=self.session
                    )
                    if self._shutting_down:
                        return
                    await webhook2.send(
                        content=payload.get("content"),
                        embeds=payload.get("embeds"),
                        username=payload.get("username"),
                        avatar_url=payload.get("avatar_url"),
                        wait=True,
                    )
                    if is_backfill:
                        self.backfill.note_sent(source_id)
                        sent, total = self.backfill.get_progress(source_id)
                        left = max(total - sent, 0)
                        logger.info(
                            "[💬] [msg-sync] Forwarded message to #%s from %s (%s) (after 429 retry) [%d left]",
                            msg["channel_name"],
                            msg["author"],
                            msg["author_id"],
                            left,
                        )
                    else:
                        logger.info(
                            "[💬]%s Forwarded message to #%s from %s (%s) (after 429 retry)",
                            tag,
                            msg["channel_name"],
                            msg["author"],
                            msg["author_id"],
                        )

                elif e.status == 404:
                    logger.debug(
                        "Webhook %s returned 404; attempting recreate...", url_to_use
                    )
                    new_url = await self._recreate_webhook(source_id)
                    if not new_url:
                        logger.warning(
                            "[⌛] No mapping for channel %s; msg from %s is queued and will be sent after sync",
                            msg["channel_name"],
                            msg["author"],
                        )
                        msg["__buffered__"] = True
                        self._pending_msgs.setdefault(source_id, []).append(msg)
                        return

                    await self.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=rl_key)
                    webhook3 = self._webhooks.get(new_url) or Webhook.from_url(
                        new_url, session=self.session
                    )
                    self._webhooks[new_url] = webhook3
                    if self._shutting_down:
                        return
                    await webhook3.send(
                        content=payload.get("content"),
                        embeds=payload.get("embeds"),
                        username=payload.get("username"),
                        avatar_url=payload.get("avatar_url"),
                        wait=True,
                    )

                    if is_backfill:
                        self.backfill.note_sent(source_id)
                        sent, total = self.backfill.get_progress(source_id)
                        left = max(total - sent, 0)
                        logger.info(
                            "[💬] [msg-sync] Forwarded message to #%s from %s (%s) [%d left]",
                            msg["channel_name"],
                            msg["author"],
                            msg["author_id"],
                            left,
                        )
                    else:
                        logger.info(
                            "[💬]%s Forwarded message to #%s from %s (%s)",
                            tag,
                            msg["channel_name"],
                            msg["author"],
                            msg["author_id"],
                        )
                else:
                    logger.error(
                        "[⛔] Failed to send to #%s (status %s): %s",
                        msg["channel_name"],
                        e.status,
                        e.text,
                    )
            except (ClientError, asyncio.TimeoutError) as e:
                # network issue → queue for later rather than drop
                logger.warning(
                    "[🌐]%s Network error sending to #%s: %s — queued for retry",
                    tag,
                    msg["channel_name"],
                    e,
                )
                msg["__buffered__"] = True
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return

        # ----------------------
        # Backfill vs live send
        # ----------------------
        if is_backfill and clone_id:
            await self.backfill.ensure_temps_ready(int(clone_id))
            sem = self.backfill.semaphores.setdefault(
                int(clone_id), asyncio.Semaphore(1)
            )
            async with sem:
                url_to_use, _ = await self.backfill.pick_url_for_send(
                    int(clone_id), url, create_missing=False
                )
                rl_key = f"channel:{clone_id}"  # aggregate RL for backfill
                await _do_send(url_to_use, rl_key)
            return
        else:
            # Always rotate live messages across existing webhooks (no creation)
            if clone_id:
                sem = self.backfill.semaphores.setdefault(
                    int(clone_id), asyncio.Semaphore(1)
                )
                async with sem:
                    url_to_use, _ = await self.backfill.pick_url_for_send(
                        int(clone_id),
                        url,
                        create_missing=False,  # live never creates temps
                    )
                    rl_key = url_to_use  # per-webhook RL for live
                    await _do_send(url_to_use, rl_key)
                return

            # Fallback: no clone_id → use primary webhook
            url_to_use = url
            rl_key = url_to_use
            await _do_send(url_to_use, rl_key)

    async def _shutdown(self):
        """
        Gracefully shut down the server:
        1) stop accepting new work (WS, flags)
        2) cancel/wait background tasks
        3) let backfill clean up (DM summary, temp webhooks)
        4) close HTTP session(s) and bot last
        """
        if getattr(self, "_shutting_down", False):
            return
        self._shutting_down = True

        logger.info("Shutting down server...")

        try:
            ws = getattr(self, "ws_manager", None) or getattr(self, "ws", None)
            if ws and hasattr(ws, "stop"):
                await ws.stop()
        except Exception:
            logger.debug("[shutdown] ws stop failed", exc_info=True)

        async def _cancel_and_wait(task, name: str):
            if not task:
                return
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug(
                    "[shutdown] %s task error during cancel/wait", name, exc_info=True
                )

        await _cancel_and_wait(getattr(self, "_flush_bg_task", None), "flush")
        await _cancel_and_wait(getattr(self, "_sitemap_task", None), "sitemap")
        await _cancel_and_wait(getattr(self, "_ws_task", None), "ws")

        setattr(self, "_suppress_backfill_dm", True)
        try:
            bf = getattr(self, "backfill", None)
            if bf and hasattr(bf, "shutdown") and callable(bf.shutdown):
                await bf.shutdown()
        except Exception:
            logger.debug("[shutdown] backfill cleanup failed", exc_info=True)

        try:
            if getattr(self, "session", None) and not self.session.closed:
                await self.session.close()
        except Exception:
            logger.debug("[shutdown] aiohttp session close failed", exc_info=True)

        try:
            if hasattr(self, "bot") and self.bot and not self.bot.is_closed():
                await self.bot.close()
        except Exception:
            logger.debug("[shutdown] bot close failed", exc_info=True)

        logger.info("Shutdown complete.")

    def run(self):
        """
        Starts the Copycord server and manages the event loop.
        This method initializes the asyncio event loop, sets up signal handlers
        for graceful shutdown on SIGTERM and SIGINT, and starts the bot using
        the provided server token from the configuration. It ensures proper
        cleanup of resources and pending tasks during shutdown.
        """
        logger.info("[✨] Starting Copycord Server %s", self.config.CURRENT_VERSION)
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))

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
