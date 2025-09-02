# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import contextlib
import signal
import asyncio
import logging
from typing import List, Optional, Tuple, Dict, Union, Coroutine, Any
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
import hashlib
import time
from datetime import datetime, timezone
from asyncio import Queue

from common.config import Config, CURRENT_VERSION
from common.websockets import WebsocketManager, AdminBus
from common.db import DBManager
from server.rate_limiter import RateLimitManager, ActionType
from server.discord_hooks import install_discord_rl_probe
from server.emojis import EmojiManager
from server.stickers import StickerManager
from server.roles import RoleManager
from server.backfill import BackfillManager, BackfillTask, BackfillTracker
from server.helpers import OnJoinService, VerifyController

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
        self._flush_targets: set[int] = set()
        self._flush_thread_targets: set[int] = set()
        self._webhook_locks: Dict[int, asyncio.Lock] = {}
        self._new_webhook_gate = asyncio.Lock()
        self.sticker_map: dict[int, dict] = {}
        self.cat_map: dict[int, dict] = {}
        self.chan_map: dict[int, dict] = {}
        self._unmapped_warned: set[int] = set()
        self._unmapped_threads_warned: set[int] = set()
        self._webhooks: dict[str, Webhook] = {}
        self._warn_lock = asyncio.Lock()
        self._active_backfills: set[int] = set()
        self._send_tasks: set[asyncio.Task] = set()
        self._wh_identity_state: dict[int, bool] = {}
        self._wh_meta: dict[int, dict] = {}
        self._wh_meta_ttl = 300
        self._default_avatar_sha1: str | None = None
        self._shutting_down = False
        orig_on_connect = self.bot.on_connect
        self.bus = AdminBus(
            role="server", logger=logger, admin_ws_url=self.config.ADMIN_WS_URL
        )
        self.backfills = BackfillTracker(self.bus)
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
        self.roles = RoleManager(
            bot=self.bot,
            db=self.db,
            ratelimit=self.ratelimit,
            clone_guild_id=int(self.config.CLONE_GUILD_ID),
            delete_roles=bool(self.config.DELETE_ROLES),
            mirror_permissions=bool(self.config.MIRROR_ROLE_PERMISSIONS),
        )
        self.onjoin = OnJoinService(self.bot, self.db, logger.getChild("OnJoin"))
        install_discord_rl_probe(self.ratelimit)

        self.MAX_GUILD_CHANNELS = 500
        self.MAX_CATEGORIES = 50
        self.MAX_CHANNELS_PER_CATEGORY = 50
        self._EMOJI_RE = re.compile(r"<(a?):(?P<name>[^:]+):(?P<id>\d+)>")
        self._m_role = re.compile(r"<@&(?P<id>\d+)>")

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

    def _track(
        self, coro: Coroutine[Any, Any, Any], name: str | None = None
    ) -> asyncio.Task:
        t = asyncio.create_task(coro, name=name or "send")
        self._send_tasks.add(t)
        t.add_done_callback(lambda tt: self._send_tasks.discard(tt))
        return t

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
        if not hasattr(self, "verify"):
            self.verify = VerifyController(
                bus=self.bus,
                admin_base_url=self.config.ADMIN_WS_URL,
                bot=self.bot,
                guild_id=self.clone_guild_id,
                db=self.db,
                ratelimit=self.ratelimit,
                get_protected_channel_ids=self._protected_channel_ids,
                action_type_delete_channel=ActionType.DELETE_CHANNEL,
                logger=logger,
            )
            self.verify.start()
        self._verify_task = asyncio.create_task(self._verify_listen_loop())
        await self.bus.log("Boot completed")
        await self.update_status(f"{CURRENT_VERSION}")

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

        member = clone_guild.get_member(self.bot.user.id)
        if member:
            msg = f"Logged in as {member.display_name} in {clone_guild.name}"
        else:
            msg = f"Logged in as {self.bot.user.name} in {clone_guild.name}"

        await self.bus.status(running=True, status=msg, discord={"ready": True})

        logger.info("[🤖] %s", msg)

        if not self.config.ENABLE_CLONING:
            logger.info("[🔕] Server cloning is disabled...")

        asyncio.create_task(self.backfill.cleanup_non_primary_webhooks())

        if not self._processor_started:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))
            self._sitemap_task = asyncio.create_task(self.process_sitemap_queue())
            self._processor_started = True

    def _canonical_webhook_name(self) -> str:

        return self.backfill._canonical_temp_name()

    async def _primary_name_changed_from_db(
        self, any_channel_id: int
    ) -> tuple[bool, str | None, int | None, int | None]:
        """
        Returns (changed, current_name, original_id, clone_id)
        changed=True iff primary webhook *name* != canonical; None-safe on failures.
        """
        try:

            orig_id, clone_id, _ = self.db.resolve_original_from_any_id(
                int(any_channel_id)
            )
            if not orig_id:
                return False, None, None, None

            row = self.db.get_channel_mapping_by_original_id(int(orig_id))
            if not row:

                if clone_id:
                    row = self.db.get_channel_mapping_by_clone_id(int(clone_id))
                if not row:
                    return False, None, orig_id, clone_id

            purl = row["channel_webhook_url"]
            if not purl:
                return False, None, orig_id, clone_id

            wid = int(str(purl).rstrip("/").split("/")[-2])
            wh = await self.bot.fetch_webhook(wid)

            current = (wh.name or "").strip()
            canonical = self._canonical_webhook_name()
            changed = bool(current and current != canonical)
            return changed, current, orig_id, clone_id
        except Exception:
            return False, None, None, None

    async def _log_primary_name_toggle_if_needed(self, any_channel_id: int) -> None:
        changed, current_name, orig_id, clone_id = (
            await self._primary_name_changed_from_db(any_channel_id)
        )
        if orig_id is None:
            return

        prev = self._wh_identity_state.get(orig_id)
        if prev is not None and prev == changed:
            return

        self._wh_identity_state[orig_id] = changed

        try:
            where = f"clone #{clone_id}" if clone_id else f"original #{orig_id}"
            canonical = self._canonical_webhook_name()
            if changed:
                logger.warning(
                    "[ℹ️] Primary webhook name changed to %r in %s — "
                    "per-message author metadata (username & avatar) will be DISABLED to honor the webhook's identity. "
                    "If you want author metadata again, rename the webhook back to %r.",
                    current_name,
                    where,
                    canonical,
                )
        except Exception:
            pass

    async def on_webhooks_update(self, channel: discord.abc.GuildChannel):
        if self.config.ENABLE_CLONING:
            if self._shutting_down:
                return
            try:
                if channel.guild.id != self.clone_guild_id:
                    return
            except AttributeError:
                return
            self.backfill.invalidate_rotation(int(channel.id))
            self._wh_meta.clear()

            await self._log_primary_name_toggle_if_needed(int(channel.id))

            logger.debug(
                "[rotate] Webhooks changed in #%s — rotation invalidated + meta cleared",
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

            if getattr(self, "_sync_lock", None) and self._sync_lock.locked():
                logger.debug(
                    "[🛑] Sync in progress — ignoring sitemap request for deleted channel %s",
                    channel.id,
                )
                return

            is_category = (
                isinstance(channel, discord.CategoryChannel)
                or getattr(channel, "type", None) == discord.ChannelType.category
            )

            if is_category:

                hit_src_cat_id = None
                for orig_cat_id, row in list(self.cat_map.items()):
                    if int(row.get("cloned_category_id") or 0) == int(channel.id):
                        hit_src_cat_id = int(orig_cat_id)
                        break

                if hit_src_cat_id is None:
                    return

                self.cat_map.pop(hit_src_cat_id, None)

                logger.warning(
                    "[🧹] Cloned category deleted: id=%s name=%s (src_cat=%s). Requesting sitemap.",
                    channel.id,
                    getattr(channel, "name", "?"),
                    hit_src_cat_id,
                )

                await self.bot.ws_manager.send({"type": "sitemap_request"})
                return

            hit_src_id = None
            for src_id, row in list(self.chan_map.items()):
                if int(row.get("cloned_channel_id") or 0) == int(channel.id):
                    hit_src_id = int(src_id)
                    break

            if hit_src_id is None:
                return

            try:
                self.backfill.invalidate_rotation(int(channel.id))
            except Exception:
                pass

            self.chan_map.pop(hit_src_id, None)

            logger.warning(
                "[🧹] Cloned channel deleted: id=%s name=%s (src=%s). Requesting sitemap.",
                channel.id,
                getattr(channel, "name", "?"),
                hit_src_id,
            )

            await self.bot.ws_manager.send({"type": "sitemap_request"})

    async def _verify_listen_loop(self):
        """
        Subscribes to /ws/out and handles UI 'verify' requests.
        """
        base = self._admin_base()

        async def _handler(ev: dict):
            if ev.get("kind") != "verify" or ev.get("role") != "ui":
                return
            payload = ev.get("payload") or {}
            await self._handle_verify_payload(payload)

        await self.bus.subscribe(base, _handler)

    async def _on_ws(self, msg: dict):
        """
        Handles incoming WebSocket messages and dispatches them based on their type.
        """
        if self._shutting_down:
            return
        typ = msg.get("type")
        data = msg.get("data", {})
        if typ == "sitemap":
            if getattr(self, "_shutting_down", False):
                return
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
                logger.debug("[🔇] Drop voice/stage msg | type=%s data=%s", ct, data)
                return

            if data.get("__backfill__"):
                try:
                    orig = int(data.get("channel_id"))
                except Exception:
                    return
                if orig not in self._active_backfills:
                    logger.warning(
                        "Dropping stray backfill message for %s (no active lock)", orig
                    )
                    return
                t = self._track(self._handle_backfill_message(data), name="bf-handle")
                self.backfill.attach_task(orig, t)
            else:

                self._track(self.forward_message(data), name="live-forward")

        elif typ == "thread_message":
            self._track(self.handle_thread_message(data), name="thread-msg")

        elif typ == "thread_delete":
            asyncio.create_task(self.handle_thread_delete(data))

        elif typ == "thread_rename":
            asyncio.create_task(self.handle_thread_rename(data))

        elif typ == "announce":
            asyncio.create_task(self.handle_announce(data))

        elif typ == "backfill_started":
            data = msg.get("data") or {}
            cid_raw = data.get("channel_id")
            try:
                orig = int(cid_raw)
            except (TypeError, ValueError):
                logger.error("backfill_started missing/invalid channel_id: %r", cid_raw)
                return

            if orig in self._active_backfills:
                await self.bus.publish(
                    "client", {"type": "backfill_busy", "data": {"channel_id": orig}}
                )
                return

            self._active_backfills.add(orig)

            await self.backfill.on_started(orig, meta={"range": data.get("range")})

            await self.bus.publish(
                "client",
                {"type": "backfill_ack", "data": {"channel_id": str(orig)}, "ok": True},
            )
            return

        elif typ == "backfill_progress":
            data = msg.get("data") or {}
            cid_raw = data.get("channel_id")
            try:
                cid = int(cid_raw)
            except (TypeError, ValueError):
                logger.error(
                    "backfill_progress missing/invalid channel_id: %r", cid_raw
                )
                return

            total = data.get("total")
            sent = data.get("sent")
            count = data.get("count")

            if total is not None:
                try:
                    self.backfill.update_expected_total(cid, int(total))
                except Exception:
                    pass
            if sent is not None:
                try:
                    await self.backfill.on_progress(cid, int(sent))
                except Exception:
                    pass
            elif count is not None:
                try:
                    await self.backfill.on_progress(cid, int(count))
                except Exception:
                    pass

            delivered, total_est = self.backfill.get_progress(cid)

            await self.bus.publish(
                "client",
                {
                    "type": "backfill_progress",
                    "data": {
                        "channel_id": str(cid),
                        "delivered": delivered,
                        "total": total_est,
                    },
                },
            )
            return

        elif typ in ("backfill_done", "backfill_stream_end"):
            data = msg.get("data") or {}
            cid_raw = data.get("channel_id")
            try:
                orig = int(cid_raw)
            except (TypeError, ValueError):
                logger.error("backfill_done missing/invalid channel_id: %r", cid_raw)
                return

            try:
                await self.backfill.on_done(orig)
                delivered, total_est = self.backfill.get_progress(orig)
                await self.bus.publish(
                    "client",
                    {
                        "type": "backfill_done",
                        "data": {
                            "channel_id": str(orig),
                            "sent": delivered,
                            "total": total_est,
                        },
                    },
                )
            finally:
                self._active_backfills.discard(orig)
            return

        elif typ == "member_joined":
            asyncio.create_task(self.onjoin.handle_member_joined(data))

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
                logger.info("[💾] Sync task #%d completed: %s", task_id, summary)
            finally:
                self.sitemap_queue.task_done()

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

        for orig, row in list(self.cat_map.items()):
            if not guild.get_channel(row["cloned_category_id"]):
                logger.info("[🗑️] Purging category mapping %d", orig)
                self.db.delete_category_mapping(orig)
                self.cat_map.pop(orig)

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

            if self.config.CLONE_EMOJI:
                self.emojis.kickoff_sync(sitemap.get("emojis", []))

            if self.config.CLONE_STICKER:
                self.stickers.kickoff_sync()

            if self.config.CLONE_ROLES:
                self.roles.kickoff_sync(sitemap.get("roles", []))

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
                )

                for ch_orig_id, ch_row in self.chan_map.items():
                    if ch_row["original_parent_category_id"] != orig_cat_id:
                        continue

                    ch = guild.get_channel(ch_row["cloned_channel_id"])
                    ctype = (
                        int(ch.type.value)
                        if ch
                        else int(ch_row.get("channel_type") or ChannelType.text.value)
                    )

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

            if fmap and guild.get_channel(fmap["cloned_channel_id"]):
                continue

            parent = None
            if forum.get("category_id") is not None:
                cat_row = self.cat_map.get(forum["category_id"])
                parent = (
                    guild.get_channel(cat_row["cloned_category_id"])
                    if cat_row
                    else None
                )

            ch = await self._create_channel(guild, "forum", forum["name"], parent)
            created += 1

            wh = await self._create_webhook_safely(
                ch, "Copycord", await self._get_default_avatar_bytes()
            )
            url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

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

            if ctype == ChannelType.news.value:

                if "NEWS" in guild.features and ch.type != ChannelType.news:
                    await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                    await ch.edit(type=ChannelType.news)
                    converted += 1
                    logger.info(
                        "[✏️] Converted channel '%s' #%d → Announcement", ch.name, ch.id
                    )

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

            did_rename, _reason = await self._maybe_rename_channel(ch, name, orig)
            if did_rename:
                renamed += 1

        if created:
            parts.append(f"Created {created} channels")
        if converted:
            parts.append(f"Converted {converted} channels to Announcement")
        if renamed:
            parts.append(f"Renamed {renamed} channels")

        return parts

    async def _sync_threads(self, guild: Guild, sitemap: Dict) -> List[str]:
        """
        Reconcile thread mappings:
        • If the CLONE thread is missing but the ORIGINAL still exists upstream -> clear DB mapping only.
        • If the ORIGINAL thread is gone upstream -> clear mapping and optionally delete the clone.
        • Rename surviving cloned threads whose names changed upstream.
        """
        parts: List[str] = []

        valid_upstream_ids: set[int] = set()
        for t in sitemap.get("threads", []):
            try:
                valid_upstream_ids.add(int(t["id"]))
            except Exception:
                pass

        for row in self.db.get_all_threads():
            try:
                valid_upstream_ids.add(int(row["original_thread_id"]))
            except Exception:
                pass

        deleted_original_gone = 0
        cleared_missing_clone = 0

        for row in self.db.get_all_threads():
            try:
                orig_id = int(row["original_thread_id"])
                clone_id = int(row["cloned_thread_id"])
            except (TypeError, ValueError):

                self.db.delete_forum_thread_mapping(row.get("original_thread_id"))
                continue

            thread_name = row["original_thread_name"]

            try:
                clone_ch = guild.get_channel(clone_id) or await self.bot.fetch_channel(
                    clone_id
                )
            except (NotFound, HTTPException):
                clone_ch = None

            if clone_ch is None and orig_id in valid_upstream_ids:
                logger.info(
                    "[🧹] Cloned thread missing (clone=%s) for '%s'; clearing mapping.",
                    clone_id,
                    thread_name,
                )
                self.db.delete_forum_thread_mapping(orig_id)
                cleared_missing_clone += 1
                continue

            if orig_id not in valid_upstream_ids:
                host_guild = self.bot.get_guild(self.host_guild_id)
                still_exists = False
                if host_guild:
                    ch = host_guild.get_channel(orig_id)
                    if ch is None:
                        try:
                            ch = await self.bot.fetch_channel(orig_id)
                        except (NotFound, HTTPException):
                            ch = None
                    from discord import Thread

                    still_exists = isinstance(ch, Thread)

                if still_exists:

                    logger.debug(
                        "[sync-threads] Skipping delete: host thread %s still exists",
                        orig_id,
                    )
                else:

                    logger.info(
                        "[🗑️] Thread %s no longer present in the host server; clearing mapping (clone=%s)",
                        thread_name,
                        clone_id,
                    )
                    if clone_ch and getattr(self.config, "DELETE_THREADS", False):
                        await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                        await clone_ch.delete()
                        logger.info("[🗑️] Deleted cloned thread %s", clone_id)
                    self.db.delete_forum_thread_mapping(orig_id)
                    deleted_original_gone += 1
                    continue

        if deleted_original_gone:
            parts.append(f"Deleted {deleted_original_gone} threads (original gone)")
        if cleared_missing_clone:
            parts.append(f"Cleared {cleared_missing_clone} missing-clone mappings")

        renamed = 0
        for src in sitemap.get("threads", []):
            try:
                src_id_int = int(src["id"])
            except (KeyError, TypeError, ValueError):
                continue

            mapping = next(
                (
                    r
                    for r in self.db.get_all_threads()
                    if int(r["original_thread_id"]) == src_id_int
                ),
                None,
            )
            if not mapping:
                continue

            try:
                cloned_id = int(mapping["cloned_thread_id"])
            except (TypeError, ValueError):
                continue

            ch = guild.get_channel(cloned_id)
            if ch and ch.name != src["name"]:
                old = ch.name
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await ch.edit(name=src["name"])
                # Keep mapping's name in sync
                self.db.upsert_forum_thread_mapping(
                    orig_thread_id=src_id_int,
                    orig_thread_name=src["name"],
                    clone_thread_id=ch.id,
                    forum_orig_id=(
                        int(mapping["forum_original_id"])
                        if mapping["forum_original_id"] is not None
                        else None
                    ),
                    forum_clone_id=(
                        int(mapping["forum_cloned_id"])
                        if mapping["forum_cloned_id"] is not None
                        else None
                    ),
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

        if target_chans:
            for cid in list(target_chans):
                await self._flush_channel_buffer(cid)
        else:
            for cid in list(self._pending_msgs.keys()):
                await self._flush_channel_buffer(cid)

        if target_thread_parents:
            for pid in list(target_thread_parents):
                await self._flush_thread_parent_buffer(pid)
        else:

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

                self._pending_msgs.setdefault(original_id, []).append(m)
                logger.exception(
                    "[⚠️] Error forwarding buffered msg for #%s; requeued", original_id
                )

    async def _flush_thread_parent_buffer(self, parent_original_id: int) -> None:
        """Flush queued thread messages whose parent is now available."""
        if self._shutting_down or not self._pending_thread_msgs:
            return

        to_send: list[dict] = []
        remaining: list[dict] = []
        for data in list(self._pending_thread_msgs):
            if data.get("thread_parent_id") == parent_original_id:
                to_send.append(data)
            else:
                remaining.append(data)

        self._pending_thread_msgs = remaining

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

        if not chan_ids and not thread_parent_ids:
            self._flush_full_flag = True
        else:
            if chan_ids:
                self._flush_targets |= set(chan_ids)
            if thread_parent_ids:
                self._flush_thread_targets |= set(thread_parent_ids)

        if self._flush_bg_task and not self._flush_bg_task.done():
            return

        async def _runner():
            try:

                while True:
                    full = self._flush_full_flag
                    chans = self._flush_targets.copy()
                    threads = self._flush_thread_targets.copy()

                    self._flush_full_flag = False
                    self._flush_targets.clear()
                    self._flush_thread_targets.clear()

                    if full:
                        await self._flush_buffers()
                    else:
                        await self._flush_buffers(
                            target_chans=(chans or None),
                            target_thread_parents=(threads or None),
                        )

                    if (
                        not self._flush_full_flag
                        and not self._flush_targets
                        and not self._flush_thread_targets
                    ):
                        break

                    await asyncio.sleep(0)
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

    async def _maybe_rename_channel(
        self,
        ch: discord.abc.GuildChannel,
        upstream_name: str,
        orig_source_id: int,
    ) -> tuple[bool, str]:
        """
        Ensure the cloned channel's name is correct given:
        - upstream_name: the current name from the host/server (client sitemap)
        - clone "pin": channel_mappings.clone_channel_name

        Rules:
        - If a pin (clone_channel_name) exists:
            * If the live clone name != pin, rename to the pin.
            * Always persist the latest upstream name into DB (mapping), but keep the pin.
        - If no pin:
            * If live clone name != upstream_name, rename to upstream_name.

        Returns:
        (did_rename, reason)
            reason ∈ {"pinned_enforced","match_upstream","skipped_already_ok","skipped_error"}
        """
        try:

            mapping = self.chan_map.get(orig_source_id)
            if mapping is None:
                with contextlib.suppress(Exception):
                    self._load_mappings()
                    mapping = self.chan_map.get(orig_source_id)

            pinned_name_raw = (mapping or {}).get("clone_channel_name") or ""
            pinned_name = pinned_name_raw.strip()
            has_pin = bool(pinned_name)

            # Always keep DB's original_channel_name in sync with upstream, preserving pin
            if mapping is not None:
                try:
                    self.db.upsert_channel_mapping(
                        orig_source_id,
                        upstream_name,
                        mapping.get("cloned_channel_id"),
                        mapping.get("channel_webhook_url"),
                        mapping.get("original_parent_category_id"),
                        mapping.get("cloned_parent_category_id"),
                        int(getattr(ch.type, "value", 0)),
                        clone_name=pinned_name if has_pin else None,
                    )

                    mapping["original_channel_name"] = upstream_name
                    if has_pin:
                        mapping["clone_channel_name"] = pinned_name
                except Exception:
                    logger.debug("[rename] mapping upsert failed", exc_info=True)

            if has_pin:

                if ch.name != pinned_name:
                    old = ch.name
                    await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                    await ch.edit(name=pinned_name)
                    logger.info(
                        "[📌] Enforced pinned name on #%d: %r → %r",
                        ch.id,
                        old,
                        pinned_name,
                    )
                    return True, "pinned_enforced"
                return False, "skipped_already_ok"

            if ch.name != upstream_name:
                old = ch.name
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await ch.edit(name=upstream_name)
                logger.info(
                    "[✏️] Renamed channel #%d: %r → %r", ch.id, old, upstream_name
                )
                return True, "match_upstream"

            return False, "skipped_already_ok"

        except Exception:
            logger.debug(
                "[rename] _maybe_apply_pinned_or_upstream_name error", exc_info=True
            )
            return False, "skipped_error"

    async def _handle_removed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        """
        Handles the removal of categories that are no longer present in the sitemap.
        """
        valid_ids = {c["id"] for c in sitemap.get("categories", [])}
        removed = 0

        for orig_id, row in list(self.cat_map.items()):
            if orig_id not in valid_ids:

                ch = guild.get_channel(row["cloned_category_id"])
                if ch and self.config.DELETE_CHANNELS:
                    await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                    await ch.delete()
                    logger.info("[🗑️] Deleted category %s", ch.name)

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

                if ch.id in protected:
                    logger.info(
                        "[🛡️] Skipping deletion of protected channel #%s (%d) (community/system assignment).",
                        ch.name,
                        ch.id,
                    )
                else:

                    await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                    try:
                        await ch.delete()
                        logger.info("[🗑️] Deleted channel #%s (%d)", ch.name, ch.id)
                    except discord.HTTPException as e:

                        if getattr(
                            e, "code", None
                        ) == 50074 or "required for community" in str(e):
                            logger.info(
                                "[🛡️] API blocked deletion of #%s (%d): protected. Will skip and drop mapping.",
                                getattr(ch, "name", "?"),
                                ch.id,
                            )
                        else:
                            logger.warning(
                                "[⚠️] Failed to delete channel #%d: %s", ch.id, e
                            )

            elif not ch:
                logger.info(
                    "[🗑️] Cloned channel #%d not found; removing mapping", clone_id
                )

            # Always drop the mapping, even if we couldn't delete the channel.
            self.db.delete_channel_mapping(orig_id)
            self.chan_map.pop(orig_id, None)
            removed += 1

        return removed

    async def _maybe_rename_category(
        self,
        cat: discord.CategoryChannel,
        upstream_name: str,
        orig_cat_id: int,
    ) -> tuple[bool, str]:
        """
        Apply a pinned name (if any) for this category; otherwise match upstream.
        """
        try:

            mapping = self.cat_map.get(orig_cat_id)
            if mapping is None:
                with contextlib.suppress(Exception):
                    self._load_mappings()
                    mapping = self.cat_map.get(orig_cat_id)

            pinned_raw = (mapping or {}).get("cloned_category_name") or ""
            pinned_name = pinned_raw.strip()
            has_pin = bool(pinned_name)

            if mapping is not None:
                try:
                    self.db.upsert_category_mapping(
                        orig_cat_id,
                        upstream_name,
                        int(mapping.get("cloned_category_id") or cat.id),
                        clone_name=pinned_name if has_pin else None,
                    )
                    mapping["original_category_name"] = upstream_name
                    if has_pin:
                        mapping["cloned_category_name"] = pinned_name
                except Exception:
                    logger.debug(
                        "[rename] category mapping upsert failed", exc_info=True
                    )

            desired = pinned_name if has_pin else upstream_name
            if cat.name == desired:
                return (False, "skipped_already_ok")

            old = cat.name
            await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
            await cat.edit(name=desired)

            if has_pin:
                logger.info(
                    "[📌] Enforced pinned category name on %d: %r → %r",
                    cat.id,
                    old,
                    desired,
                )
                return (True, "pinned_enforced")
            else:
                logger.info("[✏️] Renamed category %d: %r → %r", cat.id, old, desired)
                return (True, "match_upstream")

        except Exception:
            logger.debug("[rename] _maybe_rename_category error", exc_info=True)
            return (False, "skipped_error")

    async def _handle_renamed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        """
        Handles the renaming of cloned categories.
        """
        renamed = 0
        desired = {c["id"]: c["name"] for c in sitemap.get("categories", [])}

        for orig_id, row in list(self.cat_map.items()):
            upstream_name = desired.get(orig_id)
            if not upstream_name:
                continue

            clone_id = row.get("cloned_category_id")
            if not clone_id:
                continue

            clone_cat = guild.get_channel(int(clone_id))
            if not clone_cat:
                continue

            did, _reason = await self._maybe_rename_category(
                clone_cat, upstream_name, orig_id
            )
            if did:
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

        await self.ratelimit.acquire(ActionType.CREATE_CHANNEL)
        cat = await guild.create_category(name)
        logger.info(
            "[➕] Created category %r (orig ID %d) → clone ID %d",
            name,
            original_id,
            cat.id,
        )

        self.db.upsert_category_mapping(
            original_id,
            name,
            cat.id,
        )

        self.cat_map[original_id] = {
            "original_category_id": original_id,
            "cloned_category_id": cat.id,
            "original_category_name": name,
        }
        return cat, True

    async def _create_webhook_safely(self, ch, name, avatar_bytes):
        if self._shutting_down:
            return
        async with self._new_webhook_gate:
            rem = self.ratelimit.remaining(ActionType.WEBHOOK_CREATE)
            await self.ratelimit.acquire(ActionType.WEBHOOK_CREATE)
            try:
                await self._get_default_avatar_bytes()
            except Exception:
                pass
            webhook = await ch.create_webhook(name=name, avatar=avatar_bytes)
            logger.info("[➕] Created a webhook in channel %s", ch.name)

            if hasattr(self, "_wh_meta"):
                self._wh_meta.clear()

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

                self.db.delete_channel_mapping(original_id)
                break

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

            upstream_parent = item["parent_id"]
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

            actual_parent = ch.category
            actual_parent_id = actual_parent.id if actual_parent else None

            if actual_parent_id == desired_parent_clone_id:
                continue

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

                        self._default_avatar_sha1 = hashlib.sha1(
                            self._default_avatar_bytes
                        ).hexdigest()
                    else:
                        logger.warning(
                            "[⚠️] Avatar download failed %s (HTTP %s)", url, resp.status
                        )
            except Exception as e:
                logger.warning("[⚠️] Error downloading avatar %s: %s", url, e)
        return self._default_avatar_bytes

    async def _get_webhook_meta(
        self, original_id: int, webhook_url: str, *, force: bool = False
    ) -> dict:
        """Return cached info about whether the channel webhook was customized by the user."""
        now = time.time()
        meta = self._wh_meta.get(original_id)
        if meta and not force and (now - meta.get("checked_at", 0) < self._wh_meta_ttl):
            return meta

        try:
            webhook_id = int(webhook_url.rstrip("/").split("/")[-2])
        except Exception:

            meta = {
                "custom": False,
                "name": None,
                "avatar_sha1": None,
                "checked_at": now,
            }
            self._wh_meta[original_id] = meta
            return meta

        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

        try:
            wh = await self.bot.fetch_webhook(webhook_id)
        except (NotFound, HTTPException):

            meta = {
                "custom": False,
                "name": None,
                "avatar_sha1": None,
                "checked_at": now,
            }
            self._wh_meta[original_id] = meta
            return meta

        avatar_sha = None
        custom_avatar = False
        try:
            if wh.avatar and self._default_avatar_sha1:
                b = await wh.avatar.read()
                avatar_sha = hashlib.sha1(b).hexdigest()
                custom_avatar = avatar_sha != self._default_avatar_sha1
        except Exception:
            custom_avatar = False

        canonical = self._canonical_webhook_name()
        custom_name = (wh.name or "").strip().lower() != canonical.strip().lower()

        custom = custom_name or custom_avatar

        meta = {
            "custom": custom,
            "name": wh.name,
            "avatar_sha1": avatar_sha,
            "checked_at": now,
        }
        self._wh_meta[original_id] = meta
        return meta

    async def _recreate_webhook(self, original_id: int) -> Optional[str]:
        """
        Recreates a webhook for a given channel if it is missing or invalid.
        This method attempts to retrieve the webhook URL for a channel from the internal
        channel mapping. If the webhook is missing or invalid, it creates a new webhook
        for the corresponding cloned channel and updates the database and internal mapping.
        """
        if self._shutting_down:
            return

        row = self.chan_map.get(original_id)
        if not row:
            logger.error(
                "[⛔] No DB row for #%s; cannot recreate webhook.", original_id
            )
            return None

        lock = self._webhook_locks.setdefault(original_id, asyncio.Lock())

        async with lock:

            fresh = self.chan_map.get(original_id)
            if not fresh:
                logger.error("[⛔] Mapping disappeared for #%s!", original_id)
                return None

            url = fresh["channel_webhook_url"]
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
                self._wh_meta.pop(original_id, None)
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

        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.error("[⛔] Clone guild %s not available", self.clone_guild_id)
            return

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
        if not isinstance(chan_map, dict):
            chan_map = dict(chan_map)

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

        payload = self._build_webhook_payload(data)

        webhook_url = chan_map.get(
            "channel_webhook_url"
        ) or await self._ensure_primary_webhook_url(parent_id)

        meta = {}
        if webhook_url:
            meta = await self._get_webhook_meta(parent_id, webhook_url)
        if meta.get("custom"):

            payload.pop("username", None)
            payload.pop("avatar_url", None)
        if not payload or (not payload.get("content") and not payload.get("embeds")):
            logger.info("[⚠️] Skipping empty payload for '%s'", data["thread_name"])
            return

        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        thread_webhook = Webhook.from_url(webhook_url, session=self.session)

        lock = self._thread_locks.setdefault(orig_tid, asyncio.Lock())
        created = False

        try:
            async with lock:

                thr_map = next(
                    (
                        r
                        for r in self.db.get_all_threads()
                        if r["original_thread_id"] == orig_tid
                    ),
                    None,
                )

                clone_thread = None

                def drop_mapping():
                    self.db.delete_forum_thread_mapping(orig_tid)
                    return None

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

                        resp_msg = await thread_webhook.send(
                            content=payload.get("content"),
                            embeds=payload.get("embeds"),
                            username=payload.get("username"),
                            avatar_url=payload.get("avatar_url"),
                            thread_name=data["thread_name"],
                            wait=True,
                        )

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

                        new_thread = await cloned_parent.create_thread(
                            name=data["thread_name"],
                            type=ChannelType.public_thread,
                            auto_archive_duration=60,
                        )
                        new_id = new_thread.id
                        clone_thread = new_thread

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

                    self.db.upsert_forum_thread_mapping(
                        orig_thread_id=orig_tid,
                        orig_thread_name=data["thread_name"],
                        clone_thread_id=new_id,
                        forum_orig_id=parent_id,
                        forum_clone_id=cloned_id,
                    )

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
            row["forum_original_id"],
            row["forum_cloned_id"],
        )

    async def _enforce_thread_limit(self, guild: discord.Guild):
        """
        Enforces the thread limit for the clone guild by archiving the oldest active threads
        if the number of active threads exceeds the configured maximum.
        """

        valid_clone_ids = {r["cloned_thread_id"] for r in self.db.get_all_threads()}

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

        if len(active) <= self.max_threads:
            return

        active.sort(
            key=lambda t: t.created_at or datetime.min.replace(tzinfo=timezone.utc)
        )
        num_to_archive = len(active) - self.max_threads
        to_archive = active[:num_to_archive]

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

    def _sanitize_inline(self, s: str | None) -> str | None:
        if not s:
            return s
        s = self._replace_emoji_ids(s)
        s = self._remap_channel_mentions(s)
        s = self._remap_role_mentions(s)
        return s

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

            if row and row.get("cloned_channel_id"):
                return f"<#{row['cloned_channel_id']}>"
            return match.group(0)

        return self._m_ch.sub(repl, content)

    def _remap_role_mentions(self, content: str) -> str:
        """Map host role mentions to cloned role mentions using role_mappings."""
        if not content:
            return content

        def repl(match: re.Match) -> str:
            orig_role_id = int(match.group("id"))
            row = self.db.get_role_mapping(orig_role_id)

            if row and "cloned_role_id" in row.keys():
                cloned_id = row["cloned_role_id"]
                if cloned_id:
                    return f"<@&{cloned_id}>"

            return match.group(0)

        return self._m_role.sub(repl, content)

    def _build_webhook_payload(self, msg: Dict) -> dict:
        """
        Constructs a webhook payload from a given message dictionary.
        Processes text, attachments, embeds, channel mentions, and stickers (as image embeds).
        Also replaces custom emoji IDs in text and embed fields.
        """

        text = self._sanitize_inline(msg.get("content", "") or "")

        for att in msg.get("attachments", []) or []:
            url = att.get("url")
            if url and url not in text:
                text += f"\n{url}"

        raw_embeds = msg.get("embeds", []) or []
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
                    logger.warning("[⚠️] Could not convert embed dict to Embed: %s", e)
            elif isinstance(raw, Embed):
                embeds.append(raw)

        for e in embeds:

            if getattr(e, "description", None):
                e.description = self._sanitize_inline(e.description)
            if getattr(e, "title", None):
                e.title = self._sanitize_inline(e.title)

            if getattr(e, "footer", None) and getattr(e.footer, "text", None):
                e.footer.text = self._sanitize_inline(e.footer.text)

            if getattr(e, "author", None) and getattr(e.author, "name", None):
                e.author.name = self._sanitize_inline(e.author.name)

            for f in getattr(e, "fields", []) or []:
                if getattr(f, "name", None):
                    f.name = self._sanitize_inline(f.name)
                if getattr(f, "value", None):
                    f.value = self._sanitize_inline(f.value)

        base = {
            "username": msg.get("author") or "Unknown",
            "avatar_url": msg.get("avatar_url"),
        }

        if len(text) > 2000:
            long_embed = Embed(description=text[:4096])
            return {**base, "content": None, "embeds": [long_embed] + embeds}

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

        Policy:
        - If the PRIMARY webhook's name is unchanged (canonical), always include per-message user metadata.
        - If the PRIMARY webhook's name is customized:
            * When sending via the PRIMARY webhook → use the webhook's stored identity.
            * When sending via a TEMP webhook → override per-message username/avatar_url to match PRIMARY.
        """
        if self._shutting_down:
            return

        tag = self._log_tag(msg)
        source_id = msg["channel_id"]
        is_backfill = bool(msg.get("__backfill__"))

        mapping = self.chan_map.get(source_id)
        if mapping is None:
            self._load_mappings()
            mapping = self.chan_map.get(source_id)

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
                return

        payload = self._build_webhook_payload(msg)
        if payload is None:
            logger.debug(
                "No webhook payload built for #%s; skipping", msg.get("channel_name")
            )
            return

        if (
            not payload.get("content")
            and not payload.get("embeds")
            and not (msg.get("stickers") or [])
        ):
            logger.info(
                "[⚠️]%s Skipping empty message in #%s (attachments=%d stickers=%d)",
                tag,
                msg.get("channel_name"),
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
                    msg.get("channel_name"),
                    e,
                    payload["content"],
                )
                return

        if not hasattr(self, "_webhooks"):
            self._webhooks = {}

        async def _primary_name_changed(purl: str) -> bool:
            """True iff PRIMARY webhook name differs from canonical default."""
            try:
                wid = int(purl.rstrip("/").split("/")[-2])
                wh = await self.bot.fetch_webhook(wid)
                canonical = self.backfill._canonical_temp_name()
                name = (wh.name or "").strip()
                return bool(name and name != canonical)
            except Exception:
                return False

        async def _get_primary_identity_for_source(
            src_id: int,
        ) -> tuple[str | None, str | None, str | None]:
            """
            Returns (primary_url, name, avatar_url).
            avatar_url is a CDN URL if available.
            """
            mapping = self.chan_map.get(src_id) or {}
            purl = mapping.get("channel_webhook_url") or mapping.get("webhook_url")
            if not purl:
                return None, None, None
            try:
                wid = int(purl.rstrip("/").split("/")[-2])
                wh = await self.bot.fetch_webhook(wid)
                name = (wh.name or "").strip() or None
                av_url = None
                try:
                    av_asset = getattr(wh, "avatar", None)
                    if av_asset:

                        av_url = str(getattr(av_asset, "url", None)) or None
                except Exception:
                    av_url = None
                return purl, name, av_url
            except Exception:
                return purl, None, None

        async def _primary_name_changed_for_source(src_id: int) -> bool:
            mapping = self.chan_map.get(src_id) or {}
            purl = mapping.get("channel_webhook_url") or mapping.get("webhook_url")
            if not purl:
                return False
            return await _primary_name_changed(purl)

        async def _do_send(
            url_to_use: str,
            rl_key: str,
            *,
            use_webhook_identity: bool,
            override_identity: dict | None = None,
        ):
            """
            - use_webhook_identity=True → do not pass username/avatar_url (use stored webhook identity).
            - override_identity={"username": ..., "avatar_url": ...} → force those values on send.
            (Takes precedence over use_webhook_identity.)
            """
            if self._shutting_down:
                return
            from aiohttp import ClientError
            import aiohttp, asyncio

            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()

            webhook = self._webhooks.get(url_to_use)
            if webhook is None or webhook.session is None or webhook.session.closed:
                webhook = Webhook.from_url(url_to_use, session=self.session)
                self._webhooks[url_to_use] = webhook

            await self.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=rl_key)
            if self._shutting_down:
                return

            if override_identity is not None:
                kw_username = override_identity.get("username")
                kw_avatar = override_identity.get("avatar_url")
            else:
                kw_username = None if use_webhook_identity else payload.get("username")
                kw_avatar = None if use_webhook_identity else payload.get("avatar_url")

            logger.debug(
                "[send] use_webhook_identity=%s override=%s | src=%s | ch=%s | username=%r avatar_url=%r",
                use_webhook_identity,
                bool(override_identity),
                source_id,
                msg.get("channel_name"),
                kw_username,
                kw_avatar,
            )

            try:
                await webhook.send(
                    content=payload.get("content"),
                    embeds=payload.get("embeds"),
                    username=kw_username,
                    avatar_url=kw_avatar,
                    wait=True,
                )
                if is_backfill:
                    self.backfill.note_sent(source_id)
                    delivered, total = self.backfill.get_progress(source_id)
                    suffix = (
                        f" [{max(total - delivered, 0)} left]"
                        if total is not None
                        else f" [{delivered} sent]"
                    )
                    logger.info(
                        "[💬] [msg-sync] Forwarded message to #%s from %s (%s)%s",
                        msg.get("channel_name"),
                        msg.get("author"),
                        msg.get("author_id"),
                        suffix,
                    )
                    self.ratelimit.relax(ActionType.WEBHOOK_MESSAGE, key=rl_key)
                else:
                    logger.info(
                        "[💬]%s Forwarded message to #%s from %s (%s)",
                        tag,
                        msg.get("channel_name"),
                        msg.get("author"),
                        msg.get("author_id"),
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
                        msg.get("channel_name"),
                        delay,
                    )
                    await asyncio.sleep(delay)

                    await _do_send(
                        url_to_use,
                        rl_key,
                        use_webhook_identity=use_webhook_identity,
                        override_identity=override_identity,
                    )
                    return
                elif e.status == 404:
                    logger.debug(
                        "Webhook %s returned 404; attempting recreate...", url_to_use
                    )
                    new_url = await self._recreate_webhook(source_id)
                    if not new_url:
                        logger.warning(
                            "[⌛] No mapping for channel %s; msg from %s is queued and will be sent after sync",
                            msg.get("channel_name"),
                            msg.get("author"),
                        )
                        msg["__buffered__"] = True
                        self._pending_msgs.setdefault(source_id, []).append(msg)
                        return
                    await _do_send(
                        new_url,
                        rl_key,
                        use_webhook_identity=use_webhook_identity,
                        override_identity=override_identity,
                    )
                    return
                else:
                    logger.error(
                        "[⛔] Failed to send to #%s (status %s): %s",
                        msg.get("channel_name"),
                        e.status,
                        e.text,
                    )
            except (ClientError, asyncio.TimeoutError) as e:
                logger.warning(
                    "[🌐]%s Network error sending to #%s: %s — queued for retry",
                    tag,
                    msg.get("channel_name"),
                    e,
                )
                msg["__buffered__"] = True
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return

        forced_url = msg.get("__force_webhook_url__")
        if forced_url:

            primary_url, primary_name, primary_avatar_url = (
                await _get_primary_identity_for_source(source_id)
            )
            primary_customized = await _primary_name_changed_for_source(source_id)

            is_primary = bool(primary_url and forced_url == primary_url)
            use_webhook_identity = bool(primary_customized and is_primary)
            override = None
            if primary_customized and not is_primary:
                override = {"username": primary_name, "avatar_url": primary_avatar_url}
            rl_key = f"bf-forced:{msg.get('channel_id')}"
            await _do_send(
                forced_url,
                rl_key,
                use_webhook_identity=use_webhook_identity,
                override_identity=override,
            )
            return

        if self.backfill.is_backfilling(source_id) and not is_backfill:
            msg["__buffered__"] = True
            self._pending_msgs.setdefault(source_id, []).append(msg)
            logger.debug(
                "[⏳] Buffered live message during backfill for #%s", source_id
            )
            return

        mapping = self.chan_map.get(source_id)
        if mapping is None:
            self._load_mappings()
            mapping = self.chan_map.get(source_id)
        if mapping is None:
            async with self._warn_lock:
                if source_id not in self._unmapped_warned:
                    logger.info(
                        "[⌛] No mapping yet for channel %s (%s); msg from %s is queued and will be sent after sync",
                        msg.get("channel_name"),
                        msg.get("channel_id"),
                        msg.get("author"),
                    )
                    self._unmapped_warned.add(source_id)
            msg["__buffered__"] = True
            self._pending_msgs.setdefault(source_id, []).append(msg)
            return

        url = mapping.get("channel_webhook_url") or mapping.get("webhook_url")
        clone_id = mapping.get("cloned_channel_id") or mapping.get("clone_channel_id")

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
                        msg.get("channel_name"),
                        msg.get("author"),
                        msg.get("author_id"),
                        suffix,
                    )
                return

        if mapping and not url:
            if self._sync_lock.locked():
                logger.info(
                    "[⌛] Sync in progress; message in #%s from %s is queued and will be sent after sync",
                    msg.get("channel_name"),
                    msg.get("author"),
                )
                msg["__buffered__"] = True
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return
            logger.warning(
                "[⚠️] Mapped channel %s has no webhook; attempting to recreate",
                msg.get("channel_name"),
            )
            url = await self._recreate_webhook(source_id)
            if not url:
                logger.info(
                    "[⌛] Could not recreate webhook for #%s; queued message from %s",
                    msg.get("channel_name"),
                    msg.get("author"),
                )
                msg["__buffered__"] = True
                self._pending_msgs.setdefault(source_id, []).append(msg)
                return

        if is_backfill and clone_id:
            await self.backfill.ensure_temps_ready(int(clone_id))

            primary_url, primary_name, primary_avatar_url = (
                await _get_primary_identity_for_source(source_id)
            )
            primary_customized = await _primary_name_changed_for_source(source_id)

            sem = self.backfill.semaphores.setdefault(
                int(clone_id), asyncio.Semaphore(1)
            )
            async with sem:
                url_to_use, _ = await self.backfill.pick_url_for_send(
                    int(clone_id), url, create_missing=False
                )
                rl_key = f"channel:{clone_id}"

                if primary_customized:
                    is_primary = bool(primary_url and url_to_use == primary_url)
                    if is_primary:
                        # Primary: use the webhook's stored identity
                        await _do_send(
                            url_to_use,
                            rl_key,
                            use_webhook_identity=True,
                            override_identity=None,
                        )
                    else:

                        override = {
                            "username": primary_name,
                            "avatar_url": primary_avatar_url,
                        }
                        await _do_send(
                            url_to_use,
                            rl_key,
                            use_webhook_identity=False,
                            override_identity=override,
                        )
                else:

                    await _do_send(
                        url_to_use,
                        rl_key,
                        use_webhook_identity=False,
                        override_identity=None,
                    )
            return

        primary_customized = await _primary_name_changed_for_source(source_id)
        url_to_use = url
        rl_key = url_to_use

        await _do_send(
            url_to_use,
            rl_key,
            use_webhook_identity=primary_customized,
            override_identity=None,
        )

    async def _handle_backfill_message(self, data: dict) -> None:
        if self._shutting_down:
            return
        try:
            original_id = int(data["channel_id"])
        except Exception:
            logger.warning("[bf] bad channel_id in payload: %r", data.get("channel_id"))
            return

        row = None
        if hasattr(self.db, "get_channel_mapping_by_original_id"):
            row = self.db.get_channel_mapping_by_original_id(original_id)
        if not row and hasattr(self.db, "get_channel_mapping_by_clone_id"):

            row = self.db.get_channel_mapping_by_clone_id(original_id)
            if row:
                try:
                    original_id = int(row["original_channel_id"])
                except Exception:
                    pass

        if not row:
            logger.warning("[bf] no mapping for channel=%s; cannot rotate", original_id)
            await self.forward_message(data)
            return

        try:
            row = dict(row)
        except Exception:
            logger.error(
                "[bf] mapping row not dict-like: type=%r row=%r", type(row), row
            )
            await self.forward_message(data)
            return

        clone_id = None
        for k in ("cloned_channel_id", "clone_channel_id"):
            v = row.get(k)
            if v is not None:
                try:
                    clone_id = int(v)
                    break
                except Exception:
                    pass

        primary_url = None
        for k in ("channel_webhook_url", "webhook_url", "webhook"):
            v = row.get(k)
            if v:
                primary_url = v
                break

        if not primary_url:
            logger.warning(
                "[bf] mapping found but no webhook URL | original=%s clone=%s row=%s",
                original_id,
                clone_id,
                row,
            )
            await self.forward_message(data)
            return

        st = self.backfill._progress.get(int(original_id))
        if not st:
            self.backfill.register_sink(
                original_id, user_id=None, clone_channel_id=clone_id, msg=None
            )
            logger.debug(
                "[bf] sink registered | original=%s clone=%s", original_id, clone_id
            )
        else:
            if clone_id and st.get("clone_channel_id") != clone_id:
                st["clone_channel_id"] = clone_id
            if clone_id:
                self.backfill._by_clone[clone_id] = int(original_id)

        try:
            if clone_id:
                await self.backfill.ensure_temps_ready(clone_id)
        except Exception as e:
            logger.debug(
                "[bf] ensure_temps_ready failed | clone=%s err=%s", clone_id, e
            )

        try:
            url, used_pool = await self.backfill.pick_url_for_send(
                clone_channel_id=clone_id or 0,
                primary_url=primary_url,
                create_missing=True,
            )
        except Exception as e:
            logger.warning("[bf] rotation failed, using primary | err=%s", e)
            url = primary_url

        forced = dict(data)
        forced["__force_webhook_url__"] = url
        await self.forward_message(forced)

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
        if getattr(self, "_send_tasks", None):

            for t in list(self._send_tasks):
                t.cancel()
            await asyncio.gather(*self._send_tasks, return_exceptions=True)
            self._send_tasks.clear()
        with contextlib.suppress(Exception):
            self.bus.begin_shutdown()
        if getattr(self, "verify", None):
            asyncio.create_task(self.verify.stop())
        self.bus.begin_shutdown()
        with contextlib.suppress(Exception, asyncio.TimeoutError):
            await asyncio.wait_for(
                self.bus.status(running=False, status="Stopped"), 0.4
            )

        for orig in list(getattr(self, "_active_backfills", set())):
            try:
                await self.bus.publish(
                    "client",
                    {
                        "type": "backfill_cancelled",
                        "data": {"channel_id": str(orig), "reason": "server_shutdown"},
                    },
                )
            except Exception:
                pass
        self._active_backfills.clear()

        bf = getattr(self, "backfill", None)
        if bf and hasattr(bf, "cancel_all_active"):

            await bf.cancel_all_active()

        try:
            ws = getattr(self, "ws_manager", None) or getattr(self, "ws", None)
            if ws and hasattr(ws, "stop"):
                await ws.stop()
        except Exception:
            logger.debug("[shutdown] ws stop failed", exc_info=True)
        finally:
            logging.info("Server shutdown complete.")

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
        logger.info("[✨] Starting Copycord Server %s", CURRENT_VERSION)
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))

        try:
            loop.run_until_complete(self.bot.start(self.config.SERVER_TOKEN))
        finally:
            pending = asyncio.all_tasks(loop=loop)
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))


def _autostart_enabled() -> bool:
    return os.getenv("COPYCORD_AUTOSTART", "true").lower() in ("1", "true", "yes", "on")


if __name__ == "__main__":
    if _autostart_enabled():
        ServerReceiver().run()
    else:
        while True:
            time.sleep(3600)
