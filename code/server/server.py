# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import contextlib
import signal
import asyncio
import logging
import random
from typing import List, Optional, Tuple, Dict, Union, Coroutine, Any
import aiohttp
import discord
import json
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
from common.common_helpers import resolve_mapping_settings
from common.websockets import WebsocketManager, AdminBus
from common.db import DBManager
from server.rate_limiter import RateLimitManager, ActionType
from server.discord_hooks import install_discord_rl_probe
from server.emojis import EmojiManager
from server.stickers import StickerManager
from server.roles import RoleManager
from server.backfill import BackfillManager, BackfillTracker
from server.helpers import (
    OnJoinService,
    VerifyController,
    WebhookDMExporter,
    OnCloneJoin,
)
from server.permission_sync import ChannelPermissionSync
from server.guild_resolver import GuildResolver
from server import logctx

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
        self.bot.ws_manager = self.ws
        self.db = DBManager(self.config.DB_PATH)
        self._clone_guild_ids = set(self.db.get_all_clone_guild_ids())
        self.guild_resolver = GuildResolver(self.db, self.config)
        self.session: aiohttp.ClientSession = None
        self._processor_started = False
        self._sitemap_task_counter = 0
        self._sync_lock = asyncio.Lock()
        self._thread_locks: dict[int, asyncio.Lock] = {}
        self.max_threads = 950
        self._m_ch = re.compile(r"<#(\d+)>")
        self._m_msg_link = re.compile(
            r"(https?://(?:ptb\.|canary\.)?discord(?:app)?\.com/channels/)(\d+|@me)/(\d+)/(\d+)",
            re.IGNORECASE,
        )
        self.bot.event(self.on_ready)
        self.bot.event(self.on_webhooks_update)
        self.bot.event(self.on_guild_channel_delete)
        self.bot.event(self.on_member_join)
        self._default_avatar_bytes: Optional[bytes] = None
        self._ws_task: asyncio.Task | None = None
        self._sitemap_queues: dict[int, asyncio.Queue] = {}
        self._sitemap_workers: dict[int, asyncio.Task] = {}
        self._guild_sync_locks: dict[int, asyncio.Lock] = {}
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
        self._bf_event_buffer: dict[int, list[tuple[str, dict]]] = {}
        self._send_tasks: set[asyncio.Task] = set()
        self._wh_identity_state: dict[int, bool] = {}
        self._wh_meta: dict[int, dict] = {}
        self._wh_meta_ttl = 300
        self._default_avatar_sha1: str | None = None
        self._shutting_down = False
        self._inflight_events: dict[int, asyncio.Event] = {}
        self._latest_edit_payload: dict[int, dict] = {}
        self._pending_deletes: set[int] = set()
        self._bf_throttle: dict[int, dict] = {}
        self._task_for_channel: dict[int, str] = {}
        self._done_task_ids: set[str] = set()
        self._host_name_cache: dict[int, str] = {}
        self._task_display_id: dict[int, str] = {}
        self._bf_delay = 2.0
        orig_on_connect = self.bot.on_connect
        self.onclonejoin = OnCloneJoin(self.bot, self.db)
        self.bus = AdminBus(
            role="server", logger=logger, admin_ws_url=self.config.ADMIN_WS_URL
        )
        self.ratelimit = RateLimitManager()
        self.backfill = BackfillManager(self, ratelimit=self.ratelimit)
        self.backfills = BackfillTracker(
            bus=self.bus,
            on_done_cb=self.backfill.on_done,
            progress_provider=self.backfill.get_progress,
        )
        self.backfill.tracker = self.backfills
        self.emojis = EmojiManager(
            bot=self.bot,
            db=self.db,
            ratelimit=self.ratelimit,
            session=self.session,
            guild_resolver=self.guild_resolver,
        )
        self.stickers = StickerManager(
            bot=self.bot,
            db=self.db,
            ratelimit=self.ratelimit,
            session=self.session,
            guild_resolver=self.guild_resolver,
        )
        self.roles = RoleManager(
            bot=self.bot,
            db=self.db,
            ratelimit=self.ratelimit,
            guild_resolver=self.guild_resolver,
        )
        self.perms = ChannelPermissionSync(
            config=self.config,
            db=self.db,
            bot=self.bot,
            logger=logger,
            ratelimit=self.ratelimit,
            rate_limiter_action=ActionType.EDIT_CHANNEL,
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

    def _target_clone_gid_for_origin(self, host_guild_id: int | None) -> int | None:
        return self.guild_resolver.resolve_target_clone(host_guild_id=host_guild_id)

    def _clone_gid_for_ctx(
        self, *, host_guild_id: int | None = None, mapping_row: dict | None = None
    ) -> int | None:
        """
        Prefer the mapping row's cloned_guild_id; otherwise resolve from the origin guild.
        Falls back to config.CLONE_GUILD_ID for legacy single-guild installs.
        """
        try:
            if mapping_row and mapping_row.get("cloned_guild_id"):
                return int(mapping_row["cloned_guild_id"])
        except Exception:
            pass
        if host_guild_id:
            try:
                gid = self._target_clone_gid_for_origin(int(host_guild_id))
                if gid:
                    return int(gid)
            except Exception:
                pass

    def _prep_sitemap_task_meta(self, sitemap: dict) -> tuple[int, str, str]:
        """
        Extracts:
        - host/original guild id (int)
        - host/original guild name (str fallback "host:<id>")
        - human-friendly 5-char task code like 'az4dc'
        """
        ginfo = sitemap.get("guild") or {}
        raw_id = ginfo.get("id") or 0
        try:
            host_gid = int(raw_id)
        except Exception:
            host_gid = 0

        host_name = (ginfo.get("name") or "").strip() or f"host:{host_gid}"

        display_id = "".join(
            random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(5)
        )

        if host_gid:
            self._host_name_cache[host_gid] = host_name

        return host_gid, host_name, display_id

    def _get_or_create_sitemap_queue(self, host_guild_id: int) -> asyncio.Queue:
        q = self._sitemap_queues.get(host_guild_id)
        if q is None:
            q = asyncio.Queue()
            self._sitemap_queues[host_guild_id] = q
            self._sitemap_workers[host_guild_id] = asyncio.create_task(
                self._process_sitemap_queue_for_guild(host_guild_id, q)
            )
        return q

    async def _process_sitemap_queue_for_guild(self, host_gid: int, q: asyncio.Queue):
        """
        Worker loop for ONE origin/host guild.
        We coalesce bursts, then run sync_structure once using the newest sitemap.
        """
        while True:

            task_id, display_id, sitemap = await q.get()

            dropped = 0
            while True:
                try:
                    task_id, display_id, sitemap = q.get_nowait()
                    q.task_done()
                    dropped += 1
                except asyncio.QueueEmpty:
                    break

            guild_name = self._host_name_cache.get(host_gid, f"host:{host_gid}")

            if dropped:
                logger.debug(
                    "[%s] Dropped %d outdated sitemap(s); processing %s",
                    guild_name,
                    dropped,
                    display_id,
                )

            try:
                summary = await self.sync_structure(task_id, sitemap)
                logger.info(
                    "[✅] [%s] [%s] Sync task primary pass complete: %s",
                    guild_name,
                    display_id,
                    summary,
                )
            except Exception:
                logger.exception(
                    "Error processing sitemap %s for host guild %s",
                    display_id,
                    host_gid,
                )
            finally:
                q.task_done()

    def _get_sync_lock(self, clone_guild_id: int) -> asyncio.Lock:
        lock = self._guild_sync_locks.get(int(clone_guild_id))
        if lock is None:
            lock = asyncio.Lock()
            self._guild_sync_locks[int(clone_guild_id)] = lock
        return lock

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
                guild_id=self._pick_verify_guild_id(),
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
        self.webhook_exporter = WebhookDMExporter(self.session, logger)
        mapped = set(self.db.get_all_clone_guild_ids())
        present = (
            {g.id for g in self.bot.guilds} & mapped
            if mapped
            else {g.id for g in self.bot.guilds}
        )
        if mapped and not present:
            logger.error(
                "[⛔] Bot is not a member of any mapped clone guilds: %s",
                sorted(mapped),
            )
            await self.bot.close()
            sys.exit(1)
        self._load_mappings()
        self.emojis.set_session(self.session)
        self.stickers.set_session(self.session)
        for clone_gid in present:
            await self.stickers.refresh_cache(clone_gid)

        msg = f"Logged in as {self.bot.user.name}"

        await self.bus.status(running=True, status=msg, discord={"ready": True})

        logger.info("[🤖] %s", msg)

        asyncio.create_task(self.backfill.cleanup_non_primary_webhooks())

        if not self._processor_started:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))
            self._processor_started = True
            self._prune_old_messages_loop()

    async def on_member_join(self, member: discord.Member):
        g = getattr(member, "guild", None)
        try:
            if not g:
                return

            if not self.db.is_clone_guild_id(int(g.id)):
                return
            logger.info("[👤] %s (%s) has joined the server!", member.name, member.id)
            await self.onclonejoin.handle_member_join(member)
        except Exception:
            logger.exception(
                "[👤] on_member_join: unhandled exception guild_id=%s member_id=%s",
                getattr(g, "id", "unknown"),
                getattr(member, "id", "unknown"),
            )

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
        if self._shutting_down:
            return
        try:
            if not self.db.is_clone_guild_id(channel.guild.id):
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
        """
        When a cloned channel/category is deleted in one clone guild,
        request a sitemap refresh ONLY for that mapping's host/original guild.
        """
        try:

            if not self.db.is_clone_guild_id(channel.guild.id):
                logger.info(
                    "[🛑] Ignoring delete of channel %s in non-clone guild %s",
                    channel.id,
                    channel.guild.id,
                )
                return
        except AttributeError:

            return

        lock = self._guild_sync_locks.get(int(channel.guild.id))
        if lock and lock.locked():
            logger.debug(
                "[🛑] g=%s sync in progress — ignoring sitemap request for deleted channel %s",
                channel.guild.id,
                channel.id,
            )
            return

        is_category = (
            isinstance(channel, discord.CategoryChannel)
            or getattr(channel, "type", None) == discord.ChannelType.category
        )

        if is_category:
            hit_src_cat_id = None
            hit_row = None

            for orig_cat_id, row in list(self.cat_map.items()):

                if int(row.get("cloned_category_id") or 0) == int(channel.id):
                    hit_src_cat_id = int(orig_cat_id)
                    hit_row = row
                    break

            if hit_src_cat_id is None or hit_row is None:
                return

            self.cat_map.pop(hit_src_cat_id, None)

            host_guild_id = hit_row.get("original_guild_id")
            if not host_guild_id:
                logger.info(
                    "[⚠️] No original_guild_id in cat_map row for deleted category "
                    "clone_g=%s chan=%s",
                    channel.guild.id,
                    channel.id,
                )
                return

            logger.warning(
                "[🧹] Cloned category deleted in clone_g=%s: clone_cat=%s name=%s "
                "(src_cat=%s host_g=%s). Requesting sitemap.",
                channel.guild.id,
                channel.id,
                getattr(channel, "name", "?"),
                hit_src_cat_id,
                host_guild_id,
            )

            logger.warning(
                "Cloned category deleted in clone_g=%s: clone_cat=%s name=%s "
                "(src_cat=%s host_g=%s). Requesting sitemap.",
                channel.guild.id,
                channel.id,
                getattr(channel, "name", "?"),
                hit_src_cat_id,
                host_guild_id,
            )

            # tell the client EXACTLY which host/original guild's sitemap to refresh
            await self.bot.ws_manager.send(
                {
                    "type": "sitemap_request",
                    "data": {
                        "guild_id": str(host_guild_id),
                    },
                }
            )
            return

        hit_src_id = None
        hit_row = None

        for src_id, row in list(self.chan_map.items()):
            if int(row.get("cloned_channel_id") or 0) == int(channel.id):
                hit_src_id = int(src_id)
                hit_row = row
                break

        if hit_src_id is None or hit_row is None:
            return

        try:
            self.backfill.invalidate_rotation(int(channel.id))
        except Exception:
            pass

        self.chan_map.pop(hit_src_id, None)

        host_guild_id = hit_row.get("original_guild_id")
        if not host_guild_id:
            logger.debug(
                "[⚠️] No original_guild_id in chan_map row for deleted channel "
                "clone_g=%s chan=%s",
                channel.guild.id,
                channel.id,
            )
            return

        logger.warning(
            "[🧹] Cloned channel deleted in clone_g=%s: clone_chan=%s name=%s "
            "(src_chan=%s host_g=%s). Requesting sitemap.",
            channel.guild.id,
            channel.id,
            getattr(channel, "name", "?"),
            hit_src_id,
            host_guild_id,
        )

        await self.bot.ws_manager.send(
            {
                "type": "sitemap_request",
                "data": {
                    "guild_id": str(host_guild_id),
                },
            }
        )

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

    def _bf_channel_key_for_event(self, typ: str, data: dict) -> int | None:
        """
        Return the 'origin channel id' we use to track backfills for this event.
        For thread events, we key by the parent channel; otherwise by channel_id.
        """
        try:
            if typ.startswith("thread_"):
                return int(data.get("thread_parent_id") or 0) or None
            return int(data.get("channel_id") or 0) or None
        except Exception:
            return None

    def _maybe_buffer_if_backfilling(self, typ: str, data: dict) -> bool:
        key = self._bf_channel_key_for_event(typ, data)
        if not key:
            return False

        is_bf = (key in self._active_backfills) or (
            hasattr(self, "backfill") and self.backfill.is_backfilling(key)
        )

        if is_bf:
            buf = self._bf_event_buffer.setdefault(key, [])

            if typ in ("message_edit", "thread_message_edit"):
                try:
                    mid = int((data or {}).get("message_id") or 0)
                except Exception:
                    mid = 0
                if mid:
                    buf[:] = [
                        (t, d)
                        for (t, d) in buf
                        if not (
                            t in ("message_edit", "thread_message_edit")
                            and int((d or {}).get("message_id") or 0) == mid
                        )
                    ]

            buf.append((typ, data))
            logger.debug("[bf] Buffered %s for #%s (backfill active)", typ, key)
            return True
        return False

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

            host_gid, host_name, display_id = self._prep_sitemap_task_meta(data)

            if not host_gid:
                logger.warning(
                    "[⛔] sitemap missing guild.id; dropping task %s", display_id
                )
                return

            self._task_display_id[task_id] = display_id

            q = self._get_or_create_sitemap_queue(host_gid)
            q.put_nowait((task_id, display_id, data))

            logger.info("[✉️][%s] [%s] Sync task received", host_name, display_id)
            return

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

        elif typ == "message_edit":
            if self._maybe_buffer_if_backfilling(typ, data):
                return
            self._track(self.handle_message_edit(data), name="edit-msg")

        elif typ == "thread_message_edit":
            if self._maybe_buffer_if_backfilling(typ, data):
                return
            self._track(self.handle_message_edit(data), name="edit-thread-msg")

        elif typ == "message_delete":
            if self._maybe_buffer_if_backfilling(typ, data):
                return
            self._track(self.handle_message_delete(data), name="del-msg")

        elif typ == "thread_message_delete":
            if self._maybe_buffer_if_backfilling(typ, data):
                return
            self._track(self.handle_message_delete(data), name="del-thread-msg")

        elif typ == "thread_message":
            if data.get("__backfill__"):
                try:
                    parent = int(data.get("thread_parent_id") or 0)
                except Exception:
                    parent = 0
                t = self._track(
                    self._handle_backfill_thread_message(data), name="bf-thread"
                )
                if parent:
                    self.backfill.attach_task(parent, t)
            else:
                self._track(self.handle_thread_message(data), name="thread-msg")

        elif typ == "thread_delete":
            parent_id = None
            try:
                tid = int((data or {}).get("thread_id") or 0)
            except Exception:
                tid = 0
            if tid:
                try:
                    row = next(
                        (
                            r
                            for r in self.db.get_all_threads()
                            if r["original_thread_id"] == tid
                        ),
                        None,
                    )
                    if row:
                        parent_id = (
                            int(row.get("forum_original_id") or 0)
                            or int(row.get("original_parent_id") or 0)
                            or None
                        )
                except Exception:
                    parent_id = None

            if parent_id and parent_id in self._active_backfills:
                self._bf_event_buffer.setdefault(parent_id, []).append((typ, data))
                logger.debug(
                    "[bf] Buffered thread_delete for parent #%s (backfill active)",
                    parent_id,
                )
                return

            asyncio.create_task(self.handle_thread_delete(data))

        elif typ == "thread_rename":
            try:
                parent_id = int((data or {}).get("parent_id") or 0)
            except Exception:
                parent_id = 0

            if parent_id and parent_id in self._active_backfills:
                self._bf_event_buffer.setdefault(parent_id, []).append((typ, data))
                logger.debug(
                    "[bf] Buffered thread_rename for parent #%s (backfill active)",
                    parent_id,
                )
                return

            asyncio.create_task(self.handle_thread_rename(data))

        elif typ == "announce":
            asyncio.create_task(self.handle_announce(data))

        elif typ == "backfill_started":
            data = msg.get("data") or {}
            cid_raw = data.get("channel_id")
            mapping_id = data.get("mapping_id")
            original_gid = data.get("original_guild_id")
            cloned_gid = data.get("cloned_guild_id")
            try:
                orig = int(cid_raw)
            except (TypeError, ValueError):
                logger.error("backfill_started missing/invalid channel_id: %r", cid_raw)
                return

            tid = msg.get("task_id") or (
                data.get("task_id") if isinstance(data, dict) else None
            )
            if tid:
                self._task_for_channel[orig] = str(tid)

            if orig in self._active_backfills:
                await self.bus.publish(
                    "client", {"type": "backfill_busy", "data": {"channel_id": orig}}
                )
                return

            is_resume = bool((msg.get("data") or {}).get("resume"))

            self._active_backfills.add(orig)

            await self.backfill.on_started(
                orig,
                meta={
                    "range": (msg.get("data") or {}).get("range"),
                    "resume": is_resume,
                    "clone_channel_id": (msg.get("data") or {}).get("clone_channel_id"),
                    "mapping_id": mapping_id,
                    "original_guild_id": original_gid,
                    "cloned_guild_id": cloned_gid,
                },
            )

            tid = None
            try:
                st = self.backfill._progress.get(orig) or {}
                tid = st.get("task_id")
                if not tid and hasattr(self.backfill, "tracker"):
                    with contextlib.suppress(Exception):
                        tid = await self.backfill.tracker.get_task_id(str(orig))
            except Exception:
                tid = None

            await self.bus.publish(
                "client",
                {
                    "type": "backfill_ack",
                    "task_id": tid,
                    "data": {
                        "channel_id": str(orig),
                        "task_id": tid,
                        "mapping_id": mapping_id,
                        "cloned_guild_id": str(cloned_gid),
                    },
                    "ok": True,
                },
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
            return

        elif typ == "backfill_stream_end":
            data = msg.get("data") or {}
            cid_raw = data.get("channel_id")
            try:
                orig = int(cid_raw)
            except (TypeError, ValueError):
                logger.error("backfill_done missing/invalid channel_id: %r", cid_raw)
                return

            tid = None
            if hasattr(self.backfill, "tracker"):
                with contextlib.suppress(Exception):
                    tid = await self.backfill.tracker.get_task_id(str(orig))

            await self.backfill.on_done(
                orig,
                wait_cleanup=True,
                expected_task_id=(str(tid) if tid else None),
            )

            self._active_backfills.discard(orig)

            try:
                await self._flush_channel_buffer(orig)
            except Exception:
                logger.exception(
                    "[bf] Failed flushing queued live messages for #%s", orig
                )

            pending = self._bf_event_buffer.pop(orig, [])
            if pending:
                logger.info(
                    "[bf] Draining %d buffered edit/delete events for #%s",
                    len(pending),
                    orig,
                )
            for ev_typ, ev_data in pending:
                if ev_typ in ("message_edit", "thread_message_edit"):
                    self._track(self.handle_message_edit(ev_data), name="bf-drain-edit")
                elif ev_typ in ("message_delete", "thread_message_delete"):
                    self._track(
                        self.handle_message_delete(ev_data), name="bf-drain-del"
                    )

            try:
                delivered, total_est = self.backfill.get_progress(orig)
            except Exception:
                delivered, total_est = (None, None)

            if getattr(self, "_shutting_down", False):
                return

            no_work = total_est is not None and int(total_est) == 0

            try:
                await self.bot.ws_manager.send(
                    {
                        "type": "backfill_done",
                        "data": {
                            "channel_id": str(orig),
                            "sent": delivered,
                            "total": total_est,
                            **({"no_work": True} if no_work else {}),
                        },
                    }
                )
            except Exception:
                logger.debug(
                    "[bf] failed WS notify backfill_done for #%s", orig, exc_info=True
                )
            return

        elif typ == "backfills_status_query":

            logger.debug("Backfill status query received")
            try:
                items = self.backfill.snapshot_in_progress()
            except Exception as e:
                logger.exception("Failed to snapshot backfills: %s", e)
                items = {}

            return {
                "type": "backfills_status",
                "data": {"items": items},
            }

        elif typ == "member_joined":
            asyncio.create_task(self.onjoin.handle_member_joined(data))

        elif typ == "export_dm_message":
            if (
                getattr(self, "shutting_down", False)
                or self.webhook_exporter.is_stopped
            ):
                return
            await self.webhook_exporter.handle_ws_export_dm_message(data)

        elif typ == "export_dm_done":
            await self.webhook_exporter.handle_ws_export_dm_done(data)

        elif typ == "export_message":
            if (
                getattr(self, "shutting_down", False)
                or self.webhook_exporter.is_stopped
            ):
                return
            await self.webhook_exporter.handle_ws_export_message(data)

        elif typ == "export_messages_done":
            await self.webhook_exporter.handle_ws_export_messages_done(data)

    async def handle_announce(self, data: dict):
        if self._shutting_down:
            return

        try:
            guild_id = int(data["guild_id"])
            raw_kw = data["keyword"]
            content = data["content"]
            author = data["author"]
            orig_chan_id = data.get("channel_id")
            timestamp = data["timestamp"]

            channel_mention = f"<#{orig_chan_id}>" if orig_chan_id else "unknown"

            all_sub_keys = self.db.get_announcement_keywords(guild_id)
            matching_keys = [
                sub_kw
                for sub_kw in all_sub_keys
                if sub_kw == "*"
                or re.search(rf"\b{re.escape(sub_kw)}\b", content, re.IGNORECASE)
            ]

            user_ids = set()
            for mk in matching_keys:
                user_ids.update(self.db.get_announcement_users(guild_id, mk))

            if not user_ids:
                return

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
            embed.add_field(name="Guild ID", value=f"`{str(guild_id)}`", inline=True)
            if orig_chan_id:
                embed.add_field(name="Channel", value=channel_mention, inline=True)
            embed.add_field(name="Keyword", value=kw_value, inline=True)

            for uid in user_ids:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    await user.send(embed=embed)
                    logger.info(
                        f"[🔔] DM’d {user} for keys={matching_keys} in g={guild_id}"
                    )
                except Exception as e:
                    logger.warning(
                        f"[⚠️] Failed DM uid={uid} keys={matching_keys} g={guild_id}: {e}"
                    )

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

    def _purge_stale_mappings(self, guild: discord.Guild) -> None:

        for orig, row in list(self.cat_map.items()):
            if int(row.get("cloned_guild_id") or 0) != guild.id:
                continue
            if not guild.get_channel(row["cloned_category_id"]):
                logging.info("[🗑️] Purging category mapping %s", orig)
                try:
                    self.db.delete_category_mapping(int(orig))
                except Exception:
                    logger.exception("Failed to delete stale category mapping %s", orig)
                self.cat_map.pop(orig, None)

        for orig, row in list(self.chan_map.items()):
            if int(row.get("cloned_guild_id") or 0) != guild.id:
                continue
            if not guild.get_channel(row["cloned_channel_id"]):
                logging.info("[🗑️] Purging channel mapping %s", orig)
                try:
                    self.db.delete_channel_mapping(int(orig))
                except Exception:
                    logger.exception("Failed to delete stale channel mapping %s", orig)
                self.chan_map.pop(orig, None)

    async def sync_structure(self, task_id: int, sitemap: Dict) -> str:
        """
        Synchronizes the structure of a Discord guild based on the provided sitemap.
        """
        logger.debug(f"Sync Task #{task_id}: Processing sitemap {sitemap}")

        host_guild_id_raw = (sitemap.get("guild") or {}).get("id")
        host_guild_id = int(host_guild_id_raw) if host_guild_id_raw else None
        target_clone_gid = self._target_clone_gid_for_origin(host_guild_id)
        self._maybe_enrich_mapping_identity_from_sitemap(sitemap)

        try:
            host_gid_int = int(host_guild_id) if host_guild_id else 0
        except Exception:
            host_gid_int = 0

        host_name = self._host_name_cache.get(host_gid_int, f"host:{host_gid_int}")
        sync_tag = self._task_display_id.get(task_id, "?")

        _host_token = logctx.sync_host_name.set(host_name)
        _id_token = logctx.sync_display_id.set(sync_tag)

        try:
            if not target_clone_gid:
                logger.warning(
                    "[⛔] No guild_mappings row for origin %s; cannot sync.",
                    host_guild_id,
                )
                return "Error: no mapped clone guild for origin"

            lock = self._get_sync_lock(int(target_clone_gid))
            async with lock:
                guild = self.bot.get_guild(target_clone_gid)
                if not guild:
                    logger.warning(
                        "[⛔] Clone guild %s not found (origin=%s)",
                        target_clone_gid,
                        host_guild_id,
                    )
                    return "Error: clone guild missing"

                settings = resolve_mapping_settings(
                    self.db, self.config, original_guild_id=host_guild_id
                )

                # Track any background jobs we kick off so we can report them
                bg_jobs: list[str] = []

                # Refresh mappings before we act
                self._load_mappings()

                # Stash latest stickers in case sticker sync needs them later
                self.stickers.set_last_sitemap(
                    clone_guild_id=target_clone_gid,
                    stickers=sitemap.get("stickers"),
                    host_guild_id=host_guild_id,
                )

                # Emoji sync (background task)
                if settings.get("CLONE_EMOJI", True):
                    self.emojis.kickoff_sync(
                        sitemap.get("emojis", []),
                        host_guild_id,
                        target_clone_guild_id=target_clone_gid,
                    )
                    bg_jobs.append("emoji")

                # Sticker sync (background task)
                if settings.get("CLONE_STICKER", True):
                    self.stickers.kickoff_sync(
                        target_clone_guild_id=target_clone_gid
                    )
                    bg_jobs.append("sticker")

                # Role sync (background task, plus optional permission mirroring)
                roles_handle = None
                if settings.get("CLONE_ROLES", True):
                    roles_handle = self.roles.kickoff_sync(
                        sitemap.get("roles", []),
                        host_guild_id=host_guild_id,
                        target_clone_guild_id=target_clone_gid,
                        delete_roles=settings.get("DELETE_ROLES", False),
                        mirror_permissions=settings.get(
                            "MIRROR_ROLE_PERMISSIONS", False
                        ),
                    )
                    bg_jobs.append("roles")

                # --- Immediate / foreground structure work ---

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
                    guild, self._parse_sitemap(sitemap), host_guild_id
                )
                if moved:
                    parts.append(f"Reparented {moved} channels")

                parts += await self._sync_threads(guild, sitemap)

                # Reload mappings after all structure edits
                self._load_mappings()

                # Channel permission sync (also background-ish, usually depends on roles)
                if settings.get("MIRROR_ROLE_PERMISSIONS", False) and settings.get(
                    "CLONE_ROLES", False
                ):
                    self.perms.schedule_after_role_sync(
                        roles_manager=self.roles,
                        roles_handle_or_none=roles_handle,
                        guild=guild,
                        sitemap=sitemap,
                    )
                    bg_jobs.append("channel-perms")

            # Build the human summary string we return to caller / log
            main_summary = "; ".join(parts) if parts else "No structure changes needed"

            # queue a flush after all this work
            self._schedule_flush()

            return main_summary

        finally:
            logctx.sync_host_name.reset(_host_token)
            logctx.sync_display_id.reset(_id_token)


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
                    if rc and uc and curr_rules == rc and curr_updates == uc:
                        return parts
            else:
                return parts

        # Case 1: We want community disabled and it's currently enabled → disable it.
        if curr_enabled and not want:
            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await guild.edit(community=False)
                parts.append("[⚙️] Disabled Community mode")

                logging.info("[⚙️] Community mode disabled for guild %s", guild.id
                )

            except Exception as e:
                logging.warning(
                    "[⚠️] Failed disabling Community mode for guild %s: %s",
                    guild.id,
                    e,
                )

            return parts

        if want and rules_id and updates_id:
            rm = self.chan_map.get(rules_id)
            um = self.chan_map.get(updates_id)
            if rm and um:
                rc = guild.get_channel(rm["cloned_channel_id"])
                uc = guild.get_channel(um["cloned_channel_id"])

                # If either channel is missing in the clone guild, we can't safely edit

                if not rc or not uc:
                    logging.warning(
                        "[⚠️] Community sync skipped for guild %s: "
                        "rules/updates clone channel missing (rc=%s uc=%s wanted=%s).",
                        guild.id,
                        getattr(rc, "id", None),
                        getattr(uc, "id", None),
                        want,
                    )

                    return parts

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
                        "rules {}→{}".format(
                            getattr(curr_rules, "id", "None"),
                            getattr(rc, "id", "None"),
                        )
                    )
                if curr_updates != uc:
                    changes.append(
                        "updates {}→{}".format(
                            getattr(curr_updates, "id", "None"),
                            getattr(uc, "id", "None"),
                        )
                    )

                try:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, guild.id
                    )
                    await guild.edit(**edit_kwargs)
                    parts.append("Updated Community mode")
                    logging.info(
                        "[⚙️] Community settings changed: %s",
                        ", ".join(changes),
                    )
                except discord.Forbidden as e:

                    if (
                        "150011" in getattr(e, "text", "")
                        or getattr(e, "code", None) == 150011
                    ):
                        logging.warning(
                            "[⚠️] Cannot enable Community mode automatically for guild %s: "
                            "missing permission or prerequisite setup.",
                            guild.id,
                        )

                    else:
                        logging.warning(
                            "[⚠️] Failed editing Community settings for guild %s: %s",
                            guild.id,
                            e,
                        )
                except Exception as e:
                    logging.warning(
                        "[⚠️] Failed editing Community settings for guild %s: %s",
                        guild.id,
                        e,
                    )

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

        host_guild_id = (sitemap.get("guild") or {}).get("id")

        wanted = {c["id"] for c in sitemap.get("categories", [])}
        name_for = {c["id"]: c["name"] for c in sitemap.get("categories", [])}

        for cat_row in self.db.get_all_category_mappings():
            orig_cat_id = cat_row["original_category_id"]
            if orig_cat_id not in wanted:
                continue

            if not guild.get_channel(cat_row["cloned_category_id"]):
                new_cat, did_create = await self._ensure_category(
                    guild, orig_cat_id, name_for[orig_cat_id], host_guild_id
                )
                if did_create:
                    created += 1

                self.db.upsert_category_mapping(
                    orig_id=orig_cat_id,
                    orig_name=name_for[orig_cat_id],
                    clone_id=new_cat.id,
                    original_guild_id=host_guild_id,
                    cloned_guild_id=guild.id,
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
                        original_guild_id=host_guild_id,
                        cloned_guild_id=guild.id,
                    )

                    self.chan_map[ch_orig_id]["cloned_parent_category_id"] = new_cat.id

                    ch = guild.get_channel(ch_row["cloned_channel_id"])
                    if ch:
                        await self.ratelimit.acquire_for_guild(
                            ActionType.EDIT_CHANNEL, guild.id
                        )
                        await ch.edit(category=new_cat)
                        logging.info(
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
        host_guild_id = (sitemap.get("guild") or {}).get("id")

        rem = await self._handle_removed_categories(guild, sitemap)
        if rem:
            parts.append(f"Deleted {rem} categories")
        ren = await self._handle_renamed_categories(guild, sitemap)
        if ren:
            parts.append(f"Renamed {ren} categories")
        created = 0
        for cat in sitemap.get("categories", []):
            _, did_create = await self._ensure_category(
                guild, cat["id"], cat["name"], host_guild_id
            )
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

        host_guild_id = (sitemap.get("guild") or {}).get("id")

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
                original_guild_id=host_guild_id,
                cloned_guild_id=guild.id,
            )
            self.chan_map[orig] = {
                "original_channel_id": orig,
                "original_channel_name": forum["name"],
                "cloned_channel_id": ch.id,
                "channel_webhook_url": url,
                "original_parent_category_id": forum.get("category_id"),
                "cloned_parent_category_id": parent.id if parent else None,
                "channel_type": ChannelType.forum.value,
                "original_guild_id": host_guild_id,
                "cloned_guild_id": guild.id,
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
        host_guild_id = (sitemap.get("guild") or {}).get("id")

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
                host_guild_id, guild, orig, name, pid, pname, ctype
            )
            if is_new:
                created += 1

            ch = guild.get_channel(clone_id)
            if not ch:
                continue

            if ctype == ChannelType.news.value:

                if "NEWS" in guild.features and ch.type != ChannelType.news:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, guild.id
                    )
                    await ch.edit(type=ChannelType.news)
                    converted += 1
                    logging.info(
                        "[✏️] Converted channel '%s' (ID %d) to Announcement",
                        ch.name,
                        ch.id,
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
                        original_guild_id=host_guild_id,
                        cloned_guild_id=guild.id,
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

                logging.info(
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
                    logging.warning(
                        "[🗑️] Original thread '%s' no longer exists; clearing mapping (clone=%s)",
                        thread_name,
                        clone_id,
                    )

                    if clone_ch and getattr(self.config, "DELETE_THREADS", False):
                        await self.ratelimit.acquire_for_guild(
                            ActionType.DELETE_CHANNEL, guild.id
                        )
                        await clone_ch.delete()
                        logging.info("[🗑️] Deleted cloned thread %s", clone_id
                        )
                    self.db.delete_forum_thread_mapping(orig_id)
                    deleted_original_gone += 1
                    continue

        if deleted_original_gone:
            parts.append(f"Deleted {deleted_original_gone} threads (original gone)")
        if cleared_missing_clone:
            parts.append(
                f"Cleared {cleared_missing_clone} missing clone thread mappings"
            )

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
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await ch.edit(name=src["name"])
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
                    original_guild_id=int(sitemap.get("guild_id") or 0),
                    cloned_guild_id=guild.id,
                )

                logging.info("[✏️] Renamed thread %s: %r → %r", ch.id, old, src["name"]
                )

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
            logging.warning(
                "[⚠️] Category %s full for guild %s (or guild at cap); creating '%s' as standalone",
                guild.id,
                cat_label,
                name,
            )

            category = None

        if kind == "forum":
            await self.ratelimit.acquire_for_guild(ActionType.CREATE_CHANNEL, guild.id)
            ch = await guild.create_forum_channel(name=name, category=category)
        else:
            await self.ratelimit.acquire_for_guild(ActionType.CREATE_CHANNEL, guild.id)
            ch = await guild.create_text_channel(name=name, category=category)

        logging.info("[➕] Created %s channel '%s' #%s", kind, name, ch.id)

        if kind == "news":
            if "NEWS" in guild.features:
                try:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, guild.id
                    )
                    await ch.edit(type=ChannelType.news)
                    logging.info("[✏️] Converted '%s' #%d to Announcement", name, ch.id
                    )
                except HTTPException as e:
                    logging.warning(
                        "[⚠️] Could not convert '%s' to Announcement in guild %s: %s; left as text",
                        name,
                        guild.id,
                        e,
                    )
            else:
                logging.warning(
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
                        original_guild_id=mapping.get("original_guild_id"),
                        cloned_guild_id=mapping.get("cloned_guild_id"),
                    )

                    mapping["original_channel_name"] = upstream_name
                    if has_pin:
                        mapping["clone_channel_name"] = pinned_name
                except Exception:
                    logger.debug("[rename] mapping upsert failed", exc_info=True)

            if has_pin:

                if ch.name != pinned_name:
                    old = ch.name
                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, ch.guild.id
                    )
                    await ch.edit(name=pinned_name)
                    logging.info(
                        "[📌] Enforced pinned name on #%d: %r → %r",
                        ch.id,
                        old,
                        pinned_name,
                    )
                    return True, "pinned_enforced"
                return False, "skipped_already_ok"

            if ch.name != upstream_name:
                old = ch.name
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, ch.guild.id
                )
                await ch.edit(name=upstream_name)
                logging.info(
                    "[✏️] Renamed channel #%d: %r → %r",
                    ch.id,
                    old,
                    upstream_name,
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
            if int(row.get("cloned_guild_id") or 0) != guild.id:
                continue
            if orig_id not in valid_ids:
                ch = guild.get_channel(row["cloned_category_id"])
                delete_channels = False
                settings = (
                    resolve_mapping_settings(
                        self.db,
                        self.config,
                        original_guild_id=int(row.get("original_guild_id") or 0),
                        cloned_guild_id=int(row.get("cloned_guild_id") or guild.id),
                    )
                    or {}
                )
                delete_channels = bool(settings.get("DELETE_CHANNELS", False))

                if ch and delete_channels:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.DELETE_CHANNEL, guild.id
                    )
                    await ch.delete()
                    logging.info("[🗑️] Deleted category %s", ch.name)

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
            if int(row.get("cloned_guild_id") or 0) != guild.id:
                continue
            if int(orig_id) in valid_ids:
                continue

            clone_id = int(row["cloned_channel_id"])
            ch = guild.get_channel(clone_id)

        for orig_id, row in list(self.chan_map.items()):
            if int(row.get("cloned_guild_id") or 0) != guild.id:
                continue
            if int(orig_id) in valid_ids:
                continue

            clone_id = int(row["cloned_channel_id"])
            ch = guild.get_channel(clone_id)

            delete_channels = False
            settings = (
                resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=int(row.get("original_guild_id") or 0),
                    cloned_guild_id=int(row.get("cloned_guild_id") or guild.id),
                )
                or {}
            )
            delete_channels = bool(settings.get("DELETE_CHANNELS", False))

            if ch and delete_channels:

                if ch.id in protected:
                    logging.info(
                        "[🛡️] Skipping deletion of protected channel #%s (%d) (community/system assignment).",
                        ch.name,
                        ch.id,
                    )
                else:

                    await self.ratelimit.acquire_for_guild(
                        ActionType.DELETE_CHANNEL, guild.id
                    )
                    try:
                        await ch.delete()
                        logging.info("[🗑️] Deleted channel #%s (%d)", ch.name, ch.id
                        )
                    except discord.HTTPException as e:

                        if getattr(
                            e, "code", None
                        ) == 50074 or "required for community" in str(e):
                            logging.info(
                                "[🛡️] API blocked deletion of #%s (%d): protected. Will skip and drop mapping.",
                                getattr(ch, "name", "?"),
                                ch.id,
                            )

                        else:
                            logging.warning(
                                "[⚠️] Failed to delete channel #%d: %s",
                                ch.id,
                                e,
                            )

            elif not ch:
                logging.info(
                    "[🗑️] Cloned channel #%d not found; removing mapping",
                    clone_id,
                )

            self.db.delete_channel_mapping(orig_id)
            self.chan_map.pop(orig_id, None)
            removed += 1

        return removed

    async def _maybe_rename_category(
        self,
        cat: discord.CategoryChannel,
        upstream_name: str,
        orig_cat_id: int,
        host_guild_id: Optional[int],
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
                        orig_id=orig_cat_id,
                        orig_name=upstream_name,
                        clone_id=int(mapping.get("cloned_category_id") or cat.id),
                        clone_name=pinned_name if has_pin else None,
                        original_guild_id=host_guild_id,
                        cloned_guild_id=cat.guild.id,
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
            await self.ratelimit.acquire_for_guild(
                ActionType.EDIT_CHANNEL, cat.guild.id
            )
            await cat.edit(name=desired)

            if has_pin:

                logging.info(
                    "[📌] Enforced pinned category name on %d: %r → %r",
                    cat.id,
                    old,
                    desired,
                )

                return (True, "pinned_enforced")
            else:
                logging.info(
                    "[✏️] Renamed category %d: %r → %r",
                    cat.id,
                    old,
                    desired,
                )
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
        host_guild_id = (sitemap.get("guild") or {}).get("id")

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
                clone_cat, upstream_name, orig_id, host_guild_id
            )
            if did:
                renamed += 1

        return renamed

    async def _ensure_category(
        self,
        guild: discord.Guild,
        original_id: int,
        name: str,
        host_guild_id: Optional[int] = None,
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

        await self.ratelimit.acquire_for_guild(ActionType.CREATE_CHANNEL, guild.id)
        cat = await guild.create_category(name)
        logging.info("[➕] Created category '%s' #%d", name, cat.id)

        self.db.upsert_category_mapping(
            orig_id=original_id,
            orig_name=name,
            clone_id=cat.id,
            original_guild_id=host_guild_id,
            cloned_guild_id=guild.id,
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
            await self.ratelimit.acquire_for_guild(
                ActionType.WEBHOOK_CREATE, ch.guild.id
            )
            try:
                await self._get_default_avatar_bytes()
            except Exception:
                pass
            webhook = await ch.create_webhook(name=name, avatar=avatar_bytes)
            logging.info("[➕] Created webhook '%s' in channel #%s", name, ch.id
            )

            if hasattr(self, "_wh_meta"):
                self._wh_meta.clear()

            return webhook

    async def _ensure_channel_and_webhook(
        self,
        host_guild_id: Optional[int],
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
            category, _ = await self._ensure_category(
                guild, parent_id, parent_name, host_guild_id
            )

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
                        original_guild_id=host_guild_id,
                        cloned_guild_id=guild.id,
                    )

                    self.chan_map[original_id] = {
                        "original_channel_id": original_id,
                        "original_channel_name": original_name,
                        "cloned_channel_id": clone_id,
                        "channel_webhook_url": url,
                        "original_parent_category_id": parent_id,
                        "cloned_parent_category_id": category.id if category else None,
                        "channel_type": channel_type,
                        "original_guild_id": host_guild_id,
                        "cloned_guild_id": guild.id,
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
            original_guild_id=host_guild_id,
            cloned_guild_id=guild.id,
        )
        self.chan_map[original_id] = {
            "original_channel_id": original_id,
            "original_channel_name": original_name,
            "cloned_channel_id": ch.id,
            "channel_webhook_url": url,
            "original_parent_category_id": parent_id,
            "cloned_parent_category_id": category.id if category else None,
            "channel_type": channel_type,
            "original_guild_id": host_guild_id,
            "cloned_guild_id": guild.id,
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
        host_guild_id: int | None,
    ) -> bool:
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
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await ch.edit(category=desired_parent)
                moved += 1
                old_name = actual_parent.name if actual_parent else "standalone"
                new_name = desired_parent.name if desired_parent else "standalone"
                logging.info(
                    "[✏️] Reparented channel '%s' (ID %d) from '%s' → '%s'",
                    ch.name,
                    clone_id,
                    old_name,
                    new_name,
                )
            except Exception as e:
                logging.warning(
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
                original_guild_id=host_guild_id,
                cloned_guild_id=guild.id,
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

    async def _recreate_webhook(
        self, original_id: int, host_guild_id: Optional[int]
    ) -> Optional[str]:
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
            clone_gid = self._clone_gid_for_ctx(
                host_guild_id=int(host_guild_id) if host_guild_id else None,
                mapping_row=fresh,
            )
            guild = self.bot.get_guild(int(clone_gid)) if clone_gid else None
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
                    original_guild_id=host_guild_id,
                    cloned_guild_id=guild.id,
                )

                logging.info(
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

    async def handle_thread_delete(self, data: dict):
        try:
            orig_tid = int(data.get("thread_id") or data.get("channel_id") or 0)
            if not orig_tid:
                return

            thr_row = next(
                (
                    dict(r)
                    for r in self.db.get_all_threads()
                    if int(r["original_thread_id"]) == orig_tid
                ),
                None,
            )

            host_gid = int(data.get("guild_id") or 0) or None
            clone_gid = self._clone_gid_for_ctx(
                host_guild_id=host_gid, mapping_row=thr_row
            )

            if not clone_gid:
                logger.debug(
                    "[threads] delete: no mapped clone guild for thread %s", orig_tid
                )
                return

            g = self.bot.get_guild(int(clone_gid))
            if not g:
                logger.debug("[threads] delete: clone guild %s not in cache", clone_gid)
                return

            if thr_row and thr_row.get("cloned_thread_id"):
                try:
                    ch = g.get_channel(
                        int(thr_row["cloned_thread_id"])
                    ) or await self.bot.fetch_channel(int(thr_row["cloned_thread_id"]))
                    if ch:
                        await ch.delete(reason="Source thread deleted")
                except Exception:
                    logger.debug(
                        "[threads] delete: could not delete cloned thread",
                        exc_info=True,
                    )

            self.db.delete_forum_thread_mapping(orig_tid)
            logging.info(
                "[🗑️] Deleted thread mapping for thread %s in guild %s ",
                orig_tid,
                clone_gid,
            )

        except Exception:
            logger.exception("handle_thread_delete failed")

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
        parent_id = int(data.get("thread_parent_id") or 0) or None
        chan_map = self.chan_map.get(parent_id) or {}
        clone_gid = self._clone_gid_for_ctx(
            host_guild_id=int(data.get("guild_id") or 0) or None,
            mapping_row=chan_map,
        )
        guild = self.bot.get_guild(int(clone_gid)) if clone_gid else None
        if not guild:
            logger.error(
                "[⛔] No clone guild could be resolved for thread parent=%s", parent_id
            )
            return

        if not guild:
            logger.error("[⛔] Clone guild not available for thread renames")
            return

        ch = guild.get_channel(cloned_id)
        if not ch:
            try:
                ch = await self.bot.fetch_channel(cloned_id)
            except NotFound:
                logging.warning(
                    "[⚠️] Thread renamed in #%s: %r → %r; not found in cloned server, cannot rename",
                    parent_name,
                    old_name,
                    new_name,
                )

                return

        try:
            await self.ratelimit.acquire_for_guild(ActionType.EDIT_CHANNEL, guild.id)
            await ch.edit(name=new_name)
            logger.info(
                "[✏️] Renamed thread in #%s: %r → %r",
                ch.parent.name if ch.parent else "Unknown",
                old_name,
                new_name,
            )

        except Exception as e:
            logger.error(
                "[⛔] Failed to rename thread in #%s: %s",
                ch.parent.name if ch.parent else "Unknown",
                e,
            )

        self.db.upsert_forum_thread_mapping(
            orig_thread_id,
            new_name,
            cloned_id,
            row["forum_original_id"],
            row["forum_cloned_id"],
            original_guild_id=int(data.get("guild_id") or 0),
            cloned_guild_id=guild.id,
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
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
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
                    logger.warning(
                        "[⚠️] Thread %s not found; clearing mapping and skipping future attempts",
                        thread.id,
                    )

                    self.db.delete_forum_thread_mapping(thread.id)
                else:
                    logger.warning(
                        "[⚠️] Failed to auto-archive thread '%s' in #%s: %s",
                        thread.name,
                        parent_name,
                        e,
                    )

    def _sanitize_inline(self, s: str | None) -> str | None:
        if not s:
            return s
        s = self._replace_emoji_ids(s)
        s = self._remap_channel_mentions(s)
        s = self._remap_role_mentions(s)
        s = self._rewrite_message_links(s)
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

    def _rewrite_message_links(
        self,
        content: str,
        *,
        ctx_guild_id: int | None = None,
        ctx_mapping_row: dict | None = None,
    ) -> str:
        """
        Rewrites https://discord.com/channels/<gid>/<cid>/<mid> links in `content`
        to point at the mapped *clone* guild/channel/message where we can.
        """
        _link = re.compile(
            r"(https?://(?:ptb\.|canary\.)?discord(?:app)?\.com/channels/)"
            r"(?P<gid>\d+|@me)/(?P<cid>\d+)/(?P<mid>\d+)"
        )

        def repl(m: re.Match) -> str:
            base = m.group(1)
            gid = m.group("gid")
            cid = int(m.group("cid"))
            mid = int(m.group("mid"))

            row = None
            try:
                row = self.db.get_mapping_by_original(mid)
            except Exception:
                row = None

            ch_row = None
            if not row:
                try:
                    ch_row = self.db.get_channel_mapping_by_original_id(cid)
                except Exception:
                    ch_row = None

            try:
                host_gid = (
                    int(
                        (row or ch_row or {}).get("original_guild_id")
                        or (gid if gid != "@me" else 0)
                    )
                    or None
                )
            except Exception:
                host_gid = None

            clone_gid = self._clone_gid_for_ctx(
                host_guild_id=host_gid, mapping_row=row or ch_row
            ) or self._target_clone_gid_for_origin(ctx_guild_id)

            if not clone_gid:
                return m.group(0)

            cloned_cid = int((row or ch_row or {}).get("cloned_channel_id") or cid)
            cloned_mid = int((row or {}).get("cloned_message_id") or mid)

            return f"{base}{clone_gid}/{cloned_cid}/{cloned_mid}"

        return _link.sub(repl, content)

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
        """
        Return ' [backfill]' and/or ' [buffered]' when applicable.
        Live messages get no tag.
        """
        parts = []
        if data.get("__backfill__"):
            parts.append("backfill")
        if data.get("__buffered__"):
            parts.append("buffered")
        return f" [{' & '.join(parts)}]" if parts else ""

    def _bf_state(self, clone_id: int) -> dict:
        st = self._bf_throttle.get(int(clone_id))
        if st is None:
            st = {"lock": asyncio.Lock(), "last": 0.0}
            self._bf_throttle[int(clone_id)] = st
        return st

    async def _bf_gate(self, clone_id: int) -> None:
        st = self._bf_state(int(clone_id))
        async with st["lock"]:
            now = asyncio.get_event_loop().time()
            wait = max(0.0, (st["last"] + self._bf_delay) - now)
            if wait >= 0.001:
                await asyncio.sleep(wait)
            st["last"] = asyncio.get_event_loop().time()

    def _clear_bf_throttle(self, clone_id: int) -> None:
        self._bf_throttle.pop(int(clone_id), None)

    async def forward_message(self, msg: Dict):
        """
        Forwards a message to the appropriate channel webhook based on the channel mapping.
        """
        if self._shutting_down:
            return

        tag = self._log_tag(msg)
        source_id = msg["channel_id"]
        host_guild_id = msg.get("guild_id")
        is_backfill = bool(msg.get("__backfill__"))

        def _cached_primary_for_source(src_id: int):
            st = self.backfill._progress.get(int(src_id)) or {}
            ident = st.get("primary_identity") or {}
            mapping = self.chan_map.get(src_id) or {}
            purl = mapping.get("channel_webhook_url") or mapping.get("webhook_url")
            name = ident.get("name")
            avatar_url = ident.get("avatar_url")
            canonical = self.backfill._canonical_temp_name()
            customized = bool(name and name != canonical)
            return purl, name, avatar_url, customized

        try:
            _orig_mid_for_inflight = int((msg.get("message_id") or 0))
        except Exception:
            _orig_mid_for_inflight = 0
        if (
            _orig_mid_for_inflight
            and _orig_mid_for_inflight not in self._inflight_events
        ):
            self._inflight_events[_orig_mid_for_inflight] = asyncio.Event()

        mapping = self.chan_map.get(source_id)
        if mapping is None:
            self._load_mappings()
            mapping = self.chan_map.get(source_id)

        stickers = msg.get("stickers") or []
        if stickers:
            clone_gid = self._clone_gid_for_ctx(
                host_guild_id=int(msg.get("guild_id") or 0) or None,
                mapping_row=mapping,
            )
            guild = self.bot.get_guild(int(clone_gid)) if clone_gid else None
            ch = (
                guild.get_channel(int(mapping["cloned_channel_id"]))
                if (guild and mapping)
                else None
            )
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
                    self.backfill.note_sent(source_id, int(msg["message_id"]))
                    self.backfill.note_checkpoint(
                        source_id, int(msg["message_id"]), msg.get("timestamp")
                    )
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

            current_url = url_to_use
            while True:
                await self.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=rl_key)
                released = False
                try:

                    if override_identity is not None:
                        kw_username = override_identity.get("username")
                        kw_avatar = override_identity.get("avatar_url")
                    else:
                        kw_username = (
                            None if use_webhook_identity else payload.get("username")
                        )
                        kw_avatar = (
                            None if use_webhook_identity else payload.get("avatar_url")
                        )

                    logger.debug(
                        "[send] use_webhook_identity=%s override=%s | src=%s | ch=%s | username=%r avatar_url=%r",
                        use_webhook_identity,
                        bool(override_identity),
                        source_id,
                        msg.get("channel_name"),
                        kw_username,
                        kw_avatar,
                    )

                    sent_msg = await webhook.send(
                        content=payload.get("content"),
                        embeds=payload.get("embeds"),
                        username=kw_username,
                        avatar_url=kw_avatar,
                        wait=True,
                    )

                    try:
                        orig_gid = int(msg.get("guild_id") or 0)
                        orig_cid = int(msg.get("channel_id") or 0)
                        orig_mid = int(msg.get("message_id") or 0)
                        cloned_cid = int(mapping["cloned_channel_id"])
                        used_url = getattr(webhook, "url", None)
                        cloned_mid = (
                            int(getattr(sent_msg, "id", 0)) if sent_msg else None
                        )

                        clone_gid = self._clone_gid_for_ctx(
                            host_guild_id=orig_gid or None,
                            mapping_row=mapping,
                        )
                        self.db.upsert_message_mapping(
                            original_guild_id=orig_gid,
                            original_channel_id=orig_cid,
                            original_message_id=orig_mid,
                            cloned_channel_id=cloned_cid,
                            cloned_message_id=cloned_mid,
                            webhook_url=used_url,
                            cloned_guild_id=int(clone_gid) if clone_gid else None,
                        )

                        ev = self._inflight_events.get(orig_mid)
                        if ev:
                            ev.set()
                        try:
                            row = self.db.get_mapping_by_original(orig_mid)
                        except Exception:
                            row = None

                        if orig_mid in self._pending_deletes:
                            try:
                                if row:
                                    ok = await self._delete_with_row(
                                        row, orig_mid, msg.get("channel_name")
                                    )
                                    if ok:
                                        logger.debug(
                                            "[🧹] Applied queued delete right after initial send for orig %s",
                                            orig_mid,
                                        )
                                else:
                                    logger.debug(
                                        "[🕒] Pending delete found but mapping re-read failed for orig %s",
                                        orig_mid,
                                    )
                            finally:
                                self._pending_deletes.discard(orig_mid)
                                self._latest_edit_payload.pop(orig_mid, None)
                                self._inflight_events.pop(orig_mid, None)
                        else:
                            latest = self._latest_edit_payload.pop(orig_mid, None)
                            if latest and row:
                                try:
                                    await self._edit_with_row(row, latest, orig_mid)
                                    logger.debug(
                                        "[edit-coalesce] applied latest edit after initial send for orig %s",
                                        orig_mid,
                                    )
                                except Exception:
                                    logger.debug(
                                        "[edit-coalesce] immediate apply failed",
                                        exc_info=True,
                                    )
                            self._inflight_events.pop(orig_mid, None)
                    except Exception:
                        logger.exception(
                            "upsert_message_mapping failed (normal channel)"
                        )

                    if is_backfill:
                        self.backfill.note_sent(
                            source_id, int(msg.get("message_id") or 0) or None
                        )
                        self.backfill.note_checkpoint(
                            source_id, int(msg["message_id"]), msg.get("timestamp")
                        )
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
                    else:
                        logger.info(
                            "[💬]%s Forwarded message to #%s from %s (%s)",
                            tag,
                            msg.get("channel_name"),
                            msg.get("author"),
                            msg.get("author_id"),
                        )

                    return

                except HTTPException as e:

                    self.ratelimit.relax(ActionType.WEBHOOK_MESSAGE, key=rl_key)
                    released = True

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

                        continue

                    elif e.status == 404:
                        logger.debug(
                            "Webhook %s returned 404; attempting recreate...",
                            current_url,
                        )
                        new_url = await self._recreate_webhook(source_id, host_guild_id)
                        if not new_url:
                            logger.warning(
                                "[⌛] No mapping for channel %s; msg from %s is queued and will be sent after sync",
                                msg.get("channel_name"),
                                msg.get("author"),
                            )
                            msg["__buffered__"] = True
                            self._pending_msgs.setdefault(source_id, []).append(msg)
                            return

                        current_url = new_url
                        webhook = Webhook.from_url(current_url, session=self.session)
                        self._webhooks[current_url] = webhook
                        continue

                    else:
                        logger.error(
                            "[⛔] Failed to send to #%s (status %s): %s",
                            msg.get("channel_name"),
                            e.status,
                            e.text,
                        )
                        return

                except (ClientError, asyncio.TimeoutError) as e:

                    if not released:
                        self.ratelimit.relax(ActionType.WEBHOOK_MESSAGE, key=rl_key)
                    logger.warning(
                        "[🌐]%s Network error sending to #%s: %s — queued for retry",
                        tag,
                        msg.get("channel_name"),
                        e,
                    )
                    msg["__buffered__"] = True
                    self._pending_msgs.setdefault(source_id, []).append(msg)
                    return

                finally:

                    if not released:
                        self.ratelimit.relax(ActionType.WEBHOOK_MESSAGE, key=rl_key)

        forced_url = msg.get("__force_webhook_url__")
        if forced_url:
            primary_url, primary_name, primary_avatar_url, primary_customized = (
                _cached_primary_for_source(source_id)
            )
            sem = None
            clone_for_gate = None
            if is_backfill:
                m = self.chan_map.get(source_id) or (
                    self._load_mappings() or self.chan_map.get(source_id)
                )
                clone_for_gate = (m or {}).get("cloned_channel_id") or (m or {}).get(
                    "clone_channel_id"
                )
                if clone_for_gate:

                    sem = self.backfill.semaphores.setdefault(
                        int(clone_for_gate), asyncio.Semaphore(1)
                    )

            is_primary = bool(primary_url and forced_url == primary_url)
            use_webhook_identity = bool(primary_customized and is_primary)
            override = None
            if primary_customized and not is_primary:
                override = {"username": primary_name, "avatar_url": primary_avatar_url}

            rl_key = f"channel:{clone_for_gate or source_id}"

            if sem:
                async with sem:
                    await self._bf_gate(int(clone_for_gate))
                    await _do_send(
                        forced_url,
                        rl_key,
                        use_webhook_identity=use_webhook_identity,
                        override_identity=override,
                    )
            else:
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
            clone_gid = self._clone_gid_for_ctx(
                host_guild_id=int(msg.get("guild_id") or 0) or None,
                mapping_row=mapping,
            )
            guild = self.bot.get_guild(int(clone_gid)) if clone_gid else None
            ch = (
                guild.get_channel(int(mapping["cloned_channel_id"]))
                if (guild and mapping)
                else None
            )
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
                    self.backfill.note_sent(source_id, int(msg["message_id"]))
                    self.backfill.note_checkpoint(
                        source_id, int(msg["message_id"]), msg.get("timestamp")
                    )
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

            clone_gid = None
            try:
                clone_gid = int(mapping.get("cloned_guild_id") or 0)
            except Exception:
                pass

            if clone_gid:
                lock = self._guild_sync_locks.get(clone_gid)
                if lock and lock.locked():
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

            primary_url, primary_name, primary_avatar_url, primary_customized = (
                _cached_primary_for_source(source_id)
            )

            sem = self.backfill.semaphores.setdefault(
                int(clone_id), asyncio.Semaphore(1)
            )
            async with sem:

                await self._bf_gate(int(clone_id))

                url_to_use, _ = await self.backfill.pick_url_for_send(
                    int(clone_id), url, create_missing=False
                )

                rl_key = f"channel:{clone_id}"

                if primary_customized:
                    is_primary = bool(primary_url and url_to_use == primary_url)
                    if is_primary:
                        await _do_send(
                            url_to_use,
                            rl_key,
                            use_webhook_identity=True,
                            override_identity=None,
                        )
                    else:
                        await _do_send(
                            url_to_use,
                            rl_key,
                            use_webhook_identity=False,
                            override_identity={
                                "username": primary_name,
                                "avatar_url": primary_avatar_url,
                            },
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
        clone_id = mapping.get("cloned_channel_id") or mapping.get("clone_channel_id")
        rl_key = f"channel:{clone_id or source_id}"

        await _do_send(
            url_to_use,
            rl_key,
            use_webhook_identity=primary_customized,
            override_identity=None,
        )

    def _coerce_embeds(self, lst):
        result = []
        for e in lst or []:
            if isinstance(e, discord.Embed):
                result.append(e)
            elif isinstance(e, dict):
                emb = discord.Embed(
                    title=e.get("title"),
                    description=e.get("description"),
                )
                img = e.get("image") or {}
                if isinstance(img, dict) and img.get("url"):
                    emb.set_image(url=img["url"])
                thumb = e.get("thumbnail") or {}
                if isinstance(thumb, dict) and thumb.get("url"):
                    emb.set_thumbnail(url=thumb["url"])
                result.append(emb)
        return result

    async def _get_mapping_with_retry(
        self,
        orig_mid: int,
        *,
        attempts: int = 5,
        base_delay: float = 0.08,
        max_delay: float = 0.8,
        jitter: float = 0.25,
        log_prefix: str = "mapping",
    ):
        """Short, bounded retry to tolerate late DB writes."""
        row = None
        for i in range(1, attempts + 1):
            try:
                row = self.db.get_mapping_by_original(orig_mid)
            except Exception:
                row = None
            if row is not None:
                if i > 1:
                    logger.debug(
                        "[⏱️] %s found on attempt %d for orig %s",
                        log_prefix,
                        i,
                        orig_mid,
                    )
                return row
            delay = min(max_delay, base_delay * (2 ** (i - 1)))
            delay += random.uniform(-jitter * delay, jitter * delay)
            await asyncio.sleep(max(0.0, delay))
        logger.debug(
            "[⌛] %s not found after %d attempts for orig %s",
            log_prefix,
            attempts,
            orig_mid,
        )
        return None

    async def _edit_with_row(self, row, data: dict, orig_mid: int) -> bool:
        try:
            cloned_mid = int(row["cloned_message_id"])
            webhook_url = row["webhook_url"]
        except Exception:
            cloned_mid = None
            webhook_url = None

        if not (cloned_mid and webhook_url):
            return False

        try:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()
            wh = Webhook.from_url(webhook_url, session=self.session)
            built = self._build_webhook_payload(data)
            await wh.edit_message(
                cloned_mid,
                content=built.get("content"),
                embeds=built.get("embeds"),
                allowed_mentions=None,
            )
            logger.info(
                "[✏️] Edited cloned msg %s (orig %s) in #%s",
                cloned_mid,
                orig_mid,
                data.get("channel_name"),
            )
            return True
        except Exception as e:
            logger.warning("[⚠️] Edit failed for orig %s (will resend): %s", orig_mid, e)
            return False

    async def _fallback_resend_edit(self, data: dict, orig_mid: int):

        try:
            cid = int((data or {}).get("channel_id") or 0)
        except Exception:
            cid = 0
        try:
            pid = int((data or {}).get("thread_parent_id") or 0)
        except Exception:
            pid = 0
        key = pid or cid

        if key and (
            key in self._active_backfills
            or (hasattr(self, "backfill") and self.backfill.is_backfilling(key))
        ):
            buf = self._bf_event_buffer.setdefault(key, [])

            try:
                mid = int((data or {}).get("message_id") or 0)
            except Exception:
                mid = 0
            if mid:
                buf[:] = [
                    (t, d)
                    for (t, d) in buf
                    if not (
                        t in ("message_edit", "thread_message_edit")
                        and int((d or {}).get("message_id") or 0) == mid
                    )
                ]
            buf.append(("thread_message_edit" if pid else "message_edit", data))
            logger.debug(
                "[bf] Deferred fallback resend for edited message (orig %s) while backfill active on #%s",
                orig_mid,
                key,
            )
            return

        if cid and any(
            int((m or {}).get("message_id") or 0) == orig_mid
            for m in self._pending_msgs.get(cid, [])
        ):

            self._latest_edit_payload[orig_mid] = data
            logger.debug(
                "[queue] Deferred fallback resend; orig %s still queued for #%s",
                orig_mid,
                cid,
            )
            return

        await self.forward_message(data)
        logger.info("[♻️] Resent edited message as new (orig %s)", orig_mid)

    async def handle_message_edit(self, data: dict):
        """
        Try to edit the cloned webhook message that corresponds to the original one.
        If the original send is still in-flight (rate limited / queued), wait briefly
        for its mapping to appear; coalesce multiple edits during the wait.
        """
        try:
            orig_mid = int(data.get("message_id") or 0)
            _ = int(data.get("channel_id") or 0)
        except Exception:
            return
        if not orig_mid:
            return

        row = None
        try:
            row = self.db.get_mapping_by_original(orig_mid)
        except Exception:
            row = None

        if row is not None:
            await self._edit_with_row(row, data, orig_mid)
            return

        ev = self._inflight_events.get(orig_mid)
        if ev and not ev.is_set():

            self._latest_edit_payload[orig_mid] = data

            try:
                await asyncio.wait_for(ev.wait(), timeout=5.0)
            except asyncio.TimeoutError:

                await self._fallback_resend_edit(data, orig_mid)
                return

            try:
                row = self.db.get_mapping_by_original(orig_mid)
            except Exception:
                row = None

            payload = self._latest_edit_payload.pop(orig_mid, data)
            if row is not None:
                await self._edit_with_row(row, payload, orig_mid)
                self._inflight_events.pop(orig_mid, None)
                return

            await self._fallback_resend_edit(payload, orig_mid)
            return

        row = await self._get_mapping_with_retry(
            orig_mid,
            attempts=5,
            base_delay=0.08,
            max_delay=0.8,
            jitter=0.25,
            log_prefix="edit-wait",
        )
        if row is not None:
            await self._edit_with_row(row, data, orig_mid)
            return

        await self._fallback_resend_edit(data, orig_mid)

    async def forward_to_webhook(self, msg_data: dict, webhook_url: str):
        async with self.session.post(
            webhook_url,
            json={
                "username": msg_data["author"]["name"],
                "avatar_url": msg_data["author"].get("avatar_url"),
                "content": msg_data["content"],
            },
        ) as resp:
            if resp.status != 200 and resp.status != 204:
                logger.warning(f"Webhook send failed: {resp.status}")

    async def handle_thread_message(self, data: dict):
        """
        Handles forwarding of thread messages from the original guild to the cloned guild.
        """
        if self._shutting_down:
            return

        import asyncio
        import aiohttp
        import discord
        from discord import ChannelType, ForumChannel, HTTPException, Webhook

        parent_id = int(data.get("thread_parent_id") or 0) or None
        chan_map = self.chan_map.get(parent_id) or {}
        clone_gid = self._clone_gid_for_ctx(
            host_guild_id=int(data.get("guild_id") or 0) or None,
            mapping_row=chan_map,
        )
        guild = self.bot.get_guild(int(clone_gid)) if clone_gid else None
        if not guild:
            logger.error(
                "[⛔] No clone guild could be resolved for thread parent=%s", parent_id
            )
            return

        self._load_mappings()

        try:
            orig_tid = int(data["thread_id"])
            parent_id = int(data["thread_parent_id"])
        except Exception:
            logger.warning(
                "[thread] bad ids in payload: thread_id=%r parent_id=%r",
                data.get("thread_id"),
                data.get("thread_parent_id"),
            )
            return

        tag = self._log_tag(data)
        is_backfill = bool(data.get("__backfill__"))

        def _bf_suffix() -> str:
            if not is_backfill or not hasattr(self, "backfill"):
                return ""
            d, t = self.backfill.get_progress(parent_id)
            if d is None:
                return ""
            left = max((t or 0) - (d or 0), 0)
            return f" [{left} left]" if t is not None else f" [{d or 0} sent]"

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
        if cloned_id is None or not cloned_parent:
            logger.info(
                "[⌛] Channel %s not cloned yet; queueing message until it’s created",
                cloned_id or data.get("channel_name"),
            )
            self._pending_thread_msgs.append(data)
            return

        payload = self._build_webhook_payload(data)

        forced_url = data.get("__force_webhook_url__")
        if forced_url:
            webhook_url = forced_url
        else:
            webhook_url = chan_map.get(
                "channel_webhook_url"
            ) or await self._ensure_primary_webhook_url(parent_id)

        if not webhook_url:
            logger.warning(
                "[⚠️] No webhook for parent %s; queueing thread msg", parent_id
            )
            self._pending_thread_msgs.append(data)
            return

        async def _get_primary_identity_for_source(src_parent_id: int):
            mapping = self.chan_map.get(src_parent_id) or {}
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

        async def _primary_name_changed_for_source(src_parent_id: int) -> bool:
            mapping = self.chan_map.get(src_parent_id) or {}
            purl = mapping.get("channel_webhook_url") or mapping.get("webhook_url")
            if not purl:
                return False
            try:
                wid = int(purl.rstrip("/").split("/")[-2])
                wh = await self.bot.fetch_webhook(wid)
                canonical = self.backfill._canonical_temp_name()
                name = (wh.name or "").strip()
                return bool(name and name != canonical)
            except Exception:
                return False

        primary_url, primary_name, primary_avatar_url = (
            await _get_primary_identity_for_source(parent_id)
        )
        primary_customized = await _primary_name_changed_for_source(parent_id)

        use_webhook_identity = False
        override_identity = None
        if primary_customized:
            if forced_url and primary_url and forced_url == primary_url:

                use_webhook_identity = True
            else:

                override_identity = {
                    "username": primary_name,
                    "avatar_url": primary_avatar_url,
                }

        stickers = data.get("stickers") or []

        def _is_custom_sticker(s: dict) -> bool:
            try:
                return int(s.get("type", 0)) == 2
            except Exception:
                return bool(s.get("guild_id") or s.get("custom") or s.get("is_custom"))

        def _has_custom(sts: list[dict]) -> bool:
            return any(_is_custom_sticker(s) for s in (sts or []))

        def _has_standard(sts: list[dict]) -> bool:
            return any(not _is_custom_sticker(s) for s in (sts or []))

        has_custom = _has_custom(stickers)
        has_standard = _has_standard(stickers)

        has_textish = bool(
            payload and (payload.get("content") or payload.get("embeds"))
        )
        if not has_textish and not stickers:
            logger.info(
                "[⚠️]%s Skipping empty payload for '%s'", tag, data.get("thread_name")
            )
            return

        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

        thread_webhook = Webhook.from_url(webhook_url, session=self.session)

        if is_backfill and cloned_id:
            await self.backfill.ensure_temps_ready(int(cloned_id))
            sem = self.backfill.semaphores.setdefault(
                int(cloned_id), asyncio.Semaphore(1)
            )
            rl_key_backfill = f"channel:{cloned_id}"
        else:
            sem = None
            rl_key_backfill = webhook_url

        lock = self._thread_locks.setdefault(orig_tid, asyncio.Lock())
        created = False
        clone_thread: discord.Thread | None = None

        def _thread_mapping(thread_id: int) -> dict:
            m = dict(chan_map)
            m["cloned_channel_id"] = int(thread_id)
            return m

        def _merge_embeds_into_payload(dst_payload: dict, src_msg: dict):
            if not dst_payload:
                return
            dst_payload["embeds"] = (dst_payload.get("embeds") or []) + (
                src_msg.get("embeds") or []
            )

        def _coerce_embeds_inplace(p: dict) -> None:
            """Ensure p['embeds'] is list[discord.Embed], converting dict fallbacks."""
            lst = p.get("embeds")
            if not lst:
                p["embeds"] = []
                return
            converted = []
            for e in lst:
                if isinstance(e, discord.Embed):
                    converted.append(e)
                elif isinstance(e, dict):
                    emb = discord.Embed(
                        title=e.get("title"), description=e.get("description")
                    )
                    if isinstance(e.get("image"), dict) and e["image"].get("url"):
                        emb.set_image(url=e["image"]["url"])
                    if isinstance(e.get("thumbnail"), dict) and e["thumbnail"].get(
                        "url"
                    ):
                        emb.set_thumbnail(url=e["thumbnail"]["url"])
                    converted.append(emb)
            p["embeds"] = converted

        async def _send_webhook_into_thread(p: dict, *, include_text: bool, thread_obj):
            """
            Single send into a thread with proper identity + RL + backfill counting.
            Uses a local webhook handle (wh) to avoid rebinding the outer thread_webhook and
            thereby prevents UnboundLocalError when retrying.
            """
            _coerce_embeds_inplace(p)

            try:
                thread_id = getattr(thread_obj, "id", None)
                if thread_id is None and isinstance(thread_obj, int):
                    thread_id = thread_obj
            except Exception:
                thread_id = None

            kw = {
                "content": (p.get("content") if include_text else None),
                "embeds": p.get("embeds"),
                "thread": thread_obj,
                "wait": True,
            }

            if override_identity is not None:
                if override_identity.get("username"):
                    kw["username"] = override_identity["username"]
                if override_identity.get("avatar_url"):
                    kw["avatar_url"] = override_identity["avatar_url"]
            elif not use_webhook_identity and override_identity is None:
                if p.get("username"):
                    kw["username"] = p.get("username")
                if p.get("avatar_url"):
                    kw["avatar_url"] = p.get("avatar_url")

            wh = thread_webhook

            if is_backfill:

                try:
                    st = self._bf_state(int(cloned_id))
                    now = asyncio.get_event_loop().time()
                    wait_s = max(0.0, (st["last"] + self._bf_delay) - now)
                except Exception:
                    wait_s = 0.0
                if wait_s > 0:
                    logger.debug(
                        "%s [bf-gate] waiting %.3fs before send into thread_id=%s (clone_id=%s)",
                        tag,
                        wait_s,
                        thread_id,
                        cloned_id,
                    )
                await self._bf_gate(int(cloned_id))
                logger.debug(
                    "%s [bf-gate] passed for send into thread_id=%s (clone_id=%s)",
                    tag,
                    thread_id,
                    cloned_id,
                )
            else:
                logger.debug(
                    "%s [rl] acquiring WEBHOOK_MESSAGE for key=%s thread_id=%s",
                    tag,
                    rl_key_backfill,
                    thread_id,
                )
                await self.ratelimit.acquire(
                    ActionType.WEBHOOK_MESSAGE, key=rl_key_backfill
                )
                logger.debug(
                    "%s [rl] acquired WEBHOOK_MESSAGE for key=%s thread_id=%s",
                    tag,
                    rl_key_backfill,
                    thread_id,
                )
            try:
                sent_msg = await wh.send(**kw)
            except NotFound:

                logger.warning(
                    "%s [webhook] NotFound on send; rotating webhook and retrying (thread_id=%s, url=%s)",
                    tag,
                    thread_id,
                    getattr(wh, "url", None),
                )
                try:
                    forced_url_local = data.get("__force_webhook_url__")
                    if forced_url_local:
                        try:
                            self.backfill.invalidate_rotation(int(cloned_id))
                        except Exception:
                            pass
                        mapping = self.chan_map.get(parent_id) or {}
                        primary = mapping.get("channel_webhook_url") or mapping.get(
                            "webhook_url"
                        )
                        url2, _ = await self.backfill.pick_url_for_send(
                            int(cloned_id), primary_url=primary, create_missing=True
                        )
                    else:
                        url2 = await self._ensure_primary_webhook_url(parent_id)

                    wh = Webhook.from_url(url2, session=self.session)
                    logger.info(
                        "%s [webhook] retrying send with new webhook url (thread_id=%s)",
                        tag,
                        thread_id,
                    )
                    sent_msg = await wh.send(**kw)
                except Exception:
                    logger.exception(
                        "%s [webhook] retry failed (thread_id=%s)", tag, thread_id
                    )
                    raise

            try:
                orig_gid = int(data.get("guild_id") or 0)
                orig_cid = int(data.get("thread_id") or data.get("channel_id") or 0)
                orig_mid = int(data.get("message_id") or 0)
                cloned_cid = int(getattr(thread_obj, "id", 0)) if thread_obj else 0
                cloned_mid = int(getattr(sent_msg, "id", 0)) if sent_msg else None
                used_url = getattr(wh, "url", None)

                self.db.upsert_message_mapping(
                    original_guild_id=orig_gid,
                    original_channel_id=orig_cid,
                    original_message_id=orig_mid,
                    cloned_channel_id=cloned_cid,
                    cloned_message_id=cloned_mid,
                    webhook_url=used_url,
                    cloned_guild_id=guild.id,
                )
                logger.debug(
                    "[map] upserted thread message map orig_tid=%s→clone_tid=%s orig_mid=%s→clone_mid=%s",
                    orig_cid,
                    cloned_cid,
                    orig_mid,
                    cloned_mid,
                )
            except Exception:
                logger.exception("upsert_message_mapping failed (thread)")

        try:
            async with lock:

                thr_map = next(
                    (
                        r
                        for r in self.db.get_all_threads()
                        if int(r["original_thread_id"]) == orig_tid
                    ),
                    None,
                )

                if thr_map:
                    thr_map = dict(thr_map)
                    try:
                        clone_thread = guild.get_channel(
                            int(thr_map["cloned_thread_id"])
                        ) or await self.bot.fetch_channel(
                            int(thr_map["cloned_thread_id"])
                        )
                    except HTTPException as e:
                        if e.status == 404:
                            self.db.delete_forum_thread_mapping(orig_tid)
                            thr_map = None
                            clone_thread = None
                        else:
                            logger.warning(
                                "[⌛]%s Error fetching thread %s; queueing for next sync",
                                tag,
                                thr_map.get("cloned_thread_id"),
                            )
                            self._pending_thread_msgs.append(data)
                            return

                if thr_map is None:

                    async def _create_forum_thread_and_first_post():

                        tmp = {
                            "content": (payload.get("content") or None),
                            "embeds": payload.get("embeds"),
                            "username": payload.get("username"),
                            "avatar_url": payload.get("avatar_url"),
                        }

                        if stickers and has_textish:
                            if has_custom:
                                data["__stickers_no_text__"] = True
                                data["__stickers_prefer_embeds__"] = True
                                await self.stickers.send_with_fallback(
                                    receiver=self,
                                    ch=None,
                                    stickers=stickers,
                                    mapping=chan_map,
                                    msg=data,
                                    source_id=orig_tid,
                                )
                                _merge_embeds_into_payload(tmp, data)
                            elif has_standard:
                                tmp["content"] = "\u200b"
                                tmp["embeds"] = None
                        elif stickers and not has_textish:
                            if has_custom:
                                data["__stickers_no_text__"] = True
                                data["__stickers_prefer_embeds__"] = True
                                await self.stickers.send_with_fallback(
                                    receiver=self,
                                    ch=None,
                                    stickers=stickers,
                                    mapping=chan_map,
                                    msg=data,
                                    source_id=orig_tid,
                                )
                                _merge_embeds_into_payload(tmp, data)
                            elif has_standard:
                                tmp["content"] = "\u200b"
                                tmp["embeds"] = None

                        _coerce_embeds_inplace(tmp)

                        async def _try_resolve_thread_from_message(
                            msg, tries=6, delay=0.2
                        ):
                            t = None
                            last_err = None
                            for _ in range(tries):
                                try:

                                    ch = getattr(msg, "channel", None)
                                    if isinstance(ch, discord.Thread):
                                        return ch

                                    ch_id = getattr(msg, "channel_id", None)
                                    if ch_id:
                                        t = guild.get_channel(
                                            int(ch_id)
                                        ) or await self.bot.fetch_channel(int(ch_id))
                                        if isinstance(t, discord.Thread):
                                            return t

                                    j = getattr(msg, "jump_url", "") or ""
                                    if j:
                                        parts = j.strip("/").split("/")
                                        if len(parts) >= 3:
                                            maybe_cid = parts[-2]
                                            if maybe_cid.isdigit():
                                                t = guild.get_channel(
                                                    int(maybe_cid)
                                                ) or await self.bot.fetch_channel(
                                                    int(maybe_cid)
                                                )
                                                if isinstance(t, discord.Thread):
                                                    return t

                                    act = await cloned_parent.fetch_active_threads()
                                    t = next(
                                        (
                                            th
                                            for th in act.threads
                                            if th.name == data["thread_name"]
                                        ),
                                        None,
                                    )
                                    if isinstance(t, discord.Thread):
                                        return t

                                    try:
                                        arch = (
                                            await cloned_parent.fetch_archived_threads(
                                                limit=50
                                            )
                                        )
                                        t = next(
                                            (
                                                th
                                                for th in arch.threads
                                                if th.name == data["thread_name"]
                                            ),
                                            None,
                                        )
                                        if isinstance(t, discord.Thread):
                                            return t
                                    except Exception:
                                        pass

                                except Exception as e:
                                    last_err = e

                                await asyncio.sleep(delay)

                            if last_err:
                                logger.debug(
                                    "[🧵] resolve retries exhausted with last_err=%r",
                                    last_err,
                                )
                            return None

                        if sem:
                            async with sem:
                                if is_backfill:
                                    await self._bf_gate(int(cloned_id))
                                uname = None
                                av = None
                                if override_identity is not None:
                                    uname = override_identity.get("username")
                                    av = override_identity.get("avatar_url")
                                elif not use_webhook_identity:
                                    uname = tmp.get("username")
                                    av = tmp.get("avatar_url")

                                sent_msg = await thread_webhook.send(
                                    content=(tmp.get("content") or None),
                                    embeds=tmp.get("embeds"),
                                    username=uname,
                                    avatar_url=av,
                                    thread_name=data["thread_name"],
                                    wait=True,
                                )

                        else:
                            uname = None
                            av = None
                            if override_identity is not None:
                                uname = override_identity.get("username")
                                av = override_identity.get("avatar_url")
                            elif not use_webhook_identity:
                                uname = tmp.get("username")
                                av = tmp.get("avatar_url")
                            if is_backfill:
                                await self._bf_gate(int(cloned_id))

                            sent_msg = await thread_webhook.send(
                                content=(tmp.get("content") or None),
                                embeds=tmp.get("embeds"),
                                username=uname,
                                avatar_url=av,
                                thread_name=data["thread_name"],
                                wait=True,
                            )

                        t = await _try_resolve_thread_from_message(
                            sent_msg, tries=6, delay=0.2
                        )

                        if not t:
                            logger.warning(
                                "[🧵]%s Created forum thread '%s' but couldn't resolve thread object yet; will retry later.",
                                tag,
                                data["thread_name"],
                            )
                            if is_backfill and hasattr(self, "backfill"):
                                self.backfill.note_checkpoint(
                                    parent_id,
                                    int(data["message_id"]),
                                    data.get("timestamp"),
                                )
                            return None

                        logger.info(
                            "[🧵]%s Created forum thread '%s' → cloned_thread_id=%s in #%s",
                            tag,
                            data["thread_name"],
                            t.id,
                            getattr(cloned_parent, "name", cloned_id),
                        )

                        if (
                            is_backfill
                            and hasattr(self, "backfill")
                            and not data.get("__firstpost_counted__")
                        ):
                            self.backfill.note_sent(parent_id, int(data["message_id"]))
                            self.backfill.note_checkpoint(
                                parent_id,
                                int(data["message_id"]),
                                data.get("timestamp"),
                            )
                            data["__firstpost_counted__"] = True
                            logger.info(
                                "[💬]%s Forwarding message to thread '%s' in #%s from %s (%s)%s",
                                tag,
                                data["thread_name"],
                                getattr(cloned_parent, "name", cloned_id),
                                data["author"],
                                data["author_id"],
                                _bf_suffix(),
                            )

                        try:
                            await t.edit(auto_archive_duration=60)
                        except Exception:
                            logger.debug(
                                "[🧵] could not set auto_archive_duration for thread_id=%s",
                                t.id,
                            )

                        if is_backfill and hasattr(self, "backfill"):
                            self.backfill.note_checkpoint(
                                parent_id,
                                int(data["message_id"]),
                                data.get("timestamp"),
                            )

                        return t

                    async def _create_text_thread():

                        if sem:
                            async with sem:
                                if is_backfill:
                                    await self._bf_gate(int(cloned_id))
                                new_thread = await cloned_parent.create_thread(
                                    name=data["thread_name"],
                                    type=ChannelType.public_thread,
                                    auto_archive_duration=60,
                                )
                        else:
                            if is_backfill:
                                await self._bf_gate(int(cloned_id))
                            new_thread = await cloned_parent.create_thread(
                                name=data["thread_name"],
                                type=ChannelType.public_thread,
                                auto_archive_duration=60,
                            )
                        if is_backfill and hasattr(self, "backfill"):
                            self.backfill.add_expected_total(parent_id, 1)
                            self.backfill.note_sent(parent_id, None)
                            self.backfill.note_checkpoint(
                                parent_id,
                                int(data["message_id"]),
                                data.get("timestamp"),
                            )

                        logger.info(
                            "[🧵]%s Created text thread '%s' → cloned_thread_id=%s in #%s",
                            tag,
                            data["thread_name"],
                            new_thread.id,
                            getattr(cloned_parent, "name", cloned_id),
                        )

                        return new_thread

                    meta = await self._get_webhook_meta(parent_id, webhook_url)

                    if not is_backfill:
                        await self.ratelimit.acquire_for_guild(
                            ActionType.THREAD, guild.id
                        )

                    if isinstance(cloned_parent, ForumChannel):
                        clone_thread = await _create_forum_thread_and_first_post()

                        if not clone_thread:
                            return
                        new_id = clone_thread.id

                        if stickers and has_standard:
                            sent = await self.stickers.send_with_fallback(
                                receiver=self,
                                ch=clone_thread,
                                stickers=stickers,
                                mapping=_thread_mapping(new_id),
                                msg=data,
                                source_id=orig_tid,
                            )
                            if sent:
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                                self.db.upsert_forum_thread_mapping(
                                    orig_thread_id=orig_tid,
                                    orig_thread_name=data["thread_name"],
                                    clone_thread_id=new_id,
                                    forum_orig_id=parent_id,
                                    forum_clone_id=cloned_id,
                                    original_guild_id=int(data.get("guild_id") or 0),
                                    cloned_guild_id=guild.id,
                                )
                                created = True
                            else:

                                payload2 = self._build_webhook_payload(data)
                                if meta.get("custom") or use_webhook_identity:
                                    payload2.pop("username", None)
                                    payload2.pop("avatar_url", None)
                                if not has_custom:
                                    payload2.setdefault(
                                        "content", payload2.get("content")
                                    )
                                if sem:
                                    async with sem:
                                        await _send_webhook_into_thread(
                                            payload2,
                                            include_text=True,
                                            thread_obj=clone_thread,
                                        )
                                else:
                                    await _send_webhook_into_thread(
                                        payload2,
                                        include_text=True,
                                        thread_obj=clone_thread,
                                    )
                                created = True

                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )

                    else:
                        clone_thread = await _create_text_thread()
                        new_id = clone_thread.id

                        async def _send_text_thread_followup(p):
                            if sem:
                                async with sem:
                                    await _send_webhook_into_thread(
                                        p, include_text=True, thread_obj=clone_thread
                                    )
                                    if is_backfill and hasattr(self, "backfill"):

                                        self.backfill.note_sent(
                                            parent_id, int(data["message_id"])
                                        )
                                        self.backfill.note_checkpoint(
                                            parent_id,
                                            int(data["message_id"]),
                                            data.get("timestamp"),
                                        )

                                        logger.info(
                                            "[💬]%s Forwarding message to thread '%s' in #%s from %s (%s)%s",
                                            tag,
                                            data["thread_name"],
                                            data.get("thread_parent_name"),
                                            data["author"],
                                            data["author_id"],
                                            _bf_suffix(),
                                        )
                            else:
                                await _send_webhook_into_thread(
                                    p, include_text=True, thread_obj=clone_thread
                                )
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                                    logger.info(
                                        "[💬]%s Forwarding message to thread '%s' in #%s from %s (%s)%s",
                                        tag,
                                        data["thread_name"],
                                        data.get("thread_parent_name"),
                                        data["author"],
                                        data["author_id"],
                                        _bf_suffix(),
                                    )

                        if stickers and has_textish:
                            if has_custom:
                                data["__stickers_no_text__"] = True
                                data["__stickers_prefer_embeds__"] = True
                                await self.stickers.send_with_fallback(
                                    receiver=self,
                                    ch=clone_thread,
                                    stickers=stickers,
                                    mapping=_thread_mapping(new_id),
                                    msg=data,
                                    source_id=orig_tid,
                                )
                                _merge_embeds_into_payload(payload, data)
                                await _send_text_thread_followup(payload)
                            elif has_standard:
                                data.pop("__stickers_no_text__", None)
                                data.pop("__stickers_prefer_embeds__", None)
                                sent = await self.stickers.send_with_fallback(
                                    receiver=self,
                                    ch=clone_thread,
                                    stickers=stickers,
                                    mapping=_thread_mapping(new_id),
                                    msg=data,
                                    source_id=orig_tid,
                                )
                                if not sent:
                                    _merge_embeds_into_payload(payload, data)
                                    await _send_text_thread_followup(payload)

                        elif stickers and not has_textish:
                            if has_custom:
                                data["__stickers_no_text__"] = True
                                data["__stickers_prefer_embeds__"] = True
                                await self.stickers.send_with_fallback(
                                    receiver=self,
                                    ch=clone_thread,
                                    stickers=stickers,
                                    mapping=_thread_mapping(new_id),
                                    msg=data,
                                    source_id=orig_tid,
                                )
                                payload2 = self._build_webhook_payload(data)
                                if meta.get("custom") or use_webhook_identity:
                                    payload2.pop("username", None)
                                    payload2.pop("avatar_url", None)
                                await _send_text_thread_followup(payload2)
                            else:
                                sent = await self.stickers.send_with_fallback(
                                    receiver=self,
                                    ch=clone_thread,
                                    stickers=stickers,
                                    mapping=_thread_mapping(new_id),
                                    msg=data,
                                    source_id=orig_tid,
                                )
                                if not sent:
                                    payload2 = self._build_webhook_payload(data)
                                    if meta.get("custom") or use_webhook_identity:
                                        payload2.pop("username", None)
                                        payload2.pop("avatar_url", None)
                                    await _send_text_thread_followup(payload2)
                        else:
                            await _send_text_thread_followup(payload)

                    created = True

                    self.db.upsert_forum_thread_mapping(
                        orig_thread_id=orig_tid,
                        orig_thread_name=data["thread_name"],
                        clone_thread_id=new_id,
                        forum_orig_id=parent_id,
                        forum_clone_id=cloned_id,
                        original_guild_id=int(data.get("guild_id") or 0),
                        cloned_guild_id=guild.id,
                    )

                if not created:

                    if stickers and not has_textish:
                        if has_custom:
                            data["__stickers_no_text__"] = True
                            data["__stickers_prefer_embeds__"] = True
                            _ = await self.stickers.send_with_fallback(
                                receiver=self,
                                ch=clone_thread,
                                stickers=stickers,
                                mapping=_thread_mapping(clone_thread.id),
                                msg=data,
                                source_id=orig_tid,
                            )
                            payload2 = self._build_webhook_payload(data)
                            if meta.get("custom") or use_webhook_identity:
                                payload2.pop("username", None)
                                payload2.pop("avatar_url", None)
                            if sem:
                                async with sem:
                                    await _send_webhook_into_thread(
                                        payload2,
                                        include_text=True,
                                        thread_obj=clone_thread,
                                    )
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                            else:
                                await _send_webhook_into_thread(
                                    payload2, include_text=True, thread_obj=clone_thread
                                )
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                            return
                        else:
                            sent = await self.stickers.send_with_fallback(
                                receiver=self,
                                ch=clone_thread,
                                stickers=stickers,
                                mapping=_thread_mapping(clone_thread.id),
                                msg=data,
                                source_id=orig_tid,
                            )
                            if sent:
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                                return
                            payload2 = self._build_webhook_payload(data)
                            if meta.get("custom") or use_webhook_identity:
                                payload2.pop("username", None)
                                payload2.pop("avatar_url", None)
                            if sem:
                                async with sem:
                                    await _send_webhook_into_thread(
                                        payload2,
                                        include_text=True,
                                        thread_obj=clone_thread,
                                    )
                                    if is_backfill and hasattr(self, "backfill"):
                                        self.backfill.note_sent(
                                            parent_id, int(data["message_id"])
                                        )
                                        self.backfill.note_checkpoint(
                                            parent_id,
                                            int(data["message_id"]),
                                            data.get("timestamp"),
                                        )
                            else:
                                await _send_webhook_into_thread(
                                    payload2, include_text=True, thread_obj=clone_thread
                                )
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                            return

                    if stickers and has_textish:
                        if has_custom:
                            data["__stickers_no_text__"] = True
                            data["__stickers_prefer_embeds__"] = True
                            _ = await self.stickers.send_with_fallback(
                                receiver=self,
                                ch=clone_thread,
                                stickers=stickers,
                                mapping=_thread_mapping(clone_thread.id),
                                msg=data,
                                source_id=orig_tid,
                            )
                            _merge_embeds_into_payload(payload, data)
                            if sem:
                                async with sem:
                                    await _send_webhook_into_thread(
                                        payload,
                                        include_text=True,
                                        thread_obj=clone_thread,
                                    )
                                    if is_backfill and hasattr(self, "backfill"):
                                        self.backfill.note_sent(
                                            parent_id, int(data["message_id"])
                                        )
                                        self.backfill.note_checkpoint(
                                            parent_id,
                                            int(data["message_id"]),
                                            data.get("timestamp"),
                                        )
                            else:
                                await _send_webhook_into_thread(
                                    payload, include_text=True, thread_obj=clone_thread
                                )
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                            return
                        elif has_standard:
                            data.pop("__stickers_no_text__", None)
                            data.pop("__stickers_prefer_embeds__", None)
                            sent = await self.stickers.send_with_fallback(
                                receiver=self,
                                ch=clone_thread,
                                stickers=stickers,
                                mapping=_thread_mapping(clone_thread.id),
                                msg=data,
                                source_id=orig_tid,
                            )
                            if sent:
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                                return
                            _merge_embeds_into_payload(payload, data)
                            if sem:
                                async with sem:
                                    await _send_webhook_into_thread(
                                        payload,
                                        include_text=True,
                                        thread_obj=clone_thread,
                                    )
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                            else:
                                await _send_webhook_into_thread(
                                    payload, include_text=True, thread_obj=clone_thread
                                )
                                if is_backfill and hasattr(self, "backfill"):
                                    self.backfill.note_sent(
                                        parent_id, int(data["message_id"])
                                    )
                                    self.backfill.note_checkpoint(
                                        parent_id,
                                        int(data["message_id"]),
                                        data.get("timestamp"),
                                    )
                            return

                    if has_textish:

                        if sem:
                            async with sem:
                                await _send_webhook_into_thread(
                                    payload, include_text=True, thread_obj=clone_thread
                                )
                        else:
                            await _send_webhook_into_thread(
                                payload, include_text=True, thread_obj=clone_thread
                            )

                        if is_backfill and hasattr(self, "backfill"):
                            self.backfill.note_sent(parent_id, int(data["message_id"]))
                            self.backfill.note_checkpoint(
                                parent_id,
                                int(data["message_id"]),
                                data.get("timestamp"),
                            )

                        logger.info(
                            "[💬]%s Forwarding message to thread '%s' in #%s from %s (%s)%s",
                            tag,
                            data["thread_name"],
                            data.get("thread_parent_name"),
                            data["author"],
                            data["author_id"],
                            _bf_suffix(),
                        )

        finally:
            try:
                await self._enforce_thread_limit(guild)
            except Exception:
                logger.exception("Error enforcing thread limit.")

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

    async def _handle_backfill_thread_message(self, data: dict) -> None:
        if self._shutting_down:
            return

        try:
            parent_id = int(data["thread_parent_id"])
        except Exception:
            logger.warning(
                "[bf] bad thread_parent_id in payload: %r", data.get("thread_parent_id")
            )
            return

        row = None
        if hasattr(self.db, "get_channel_mapping_by_original_id"):
            row = self.db.get_channel_mapping_by_original_id(parent_id)
        if not row and hasattr(self.db, "get_channel_mapping_by_clone_id"):
            row = self.db.get_channel_mapping_by_clone_id(parent_id)
            if row:
                try:
                    parent_id = int(row["original_channel_id"])
                except Exception:
                    pass

        if not row:
            logger.warning("[bf] no mapping for parent=%s; cannot rotate", parent_id)

            await self.handle_thread_message(data)
            return

        try:
            row = dict(row)
        except Exception:
            logger.error(
                "[bf] mapping row not dict-like: type=%r row=%r", type(row), row
            )
            await self.handle_thread_message(data)
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
                "[bf] mapping found but no webhook URL | parent=%s clone=%s row=%s",
                parent_id,
                clone_id,
                row,
            )
            await self.handle_thread_message(data)
            return

        st = self.backfill._progress.get(int(parent_id))
        if not st:
            self.backfill.register_sink(
                parent_id, user_id=None, clone_channel_id=clone_id, msg=None
            )
            logger.debug(
                "[bf] sink registered | parent=%s clone=%s", parent_id, clone_id
            )
        else:
            if clone_id and st.get("clone_channel_id") != clone_id:
                st["clone_channel_id"] = clone_id
            if clone_id:
                self.backfill._by_clone[clone_id] = int(parent_id)

        try:
            if clone_id:
                await self.backfill.ensure_temps_ready(clone_id)
        except Exception as e:
            logger.debug(
                "[bf] ensure_temps_ready failed | clone=%s err=%s", clone_id, e
            )

        try:
            url, _used_pool = await self.backfill.pick_url_for_send(
                clone_channel_id=clone_id or 0,
                primary_url=primary_url,
                create_missing=True,
            )
        except Exception as e:
            logger.warning("[bf] rotation failed, using primary | err=%s", e)
            url = primary_url

        forced = dict(data)
        forced["__force_webhook_url__"] = url

        await self.handle_thread_message(forced)

    def _prune_old_messages_loop(
        self, retention_seconds: int | None = None
    ) -> asyncio.Task:
        """
        Start hourly task that deletes old rows from the `messages` table.
        """

        if getattr(self, "_prune_task", None) and not self._prune_task.done():
            return self._prune_task

        if retention_seconds is None:
            env_sec = os.getenv("MESSAGE_RETENTION_SECONDS")
            env_days = os.getenv("MESSAGE_RETENTION_DAYS")
            if env_sec and env_sec.isdigit():
                retention_seconds = int(env_sec)
            elif env_days and env_days.isdigit():
                retention_seconds = int(env_days) * 24 * 3600
            else:
                retention_seconds = 7 * 24 * 3600

        try:
            with self.db.lock, self.db.conn:
                self.db.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);"
                )
        except Exception:
            logger.exception("[prune] failed to ensure idx_messages_created_at")

        async def _runner():
            logger.debug(
                "[prune] starting hourly pruner (retention=%d seconds ~= %.2f days)",
                retention_seconds,
                retention_seconds / 86400.0,
            )
            try:
                while True:
                    try:
                        deleted = self.db.delete_old_messages(retention_seconds)
                        if deleted:
                            logger.info(
                                "[prune] deleted %d old message mappings from db.",
                                deleted,
                            )

                    except Exception:
                        logger.exception("[prune] delete_old_messages failed")

                    await asyncio.sleep(60 * 60)
            except asyncio.CancelledError:
                raise

        self._prune_task = asyncio.create_task(_runner(), name="prune-old-messages")
        return self._prune_task

    async def _delete_with_row(
        self, row, orig_mid: int, channel_name: str | None = None
    ) -> bool:
        try:
            cloned_mid = int(row["cloned_message_id"])
            webhook_url = row["webhook_url"]
        except Exception:
            logger.debug(
                "[🗑️] Mapping incomplete for orig %s; nothing to delete", orig_mid
            )
            return False

        if not (cloned_mid and webhook_url):
            logger.debug(
                "[🗑️] Missing cloned_mid/webhook for orig %s; nothing to delete",
                orig_mid,
            )
            return False

        try:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()
            wh = Webhook.from_url(webhook_url, session=self.session)
            await wh.delete_message(cloned_mid)
            logger.info(
                "[🗑️] Deleted cloned msg %s (orig %s) in #%s",
                cloned_mid,
                orig_mid,
                channel_name,
            )
        except NotFound:

            logger.info(
                "[🗑️] Cloned msg already gone (orig %s) in #%s; treating as deleted",
                orig_mid,
                channel_name,
            )
        except Exception as e:
            logger.warning(
                "[⚠️] Failed to delete cloned msg for orig %s: %s", orig_mid, e
            )
            return False

        try:
            self.db.delete_message_mapping(orig_mid)
        except Exception:
            logger.debug(
                "Could not delete mapping row for orig %s", orig_mid, exc_info=True
            )
        return True

    async def handle_message_delete(self, data: dict):
        """
        Delete the cloned webhook message that corresponds to the original one.
        If the original send is still in-flight (rate limited / queued), queue the delete,
        wait briefly for the mapping, and apply it once ready.
        """
        try:
            orig_mid = int(data.get("message_id") or 0)
        except Exception:
            return
        if not orig_mid:
            return

        channel_name = data.get("channel_name")

        row = None
        try:
            row = self.db.get_mapping_by_original(orig_mid)
        except Exception:
            row = None

        if row is not None:
            await self._delete_with_row(row, orig_mid, channel_name)
            return

        ev = self._inflight_events.get(orig_mid)
        if ev and not ev.is_set():

            self._pending_deletes.add(orig_mid)

            try:
                await asyncio.wait_for(ev.wait(), timeout=7.0)
            except asyncio.TimeoutError:

                logger.debug(
                    "[🕒] Delete queued; mapping not ready yet for orig %s", orig_mid
                )
                return

            try:
                row = self.db.get_mapping_by_original(orig_mid)
            except Exception:
                row = None

            if row is not None:
                await self._delete_with_row(row, orig_mid, channel_name)
                self._pending_deletes.discard(orig_mid)

                self._inflight_events.pop(orig_mid, None)
                return

            logger.debug(
                "[🕒] Delete remains queued; mapping still missing for orig %s",
                orig_mid,
            )
            return

        row = await self._get_mapping_with_retry(
            orig_mid,
            attempts=5,
            base_delay=0.08,
            max_delay=0.8,
            jitter=0.25,
            log_prefix="delete-wait",
        )
        if row is not None:
            await self._delete_with_row(row, orig_mid, channel_name)
            return

        self._pending_deletes.add(orig_mid)
        logger.debug(
            "[🕒] Delete queued with no mapping/in-flight info for orig %s", orig_mid
        )

    def _pick_verify_guild_id(self) -> int | None:
        """
        Prefer a mapped clone guild the bot is actually in.
        Fallbacks:
        - any mapped clone guild id from DB
        - legacy config.CLONE_GUILD_ID
        - any guild the bot is in
        - None
        """
        try:
            mapped = {int(x) for x in (self.db.get_all_clone_guild_ids() or [])}
        except Exception:
            mapped = set()

        for g in self.bot.guilds or []:
            if mapped and int(g.id) in mapped:
                return int(g.id)

        if mapped:
            return int(next(iter(mapped)))

        if getattr(self.bot, "guilds", None):
            return int(self.bot.guilds[0].id)

        return None

    def _maybe_enrich_mapping_identity_from_sitemap(self, sitemap: dict) -> None:
        """
        Backfill identity fields in guild_mappings (names/icon URLs) using data
        we already have at sync time.
        """
        guild_info = sitemap.get("guild") or {}
        try:
            host_gid = int(guild_info.get("id") or 0)
        except (TypeError, ValueError):
            host_gid = 0

        if not host_gid:
            return  # can't map without a source guild id

        mrow = self.db.get_mapping_by_original(host_gid)
        if not mrow:

            return

        # See what's missing.
        need_orig_name = not str(mrow.get("original_guild_name") or "").strip()
        need_orig_icon = not str(mrow.get("original_guild_icon_url") or "").strip()
        need_clone_name = not str(mrow.get("cloned_guild_name") or "").strip()

        if not (need_orig_name or need_orig_icon or need_clone_name):

            return

        sitemap_orig_name = str(guild_info.get("name") or "").strip()
        sitemap_icon_url = str(guild_info.get("icon") or "").strip()

        clone_gid = mrow.get("cloned_guild_id")
        clone_guild_name = None
        if need_clone_name and clone_gid:
            try:
                clone_gid_int = int(clone_gid)
            except (TypeError, ValueError):
                clone_gid_int = 0
            if clone_gid_int:
                clone_guild_obj = self.bot.get_guild(clone_gid_int)
                if clone_guild_obj:
                    clone_guild_name = getattr(clone_guild_obj, "name", None)

        # fall back to existing if we didn't discover something new).
        final_orig_name = (
            sitemap_orig_name
            if (need_orig_name and sitemap_orig_name)
            else mrow.get("original_guild_name") or ""
        )
        final_orig_icon = (
            sitemap_icon_url
            if (need_orig_icon and sitemap_icon_url)
            else mrow.get("original_guild_icon_url") or None
        )
        final_clone_name = (
            clone_guild_name
            if (need_clone_name and clone_guild_name)
            else mrow.get("cloned_guild_name") or ""
        )

        mapping_id = mrow.get("mapping_id")
        mapping_name = mrow.get("mapping_name") or ""
        cloned_gid_val = mrow.get("cloned_guild_id") or 0
        settings_dict = mrow.get("settings") or {}

        try:
            self.db.upsert_guild_mapping(
                mapping_id=mapping_id,
                mapping_name=mapping_name,
                original_guild_id=host_gid,
                original_guild_name=final_orig_name,
                original_guild_icon_url=final_orig_icon,
                cloned_guild_id=cloned_gid_val,
                cloned_guild_name=final_clone_name,
                settings=settings_dict,
                overwrite_identity=True,
            )
            logger.debug(
                "[guild-id-meta] enriched mapping %s: orig_name=%r icon=%r clone_name=%r",
                mapping_id,
                final_orig_name,
                final_orig_icon,
                final_clone_name,
            )
        except Exception:
            logger.exception(
                "[guild-id-meta] failed to enrich mapping for origin guild %s", host_gid
            )

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
        await _cancel_and_wait(getattr(self, "_prune_task", None), "prune-old-messages")

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
