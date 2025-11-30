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
import random
from typing import List, Optional, Set, Tuple, Dict, Union, Coroutine, Any
import unicodedata
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
    HTTPException,
)
from discord.errors import HTTPException, Forbidden, NotFound
import os
import sys
import hashlib
import time
from datetime import datetime, timezone
from asyncio import Queue
from pathlib import Path
from dotenv import load_dotenv
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
    _is_image_att,
    _calc_text_len_with_urls,
    _safe_mid,
    _anonymize_user,
)
from server.permission_sync import ChannelPermissionSync
from server.guild_resolver import GuildResolver
from server import logctx

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DATA_DIR = os.getenv("DATA_DIR")
LOG_DIR = os.getenv("LOG_DIR") or (DATA_DIR if DATA_DIR else "/data")
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


class _GuildPrefixFilter(logging.Filter):
    """
    Prepend mapping name to every server log line.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            prefix = logctx.guild_prefix()
        except Exception:
            prefix = ""

        if prefix and not getattr(record, "_guild_prefix_injected", False):
            record.msg = prefix + str(record.msg)
            record._guild_prefix_injected = True
        return True


logger.addFilter(_GuildPrefixFilter())

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
        self.bot.event(self.on_ready)
        self.bot.event(self.on_webhooks_update)
        self.bot.event(self.on_guild_channel_delete)
        self.bot.event(self.on_member_join)
        self._blocked_keywords_cache: dict[
            tuple[int, int], list[tuple[re.Pattern, str]]
        ] = {}
        self._blocked_keywords_lock = asyncio.Lock()
        self._user_filters_cache: dict[tuple[int, int], dict[str, set[int]]] = {}
        self._user_filters_lock = asyncio.Lock()
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
        self.chan_map_by_clone: dict[int, dict[int, dict]] = {}
        self.cat_map_by_clone: dict[int, dict[int, dict]] = {}
        self._unmapped_warned: set[int] = set()
        self._unmapped_threads_warned: set[int] = set()
        self._webhooks: dict[str, Webhook] = {}
        self._warn_lock = asyncio.Lock()
        self._webhook_gate_by_clone: dict[int, asyncio.Lock] = {}
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
        self._M_ROLE = re.compile(r"<@&(?P<id>\d+)>")
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
            logger=logger.getChild("perm-sync"),
            ratelimit=self.ratelimit,
            rate_limiter_action=ActionType.EDIT_CHANNEL,
        )
        self.onjoin = OnJoinService(self.bot, self.db, logger.getChild("OnJoin"))
        install_discord_rl_probe(self.ratelimit)

        self.MAX_GUILD_CHANNELS = 500
        self.MAX_CATEGORIES = 50
        self.MAX_CHANNELS_PER_CATEGORY = 50

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

    def _rowdict(self, row):
        """sqlite3.Row → dict; dict stays dict; None stays None."""
        if row is None:
            return None
        return row if isinstance(row, dict) else dict(row)

    async def _get_mappings_with_retry(
        self,
        original_message_id: int,
        *,
        attempts: int = 5,
        base_delay: float = 0.08,
        max_delay: float = 0.8,
        jitter: float = 0.25,
        log_prefix: str = "map-wait",
    ) -> list[dict]:
        """
        Poll the DB for *all* message->clone mappings for a given original message id.
        Returns a list of plain dict rows (possibly empty).
        """
        try:
            mid = int(original_message_id)
        except Exception:
            return []

        delay = float(base_delay)
        for i in range(max(1, int(attempts))):
            try:
                rows = self.db.get_message_mappings_for_original(mid) or []
            except Exception:
                rows = []

            if rows:

                return [r if isinstance(r, dict) else dict(r) for r in rows]

            if i < attempts - 1:

                try:
                    import random as _rnd

                    j = (2.0 * _rnd.random() - 1.0) * float(jitter)
                except Exception:
                    j = 0.0
                await asyncio.sleep(max(0.0, delay + j))
                delay = min(delay * 1.6, max_delay)

        logger.debug(
            "[%s] No mappings found for orig_mid=%s after %s attempts",
            log_prefix,
            mid,
            attempts,
        )
        return []

    def _get_webhook_gate(self, gid: int) -> asyncio.Lock:
        lock = self._webhook_gate_by_clone.get(int(gid))
        if lock is None:
            lock = asyncio.Lock()
            self._webhook_gate_by_clone[int(gid)] = lock
        return lock

    @contextlib.contextmanager
    def _clone_log_label(self, clone_gid: int):
        """
        Temporarily set the log prefix to the clone mapping's label (e.g. 'Server B').
        Always resets after the wrapped block.
        """
        try:
            label = self._label_for_clone_gid(int(clone_gid))
        except Exception:
            label = f"guild:{clone_gid}"
        token = logctx.guild_name.set(label)
        try:
            yield
        finally:
            logctx.guild_name.reset(token)

    def _label_for_clone_gid(self, clone_gid: int) -> str:
        """
        Prefer mapping_name for the clone; fall back to the clone guild's Discord name;
        last resort: 'guild:<id>'.
        """
        try:
            row = self.db.get_mapping_by_clone(int(clone_gid))
            if row:
                nm = (row.get("mapping_name") or "").strip()
                if nm:
                    return nm
        except Exception:
            pass

        try:
            g = self.bot.get_guild(int(clone_gid))
            if g and getattr(g, "name", None):
                return g.name.strip()
        except Exception:
            pass

        return f"guild:{clone_gid}"

    def _resolve_guild_label(self, typ: str, data: dict) -> str | None:
        """
        Best-effort human-readable label for logging.
        Prefer per-mapping 'mapping_name' (clone-aware), then Discord name, then "guild:<id>".
        """
        gid = 0
        label = ""

        if typ == "sitemap":
            ginfo = (data or {}).get("guild") or {}
            tgt = (data or {}).get("target") or {}
            raw_host = ginfo.get("id")
            raw_clone = tgt.get("cloned_guild_id")

            try:
                clone_gid = int(raw_clone or 0)
            except Exception:
                clone_gid = 0
            try:
                host_gid = int(raw_host or 0)
            except Exception:
                host_gid = 0

            if clone_gid:
                with contextlib.suppress(Exception):
                    mrow_clone = self.db.get_mapping_by_clone(clone_gid)
                    if mrow_clone:
                        nm = (mrow_clone.get("mapping_name") or "").strip()
                        if nm:
                            self._host_name_cache[host_gid or clone_gid] = nm
                            return nm
                        cg = self.bot.get_guild(clone_gid)
                        if cg and getattr(cg, "name", None):
                            nm2 = cg.name.strip()
                            if nm2:
                                self._host_name_cache[host_gid or clone_gid] = nm2
                                return nm2

            gid = host_gid
            name = (ginfo.get("name") or "").strip()

            if gid:
                with contextlib.suppress(Exception):
                    mrow_host = self.db.get_mapping_by_original(gid)
                    if mrow_host:
                        nm = (mrow_host.get("mapping_name") or "").strip()
                        if nm:
                            self._host_name_cache[gid] = nm
                            return nm

            if name:
                self._host_name_cache[gid] = name
                return name

            g_obj = self.bot.get_guild(gid)
            if g_obj and getattr(g_obj, "name", None):
                nm = g_obj.name.strip()
                self._host_name_cache[gid] = nm
                return nm

            return f"guild:{gid}" if gid else None

        raw_gid = (data or {}).get("guild_id")
        try:
            gid = int(raw_gid or 0)
        except Exception:
            gid = 0

        if gid:
            with contextlib.suppress(Exception):
                mrow = self.db.get_mapping_by_original(gid)
                if mrow:
                    nm = (mrow.get("mapping_name") or "").strip()
                    if nm:
                        return nm

            with contextlib.suppress(Exception):
                mrow2 = self.db.get_mapping_by_clone(gid)
                if mrow2:
                    nm = (mrow2.get("mapping_name") or "").strip()
                    if nm:
                        return nm

            cached = (self._host_name_cache.get(gid) or "").strip()
            if cached:
                return cached

            g_obj = self.bot.get_guild(gid)
            if g_obj and getattr(g_obj, "name", None):
                return g_obj.name.strip()

            return f"guild:{gid}"

        return None

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

        - Coalesces per-clone for a short "idle gap" so near-simultaneous sitemaps batch together.
        - Never waits longer than MAX_COALESCE_TOTAL to avoid head-of-line blocking.
        - Keeps only the newest sitemap PER clone (older same-clone items are dropped).
        - Launches one sync per clone IMMEDIATELY (fire-and-forget) and logs full tracebacks.
        """
        IDLE_GAP = 0.25
        MAX_COALESCE_TOTAL = 2.0

        while True:
            task_id, display_id, sitemap = await q.get()
            token = None
            try:
                guild_label = self._resolve_guild_label("sitemap", sitemap)
                if guild_label:
                    token = logctx.guild_name.set(guild_label)

                latest_by_clone: dict[int, tuple[str, str, dict]] = {}
                dropped = 0

                first_tgt = sitemap.get("target") or {}
                first_cg = int(first_tgt.get("cloned_guild_id") or 0)
                latest_by_clone[first_cg] = (task_id, display_id, sitemap)

                loop = asyncio.get_running_loop()
                deadline = loop.time() + MAX_COALESCE_TOTAL
                while True:
                    timeout = max(0.0, min(IDLE_GAP, deadline - loop.time()))
                    if timeout == 0.0:
                        break
                    try:
                        t2, d2, sm2 = await asyncio.wait_for(q.get(), timeout=timeout)
                        q.task_done()
                        tgt = sm2.get("target") or {}
                        cg = int(tgt.get("cloned_guild_id") or 0)
                        latest_by_clone[cg] = (t2, d2, sm2)
                        dropped += 1
                    except asyncio.TimeoutError:
                        break

                if dropped:
                    logger.debug(
                        "Dropped %d outdated sitemap(s); processing latest per clone",
                        dropped,
                    )

                items = list(latest_by_clone.values())
                items.sort(
                    key=lambda t: int(
                        (t[2].get("target") or {}).get("cloned_guild_id") or 0
                    )
                )

                def _on_done(fut: asyncio.Future, _did: str, _cgid: int):

                    disp_token = logctx.sync_display_id.set(_did)
                    try:
                        with self._clone_log_label(int(_cgid)):
                            try:
                                res = fut.result()
                            except asyncio.CancelledError:
                                logger.info(
                                    "[🗑️] Canceled sync task %s cloning disabled", _did
                                )
                            except Exception as e:
                                logger.error(
                                    "Error processing sitemap %s for clone %s",
                                    _did,
                                    _cgid,
                                    exc_info=(type(e), e, e.__traceback__),
                                )
                            else:
                                if isinstance(res, str) and res.upper().startswith(
                                    "CANCELED"
                                ):
                                    logger.info(
                                        "[🗑️] Canceled sync task %s cloning disabled",
                                        _did,
                                    )
                                else:
                                    logger.info(
                                        "[✅] Sync task %s complete: %s", _did, res
                                    )
                    finally:
                        logctx.sync_display_id.reset(disp_token)

                for tid, did, sm in items:
                    cgid = int((sm.get("target") or {}).get("cloned_guild_id") or 0)
                    task = asyncio.create_task(self.sync_structure(tid, sm))
                    task.add_done_callback(
                        lambda fut, _did=did, _cgid=cgid: _on_done(fut, _did, _cgid)
                    )

            finally:
                if token:
                    logctx.guild_name.reset(token)
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

    def _log_guild_mapping_summary(self) -> None:
        """
        Log how many guild mappings are active vs paused, and warn if nothing is cloning.
        """
        try:
            maps = self.db.list_guild_mappings()
            total = len(maps)

            active_maps: list[dict] = []
            paused_maps: list[dict] = []

            for m in maps:
                status = str(m.get("status") or "active").strip().lower()
                if status == "paused":
                    paused_maps.append(m)
                else:
                    active_maps.append(m)

            sources: list[str] = []
            for m in active_maps:
                host_name = (m.get("original_guild_name") or "").strip()
                if not host_name:
                    host_name = str(m.get("original_guild_id") or "").strip()
                if host_name:
                    sources.append(host_name)

            num_active = len(active_maps)
            num_paused = len(paused_maps)

            if num_active == 0:
                if num_paused:
                    logger.warning(
                        "[⚠️] All %d guild mappings are paused. "
                        "Nothing is currently set to clone.",
                        total,
                    )
                else:
                    logger.warning(
                        "[⚠️] No guild mappings found. Nothing is currently set to clone."
                    )
            else:
                if num_paused:
                    logger.info(
                        "[🧙‍♂️] Copycord is cloning %d of %d server%s (%d paused)",
                        num_active,
                        total,
                        "" if total == 1 else "s",
                        num_paused,
                    )
                else:
                    logger.info(
                        "[🧙‍♂️] Copycord is cloning %d of %d server%s",
                        num_active,
                        total,
                        "" if total == 1 else "s",
                    )

        except Exception:
            logger.exception("Failed to summarize guild_mappings on startup")

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
        await self._load_blocked_keywords_cache()
        await self._load_user_filters_cache()
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

        self._log_guild_mapping_summary()

        asyncio.create_task(self.backfill.cleanup_non_primary_webhooks())

        if not self._processor_started:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))
            self._processor_started = True
            self._prune_old_messages_loop()

    async def on_member_join(self, member: discord.Member):
        g = getattr(member, "guild", None)
        guild_name = g.name if g else "unknown"
        try:
            if not g:
                return

            if not self.db.is_clone_guild_id(int(g.id)):
                return
            logger.info(
                "[👤] %s (%s) has joined %s!", member.name, member.id, guild_name
            )
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
        Works with multiple clones per host by using per-clone caches and DB fallback.
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

            per_cat = getattr(self, "cat_map_by_clone", {}) or {}
            per_cat = per_cat.get(int(channel.guild.id), {}) or {}

            hit_src_cat_id = None
            hit_row = None

            for orig_cat_id, row in list(per_cat.items()):

                if not isinstance(row, dict):
                    row = dict(row)
                    per_cat[int(orig_cat_id)] = row
                if int(row.get("cloned_category_id") or 0) == int(channel.id):
                    hit_src_cat_id = int(orig_cat_id)
                    hit_row = row
                    break

            if hit_src_cat_id is None or hit_row is None:

                dbrow = self.db.conn.execute(
                    "SELECT * FROM category_mappings WHERE cloned_category_id=? AND cloned_guild_id=? LIMIT 1",
                    (int(channel.id), int(channel.guild.id)),
                ).fetchone()
                if dbrow:
                    if not isinstance(dbrow, dict):
                        dbrow = {k: dbrow[k] for k in dbrow.keys()}
                    hit_src_cat_id = int(dbrow.get("original_category_id") or 0)
                    hit_row = dbrow

                    self.cat_map_by_clone.setdefault(int(channel.guild.id), {})[
                        hit_src_cat_id
                    ] = dbrow

            if hit_src_cat_id is None or hit_row is None:
                return

            self.cat_map_by_clone.get(int(channel.guild.id), {}).pop(
                hit_src_cat_id, None
            )
            if hasattr(self, "cat_map") and self.cat_map:
                self.cat_map.pop(hit_src_cat_id, None)

            host_guild_id = hit_row.get("original_guild_id")
            if not host_guild_id:
                logger.info(
                    "[⚠️] No original_guild_id for deleted category clone_g=%s chan=%s",
                    channel.guild.id,
                    channel.id,
                )
                return

            logger.warning(
                "[🧹] Cloned category deleted in clone_g=%s: clone_cat=%s name=%s (src_cat=%s host_g=%s). Requesting sitemap.",
                channel.guild.id,
                channel.id,
                getattr(channel, "name", "?"),
                hit_src_cat_id,
                host_guild_id,
            )

            await self.bot.ws_manager.send(
                {"type": "sitemap_request", "data": {"guild_id": str(host_guild_id)}}
            )
            return

        per_chan = getattr(self, "chan_map_by_clone", {}) or {}
        per_chan = per_chan.get(int(channel.guild.id), {}) or {}

        hit_src_id = None
        hit_row = None

        for src_id, row in list(per_chan.items()):
            if not isinstance(row, dict):
                row = dict(row)
                per_chan[int(src_id)] = row
            if int(row.get("cloned_channel_id") or 0) == int(channel.id):
                hit_src_id = int(src_id)
                hit_row = row
                break

        if hit_src_id is None or hit_row is None:

            dbrow = self.db.conn.execute(
                "SELECT * FROM channel_mappings WHERE cloned_channel_id=? AND cloned_guild_id=? LIMIT 1",
                (int(channel.id), int(channel.guild.id)),
            ).fetchone()
            if dbrow:
                if not isinstance(dbrow, dict):
                    dbrow = {k: dbrow[k] for k in dbrow.keys()}
                hit_src_id = int(dbrow.get("original_channel_id") or 0)
                hit_row = dbrow

                self.chan_map_by_clone.setdefault(int(channel.guild.id), {})[
                    hit_src_id
                ] = dbrow

        if hit_src_id is None or hit_row is None:
            return

        try:
            self.backfill.invalidate_rotation(int(channel.id))
        except Exception:
            pass

        self.chan_map_by_clone.get(int(channel.guild.id), {}).pop(hit_src_id, None)
        if hasattr(self, "chan_map") and self.chan_map:
            self.chan_map.pop(hit_src_id, None)

        host_guild_id = hit_row.get("original_guild_id")
        if not host_guild_id:
            logger.debug(
                "[⚠️] No original_guild_id for deleted channel clone_g=%s chan=%s",
                channel.guild.id,
                channel.id,
            )
            return

        logger.warning(
            "[🧹] Cloned channel deleted in clone_g=%s: clone_chan=%s name=%s (src_chan=%s host_g=%s). Requesting sitemap.",
            channel.guild.id,
            channel.id,
            getattr(channel, "name", "?"),
            hit_src_id,
            host_guild_id,
        )

        await self.bot.ws_manager.send(
            {"type": "sitemap_request", "data": {"guild_id": str(host_guild_id)}}
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

        token = None
        try:

            guild_label = self._resolve_guild_label(typ, data)
            if guild_label:
                token = logctx.guild_name.set(guild_label)

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

                logger.info("[✉️] Sync task %s received", display_id)
                return

            elif typ == "message":
                if data.get("__backfill__"):
                    try:
                        orig = int(data.get("channel_id"))
                    except Exception:
                        return
                    if orig not in self._active_backfills:
                        logger.warning(
                            "Dropping stray backfill message for %s (no active lock)",
                            orig,
                        )
                        return
                    t = self._track(
                        self._handle_backfill_message(data), name="bf-handle"
                    )
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
                try:
                    tid = int((data or {}).get("thread_id") or 0)
                except Exception:
                    tid = 0

                parent_id = None
                if tid:
                    try:
                        rows = self.db.get_thread_mappings_for_original(tid) or []
                        first = self._rowdict(rows[0]) if rows else None
                        if first:
                            parent_id = int(first.get("forum_original_id") or 0) or None
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
                    logger.error(
                        "backfill_started missing/invalid channel_id: %r", cid_raw
                    )
                    return

                tid = msg.get("task_id") or (
                    data.get("task_id") if isinstance(data, dict) else None
                )
                if tid:
                    self._task_for_channel[orig] = str(tid)

                if orig in self._active_backfills:
                    await self.bus.publish(
                        "client",
                        {"type": "backfill_busy", "data": {"channel_id": orig}},
                    )
                    return

                is_resume = bool((msg.get("data") or {}).get("resume"))

                self._active_backfills.add(orig)

                await self.backfill.on_started(
                    orig,
                    meta={
                        "range": (msg.get("data") or {}).get("range"),
                        "resume": is_resume,
                        "clone_channel_id": (msg.get("data") or {}).get(
                            "clone_channel_id"
                        ),
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
                    logger.error(
                        "backfill_done missing/invalid channel_id: %r", cid_raw
                    )
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
                        self._track(
                            self.handle_message_edit(ev_data), name="bf-drain-edit"
                        )
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
                        "[bf] failed WS notify backfill_done for #%s",
                        orig,
                        exc_info=True,
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
        finally:
            if token is not None:
                logctx.guild_name.reset(token)

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

    def _load_mappings(self) -> None:
        """
        Refresh in-memory caches from DB. Safe to call anytime.
        Builds both flat (legacy) and per-clone caches; runtime logic should
        prefer the per-clone maps.
        """

        self.chan_map = {}
        self.chan_map_by_clone = {}

        try:
            rows = self.db.get_all_channel_mappings()
        except Exception:
            logger.debug("get_all_channel_mappings failed", exc_info=True)
            rows = []

        for r in rows:
            rr = dict(r)

            for k in (
                "original_channel_id",
                "cloned_channel_id",
                "original_parent_category_id",
                "cloned_parent_category_id",
                "original_guild_id",
                "cloned_guild_id",
                "channel_type",
            ):
                if rr.get(k) is not None:
                    try:
                        rr[k] = int(rr[k])
                    except Exception:
                        pass

            ocid = rr.get("original_channel_id")
            cg = rr.get("cloned_guild_id") or 0

            self.chan_map_by_clone.setdefault(int(cg), {})[int(ocid)] = rr

            self.chan_map[int(ocid)] = rr

        self.cat_map = {}
        self.cat_map_by_clone = {}

        try:
            cats = self.db.get_all_category_mappings()
        except Exception:
            logger.debug("get_all_category_mappings failed", exc_info=True)
            cats = []

        for r in cats:
            rr = dict(r)
            for k in (
                "original_category_id",
                "cloned_category_id",
                "original_guild_id",
                "cloned_guild_id",
            ):
                if rr.get(k) is not None:
                    try:
                        rr[k] = int(rr[k])
                    except Exception:
                        pass

            ocid = rr.get("original_category_id")
            cg = rr.get("cloned_guild_id") or 0

            self.cat_map[int(ocid)] = rr
            self.cat_map_by_clone.setdefault(int(cg), {})[int(ocid)] = rr

    def _purge_stale_mappings(self, guild: discord.Guild) -> int:
        removed = 0
        per = self.chan_map_by_clone.get(int(guild.id)) or {}

        for orig_id, row in list(per.items()):

            if not isinstance(row, dict):
                row = dict(row)
                per[int(orig_id)] = row

            clone_cid = int(row.get("cloned_channel_id") or 0)
            if not clone_cid:
                continue
            if guild.get_channel(clone_cid):
                continue

            logger.debug(
                "[🗑️] Purging channel mapping pair origin=%s clone_guild=%s cloned_channel=%s",
                int(orig_id),
                int(guild.id),
                clone_cid,
            )

            try:
                self.db.delete_channel_mapping_pair(int(orig_id), int(guild.id))
            except Exception:
                logger.exception(
                    "Failed to delete stale mapping pair for orig=%s clone=%s",
                    orig_id,
                    guild.id,
                )

            per.pop(int(orig_id), None)
            removed += 1

        return removed

    async def sync_structure(self, task_id: int, sitemap: Dict) -> str:
        """
        Synchronizes the structure of a clone based on the provided sitemap.
        """
        logger.debug(f"Sync Task #{task_id}: Processing sitemap {sitemap}")

        host_guild_id_raw = (sitemap.get("guild") or {}).get("id")
        host_guild_id = int(host_guild_id_raw) if host_guild_id_raw else None

        tgt = sitemap.get("target") or {}
        explicit_clone_gid = tgt.get("cloned_guild_id")
        if explicit_clone_gid is not None:
            try:
                explicit_clone_gid = int(explicit_clone_gid)
            except Exception:
                explicit_clone_gid = None

        target_clone_gid = explicit_clone_gid or self._target_clone_gid_for_origin(
            host_guild_id
        )

        settings = resolve_mapping_settings(
            self.db,
            self.config,
            original_guild_id=host_guild_id,
            cloned_guild_id=int(target_clone_gid),
        )

        logging.debug(
            f"[sync] Resolved settings for host_g={host_guild_id} clone_g={target_clone_gid}: {settings}"
        )

        if not settings.get("ENABLE_CLONING", True):
            logger.debug(
                "[🗑️] Cloning disabled for clone_g=%s; canceling task.", target_clone_gid
            )
            return "CANCELED: cloning disabled"

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
                    "[⛔] No clone target for origin %s (explicit=%s); cannot sync.",
                    host_guild_id,
                    explicit_clone_gid,
                )
                return "Error: no mapped clone guild for origin"

            lock = self._get_sync_lock(int(target_clone_gid))
            async with lock:
                guild = self.bot.get_guild(int(target_clone_gid))
                if not guild:
                    logger.warning(
                        "[⛔] Clone guild %s not found (origin=%s, explicit=%s)",
                        target_clone_gid,
                        host_guild_id,
                        explicit_clone_gid,
                    )
                    return "Error: clone guild missing"

                bg_jobs: list[str] = []

                self._load_mappings()

                with self._clone_log_label(int(target_clone_gid)):
                    self.stickers.set_last_sitemap(
                        clone_guild_id=int(target_clone_gid),
                        stickers=sitemap.get("stickers"),
                        host_guild_id=host_guild_id,
                    )

                    if settings.get("CLONE_EMOJI", True):
                        self.emojis.kickoff_sync(
                            sitemap.get("emojis", []),
                            host_guild_id,
                            target_clone_guild_id=int(target_clone_gid),
                        )
                        bg_jobs.append("emoji")

                    if settings.get("CLONE_STICKER", True):
                        self.stickers.kickoff_sync(
                            target_clone_guild_id=int(target_clone_gid)
                        )
                        bg_jobs.append("sticker")

                    roles_handle = None
                    if settings.get("CLONE_ROLES", True):
                        roles_handle = self.roles.kickoff_sync(
                            sitemap.get("roles", []),
                            host_guild_id=host_guild_id,
                            target_clone_guild_id=int(target_clone_gid),
                            delete_roles=settings.get("DELETE_ROLES", False),
                            mirror_permissions=settings.get(
                                "MIRROR_ROLE_PERMISSIONS", False
                            ),
                            update_roles=settings.get("UPDATE_ROLES", True),
                            rearrange_roles=settings.get("REARRANGE_ROLES", False),
                        )
                        bg_jobs.append("roles")

                    cat_created, ch_repaired, repaired_ids = (
                        await self._repair_deleted_categories(guild, sitemap)
                    )
                    self._purge_stale_mappings(guild)

                    parts: List[str] = []
                    if cat_created:
                        parts.append(f"Created {cat_created} categories")

                    parts += await self._sync_categories(guild, sitemap)
                    parts += await self._sync_forums(guild, sitemap)
                    parts += await self._sync_channels(guild, sitemap)
                    parts += await self._sync_community(guild, sitemap)
                    parts += await self._sync_channels(
                        guild,
                        sitemap,
                        stage_only=True,
                        skip_removed=True,
                    )
                    parts += await self._sync_guild_metadata(guild, sitemap, settings)
                    parts += await self._sync_channel_metadata(guild, sitemap)

                    moved = await self._handle_master_channel_moves(
                        guild,
                        self._parse_sitemap(sitemap),
                        host_guild_id,
                        skip_channel_ids=repaired_ids,
                    )

                    total_reparented = int(ch_repaired or 0) + int(moved or 0)
                    if total_reparented:
                        parts.append(f"Reparented {total_reparented} channels")

                    parts += await self._sync_threads(guild, sitemap)

                    self._load_mappings()

                    if settings.get(
                        "MIRROR_CHANNEL_PERMISSIONS", False
                    ) and settings.get("CLONE_ROLES", False):
                        self.perms.schedule_after_role_sync(
                            roles_manager=self.roles,
                            roles_handle_or_none=roles_handle,
                            guild=guild,
                            sitemap=sitemap,
                        )
                        bg_jobs.append("channel-perms")

            main_summary = "; ".join(parts) if parts else "No structure changes needed"

            self._schedule_flush()
            return main_summary

        finally:
            logctx.sync_host_name.reset(_host_token)
            logctx.sync_display_id.reset(_id_token)

    async def _sync_guild_metadata(
        self,
        guild: discord.Guild,
        sitemap: Dict,
        settings: Dict[str, object],
    ) -> List[str]:
        """
        Sync guild-level metadata for THIS clone.

        Currently supported (per-mapping toggles):
        - CLONE_GUILD_ICON
        - CLONE_GUILD_BANNER
        - CLONE_GUILD_SPLASH
        - CLONE_GUILD_DISCOVERY_SPLASH
        - SYNC_GUILD_DESCRIPTION
        """
        parts: List[str] = []

        gmeta = sitemap.get("guild") or {}
        host_gid_raw = gmeta.get("id")
        try:
            host_gid = int(host_gid_raw) if host_gid_raw is not None else None
        except Exception:
            host_gid = None

        host_guild = self.bot.get_guild(int(host_gid)) if host_gid else None

        me = guild.me or guild.get_member(self.bot.user.id)
        gp = getattr(me, "guild_permissions", None)
        if not gp or not (gp.administrator or gp.manage_guild):
            logger.warning(
                "[⚠️] Guild metadata sync skipped for guild %s: missing Manage Guild/Administrator.",
                guild.id,
            )
            return parts

        def _normalized_hash_from_url(u: str | None) -> str | None:
            """
            Normalize an icon/banner/splash URL (or key) to a stable hash string:
            strips guild id, query params, and file extension.
            """
            if not u:
                return None
            try:

                last = u.split("/")[-1]

                last = last.split("?", 1)[0]

                core = last.split(".", 1)[0]
                return core
            except Exception:
                return None

        def _asset_hash(asset) -> str | None:
            """
            Get a normalized hash for an Asset (or asset-like) object
            by converting it to a URL string and normalizing it.
            """
            if not asset:
                return None
            try:

                u = getattr(asset, "url", None)
                if u is None:
                    u = str(asset)
            except Exception:
                try:
                    u = str(asset)
                except Exception:
                    return None
            return _normalized_hash_from_url(u)

        origin_premium_tier = int(gmeta.get("premium_tier") or 0)
        clone_premium_tier = int(getattr(guild, "premium_tier", 0) or 0)

        def _can_clone_premium_asset(kind: str) -> bool:
            """
            Skip banner/splash/discovery_splash if host boost tier > clone boost tier.
            We still allow clearing if host has None.
            """
            if origin_premium_tier > clone_premium_tier:
                logger.info(
                    "[✨] Skipping %s sync for clone guild %s: host premium_tier=%s > "
                    "clone premium_tier=%s",
                    kind,
                    guild.id,
                    origin_premium_tier,
                    clone_premium_tier,
                )
                parts.append(
                    f"Skipped {kind} (host boost tier {origin_premium_tier} > clone tier {clone_premium_tier})"
                )
                return False
            return True

        cfg_clone_icon = settings.get("CLONE_GUILD_ICON", False)
        cfg_clone_banner = settings.get("CLONE_GUILD_BANNER", False)
        cfg_clone_splash = settings.get("CLONE_GUILD_SPLASH", False)
        cfg_clone_discovery_splash = settings.get("CLONE_GUILD_DISCOVERY_SPLASH", False)

        cfg_desc = settings.get("SYNC_GUILD_DESCRIPTION", False)

        want_desc = gmeta.get("description")

        changes: Dict[str, object] = {}
        changed_fields: list[str] = []

        if cfg_clone_icon:
            icon_url = gmeta.get("icon") or None
            host_icon = getattr(host_guild, "icon", None) if host_guild else None
            clone_icon = getattr(guild, "icon", None)

            clone_hash = _asset_hash(clone_icon)

            try:

                if icon_url is None and host_icon is None:
                    if clone_icon is not None:
                        changes["icon"] = None
                        changed_fields.append("icon (cleared)")
                else:

                    host_hash = None
                    if host_icon is not None:
                        host_hash = _asset_hash(host_icon)
                    elif icon_url is not None:
                        host_hash = _normalized_hash_from_url(icon_url)

                    if (
                        host_hash is not None
                        and clone_hash is not None
                        and host_hash == clone_hash
                    ):
                        pass
                    else:
                        icon_bytes = None

                        if host_icon is not None:
                            icon_bytes = await host_icon.read()

                        if icon_bytes is None and icon_url is not None:
                            if (
                                getattr(self, "session", None) is None
                                or self.session.closed
                            ):
                                import aiohttp

                                self.session = aiohttp.ClientSession()

                            async with self.session.get(icon_url) as resp:
                                resp.raise_for_status()
                                icon_bytes = await resp.read()

                        if icon_bytes is not None:
                            changes["icon"] = icon_bytes
                            changed_fields.append("icon")
            except Exception:
                logger.warning(
                    "[⚠️] Failed syncing guild icon for clone %s",
                    guild.id,
                    exc_info=True,
                )

        if cfg_clone_banner:
            host_banner = getattr(host_guild, "banner", None) if host_guild else None
            clone_banner = getattr(guild, "banner", None)
            host_hash = _asset_hash(host_banner)
            clone_hash = _asset_hash(clone_banner)

            try:
                if host_banner is None:
                    if clone_banner is not None:
                        changes["banner"] = None
                        changed_fields.append("banner (cleared)")
                else:
                    if not _can_clone_premium_asset("banner"):
                        pass
                    else:
                        if host_hash is None or host_hash != clone_hash:
                            banner_bytes = await host_banner.read()
                            changes["banner"] = banner_bytes
                            changed_fields.append("banner")
            except Exception as e:
                logger.warning(
                    "[⚠️] Failed syncing guild banner for clone %s: %s", guild.id, e
                )

        if cfg_clone_splash:
            host_splash = getattr(host_guild, "splash", None) if host_guild else None
            clone_splash = getattr(guild, "splash", None)
            host_hash = _asset_hash(host_splash)
            clone_hash = _asset_hash(clone_splash)

            try:
                if host_splash is None:
                    if clone_splash is not None:
                        changes["splash"] = None
                        changed_fields.append("splash (cleared)")
                else:
                    if not _can_clone_premium_asset("splash"):
                        pass
                    else:
                        if host_hash is None or host_hash != clone_hash:
                            splash_bytes = await host_splash.read()
                            changes["splash"] = splash_bytes
                            changed_fields.append("splash")
            except Exception as e:
                logger.warning(
                    "[⚠️] Failed syncing guild splash for clone %s: %s", guild.id, e
                )

        if cfg_clone_discovery_splash:
            host_ds = (
                getattr(host_guild, "discovery_splash", None) if host_guild else None
            )
            clone_ds = getattr(guild, "discovery_splash", None)
            host_hash = _asset_hash(host_ds)
            clone_hash = _asset_hash(clone_ds)

            try:
                if host_ds is None:
                    if clone_ds is not None:
                        changes["discovery_splash"] = None
                        changed_fields.append("discovery_splash (cleared)")
                else:
                    if not _can_clone_premium_asset("discovery_splash"):
                        pass
                    else:
                        if host_hash is None or host_hash != clone_hash:
                            ds_bytes = await host_ds.read()
                            changes["discovery_splash"] = ds_bytes
                            changed_fields.append("discovery_splash")
            except Exception as e:
                logger.warning(
                    "[⚠️] Failed syncing guild discovery_splash for clone %s: %s",
                    guild.id,
                    e,
                )

        if cfg_desc:
            if getattr(guild, "description", None) != want_desc:
                changes["description"] = want_desc
                changed_fields.append("description")

        if not changes:
            return parts

        try:
            await self.ratelimit.acquire_for_guild(ActionType.EDIT_CHANNEL, guild.id)
            await guild.edit(**changes)
            logger.info(
                "[🏛️] Updated guild metadata for '%s' (%d): %s",
                guild.name,
                int(guild.id),
                ", ".join(changed_fields),
            )
            parts.append(
                "Updated guild metadata: " + ", ".join(sorted(set(changed_fields)))
            )
        except Exception:
            logger.warning(
                "[⚠️] Failed to update guild metadata for clone guild %s",
                guild.id,
                exc_info=True,
            )

        return parts

    async def _sync_community(self, guild: discord.Guild, sitemap: Dict) -> List[str]:
        """
        Enable/disable Community mode and set rules/public updates channels
        for THIS clone guild only. Preflight-checks current IDs via HTTP and
        only patches when there's a real delta. Avoids 'X→X' spam.
        """
        parts: List[str] = []

        community = sitemap.get("community") or {}
        want_enabled = bool(community.get("enabled"))

        def _int(v):
            try:
                i = int(v or 0)
                return i or None
            except Exception:
                return None

        want_rules_orig = _int(community.get("rules_channel_id"))
        want_updates_orig = _int(community.get("public_updates_channel_id"))

        me = guild.me or guild.get_member(self.bot.user.id)
        gp = getattr(me, "guild_permissions", None)
        if not gp or not (gp.administrator or gp.manage_guild):
            logger.warning(
                "[⚠️] Community sync skipped for guild %s: missing Manage Guild/Administrator.",
                guild.id,
            )
            return parts

        def _map_for_clone(orig_id: int | None):
            if not orig_id:
                return None
            per = (getattr(self, "chan_map_by_clone", {}) or {}).get(
                int(guild.id), {}
            ) or {}
            row = per.get(int(orig_id))
            if not row and hasattr(self, "db"):
                try:
                    row = self.db.get_channel_mapping_for_clone(
                        int(orig_id), int(guild.id)
                    )
                except Exception:
                    row = None
            if row and not isinstance(row, dict):
                row = dict(row)
            return row

        rc_row = _map_for_clone(want_rules_orig) if want_rules_orig else None
        uc_row = _map_for_clone(want_updates_orig) if want_updates_orig else None

        want_rules_clone_id = int(rc_row["cloned_channel_id"]) if rc_row else None
        want_updates_clone_id = int(uc_row["cloned_channel_id"]) if uc_row else None

        if want_enabled and not (want_rules_clone_id and want_updates_clone_id):
            logger.warning(
                "[⚠️] Community sync skipped for guild %s: missing per-clone channel mapping.",
                guild.id,
            )
            return parts

        rc = guild.get_channel(want_rules_clone_id) if want_rules_clone_id else None
        uc = guild.get_channel(want_updates_clone_id) if want_updates_clone_id else None
        if want_enabled and (not rc or not uc):
            logger.warning(
                "[⚠️] Community sync skipped for guild %s: target channels not found (rules=%s updates=%s).",
                guild.id,
                getattr(rc, "id", None),
                getattr(uc, "id", None),
            )
            return parts

        async def _fetch_comm_ids_via_http(
            gid: int,
        ) -> tuple[int | None, int | None, bool]:
            """
            Returns (rules_id, updates_id, community_enabled) from HTTP payload.
            """
            try:
                data = await self.bot.http.get_guild(int(gid), with_counts=False)
                rid = data.get("rules_channel_id")
                uid = data.get("public_updates_channel_id")
                features = set(data.get("features") or [])
                return (
                    int(rid) if rid else None,
                    int(uid) if uid else None,
                    ("COMMUNITY" in features),
                )
            except Exception:
                try:
                    g2 = await self.bot.fetch_guild(int(gid))
                    r = getattr(getattr(g2, "rules_channel", None), "id", None)
                    u = getattr(getattr(g2, "public_updates_channel", None), "id", None)
                    enabled = "COMMUNITY" in (getattr(g2, "features", None) or [])
                    return (int(r) if r else None, int(u) if u else None, bool(enabled))
                except Exception:
                    return (None, None, False)

        async def _verify(
            gid: int,
            want_r: int | None,
            want_u: int | None,
            want_enabled_: bool,
            attempts: int = 3,
            base_delay: float = 0.3,
        ) -> tuple[bool, int | None, int | None, bool]:
            last_r = last_u = None
            last_enabled = False
            for i in range(attempts):
                if i:
                    await asyncio.sleep(base_delay * (i + 1))
                r, u, en = await _fetch_comm_ids_via_http(gid)
                last_r, last_u, last_enabled = r, u, en
                if (
                    (en == want_enabled_)
                    and (want_r is None or r == want_r)
                    and (want_u is None or u == want_u)
                ):
                    return True, r, u, en
            return False, last_r, last_u, last_enabled

        ok0, curr_r0, curr_u0, curr_enabled0 = await _verify(
            guild.id,
            want_rules_clone_id,
            want_updates_clone_id,
            want_enabled,
            attempts=2,
            base_delay=0.2,
        )
        if ok0:

            return parts

        async def _ensure_prereqs():
            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await guild.edit(
                    verification_level=getattr(discord.VerificationLevel, "medium"),
                    explicit_content_filter=getattr(
                        discord.ContentFilter, "all_members"
                    ),
                    default_notifications=getattr(
                        discord.NotificationLevel, "only_mentions"
                    ),
                )
            except Exception as e:
                logger.debug(
                    "[ℹ️] Prereq tune-up for guild %s raised %r; continuing.",
                    guild.id,
                    e,
                )

        await _ensure_prereqs()

        patch_kwargs = {}
        if curr_enabled0 != want_enabled:
            patch_kwargs["community"] = want_enabled
        if want_enabled:
            if curr_r0 != want_rules_clone_id:
                patch_kwargs["rules_channel"] = rc
            if curr_u0 != want_updates_clone_id:
                patch_kwargs["public_updates_channel"] = uc
        else:

            if curr_r0 is not None:
                patch_kwargs["rules_channel"] = None
            if curr_u0 is not None:
                patch_kwargs["public_updates_channel"] = None

        if not patch_kwargs:
            return parts

        try:
            await self.ratelimit.acquire_for_guild(ActionType.EDIT_CHANNEL, guild.id)
            await guild.edit(**patch_kwargs)
        except Exception as e:
            logger.debug(
                "[ℹ️] Minimal guild.edit failed for %s: %r (will try targeted fallbacks)",
                guild.id,
                e,
            )

        ok1, curr_r1, curr_u1, curr_enabled1 = await _verify(
            guild.id, want_rules_clone_id, want_updates_clone_id, want_enabled
        )

        if not ok1 and want_enabled:

            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await guild.edit(rules_channel=None, public_updates_channel=None)
            except Exception:
                pass
            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await guild.edit(
                    community=True, rules_channel=rc, public_updates_channel=uc
                )
            except Exception as e:
                logger.warning("[⚠️] Clear→set failed for guild %s: %s", guild.id, e)
            ok1, curr_r1, curr_u1, curr_enabled1 = await _verify(
                guild.id, want_rules_clone_id, want_updates_clone_id, want_enabled
            )

        changed_bits = []

        if curr_enabled0 != curr_enabled1:
            changed_bits.append("enabled" if curr_enabled1 else "disabled")
        if want_enabled:
            if curr_r0 != curr_r1 and curr_r1 == want_rules_clone_id:
                changed_bits.append(f"rules {curr_r0}→{curr_r1}")
            if curr_u0 != curr_u1 and curr_u1 == want_updates_clone_id:
                changed_bits.append(f"updates {curr_u0}→{curr_u1}")

        if changed_bits:
            parts.append("Updated Community channels: " + ", ".join(changed_bits))

        return parts

    async def _repair_deleted_categories(
        self, guild: discord.Guild, sitemap: dict
    ) -> tuple[int, int, set[int]]:
        """
        For THIS clone guild only:

        - Only repair categories/channels when there is already a mapping in the DB.
        - If a mapped category is missing in the clone, recreate (or adopt by name) and
        update the mapping.
        - If REPOSITION_CHANNELS is enabled, ensure mapped channels are parented to the
        correct mapped category.

        This MUST NOT act as a first-time category creator; it should only "repair"
        broken state where the DB mappings exist but the clone-side objects do not.
        """
        created = 0
        reparented = 0
        repaired_ids: set[int] = set()

        upstream_cats = {int(c["id"]): c for c in (sitemap.get("categories") or [])}
        host_guild_id = int((sitemap.get("guild") or {}).get("id") or 0)

        try:
            settings = resolve_mapping_settings(
                self.db,
                self.config,
                original_guild_id=host_guild_id,
                cloned_guild_id=int(guild.id),
            )
        except Exception:
            settings = self.config.default_mapping_settings()

        reposition_enabled = settings.get("REPOSITION_CHANNELS", True)

        per_cat = (self.cat_map_by_clone or {}).get(int(guild.id), {}) or {}
        per_chan = (self.chan_map_by_clone or {}).get(int(guild.id), {}) or {}

        if not per_cat or not per_chan:
            with contextlib.suppress(Exception):
                self._load_mappings()
                per_cat = (self.cat_map_by_clone or {}).get(int(guild.id), {}) or {}
                per_chan = (self.chan_map_by_clone or {}).get(int(guild.id), {}) or {}

        if not per_cat and not per_chan:
            return 0, 0, set()

        for orig_cat_id, upstream in upstream_cats.items():
            upstream_name = (upstream.get("name") or "").strip()

            mapping = per_cat.get(int(orig_cat_id))
            if mapping is None:
                row = self.db.get_category_mapping_by_original_and_clone(
                    int(orig_cat_id), int(guild.id)
                )
                if row and not isinstance(row, dict):
                    row = dict(row)
                if row:
                    mapping = row
                    self.cat_map_by_clone.setdefault(int(guild.id), {})[
                        int(orig_cat_id)
                    ] = mapping

            if mapping is None:
                continue

            clone_cat_id = int((mapping or {}).get("cloned_category_id") or 0)
            cat_obj = guild.get_channel(clone_cat_id) if clone_cat_id else None

            if not cat_obj:

                cat_obj = discord.utils.get(guild.categories, name=upstream_name)

                if not cat_obj:
                    try:
                        await self.ratelimit.acquire_for_guild(
                            ActionType.CREATE_CHANNEL, guild.id
                        )
                        cat_obj = await guild.create_category(
                            upstream_name or "Category"
                        )
                        created += 1
                        logger.info(
                            "[repair:category-created] guild=%s(%d) orig_cat=%d name=%r new_id=%d",
                            guild.name,
                            guild.id,
                            orig_cat_id,
                            upstream_name,
                            int(cat_obj.id),
                        )
                    except Exception:
                        logger.warning(
                            "[repair:category-create-failed] guild=%s(%d) orig_cat=%d name=%r",
                            guild.name,
                            guild.id,
                            orig_cat_id,
                            upstream_name,
                            exc_info=True,
                        )
                        continue

                try:
                    self.db.upsert_category_mapping(
                        orig_id=int(orig_cat_id),
                        orig_name=upstream_name or (cat_obj.name or "Category"),
                        clone_id=int(cat_obj.id),
                        clone_name=(mapping or {}).get("cloned_category_name"),
                        original_guild_id=int(host_guild_id),
                        cloned_guild_id=int(guild.id),
                    )
                finally:
                    m = {
                        "original_category_id": int(orig_cat_id),
                        "cloned_category_id": int(cat_obj.id),
                        "original_guild_id": int(host_guild_id),
                        "cloned_guild_id": int(guild.id),
                        "clone_category_name": (mapping or {}).get(
                            "cloned_category_name"
                        ),
                    }
                    self.cat_map_by_clone.setdefault(int(guild.id), {})[
                        int(orig_cat_id)
                    ] = m

            if not reposition_enabled:
                logger.debug(
                    "[repair] Skipping channel reparenting for clone_g=%s orig_cat=%d (REPOSITION_CHANNELS=False)",
                    guild.id,
                    orig_cat_id,
                )
                continue

            cat_id_for_this_clone = int(cat_obj.id)

            for ch_orig_id, ch_map in list(per_chan.items()):
                try:
                    if int(ch_map.get("original_parent_category_id") or 0) != int(
                        orig_cat_id
                    ):
                        continue

                    ch = guild.get_channel(int(ch_map.get("cloned_channel_id") or 0))
                    if not ch:

                        continue

                    actual_parent_id = int(ch.category.id) if ch.category else None
                    if actual_parent_id == cat_id_for_this_clone:
                        logger.debug(
                            "[repair:skip:already-correct] guild=%s(%d) ch_id=%d parent=%s",
                            guild.name,
                            guild.id,
                            int(ch.id),
                            str(actual_parent_id),
                        )
                        continue

                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, guild.id
                    )
                    await ch.edit(category=cat_obj)
                    reparented += 1
                    repaired_ids.add(int(ch.id))
                    logger.info(
                        "[repair:reparent] guild=%s(%d) ch_id=%d → cat_id=%d(%r)",
                        guild.name,
                        guild.id,
                        int(ch.id),
                        int(cat_obj.id),
                        cat_obj.name,
                    )

                    try:
                        self.db.upsert_channel_mapping(
                            int(ch_orig_id),
                            ch_map.get("original_channel_name"),
                            int(ch.id),
                            ch_map.get("channel_webhook_url"),
                            int(orig_cat_id),
                            int(cat_id_for_this_clone),
                            (
                                int(ch.type.value)
                                if hasattr(ch.type, "value")
                                else int(ch.type)
                            ),
                            original_guild_id=int(host_guild_id),
                            cloned_guild_id=int(guild.id),
                            clone_name=(ch_map.get("clone_channel_name") or None),
                        )
                    finally:
                        ch_map["cloned_parent_category_id"] = int(cat_id_for_this_clone)

                except Exception:
                    logger.debug("[repair] channel reparent error", exc_info=True)

        return created, reparented, repaired_ids

    async def _sync_channel_metadata(self, guild: Guild, sitemap: Dict) -> List[str]:
        """
        Synchronize all channel metadata in a single pass, after structural sync.

        Uses per-mapping settings for:
        - SYNC_CHANNEL_TOPIC
        - SYNC_CHANNEL_NSFW
        - SYNC_CHANNEL_SLOWMODE
        - CLONE_VOICE / CLONE_VOICE_PROPERTIES
        - CLONE_STAGE / CLONE_STAGE_PROPERTIES
        - SYNC_FORUM_PROPERTIES (post guidelines, message limit, layout, sort, archive, require_tag, default reaction)
        """

        parts: List[str] = []
        host_guild_id = (sitemap.get("guild") or {}).get("id")

        try:
            settings = resolve_mapping_settings(
                self.db,
                self.config,
                original_guild_id=int(host_guild_id) if host_guild_id else None,
                cloned_guild_id=int(guild.id),
            )
        except Exception:
            settings = self.config.default_mapping_settings()

        sync_topic = settings.get("SYNC_CHANNEL_TOPIC", False)
        sync_nsfw = settings.get("SYNC_CHANNEL_NSFW", False)
        sync_slowmode = settings.get("SYNC_CHANNEL_SLOWMODE", False)

        clone_voice = settings.get("CLONE_VOICE", False)
        clone_voice_props = settings.get("CLONE_VOICE_PROPERTIES", False)
        clone_stage = settings.get("CLONE_STAGE", False)
        clone_stage_props = settings.get("CLONE_STAGE_PROPERTIES", False)
        sync_forum_props = settings.get("SYNC_FORUM_PROPERTIES", False)

        has_community = "COMMUNITY" in guild.features

        if not any(
            [
                sync_topic,
                sync_nsfw,
                sync_slowmode,
                clone_voice and clone_voice_props,
                clone_stage and clone_stage_props and has_community,
                sync_forum_props,
            ]
        ):
            logger.debug(
                "[meta] All channel metadata sync flags disabled for clone_g=%s; skipping",
                guild.id,
            )
            return parts

        guild_info = sitemap.get("guild") or {}
        target = sitemap.get("target") or {}

        try:
            ctx_host_gid = int(guild_info.get("id") or 0)
        except (TypeError, ValueError):
            ctx_host_gid = 0

        try:
            ctx_cloned_gid = int(target.get("cloned_guild_id") or guild.id or 0)
        except (TypeError, ValueError):
            ctx_cloned_gid = int(guild.id)

        mapping_id_hint = target.get("mapping_id")

        ctx_mapping_row: dict | None = None
        if ctx_host_gid:
            mrow = None
            try:
                if mapping_id_hint:
                    mrow = self.db.get_mapping_by_id(str(mapping_id_hint))
                if not mrow and ctx_cloned_gid:
                    mrow = self.db.get_mapping_by_original_and_clone(
                        ctx_host_gid, ctx_cloned_gid
                    )
                if not mrow:
                    mrow = self.db.get_mapping_by_original(ctx_host_gid)
            except Exception:
                mrow = None

            if mrow:
                if not isinstance(mrow, dict):
                    try:
                        mrow = {k: mrow[k] for k in mrow.keys()}
                    except Exception:
                        mrow = dict(mrow)
                ctx_mapping_row = mrow

        def _sanitize_topic(text: str | None, limit: int) -> str | None:
            """
            Rewrite host channel mentions and message links inside a topic
            using the existing inline sanitizer, then clip to Discord's limits.
            """
            if not text:
                return text
            if not ctx_host_gid or not ctx_mapping_row:

                return text[:limit]
            try:
                rewritten = self._sanitize_inline(
                    text,
                    ctx_guild_id=ctx_host_gid,
                    ctx_mapping_row=ctx_mapping_row,
                )
                if rewritten is None:
                    return None
                return rewritten[:limit]
            except Exception:
                logger.debug(
                    "[meta] Topic sanitize failed; using original value",
                    exc_info=True,
                )
                return text[:limit]

        topic_map: Dict[int, Optional[str]] = {}
        nsfw_map: Dict[int, bool] = {}
        slowmode_map: Dict[int, int] = {}

        voice_meta: Dict[int, Dict[str, object]] = {}
        stage_meta: Dict[int, Dict[str, object]] = {}
        forum_meta: Dict[int, Dict[str, object]] = {}

        def ingest_channel(ch: Dict):
            try:
                cid = int(ch["id"])
            except Exception:
                return

            ch_type = int(ch.get("type", 0))

            if sync_topic:
                raw_topic = ch.get("topic")

                topic_map[cid] = _sanitize_topic(raw_topic, 1024)

            if sync_nsfw:
                nsfw_map[cid] = bool(ch.get("nsfw"))

            if sync_slowmode:
                try:
                    slowmode_map[cid] = int(ch.get("slowmode_delay", 0) or 0)
                except Exception:
                    slowmode_map[cid] = 0

            if clone_voice and clone_voice_props and ch_type == ChannelType.voice.value:
                voice_meta[cid] = {
                    "bitrate": ch.get("bitrate"),
                    "user_limit": ch.get("user_limit"),
                    "rtc_region": ch.get("rtc_region"),
                    "video_quality": ch.get("video_quality"),
                }

            if (
                has_community
                and clone_stage
                and clone_stage_props
                and ch_type == ChannelType.stage_voice.value
            ):
                stage_meta[cid] = {
                    "bitrate": ch.get("bitrate"),
                    "user_limit": ch.get("user_limit"),
                    "rtc_region": ch.get("rtc_region"),
                    "topic": _sanitize_topic(ch.get("topic"), 120),
                    "video_quality": ch.get("video_quality"),
                }

            if sync_forum_props and ch_type == ChannelType.forum.value:
                fm: Dict[str, object] = {}

                if "post_guidelines" in ch:
                    fm["topic"] = _sanitize_topic(ch.get("post_guidelines"), 1024)
                elif "topic" in ch:
                    fm["topic"] = _sanitize_topic(ch.get("topic"), 1024)

                if "message_limit_per_interval" in ch:
                    try:
                        fm["default_thread_slowmode_delay"] = int(
                            ch.get("message_limit_per_interval") or 0
                        )
                    except Exception:
                        fm["default_thread_slowmode_delay"] = 0

                if "default_layout" in ch:
                    fm["default_layout"] = ch.get("default_layout")
                if "default_sort_order" in ch:
                    fm["default_sort_order"] = ch.get("default_sort_order")
                if "hide_after_inactivity" in ch:
                    fm["default_auto_archive_duration"] = ch.get(
                        "hide_after_inactivity"
                    )

                if "require_tag" in ch:
                    fm["require_tag"] = bool(ch.get("require_tag"))

                if "default_reaction" in ch:
                    fm["default_reaction"] = ch.get("default_reaction")
                if "available_tags" in ch:
                    fm["available_tags"] = ch.get("available_tags") or []

                if fm:
                    forum_meta[cid] = fm

        for cat in sitemap.get("categories", []):
            for ch in cat.get("channels", []):
                ingest_channel(ch)

        for ch in sitemap.get("standalone_channels", []):
            ingest_channel(ch)

        for forum in sitemap.get("forums", []):
            ingest_channel(forum)

        if not any(
            [topic_map, nsfw_map, slowmode_map, voice_meta, stage_meta, forum_meta]
        ):
            logger.debug(
                "[meta] No channel metadata found in sitemap for clone_g=%s; nothing to sync",
                guild.id,
            )
            return parts

        def _norm_video_quality(
            vqm: Optional[discord.VideoQualityMode | int | str],
        ) -> str:
            """Normalize any VideoQualityMode / int / string into 'auto' or 'full'."""
            if vqm is None:
                return "auto"
            if isinstance(vqm, discord.VideoQualityMode):
                return "full" if vqm == discord.VideoQualityMode.full else "auto"
            if isinstance(vqm, int):
                return "full" if vqm == discord.VideoQualityMode.full.value else "auto"
            s = str(vqm).lower()
            return "full" if s in ("full", "720p", "high") else "auto"

        def _parse_video_quality(
            label: Optional[str | int | discord.VideoQualityMode],
        ) -> discord.VideoQualityMode:
            """Map 'auto'/'full'/int/VideoQualityMode to a proper VideoQualityMode."""
            if isinstance(label, discord.VideoQualityMode):
                return label
            if isinstance(label, int):
                return (
                    discord.VideoQualityMode.full
                    if label == discord.VideoQualityMode.full.value
                    else discord.VideoQualityMode.auto
                )

            s = (label or "auto").lower()
            if s in ("full", "720p", "high"):
                return discord.VideoQualityMode.full
            return discord.VideoQualityMode.auto

        def _normalize_region(val: Any) -> str:
            """Normalize rtc_region values to lower-case strings, 'auto' if unset."""
            if val is None:
                return "auto"
            if isinstance(val, str):
                s = val.strip().lower()
                return "auto" if not s or s in ("auto", "automatic") else s
            try:
                s = str(val).strip().lower()
                return s or "auto"
            except Exception:
                return "auto"

        def _enum_int(val, default: Optional[int]) -> Optional[int]:
            """
            Helper: unwrap Enum/None/int into a plain int, or default.
            Used for Forum layout/sort enums which are not JSON-serializable directly.
            """
            if val is None:
                return default
            try:
                if isinstance(val, int):
                    return val
                if hasattr(val, "value"):
                    return int(val.value)
                return int(val)
            except Exception:
                return default

        def _norm_forum_emoji(val: Any) -> tuple[int | None, str | None, bool]:
            """
            Normalize forum default reaction emoji into (id, name, animated).
            - custom emoji → (id, name, animated)
            - unicode emoji / plain string → (None, string, False)
            - missing/None → (None, None, False)
            """
            if val is None:
                return (None, None, False)

            if isinstance(val, dict):
                try:
                    eid = int(val.get("id") or 0) or None
                except Exception:
                    eid = None
                name = val.get("name") or None
                animated = bool(val.get("animated", False))
                return (eid, name, animated)

            if isinstance(val, (discord.Emoji, discord.PartialEmoji)):
                try:
                    eid = int(val.id) if val.id is not None else None
                except Exception:
                    eid = None
                name = getattr(val, "name", None)
                animated = bool(getattr(val, "animated", False))
                return (eid, name, animated)

            s = str(val or "")
            if not s:
                return (None, None, False)

            return (None, s, False)

        def _build_forum_emoji(norm: tuple[int | None, str | None, bool]):
            """
            Build a value suitable for default_reaction_emoji param:
            - custom emoji → discord.PartialEmoji
            - unicode → string
            - missing → None
            """
            eid, name, animated = norm
            if eid:
                return discord.PartialEmoji(
                    id=eid, name=name or None, animated=animated
                )
            if name:
                return name
            return None

        def _norm_tag_emoji_value(val: Any) -> str:
            """Normalize ForumTag.emoji to a simple comparable string."""
            if isinstance(val, (discord.Emoji, discord.PartialEmoji)):
                if getattr(val, "id", None):
                    return f"custom:{val.id}"
                return val.name or ""
            return str(val or "")

        def _canon_host_tags(
            tags_meta, desired_req: Optional[bool]
        ) -> list[tuple[str, bool, str]]:
            """
            Canonical form of host tags for idempotence comparison.
            We ignore IDs and only care about (name, moderated, emoji_str).

            If require_tag=True but host has no unmoderated tags, we
            include the implicit 'General' fallback in the canonical list
            so that once we create it on the clone, future runs are stable.
            """
            out: list[tuple[str, bool, str]] = []
            has_unmod = False

            for tm in tags_meta or []:
                name = (tm.get("name") or "").strip().lower()
                if not name:
                    continue
                moderated = bool(tm.get("moderated", False))
                emoji_name = tm.get("emoji_name") or ""
                emoji_str = emoji_name or "🏷️"
                out.append((name, moderated, emoji_str))
                if not moderated:
                    has_unmod = True

            if desired_req and not has_unmod:
                out.append(("general", False, "🏷️"))

            out.sort()
            return out

        def _canon_clone_tags(tags) -> list[tuple[str, bool, str]]:
            """
            Canonical form of clone ForumTags for idempotence comparison.
            """
            out: list[tuple[str, bool, str]] = []
            for t in tags or []:
                name = (getattr(t, "name", "") or "").strip().lower()
                if not name:
                    continue
                moderated = bool(getattr(t, "moderated", False))
                emoji_val = _norm_tag_emoji_value(getattr(t, "emoji", None))
                out.append((name, moderated, emoji_val))
            out.sort()
            return out

        def _get_http_client_and_route():
            try:
                from discord.http import Route
            except Exception:
                return None, None

            http_client = None
            for owner_attr in ("client", "bot"):
                owner = getattr(self, owner_attr, None)
                if owner is not None:
                    http_client = getattr(owner, "http", None)
                    if http_client is not None:
                        break

            if http_client is None:
                return None, None

            return http_client, Route

        async def _fetch_forum_layout(channel_id: int) -> Optional[int]:
            """
            Fetch the live default_forum_layout from Discord so we can
            compare against the host and avoid unnecessary PATCHes.
            """
            http_client, Route = _get_http_client_and_route()
            if http_client is None or Route is None:
                logger.debug(
                    "[meta] Cannot fetch forum layout for #%d: no http client/Route",
                    channel_id,
                )
                return None

            route = Route("GET", "/channels/{channel_id}", channel_id=channel_id)
            try:
                data = await http_client.request(route)
            except Exception as e:
                logger.debug(
                    "[meta] Failed to fetch forum layout for #%d: %s",
                    channel_id,
                    e,
                )
                return None

            if not isinstance(data, dict):
                return None

            raw = data.get("default_forum_layout")
            current = _enum_int(raw, None)
            logger.debug(
                "[meta] Live forum layout for #%d from API: raw=%r -> %r",
                channel_id,
                raw,
                current,
            )
            return current

        async def _raw_patch_forum_layout(channel_id: int, layout_val: int) -> bool:
            """
            Raw PATCH for forum default layout for older py-cord versions.

            Discord API expects: default_forum_layout (int)
            """
            http_client, Route = _get_http_client_and_route()
            if http_client is None or Route is None:
                logger.warning(
                    "[meta] Cannot raw-patch forum layout for #%d: no http client/Route",
                    channel_id,
                )
                return False

            route = Route("PATCH", "/channels/{channel_id}", channel_id=channel_id)
            payload = {"default_forum_layout": int(layout_val)}

            try:
                await http_client.request(route, json=payload)
                logger.debug(
                    "[🧵] Raw patched default_forum_layout=%s for forum #%d",
                    layout_val,
                    channel_id,
                )
                return True
            except Exception as e:
                logger.warning(
                    "[meta] Raw default_forum_layout PATCH failed for forum #%d: %s",
                    channel_id,
                    e,
                )
                return False

        per_clone = self.chan_map_by_clone.setdefault(int(guild.id), {}) or {}

        all_ids: Set[int] = (
            set(topic_map.keys())
            | set(nsfw_map.keys())
            | set(slowmode_map.keys())
            | set(voice_meta.keys())
            | set(stage_meta.keys())
            | set(forum_meta.keys())
        )

        topic_updated = 0
        nsfw_updated = 0
        slowmode_updated = 0
        voice_updated = 0
        stage_updated = 0
        forum_updated = 0

        require_tag_ops: list[tuple[ForumChannel, bool]] = []

        for orig_id in all_ids:
            row = per_clone.get(int(orig_id))
            if not row:
                row = self.db.get_channel_mapping_by_original_and_clone(
                    int(orig_id), int(guild.id)
                )
                if row is not None and not isinstance(row, dict):
                    row = dict(row)
                if row:
                    per_clone[int(orig_id)] = row

            if not row:
                continue

            clone_id = int(row.get("cloned_channel_id") or 0)
            if not clone_id:
                continue

            ch = guild.get_channel(clone_id)
            if not ch:
                continue

            changes: Dict[str, object] = {}
            is_voice_ch = isinstance(ch, discord.VoiceChannel)
            is_stage_ch = isinstance(ch, discord.StageChannel)
            is_forum_ch = isinstance(ch, ForumChannel)

            if sync_topic and orig_id in topic_map and hasattr(ch, "topic"):
                desired_topic = topic_map[orig_id]
                current_topic = getattr(ch, "topic", None)
                current_norm = current_topic if current_topic else None
                desired_norm = desired_topic if desired_topic else None
                if current_norm != desired_norm:
                    changes["topic"] = desired_topic
                    topic_updated += 1

            if sync_nsfw and orig_id in nsfw_map and hasattr(ch, "nsfw"):
                desired_nsfw = bool(nsfw_map[orig_id])
                current_nsfw = bool(getattr(ch, "nsfw", False))
                if current_nsfw != desired_nsfw:
                    changes["nsfw"] = desired_nsfw
                    nsfw_updated += 1

            if (
                sync_slowmode
                and orig_id in slowmode_map
                and hasattr(ch, "slowmode_delay")
                and not is_forum_ch
            ):
                desired_delay = max(0, min(21600, int(slowmode_map[orig_id] or 0)))
                current_delay = int(getattr(ch, "slowmode_delay", 0) or 0)
                if current_delay != desired_delay:
                    changes["slowmode_delay"] = desired_delay
                    slowmode_updated += 1

            voice_changed_here = False
            if (
                clone_voice
                and clone_voice_props
                and is_voice_ch
                and orig_id in voice_meta
            ):
                meta = voice_meta[orig_id]

                if meta.get("bitrate") is not None:
                    desired_bitrate = int(meta["bitrate"])
                    if ch.bitrate != desired_bitrate:
                        changes["bitrate"] = desired_bitrate
                        voice_changed_here = True

                if meta.get("user_limit") is not None:
                    desired_limit = int(meta["user_limit"])
                    if ch.user_limit != desired_limit:
                        changes["user_limit"] = desired_limit
                        voice_changed_here = True

                if "rtc_region" in meta and meta["rtc_region"] is not None:
                    raw_desired = meta["rtc_region"]
                    desired_norm = _normalize_region(raw_desired)
                    current_norm = _normalize_region(getattr(ch, "rtc_region", None))
                    if current_norm != desired_norm:
                        changes["rtc_region"] = (
                            None if desired_norm == "auto" else raw_desired
                        )
                        voice_changed_here = True

                if meta.get("video_quality") is not None:
                    desired_vq = meta["video_quality"] or "auto"
                    current_vq = _norm_video_quality(
                        getattr(ch, "video_quality_mode", None)
                    )
                    if current_vq != desired_vq:
                        changes["video_quality_mode"] = _parse_video_quality(desired_vq)
                        voice_changed_here = True

            if voice_changed_here:
                voice_updated += 1

            stage_changed_here = False
            if (
                has_community
                and clone_stage
                and clone_stage_props
                and is_stage_ch
                and orig_id in stage_meta
            ):
                meta = stage_meta[orig_id]

                if meta.get("bitrate") is not None:
                    desired_bitrate = int(meta["bitrate"])
                    if ch.bitrate != desired_bitrate:
                        changes["bitrate"] = desired_bitrate
                        stage_changed_here = True

                if meta.get("user_limit") is not None:
                    desired_limit = int(meta["user_limit"])
                    if ch.user_limit != desired_limit:
                        changes["user_limit"] = desired_limit
                        stage_changed_here = True

                if "rtc_region" in meta and meta["rtc_region"] is not None:
                    raw_desired = meta["rtc_region"]
                    desired_norm = _normalize_region(raw_desired)
                    current_norm = _normalize_region(getattr(ch, "rtc_region", None))
                    if current_norm != desired_norm:
                        changes["rtc_region"] = (
                            None if desired_norm == "auto" else raw_desired
                        )
                        stage_changed_here = True

                if meta.get("topic") is not None:
                    desired_topic = meta["topic"]
                    current_topic = getattr(ch, "topic", None)
                    if current_topic != desired_topic:
                        changes["topic"] = desired_topic
                        stage_changed_here = True

                if meta.get("video_quality") is not None:
                    desired_vq = meta["video_quality"] or "auto"
                    current_vq = _norm_video_quality(
                        getattr(ch, "video_quality_mode", None)
                    )
                    if current_vq != desired_vq:
                        changes["video_quality_mode"] = _parse_video_quality(desired_vq)
                        stage_changed_here = True

            forum_changed_here = False
            if sync_forum_props and is_forum_ch and orig_id in forum_meta:
                meta = forum_meta[orig_id]

                def _has_unmoderated_tag(tag_seq) -> bool:
                    """Return True if any tag in the sequence is non-moderated."""
                    for t in tag_seq or []:
                        if not bool(getattr(t, "moderated", False)):
                            return True
                    return False

                if meta.get("topic") is not None and hasattr(ch, "topic"):
                    desired_topic = meta["topic"]
                    current_topic = getattr(ch, "topic", None)
                    if current_topic != desired_topic:
                        changes.setdefault("topic", desired_topic)
                        forum_changed_here = True

                if "default_thread_slowmode_delay" in meta and hasattr(
                    ch, "default_thread_slowmode_delay"
                ):
                    desired_msg_limit = max(
                        0,
                        min(21600, int(meta["default_thread_slowmode_delay"] or 0)),
                    )
                    current_msg_limit = int(
                        getattr(ch, "default_thread_slowmode_delay", 0) or 0
                    )
                    if current_msg_limit != desired_msg_limit:
                        changes["default_thread_slowmode_delay"] = desired_msg_limit
                        forum_changed_here = True

                if "default_layout" in meta:
                    desired_layout = _enum_int(meta["default_layout"], None)
                    if desired_layout is not None:
                        current_layout: Optional[int] = None

                        if hasattr(ch, "default_layout"):
                            current_layout = _enum_int(
                                getattr(ch, "default_layout", None),
                                None,
                            )

                        if current_layout is None:
                            current_layout = await _fetch_forum_layout(ch.id)

                        logger.debug(
                            "[meta] Forum layout compare for #%d (%s): host=%r clone=%r",
                            ch.id,
                            getattr(ch, "name", "?"),
                            desired_layout,
                            current_layout,
                        )

                        if current_layout != desired_layout:
                            changes["_raw_forum_layout"] = desired_layout
                            forum_changed_here = True

                if "default_sort_order" in meta and hasattr(ch, "default_sort_order"):
                    desired_sort = _enum_int(meta["default_sort_order"], None)
                    current_sort = _enum_int(
                        getattr(ch, "default_sort_order", None), None
                    )
                    if desired_sort is not None and current_sort != desired_sort:
                        changes["default_sort_order"] = desired_sort
                        forum_changed_here = True

                if "default_auto_archive_duration" in meta and hasattr(
                    ch, "default_auto_archive_duration"
                ):
                    desired_arch = int(meta["default_auto_archive_duration"] or 0)
                    current_arch = int(
                        getattr(ch, "default_auto_archive_duration", 0) or 0
                    )
                    if current_arch != desired_arch:
                        changes["default_auto_archive_duration"] = desired_arch
                        forum_changed_here = True

                src_tags_meta = meta.get("available_tags") or []
                try:
                    clone_tags = list(getattr(ch, "available_tags", []) or [])
                except Exception:
                    clone_tags = []

                have_require_meta = "require_tag" in meta
                desired_req = bool(meta["require_tag"]) if have_require_meta else None

                host_canon = _canon_host_tags(src_tags_meta, desired_req)
                clone_canon = _canon_clone_tags(clone_tags)

                tags_already_match = bool(src_tags_meta) and host_canon == clone_canon

                desired_tags = []
                tags_changed = False

                if tags_already_match:
                    desired_tags = list(clone_tags)
                elif src_tags_meta:
                    for tmeta in src_tags_meta:
                        name = (tmeta.get("name") or "").strip()
                        if not name:
                            continue

                        moderated = bool(tmeta.get("moderated", False))

                        emoji_name = tmeta.get("emoji_name") or ""
                        emoji_str = emoji_name or "🏷️"

                        try:
                            new_tag_obj = discord.ForumTag(
                                name=name,
                                emoji=emoji_str,
                                moderated=moderated,
                            )
                        except Exception:
                            try:
                                new_tag_obj = discord.ForumTag(
                                    name=name,
                                    emoji="🏷️",
                                    moderated=moderated,
                                )
                            except Exception:
                                new_tag_obj = None

                        if new_tag_obj is not None:
                            desired_tags.append(new_tag_obj)

                    tags_changed = True
                else:
                    if clone_tags:
                        desired_tags = list(clone_tags)

                if (
                    have_require_meta
                    and desired_req
                    and not _has_unmoderated_tag(desired_tags)
                ):
                    try:
                        existing_names = {
                            (getattr(t, "name", "") or "").strip().lower()
                            for t in desired_tags
                        }
                        if "general" not in existing_names:
                            fallback_tag = discord.ForumTag(
                                name="General",
                                emoji="🏷️",
                                moderated=False,
                            )
                            desired_tags.append(fallback_tag)
                            tags_changed = True
                    except Exception as tag_err:
                        logger.warning(
                            "[meta] Failed to create fallback ForumTag on #%d (%s): %s",
                            ch.id,
                            ch.name,
                            tag_err,
                        )

                if tags_changed:
                    final_tags = desired_tags[:20]
                    changes["available_tags"] = final_tags
                    forum_changed_here = True
                else:
                    final_tags = desired_tags or clone_tags

                if "default_reaction" in meta:
                    desired_norm = _norm_forum_emoji(meta["default_reaction"])
                    current_norm = _norm_forum_emoji(
                        getattr(ch, "default_reaction_emoji", None)
                    )
                    if current_norm != desired_norm:
                        payload = _build_forum_emoji(desired_norm)
                        if payload is not None:
                            changes["default_reaction_emoji"] = payload
                            forum_changed_here = True

                if have_require_meta:
                    flags = getattr(ch, "flags", None)
                    if flags is not None:
                        current_req = bool(getattr(flags, "require_tag", False))
                    else:
                        current_req = bool(getattr(ch, "requires_tag", False))

                    if desired_req != current_req:
                        if desired_req and not _has_unmoderated_tag(final_tags):
                            logger.warning(
                                "[meta] Cannot enable require_tag for forum #%d (%s): "
                                "no non-moderated tags available after tag sync.",
                                ch.id,
                                ch.name,
                            )
                        else:
                            require_tag_ops.append((ch, bool(desired_req)))
                            forum_changed_here = True

            if stage_changed_here:
                stage_updated += 1
            if forum_changed_here:
                forum_updated += 1

            raw_forum_layout = changes.pop("_raw_forum_layout", None)

            if not changes and raw_forum_layout is None:
                continue

            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )

                if changes:
                    await ch.edit(**changes)

                if raw_forum_layout is not None and is_forum_ch:
                    await _raw_patch_forum_layout(ch.id, int(raw_forum_layout))

                log_items = list(changes.items())
                if raw_forum_layout is not None:
                    log_items.append(("default_forum_layout", raw_forum_layout))
                log_str = (
                    ", ".join(f"{k}={v}" for k, v in log_items)
                    or "(raw default_forum_layout only)"
                )

                if is_voice_ch and voice_changed_here:
                    logger.info(
                        "[🔊] Updated voice metadata for '%s' #%d: %s",
                        ch.name,
                        ch.id,
                        log_str,
                    )
                elif is_stage_ch and stage_changed_here:
                    logger.info(
                        "[🎭] Updated stage metadata for '%s' #%d: %s",
                        ch.name,
                        ch.id,
                        log_str,
                    )
                elif is_forum_ch and forum_changed_here:
                    logger.info(
                        "[🧵] Updated forum metadata for '%s' #%d: %s",
                        ch.name,
                        ch.id,
                        log_str,
                    )
                else:
                    logger.info(
                        "[📝] Updated channel metadata for '%s' #%d: %s",
                        ch.name,
                        ch.id,
                        log_str,
                    )

            except Exception as e:
                logger.warning(
                    "[⚠️] Failed to update metadata for channel #%d: %s", ch.id, e
                )

        for ch, desired_req in require_tag_ops:
            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )

                flags = getattr(ch, "flags", None)
                flag_changes: Dict[str, object] = {}

                if flags is not None and hasattr(discord, "ChannelFlags"):
                    new_flags = discord.ChannelFlags._from_value(flags.value)
                    new_flags.require_tag = bool(desired_req)
                    flag_changes["flags"] = new_flags.value
                else:
                    flag_changes["require_tag"] = bool(desired_req)

                await ch.edit(**flag_changes)
                logger.info(
                    "[🧵] Set require_tag=%s for forum '%s' #%d",
                    desired_req,
                    ch.name,
                    ch.id,
                )
            except Exception as e:
                logger.warning(
                    "[meta] Failed to apply require_tag=%s for forum #%d (%s): %s",
                    desired_req,
                    ch.id,
                    ch.name,
                    e,
                )

        if nsfw_updated:
            parts.append(f"Updated NSFW flag on {nsfw_updated} channels")
        if topic_updated:
            parts.append(f"Updated topic on {topic_updated} channels")
        if slowmode_updated:
            parts.append(f"Updated slowmode on {slowmode_updated} channels")
        if voice_updated:
            parts.append(f"Updated voice properties on {voice_updated} channels")
        if stage_updated:
            parts.append(f"Updated stage properties on {stage_updated} channels")
        if forum_updated:
            parts.append(f"Updated forum properties on {forum_updated} channels")

        return parts

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
        Ensure forum channels exist and are mapped for THIS clone guild only.
        - Creates forum channels as needed.
        - Ensures a webhook exists and is STORED in channel_mappings.
        - Keeps per-clone and global caches in sync.
        """
        parts: List[str] = []
        host_guild_id = int((sitemap.get("guild") or {}).get("id") or 0)

        self._load_mappings()

        forums = list(sitemap.get("forums") or [])
        if not forums:
            return parts

        def _cat_name_from_sitemap(cat_id: int | None) -> str | None:
            if not cat_id:
                return None
            for cat in sitemap.get("categories") or []:
                try:
                    if int(cat.get("id") or 0) == int(cat_id):
                        return cat.get("name")
                except Exception:
                    continue
            return None

        per_clone = (getattr(self, "chan_map_by_clone", {}) or {}).setdefault(
            int(guild.id), {}
        )

        created = 0
        renamed = 0

        async def _ensure_forum_webhook_url(
            orig_id: int,
            name: str,
            ch: discord.ForumChannel,
            parent_id: int | None,
            cloned_parent_id: int | None,
            existing_url: str | None,
        ) -> str | None:
            url = existing_url
            if not url:

                wh = await self._create_webhook_safely(
                    ch, "Copycord", await self._get_default_avatar_bytes()
                )
                if wh and getattr(wh, "id", None) and getattr(wh, "token", None):
                    url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"
                else:
                    url = None

            self.db.upsert_channel_mapping(
                original_channel_id=orig_id,
                original_channel_name=name,
                cloned_channel_id=int(ch.id),
                channel_webhook_url=url,
                original_parent_category_id=int(parent_id) if parent_id else None,
                cloned_parent_category_id=cloned_parent_id,
                channel_type=ChannelType.forum.value,
                original_guild_id=host_guild_id,
                cloned_guild_id=int(guild.id),
            )

            entry = {
                "original_channel_id": orig_id,
                "original_channel_name": name,
                "cloned_channel_id": int(ch.id),
                "channel_webhook_url": url,
                "original_parent_category_id": int(parent_id) if parent_id else None,
                "cloned_parent_category_id": cloned_parent_id,
                "channel_type": ChannelType.forum.value,
                "original_guild_id": host_guild_id,
                "cloned_guild_id": int(guild.id),
            }
            per_clone[orig_id] = dict(entry)

            self.chan_map[orig_id] = dict(entry)
            return url

        for f in forums:
            orig_id = int(f["id"])
            name = f.get("name") or "forum"
            parent_id = int(f.get("category_id") or 0) or None
            parent_nm = _cat_name_from_sitemap(parent_id) or "Text Channels"

            row = per_clone.get(orig_id)
            if row is None:
                row = self.db.get_channel_mapping_by_original_and_clone(
                    orig_id, int(guild.id)
                )
            row = self._rowdict(row)

            clone_id = (
                int(row["cloned_channel_id"])
                if row and row.get("cloned_channel_id")
                else 0
            )
            ch = guild.get_channel(clone_id) if clone_id else None

            if parent_id:
                cat, _ = await self._ensure_category(
                    guild, int(parent_id), parent_nm, host_guild_id
                )
                cloned_parent_id = int(getattr(cat, "id", 0)) or None
            else:
                cloned_parent_id = None

            if not ch:

                await self.ratelimit.acquire_for_guild(
                    ActionType.CREATE_CHANNEL, guild.id
                )
                ch = await guild.create_forum_channel(
                    name=name,
                    category=(
                        guild.get_channel(cloned_parent_id)
                        if cloned_parent_id
                        else None
                    ),
                )
                logger.info("[➕] Created forum channel '%s' #%d", name, ch.id)
                created += 1

                await _ensure_forum_webhook_url(
                    orig_id=orig_id,
                    name=name,
                    ch=ch,
                    parent_id=parent_id,
                    cloned_parent_id=cloned_parent_id,
                    existing_url=None,
                )

            else:

                if (ch.name or "") != name:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, guild.id
                    )
                    await ch.edit(name=name)
                    renamed += 1
                    logger.info("[✏️] Renamed forum #%d to %s", ch.id, name)

                want_parent = (
                    guild.get_channel(cloned_parent_id) if cloned_parent_id else None
                )
                if want_parent and getattr(ch, "category_id", None) != cloned_parent_id:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, guild.id
                    )
                    await ch.edit(category=want_parent)
                    logger.info(
                        "[📁] Moved forum #%d under category #%d",
                        ch.id,
                        cloned_parent_id,
                    )

                await _ensure_forum_webhook_url(
                    orig_id=orig_id,
                    name=name,
                    ch=ch,
                    parent_id=parent_id,
                    cloned_parent_id=cloned_parent_id,
                    existing_url=(row or {}).get("channel_webhook_url"),
                )

        if created:
            parts.append(
                f"Created {created} forum channel{'s' if created != 1 else ''}"
            )
        if renamed:
            parts.append(
                f"Renamed {renamed} forum channel{'s' if renamed != 1 else ''}"
            )
        return parts

    def _parse_video_quality_mode(self, value: str | int | None):
        try:
            if value is None:
                return None
            if isinstance(value, discord.VideoQualityMode):
                return value
            s = str(value).lower()
            if "full" in s or s == "2":
                return discord.VideoQualityMode.full
            if "auto" in s or s in ("1", "0", ""):
                return discord.VideoQualityMode.auto
        except Exception:
            pass
        return None

    async def _ensure_voice_channel_and_webhook(
        self,
        host_guild_id: Optional[int],
        guild: discord.Guild,
        original_id: int,
        original_name: str,
        parent_id: Optional[int],
        parent_name: Optional[str],
        channel_type: int,
        bitrate: Optional[int] = None,
        user_limit: Optional[int] = None,
        rtc_region: Optional[str] = None,
        video_quality: str | int | None = None,
    ) -> Tuple[int, int, str]:
        """
        Ensure a voice channel exists with webhook for its text chat.
        """
        if self._shutting_down:
            return

        category = None
        if parent_id is not None:
            category, _ = await self._ensure_category(
                guild, parent_id, parent_name, host_guild_id
            )

        per = self.chan_map_by_clone.setdefault(int(guild.id), {})
        row = per.get(int(original_id))
        if not row:
            try:
                row = self.db.get_channel_mapping_by_original_and_clone(
                    int(original_id), int(guild.id)
                )
            except Exception:
                row = None

        if row is not None and not isinstance(row, dict):
            row = dict(row)
            per[int(original_id)] = row

        if row:
            clone_id = int(row.get("cloned_channel_id") or 0)
            wh_url = row.get("channel_webhook_url")
            ch = guild.get_channel(clone_id) if clone_id else None

            if ch:
                if not wh_url:
                    wh = await self._create_webhook_safely(
                        ch, "Copycord", await self._get_default_avatar_bytes()
                    )
                    wh_url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"
                    self.db.upsert_channel_mapping(
                        int(original_id),
                        original_name,
                        int(clone_id),
                        wh_url,
                        int(parent_id) if parent_id is not None else None,
                        int(category.id) if category else None,
                        int(channel_type),
                        original_guild_id=(
                            int(host_guild_id) if host_guild_id is not None else None
                        ),
                        cloned_guild_id=int(guild.id),
                    )

                per[int(original_id)] = {
                    "original_channel_id": int(original_id),
                    "original_channel_name": original_name,
                    "cloned_channel_id": int(clone_id),
                    "channel_webhook_url": wh_url,
                    "original_parent_category_id": (
                        int(parent_id) if parent_id is not None else None
                    ),
                    "cloned_parent_category_id": int(category.id) if category else None,
                    "channel_type": int(channel_type),
                    "original_guild_id": (
                        int(host_guild_id) if host_guild_id is not None else None
                    ),
                    "cloned_guild_id": int(guild.id),
                }
                self._schedule_flush(
                    chan_ids={int(original_id)}, thread_parent_ids={int(original_id)}
                )
                self._unmapped_warned.discard(int(original_id))
                return int(original_id), int(clone_id), str(wh_url)

            try:
                self.db.delete_channel_mapping_pair(int(original_id), int(guild.id))
            except Exception:
                self.db.delete_channel_mapping(int(original_id))
            per.pop(int(original_id), None)

        ch = await self._create_channel(guild, "voice", original_name, category)

        voice_changes: dict[str, object] = {}
        if bitrate is not None:
            voice_changes["bitrate"] = int(bitrate)
        if user_limit is not None:
            voice_changes["user_limit"] = int(user_limit)
        if rtc_region is not None:
            voice_changes["rtc_region"] = rtc_region

        vmode = self._parse_video_quality_mode(video_quality)
        if vmode is not None:
            voice_changes["video_quality_mode"] = vmode

        if voice_changes:
            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await ch.edit(**voice_changes)
                logger.info(
                    "[🔊] Set voice properties for '%s' #%d: %s",
                    original_name,
                    ch.id,
                    ", ".join(f"{k}={v}" for k, v in voice_changes.items()),
                )
            except Exception as e:
                logger.warning(
                    "[⚠️] Failed to set voice properties for new channel #%d: %s",
                    ch.id,
                    e,
                )

        wh = await self._create_webhook_safely(
            ch, "Copycord", await self._get_default_avatar_bytes()
        )
        url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

        self.db.upsert_channel_mapping(
            int(original_id),
            original_name,
            int(ch.id),
            url,
            int(parent_id) if parent_id is not None else None,
            int(category.id) if category else None,
            int(channel_type),
            original_guild_id=int(host_guild_id) if host_guild_id is not None else None,
            cloned_guild_id=int(guild.id),
        )

        per[int(original_id)] = {
            "original_channel_id": int(original_id),
            "original_channel_name": original_name,
            "cloned_channel_id": int(ch.id),
            "channel_webhook_url": url,
            "original_parent_category_id": (
                int(parent_id) if parent_id is not None else None
            ),
            "cloned_parent_category_id": int(category.id) if category else None,
            "channel_type": int(channel_type),
            "original_guild_id": (
                int(host_guild_id) if host_guild_id is not None else None
            ),
            "cloned_guild_id": int(guild.id),
        }
        self._schedule_flush(
            chan_ids={int(original_id)}, thread_parent_ids={int(original_id)}
        )
        self._unmapped_warned.discard(int(original_id))
        return int(original_id), int(ch.id), str(url)

    async def _ensure_stage_channel_and_webhook(
        self,
        host_guild_id: Optional[int],
        guild: discord.Guild,
        original_id: int,
        original_name: str,
        parent_id: Optional[int],
        parent_name: Optional[str],
        channel_type: int,
        bitrate: Optional[int] = None,
        user_limit: Optional[int] = None,
        rtc_region: Optional[str] = None,
        topic: Optional[str] = None,
        video_quality: str | int | None = None,
    ) -> Tuple[int, int, str]:
        """
        Ensure a stage channel exists with webhook for its text chat.
        Similar to voice channels but for stage channels.
        """
        if self._shutting_down:
            return

        if "COMMUNITY" not in guild.features:
            logger.warning(
                "[⚠️] Cannot create stage channel '%s' in guild %s: COMMUNITY feature not enabled",
                original_name,
                guild.id,
            )
            return None, None, None

        category = None
        if parent_id is not None:
            category, _ = await self._ensure_category(
                guild, parent_id, parent_name, host_guild_id
            )

        per = self.chan_map_by_clone.setdefault(int(guild.id), {})
        row = per.get(int(original_id))
        if not row:
            try:
                row = self.db.get_channel_mapping_by_original_and_clone(
                    int(original_id), int(guild.id)
                )
            except Exception:
                row = None

        if row is not None and not isinstance(row, dict):
            row = dict(row)
            per[int(original_id)] = row

        if row:
            clone_id = int(row.get("cloned_channel_id") or 0)
            wh_url = row.get("channel_webhook_url")
            ch = guild.get_channel(clone_id) if clone_id else None

            if ch:
                if not wh_url:
                    wh = await self._create_webhook_safely(
                        ch, "Copycord", await self._get_default_avatar_bytes()
                    )
                    wh_url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"
                    self.db.upsert_channel_mapping(
                        int(original_id),
                        original_name,
                        int(clone_id),
                        wh_url,
                        int(parent_id) if parent_id is not None else None,
                        int(category.id) if category else None,
                        int(channel_type),
                        original_guild_id=(
                            int(host_guild_id) if host_guild_id is not None else None
                        ),
                        cloned_guild_id=int(guild.id),
                    )

                per[int(original_id)] = {
                    "original_channel_id": int(original_id),
                    "original_channel_name": original_name,
                    "cloned_channel_id": int(clone_id),
                    "channel_webhook_url": wh_url,
                    "original_parent_category_id": (
                        int(parent_id) if parent_id is not None else None
                    ),
                    "cloned_parent_category_id": int(category.id) if category else None,
                    "channel_type": int(channel_type),
                    "original_guild_id": (
                        int(host_guild_id) if host_guild_id is not None else None
                    ),
                    "cloned_guild_id": int(guild.id),
                }
                self._schedule_flush(
                    chan_ids={int(original_id)}, thread_parent_ids={int(original_id)}
                )
                self._unmapped_warned.discard(int(original_id))
                return int(original_id), int(clone_id), str(wh_url)

            try:
                self.db.delete_channel_mapping_pair(int(original_id), int(guild.id))
            except Exception:
                self.db.delete_channel_mapping(int(original_id))
            per.pop(int(original_id), None)

        ch = await self._create_channel(
            guild, "stage", original_name, category, topic=topic
        )

        if ch is None:
            logger.warning(
                "[⚠️] Failed to create stage channel '%s' in guild %s",
                original_name,
                guild.id,
            )
            return None, None, None

        stage_changes: dict[str, object] = {}
        if bitrate is not None:
            stage_changes["bitrate"] = int(bitrate)
        if user_limit is not None:
            stage_changes["user_limit"] = int(user_limit)
        if rtc_region is not None:
            stage_changes["rtc_region"] = rtc_region
        if topic is not None:
            stage_changes["topic"] = topic

        vmode = self._parse_video_quality_mode(video_quality)
        if vmode is not None:
            stage_changes["video_quality_mode"] = vmode

        if stage_changes:
            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await ch.edit(**stage_changes)
                logger.info(
                    "[🎭] Set stage properties for '%s' #%d: %s",
                    original_name,
                    ch.id,
                    ", ".join(f"{k}={v}" for k, v in stage_changes.items()),
                )
            except Exception as e:
                logger.warning(
                    "[⚠️] Failed to set stage properties for new channel #%d: %s",
                    ch.id,
                    e,
                )

        wh = await self._create_webhook_safely(
            ch, "Copycord", await self._get_default_avatar_bytes()
        )
        url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

        self.db.upsert_channel_mapping(
            int(original_id),
            original_name,
            int(ch.id),
            url,
            int(parent_id) if parent_id is not None else None,
            int(category.id) if category else None,
            int(channel_type),
            original_guild_id=int(host_guild_id) if host_guild_id is not None else None,
            cloned_guild_id=int(guild.id),
        )

        per[int(original_id)] = {
            "original_channel_id": int(original_id),
            "original_channel_name": original_name,
            "cloned_channel_id": int(ch.id),
            "channel_webhook_url": url,
            "original_parent_category_id": (
                int(parent_id) if parent_id is not None else None
            ),
            "cloned_parent_category_id": int(category.id) if category else None,
            "channel_type": int(channel_type),
            "original_guild_id": (
                int(host_guild_id) if host_guild_id is not None else None
            ),
            "cloned_guild_id": int(guild.id),
        }
        self._schedule_flush(
            chan_ids={int(original_id)}, thread_parent_ids={int(original_id)}
        )
        self._unmapped_warned.discard(int(original_id))
        return int(original_id), int(ch.id), str(url)

    async def _sync_channels(
        self,
        guild: Guild,
        sitemap: Dict,
        *,
        stage_only: bool = False,
        skip_removed: bool = False,
    ) -> List[str]:
        """
        Synchronizes the channels of a guild with the provided sitemap.
        """
        parts: List[str] = []
        incoming = self._parse_sitemap(sitemap)
        host_guild_id = (sitemap.get("guild") or {}).get("id")

        try:
            settings = resolve_mapping_settings(
                self.db,
                self.config,
                original_guild_id=int(host_guild_id) if host_guild_id else None,
                cloned_guild_id=int(guild.id),
            )
        except Exception:
            settings = self.config.default_mapping_settings()

        clone_voice = settings.get("CLONE_VOICE", False)
        clone_voice_properties = settings.get("CLONE_VOICE_PROPERTIES", False)
        clone_stage = settings.get("CLONE_STAGE", False)
        clone_stage_properties = settings.get("CLONE_STAGE_PROPERTIES", False)

        has_community = "COMMUNITY" in guild.features

        rem = 0
        if not skip_removed:
            rem = await self._handle_removed_channels(guild, incoming)
            if rem:
                parts.append(f"Deleted {rem} channels")

        created = renamed = converted = 0
        voice_skipped = stage_skipped = 0

        for item in incoming:
            orig, name, pid, pname, ctype = (
                item["id"],
                item["name"],
                item["parent_id"],
                item["parent_name"],
                item["type"],
            )

            per = self.chan_map_by_clone.setdefault(int(guild.id), {})
            mrow = per.get(
                int(orig)
            ) or self.db.get_channel_mapping_by_original_and_clone(
                int(orig), int(guild.id)
            )
            if mrow is not None and not isinstance(mrow, dict):
                mrow = dict(mrow)
                per[int(orig)] = mrow

            is_new = not (
                mrow
                and mrow.get("cloned_channel_id")
                and guild.get_channel(int(mrow["cloned_channel_id"]))
            )

            is_voice = ctype == ChannelType.voice.value
            is_stage = ctype == ChannelType.stage_voice.value

            if stage_only and not is_stage:
                continue

            if is_voice:
                if not clone_voice:
                    voice_skipped += 1
                    logger.debug(
                        "[🔇] Skipping voice channel '%s' for clone_g=%s (CLONE_VOICE=False)",
                        name,
                        guild.id,
                    )
                    continue

                bitrate = item.get("bitrate") if clone_voice_properties else None
                user_limit = item.get("user_limit") if clone_voice_properties else None
                rtc_region = item.get("rtc_region") if clone_voice_properties else None
                video_quality = (
                    item.get("video_quality") if clone_voice_properties else None
                )

                _, clone_id, _ = await self._ensure_voice_channel_and_webhook(
                    host_guild_id,
                    guild,
                    orig,
                    name,
                    pid,
                    pname,
                    ctype,
                    bitrate=bitrate,
                    user_limit=user_limit,
                    rtc_region=rtc_region,
                    video_quality=video_quality,
                )

                if is_new:
                    created += 1

                ch = guild.get_channel(clone_id)
                if not ch:
                    continue

                did_rename, _reason = await self._maybe_rename_channel(ch, name, orig)
                if did_rename:
                    renamed += 1

            elif is_stage:
                if not clone_stage:
                    stage_skipped += 1
                    logger.debug(
                        "[🎭] Skipping stage channel '%s' for clone_g=%s (CLONE_STAGE=False)",
                        name,
                        guild.id,
                    )
                    continue

                if not has_community:
                    if not stage_only:
                        logger.debug(
                            "[🎭] Deferring stage channel '%s' for clone_g=%s until after community sync",
                            name,
                            guild.id,
                        )
                        continue

                    stage_skipped += 1
                    logger.warning(
                        "[⚠️] Skipping stage channel '%s' for clone_g=%s (COMMUNITY not enabled even after community sync)",
                        name,
                        guild.id,
                    )
                    continue

                bitrate = item.get("bitrate") if clone_stage_properties else None
                user_limit = item.get("user_limit") if clone_stage_properties else None
                rtc_region = item.get("rtc_region") if clone_stage_properties else None
                topic = item.get("topic") if clone_stage_properties else None
                video_quality = (
                    item.get("video_quality") if clone_stage_properties else None
                )

                result = await self._ensure_stage_channel_and_webhook(
                    host_guild_id,
                    guild,
                    orig,
                    name,
                    pid,
                    pname,
                    ctype,
                    bitrate=bitrate,
                    user_limit=user_limit,
                    rtc_region=rtc_region,
                    topic=topic,
                    video_quality=video_quality,
                )

                if result is None or result == (None, None, None):
                    stage_skipped += 1
                    continue

                _, clone_id, _ = result

                if is_new:
                    created += 1

                ch = guild.get_channel(clone_id)
                if not ch:
                    continue

                did_rename, _reason = await self._maybe_rename_channel(ch, name, orig)
                if did_rename:
                    renamed += 1

            else:
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
                        logger.info(
                            "[✏️] Converted channel '%s' (ID %d) to Announcement",
                            ch.name,
                            ch.id,
                        )

                        mrow = per.get(int(orig)) or mrow or {}
                        if not isinstance(mrow, dict):
                            mrow = dict(mrow)

                        self.db.upsert_channel_mapping(
                            orig,
                            mrow.get("original_channel_name", name),
                            ch.id,
                            mrow.get("channel_webhook_url"),
                            mrow.get("original_parent_category_id"),
                            mrow.get("cloned_parent_category_id"),
                            ChannelType.news.value,
                            original_guild_id=host_guild_id,
                            cloned_guild_id=guild.id,
                            clone_name=mrow.get("clone_channel_name") or None,
                        )

                        per[int(orig)] = {
                            "original_channel_id": int(orig),
                            "original_channel_name": mrow.get(
                                "original_channel_name", name
                            ),
                            "cloned_channel_id": int(ch.id),
                            "channel_webhook_url": mrow.get("channel_webhook_url"),
                            "original_parent_category_id": mrow.get(
                                "original_parent_category_id"
                            ),
                            "cloned_parent_category_id": mrow.get(
                                "cloned_parent_category_id"
                            ),
                            "channel_type": ChannelType.news.value,
                            "original_guild_id": host_guild_id,
                            "cloned_guild_id": guild.id,
                            "clone_channel_name": mrow.get("clone_channel_name")
                            or None,
                        }
                        self.chan_map[int(orig)] = dict(per[int(orig)])

                did_rename, _reason = await self._maybe_rename_channel(ch, name, orig)
                if did_rename:
                    renamed += 1

        if created:
            parts.append(f"Created {created} channels")
        if converted:
            parts.append(f"Converted {converted} channels to Announcement")
        if renamed:
            parts.append(f"Renamed {renamed} channels")
        if voice_skipped:
            logger.debug(
                "[🔇] Skipped %d voice channels for clone_g=%s (CLONE_VOICE=False)",
                voice_skipped,
                guild.id,
            )
        if stage_skipped:
            logger.debug(
                "[🎭] Skipped %d stage channels for clone_g=%s (CLONE_STAGE=False or no COMMUNITY)",
                stage_skipped,
                guild.id,
            )

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
                    logger.warning(
                        "[🗑️] Original thread '%s' no longer exists; clearing mapping (clone=%s)",
                        thread_name,
                        clone_id,
                    )

                    if clone_ch and getattr(self.config, "DELETE_THREADS", False):
                        await self.ratelimit.acquire_for_guild(
                            ActionType.DELETE_CHANNEL, guild.id
                        )
                        await clone_ch.delete()
                        logger.info("[🗑️] Deleted cloned thread %s", clone_id)
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

    def _parse_sitemap(self, sitemap: dict) -> list[dict]:
        items = []

        for cat in sitemap.get("categories", []):
            for ch in cat.get("channels", []):
                items.append(
                    {
                        "id": ch["id"],
                        "name": ch["name"],
                        "parent_id": cat["id"],
                        "parent_name": cat["name"],
                        "type": ch["type"],
                        "bitrate": ch.get("bitrate"),
                        "user_limit": ch.get("user_limit"),
                        "rtc_region": ch.get("rtc_region"),
                        "video_quality": ch.get("video_quality"),
                    }
                )

        for ch in sitemap.get("standalone_channels", []):
            items.append(
                {
                    "id": ch["id"],
                    "name": ch["name"],
                    "parent_id": None,
                    "parent_name": None,
                    "type": ch["type"],
                    "bitrate": ch.get("bitrate"),
                    "user_limit": ch.get("user_limit"),
                    "rtc_region": ch.get("rtc_region"),
                    "video_quality": ch.get("video_quality"),
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
                    "bitrate": None,
                    "user_limit": None,
                    "rtc_region": None,
                    "video_quality": None,
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
        self,
        guild: Guild,
        kind: str,
        name: str,
        category: CategoryChannel | None,
        topic: str | None = None,
    ) -> Union[TextChannel, ForumChannel, discord.VoiceChannel, discord.StageChannel]:
        """
        Create a channel of `kind` ('text'|'news'|'forum'|'voice'|'stage') named `name` under
        `category`.  If the category or guild is at capacity, it falls back to
        standalone (category=None).  Returns the created channel object.
        """
        if self._shutting_down:
            return
        if not self._can_create_in_category(guild, category):
            cat_label = category.name if category else "<root>"
            logger.warning(
                "[⚠️] Category %s full for guild %s (or guild at cap); creating '%s' as standalone",
                guild.id,
                cat_label,
                name,
            )
            category = None

        if kind == "forum":
            await self.ratelimit.acquire_for_guild(ActionType.CREATE_CHANNEL, guild.id)
            ch = await guild.create_forum_channel(name=name, category=category)
        elif kind == "voice":
            await self.ratelimit.acquire_for_guild(ActionType.CREATE_CHANNEL, guild.id)
            ch = await guild.create_voice_channel(name=name, category=category)
        elif kind == "stage":
            if "COMMUNITY" not in guild.features:
                logger.warning(
                    "[⚠️] Cannot create stage channel '%s': guild %s doesn't have COMMUNITY enabled",
                    name,
                    guild.id,
                )
                return None
            else:
                await self.ratelimit.acquire_for_guild(
                    ActionType.CREATE_CHANNEL, guild.id
                )

                ch = await guild.create_stage_channel(
                    name=name,
                    topic=topic if topic else "Stage Channel",
                    category=category,
                )
        else:
            await self.ratelimit.acquire_for_guild(ActionType.CREATE_CHANNEL, guild.id)
            ch = await guild.create_text_channel(name=name, category=category)

        logger.info("[➕] Created %s channel '%s' #%s", kind, name, ch.id)

        if kind == "news":
            if "NEWS" in guild.features:
                try:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.EDIT_CHANNEL, guild.id
                    )
                    await ch.edit(type=ChannelType.news)
                    logger.info("[✏️] Converted '%s' #%d to Announcement", name, ch.id)
                except HTTPException as e:
                    logger.warning(
                        "[⚠️] Could not convert '%s' to Announcement in guild %s: %s; left as text",
                        name,
                        guild.id,
                        e,
                    )
            else:
                logger.warning(
                    "[⚠️] Guild %s doesn't support NEWS; '%s' left as text",
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
        Enforce per-clone pinned name (channel_mappings.clone_channel_name);
        otherwise match upstream IF RENAME_CHANNELS is enabled.
        """
        try:
            per_clone = (self.chan_map_by_clone or {}).get(int(ch.guild.id), {}) or {}
            mapping = per_clone.get(int(orig_source_id))

            if mapping is None or "clone_channel_name" not in mapping:
                dbrow = self.db.get_channel_mapping_by_original_and_clone(
                    int(orig_source_id), int(ch.guild.id)
                )
                if dbrow:
                    mapping = dict(dbrow)
                    for k in (
                        "original_channel_id",
                        "cloned_channel_id",
                        "original_parent_category_id",
                        "cloned_parent_category_id",
                        "original_guild_id",
                        "cloned_guild_id",
                        "channel_type",
                    ):
                        if mapping.get(k) is not None:
                            try:
                                mapping[k] = int(mapping[k])
                            except Exception:
                                pass
                    self.chan_map_by_clone.setdefault(int(ch.guild.id), {})[
                        int(orig_source_id)
                    ] = mapping

            pinned_name = ((mapping or {}).get("clone_channel_name") or "").strip()
            has_pin = bool(pinned_name)

            try:
                settings = resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=int(mapping.get("original_guild_id") or 0),
                    cloned_guild_id=int(ch.guild.id),
                )
            except Exception:
                settings = self.config.default_mapping_settings()

            rename_enabled = settings.get("RENAME_CHANNELS", True)

            if mapping is not None:
                try:
                    self.db.upsert_channel_mapping(
                        int(orig_source_id),
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
                target = pinned_name
            elif rename_enabled:
                target = upstream_name.strip()
            else:
                return False, "skipped_rename_disabled"

            if (ch.name or "").strip() == target:
                return False, "skipped_already_ok"

            old_name = ch.name

            await self.ratelimit.acquire_for_guild(ActionType.EDIT_CHANNEL, ch.guild.id)
            await ch.edit(name=target)

            if has_pin:
                logger.info(
                    "[📌] Enforced pinned name on #%d: %r → %r", ch.id, old_name, target
                )
                return True, "pinned_enforced"
            else:
                logger.info("[✏️] Renamed channel #%d: %r → %r", ch.id, old_name, target)
                return True, "match_upstream"

        except Exception:
            logger.debug("[rename] _maybe_rename_channel error", exc_info=True)
            return False, "skipped_error"

    async def _handle_removed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        """
        For THIS clone guild only:
        - Remove categories that no longer exist in the *filtered* sitemap
        (i.e. they were removed on the host or filtered out client-side).
        - If DELETE_CHANNELS is true for this clone, delete all cloned child channels and then
        the cloned category itself.
        - If DELETE_CHANNELS is false, keep the existing channels/category in the clone, but
        delete only the DB mappings so Copycord no longer tracks them.
        - ALWAYS delete channel mappings (and the category mapping) for this clone when a
        category is out-of-scope for this clone.
        """

        self._load_mappings()

        valid_ids: set[int] = {int(c["id"]) for c in (sitemap.get("categories") or [])}
        removed = 0
        removed_with_delete_channels = 0
        removed_mappings_only = 0

        per_all = getattr(self, "cat_map_by_clone", {}) or {}
        per_clone = dict(per_all.get(int(guild.id), {}) or {})

        for orig_id_key, raw_row in list(per_clone.items()):
            row = self._rowdict(raw_row)

            try:
                orig_id = int(orig_id_key)
            except Exception:
                try:
                    orig_id = int(row.get("original_category_id"))
                except Exception:
                    continue

            if int(row.get("cloned_guild_id") or 0) != int(guild.id):
                continue

            in_sitemap = int(orig_id) in valid_ids

            if in_sitemap:
                continue

            try:
                origin_gid = int(row.get("original_guild_id") or 0)
            except Exception:
                origin_gid = 0

            settings = (
                resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=origin_gid,
                    cloned_guild_id=int(guild.id),
                )
                or {}
            )
            delete_channels = bool(settings.get("DELETE_CHANNELS", False))

            clone_cat_id = int(row.get("cloned_category_id") or 0)
            clone_cat = guild.get_channel(clone_cat_id) if clone_cat_id else None

            if clone_cat and delete_channels:
                try:
                    for ch in list(getattr(clone_cat, "channels", []) or []):
                        try:
                            await self.ratelimit.acquire_for_guild(
                                ActionType.DELETE_CHANNEL, guild.id
                            )
                            await ch.delete()
                            logger.info(
                                "[🗑️] Deleted channel %s in removed category %s",
                                getattr(ch, "name", ch.id),
                                getattr(clone_cat, "name", clone_cat.id),
                            )
                        except Exception:
                            logger.debug(
                                "Failed to delete channel %s under category %s",
                                getattr(ch, "id", "?"),
                                getattr(clone_cat, "id", "?"),
                                exc_info=True,
                            )
                except Exception:
                    logger.debug(
                        "Iterating child channels failed for category %s",
                        clone_cat_id,
                        exc_info=True,
                    )

            if clone_cat and delete_channels:
                try:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.DELETE_CHANNEL, guild.id
                    )
                    await clone_cat.delete()
                    logger.info(
                        "[🗑️] Deleted category %s (id=%s) for clone %s",
                        getattr(clone_cat, "name", clone_cat_id),
                        clone_cat_id,
                        guild.id,
                    )
                except Exception:
                    logger.debug(
                        "Failed to delete category %s", clone_cat_id, exc_info=True
                    )

            try:
                cur = self.db.conn.execute(
                    """
                        SELECT original_channel_id
                        FROM channel_mappings
                        WHERE original_parent_category_id = ? AND cloned_guild_id = ?
                        """,
                    (int(orig_id), int(guild.id)),
                )
                rows = cur.fetchall() or []
            except Exception:
                rows = []

            for r in rows:
                try:
                    ocid = int(
                        r["original_channel_id"] if isinstance(r, dict) else r[0]
                    )
                except Exception:
                    continue
                try:
                    self.db.delete_channel_mapping_pair(int(ocid), int(guild.id))
                except Exception:
                    with contextlib.suppress(Exception):
                        self.db.delete_channel_mapping(int(ocid))

                try:
                    self.chan_map_by_clone.setdefault(int(guild.id), {}).pop(
                        int(ocid), None
                    )
                except Exception:
                    pass
                try:
                    self.chan_map.pop(int(ocid), None)
                except Exception:
                    pass

            try:
                self.db.delete_category_mapping_pair(int(orig_id), int(guild.id))
            except Exception:
                with contextlib.suppress(Exception):
                    self.db.delete_category_mapping(int(orig_id))

            try:
                per_all.setdefault(int(guild.id), {}).pop(int(orig_id), None)
            except Exception:
                pass
            try:
                self.cat_map.pop(int(orig_id), None)
            except Exception:
                pass

            if delete_channels:
                removed_with_delete_channels += 1
            else:
                removed_mappings_only += 1

            removed += 1

        if removed_with_delete_channels:
            logger.info(
                "[🗑️] Cleanup: removed %d category(ies) and their channel mappings for clone %s",
                removed_with_delete_channels,
                guild.id,
            )
        if removed_mappings_only:
            logger.info(
                "[🧹] Cleanup: removed DB mappings for %d category(ies) in clone %s",
                removed_mappings_only,
                guild.id,
            )

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
        Delete cloned channels not present on the host, for THIS clone guild only.
        Always removes the (origin, clone) mapping row for this guild when host no longer has it.

        Summary:
        - If DELETE_CHANNELS is true and the channel is not protected, delete the channel + mapping.
        - Otherwise, keep the channel (or it's already gone / protected / API-blocked) and
        delete only the DB mapping.
        """

        self._load_mappings()

        valid_ids = {int(c["id"]) for c in (incoming or [])}
        removed = 0
        removed_with_delete = 0
        removed_mappings_only = 0

        protected = self._protected_channel_ids(guild)

        per_all = getattr(self, "chan_map_by_clone", {}) or {}
        per_clone = dict(per_all.get(int(guild.id), {}) or {})

        for orig_id_key, raw_row in list(per_clone.items()):
            row = self._rowdict(raw_row)
            try:
                orig_id = int(orig_id_key)
            except Exception:
                try:
                    orig_id = int(row.get("original_channel_id"))
                except Exception:
                    continue

            if int(row.get("cloned_guild_id") or 0) != int(guild.id):
                continue
            if int(orig_id) in valid_ids:
                continue

            clone_id = int(row.get("cloned_channel_id") or 0)
            ch = guild.get_channel(clone_id)

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

            deleted_here = False
            mappings_only_here = False

            if ch and delete_channels:
                if ch.id in protected:
                    logger.info(
                        "[🛡️] Skipping deletion of protected channel #%s (%d).",
                        ch.name,
                        ch.id,
                    )
                    mappings_only_here = True
                else:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.DELETE_CHANNEL, guild.id
                    )
                    try:
                        await ch.delete()
                        logger.info("[🗑️] Deleted channel #%s (%d)", ch.name, ch.id)
                        deleted_here = True
                    except discord.HTTPException as e:

                        if getattr(
                            e, "code", None
                        ) == 50074 or "required for community" in str(e):
                            logger.info(
                                "[🛡️] API blocked deletion of #%s (%d): protected. Dropping mapping only.",
                                getattr(ch, "name", "?"),
                                ch.id,
                            )
                        else:
                            logger.warning(
                                "[⚠️] Failed to delete channel #%d: %s", ch.id, e
                            )
                        mappings_only_here = True
            elif ch and not delete_channels:

                mappings_only_here = True
            elif not ch:

                logger.info(
                    "[🗑️] Cloned channel #%d not found; removing mapping", clone_id
                )
                mappings_only_here = True

            try:
                self.db.delete_channel_mapping_pair(int(orig_id), int(guild.id))
            except Exception:
                self.db.delete_channel_mapping(int(orig_id))

            per_all.setdefault(int(guild.id), {}).pop(int(orig_id), None)
            self.chan_map.pop(int(orig_id), None)

            if deleted_here:
                removed_with_delete += 1
            elif mappings_only_here:
                removed_mappings_only += 1

            removed += 1

        if removed_with_delete:
            logger.info(
                "[🗑️] Cleanup: removed %d channel(s) and their mappings for clone %s",
                removed_with_delete,
                guild.id,
            )
        if removed_mappings_only:
            logger.info(
                "[🧹] Cleanup: removed DB mappings for %d channel(s) in clone %s",
                removed_mappings_only,
                guild.id,
            )

        return removed

    async def _maybe_rename_category(
        self,
        cat: discord.CategoryChannel,
        *,
        host_guild_id: int,
        original_category_id: int,
        upstream_name: str | None,
    ) -> tuple[bool, str]:
        try:
            per_clone = (self.cat_map_by_clone or {}).get(int(cat.guild.id), {}) or {}
            mapping = per_clone.get(int(original_category_id))
            if mapping is None:
                mapping = self.db.get_category_mapping_by_original_and_clone(
                    int(original_category_id), int(cat.guild.id)
                )
                if mapping and not isinstance(mapping, dict):
                    mapping = dict(mapping)
                if mapping:
                    self.cat_map_by_clone.setdefault(int(cat.guild.id), {})[
                        int(original_category_id)
                    ] = mapping

            pinned = ((mapping or {}).get("cloned_category_name") or "").strip()
            has_pin = bool(pinned)

            try:
                settings = resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=int(host_guild_id),
                    cloned_guild_id=int(cat.guild.id),
                )
            except Exception:
                settings = self.config.default_mapping_settings()

            rename_enabled = settings.get("RENAME_CHANNELS", True)

            if has_pin:
                desired = pinned
            elif rename_enabled and upstream_name:
                desired = upstream_name.strip()
            else:
                return False, "skipped_rename_disabled"

            current = (getattr(cat, "name", "") or "").strip()

            if not desired or current == desired:
                return False, "skipped_already_ok"

            await self.ratelimit.acquire_for_guild(
                ActionType.EDIT_CHANNEL, cat.guild.id
            )
            await cat.edit(
                name=desired, reason="Copycord: apply pinned/or upstream category name"
            )

            logger.info("[📌] Renamed category %r → %r", current, desired)

            try:
                self.db.upsert_category_mapping(
                    orig_id=int(original_category_id),
                    orig_name=(upstream_name or cat.name or "").strip(),
                    clone_id=int(cat.id),
                    clone_name=(pinned or None),
                    original_guild_id=int(host_guild_id),
                    cloned_guild_id=int(cat.guild.id),
                )
            except Exception:
                logger.debug("[rename] category upsert failed", exc_info=True)

            return True, ("pinned_enforced" if has_pin else "match_upstream")

        except Exception:
            logger.debug(
                "_maybe_rename_category failed for cat=%s",
                getattr(cat, "id", "?"),
                exc_info=True,
            )
            return False, "skipped_error"

    async def _handle_renamed_categories(
        self, guild: discord.Guild, sitemap: Dict
    ) -> int:
        """Rename categories in THIS clone to match upstream (or pinned) if RENAME_CHANNELS is enabled."""
        renamed = 0
        desired = {c["id"]: c["name"] for c in sitemap.get("categories", [])}
        host_guild_id = (sitemap.get("guild") or {}).get("id")

        per_clone = (getattr(self, "cat_map_by_clone", {}) or {}).get(int(guild.id), {})
        for orig_id, row in list(per_clone.items()):
            upstream_name = desired.get(orig_id)
            if not upstream_name:
                continue
            clone_id = int(row.get("cloned_category_id") or 0)
            if not clone_id:
                continue
            clone_cat = guild.get_channel(clone_id)
            if not clone_cat:
                continue

            did, _ = await self._maybe_rename_category(
                clone_cat,
                host_guild_id=host_guild_id,
                original_category_id=orig_id,
                upstream_name=upstream_name,
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
        Ensure a mapping exists for (original_category_id, THIS clone guild).
        Returns (category_obj, did_create).
        """

        if not hasattr(self, "cat_map_by_clone") or self.cat_map_by_clone is None:
            self.cat_map_by_clone = {}
        per_clone = self.cat_map_by_clone.setdefault(int(guild.id), {})

        row = per_clone.get(int(original_id))
        if row is not None and not isinstance(row, dict):
            row = dict(row)
            per_clone[int(original_id)] = row

        if row:
            clone_cid = int(row.get("cloned_category_id") or 0)
            if clone_cid:
                cat = guild.get_channel(clone_cid)
                if cat:
                    return cat, False

        try:
            dbrow = self.db.get_category_mapping_by_original_and_clone(
                int(original_id), int(guild.id)
            )
        except Exception:
            dbrow = None

        if dbrow:

            if not isinstance(dbrow, dict):
                dbrow = {k: dbrow[k] for k in dbrow.keys()}
            clone_id = int(dbrow.get("cloned_category_id") or 0)
            if clone_id:
                cat = guild.get_channel(clone_id)
                if cat:
                    entry = {
                        "original_category_id": int(original_id),
                        "cloned_category_id": clone_id,
                        "original_category_name": dbrow.get("original_category_name")
                        or name,
                        "cloned_category_name": dbrow.get("cloned_category_name"),
                        "original_guild_id": int(dbrow.get("original_guild_id") or 0),
                        "cloned_guild_id": int(guild.id),
                    }
                    per_clone[int(original_id)] = entry

                    if not hasattr(self, "cat_map") or self.cat_map is None:
                        self.cat_map = {}
                    self.cat_map[int(original_id)] = dict(entry)
                    return cat, False

        await self.ratelimit.acquire_for_guild(ActionType.CREATE_CHANNEL, guild.id)
        cat = await guild.create_category(name)
        logger.info("[➕] Created category '%s' #%d", name, cat.id)

        self.db.upsert_category_mapping(
            orig_id=original_id,
            orig_name=name,
            clone_id=cat.id,
            clone_name=None,
            original_guild_id=host_guild_id,
            cloned_guild_id=guild.id,
        )

        entry = {
            "original_category_id": int(original_id),
            "cloned_category_id": int(cat.id),
            "original_category_name": name,
            "cloned_category_name": None,
            "original_guild_id": int(host_guild_id or 0),
            "cloned_guild_id": int(guild.id),
        }
        per_clone[int(original_id)] = entry
        if not hasattr(self, "cat_map") or self.cat_map is None:
            self.cat_map = {}
        self.cat_map[int(original_id)] = dict(entry)
        return cat, True

    async def _create_webhook_safely(self, ch, name, avatar_bytes):
        if self._shutting_down:
            return
        gate = self._get_webhook_gate(int(ch.guild.id))
        async with gate:
            await self.ratelimit.acquire_for_guild(
                ActionType.WEBHOOK_CREATE, ch.guild.id
            )
            try:
                await self._get_default_avatar_bytes()
            except Exception:
                pass
            webhook = await ch.create_webhook(name=name, avatar=avatar_bytes)
            logger.info("[➕] Created webhook '%s' in channel #%s", name, ch.id)
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
        if self._shutting_down:
            return

        category = None
        if parent_id is not None:
            category, _ = await self._ensure_category(
                guild, parent_id, parent_name, host_guild_id
            )

        per = self.chan_map_by_clone.setdefault(int(guild.id), {})
        row = per.get(int(original_id))
        if not row:
            try:
                row = self.db.get_channel_mapping_by_original_and_clone(
                    int(original_id), int(guild.id)
                )
            except Exception:
                row = None

        if row is not None and not isinstance(row, dict):
            row = dict(row)
            per[int(original_id)] = row

        if row:
            clone_id = int(row.get("cloned_channel_id") or 0)
            wh_url = row.get("channel_webhook_url")
            ch = guild.get_channel(clone_id) if clone_id else None

            if ch:
                if not wh_url:
                    wh = await self._create_webhook_safely(
                        ch, "Copycord", await self._get_default_avatar_bytes()
                    )
                    wh_url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"
                    self.db.upsert_channel_mapping(
                        int(original_id),
                        original_name,
                        int(clone_id),
                        wh_url,
                        int(parent_id) if parent_id is not None else None,
                        int(category.id) if category else None,
                        int(channel_type),
                        original_guild_id=(
                            int(host_guild_id) if host_guild_id is not None else None
                        ),
                        cloned_guild_id=int(guild.id),
                    )

                per[int(original_id)] = {
                    "original_channel_id": int(original_id),
                    "original_channel_name": original_name,
                    "cloned_channel_id": int(clone_id),
                    "channel_webhook_url": wh_url,
                    "original_parent_category_id": (
                        int(parent_id) if parent_id is not None else None
                    ),
                    "cloned_parent_category_id": int(category.id) if category else None,
                    "channel_type": int(channel_type),
                    "original_guild_id": (
                        int(host_guild_id) if host_guild_id is not None else None
                    ),
                    "cloned_guild_id": int(guild.id),
                }
                self._schedule_flush(
                    chan_ids={int(original_id)}, thread_parent_ids={int(original_id)}
                )
                self._unmapped_warned.discard(int(original_id))
                return int(original_id), int(clone_id), str(wh_url)

            try:
                self.db.delete_channel_mapping_pair(int(original_id), int(guild.id))
            except Exception:
                self.log.exception(
                    "Failed to delete stale mapping pair for orig=%s clone=%s",
                    original_id,
                    guild.id,
                )
            per.pop(int(original_id), None)

        kind = "news" if int(channel_type) == discord.ChannelType.news.value else "text"
        ch = await self._create_channel(guild, kind, original_name, category)
        wh = await self._create_webhook_safely(
            ch, "Copycord", await self._get_default_avatar_bytes()
        )
        url = f"https://discord.com/api/webhooks/{wh.id}/{wh.token}"

        self.db.upsert_channel_mapping(
            int(original_id),
            original_name,
            int(ch.id),
            url,
            int(parent_id) if parent_id is not None else None,
            int(category.id) if category else None,
            int(channel_type),
            original_guild_id=int(host_guild_id) if host_guild_id is not None else None,
            cloned_guild_id=int(guild.id),
        )

        per[int(original_id)] = {
            "original_channel_id": int(original_id),
            "original_channel_name": original_name,
            "cloned_channel_id": int(ch.id),
            "channel_webhook_url": url,
            "original_parent_category_id": (
                int(parent_id) if parent_id is not None else None
            ),
            "cloned_parent_category_id": int(category.id) if category else None,
            "channel_type": int(channel_type),
            "original_guild_id": (
                int(host_guild_id) if host_guild_id is not None else None
            ),
            "cloned_guild_id": int(guild.id),
        }
        self._schedule_flush(
            chan_ids={int(original_id)}, thread_parent_ids={int(original_id)}
        )
        self._unmapped_warned.discard(int(original_id))
        return int(original_id), int(ch.id), str(url)

    async def _handle_master_channel_moves(
        self,
        guild: discord.Guild,
        incoming: list[dict],
        host_guild_id: int | None,
        *,
        skip_channel_ids: set[int] | None = None,
    ) -> int:
        """
        Re-parent cloned channels for THIS clone guild only, when upstream parent differs.
        Updates DB mapping for THIS clone so future syncs keep the new parent.
        """
        moved = 0
        skip_channel_ids = skip_channel_ids or set()
        host_guild_id = int(host_guild_id or 0)

        try:
            settings = resolve_mapping_settings(
                self.db,
                self.config,
                original_guild_id=host_guild_id,
                cloned_guild_id=int(guild.id),
            )
        except Exception:
            settings = self.config.default_mapping_settings()

        if not settings.get("REPOSITION_CHANNELS", True):
            logger.debug(
                "[reparent] Skipping channel repositioning for clone_g=%s (REPOSITION_CHANNELS=False)",
                guild.id,
            )
            return 0

        per_chan = (self.chan_map_by_clone or {}).get(int(guild.id), {}) or {}
        per_cat = (self.cat_map_by_clone or {}).get(int(guild.id), {}) or {}

        if not per_chan or not per_cat:
            import contextlib

            with contextlib.suppress(Exception):
                self._load_mappings()
                per_chan = (self.chan_map_by_clone or {}).get(int(guild.id), {}) or {}
                per_cat = (self.cat_map_by_clone or {}).get(int(guild.id), {}) or {}

        for item in incoming:
            orig_id = int(item["id"])
            row = per_chan.get(orig_id)
            if not row:
                r = self.db.get_channel_mapping_by_original_and_clone(
                    orig_id, int(guild.id)
                )
                if r and not isinstance(r, dict):
                    r = dict(r)
                if r:
                    row = r
                    per_chan[orig_id] = row
            if not row:
                continue

            clone_id = int(row.get("cloned_channel_id") or 0)
            ch = guild.get_channel(clone_id) if clone_id else None
            if not ch:
                continue

            if int(ch.id) in skip_channel_ids:
                continue

            upstream_parent = item.get("parent_id", None)
            upstream_parent_id = (
                int(upstream_parent) if upstream_parent is not None else None
            )

            cat_row = None
            if upstream_parent_id is not None:
                cat_row = per_cat.get(upstream_parent_id)
                if cat_row is None:
                    cr = self.db.get_category_mapping_by_original_and_clone(
                        upstream_parent_id, int(guild.id)
                    )
                    if cr and not isinstance(cr, dict):
                        cr = dict(cr)
                    if cr:
                        cat_row = cr
                        self.cat_map_by_clone.setdefault(int(guild.id), {})[
                            upstream_parent_id
                        ] = cr

            desired_parent_clone_id = (
                int(cat_row["cloned_category_id"]) if cat_row else None
            )
            desired_parent = (
                guild.get_channel(desired_parent_clone_id)
                if desired_parent_clone_id
                else None
            )

            actual_parent_id = int(ch.category.id) if ch.category else None
            actual_parent_name = getattr(ch.category, "name", None)

            if actual_parent_id == (
                desired_parent_clone_id if desired_parent_clone_id is not None else None
            ):
                continue

            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await ch.edit(category=desired_parent)
                moved += 1
                old_name = actual_parent_name or "standalone"
                new_name = getattr(desired_parent, "name", None) or "standalone"
                logger.info(
                    "[reparent:done] guild=%s(%d) ch=%r id=%d from %r → %r",
                    guild.name,
                    int(guild.id),
                    getattr(ch, "name", "?"),
                    int(ch.id),
                    old_name,
                    new_name,
                )
            except Exception:
                logger.warning(
                    "[reparent:failed] guild=%s(%d) ch=%r id=%d (desired_parent=%s)",
                    guild.name,
                    int(guild.id),
                    getattr(ch, "name", "?"),
                    int(ch.id),
                    str(desired_parent_clone_id),
                    exc_info=True,
                )
                continue

            ctype = (
                int(getattr(ch.type, "value", 0))
                if hasattr(ch.type, "value")
                else int(ch.type)
            )
            try:
                self.db.upsert_channel_mapping(
                    int(orig_id),
                    row.get("original_channel_name"),
                    int(ch.id),
                    row.get("channel_webhook_url"),
                    int(upstream_parent_id) if upstream_parent_id is not None else None,
                    (
                        int(desired_parent_clone_id)
                        if desired_parent_clone_id is not None
                        else None
                    ),
                    ctype,
                    original_guild_id=host_guild_id,
                    cloned_guild_id=int(guild.id),
                    clone_name=(row.get("clone_channel_name") or None),
                )
            finally:
                row["original_parent_category_id"] = (
                    int(upstream_parent_id) if upstream_parent_id is not None else None
                )
                row["cloned_parent_category_id"] = (
                    int(desired_parent_clone_id)
                    if desired_parent_clone_id is not None
                    else None
                )

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

    async def handle_thread_delete(self, data: dict):
        """
        Delete the cloned thread(s) that correspond to the original one.
        Works across multi-clone setups; respects per-mapping ENABLE_CLONING / DELETE_THREADS.
        """
        try:
            orig_tid = int(data.get("thread_id") or data.get("channel_id") or 0)
        except Exception:
            return
        if not orig_tid:
            return

        async def _resolve_thread_obj(
            guild, thread_id: int, forum_parent_id: int | None
        ):
            """
            Find a discord.Thread in the clone guild, trying multiple strategies:
            1) guild.get_thread(id)
            2) guild.get_channel(id)
            3) fetch_channel(id)
            4) search parent forum (active + archived)
            Returns discord.Thread or None
            """
            import discord

            try:
                th = getattr(guild, "get_thread", None)
                if callable(th):
                    t = guild.get_thread(thread_id)
                    if isinstance(t, discord.Thread):
                        return t
            except Exception:
                pass

            try:
                t = guild.get_channel(thread_id)
                if isinstance(t, discord.Thread):
                    return t
            except Exception:
                pass

            try:
                t = await self.bot.fetch_channel(thread_id)
                if isinstance(t, discord.Thread):
                    return t
            except Exception:
                t = None

            if forum_parent_id:
                try:
                    parent = guild.get_channel(
                        int(forum_parent_id)
                    ) or await self.bot.fetch_channel(int(forum_parent_id))
                except Exception:
                    parent = None

                if parent:
                    try:
                        act = await parent.fetch_active_threads()
                        for th in act.threads:
                            if int(getattr(th, "id", 0)) == thread_id:
                                return th
                    except Exception:
                        pass

                    try:
                        arch = await parent.fetch_archived_threads(limit=50)
                        for th in arch.threads:
                            if int(getattr(th, "id", 0)) == thread_id:
                                return th
                    except Exception:
                        pass

            return None

        try:
            rows = self.db.get_thread_mappings_for_original(orig_tid) or []
        except Exception:
            rows = []

        if not rows:
            logger.debug("[threads] delete: no mapping rows for thread %s", orig_tid)
            return

        host_gid = int(data.get("guild_id") or 0) or 0

        for row in rows:
            r = self._rowdict(row)
            try:
                clone_gid = int(r.get("cloned_guild_id") or 0)
                cloned_tid = int(r.get("cloned_thread_id") or 0)
                forum_cid = int(r.get("forum_cloned_id") or 0) or None
                orig_host = int(r.get("original_guild_id") or 0) or host_gid
            except Exception:
                continue

            if not (clone_gid and cloned_tid):
                continue

            try:
                settings = resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=orig_host,
                    cloned_guild_id=clone_gid,
                )
            except Exception:
                settings = self.config.default_mapping_settings()

            if not settings.get("ENABLE_CLONING", True):
                logger.debug(
                    "[threads] delete: skip gid=%s (cloning disabled)", clone_gid
                )
                continue
            if not settings.get("DELETE_THREADS", True):
                logger.debug(
                    "[threads] delete: skip gid=%s (DELETE_THREADS disabled)", clone_gid
                )
                continue

            g = self.bot.get_guild(clone_gid)
            if not g:
                logger.debug("[threads] delete: clone guild %s not in cache", clone_gid)
                continue

            try:
                t = await _resolve_thread_obj(g, cloned_tid, forum_cid)
                if t:
                    await t.delete()
                    logger.info(
                        "[🗑️] Deleted cloned thread tid=%s in clone_g=%s (parent=%s)",
                        cloned_tid,
                        clone_gid,
                        getattr(getattr(t, "parent", None), "id", None),
                    )
                else:
                    logger.debug(
                        "[threads] delete: cloned thread not found (gid=%s tid=%s) — will drop mapping",
                        clone_gid,
                        cloned_tid,
                    )
            except Exception as e:

                code = getattr(e, "code", None)
                status = getattr(e, "status", None)
                if status == 403 or code in (50013,):
                    logger.warning(
                        "[threads] delete: missing permissions to delete tid=%s in gid=%s: %r",
                        cloned_tid,
                        clone_gid,
                        e,
                    )
                else:
                    logger.warning(
                        "[threads] delete: failed to delete tid=%s in gid=%s: %r",
                        cloned_tid,
                        clone_gid,
                        e,
                    )

        try:
            self.db.delete_forum_thread_mapping(orig_tid)
        except Exception:
            pass
        logger.debug("[🗑️] Deleted thread mappings for original thread %s", orig_tid)

    async def handle_thread_rename(self, data: dict):
        """
        Rename all cloned threads that correspond to the original thread.
        Works across multi-clone setups; respects per-mapping ENABLE_CLONING.
        """
        if self._shutting_down:
            return

        try:
            orig_thread_id = int(data.get("thread_id") or 0)
            new_name = data.get("new_name")
            old_name = data.get("old_name")
            parent_name = data.get("parent_name")
            host_gid = int(data.get("guild_id") or 0) or 0
        except Exception:
            return
        if not (orig_thread_id and new_name):
            return

        try:
            rows = self.db.get_thread_mappings_for_original(orig_thread_id) or []
        except Exception:
            rows = []

        if not rows:
            logger.warning(
                "[⚠️] Thread renamed in #%s: %r → %r; no cloned mappings found, skipping",
                parent_name,
                old_name,
                new_name,
            )
            return

        for row in rows:
            r = self._rowdict(row)
            cloned_id = int(r.get("cloned_thread_id") or 0)
            clone_gid = int(r.get("cloned_guild_id") or 0)
            if not (cloned_id and clone_gid):
                continue

            try:
                settings = resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=int(r.get("original_guild_id") or host_gid or 0),
                    cloned_guild_id=clone_gid,
                )
            except Exception:
                settings = self.config.default_mapping_settings()

            if not settings.get("ENABLE_CLONING", True):
                logger.debug(
                    "[threads] rename: skip gid=%s (cloning disabled)", clone_gid
                )
                continue

            guild = self.bot.get_guild(clone_gid)
            if not guild:
                logger.error(
                    "[⛔] Clone guild %s not available for thread renames", clone_gid
                )
                continue

            ch = guild.get_channel(cloned_id)
            if not ch:
                try:
                    ch = await self.bot.fetch_channel(cloned_id)
                except NotFound:
                    logger.warning(
                        "[⚠️] Thread renamed in #%s: %r → %r; not found in clone_g=%s, cannot rename",
                        parent_name,
                        old_name,
                        new_name,
                        clone_gid,
                    )
                    continue

            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.EDIT_CHANNEL, guild.id
                )
                await ch.edit(name=new_name)
                logger.info(
                    "[✏️] Renamed thread in clone_g=%s parent #%s: %r → %r",
                    clone_gid,
                    (ch.parent.name if getattr(ch, "parent", None) else "Unknown"),
                    old_name,
                    new_name,
                )
            except Exception as e:
                logger.error(
                    "[⛔] Failed to rename thread in clone_g=%s parent #%s: %s",
                    clone_gid,
                    (ch.parent.name if getattr(ch, "parent", None) else "Unknown"),
                    e,
                )

            try:
                self.db.upsert_forum_thread_mapping(
                    orig_thread_id,
                    new_name,
                    cloned_id,
                    int(r.get("forum_original_id") or 0),
                    int(r.get("forum_cloned_id") or 0),
                    original_guild_id=int(r.get("original_guild_id") or host_gid or 0),
                    cloned_guild_id=clone_gid,
                )
            except Exception:
                logger.exception("[db] upsert_forum_thread_mapping failed on rename")

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

    def _sanitize_inline(
        self,
        s: str | None,
        *,
        ctx_guild_id: int | None = None,
        ctx_mapping_row: dict | None = None,
    ) -> str | None:
        """
        Normalize inline content (message content, embed text, etc.).

        ctx_guild_id      = original/host guild id (where the message came from)
        ctx_mapping_row   = the specific mapping row for the clone we are targeting

        With both of these, per-clone mappings (channels/roles/emojis/message links)
        can be resolved correctly when there are multiple clones for a single host.
        """
        if not s:
            return s

        clone_gid = self._clone_gid_for_ctx(
            host_guild_id=ctx_guild_id,
            mapping_row=ctx_mapping_row,
        )

        s = self._replace_emoji_ids(
            s,
            cloned_guild_id=clone_gid,
        )
        s = self._remap_channel_mentions(
            s,
            cloned_guild_id=clone_gid,
        )
        s = self._remap_role_mentions(
            s,
            cloned_guild_id=clone_gid,
        )
        s = self._rewrite_message_links(
            s,
            ctx_guild_id=ctx_guild_id,
            ctx_mapping_row=ctx_mapping_row,
        )
        return s

    def _replace_emoji_ids(
        self,
        content: str,
        *,
        cloned_guild_id: int | None = None,
    ) -> str:
        """
        Replace custom emoji IDs with the clone's emoji IDs.

        If cloned_guild_id is provided, we prefer the mapping in that clone.
        Otherwise we fall back to the legacy "any mapping" behaviour.
        """
        _emoji_re = re.compile(r"<:(\w+):(\d+)?\>")

        def repl(m: re.Match) -> str:
            full = m.group(0)
            name = m.group(1)
            mid = m.group(2)
            if not mid:
                return full
            try:
                orig_id = int(mid)
            except Exception:
                return full

            row = None

            if cloned_guild_id:
                try:
                    row = self.db.get_emoji_mapping_for_clone(
                        original_id=orig_id,
                        cloned_guild_id=int(cloned_guild_id),
                    )
                except Exception:
                    row = None

            if row is None:
                row = self.db.get_emoji_mapping(orig_id)

            if not row:
                return full

            if not isinstance(row, dict):
                try:
                    row = {k: row[k] for k in row.keys()}
                except Exception:
                    return full

            cloned_id = int(row.get("cloned_emoji_id") or orig_id)
            return f"<:{name}:{cloned_id}>"

        return _emoji_re.sub(repl, content)

    def _remap_channel_mentions(
        self,
        content: str,
        *,
        cloned_guild_id: int | None = None,
    ) -> str:
        """
        Rewrite <

        If cloned_guild_id is provided, use the mapping for THAT clone; otherwise
        fall back to the old global chan_map behaviour.
        """
        _m_ch = re.compile(r"<#(\d+)>")

        def repl(m: re.Match) -> str:
            full = m.group(0)
            raw_id = m.group(1)
            try:
                cid = int(raw_id)
            except Exception:
                return full

            row = None

            if cloned_guild_id:
                try:
                    per_clone_map = (
                        self.chan_map_by_clone.get(int(cloned_guild_id)) or {}
                    )
                    row = per_clone_map.get(cid)
                except Exception:
                    row = None

            if row is None:
                row = self.chan_map.get(cid)

            if not row:
                return full

            if not isinstance(row, dict):
                try:
                    row = {k: row[k] for k in row.keys()}
                except Exception:
                    return full

            cloned_id = int(row.get("cloned_channel_id") or cid)
            return f"<#{cloned_id}>"

        return _m_ch.sub(repl, content)

    def _remap_role_mentions(
        self,
        content: str,
        *,
        cloned_guild_id: int | None = None,
    ) -> str:
        """
        Rewrite <@&role_id> mentions to the appropriate cloned role id.
        """

        def repl(m: re.Match) -> str:
            full = m.group(0)
            raw_id = m.group("id")
            try:
                rid = int(raw_id)
            except Exception:
                return full

            row = None

            if cloned_guild_id:
                try:
                    row = self.db.get_role_mapping_for_clone(
                        original_id=rid,
                        cloned_guild_id=int(cloned_guild_id),
                    )
                except Exception:
                    row = None

            if row is None:
                row = self.db.get_role_mapping(rid)

            if not row:
                return full

            if not isinstance(row, dict):
                try:
                    row = {k: row[k] for k in row.keys()}
                except Exception:
                    return full

            cloned_id = int(row.get("cloned_role_id") or rid)
            return f"<@&{cloned_id}>"

        return self._M_ROLE.sub(repl, content)

    def _fallback_unknown_role_mentions(
        self,
        content: str,
        *,
        orig_id_to_name: dict[int, str] | None,
        cloned_id_to_name: dict[int, str] | None,
        valid_ids: set[int] | None,
    ) -> str:
        """
        Given precomputed role maps and valid clone role IDs, replace any
        <@&id> mentions that don't resolve in the clone guild with a
        plain-text '@RoleName'.

        - orig_id_to_name: original role id -> name (from client payload)
        - cloned_id_to_name: cloned role id -> name (via DB mappings)
        - valid_ids: set of role IDs that actually exist in the clone guild
        """
        if not content or not orig_id_to_name:
            return content

        cloned_id_to_name = cloned_id_to_name or {}
        valid_ids = valid_ids or set()

        def repl(m: re.Match) -> str:
            full = m.group(0)
            raw_id = m.group("id")
            try:
                rid = int(raw_id)
            except Exception:
                return full

            if rid in valid_ids:
                return full

            name = cloned_id_to_name.get(rid) or orig_id_to_name.get(rid)
            if not name:
                return full

            return f"@{name}"

        return self._M_ROLE.sub(repl, content)

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

        def _row_to_dict(r):
            if r is None or isinstance(r, dict):
                return r
            try:
                return {k: r[k] for k in r.keys()}
            except Exception:
                return None

        def repl(m: re.Match) -> str:
            base = m.group(1)
            gid_str = m.group("gid")
            try:
                cid = int(m.group("cid"))
                mid = int(m.group("mid"))
            except Exception:
                return m.group(0)

            if gid_str == "@me":
                return m.group(0)

            host_gid = None
            try:
                if ctx_guild_id:
                    host_gid = int(ctx_guild_id)
                elif gid_str != "@me":
                    host_gid = int(gid_str)
            except Exception:
                host_gid = None

            clone_gid = None
            if host_gid:
                try:
                    clone_gid = self._clone_gid_for_ctx(
                        host_guild_id=host_gid,
                        mapping_row=ctx_mapping_row,
                    )
                except Exception:
                    clone_gid = None

                if not clone_gid:
                    try:
                        clone_gid = self._target_clone_gid_for_origin(host_gid)
                    except Exception:
                        clone_gid = None

            if not clone_gid:
                return m.group(0)

            row = None
            try:
                if hasattr(self.db, "get_message_mapping_pair"):
                    row = self.db.get_message_mapping_pair(mid, int(clone_gid))
            except Exception:
                row = None

            if row is None and hasattr(self.db, "get_mapping_by_cloned"):
                try:
                    src = self.db.get_mapping_by_cloned(mid)
                except Exception:
                    src = None
                src = _row_to_dict(src)
                if src:
                    try:
                        orig_mid = int(src.get("original_message_id") or mid)
                    except Exception:
                        orig_mid = mid
                    try:
                        row = self.db.get_message_mapping_pair(orig_mid, int(clone_gid))
                    except Exception:
                        row = None

            row = _row_to_dict(row)

            ch_row = None
            if (row is None) or not row.get("cloned_channel_id"):
                try:
                    per_clone = getattr(self, "chan_map_by_clone", None) or {}
                    per = per_clone.get(int(clone_gid)) or {}
                    ch_row = per.get(cid)
                except Exception:
                    ch_row = None
                ch_row = _row_to_dict(ch_row)

            try:
                if row:
                    cloned_cid = int(
                        row.get("cloned_channel_id")
                        or (ch_row or {}).get("cloned_channel_id")
                        or cid
                    )
                    cloned_mid = int(row.get("cloned_message_id") or mid)
                elif ch_row:
                    cloned_cid = int(ch_row.get("cloned_channel_id") or cid)
                    cloned_mid = mid
                else:

                    cloned_cid = cid
                    cloned_mid = mid
            except Exception:

                return m.group(0)

            return f"{base}{int(clone_gid)}/{cloned_cid}/{cloned_mid}"

        return _link.sub(repl, content)

    def _get_role_mentions_for_message(
        self,
        cloned_channel_id: int,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> list[int]:
        """
        Get role IDs that should be mentioned for messages in this cloned channel.
        Returns list of cloned role IDs.

        Args:
            cloned_channel_id: The channel ID in the clone guild where the message will be sent
            original_guild_id: The source/host guild ID
            cloned_guild_id: The clone guild ID

        Returns:
            List of role IDs (in the clone guild) that should be mentioned
        """
        try:
            role_ids = self.db.get_role_mentions(
                original_guild_id=int(original_guild_id),
                cloned_guild_id=int(cloned_guild_id),
                cloned_channel_id=int(cloned_channel_id),
            )
            return role_ids or []
        except Exception:
            logger.exception(
                "[role-mentions] Failed to fetch role mentions for orig_g=%s clone_g=%s clone_ch=%s",
                original_guild_id,
                cloned_guild_id,
                cloned_channel_id,
            )
            return []

    def _build_webhook_payload(
            self,
            msg: Dict,
            *,
            ctx_guild_id: int | None = None,
            ctx_mapping_row: dict | None = None,
            prepend_roles: list[int] | None = None,
            target_cloned_channel_id: int | None = None,
        ) -> dict:
            """
            Constructs a webhook payload from a given message dictionary.
            """

            cloned_guild_id_for_mentions: int | None = None
            if target_cloned_channel_id is not None:
                try:
                    ch = self.bot.get_channel(int(target_cloned_channel_id))
                except Exception:
                    ch = None
                if ch is not None and getattr(ch, "guild", None) is not None:
                    try:
                        cloned_guild_id_for_mentions = int(ch.guild.id)
                    except Exception:
                        cloned_guild_id_for_mentions = None

            role_mentions_meta = msg.get("role_mentions") or []

            orig_id_to_name: dict[int, str] | None = None
            cloned_id_to_name: dict[int, str] | None = None
            valid_role_ids: set[int] | None = None

            if role_mentions_meta and cloned_guild_id_for_mentions:
                tmp_orig: dict[int, str] = {}

                for item in role_mentions_meta:
                    if not isinstance(item, dict):
                        continue
                    raw_id = item.get("id")
                    name = (item.get("name") or "").strip()
                    try:
                        rid = int(raw_id)
                    except Exception:
                        continue
                    if not rid or not name:
                        continue
                    tmp_orig[rid] = name

                if tmp_orig:
                    orig_id_to_name = tmp_orig
                    tmp_cloned: dict[int, str] = {}

                    for orig_id, name in tmp_orig.items():
                        row = None
                        try:
                            row = self.db.get_role_mapping_for_clone(
                                original_id=orig_id,
                                cloned_guild_id=int(cloned_guild_id_for_mentions),
                            )
                        except Exception:
                            row = None

                        if row is None:
                            try:
                                row = self.db.get_role_mapping(orig_id)
                            except Exception:
                                row = None

                        if not row:
                            continue

                        if not isinstance(row, dict):
                            try:
                                row = {k: row[k] for k in row.keys()}
                            except Exception:
                                continue

                        try:
                            cloned_id = int(row.get("cloned_role_id") or orig_id)
                        except Exception:
                            cloned_id = orig_id

                        tmp_cloned[cloned_id] = name

                    cloned_id_to_name = tmp_cloned

                    try:
                        g = self.bot.get_guild(int(cloned_guild_id_for_mentions))
                    except Exception:
                        g = None

                    if g is not None:
                        try:
                            valid_role_ids = {r.id for r in getattr(g, "roles", [])}
                        except Exception:
                            valid_role_ids = set()
                    else:
                        valid_role_ids = set()

            text = self._sanitize_inline(
                msg.get("content", "") or "",
                ctx_guild_id=ctx_guild_id,
                ctx_mapping_row=ctx_mapping_row,
            )

            if orig_id_to_name:
                text = self._fallback_unknown_role_mentions(
                    text,
                    orig_id_to_name=orig_id_to_name,
                    cloned_id_to_name=cloned_id_to_name,
                    valid_ids=valid_role_ids,
                )

            if prepend_roles and not msg.get("__backfill__"):
                role_mentions = " ".join(f"<@&{rid}>" for rid in prepend_roles)
                if text:
                    text = f"{role_mentions}\n{text}"
                else:
                    text = role_mentions

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
                    e.description = self._sanitize_inline(
                        e.description,
                        ctx_guild_id=ctx_guild_id,
                        ctx_mapping_row=ctx_mapping_row,
                    )
                    if orig_id_to_name:
                        e.description = self._fallback_unknown_role_mentions(
                            e.description,
                            orig_id_to_name=orig_id_to_name,
                            cloned_id_to_name=cloned_id_to_name,
                            valid_ids=valid_role_ids,
                        )

                if getattr(e, "title", None):
                    e.title = self._sanitize_inline(
                        e.title,
                        ctx_guild_id=ctx_guild_id,
                        ctx_mapping_row=ctx_mapping_row,
                    )
                    if orig_id_to_name:
                        e.title = self._fallback_unknown_role_mentions(
                            e.title,
                            orig_id_to_name=orig_id_to_name,
                            cloned_id_to_name=cloned_id_to_name,
                            valid_ids=valid_role_ids,
                        )

                if getattr(e, "footer", None) and getattr(e.footer, "text", None):
                    e.footer.text = self._sanitize_inline(
                        e.footer.text,
                        ctx_guild_id=ctx_guild_id,
                        ctx_mapping_row=ctx_mapping_row,
                    )
                    if orig_id_to_name:
                        e.footer.text = self._fallback_unknown_role_mentions(
                            e.footer.text,
                            orig_id_to_name=orig_id_to_name,
                            cloned_id_to_name=cloned_id_to_name,
                            valid_ids=valid_role_ids,
                        )

                if getattr(e, "author", None) and getattr(e.author, "name", None):
                    e.author.name = self._sanitize_inline(
                        e.author.name,
                        ctx_guild_id=ctx_guild_id,
                        ctx_mapping_row=ctx_mapping_row,
                    )
                    if orig_id_to_name:
                        e.author.name = self._fallback_unknown_role_mentions(
                            e.author.name,
                            orig_id_to_name=orig_id_to_name,
                            cloned_id_to_name=cloned_id_to_name,
                            valid_ids=valid_role_ids,
                        )

                for f in getattr(e, "fields", []) or []:
                    if getattr(f, "name", None):
                        f.name = self._sanitize_inline(
                            f.name,
                            ctx_guild_id=ctx_guild_id,
                            ctx_mapping_row=ctx_mapping_row,
                        )
                        if orig_id_to_name:
                            f.name = self._fallback_unknown_role_mentions(
                                f.name,
                                orig_id_to_name=orig_id_to_name,
                                cloned_id_to_name=cloned_id_to_name,
                                valid_ids=valid_role_ids,
                            )
                    if getattr(f, "value", None):
                        f.value = self._sanitize_inline(
                            f.value,
                            ctx_guild_id=ctx_guild_id,
                            ctx_mapping_row=ctx_mapping_row,
                        )
                        if orig_id_to_name:
                            f.value = self._fallback_unknown_role_mentions(
                                f.value,
                                orig_id_to_name=orig_id_to_name,
                                cloned_id_to_name=cloned_id_to_name,
                                valid_ids=valid_role_ids,
                            )

            custom_username = msg.get("author") or "Unknown"
            custom_avatar_url = msg.get("avatar_url")

            if ctx_mapping_row:
                orig_gid = ctx_mapping_row.get("original_guild_id")
                clone_gid = ctx_mapping_row.get("cloned_guild_id")
                try:
                    mapping_settings = resolve_mapping_settings(
                        self.db,
                        self.config,
                        original_guild_id=int(orig_gid) if orig_gid else None,
                        cloned_guild_id=int(clone_gid) if clone_gid else None,
                    )
                except Exception:
                    mapping_settings = {}

                if mapping_settings.get("ANONYMIZE_USERS", False):
                    user_id = msg.get("author_id") or msg.get("user_id") or 0
                    anon_name, anon_avatar = _anonymize_user(user_id)
                    custom_username = anon_name
                    custom_avatar_url = anon_avatar

                if mapping_settings.get("DISABLE_EVERYONE_MENTIONS", False):

                    text = text.replace("@everyone", "@\u200beveryone")
                    text = text.replace("@here", "@\u200bhere")

                    for e in embeds:
                        if getattr(e, "description", None):
                            e.description = e.description.replace(
                                "@everyone", "@\u200beveryone"
                            ).replace("@here", "@\u200bhere")
                        if getattr(e, "title", None):
                            e.title = e.title.replace(
                                "@everyone", "@\u200beveryone"
                            ).replace("@here", "@\u200bhere")
                        if getattr(e, "footer", None) and getattr(e.footer, "text", None):
                            e.footer.text = e.footer.text.replace(
                                "@everyone", "@\u200beveryone"
                            ).replace("@here", "@\u200bhere")
                        if getattr(e, "author", None) and getattr(e.author, "name", None):
                            e.author.name = e.author.name.replace(
                                "@everyone", "@\u200beveryone"
                            ).replace("@here", "@\u200bhere")
                        for f in getattr(e, "fields", []) or []:
                            if getattr(f, "name", None):
                                f.name = f.name.replace(
                                    "@everyone", "@\u200beveryone"
                                ).replace("@here", "@\u200bhere")
                            if getattr(f, "value", None):
                                f.value = f.value.replace(
                                    "@everyone", "@\u200beveryone"
                                ).replace("@here", "@\u200bhere")

            if target_cloned_channel_id and ctx_mapping_row:
                try:
                    clone_gid = int(ctx_mapping_row.get("cloned_guild_id") or 0)
                    if clone_gid:
                        profile = self.db.get_channel_webhook_profile(
                            cloned_channel_id=int(target_cloned_channel_id),
                            cloned_guild_id=clone_gid,
                        )
                        if profile:
                            if profile.get("webhook_name"):
                                custom_username = profile["webhook_name"]
                            if profile.get("webhook_avatar_url"):
                                custom_avatar_url = profile["webhook_avatar_url"]
                except Exception:
                    logger.debug("Failed to fetch channel webhook profile", exc_info=True)

            base = {
                "username": custom_username,
                "avatar_url": custom_avatar_url,
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
        Forwards a message to the appropriate channel webhook(s) based on the channel mapping.
        Now fans out to ALL clone mappings for the origin channel.
        """
        if self._shutting_down:
            return

        tag = self._log_tag(msg)
        source_id = int(msg["channel_id"])
        host_guild_id = int(msg.get("guild_id") or 0)
        is_backfill = bool(msg.get("__backfill__"))

        def _all_chan_mappings_for_origin(origin_channel_id: int) -> list[dict]:
            origin_channel_id = int(origin_channel_id)
            rows: list[dict] = []
            by_clone = getattr(self, "chan_map_by_clone", None) or {}
            for _cg, per_map in by_clone.items():
                r = per_map.get(origin_channel_id)
                if r:
                    rows.append(dict(r))
            if not rows:
                r = (self.chan_map or {}).get(origin_channel_id)
                if r:
                    rows.append(dict(r))

            if not rows:
                try:
                    rows = list(
                        self.db.get_channel_mappings_for_original(origin_channel_id)
                        or []
                    )
                except Exception:
                    rows = []
            return rows

        def _bf_targets_for_source(source_channel_id: int) -> set[int]:
            """
            Determine which *clone channel IDs* should receive backfill messages for this source.
            Priority:
            1) Explicit hints carried on the message (e.g. __bf_target_clone_cids__/__bf_target_clone_cid__)
            2) BackfillManager (if it exposes an API/attribute with active targets)
            3) Fallback: empty set (means 'no restriction' — send to all)
            """
            targets: set[int] = set()

            hint_many = msg.get("__bf_target_clone_cids__")
            hint_one = msg.get("__bf_target_clone_cid__")
            try:
                if isinstance(hint_many, (list, tuple, set)):
                    targets |= {int(x) for x in hint_many if x is not None}
                if isinstance(hint_one, int):
                    targets.add(int(hint_one))
                elif isinstance(hint_one, str) and hint_one.isdigit():
                    targets.add(int(hint_one))
            except Exception:
                pass

            if not targets:
                bf = getattr(self, "backfill", None)
                try:

                    if hasattr(bf, "targets_for_source"):
                        maybe = bf.targets_for_source(int(source_channel_id)) or []
                        targets |= {int(x) for x in maybe}

                    elif hasattr(bf, "active_targets"):
                        maybe = (bf.active_targets or {}).get(
                            int(source_channel_id)
                        ) or []
                        targets |= {int(x) for x in maybe}
                except Exception:
                    pass

            return targets

        def _cached_primary_for_mapping(mapping_row: dict):
            """
            Primary webhook identity derived from backfill primary_identity and this mapping's URL.
            """
            st = self.backfill._progress.get(int(source_id)) or {}
            ident = st.get("primary_identity") or {}
            purl = mapping_row.get("channel_webhook_url") or mapping_row.get(
                "webhook_url"
            )
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

        if not msg.get("__split_total__"):
            try:
                atts = list(msg.get("attachments") or [])
            except Exception:
                atts = []

            dedup_atts = []
            seen_keys = set()
            for a in atts:
                key = (
                    (a.get("url") or "").lower(),
                    (a.get("filename") or "").lower(),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                dedup_atts.append(a)

            image_atts = [a for a in dedup_atts if _is_image_att(a)]
            other_atts = [a for a in dedup_atts if not _is_image_att(a)]

            if len(image_atts) > 5:
                base_text = (msg.get("content") or "").strip()
                chunks = [image_atts[i : i + 5] for i in range(0, len(image_atts), 5)]
                for idx, chunk in enumerate(chunks):
                    sub = dict(msg)
                    sub["__split_seq__"] = idx
                    sub["__split_total__"] = len(chunks)
                    if idx == 0:
                        img_chunk = list(chunk)
                        urls = [a.get("url") for a in img_chunk if a.get("url")]
                        while urls and _calc_text_len_with_urls(base_text, urls) > 2000:
                            img_chunk.pop()
                            urls = [a.get("url") for a in img_chunk if a.get("url")]
                        sub["attachments"] = img_chunk + other_atts
                    else:
                        sub["content"] = ""
                        img_chunk = list(chunk)
                        urls = [a.get("url") for a in img_chunk if a.get("url")]
                        while urls and _calc_text_len_with_urls("", urls) > 2000:
                            img_chunk.pop()
                            urls = [a.get("url") for a in img_chunk if a.get("url")]
                        sub["attachments"] = img_chunk
                    if idx != len(chunks) - 1:
                        sub["__skip_backfill_mark__"] = True
                    await self.forward_message(sub)
                return

        precheck_payload = self._build_webhook_payload(
            msg,
            ctx_guild_id=host_guild_id or None,
            ctx_mapping_row=None,
        )
        if precheck_payload is None:
            logger.debug(
                "No webhook payload built for #%s; skipping", msg.get("channel_name")
            )
            return

        if (
            not precheck_payload.get("content")
            and not precheck_payload.get("embeds")
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

        if precheck_payload.get("content"):
            try:
                json.dumps({"content": precheck_payload["content"]})
            except (TypeError, ValueError) as e:
                logger.error(
                    "[⛔] Skipping message from #%s: content not JSON serializable: %s; content=%r",
                    msg.get("channel_name"),
                    e,
                    precheck_payload["content"],
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

        async def _primary_name_changed_for_mapping(mapping_row: dict) -> bool:
            purl = mapping_row.get("channel_webhook_url") or mapping_row.get(
                "webhook_url"
            )
            if not purl:
                return False
            return await _primary_name_changed(purl)

        async def _do_send(
            url_to_use: str,
            rl_key: str,
            *,
            mapping_row: dict,
            use_webhook_identity: bool,
            payload: dict,
            override_identity: dict | None = None,
        ):
            """
            Core send path. Uses mapping_row for clone-aware upserts and 404 recreate.
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
                        cloned_cid = int(mapping_row.get("cloned_channel_id"))
                        used_url = getattr(webhook, "url", None)
                        cloned_mid = (
                            int(getattr(sent_msg, "id", 0)) if sent_msg else None
                        )

                        clone_gid = self._clone_gid_for_ctx(
                            host_guild_id=orig_gid or None,
                            mapping_row=mapping_row,
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

                        is_last_chunk = not msg.get("__skip_backfill_mark__")
                        if is_last_chunk:
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
                        logger.exception("upsert_message_mapping failed (clone-aware)")

                    if is_backfill and not msg.get("__skip_backfill_mark__"):
                        orig_mid_val = _safe_mid(msg)
                        self.backfill.note_sent(source_id, orig_mid_val)
                        if orig_mid_val is not None:
                            self.backfill.note_checkpoint(
                                source_id, orig_mid_val, msg.get("timestamp")
                            )
                        delivered, total = self.backfill.get_progress(source_id)
                        suffix = (
                            f" [{max(total - delivered, 0)} left]"
                            if total is not None
                            else f" [{delivered} sent]"
                        )
                        logger.info(
                            "[💬] [backfill] Forwarded message to #%s (clone ch=%s) from %s (%s)%s",
                            msg.get("channel_name"),
                            mapping_row.get("cloned_channel_id"),
                            msg.get("author"),
                            msg.get("author_id"),
                            suffix,
                        )
                    else:
                        split_meta = ""
                        if msg.get("__split_total__"):
                            split_meta = f" [split {msg.get('__split_seq__', 0)+1}/{msg.get('__split_total__')} (no-count)]"
                        logger.info(
                            "[💬]%s Forwarded message to #%s (clone ch=%s) from %s (%s)%s",
                            tag,
                            msg.get("channel_name"),
                            mapping_row.get("cloned_channel_id"),
                            msg.get("author"),
                            msg.get("author_id"),
                            split_meta,
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

                        new_url = await self._recreate_webhook(
                            source_id,
                            int(mapping_row.get("cloned_guild_id") or 0) or None,
                        )
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
                            "[⛔] Failed to send message to #%s (status %s): %s",
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
            rows = _all_chan_mappings_for_origin(source_id)
            if not rows:
                self._load_mappings()
                rows = _all_chan_mappings_for_origin(source_id)

            forced_clone_cid = msg.get("__force_clone_channel_id__")
            chosen = None
            if forced_clone_cid:
                try:
                    fcid = int(forced_clone_cid)
                    for r in rows:
                        if int(r.get("cloned_channel_id") or 0) == fcid:
                            chosen = r
                            break
                except Exception:
                    chosen = None
            if chosen is None and rows:
                chosen = rows[0]

            if chosen is None:
                logger.debug(
                    "[forward] forced_url present but no mapping rows found for src %s",
                    source_id,
                )
                return

            try:
                forced_clone_gid = int(chosen.get("cloned_guild_id") or 0)
            except Exception:
                forced_clone_gid = 0

            with self._clone_log_label(forced_clone_gid):
                sem = None
                clone_for_gate = chosen.get("cloned_channel_id") or chosen.get(
                    "clone_channel_id"
                )
                if is_backfill and clone_for_gate:
                    sem = self.backfill.semaphores.setdefault(
                        int(clone_for_gate), asyncio.Semaphore(1)
                    )

                primary_url, primary_name, primary_avatar_url, primary_customized = (
                    _cached_primary_for_mapping(chosen)
                )
                try:
                    _settings_for_forced = (
                        resolve_mapping_settings(
                            self.db,
                            self.config,
                            original_guild_id=int(
                                chosen.get("original_guild_id") or host_guild_id or 0
                            ),
                            cloned_guild_id=int(chosen.get("cloned_guild_id") or 0),
                        )
                        or {}
                    )
                except Exception:
                    _settings_for_forced = {}

                if not _settings_for_forced.get("ENABLE_CLONING", True):
                    logger.debug(
                        "[forward] Skipping forced send for clone_g=%s (ENABLE_CLONING=False)",
                        chosen.get("cloned_guild_id"),
                    )
                    return

                if not is_backfill and not _settings_for_forced.get(
                    "CLONE_MESSAGES", True
                ):
                    logger.debug(
                        "[forward] Skipping message send for clone_g=%s (CLONE_MESSAGES=False)",
                        chosen.get("cloned_guild_id"),
                    )
                    return

                stickers = msg.get("stickers") or []
                if stickers:
                    cg = self._clone_gid_for_ctx(
                        host_guild_id=host_guild_id or None,
                        mapping_row=chosen,
                    )
                    guild = self.bot.get_guild(int(cg)) if cg else None
                    ch = (
                        guild.get_channel(int(chosen["cloned_channel_id"]))
                        if (
                            guild
                            and chosen
                            and chosen.get("cloned_channel_id") is not None
                        )
                        else None
                    )

                    handled = await self.stickers.send_with_fallback(
                        receiver=self,
                        ch=ch,
                        stickers=stickers,
                        mapping=chosen,
                        msg=msg,
                        source_id=source_id,
                    )

                    if handled:

                        if is_backfill:
                            self.backfill.note_sent(
                                source_id, int(msg.get("message_id") or 0)
                            )
                            self.backfill.note_checkpoint(
                                source_id,
                                int(msg.get("message_id") or 0),
                                msg.get("timestamp"),
                            )
                            delivered, total = self.backfill.get_progress(source_id)
                            if total is not None:
                                left = max(total - delivered, 0)
                                suffix = f" [{left} left]"
                            else:
                                suffix = f" [{delivered} sent]"
                            logger.info(
                                "[💬]%s Forwarded (stickers) to #%s (clone ch=%s) from %s (%s)%s",
                                tag,
                                msg.get("channel_name"),
                                chosen.get("cloned_channel_id")
                                or chosen.get("clone_channel_id"),
                                msg.get("author"),
                                msg.get("author_id"),
                                suffix,
                            )
                        return

                is_primary = bool(primary_url and forced_url == primary_url)
                use_webhook_identity = bool(primary_customized and is_primary)
                override = None
                if primary_customized and not is_primary:
                    override = {
                        "username": primary_name,
                        "avatar_url": primary_avatar_url,
                    }

                ctx_gid = (
                    int(chosen.get("original_guild_id") or host_guild_id or 0) or None
                )
                payload_for_forced = self._build_webhook_payload(
                    msg,
                    ctx_guild_id=ctx_gid,
                    ctx_mapping_row=chosen,
                )

                rl_key = f"channel:{clone_for_gate or source_id}"
                if sem:
                    async with sem:
                        if clone_for_gate:
                            await self._bf_gate(int(clone_for_gate))
                        await _do_send(
                            forced_url,
                            rl_key,
                            mapping_row=chosen,
                            use_webhook_identity=use_webhook_identity,
                            payload=payload_for_forced,
                            override_identity=override,
                        )
                else:
                    await _do_send(
                        forced_url,
                        rl_key,
                        mapping_row=chosen,
                        use_webhook_identity=use_webhook_identity,
                        payload=payload_for_forced,
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

        rows = _all_chan_mappings_for_origin(source_id)

        limit_cids = msg.get("__limit_clone_cids__")
        if limit_cids:
            try:
                _lim = {int(x) for x in (limit_cids or [])}
                rows = [r for r in rows if int(r.get("cloned_guild_id") or 0) in _lim]
            except Exception:
                pass

        if not rows:
            self._load_mappings()
            rows = _all_chan_mappings_for_origin(source_id)

        if is_backfill:
            clone_gid_hint = msg.get("cloned_guild_id")
            mapping_id_hint = msg.get("mapping_id")

            if clone_gid_hint is not None or mapping_id_hint is not None:
                narrowed: list[dict] = []

                for r in rows:
                    keep = False

                    if clone_gid_hint is not None:
                        try:
                            if int(r.get("cloned_guild_id") or 0) == int(
                                clone_gid_hint
                            ):
                                keep = True
                        except Exception:
                            pass

                    if not keep and mapping_id_hint is not None:
                        rid = r.get("id") or r.get("mapping_id")
                        if rid is not None and str(rid) == str(mapping_id_hint):
                            keep = True

                    if keep:
                        narrowed.append(r)

                if narrowed:
                    rows = narrowed

        bf_targets: set[int] = set()
        if is_backfill:
            bf_targets = _bf_targets_for_source(source_id)

        if not rows:

            async with self._warn_lock:
                if source_id not in self._unmapped_warned:
                    logger.debug(
                        "[🚫] No mapping for channel %s (%s); dropping message from %s",
                        msg.get("channel_name"),
                        msg.get("channel_id"),
                        msg.get("author"),
                    )
                    self._unmapped_warned.add(source_id)
            return

        for mapping in rows:

            try:
                clone_gid = int(mapping.get("cloned_guild_id") or 0)
            except Exception:
                clone_gid = 0

            with self._clone_log_label(clone_gid):
                try:
                    _settings = (
                        resolve_mapping_settings(
                            self.db,
                            self.config,
                            original_guild_id=int(
                                mapping.get("original_guild_id") or host_guild_id or 0
                            ),
                            cloned_guild_id=int(mapping.get("cloned_guild_id") or 0),
                        )
                        or {}
                    )

                    if not _settings.get("ENABLE_CLONING", True):
                        logger.debug(
                            "[forward] Skipping clone %s for src #%s — ENABLE_CLONING=False",
                            mapping.get("cloned_guild_id"),
                            source_id,
                        )
                        continue

                    if not is_backfill and not _settings.get("CLONE_MESSAGES", True):
                        logger.debug(
                            "[forward] Skipping clone %s for src #%s — CLONE_MESSAGES=False",
                            mapping.get("cloned_guild_id"),
                            source_id,
                        )
                        continue

                    try:
                        orig_gid = int(
                            mapping.get("original_guild_id") or host_guild_id or 0
                        )
                        content_to_check = msg.get("content", "") or ""

                        should_block, blocked_keyword = self._should_block_for_mapping(
                            content_to_check,
                            orig_gid,
                            clone_gid,
                        )

                        if should_block:
                            logger.info(
                                "[❌] Blocking message %s for clone %s: keyword '%s'",
                                msg.get("message_id"),
                                clone_gid,
                                blocked_keyword,
                            )
                            continue
                    except Exception:
                        logger.exception(
                            "[forward] Keyword check failed for clone %s", clone_gid
                        )

                    try:
                        author_id = int(msg.get("author_id") or 0)
                        if author_id:
                            should_block_user, block_reason = (
                                self._should_block_user_for_mapping(
                                    author_id,
                                    orig_gid,
                                    clone_gid,
                                )
                            )

                            if should_block_user:
                                logger.info(
                                    "[❌] Blocking message %s from user %s for clone %s: %s",
                                    msg.get("message_id"),
                                    author_id,
                                    clone_gid,
                                    block_reason,
                                )
                                continue
                    except Exception:
                        logger.exception(
                            "[forward] User filter check failed for clone %s", clone_gid
                        )

                    try:
                        ctx_gid = (
                            int(mapping.get("original_guild_id") or host_guild_id or 0)
                            or None
                        )
                    except Exception:
                        ctx_gid = host_guild_id or None

                    role_mentions = []
                    if not is_backfill:
                        try:

                            clone_cid = mapping.get("cloned_channel_id") or mapping.get(
                                "clone_channel_id"
                            )
                            if clone_cid:
                                role_mentions = self._get_role_mentions_for_message(
                                    cloned_channel_id=int(clone_cid),
                                    original_guild_id=int(ctx_gid or 0),
                                    cloned_guild_id=int(
                                        mapping.get("cloned_guild_id") or 0
                                    ),
                                )

                                if role_mentions:
                                    logger.debug(
                                        "[role-mentions] Found %d role(s) to mention for clone_ch=%s in clone_g=%s",
                                        len(role_mentions),
                                        clone_cid,
                                        mapping.get("cloned_guild_id"),
                                    )
                        except Exception:
                            logger.exception(
                                "[role-mentions] Failed to get role mentions",
                                exc_info=True,
                            )
                            role_mentions = []

                    payload_for_mapping = self._build_webhook_payload(
                        msg,
                        ctx_guild_id=ctx_gid,
                        ctx_mapping_row=mapping,
                        prepend_roles=role_mentions,
                        target_cloned_channel_id=int(clone_cid) if clone_cid else None,
                    )

                    url = mapping.get("channel_webhook_url") or mapping.get(
                        "webhook_url"
                    )
                    clone_cid = mapping.get("cloned_channel_id") or mapping.get(
                        "clone_channel_id"
                    )

                    if is_backfill and bf_targets and clone_cid not in bf_targets:
                        continue

                    stickers = msg.get("stickers") or []
                    if stickers:
                        cg = self._clone_gid_for_ctx(
                            host_guild_id=host_guild_id or None,
                            mapping_row=mapping,
                        )
                        guild = self.bot.get_guild(int(cg)) if cg else None
                        ch = (
                            guild.get_channel(int(mapping["cloned_channel_id"]))
                            if (
                                guild
                                and mapping
                                and mapping.get("cloned_channel_id") is not None
                            )
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
                                self.backfill.note_sent(
                                    source_id, int(msg.get("message_id") or 0)
                                )
                                self.backfill.note_checkpoint(
                                    source_id,
                                    int(msg.get("message_id") or 0),
                                    msg.get("timestamp"),
                                )
                                delivered, total = self.backfill.get_progress(source_id)
                                if total is not None:
                                    left = max(total - delivered, 0)
                                    suffix = f" [{left} left]"
                                else:
                                    suffix = f" [{delivered} sent]"
                                logger.info(
                                    "[💬]%s Forwarded (stickers) to #%s (clone ch=%s) from %s (%s)%s",
                                    tag,
                                    msg.get("channel_name"),
                                    clone_cid,
                                    msg.get("author"),
                                    msg.get("author_id"),
                                    suffix,
                                )
                            continue

                    if mapping and not url:

                        lock = self._guild_sync_locks.get(clone_gid)
                        if lock and lock.locked():
                            logger.info(
                                "[⌛] Sync in progress; message in #%s from %s is queued and will be sent after sync",
                                msg.get("channel_name"),
                                msg.get("author"),
                            )
                            msg["__buffered__"] = True
                            self._pending_msgs.setdefault(source_id, []).append(msg)
                            continue

                        logger.warning(
                            "[⚠️] Mapped channel %s has no webhook (clone ch=%s); attempting to recreate",
                            msg.get("channel_name"),
                            clone_cid,
                        )
                        url = await self._recreate_webhook(source_id, clone_gid)
                        if not url:
                            logger.info(
                                "[⌛] Could not recreate webhook for #%s (clone ch=%s); queued message from %s",
                                msg.get("channel_name"),
                                clone_cid,
                                msg.get("author"),
                            )
                            msg["__buffered__"] = True
                            self._pending_msgs.setdefault(source_id, []).append(msg)
                            continue

                    if is_backfill and clone_cid:
                        await self.backfill.ensure_temps_ready(int(clone_cid))
                        (
                            primary_url,
                            primary_name,
                            primary_avatar_url,
                            primary_customized,
                        ) = _cached_primary_for_mapping(mapping)
                        sem = self.backfill.semaphores.setdefault(
                            int(clone_cid), asyncio.Semaphore(1)
                        )
                        async with sem:
                            await self._bf_gate(int(clone_cid))
                            url_to_use, _ = await self.backfill.pick_url_for_send(
                                int(clone_cid), url, create_missing=False
                            )
                            rl_key = f"channel:{clone_cid}"

                            if primary_customized:
                                is_primary = bool(
                                    primary_url and url_to_use == primary_url
                                )
                                if is_primary:
                                    await _do_send(
                                        url_to_use,
                                        rl_key,
                                        mapping_row=mapping,
                                        use_webhook_identity=True,
                                        payload=payload_for_mapping,
                                        override_identity=None,
                                    )
                                else:
                                    await _do_send(
                                        url_to_use,
                                        rl_key,
                                        mapping_row=mapping,
                                        use_webhook_identity=False,
                                        payload=payload_for_mapping,
                                        override_identity={
                                            "username": primary_name,
                                            "avatar_url": primary_avatar_url,
                                        },
                                    )
                            else:
                                await _do_send(
                                    url_to_use,
                                    rl_key,
                                    mapping_row=mapping,
                                    use_webhook_identity=False,
                                    payload=payload_for_mapping,
                                    override_identity=None,
                                )
                        continue

                    primary_customized = await _primary_name_changed_for_mapping(
                        mapping
                    )
                    url_to_use = url
                    rl_key = f"channel:{clone_cid or source_id}"
                    await _do_send(
                        url_to_use,
                        rl_key,
                        mapping_row=mapping,
                        use_webhook_identity=bool(primary_customized),
                        payload=payload_for_mapping,
                        override_identity=None,
                    )

                except Exception:
                    logger.exception(
                        "[forward] Clone fan-out failed for origin ch=%s -> clone row %r",
                        source_id,
                        (
                            {
                                k: mapping.get(k)
                                for k in ("cloned_guild_id", "cloned_channel_id")
                            }
                            if isinstance(mapping, dict)
                            else mapping
                        ),
                    )
                    continue

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

    async def _get_message_mappings_with_retry(
        self,
        original_message_id: int,
        *,
        attempts: int = 5,
        base_delay: float = 0.08,
        max_delay: float = 0.8,
        jitter: float = 0.25,
        log_prefix: str = "get-msg-maps",
    ) -> list[dict]:
        """
        Retry helper to fetch ALL message-mapping rows for a given original message id.
        """
        delay = base_delay
        for i in range(attempts):
            try:
                rows = list(
                    self.db.get_message_mappings_for_original(original_message_id) or []
                )
            except Exception:
                rows = []
            if rows:
                return rows
            if i < attempts - 1:
                jd = jitter * (2 * (random.random() - 0.5)) if jitter else 0.0
                await asyncio.sleep(max(0.0, min(max_delay, delay + jd)))
                delay = min(max_delay, delay * 2)
            else:
                logger.debug(
                    "[%s] no rows for orig_mid=%s after %s attempts",
                    log_prefix,
                    original_message_id,
                    attempts,
                )
        return []

    async def _edit_with_row(self, row, data: dict, orig_mid: int) -> bool:
        try:
            cloned_mid = int(row["cloned_message_id"])
            webhook_url = row["webhook_url"]
        except Exception:
            cloned_mid = None
            webhook_url = None

        if not (cloned_mid and webhook_url):
            return False

        clone_gid = int(row.get("cloned_guild_id") or 0)

        try:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()
            wh = Webhook.from_url(webhook_url, session=self.session)
            with self._clone_log_label(clone_gid):
                try:
                    host_gid = (
                        int(row.get("original_guild_id") or data.get("guild_id") or 0)
                        or None
                    )
                except Exception:
                    host_gid = None

                built = self._build_webhook_payload(
                    data,
                    ctx_guild_id=host_gid,
                    ctx_mapping_row=row,
                )
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

    async def _fallback_resend_edit(
        self, data: dict, orig_mid: int | None = None
    ) -> None:
        """
        When native webhook edit fails, resend as a new message — but ONLY to clones
        whose mapping has EDIT_MESSAGES=True (per-clone).
        """
        try:
            source_id = int(data.get("channel_id") or 0)
            host_gid = int(data.get("guild_id") or 0)
        except Exception:
            return

        rows: list[dict] = []
        by_clone = getattr(self, "chan_map_by_clone", None) or {}
        for _cg, per_map in by_clone.items():
            r = per_map.get(int(source_id))
            if r:
                rows.append(dict(r))
        if not rows:
            r = (self.chan_map or {}).get(int(source_id))
            if r:
                rows.append(dict(r))
        if not rows:
            try:
                rows = list(
                    self.db.get_channel_mappings_for_original(int(source_id)) or []
                )
            except Exception:
                rows = []

        allowed: set[int] = set()
        for r in rows:
            try:
                clone_gid = int(r.get("cloned_guild_id") or 0)
                if not clone_gid:
                    continue
                settings = resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=host_gid,
                    cloned_guild_id=clone_gid,
                )
                if settings.get("ENABLE_CLONING", True) and settings.get(
                    "EDIT_MESSAGES", True
                ):
                    allowed.add(clone_gid)
            except Exception:
                continue

        if not allowed:
            logger.info(
                "[✏️] Fallback edit skipped for channel %s (no clones allow EDIT_MESSAGES)",
                source_id,
            )
            return

        try:
            mid_for_log = str(
                orig_mid if orig_mid is not None else (data.get("message_id") or "?")
            )
            clones_str = ", ".join(str(c) for c in sorted(allowed))
            logger.info(
                "[✏️🔁] Edited message does not exist in the clone. Resending edited message as new."
            )
        except Exception:
            pass

        resend = dict(data)
        resend["__limit_clone_cids__"] = list(allowed)
        await self.forward_message(resend)

    async def handle_message_edit(self, data: dict):
        """
        Edit the cloned message(s) corresponding to the original one.
        Works across multi-clone setups and respects per-mapping EDIT_MESSAGES.
        """
        try:
            orig_mid = int(data.get("message_id") or 0)
            orig_gid = int(data.get("guild_id") or 0)
        except Exception:
            return
        if not orig_mid:
            return

        async def _maybe_edit_for_row(row: dict, payload: dict):

            if row is not None and not isinstance(row, dict):
                row = dict(row)

            try:

                settings = resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=int(
                        row.get("original_guild_id") or orig_gid or 0
                    ),
                    cloned_guild_id=int(row.get("cloned_guild_id") or 0),
                )
            except Exception:

                settings = dict(self.config.default_mapping_settings())
                settings["EDIT_MESSAGES"] = False
                settings["ENABLE_CLONING"] = settings.get("ENABLE_CLONING", True)

            if not settings.get("ENABLE_CLONING", True):
                logger.debug(
                    "[✏️] Skipping edit for clone_g=%s (cloning disabled for this mapping)",
                    row.get("cloned_guild_id"),
                )
                return False

            if not settings.get("EDIT_MESSAGES", True):
                logger.debug(
                    "[✏️] Skipping edit for clone_g=%s (EDIT_MESSAGES disabled for this mapping)",
                    row.get("cloned_guild_id"),
                )
                return False

            return await self._edit_with_row(row, payload, orig_mid)

        rows = []
        try:
            rows = self.db.get_message_mappings_for_original(orig_mid) or []
        except Exception:
            rows = []

        rows = [dict(r) if not isinstance(r, dict) else r for r in rows]

        if rows:

            payload = self._latest_edit_payload.pop(orig_mid, data)

            edited_any = False
            for r in rows:
                cg = int((r or {}).get("cloned_guild_id") or 0)
                with self._clone_log_label(cg):
                    ok = await _maybe_edit_for_row(r, payload)
                edited_any = edited_any or ok
            if not edited_any:
                logger.debug(
                    "[✏️] No edits performed for orig %s (all disabled or failed).",
                    orig_mid,
                )
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
                rows = self.db.get_message_mappings_for_original(orig_mid) or []
            except Exception:
                rows = []

            rows = [dict(r) if not isinstance(r, dict) else r for r in rows]
            payload = self._latest_edit_payload.pop(orig_mid, data)
            if rows:
                for r in rows:
                    await _maybe_edit_for_row(r, payload)
                self._inflight_events.pop(orig_mid, None)
                return

            await self._fallback_resend_edit(payload, orig_mid)
            return

        self._latest_edit_payload[orig_mid] = data
        rows = await self._get_mappings_with_retry(
            orig_mid,
            attempts=5,
            base_delay=0.08,
            max_delay=0.8,
            jitter=0.25,
            log_prefix="edit-wait",
        )
        payload = self._latest_edit_payload.pop(orig_mid, data)

        if rows:
            for r in rows:
                if not isinstance(r, dict):
                    r = dict(r)
                await _maybe_edit_for_row(r, payload)
            return

        await self._fallback_resend_edit(payload, orig_mid)

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
        Handles forwarding of thread messages from the original guild to ALL cloned guilds
        that map the thread's parent channel (forum or text).
        """
        if self._shutting_down:
            return

        try:
            parent_id = int(data.get("thread_parent_id") or 0)
            orig_tid = int(data.get("thread_id") or 0)
        except Exception:
            logger.warning(
                "[thread] bad ids in payload: thread_id=%r parent_id=%r",
                data.get("thread_id"),
                data.get("thread_parent_id"),
            )
            return
        if not parent_id or not orig_tid:
            logger.warning("[thread] missing parent_id or thread_id in payload")
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

        host_guild_id = int(data.get("guild_id") or 0)

        payload = self._build_webhook_payload(
            data,
            ctx_guild_id=host_guild_id or None,
            ctx_mapping_row=None,
        )
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

        self._load_mappings()

        mappings: list[dict] = []

        by_clone = getattr(self, "chan_map_by_clone", None) or {}
        for cg, per in by_clone.items():
            row = per.get(int(parent_id))
            if row:
                r = dict(row) if not isinstance(row, dict) else row.copy()
                r["cloned_guild_id"] = int(cg)
                mappings.append(r)

        if not mappings:
            try:
                for r in (
                    self.db.get_channel_mappings_for_original(int(parent_id)) or []
                ):
                    rr = {k: r[k] for k in r.keys()}
                    mappings.append(rr)
            except Exception:
                pass

        if not mappings:

            async with self._warn_lock:
                if orig_tid not in self._unmapped_threads_warned:
                    logger.debug(
                        "[🚫] No mapping for thread '%s' (thread_id=%s, parent=%s); "
                        "dropping message from %s",
                        data.get("thread_name", "<unnamed>"),
                        orig_tid,
                        data.get("thread_parent_name")
                        or data.get("channel_name")
                        or parent_id,
                        data.get("author", "<unknown>"),
                    )
                    self._unmapped_threads_warned.add(orig_tid)
            return

        if is_backfill:
            clone_gid_hint = data.get("cloned_guild_id")
            mapping_id_hint = data.get("mapping_id")

            if clone_gid_hint is not None or mapping_id_hint is not None:
                narrowed: list[dict] = []

                for r in mappings:
                    keep = False

                    if clone_gid_hint is not None:
                        try:
                            if int(r.get("cloned_guild_id") or 0) == int(
                                clone_gid_hint
                            ):
                                keep = True
                        except Exception:
                            pass

                    if not keep and mapping_id_hint is not None:
                        rid = r.get("id") or r.get("mapping_id")
                        if rid is not None and str(rid) == str(mapping_id_hint):
                            keep = True

                    if keep:
                        narrowed.append(r)

                if narrowed:
                    mappings = narrowed

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

        forced_url = data.get("__force_webhook_url__")
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

        def _merge_embeds_into_payload(dst_payload: dict, src_msg: dict):
            if not dst_payload:
                return
            dst_payload["embeds"] = (dst_payload.get("embeds") or []) + (
                src_msg.get("embeds") or []
            )

        def _coerce_embeds_inplace(p: dict) -> None:
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

        def _thread_lock_key(tid: int, gid: int):
            return (int(tid), int(gid))

        forced_clone_cid = data.get("__force_clone_channel_id__")
        if is_backfill and forced_clone_cid:
            try:
                fcid = int(forced_clone_cid)
                narrowed = [
                    r for r in mappings if int(r.get("cloned_channel_id") or 0) == fcid
                ]
                if narrowed:
                    mappings = narrowed
            except Exception:
                pass

        for mrow in mappings:

            try:
                clone_gid = int(mrow.get("cloned_guild_id") or 0)
            except Exception:
                clone_gid = 0

            with self._clone_log_label(clone_gid):

                try:
                    _settings = (
                        resolve_mapping_settings(
                            self.db,
                            self.config,
                            original_guild_id=int(data.get("guild_id") or 0),
                            cloned_guild_id=int(mrow.get("cloned_guild_id") or 0),
                        )
                        or {}
                    )
                except Exception:
                    _settings = {}

                if not _settings.get("ENABLE_CLONING", True):
                    logger.debug(
                        "[thread-forward] Skipping clone_g=%s for thread_id=%s — ENABLE_CLONING=False",
                        mrow.get("cloned_guild_id"),
                        data.get("thread_id"),
                    )
                    continue

                if not is_backfill and not _settings.get("CLONE_MESSAGES", True):
                    logger.debug(
                        "[thread-forward] Skipping clone_g=%s for thread_id=%s — CLONE_MESSAGES=False",
                        mrow.get("cloned_guild_id"),
                        data.get("thread_id"),
                    )
                    continue

                try:
                    content_to_check = data.get("content", "") or ""

                    should_block, blocked_keyword = self._should_block_for_mapping(
                        content_to_check,
                        int(data.get("guild_id") or 0),
                        clone_gid,
                    )

                    if should_block:
                        logger.info(
                            "[❌] Blocking thread message for clone %s: keyword '%s'",
                            clone_gid,
                            blocked_keyword,
                        )
                        continue
                except Exception:
                    logger.exception(
                        "[thread] Keyword check failed for clone %s", clone_gid
                    )

                try:
                    author_id = int(data.get("author_id") or 0)
                    if author_id:
                        should_block_user, block_reason = (
                            self._should_block_user_for_mapping(
                                author_id,
                                int(data.get("guild_id") or 0),
                                clone_gid,
                            )
                        )

                        if should_block_user:
                            logger.info(
                                "[❌] Blocking thread message from user %s for clone %s: %s",
                                author_id,
                                clone_gid,
                                block_reason,
                            )
                            continue
                except Exception:
                    logger.exception(
                        "[thread] User filter check failed for clone %s", clone_gid
                    )

                try:
                    cloned_id = int(mrow.get("cloned_channel_id") or 0)
                except Exception:
                    cloned_id = 0

                guild = self.bot.get_guild(clone_gid) if clone_gid else None
                if not guild:
                    logger.error(
                        "[⛔] No clone guild could be resolved for thread parent=%s (clone_gid=%s)",
                        parent_id,
                        clone_gid,
                    )
                    continue

                cloned_parent = guild.get_channel(cloned_id)
                if not cloned_id or not cloned_parent:
                    logger.info(
                        "[⌛] Channel %s not cloned yet in clone_g=%s; queueing",
                        cloned_id,
                        getattr(guild, "id", clone_gid),
                    )
                    self._pending_thread_msgs.append(data)
                    continue

                role_mentions: list[int] = []
                if not is_backfill:
                    try:
                        orig_gid = int(data.get("guild_id") or 0)
                        if cloned_id:
                            role_mentions = (
                                self._get_role_mentions_for_message(
                                    cloned_channel_id=int(cloned_id),
                                    original_guild_id=orig_gid,
                                    cloned_guild_id=clone_gid,
                                )
                                or []
                            )
                            if role_mentions:
                                logger.debug(
                                    "[role-mentions] Found %d role(s) to mention for thread in clone_ch=%s clone_g=%s",
                                    len(role_mentions),
                                    cloned_id,
                                    clone_gid,
                                )
                    except Exception:
                        logger.exception(
                            "[role-mentions] Failed to get role mentions for thread",
                            exc_info=True,
                        )
                        role_mentions = []

                if forced_url:
                    webhook_url = forced_url
                else:
                    webhook_url = mrow.get("channel_webhook_url") or mrow.get(
                        "webhook_url"
                    )
                    if not webhook_url:
                        try:
                            primary = mrow.get("channel_webhook_url") or mrow.get(
                                "webhook_url"
                            )
                            webhook_url, _ = await self.backfill.pick_url_for_send(
                                int(cloned_id),
                                primary_url=primary,
                                create_missing=True,
                            )
                        except Exception:

                            webhook_url = await self._recreate_webhook(
                                int(parent_id), int(data.get("guild_id") or 0)
                            )
                if not webhook_url:
                    logger.warning(
                        "[⚠️] No webhook for parent %s in clone_g=%s; queueing thread msg",
                        parent_id,
                        guild.id,
                    )
                    self._pending_thread_msgs.append(data)
                    continue

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

                lock = self._thread_locks.setdefault(
                    _thread_lock_key(orig_tid, guild.id), asyncio.Lock()
                )

                def _thread_mapping(thread_id: int) -> dict:
                    m = dict(mrow)
                    m["cloned_channel_id"] = int(thread_id)
                    return m

                async def _send_webhook_into_thread(
                    p: dict, *, include_text: bool, thread_obj
                ):
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

                    channel_profile_override = None
                    if cloned_id:
                        try:
                            profile = self.db.get_channel_webhook_profile(
                                cloned_channel_id=int(cloned_id),
                                cloned_guild_id=int(guild.id),
                            )
                            if profile:
                                channel_profile_override = {
                                    "username": profile.get("webhook_name"),
                                    "avatar_url": profile.get("webhook_avatar_url"),
                                }
                        except Exception:
                            logger.debug(
                                "Failed to fetch channel webhook profile for thread",
                                exc_info=True,
                            )

                    if channel_profile_override:

                        if channel_profile_override.get("username"):
                            kw["username"] = channel_profile_override["username"]
                        if channel_profile_override.get("avatar_url"):
                            kw["avatar_url"] = channel_profile_override["avatar_url"]
                    elif override_identity is not None:

                        if override_identity.get("username"):
                            kw["username"] = override_identity["username"]
                        if override_identity.get("avatar_url"):
                            kw["avatar_url"] = override_identity["avatar_url"]
                    elif not use_webhook_identity:

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
                            primary = mrow.get("channel_webhook_url") or mrow.get(
                                "webhook_url"
                            )

                            if forced_url_local:
                                try:
                                    self.backfill.invalidate_rotation(int(cloned_id))
                                except Exception:
                                    pass

                                url2, _ = await self.backfill.pick_url_for_send(
                                    int(cloned_id),
                                    primary_url=primary,
                                    create_missing=True,
                                )
                            else:

                                url2 = primary or getattr(wh, "url", None)

                            wh = Webhook.from_url(url2, session=self.session)
                            logger.info(
                                "%s [webhook] retrying send with new webhook url (thread_id=%s)",
                                tag,
                                thread_id,
                            )
                            sent_msg = await wh.send(**kw)
                        except Exception:
                            logger.exception(
                                "%s [webhook] retry failed (thread_id=%s)",
                                tag,
                                thread_id,
                            )
                            raise

                    try:
                        orig_gid = int(data.get("guild_id") or 0)
                        orig_cid = int(
                            data.get("thread_id") or data.get("channel_id") or 0
                        )
                        orig_mid = int(data.get("message_id") or 0)
                        cloned_cid = (
                            int(getattr(thread_obj, "id", 0)) if thread_obj else 0
                        )
                        cloned_mid = (
                            int(getattr(sent_msg, "id", 0)) if sent_msg else None
                        )
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
                        created = False

                        thr_map = self.db.get_thread_mapping_by_original_and_clone(
                            int(orig_tid), int(guild.id)
                        )
                        clone_thread: discord.Thread | None = None
                        if thr_map:
                            try:
                                t_id = int(thr_map["cloned_thread_id"] or 0)
                            except Exception:
                                t_id = 0
                            if t_id:
                                try:
                                    clone_thread = guild.get_channel(
                                        t_id
                                    ) or await self.bot.fetch_channel(t_id)
                                except HTTPException as e:
                                    if e.status == 404:
                                        self.db.delete_forum_thread_mapping(
                                            int(orig_tid)
                                        )
                                        thr_map = None
                                        clone_thread = None
                                    else:
                                        logger.warning(
                                            "[⌛]%s Error fetching thread %s in clone_g=%s; queueing",
                                            tag,
                                            t_id,
                                            guild.id,
                                        )
                                        self._pending_thread_msgs.append(data)
                                        continue

                        if not thr_map:

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
                                            ) or await self.bot.fetch_channel(
                                                int(ch_id)
                                            )
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
                                            arch = await cloned_parent.fetch_archived_threads(
                                                limit=50
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

                            async def _create_forum_thread_and_first_post():
                                base_payload_for_forum = (
                                    self._build_webhook_payload(
                                        data,
                                        ctx_guild_id=host_guild_id or None,
                                        ctx_mapping_row=mrow,
                                        prepend_roles=role_mentions,
                                        target_cloned_channel_id=(
                                            int(cloned_id) if cloned_id else None
                                        ),
                                    )
                                    or {}
                                )

                                tmp = {
                                    "content": (
                                        base_payload_for_forum.get("content") or None
                                    ),
                                    "embeds": base_payload_for_forum.get("embeds"),
                                    "username": base_payload_for_forum.get("username"),
                                    "avatar_url": base_payload_for_forum.get(
                                        "avatar_url"
                                    ),
                                }

                                if stickers and has_textish:
                                    if has_custom:
                                        data["__stickers_no_text__"] = True
                                        data["__stickers_prefer_embeds__"] = True
                                        await self.stickers.send_with_fallback(
                                            receiver=self,
                                            ch=None,
                                            stickers=stickers,
                                            mapping=mrow,
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
                                            mapping=mrow,
                                            msg=data,
                                            source_id=orig_tid,
                                        )
                                        _merge_embeds_into_payload(tmp, data)
                                    elif has_standard:
                                        tmp["content"] = "\u200b"
                                        tmp["embeds"] = None

                                _coerce_embeds_inplace(tmp)

                                if not is_backfill:
                                    await self.ratelimit.acquire_for_guild(
                                        ActionType.THREAD, guild.id
                                    )

                                channel_profile_override = None
                                if cloned_id:
                                    try:
                                        profile = self.db.get_channel_webhook_profile(
                                            cloned_channel_id=int(cloned_id),
                                            cloned_guild_id=int(guild.id),
                                        )
                                        if profile:
                                            channel_profile_override = {
                                                "username": profile.get("webhook_name"),
                                                "avatar_url": profile.get(
                                                    "webhook_avatar_url"
                                                ),
                                            }
                                    except Exception:
                                        logger.debug(
                                            "Failed to fetch channel webhook profile for forum thread creation",
                                            exc_info=True,
                                        )

                                if channel_profile_override:
                                    final_uname = channel_profile_override.get(
                                        "username"
                                    )
                                    final_av = channel_profile_override.get(
                                        "avatar_url"
                                    )
                                elif override_identity:
                                    final_uname = override_identity.get("username")
                                    final_av = override_identity.get("avatar_url")
                                elif not use_webhook_identity:
                                    final_uname = tmp.get("username")
                                    final_av = tmp.get("avatar_url")
                                else:
                                    final_uname = None
                                    final_av = None

                                if sem:
                                    async with sem:
                                        if is_backfill:
                                            await self._bf_gate(int(cloned_id))
                                        sent_msg = await thread_webhook.send(
                                            content=(tmp.get("content") or None),
                                            embeds=tmp.get("embeds"),
                                            username=final_uname,
                                            avatar_url=final_av,
                                            thread_name=data["thread_name"],
                                            wait=True,
                                        )
                                else:
                                    if is_backfill:
                                        await self._bf_gate(int(cloned_id))
                                    sent_msg = await thread_webhook.send(
                                        content=(tmp.get("content") or None),
                                        embeds=tmp.get("embeds"),
                                        username=final_uname,
                                        avatar_url=final_av,
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

                            meta = await self._get_webhook_meta(parent_id, webhook_url)

                            if isinstance(cloned_parent, ForumChannel):
                                clone_thread = (
                                    await _create_forum_thread_and_first_post()
                                )
                                if not clone_thread:
                                    continue
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
                                    if not sent:
                                        payload2 = self._build_webhook_payload(
                                            data,
                                            ctx_guild_id=host_guild_id or None,
                                            ctx_mapping_row=_thread_mapping(new_id),
                                            prepend_roles=role_mentions,
                                        )
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
                            else:
                                clone_thread = await _create_text_thread()
                                new_id = clone_thread.id

                                payload_for_thread = (
                                    self._build_webhook_payload(
                                        data,
                                        ctx_guild_id=host_guild_id or None,
                                        ctx_mapping_row=_thread_mapping(new_id),
                                        prepend_roles=role_mentions,
                                    )
                                    or {}
                                )

                                async def _send_text_thread_followup(p):
                                    if sem:
                                        async with sem:
                                            await _send_webhook_into_thread(
                                                p,
                                                include_text=True,
                                                thread_obj=clone_thread,
                                            )
                                            if is_backfill and hasattr(
                                                self, "backfill"
                                            ):
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
                                            p,
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
                                        _merge_embeds_into_payload(
                                            payload_for_thread, data
                                        )
                                        await _send_text_thread_followup(
                                            payload_for_thread
                                        )
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
                                            _merge_embeds_into_payload(
                                                payload_for_thread, data
                                            )
                                            await _send_text_thread_followup(
                                                payload_for_thread
                                            )
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
                                        payload2 = self._build_webhook_payload(
                                            data,
                                            ctx_guild_id=host_guild_id or None,
                                            ctx_mapping_row=_thread_mapping(new_id),
                                            prepend_roles=role_mentions,
                                        )
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
                                            payload2 = self._build_webhook_payload(
                                                data,
                                                ctx_guild_id=host_guild_id or None,
                                                ctx_mapping_row=_thread_mapping(new_id),
                                                prepend_roles=role_mentions,
                                            )
                                            if (
                                                meta.get("custom")
                                                or use_webhook_identity
                                            ):
                                                payload2.pop("username", None)
                                                payload2.pop("avatar_url", None)
                                            await _send_text_thread_followup(payload2)
                                else:
                                    await _send_text_thread_followup(payload_for_thread)

                                created = True

                            self.db.upsert_forum_thread_mapping(
                                orig_thread_id=int(orig_tid),
                                orig_thread_name=data["thread_name"],
                                clone_thread_id=int(new_id),
                                forum_orig_id=int(parent_id),
                                forum_clone_id=int(cloned_id),
                                original_guild_id=int(data.get("guild_id") or 0),
                                cloned_guild_id=int(guild.id),
                            )

                        if not created and clone_thread is not None:
                            meta = await self._get_webhook_meta(parent_id, webhook_url)

                            payload_for_existing = (
                                self._build_webhook_payload(
                                    data,
                                    ctx_guild_id=host_guild_id or None,
                                    ctx_mapping_row=_thread_mapping(clone_thread.id),
                                    prepend_roles=role_mentions,
                                )
                                or {}
                            )

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
                                    payload2 = self._build_webhook_payload(
                                        data,
                                        ctx_guild_id=host_guild_id or None,
                                        ctx_mapping_row=_thread_mapping(
                                            clone_thread.id
                                        ),
                                        prepend_roles=role_mentions,
                                    )
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
                                            if is_backfill and hasattr(
                                                self, "backfill"
                                            ):
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
                                    continue
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
                                        continue
                                    payload2 = self._build_webhook_payload(
                                        data,
                                        ctx_guild_id=host_guild_id or None,
                                        ctx_mapping_row=_thread_mapping(
                                            clone_thread.id
                                        ),
                                        prepend_roles=role_mentions,
                                    )
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
                                            if is_backfill and hasattr(
                                                self, "backfill"
                                            ):
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
                                    continue

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
                                    _merge_embeds_into_payload(
                                        payload_for_existing, data
                                    )
                                    if sem:
                                        async with sem:
                                            await _send_webhook_into_thread(
                                                payload_for_existing,
                                                include_text=True,
                                                thread_obj=clone_thread,
                                            )
                                            if is_backfill and hasattr(
                                                self, "backfill"
                                            ):
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
                                            payload_for_existing,
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
                                    continue
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
                                        continue
                                    _merge_embeds_into_payload(
                                        payload_for_existing, data
                                    )
                                    if sem:
                                        async with sem:
                                            await _send_webhook_into_thread(
                                                payload_for_existing,
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
                                            payload_for_existing,
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
                                    continue

                            if has_textish:
                                if sem:
                                    async with sem:
                                        await _send_webhook_into_thread(
                                            payload_for_existing,
                                            include_text=True,
                                            thread_obj=clone_thread,
                                        )
                                else:
                                    await _send_webhook_into_thread(
                                        payload_for_existing,
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

    def _bf_pick_mapping_for_target(self, original_channel_id: int) -> dict | None:
        """
        Resolve the precise mapping row for a backfill run, preferring this run's
        mapping_id / cloned_guild_id over a generic 'first row for this origin'.

        Priority:
        1) mapping_id (from guild_mappings)
        2) (original_channel_id, cloned_guild_id)
        3) cloned_channel_id
        4) first row for original_channel_id
        """
        oid = int(original_channel_id)
        st = self.backfill._progress.get(oid) or {}

        mapping_id = st.get("mapping_id")
        clone_gid = st.get("cloned_guild_id")
        clone_cid = st.get("clone_channel_id")

        if mapping_id and hasattr(self.db, "get_channel_mapping_for_mapping"):
            try:
                row = self.db.get_channel_mapping_for_mapping(oid, str(mapping_id))
            except Exception:
                row = None
            if row:
                return dict(row)

        if clone_gid and hasattr(self.db, "get_channel_mapping_by_original_and_clone"):
            try:
                row = self.db.get_channel_mapping_by_original_and_clone(
                    oid, int(clone_gid)
                )
            except Exception:
                row = None
            if row:
                return dict(row)

        if clone_cid and hasattr(self.db, "get_channel_mapping_by_clone_id"):
            try:
                row = self.db.get_channel_mapping_by_clone_id(int(clone_cid))
            except Exception:
                row = None
            if row:
                return dict(row)

        if hasattr(self.db, "get_channel_mapping_by_original_id"):
            try:
                row = self.db.get_channel_mapping_by_original_id(oid)
            except Exception:
                row = None
            if row:
                return dict(row)

        return None

    async def _handle_backfill_message(self, data: dict) -> None:
        if self._shutting_down:
            return

        try:
            cid_raw = data.get("channel_id")
            if cid_raw is None and isinstance(data.get("message"), dict):
                cid_raw = data["message"].get("channel_id")
            original_id = int(cid_raw)
        except Exception:
            logger.warning("[bf] bad channel_id in payload: %r", data.get("channel_id"))
            return

        cloned_gid_hint = None
        try:
            if data.get("cloned_guild_id") is not None:
                cloned_gid_hint = int(data["cloned_guild_id"])
        except Exception:
            cloned_gid_hint = None

        mapping_id_hint = data.get("mapping_id") or None

        if cloned_gid_hint is not None or mapping_id_hint:
            st = self.backfill._progress.setdefault(int(original_id), {})

            if cloned_gid_hint is not None:
                if st.get("cloned_guild_id") != cloned_gid_hint:
                    st.pop("clone_channel_id", None)
                st["cloned_guild_id"] = cloned_gid_hint

            if mapping_id_hint is not None:
                if st.get("mapping_id") and st.get("mapping_id") != mapping_id_hint:
                    st.pop("clone_channel_id", None)
                st["mapping_id"] = mapping_id_hint

        row = self._bf_pick_mapping_for_target(original_id)
        if not row:
            logger.warning(
                "[bf] no mapping for channel=%s; using live forward", original_id
            )
            await self.handle_message(data)
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
                "[bf] mapping found but no webhook URL | ch=%s row=%s", original_id, row
            )
            await self.handle_message(data)
            return

        st = self.backfill._progress.get(int(original_id))
        if not st:
            self.backfill.register_sink(
                original_id, user_id=None, clone_channel_id=clone_id, msg=None
            )
            logger.debug("[bf] sink registered | ch=%s clone=%s", original_id, clone_id)
        else:
            if clone_id and st.get("clone_channel_id") is None:
                st["clone_channel_id"] = clone_id

        url = primary_url

        base = dict(data)
        inner = base.pop("message", None)

        if isinstance(inner, dict):
            OVERWRITE_KEYS = {
                "content",
                "embeds",
                "attachments",
                "stickers",
                "author",
                "author_id",
                "avatar_url",
                "timestamp",
                "message_id",
            }

            for k, v in inner.items():
                if k in OVERWRITE_KEYS:
                    base[k] = v
                elif k not in base:
                    base[k] = v

        forced = base
        forced["__backfill__"] = True
        forced["__force_webhook_url__"] = url
        if clone_id is not None:
            forced["__force_clone_channel_id__"] = str(clone_id)

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

        cloned_gid_hint = None
        try:
            if data.get("cloned_guild_id") is not None:
                cloned_gid_hint = int(data["cloned_guild_id"])
        except Exception:
            cloned_gid_hint = None

        mapping_id_hint = data.get("mapping_id") or None
        if cloned_gid_hint is not None or mapping_id_hint:
            st = self.backfill._progress.setdefault(int(parent_id), {})
            if cloned_gid_hint is not None:
                st["cloned_guild_id"] = cloned_gid_hint
            if mapping_id_hint:
                st["mapping_id"] = mapping_id_hint

        row = self._bf_pick_mapping_for_target(parent_id)
        if not row:
            logger.warning(
                "[bf] no mapping for parent=%s; using live thread forward", parent_id
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
                "[bf] mapping found but no webhook URL | parent=%s row=%s",
                parent_id,
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
            if clone_id and st.get("clone_channel_id") is None:
                st["clone_channel_id"] = clone_id

        url = primary_url

        forced = dict(data)
        forced["__backfill__"] = True
        forced["__force_webhook_url__"] = url
        if clone_id is not None:
            forced["__force_clone_channel_id__"] = str(clone_id)

        if cloned_gid_hint is not None:
            forced["cloned_guild_id"] = cloned_gid_hint
        if mapping_id_hint is not None:
            forced["mapping_id"] = mapping_id_hint

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

            clone_gid = int(row.get("cloned_guild_id") or 0)

            with self._clone_log_label(clone_gid):
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
        Delete the cloned message(s) that correspond to the original one.
        Works across multi-clone setups and respects per-mapping DELETE_MESSAGES.
        """
        try:
            orig_mid = int(data.get("message_id") or 0)
            orig_gid = int(data.get("guild_id") or 0)
        except Exception:
            return
        if not orig_mid:
            return

        channel_name = data.get("channel_name")

        async def _maybe_delete_for_row(r) -> bool:

            try:
                if r is None:
                    r = {}
                elif not isinstance(r, dict):
                    r = dict(r)
            except Exception:
                r = {}

            try:
                settings = resolve_mapping_settings(
                    self.db,
                    self.config,
                    original_guild_id=int(r.get("original_guild_id") or orig_gid or 0),
                    cloned_guild_id=int(r.get("cloned_guild_id") or 0),
                )
            except Exception:
                settings = dict(self.config.default_mapping_settings())
                settings["DELETE_MESSAGES"] = False

            logger.debug(
                "[🗑️] Delete check host_g=%s clone_g=%s settings=%s",
                r.get("original_guild_id") or orig_gid,
                r.get("cloned_guild_id"),
                settings,
            )

            if not settings.get("ENABLE_CLONING", True):
                logger.debug(
                    "[🗑️] Skipping delete for clone_g=%s (cloning disabled for this mapping)",
                    r.get("cloned_guild_id"),
                )
                return False

            if not settings.get("DELETE_MESSAGES", False):
                logger.debug(
                    "[🗑️] Skipping delete for clone_g=%s (DELETE_MESSAGES disabled for this mapping)",
                    r.get("cloned_guild_id"),
                )
                return False

            return await self._delete_with_row(r, orig_mid, channel_name)

        rows = []
        try:
            rows = self.db.get_message_mappings_for_original(orig_mid) or []
        except Exception:
            rows = []

        if rows:
            deleted_any = False
            for r in rows:
                cg = int(
                    (dict(r) if not isinstance(r, dict) else r).get("cloned_guild_id")
                    or 0
                )
                with self._clone_log_label(cg):
                    ok = await _maybe_delete_for_row(r)
                deleted_any = deleted_any or ok
            if not deleted_any:
                logger.debug(
                    "[🗑️] No deletes performed for orig %s (all disabled or failed).",
                    orig_mid,
                )
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
                rows = self.db.get_message_mappings_for_original(orig_mid) or []
            except Exception:
                rows = []

            if rows:
                for r in rows:
                    await _maybe_delete_for_row(r)
                self._pending_deletes.discard(orig_mid)
                self._inflight_events.pop(orig_mid, None)
                return

            logger.debug(
                "[🕒] Delete remains queued; mapping still missing for orig %s",
                orig_mid,
            )
            return

        rows = await self._get_mappings_with_retry(
            orig_mid,
            attempts=5,
            base_delay=0.08,
            max_delay=0.8,
            jitter=0.25,
            log_prefix="delete-wait",
        )
        if rows:
            for r in rows:
                await _maybe_delete_for_row(r)
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
        we already have at sync time, scoped to THIS sitemap's mapping.

        ⚠️ DOES NOT overwrite existing settings - only enriches identity metadata.
        """
        guild_info = sitemap.get("guild") or {}
        target = sitemap.get("target") or {}

        try:
            host_gid = int(guild_info.get("id") or 0)
        except (TypeError, ValueError):
            host_gid = 0
        try:
            cloned_gid = int(target.get("cloned_guild_id") or 0)
        except (TypeError, ValueError):
            cloned_gid = 0
        mapping_id_hint = target.get("mapping_id")

        if not host_gid:
            return

        mrow = None
        if mapping_id_hint:
            mrow = self.db.get_mapping_by_id(str(mapping_id_hint))
        if not mrow and cloned_gid:
            mrow = self.db.get_mapping_by_original_and_clone(host_gid, cloned_gid)
        if not mrow:
            mrow = self.db.get_mapping_by_original(host_gid)
        if not mrow:
            return

        if not isinstance(mrow, dict):
            try:
                mrow = {k: mrow[k] for k in mrow.keys()}
            except Exception:
                mrow = dict(mrow)

        need_orig_name = not str(mrow.get("original_guild_name") or "").strip()
        need_orig_icon = not str(mrow.get("original_guild_icon_url") or "").strip()
        need_clone_name = not str(mrow.get("cloned_guild_name") or "").strip()

        if not (need_orig_name or need_orig_icon or need_clone_name):
            return

        sitemap_orig_name = str((guild_info.get("name") or "")).strip()
        sitemap_icon_url = str((guild_info.get("icon") or "")).strip()

        clone_gid_val = int(mrow.get("cloned_guild_id") or cloned_gid or 0)
        clone_guild_name = None
        if need_clone_name and clone_gid_val:
            cg_obj = self.bot.get_guild(int(clone_gid_val))
            if cg_obj:
                clone_guild_name = getattr(cg_obj, "name", None)

        final_orig_name = (
            sitemap_orig_name
            if (need_orig_name and sitemap_orig_name)
            else (mrow.get("original_guild_name") or "")
        )
        final_orig_icon = (
            sitemap_icon_url
            if (need_orig_icon and sitemap_icon_url)
            else (mrow.get("original_guild_icon_url") or None)
        )
        final_clone_name = (
            clone_guild_name
            if (need_clone_name and clone_guild_name)
            else (mrow.get("cloned_guild_name") or "")
        )

        existing_settings = mrow.get("settings")
        if isinstance(existing_settings, str):
            try:
                existing_settings = json.loads(existing_settings)
            except Exception:
                existing_settings = {}
        elif not isinstance(existing_settings, dict):
            existing_settings = {}

        try:
            self.db.upsert_guild_mapping(
                mapping_id=mrow.get("mapping_id"),
                mapping_name=mrow.get("mapping_name") or "",
                original_guild_id=host_gid,
                original_guild_name=final_orig_name,
                original_guild_icon_url=final_orig_icon,
                cloned_guild_id=clone_gid_val or None,
                cloned_guild_name=final_clone_name,
                settings=None,
                overwrite_identity=True,
            )
            logger.debug(
                "[guild-id-meta] enriched mapping %s: orig_name=%r icon=%r clone_name=%r (settings preserved)",
                mrow.get("mapping_id"),
                final_orig_name,
                final_orig_icon,
                final_clone_name,
            )
        except Exception:
            logger.exception(
                "[guild-id-meta] failed to enrich mapping for origin=%s clone=%s",
                host_gid,
                clone_gid_val,
            )

    async def _load_blocked_keywords_cache(self) -> None:
        """
        Load all blocked keywords from DB and compile regex patterns.
        Builds cache for all (original_guild_id, cloned_guild_id) pairs.
        Called at startup and after keyword updates.
        """
        async with self._blocked_keywords_lock:
            self._blocked_keywords_cache.clear()

            logger.debug("[keywords] Loading blocked keywords from database...")

            try:

                rows = self.db.conn.execute(
                    """
                    SELECT keyword, original_guild_id, cloned_guild_id
                    FROM blocked_keywords
                    ORDER BY original_guild_id, cloned_guild_id, keyword
                    """
                ).fetchall()
            except Exception as e:
                logger.exception("[keywords] Failed to load blocked keywords from DB")
                return

            if not rows:
                logger.debug("[keywords] No blocked keywords found in database")
                return

            grouped: dict[tuple[int, int], list[str]] = {}

            for row in rows:
                keyword = (row["keyword"] or "").strip().lower()
                if not keyword:
                    continue

                orig_gid = (
                    int(row["original_guild_id"])
                    if row["original_guild_id"] is not None
                    else 0
                )
                clone_gid = (
                    int(row["cloned_guild_id"])
                    if row["cloned_guild_id"] is not None
                    else 0
                )

                key = (orig_gid, clone_gid)
                grouped.setdefault(key, []).append(keyword)

            total_patterns = 0
            failed_patterns = 0

            for key, keywords in grouped.items():
                patterns = []
                for keyword in keywords:
                    try:
                        regex = re.compile(
                            rf"(?<!\w){re.escape(keyword)}(?!\w)",
                            re.IGNORECASE,
                        )
                        patterns.append((regex, keyword))
                        total_patterns += 1
                    except Exception as e:
                        failed_patterns += 1
                        logger.warning(
                            "[keywords] Failed to compile regex for keyword '%s': %s",
                            keyword,
                            e,
                        )

                if patterns:
                    self._blocked_keywords_cache[key] = patterns

            logger.debug(
                "[keywords] Loaded %d keyword patterns across %d mapping scopes (%d failed)",
                total_patterns,
                len(grouped),
                failed_patterns,
            )

    def _get_blocked_patterns_for_mapping(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> list[tuple[re.Pattern, str]]:
        """
        Get compiled regex patterns for blocked keywords for a specific mapping.
        Returns list of (pattern, keyword) tuples from the pre-loaded cache.

        Checks three scopes in order:
        1. Mapping-specific (original_guild_id, cloned_guild_id)
        2. Host-level (original_guild_id, 0)
        3. Global (0, 0)
        """
        patterns = []

        key_mapping = (int(original_guild_id), int(cloned_guild_id))
        patterns.extend(self._blocked_keywords_cache.get(key_mapping, []))

        key_host = (int(original_guild_id), 0)
        patterns.extend(self._blocked_keywords_cache.get(key_host, []))

        key_global = (0, 0)
        patterns.extend(self._blocked_keywords_cache.get(key_global, []))

        return patterns

    async def _clear_blocked_keywords_cache_async(self):
        """
        Reload blocked keywords cache from DB.
        Called after keyword updates via slash commands.
        """
        await self._load_blocked_keywords_cache()

    def _should_block_for_mapping(
        self,
        content: str,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> tuple[bool, str | None]:
        """
        Check if message content should be blocked for a specific mapping.

        Returns:
            (should_block, matched_keyword)
        """
        if not content:
            return False, None

        normalized = unicodedata.normalize("NFKC", content)

        patterns = self._get_blocked_patterns_for_mapping(
            original_guild_id,
            cloned_guild_id,
        )

        for pattern, keyword in patterns:
            if pattern.search(normalized):
                return True, keyword

        return False, None

    async def _load_user_filters_cache(self) -> None:
        """
        Load all user filters from DB and build cache.
        Cache structure: {(original_gid, cloned_gid): {'blacklist': {uid, ...}, 'whitelist': {uid, ...}}}
        """
        async with self._user_filters_lock:
            self._user_filters_cache.clear()

            logger.debug("[user-filters] Loading user filters from database...")

            try:
                rows = self.db.conn.execute(
                    """
                    SELECT user_id, original_guild_id, cloned_guild_id, filter_type
                    FROM user_filters
                    ORDER BY original_guild_id, cloned_guild_id, filter_type
                    """
                ).fetchall()
            except Exception as e:
                logger.exception("[user-filters] Failed to load user filters from DB")
                return

            if not rows:
                logger.debug("[user-filters] No user filters found in database")
                return

            total_count = 0
            for row in rows:
                try:
                    user_id = int(row["user_id"])
                    orig_gid = (
                        int(row["original_guild_id"]) if row["original_guild_id"] else 0
                    )
                    clone_gid = (
                        int(row["cloned_guild_id"]) if row["cloned_guild_id"] else 0
                    )
                    filter_type = row["filter_type"]

                    key = (orig_gid, clone_gid)
                    if key not in self._user_filters_cache:
                        self._user_filters_cache[key] = {
                            "blacklist": set(),
                            "whitelist": set(),
                        }

                    self._user_filters_cache[key][filter_type].add(user_id)
                    total_count += 1
                except Exception as e:
                    logger.warning("[user-filters] Failed to process filter row: %s", e)

            logger.debug(
                "[user-filters] Loaded %d user filters across %d mapping scopes",
                total_count,
                len(self._user_filters_cache),
            )

    def _get_user_filters_for_mapping(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> dict[str, set[int]]:
        """
        Get user filters for a specific mapping.
        Returns dict with 'blacklist' and 'whitelist' sets of user IDs.

        Checks three scopes in order (merging):
        1. Mapping-specific (original_guild_id, cloned_guild_id)
        2. Host-level (original_guild_id, 0)
        3. Global (0, 0)
        """
        result = {"blacklist": set(), "whitelist": set()}

        key_global = (0, 0)
        if key_global in self._user_filters_cache:
            result["blacklist"].update(
                self._user_filters_cache[key_global]["blacklist"]
            )
            result["whitelist"].update(
                self._user_filters_cache[key_global]["whitelist"]
            )

        key_host = (int(original_guild_id), 0)
        if key_host in self._user_filters_cache:
            result["blacklist"].update(self._user_filters_cache[key_host]["blacklist"])
            result["whitelist"].update(self._user_filters_cache[key_host]["whitelist"])

        key_mapping = (int(original_guild_id), int(cloned_guild_id))
        if key_mapping in self._user_filters_cache:
            result["blacklist"].update(
                self._user_filters_cache[key_mapping]["blacklist"]
            )
            result["whitelist"].update(
                self._user_filters_cache[key_mapping]["whitelist"]
            )

        return result

    def _should_block_user_for_mapping(
        self,
        user_id: int,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> tuple[bool, str | None]:
        """
        Check if a user should be blocked for a specific mapping.
        """
        if not user_id:
            return False, None

        filters = self._get_user_filters_for_mapping(original_guild_id, cloned_guild_id)

        if filters["whitelist"]:
            if user_id not in filters["whitelist"]:
                return True, "user_not_in_whitelist"
            return False, None

        if user_id in filters["blacklist"]:
            return True, "user_blacklisted"

        return False, None

    async def _shutdown(self):
        """
        Gracefully shut down the server:
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
            logger.info("Server shutdown complete.")

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
        """
        logger.info("[✨] Starting Copycord Server %s", CURRENT_VERSION)
        loop = asyncio.get_event_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig, lambda s=sig: asyncio.create_task(self._shutdown())
                )
            except (NotImplementedError, RuntimeError):
                break

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
